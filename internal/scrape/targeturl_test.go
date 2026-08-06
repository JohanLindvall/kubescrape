package scrape

import (
	"testing"

	"k8s.io/apimachinery/pkg/util/intstr"

	"github.com/JohanLindvall/kubescrape/internal/servicemonitors"
	"github.com/JohanLindvall/kubescrape/internal/services"
	"github.com/JohanLindvall/kubescrape/pkg/kubemeta"
)

// The URL is a scrape target's identity: the server dedups on it, and the agent
// keys a scrape by it. MonitorTargetURL exists so the server can decide whether
// it already holds that identity BEFORE paying for a 592-byte target with the
// whole pod document embedded — so if the cheap half could name a target
// differently from the full half, the dedup would compare identities that do
// not exist, and a monitor's endpoint configuration would be dropped or
// duplicated according to which spelling won.
//
// Both halves must therefore agree on EVERY shape, resolution failures
// included.
func TestTargetURLHalvesAgree(t *testing.T) {
	pod := basePod() // container port "metrics" = 9090
	svc := &services.Service{
		Name: "svc", Namespace: "default", UID: "svc-uid",
		Ports: []services.Port{
			{Name: "http", Port: 80, TargetPortName: "metrics"},
			{Name: "direct", Port: 8080, TargetPortNum: 8080},
		},
	}
	num := intstr.FromInt32(8080)
	name := intstr.FromString("metrics")
	bad := intstr.FromInt32(70000)

	for _, tc := range []struct {
		name string
		ep   servicemonitors.Endpoint
	}{
		{"by service port name", servicemonitors.Endpoint{Port: "http"}},
		{"with scheme and path", servicemonitors.Endpoint{Port: "http", Scheme: "https", Path: "stats"}},
		{"numeric targetPort", servicemonitors.Endpoint{TargetPort: &num}},
		{"named targetPort", servicemonitors.Endpoint{TargetPort: &name}},
		{"unknown service port", servicemonitors.Endpoint{Port: "nope"}},
		{"neither port nor targetPort", servicemonitors.Endpoint{}},
		{"out-of-range targetPort", servicemonitors.Endpoint{TargetPort: &bad}},
	} {
		t.Run("servicemonitor/"+tc.name, func(t *testing.T) {
			url, ok := MonitorTargetURL(pod, svc, tc.ep)
			ts := MonitorTargets(pod, svc, "ns/m", tc.ep)
			assertAgrees(t, url, ok, ts)
		})
		t.Run("podmonitor/"+tc.name, func(t *testing.T) {
			ep := tc.ep
			if ep.Port == "http" {
				ep.Port = "metrics" // a PodMonitor names a CONTAINER port
			}
			url, ok := PodMonitorTargetURL(pod, ep)
			ts := PodMonitorTargets(pod, "ns/m", ep)
			assertAgrees(t, url, ok, ts)
		})
	}

	// A pod that cannot be scraped at all resolves to nothing on both halves.
	gone := basePod()
	gone.Phase = "Succeeded"
	if url, ok := MonitorTargetURL(gone, svc, servicemonitors.Endpoint{Port: "http"}); ok {
		t.Errorf("a finished pod resolved to %q", url)
	}
	if url, ok := PodMonitorTargetURL(gone, servicemonitors.Endpoint{Port: "metrics"}); ok {
		t.Errorf("a finished pod resolved to %q", url)
	}
	// And so does a nil Service on the ServiceMonitor arm.
	if url, ok := MonitorTargetURL(pod, nil, servicemonitors.Endpoint{Port: "http"}); ok {
		t.Errorf("a nil Service resolved to %q", url)
	}
}

func assertAgrees(t *testing.T, url string, ok bool, ts []kubemeta.ScrapeTarget) {
	t.Helper()
	if len(ts) == 0 {
		if ok {
			t.Errorf("the cheap half resolved %q where the full half resolved nothing", url)
		}
		return
	}
	if !ok {
		t.Fatalf("the cheap half resolved nothing where the full half built %q", ts[0].URL)
	}
	if url != ts[0].URL {
		t.Errorf("cheap half says %q, full half says %q", url, ts[0].URL)
	}
}
