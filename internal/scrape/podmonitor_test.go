package scrape

import (
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/util/intstr"

	"github.com/JohanLindvall/kubescrape/internal/servicemonitors"
)

func TestPodMonitorTargets(t *testing.T) {
	pod := basePod() // declares container port "metrics" = 9090

	// Port by container port name.
	ts := PodMonitorTargets(pod, "ns/pm", servicemonitors.Endpoint{Port: "metrics"})
	if len(ts) != 1 || ts[0].Port != 9090 || ts[0].Source != "podmonitor" || ts[0].Monitor != "ns/pm" {
		t.Fatalf("by name: %+v", ts)
	}
	// Numeric targetPort.
	tp := intstr.FromInt32(8080)
	ts = PodMonitorTargets(pod, "ns/pm", servicemonitors.Endpoint{TargetPort: &tp})
	if len(ts) != 1 || ts[0].Port != 8080 {
		t.Fatalf("by number: %+v", ts)
	}
	// Neither port nor targetPort: the phantom-target guard.
	if ts := PodMonitorTargets(pod, "ns/pm", servicemonitors.Endpoint{}); ts != nil {
		t.Fatalf("empty endpoint produced targets: %+v", ts)
	}
	// Unknown port name resolves to nothing.
	if ts := PodMonitorTargets(pod, "ns/pm", servicemonitors.Endpoint{Port: "nope"}); ts != nil {
		t.Fatalf("unknown port name produced targets: %+v", ts)
	}
	// Endpoint auth/TLS/relabelings are stamped onto the target.
	ts = PodMonitorTargets(pod, "ns/pm", servicemonitors.Endpoint{
		Port: "metrics", Scheme: "https", InsecureSkipVerify: true,
		BearerSecret: "ns/tok/token",
		MetricRelabelings: []servicemonitors.RelabelRule{
			{Action: "drop", SourceLabels: []string{"__name__"}, Regex: "go_.*"},
		},
	})
	if len(ts) != 1 || !ts[0].InsecureSkipVerify || ts[0].AuthSecret != "ns/tok/token" ||
		len(ts[0].MetricRelabelings) != 1 || ts[0].Scheme != "https" {
		t.Fatalf("endpoint stamping: %+v", ts)
	}
}

func TestProbeTargets(t *testing.T) {
	prober := basePod()
	probe := &servicemonitors.Probe{
		Namespace: "mon", Name: "site-check",
		ProberPath: "/probe", Module: "http_2xx",
		StaticTargets: []string{"https://example.com", "https://other.io"},
	}
	ts := ProbeTargets(prober, probe, 9115)
	if len(ts) != 2 {
		t.Fatalf("targets: %+v", ts)
	}
	if ts[0].Source != "probe" || ts[0].Monitor != "mon/site-check" {
		t.Fatalf("identity: %+v", ts[0])
	}
	if !strings.Contains(ts[0].URL, ":9115/probe?") ||
		!strings.Contains(ts[0].URL, "module=http_2xx") ||
		!strings.Contains(ts[0].URL, "target=https%3A%2F%2Fexample.com") {
		t.Fatalf("url: %s", ts[0].URL)
	}
}
