package scrape

import (
	"testing"

	"k8s.io/apimachinery/pkg/util/intstr"

	"github.com/JohanLindvall/kubescrape/internal/servicemonitors"
	"github.com/JohanLindvall/kubescrape/pkg/kubemeta"
)

// A degenerate ServiceMonitor targetPort must resolve to NOTHING — not fall
// through to name matching where an Int-typed value's empty StrVal (or an empty
// String) matches an UNNAMED container port by "" == "" and fabricates a
// phantom target prometheus-operator would never scrape.
func TestMonitorDegenerateTargetPortNoPhantom(t *testing.T) {
	pod := basePod()
	// An unnamed container port: the phantom-match target.
	pod.Containers[0].Ports = append(pod.Containers[0].Ports,
		kubemeta.ContainerPort{Name: "", Port: 6666})
	svc := monitorService()

	for name, tp := range map[string]intstr.IntOrString{
		"int-zero":     intstr.FromInt32(0),
		"int-negative": intstr.FromInt32(-1),
		"int-overflow": {Type: intstr.Int, IntVal: 70000},
		"string-empty": intstr.FromString(""),
	} {
		tp := tp
		if ts := MonitorTargets(pod, svc, "m", servicemonitors.Endpoint{TargetPort: &tp}); ts != nil {
			t.Fatalf("%s targetPort fabricated a target: %+v", name, ts)
		}
	}

	// A real name still resolves (regression guard for the fix).
	named := intstr.FromString("web")
	ts := MonitorTargets(pod, svc, "m", servicemonitors.Endpoint{TargetPort: &named})
	if len(ts) != 1 || ts[0].URL != "http://10.0.0.5:8080/metrics" {
		t.Fatalf("named targetPort broke: %+v", ts)
	}
}
