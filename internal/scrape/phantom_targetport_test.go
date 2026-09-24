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

// When an endpoint sets BOTH port and targetPort, `port` wins — matching
// prometheus-operator's precedence (targetPort is its deprecated fallback).
func TestMonitorPortWinsOverTargetPort(t *testing.T) {
	pod := basePod()
	svc := monitorService()
	tp := intstr.FromString("web") // container port 8080
	ts := MonitorTargets(pod, svc, "m", servicemonitors.Endpoint{
		Port: "metrics", TargetPort: &tp, // service port "metrics" -> pod 9090
	})
	if len(ts) != 1 || ts[0].Port != 9090 {
		t.Fatalf("port precedence broken: %+v (want the `port`-resolved 9090, not targetPort's 8080)", ts)
	}
	// An unresolvable `port` does NOT fall back to targetPort (operator parity).
	if ts := MonitorTargets(pod, svc, "m", servicemonitors.Endpoint{
		Port: "nope", TargetPort: &tp,
	}); ts != nil {
		t.Fatalf("unresolvable port fell back to targetPort: %+v", ts)
	}
}

// The same degenerate targetPort values through the POD MONITOR path. Only the
// ServiceMonitor arm was covered, so deleting containerPortByName's empty-name
// guard passed the whole suite while making this path fabricate targets
// against an unnamed container port — it reached containerPortByName without
// monitorPodPort's Type check, which both kinds now share (targetPortOnPod).
func TestPodMonitorDegenerateTargetPortNoPhantom(t *testing.T) {
	pod := basePod()
	pod.Containers[0].Ports = append(pod.Containers[0].Ports,
		kubemeta.ContainerPort{Name: "", Port: 6666})

	for name, tp := range map[string]intstr.IntOrString{
		"int-zero":     intstr.FromInt32(0),
		"int-negative": intstr.FromInt32(-1),
		"int-overflow": {Type: intstr.Int, IntVal: 70000},
		"string-empty": intstr.FromString(""),
	} {
		if ts := PodMonitorTargets(pod, "ns/pm", servicemonitors.Endpoint{TargetPort: &tp}); ts != nil {
			t.Fatalf("%s targetPort fabricated a target: %+v", name, ts)
		}
	}
	// Neither port nor targetPort: caught by PodMonitorTargets' OWN early
	// guard, before containerPortByName is reached. Kept here to bracket the
	// degenerate set, not because it exercises the empty-name guard above.
	if ts := PodMonitorTargets(pod, "ns/pm", servicemonitors.Endpoint{}); ts != nil {
		t.Fatalf("empty endpoint fabricated a target: %+v", ts)
	}

	// A real name still resolves, and a real number still resolves.
	named := intstr.FromString("web")
	if ts := PodMonitorTargets(pod, "ns/pm", servicemonitors.Endpoint{TargetPort: &named}); len(ts) != 1 {
		t.Fatalf("named targetPort broke: %+v", ts)
	}
	num := intstr.FromInt32(9090)
	if ts := PodMonitorTargets(pod, "ns/pm", servicemonitors.Endpoint{TargetPort: &num}); len(ts) != 1 || ts[0].Port != 9090 {
		t.Fatalf("numeric targetPort broke: %+v", ts)
	}
}

// Both monitor kinds resolve a targetPort through ONE function
// (targetPortOnPod). They open-coded it twice, and the PodMonitor copy had no
// Type check: it agreed with the ServiceMonitor one only because
// containerPortByName happens to refuse an empty name. The last case is the
// shape that contract left open — an Int-typed value carrying a StrVal, which
// no JSON-decoded CR produces but a hand-built Endpoint can — where the
// PodMonitor copy resolved the NAME and the ServiceMonitor copy refused it.
func TestBothMonitorKindsResolveATargetPortIdentically(t *testing.T) {
	pod := basePod()
	pod.Containers[0].Ports = append(pod.Containers[0].Ports,
		kubemeta.ContainerPort{Name: "", Port: 6666})
	svc := monitorService()
	for name, tp := range map[string]intstr.IntOrString{
		"int":               intstr.FromInt32(9090),
		"int-zero":          intstr.FromInt32(0),
		"int-overflow":      {Type: intstr.Int, IntVal: 70000},
		"numeric-string":    intstr.FromString("9090"),
		"overflow-string":   intstr.FromString("4294967297"),
		"name":              intstr.FromString("web"),
		"undeclared-name":   intstr.FromString("nope"),
		"empty-string":      intstr.FromString(""),
		"int-with-a-strval": {Type: intstr.Int, IntVal: 0, StrVal: "web"},
	} {
		ep := servicemonitors.Endpoint{TargetPort: &tp}
		sp, sok := monitorPodPort(pod, svc, ep)
		pp, pok := podMonitorPodPort(pod, ep)
		if sp != pp || sok != pok {
			t.Errorf("%s: ServiceMonitor resolves (%d, %v), PodMonitor (%d, %v)", name, sp, sok, pp, pok)
		}
	}
}
