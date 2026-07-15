package scrape

import (
	"testing"
	"time"

	"github.com/JohanLindvall/kubescrape/internal/servicemonitors"
	"github.com/JohanLindvall/kubescrape/internal/services"
	"k8s.io/apimachinery/pkg/util/intstr"
)

// TestAuditMonitorPhantomTargetGuard is a regression guard for commit 9c30b18:
// a ServiceMonitor endpoint naming NEITHER a port NOR a targetPort must resolve
// to nothing. Without the guard an empty ep.Port ("") matched a Service's
// unnamed port via "" == "" and fabricated a phantom target.
func TestAuditMonitorPhantomTargetGuard(t *testing.T) {
	pod := basePod()
	svc := monitorService()
	// A Service with an UNNAMED port (Name == "") — the "" == "" collision.
	svc.Ports = append(svc.Ports, services.Port{Name: "", Port: 7000, TargetPortNum: 7000})

	if ts := MonitorTargets(pod, svc, "m", servicemonitors.Endpoint{}); ts != nil {
		t.Fatalf("endpoint with no port/targetPort produced a phantom target: %+v", ts)
	}
	// A named endpoint still resolves normally.
	if ts := MonitorTargets(pod, svc, "m", servicemonitors.Endpoint{Port: "metrics"}); len(ts) != 1 {
		t.Fatalf("named endpoint should still resolve: %+v", ts)
	}
}

// TestAuditMonitorSchemePathDefaults covers ServiceMonitor endpoint scheme/path
// defaulting: empty scheme -> http, empty path -> /metrics, a path missing its
// leading slash is prefixed, and a bogus scheme falls back to http.
func TestAuditMonitorSchemePathDefaults(t *testing.T) {
	pod := basePod()
	svc := monitorService()
	tp := intstr.FromInt32(9090)

	ts := MonitorTargets(pod, svc, "m", servicemonitors.Endpoint{TargetPort: &tp})
	if len(ts) != 1 || ts[0].Scheme != "http" || ts[0].Path != "/metrics" {
		t.Fatalf("defaults not applied: %+v", ts)
	}
	ts = MonitorTargets(pod, svc, "m", servicemonitors.Endpoint{TargetPort: &tp, Scheme: "gopher", Path: "stats"})
	if len(ts) != 1 || ts[0].Scheme != "http" || ts[0].URL != "http://10.0.0.5:9090/stats" {
		t.Fatalf("scheme/path handling: %+v", ts)
	}
}

// TestAuditServiceTargetsDedupByPodPort: two DISTINCT service ports whose
// targetPort resolves to the SAME pod port produce a single target.
func TestAuditServiceTargetsDedupByPodPort(t *testing.T) {
	pod := basePod()
	svc := baseService()
	svc.Ports = []services.Port{
		{Name: "a", Port: 80, TargetPortName: "metrics"},   // -> 9090
		{Name: "b", Port: 8081, TargetPortName: "metrics"}, // -> 9090 as well
	}
	ts := ServiceTargets(pod, svc)
	if len(ts) != 1 || ts[0].Port != 9090 {
		t.Fatalf("two service ports -> same pod port should dedup to one: %+v", ts)
	}
}

// TestAuditDeletedPodNeverTarget: a pod with DeletedAt set yields no targets on
// any path even if otherwise scrapeable.
func TestAuditDeletedPodNeverTarget(t *testing.T) {
	now := time.Now()
	pod := basePod()
	pod.DeletedAt = &now
	pod.Annotations[AnnotationPort] = "9090"
	if ts := PodTargets(pod); ts != nil {
		t.Errorf("deleted pod produced pod targets: %+v", ts)
	}
	if ts := ServiceTargets(pod, baseService()); ts != nil {
		t.Errorf("deleted pod produced service targets: %+v", ts)
	}
	if ts := MonitorTargets(pod, monitorService(), "m", servicemonitors.Endpoint{Port: "metrics"}); ts != nil {
		t.Errorf("deleted pod produced monitor targets: %+v", ts)
	}
}
