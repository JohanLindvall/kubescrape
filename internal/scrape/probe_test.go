package scrape

import (
	"testing"

	"github.com/JohanLindvall/kubescrape/internal/servicemonitors"
	"github.com/JohanLindvall/kubescrape/internal/services"
)

// TestMonitorEmptyEndpointNoTarget: a ServiceMonitor endpoint that
// declares NEITHER a port NAME nor a targetPort resolves against a service port
// whose Name is "" (the common single-port-service case), silently producing a
// scrape target. prometheus-operator cannot reference an unnamed port by name,
// so it would generate no scrape config here; kubescrape diverges by matching
// ""=="" in monitorPodPort (targets.go:164-165). A malformed/empty endpoint
// should yield nothing.
func TestMonitorEmptyEndpointNoTarget(t *testing.T) {
	pod := basePod()
	svc := &services.Service{
		Name: "svc", Namespace: "default", UID: "u",
		Ports: []services.Port{{Name: "", Port: 80, TargetPortNum: 9090}}, // unnamed service port
	}
	ts := MonitorTargets(pod, svc, "mon/m", servicemonitors.Endpoint{}) // empty endpoint
	if len(ts) != 0 {
		t.Fatalf("empty ServiceMonitor endpoint produced a phantom target %q via the unnamed service port; want none", ts[0].URL)
	}
}
