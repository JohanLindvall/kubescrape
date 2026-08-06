package scrape

import (
	"testing"

	"github.com/JohanLindvall/kubescrape/internal/servicemonitors"
	"github.com/JohanLindvall/kubescrape/internal/services"
)

// TestMonitorEmptyEndpointNoTarget: a ServiceMonitor endpoint that declares
// NEITHER a port NAME nor a targetPort would otherwise resolve against a
// service port whose Name is "" (the common single-port-service case) by
// matching ""=="" in monitorPodPort, silently producing a scrape target the
// user never declared.
//
// prometheus-operator does something else again — and it is NOT "emits no
// config", which this comment claimed for a while: its generateEndpointConfig
// emits the port-name keep-relabeling only inside `if ep.Port != "" { } else if
// ep.TargetPort != nil { }`, so with neither set it emits no port FILTER and
// every port of every matching EndpointSlice becomes a target. kubescrape's
// answer is narrower and deliberately kept — fabricating targets from a
// malformed endpoint is the worse failure — but being narrower is a choice that
// has to be VISIBLE, which is what the endpoint's "port(unset)" Ignored entry
// (servicemonitors.noPortIgnored) is for.
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
