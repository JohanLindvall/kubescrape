package scrape

import (
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/util/intstr"

	"github.com/JohanLindvall/kubescrape/internal/servicemonitors"
)

// On a pod that is not scrapeable, monitorEndpoint and podMonitorEndpoint
// return before they look at the port — so reaching the endpoint note there
// says nothing about whether the endpoint would resolve. The note used to
// answer "this endpoint resolves, but the pod itself is excluded" for EVERY
// endpoint on such a pod, a broken one included: an endpoint naming a port that
// does not exist was described as working, while the same document said a
// pod-annotation entry with the same mistake resolves to nothing. The pod-level
// reason is only the answer for an endpoint that WOULD resolve; a broken one
// keeps its port wording on any pod.
func TestMonitorEndpointNoteOnAnExcludedPodStillBlamesABrokenPort(t *testing.T) {
	excluded := basePod()
	excluded.PodIP = "" // Pending: not scrapeable, and not because of any port
	if Scrapeable(excluded) {
		t.Fatal("setup: the pod must not be scrapeable")
	}
	svc := monitorService()
	tp := intstr.FromInt32(9091)
	for _, tc := range []struct {
		name     string
		note     func() string
		resolves bool
	}{
		{"ServiceMonitor port resolves", func() string {
			return MonitorEndpointNote(excluded, svc, servicemonitors.Endpoint{Port: "metrics"})
		}, true},
		{"ServiceMonitor targetPort resolves", func() string {
			return MonitorEndpointNote(excluded, svc, servicemonitors.Endpoint{TargetPort: &tp})
		}, true},
		{"ServiceMonitor port names no Service port", func() string {
			return MonitorEndpointNote(excluded, svc, servicemonitors.Endpoint{Port: "nosuchport"})
		}, false},
		{"ServiceMonitor endpoint names no port at all", func() string {
			return MonitorEndpointNote(excluded, svc, servicemonitors.Endpoint{})
		}, false},
		{"PodMonitor port resolves", func() string {
			return PodMonitorEndpointNote(excluded, servicemonitors.Endpoint{Port: "metrics"})
		}, true},
		{"PodMonitor port names no container port", func() string {
			return PodMonitorEndpointNote(excluded, servicemonitors.Endpoint{Port: "nosuchcontainerport"})
		}, false},
		{"PodMonitor endpoint names no port at all", func() string {
			return PodMonitorEndpointNote(excluded, servicemonitors.Endpoint{})
		}, false},
	} {
		note := tc.note()
		claimsResolves := strings.Contains(note, "resolves, but the pod itself is excluded")
		blamesPort := strings.Contains(note, "resolves to no pod port")
		switch {
		case tc.resolves && (!claimsResolves || blamesPort):
			t.Errorf("%s: a resolving endpoint on an excluded pod must be blamed on the pod: %q", tc.name, note)
		case !tc.resolves && claimsResolves:
			t.Errorf("%s: an endpoint that resolves to nothing is described as resolving: %q", tc.name, note)
		case !tc.resolves && (!blamesPort || !strings.Contains(note, "notScrapeableWhy")):
			t.Errorf("%s: a broken endpoint on an excluded pod must name the port AND point at the pod's own reason: %q",
				tc.name, note)
		}
	}

	// On a scrapeable pod nothing changes: a broken endpoint is the port.
	if note := MonitorEndpointNote(basePod(), svc, servicemonitors.Endpoint{Port: "nosuchport"}); !strings.Contains(note, "resolves to no pod port") ||
		strings.Contains(note, "excluded") {
		t.Errorf("scrapeable pod, broken ServiceMonitor endpoint: %q", note)
	}
	if note := PodMonitorEndpointNote(basePod(), servicemonitors.Endpoint{Port: "nosuchcontainerport"}); !strings.Contains(note, "resolves to no pod port") ||
		strings.Contains(note, "excluded") {
		t.Errorf("scrapeable pod, broken PodMonitor endpoint: %q", note)
	}
}
