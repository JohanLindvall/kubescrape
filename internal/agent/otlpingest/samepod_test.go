package otlpingest

// The "same Kubernetes object" predicate and the payload that told its two
// former copies apart: container A's ID on the resource, container B's ID on
// the data points, both containers in ONE pod.

import (
	"context"
	"testing"

	"github.com/JohanLindvall/kubescrape/pkg/kubemeta"
)

// twoContainerMeta resolves two container ids into the SAME pod: cid-a is
// container app-a and cid-b is container app-b, both of web-1/pod-uid-1.
func twoContainerMeta() *fakeMeta {
	pod := kubemeta.Pod{Name: "web-1", Namespace: "default", UID: "pod-uid-1", NodeName: "node1"}
	return &fakeMeta{
		containers: map[string]*kubemeta.ContainerMetadata{
			"cid-a": {Container: kubemeta.Container{Name: "app-a", ID: "containerd://cid-a"}, Pod: pod},
			"cid-b": {Container: kubemeta.Container{Name: "app-b", ID: "containerd://cid-b"}, Pod: pod},
		},
	}
}

// samePodDifferentContainer is the payload the two predicate copies disagreed
// on: the resource names container A (plus the sender's own service.name), the
// data point names container B — a sibling container of the same pod, i.e. a
// pod-internal exporter describing its co-container.
func samePodDifferentContainer() map[string]string {
	return map[string]string{"container.id": "cid-a", "service.name": "sender-chosen"}
}

// The same-object predicate used to exist twice and DISAGREE: the auto-mode
// decision (foreignID) matched on pod UID alone, while the split path's
// sameObject requires pod UID AND k8s.container.name. For container A's ID on
// the resource and container B's ID on the data points (same pod), auto mode
// said "not foreign", took the resource branch, and stamped container A's
// identity (k8s.container.name=app-a) on container B's points — while explicit
// datapoint mode treated them as different objects and attributed the points
// to app-b. The identical payload, attributed differently by mode: the exact
// bug class resolvableToken's comment records as fixed for the token-choice
// half.
//
// Pinned here: BOTH modes now attribute the point to container B. Auto mode
// demotes this payload to the split path — two distinct containers of one pod
// are two objects, so a resource-level enrichment cannot be right for the
// point's container.
func TestAutoModeMatchesDatapointModeForSamePodDifferentContainer(t *testing.T) {
	for _, mode := range []MetricsMode{MetricsAuto, MetricsDatapoint} {
		t.Run(string(mode), func(t *testing.T) {
			e := newEnricher(twoContainerMeta(), mode)
			md := gaugeWith(samePodDifferentContainer(), map[string]string{"container.id": "cid-b"})
			out := e.EnrichMetrics(context.Background(), md)

			if n := out.ResourceMetrics().Len(); n != 1 {
				t.Fatalf("resources = %d, want 1 (all points name container B)", n)
			}
			a := out.ResourceMetrics().At(0).Resource().Attributes()
			if v, _ := a.Get("k8s.container.name"); v.Str() != "app-b" {
				t.Errorf("k8s.container.name = %q, want app-b: container B's points carry container %q's identity",
					v.Str(), "app-a")
			}
		})
	}
}
