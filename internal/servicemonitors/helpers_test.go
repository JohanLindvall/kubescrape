package servicemonitors

// Fixture helpers shared by this package's tests. The builders in the other
// test files take whatever shape their test needs (a whole spec map, a secret
// reference, a broken selector) and wrap crObject for the envelope; the one
// thing none of them may spell for itself is the ENDPOINT-LIST key, which the
// two kinds name differently. Three builders used to re-derive it, from three
// different spellings of the kind, and one ignored its kind argument entirely
// — a PodMonitor fixture built that way parses with zero endpoints and passes
// whatever test assumed it had one.

import (
	"fmt"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// endpointsKey is the spec key the CRD kind ("ServiceMonitor" or "PodMonitor")
// keeps its endpoint list under, read from the parser's own spec types so a
// fixture cannot drift from what Parse and ParsePodMonitor decode. Any other
// kind is a fixture bug, and panics rather than guessing.
func endpointsKey(kind string) string {
	switch kind {
	case "ServiceMonitor":
		return (&smSpec{}).endpointsField()
	case "PodMonitor":
		return (&pmSpec{}).endpointsField()
	}
	panic(fmt.Sprintf("endpointsKey: %q is not a monitor kind (want ServiceMonitor or PodMonitor)", kind))
}

// crObject builds a monitor CR's unstructured envelope around spec. An empty
// rv leaves resourceVersion unset, as a hand-built object without one has it.
func crObject(kind, ns, name, rv string, spec map[string]any) *unstructured.Unstructured {
	meta := map[string]any{"namespace": ns, "name": name}
	if rv != "" {
		meta["resourceVersion"] = rv
	}
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "monitoring.coreos.com/v1",
		"kind":       kind,
		"metadata":   meta,
		"spec":       spec,
	}}
}
