package servicemonitors

import (
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// brokenMonitor is an object Parse refuses: selector.matchLabels must be a
// map, and this one is a string. The kind decides which index door it goes
// through; the shape is equally broken for both.
func brokenMonitorOf(kind, ns, name, rv string) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "monitoring.coreos.com/v1",
		"kind":       kind,
		"metadata": map[string]any{
			"namespace":       ns,
			"name":            name,
			"resourceVersion": rv,
		},
		"spec": map[string]any{
			"selector": map[string]any{"matchLabels": "not-a-map"},
		},
	}}
}

func validMonitorOf(kind, ns, name, rv string) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "monitoring.coreos.com/v1",
		"kind":       kind,
		"metadata": map[string]any{
			"namespace":       ns,
			"name":            name,
			"resourceVersion": rv,
		},
		"spec": map[string]any{
			"selector":  map[string]any{"matchLabels": map[string]any{"app": "x"}},
			"endpoints": []any{map[string]any{"port": "metrics"}},
		},
	}}
}

// Rejected is the STATE the gauge publishes, so every transition an operator
// would see has to move it: broken counts, a resync of the same broken object
// does not double-count, a fix clears, a delete clears, and the two kinds are
// independent. The event counter cannot carry any of this — it only goes up.
func TestRejectedTracksCurrentlyBrokenMonitors(t *testing.T) {
	ix := NewIndex()
	assertRejected := func(step string, wantSM, wantPM int) {
		t.Helper()
		sm, pm := ix.Rejected()
		if sm != wantSM || pm != wantPM {
			t.Fatalf("%s: Rejected() = (%d, %d), want (%d, %d)", step, sm, pm, wantSM, wantPM)
		}
	}

	assertRejected("empty index", 0, 0)

	if _, err := ix.UpsertChanged(brokenMonitorOf("ServiceMonitor", "team-a", "web", "1")); err == nil {
		t.Fatal("the broken ServiceMonitor parsed; the fixture no longer exercises the rejected path")
	}
	assertRejected("one broken ServiceMonitor", 1, 0)

	// The informer resync re-delivers the SAME broken object: state unchanged.
	if _, err := ix.UpsertChanged(brokenMonitorOf("ServiceMonitor", "team-a", "web", "1")); err == nil {
		t.Fatal("re-delivery parsed")
	}
	assertRejected("resync of the same broken object", 1, 0)

	if _, err := ix.UpsertPodMonitorChanged(brokenMonitorOf("PodMonitor", "team-b", "api", "7")); err == nil {
		t.Fatal("the broken PodMonitor parsed")
	}
	assertRejected("kinds are independent", 1, 1)

	// The operator fixes the ServiceMonitor: the state clears, and only the
	// fixed kind's.
	if _, err := ix.UpsertChanged(validMonitorOf("ServiceMonitor", "team-a", "web", "2")); err != nil {
		t.Fatalf("the fixed ServiceMonitor did not parse: %v", err)
	}
	assertRejected("a parse success clears the state", 0, 1)

	// The operator deletes the broken PodMonitor instead of fixing it: a
	// configuration that no longer exists is not "still broken".
	ix.DeletePodMonitor("team-b", "api")
	assertRejected("a delete clears the state", 0, 0)
}
