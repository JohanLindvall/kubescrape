package servicemonitors

// The index's change token, for the same reason as internal/services': the
// metadata service holds its monitor→Service cross product until one of the two
// tokens moves, so anything that moves a token without changing the index makes
// the memo worthless. An informer resync re-delivers every monitor
// byte-identical.

import (
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func genMonitorObj(kind, name, rv, port string) *unstructured.Unstructured {
	u := &unstructured.Unstructured{Object: map[string]any{
		"metadata": map[string]any{
			"namespace": "default", "name": name, "resourceVersion": rv,
		},
		"spec": map[string]any{
			"selector": map[string]any{"matchLabels": map[string]any{"app": "web"}},
		},
	}}
	spec := u.Object["spec"].(map[string]any)
	endpoints := []any{map[string]any{"port": port}}
	if kind == "podmonitor" {
		spec["podMetricsEndpoints"] = endpoints
	} else {
		spec["endpoints"] = endpoints
	}
	return u
}

func TestGenerationIgnoresMonitorReDeliveries(t *testing.T) {
	for _, kind := range []string{"servicemonitor", "podmonitor"} {
		t.Run(kind, func(t *testing.T) {
			ix := NewIndex()
			upsert := func(u *unstructured.Unstructured) error { return ix.Upsert(u) }
			del := func() { ix.Delete("default", "m") }
			if kind == "podmonitor" {
				upsert = func(u *unstructured.Unstructured) error { return ix.UpsertPodMonitor(u) }
				del = func() { ix.DeletePodMonitor("default", "m") }
			}

			if err := upsert(genMonitorObj(kind, "m", "7", "http")); err != nil {
				t.Fatal(err)
			}
			after := ix.Generation()
			// The resync.
			if err := upsert(genMonitorObj(kind, "m", "7", "http")); err != nil {
				t.Fatal(err)
			}
			if got := ix.Generation(); got != after {
				t.Fatalf("the change token moved (%d -> %d) for a re-delivery of the indexed monitor",
					after, got)
			}

			// A real edit moves it, and is applied.
			if err := upsert(genMonitorObj(kind, "m", "8", "metrics")); err != nil {
				t.Fatal(err)
			}
			if got := ix.Generation(); got == after {
				t.Fatal("the change token did not move for a genuine monitor update")
			}
			var port string
			if kind == "podmonitor" {
				port = ix.PodMonitors()[0].Endpoints[0].Port
			} else {
				port = ix.All()[0].Endpoints[0].Port
			}
			if port != "metrics" {
				t.Fatalf("endpoint port = %q, want the updated metrics", port)
			}

			// A delete moves it once; a repeat of the delete changes nothing.
			before := ix.Generation()
			del()
			if ix.Generation() == before {
				t.Fatal("a delete of an indexed monitor must move the token")
			}
			after = ix.Generation()
			del()
			if got := ix.Generation(); got != after {
				t.Fatalf("the change token moved (%d -> %d) for a delete of a key the index does "+
					"not hold", after, got)
			}
		})
	}
}

// An UNPARSEABLE monitor is removed rather than kept (upsertMonitor's policy).
// The removal itself is a change; a resync re-delivering the same broken object
// afterwards is not — it deletes a key that is already gone.
func TestGenerationIgnoresRepeatsOfARejectedMonitor(t *testing.T) {
	ix := NewIndex()
	broken := &unstructured.Unstructured{Object: map[string]any{
		"metadata": map[string]any{"namespace": "default", "name": "m", "resourceVersion": "7"},
	}}
	if err := ix.Upsert(broken); err == nil {
		t.Fatal("a monitor with no spec must be rejected")
	}
	after := ix.Generation()
	if err := ix.Upsert(broken); err == nil {
		t.Fatal("a monitor with no spec must be rejected")
	}
	if got := ix.Generation(); got != after {
		t.Fatalf("the change token moved (%d -> %d) for a repeat of a monitor that was already "+
			"rejected and is not in the index", after, got)
	}
}
