package servicemonitors

import (
	"fmt"
	"testing"

	"github.com/JohanLindvall/kubescrape/internal/testrace"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// AuthSecretRefs is the allowlist check on GET /v1/scrape-auth — the ONE route
// that serves Secret material, guarded by cluster-wide `secrets: get` — and
// every agent re-asks each credential once a minute. It used to harvest the
// whole cluster's monitor endpoints on every request, under the index's read
// lock, which the monitor informer's writes contend with: 27,112 B and 15
// allocations per request at the 200+200 monitors BenchmarkAuthSecretRefs
// builds (0.68 MB/s of garbage at 500 nodes × 3 credentials, i.e. 25 requests
// a second), and all of it identical between requests.
//
// The index already publishes the change token that says when it is NOT
// identical, and the server's monitor→Service cross product is memoised on it
// one layer up. This is the same memo on the same token.
func TestAuthSecretRefsIsFreeWhileTheMonitorsAreUnchanged(t *testing.T) {
	if testrace.Enabled {
		// The detector's bookkeeping allocations would fail a budget nothing
		// regressed.
		t.Skip("allocation budgets are meaningless under -race")
	}
	ix := NewIndex()
	for i := range 20 {
		if err := ix.Upsert(monitorWithSecret("sm", i)); err != nil {
			t.Fatal(err)
		}
		if err := ix.UpsertPodMonitor(monitorWithSecret("pm", i)); err != nil {
			t.Fatal(err)
		}
	}
	if n := ix.AuthSecretRefs().Len(); n != 40 {
		t.Fatalf("allowlist has %d refs, want 40", n)
	}

	builds := ix.authBuilds.Load()
	if got := testing.AllocsPerRun(100, func() {
		if !ix.AuthSecretRefs().Has("sm-3/tok/token") {
			t.Fatal("allowlist lost an entry")
		}
	}); got != 0 {
		t.Errorf("a scrape-auth allowlist check allocates %.0f times while the monitors have not changed: "+
			"the whole cluster's endpoints are being re-harvested per request, on the route that holds "+
			"cluster-wide secrets: get", got)
	}
	if got := ix.authBuilds.Load() - builds; got != 0 {
		t.Errorf("the allowlist was rebuilt %d times with nothing changed", got)
	}
}

// …and the memo must never outlive what it describes: this map is what stands
// between /v1/scrape-auth and a general secret-read API, so a monitor that
// stops referencing a Secret has to stop allowlisting it on the very next
// request. Every mutation the index can make is exercised, because the token is
// what the memo trusts and a mutation that failed to move it would leave a
// removed monitor's credential readable.
func TestAuthSecretRefsFollowsEveryMutation(t *testing.T) {
	ix := NewIndex()
	has := func(what, ref string, want bool) {
		t.Helper()
		if ok := ix.AuthSecretRefs().Has(ref); ok != want {
			t.Fatalf("after %s: allowlisted(%q) = %v, want %v (refs %v)", what, ref, ok, want, ix.AuthSecretRefs().refs)
		}
	}

	has("an empty index", "sm-0/tok/token", false)
	if err := ix.Upsert(monitorWithSecret("sm", 0)); err != nil {
		t.Fatal(err)
	}
	has("an added ServiceMonitor", "sm-0/tok/token", true)

	if err := ix.UpsertPodMonitor(monitorWithSecret("pm", 0)); err != nil {
		t.Fatal(err)
	}
	has("an added PodMonitor", "pm-0/tok/token", true)

	// An UPDATE that renames the referenced key: the old ref must go.
	updated := monitorWithSecret("sm", 0)
	updated.SetResourceVersion("2")
	eps, _, _ := unstructured.NestedSlice(updated.Object, "spec", "endpoints")
	ep := eps[0].(map[string]any)
	ep["bearerTokenSecret"] = map[string]any{"name": "tok", "key": "rotated"}
	if err := unstructured.SetNestedSlice(updated.Object, eps, "spec", "endpoints"); err != nil {
		t.Fatal(err)
	}
	if err := ix.Upsert(updated); err != nil {
		t.Fatal(err)
	}
	has("an updated ServiceMonitor", "sm-0/tok/token", false)
	has("an updated ServiceMonitor", "sm-0/tok/rotated", true)

	ix.DeletePodMonitor("pm-0", "mon")
	has("a deleted PodMonitor", "pm-0/tok/token", false)

	// An unparseable UPDATE removes the monitor (the invalid-update-removes
	// policy), and the allowlist has to shrink with it.
	broken := monitorWithSecret("sm", 0)
	broken.SetResourceVersion("3")
	if err := unstructured.SetNestedField(broken.Object, "not-a-selector", "spec", "selector"); err != nil {
		t.Fatal(err)
	}
	if err := ix.Upsert(broken); err == nil {
		t.Fatal("an unparseable update was accepted")
	}
	has("an unparseable update", "sm-0/tok/rotated", false)
}

// monitorWithSecret builds a monitor in namespace "<kind>-<i>" whose single
// endpoint references Secret "tok" key "token". kind "pm" spells the endpoint
// list the PodMonitor way (podMetricsEndpoints).
func monitorWithSecret(kind string, i int) *unstructured.Unstructured {
	field := "endpoints"
	if kind == "pm" {
		field = "podMetricsEndpoints"
	}
	return &unstructured.Unstructured{Object: map[string]any{
		"metadata": map[string]any{
			"name":            "mon",
			"namespace":       fmt.Sprintf("%s-%d", kind, i),
			"resourceVersion": "1",
		},
		"spec": map[string]any{
			"selector": map[string]any{"matchLabels": map[string]any{"app": "x"}},
			field: []any{map[string]any{
				"port":              "metrics",
				"bearerTokenSecret": map[string]any{"name": "tok", "key": "token"},
			}},
		},
	}}
}

// BenchmarkAuthSecretRefs reports what the memo above is worth, at a cluster
// size where the harvest is not free: 200 ServiceMonitors + 200 PodMonitors,
// one secret-bearing endpoint each. The `Build` arm is what every
// /v1/scrape-auth request used to pay.
func BenchmarkAuthSecretRefs(b *testing.B) {
	b.Run("Build", func(b *testing.B) {
		ix := benchIndex(b)
		b.ReportAllocs()
		for b.Loop() {
			_ = ix.buildAuthSecretRefs()
		}
	})
	b.Run("Memoised", func(b *testing.B) {
		ix := benchIndex(b)
		_ = ix.AuthSecretRefs() // the one build the loop then reuses
		b.ReportAllocs()
		for b.Loop() {
			_ = ix.AuthSecretRefs()
		}
	})
}

func benchIndex(b *testing.B) *Index {
	b.Helper()
	ix := NewIndex()
	for i := range 200 {
		if err := ix.Upsert(monitorWithSecret("sm", i)); err != nil {
			b.Fatal(err)
		}
		if err := ix.UpsertPodMonitor(monitorWithSecret("pm", i)); err != nil {
			b.Fatal(err)
		}
	}
	if n := ix.AuthSecretRefs().Len(); n != 400 {
		b.Fatalf("the fixture holds %d refs, want 400", n)
	}
	return ix
}
