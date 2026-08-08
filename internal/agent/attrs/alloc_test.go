package attrs

import (
	"fmt"
	"testing"

	"github.com/JohanLindvall/kubescrape/internal/testrace"
	"go.opentelemetry.io/collector/pdata/pcommon"
)

// Build runs once per RESOURCE, and a KSM split path builds one per described
// object — 12k of them per scrape cycle on a realistic cluster. The budget is
// what keeps that from quietly regressing: the attribute map is pre-sized from
// the Context (five reallocations otherwise) and the prefixed label keys are
// memoized (one concatenation per label per resource otherwise). A benchmark
// cannot fail a build, so the ceiling lives here.
func TestBuildAllocationBudget(t *testing.T) {
	if testrace.Enabled {
		t.Skip("the race detector changes escape analysis and adds allocations")
	}
	ctx := benchContext()
	var b *Builder
	// 22 attributes from a pod with 2 owners, 7 labels and a container: one map
	// allocation plus the pdata values. It was 37 while the map grew by doubling
	// and every label key was concatenated per call.
	const budget = 26
	if got := testing.AllocsPerRun(200, func() {
		b.Build(pcommon.NewResource(), ctx)
	}); got > budget {
		t.Fatalf("Build allocated %.0f objects, budget %d", got, budget)
	}
}

// The filter runs on every attribute of every resource built. Its verdict is a
// pure function of the key and the key set is closed and tiny, so the anchored
// regexes must not be re-run per attribute per resource — and the whole-resource
// call must not allocate at all.
func TestFilterApplyIsAllocationFree(t *testing.T) {
	if testrace.Enabled {
		t.Skip("the race detector changes escape analysis and adds allocations")
	}
	f, err := NewFilterFromLists(nil, []string{`k8s\.pod\.label\.internal\..*`, `k8s\.namespace\.label\.internal\..*`})
	if err != nil {
		t.Fatal(err)
	}
	res := pcommon.NewResource()
	for _, k := range benchKeys() {
		res.Attributes().PutStr(k, "v")
	}
	if got := testing.AllocsPerRun(200, func() { f.Apply(res) }); got != 0 {
		t.Fatalf("Filter.Apply allocated %.0f objects per call", got)
	}
}

// The memo must be a memo: the same verdict as the regexes, for a key first
// seen before the cache rotated and again after.
func TestFilterMemoAgreesWithTheRegexesAcrossRotations(t *testing.T) {
	f, err := NewFilterFromLists([]string{`k8s\..*`}, []string{`k8s\.pod\.label\..*`})
	if err != nil {
		t.Fatal(err)
	}
	keys := []string{
		"k8s.pod.name", "k8s.pod.label.team", "service.name", "k8s.namespace.label.tier",
	}
	want := map[string]bool{}
	for _, k := range keys {
		want[k] = f.match(k)
		if got := f.Keep(k); got != want[k] {
			t.Fatalf("Keep(%q) = %v, want %v", k, got, want[k])
		}
	}
	// Push the memo past its cap so both generations turn over.
	for i := range 3 * maxFilterKeys {
		f.Keep(fmt.Sprintf("k8s.pod.label.churn-%d", i))
	}
	for _, k := range keys {
		if got := f.Keep(k); got != want[k] {
			t.Fatalf("after rotation Keep(%q) = %v, want %v", k, got, want[k])
		}
	}
}
