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

// The memo must be a memo: the same verdict as the regexes on a cache HIT, on a
// hit PROMOTED out of the previous generation, and after both generations have
// turned over and the key is computed again.
//
// Every assertion below has to land on a stated cache state, and the second and
// third are the ones this test was named for and did not make. It used to
// assert only first-sight verdicts — a miss on the way in, and, after churning
// past 3x the cap, a miss on the way back — so the memoized branch of Keep was
// never read at an assertion point at all. Inverting that branch
// (`return !keep`) left this test, and the whole package, green.
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
		// First call: a miss, which computes and admits.
		if got := f.Keep(k); got != want[k] {
			t.Fatalf("first Keep(%q) = %v, want %v", k, got, want[k])
		}
		// Second call, nothing in between: a HIT in the current generation.
		// This is the branch the memo exists for, and the one every resource
		// after the first takes.
		if got := f.Keep(k); got != want[k] {
			t.Fatalf("memoized Keep(%q) = %v, want %v", k, got, want[k])
		}
	}
	// Churn just past ONE rotation, so the keys sit in the PREVIOUS generation:
	// a hit there is promoted back into the current one, which is what keeps a
	// hot key cached across a rotation.
	for i := range maxFilterKeys {
		f.Keep(fmt.Sprintf("k8s.pod.label.churn-%d", i))
	}
	for _, k := range keys {
		if got := f.Keep(k); got != want[k] {
			t.Fatalf("promoted Keep(%q) = %v, want %v", k, got, want[k])
		}
		// The promotion put it back in the current generation: hit again.
		if got := f.Keep(k); got != want[k] {
			t.Fatalf("after promotion Keep(%q) = %v, want %v", k, got, want[k])
		}
	}
	// Push the memo past its cap so both generations turn over and the verdict
	// is computed from the regexes again.
	for i := range 3 * maxFilterKeys {
		f.Keep(fmt.Sprintf("k8s.pod.label.churn2-%d", i))
	}
	for _, k := range keys {
		if got := f.Keep(k); got != want[k] {
			t.Fatalf("after rotation Keep(%q) = %v, want %v", k, got, want[k])
		}
	}
}
