package kubemeta

import (
	"strings"
	"testing"

	"github.com/JohanLindvall/kubescrape/internal/testrace"
)

var sinkAnnotations map[string]string

// ordinaryAnnotations is what a real pod carries once the deploy-tool copies
// are dropped: a handful of small values, nowhere near either ceiling.
func ordinaryAnnotations() map[string]string {
	return map[string]string{
		"prometheus.io/scrape":                             "true",
		"prometheus.io/port":                               "9090",
		"app.kubernetes.io/managed-by":                     "helm",
		"kubectl.kubernetes.io/last-applied-configuration": `{"spec":{"containers":[{"name":"app"}]}}`,
		"kubernetes.io/config.seen":                        "2026-08-30T00:00:00Z",
	}
}

func BenchmarkFilterAnnotationsOrdinary(b *testing.B) {
	in := ordinaryAnnotations()
	for b.Loop() {
		sinkAnnotations = FilterAnnotations(in)
	}
}

// A 200 KiB value must cost a COMPARISON, not a copy: this filter runs once per
// informer event for every pod, owner, namespace and Service in the cluster,
// and the whole point of the ceilings is that the blob never gets carried.
func BenchmarkFilterAnnotationsOversizedValue(b *testing.B) {
	in := ordinaryAnnotations()
	in["team.example.com/inventory"] = strings.Repeat("x", 200<<10)
	for b.Loop() {
		sinkAnnotations = FilterAnnotations(in)
	}
}

// The budget must not cost the ordinary object anything. Two allocations is the
// output map, exactly what the unbounded filter allocated: the ceilings are
// checked from LENGTHS inside the one copy loop, so there is no pre-pass and no
// key slice, and the sort only exists on the path no real object takes.
func TestFilterAnnotationsAllocationBudget(t *testing.T) {
	if testrace.Enabled {
		t.Skip("the race detector adds bookkeeping allocations")
	}
	in := ordinaryAnnotations()
	if n := testing.AllocsPerRun(200, func() { sinkAnnotations = FilterAnnotations(in) }); n > 2 {
		t.Errorf("FilterAnnotations allocates %v per ordinary object, budget 2 (the output map)", n)
	}
	big := ordinaryAnnotations()
	big["team.example.com/inventory"] = strings.Repeat("x", 200<<10)
	// The refusal path rebuilds, so it allocates more — but never in
	// proportion to the value it refused.
	if n := testing.AllocsPerRun(200, func() { sinkAnnotations = FilterAnnotations(big) }); n > 12 {
		t.Errorf("refusing one oversized value allocates %v; it must not scale with the blob", n)
	}
}
