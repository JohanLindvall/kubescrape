package obs_test

// The self-metrics allocation budgets.
//
// Every per-record call site in this repo bumps a PRE-BOUND handle
// (logenrich's four formats, cgroupstats' dozen, tailbuffer's per-policy pair)
// and the comment at each one says the pre-binding is what keeps it off the
// per-line budget. Nothing enforced that: obs had no benchmark and no budget,
// so a bump that started allocating — a wider label tuple, a key format change
// — would have shown up as a tailer per-line regression attributed to the
// tailer.
//
// Two ceilings, both at zero, both measured (bench_test.go reports the wall
// clock beside them):
//
//	a pre-bound Inc                     0 allocs
//	a SINGLE-label WithLabelValues+Inc  0 allocs  (vecKey's alloc-free arm)
//
// The MULTI-label WithLabelValues arm deliberately has no zero budget: vecKey
// builds a length-prefixed tuple, which is one 16 B allocation per call. That
// is the price of the per-call form and the reason every hot site pre-binds
// instead; TestMultiLabelResolutionAllocatesOncePerCall pins it as a KNOWN
// cost so nobody puts one on a per-record path believing it is free.

import (
	"testing"

	"github.com/JohanLindvall/kubescrape/internal/metrics"
	"github.com/JohanLindvall/kubescrape/internal/testrace"
)

func TestCounterIncIsAllocationFree(t *testing.T) {
	if testrace.Enabled {
		t.Skip("the race detector adds bookkeeping allocations; the ceiling is meaningless under it")
	}
	r := metrics.NewRegistry()
	c := r.Counter("kubescrape_budget_inc_total", "budget")
	g := r.Gauge("kubescrape_budget_gauge", "budget")
	if got := testing.AllocsPerRun(2000, func() { c.Inc() }); got != 0 {
		t.Errorf("a pre-bound counter Inc allocated %v per call; want 0", got)
	}
	if got := testing.AllocsPerRun(2000, func() { g.Set(1) }); got != 0 {
		t.Errorf("a pre-bound gauge Set allocated %v per call; want 0", got)
	}
}

func TestSingleLabelResolutionIsAllocationFree(t *testing.T) {
	if testrace.Enabled {
		t.Skip("the race detector adds bookkeeping allocations; the ceiling is meaningless under it")
	}
	r := metrics.NewRegistry()
	v := r.CounterVec("kubescrape_budget_wlv_total", "budget", "outcome")
	v.WithLabelValues("ok") // warm the wrapper cache; a miss legitimately allocates
	if got := testing.AllocsPerRun(2000, func() { v.WithLabelValues("ok").Inc() }); got != 0 {
		t.Errorf("a single-label WithLabelValues bump allocated %v per call; want 0", got)
	}
}

// Not a budget so much as a documented price: this is why the hot sites bind
// up front. If it ever becomes free the number here should be lowered, not the
// discipline relaxed.
func TestMultiLabelResolutionAllocatesOncePerCall(t *testing.T) {
	if testrace.Enabled {
		t.Skip("the race detector adds bookkeeping allocations; the ceiling is meaningless under it")
	}
	r := metrics.NewRegistry()
	v := r.CounterVec("kubescrape_budget_wlv2_total", "budget", "outcome", "pipeline")
	v.WithLabelValues("ok", "logs")
	got := testing.AllocsPerRun(2000, func() { v.WithLabelValues("ok", "logs").Inc() })
	if got > 1 {
		t.Errorf("a two-label WithLabelValues bump allocated %v per call; want at most 1 (the tuple key)", got)
	}
}
