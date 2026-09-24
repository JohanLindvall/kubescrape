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
// Three ceilings, all at zero, all measured (bench_test.go reports the wall
// clock beside them):
//
//	a pre-bound Inc                     0 allocs
//	a SINGLE-label WithLabelValues+Inc  0 allocs  (the value IS the cache key)
//	a MULTI-label WithLabelValues+Inc   0 allocs  (on a cache hit)
//
// The multi-label arm used to cost one 16 B allocation per call — the
// length-prefixed tuple key was built as a string before the cache lookup — and
// its test pinned that as a known price with a note to lower the number if it
// ever became free. It did: the key is now built into a stack buffer and
// probed with m[string(key)], which does not allocate, so only a MISS (a tuple
// seen for the first time) materialises it. Pre-binding stays the discipline
// on per-record paths — a lookup under the vec's mutex is still work — but it
// is no longer the only way to stay off the heap.

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

// Per-event call sites resolve two-label tuples per call — obs.HTTPRequests
// (pattern, code) per metadata request, obs.Exports (signal, class) per wire
// send — so a cache HIT must not allocate the tuple key.
func TestMultiLabelResolutionIsAllocationFree(t *testing.T) {
	if testrace.Enabled {
		t.Skip("the race detector adds bookkeeping allocations; the ceiling is meaningless under it")
	}
	r := metrics.NewRegistry()
	v := r.CounterVec("kubescrape_budget_wlv2_total", "budget", "outcome", "pipeline")
	v.WithLabelValues("ok", "logs") // warm the wrapper cache; a miss legitimately allocates
	if got := testing.AllocsPerRun(2000, func() { v.WithLabelValues("ok", "logs").Inc() }); got != 0 {
		t.Errorf("a two-label WithLabelValues bump allocated %v per call; want 0 (the key is built on the stack)", got)
	}
}
