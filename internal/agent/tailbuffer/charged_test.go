package tailbuffer

import (
	"context"
	"testing"
	"time"

	"github.com/JohanLindvall/kubescrape/internal/agent/tailsample"
	"github.com/JohanLindvall/kubescrape/internal/obs"
)

// rateCfg is a policy list whose only rule spends a spans/second budget — the
// one policy class where deciding a trace twice has a cost.
func rateCfg(perSecond float64) tailsample.Config {
	return tailsample.Config{Policies: []tailsample.PolicyConfig{{
		Name: "rate", Type: tailsample.TypeRateLimiting,
		RateLimiting: &tailsample.RateLimitingConfig{SpansPerSecond: perSecond},
	}}}
}

func manySpans(trace uint64, n int) []spanSpec {
	out := make([]spanSpec, n)
	for i := range out {
		out[i] = spanSpec{trace: trace, span: uint64(i) + 1, end: 10}
	}
	return out
}

// A straggler arriving past the decision cache's TTL starts a fresh window and
// its trace is decided a SECOND time. That re-decision must not spend the rate
// budget again — the spans it re-judges were charged the first time — or a
// trace's own late spans quietly shrink the budget available to genuinely new
// traces.
//
// The assertion is the new trace at the end: with the double charge it is
// dropped for budget the straggler ate.
func TestReDecisionDoesNotRechargeTheRateBudget(t *testing.T) {
	c := &capture{}
	b, clk := newTestBuffer(t, Config{
		Config: rateCfg(20), DecisionWait: "5s", DecisionCacheTTL: "10s",
	}, c)
	ctx := context.Background()

	// Trace 1: ten spans, decided normally. Budget left: 10.
	if err := b.ExportTraces(ctx, payload("checkout", manySpans(1, 10)...)); err != nil {
		t.Fatal(err)
	}
	clk.advance(6 * time.Second)
	b.Sweep(ctx)

	// Past the cache TTL, a straggler for trace 1 opens a fresh window.
	clk.advance(11 * time.Second)
	if err := b.ExportTraces(ctx, payload("checkout", spanSpec{trace: 1, span: 99, end: 10})); err != nil {
		t.Fatal(err)
	}
	clk.advance(6 * time.Second)
	b.Sweep(ctx) // re-decides trace 1 — must CHECK the budget, not spend it

	// A brand-new ten-span trace still fits the budget it was owed.
	if err := b.ExportTraces(ctx, payload("checkout", manySpans(2, 10)...)); err != nil {
		t.Fatal(err)
	}
	clk.advance(6 * time.Second)
	b.Sweep(ctx)

	if got := c.traces()[traceID(2)]; got != 10 {
		t.Fatalf("the new trace exported %d of its 10 spans: a re-decided trace was charged twice and ate its budget", got)
	}
}

// The cache's two lifetimes, directly: the VERDICT expires at the TTL (a
// straggler past it gets a fresh window, which is what the knob buys) while the
// record that the trace was decided at all outlives it (which is what stops the
// re-charge).
func TestDecisionCacheRemembersTheChargePastTheVerdict(t *testing.T) {
	now := time.Unix(1700000000, 0)
	c := newDecisionCache(10, time.Minute)
	c.put(traceID(1), true, now)

	if keep, ok := c.get(traceID(1), now); !ok || !keep {
		t.Fatal("the verdict should be live immediately after the put")
	}
	later := now.Add(2 * time.Minute)
	if _, ok := c.get(traceID(1), later); ok {
		t.Fatal("an expired verdict must not be applied: past the TTL a straggler is a new trace")
	}
	if !c.seen(traceID(1)) {
		t.Fatal("the cache forgot it had decided this trace, so the re-decision will charge the rate budget a second time")
	}
	if c.seen(traceID(2)) {
		t.Fatal("a trace never decided reads as seen")
	}
}

// Eviction under the SIZE cap is the one thing that genuinely loses both
// lifetimes, and it is counted — but only when it takes a verdict that was still
// LIVE. Reclaiming an expired tombstone is the cache working as configured, and
// counting it would make the "your cap is too small" signal fire in every steady
// state.
func TestOnlyLiveVerdictEvictionsAreCounted(t *testing.T) {
	now := time.Unix(1700000000, 0)
	c := newDecisionCache(2, time.Minute)

	before := obs.TailSampleCacheEvicted.Value()
	c.put(traceID(1), true, now)
	c.put(traceID(2), true, now)
	c.put(traceID(3), true, now) // evicts trace 1, whose verdict is still live
	if got := obs.TailSampleCacheEvicted.Value() - before; got != 1 {
		t.Fatalf("evicting a live verdict counted %v times, want 1", got)
	}
	if c.seen(traceID(1)) {
		t.Fatal("the evicted trace is still remembered")
	}

	// Now let everything age out and evict again: reclaiming tombstones is not
	// a signal.
	old := now.Add(2 * time.Minute)
	before = obs.TailSampleCacheEvicted.Value()
	c.put(traceID(4), true, old)
	c.put(traceID(5), true, old)
	if got := obs.TailSampleCacheEvicted.Value() - before; got != 0 {
		t.Fatalf("reclaiming expired tombstones counted %v evictions, want 0", got)
	}
}

// The map stays bounded by decisionCacheSize now that entries outlive their
// verdicts — that bound is the only one left.
func TestDecisionCacheStaysBounded(t *testing.T) {
	now := time.Unix(1700000000, 0)
	c := newDecisionCache(8, time.Minute)
	for i := 0; i < 1000; i++ {
		c.put(traceID(uint64(i)), i%2 == 0, now.Add(time.Duration(i)*time.Millisecond))
	}
	if c.len() > 8 {
		t.Fatalf("cache holds %d entries, above its cap of 8", c.len())
	}
	if len(c.fifo) > 32 {
		t.Fatalf("the eviction FIFO grew to %d slots for a cap of 8", len(c.fifo))
	}
}
