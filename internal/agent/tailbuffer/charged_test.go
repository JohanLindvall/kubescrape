package tailbuffer

import (
	"context"
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"

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

// The charge bit must survive EVERY re-decision, not just the first. A
// re-decision peeks the budget and therefore reports Charged == false, so a
// cache that stored that verbatim forgot the payment after one straggler and
// billed the trace again on the next one — a trace whose spans trickle in over
// hours (a batch job, a Kafka consumer, a backlog released after a collector
// outage) then eats the rate budget once per decision window.
//
// The assertion is the new trace at the end: after three windows for one
// 10-span trace, the 20-span/s budget has been spent exactly once and a
// genuinely new 10-span trace still fits.
func TestTheChargeSurvivesEveryReDecision(t *testing.T) {
	c := &capture{}
	b, clk := newTestBuffer(t, Config{
		Config: rateCfg(20), DecisionWait: "5s", DecisionCacheTTL: "1m",
	}, c)
	ctx := context.Background()
	decide := func(specs ...spanSpec) {
		t.Helper()
		if err := b.ExportTraces(ctx, payload("checkout", specs...)); err != nil {
			t.Fatal(err)
		}
		clk.advance(6 * time.Second)
		b.Sweep(ctx)
	}

	decide(manySpans(1, 10)...) // spends 10 of the 20
	if b.cache.charged(traceID(1)) == 0 {
		t.Fatal("the decision that billed the budget is not remembered as charged")
	}
	for window := 2; window <= 3; window++ {
		clk.advance(61 * time.Second) // past the TTL: a straggler opens a fresh window
		decide(spanSpec{trace: 1, span: uint64(window), end: 10})
		if b.cache.charged(traceID(1)) == 0 {
			t.Fatalf("window %d forgot that trace 1 had paid, so window %d charges the budget again", window, window+1)
		}
	}

	decide(manySpans(2, 10)...)
	if got := c.traces()[traceID(2)]; got != 10 {
		t.Fatalf("the new trace exported %d of its 10 spans: a re-decided trace was charged twice and ate its budget", got)
	}
}

// The cache's two lifetimes, directly: the VERDICT expires at the TTL (a
// straggler past it gets a fresh window, which is what the knob buys) while the
// record of what the decision BILLED outlives it (which is what stops the
// re-charge).
func TestDecisionCacheRemembersTheChargePastTheVerdict(t *testing.T) {
	now := time.Unix(1700000000, 0)
	c := newDecisionCache(10, time.Minute)
	c.put(traceID(1), true, 1, now)

	if keep, ok := c.get(traceID(1), now); !ok || !keep {
		t.Fatal("the verdict should be live immediately after the put")
	}
	later := now.Add(2 * time.Minute)
	if _, ok := c.get(traceID(1), later); ok {
		t.Fatal("an expired verdict must not be applied: past the TTL a straggler is a new trace")
	}
	if c.charged(traceID(1)) == 0 {
		t.Fatal("the cache forgot this trace had paid, so the re-decision will charge the rate budget a second time")
	}
	if c.charged(traceID(2)) != 0 {
		t.Fatal("a trace never decided reads as charged")
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
	c.put(traceID(1), true, 1, now)
	c.put(traceID(2), true, 1, now)
	c.put(traceID(3), true, 1, now) // evicts trace 1, whose verdict is still live
	if got := obs.TailSampleCacheEvicted.Value() - before; got != 1 {
		t.Fatalf("evicting a live verdict counted %v times, want 1", got)
	}
	if _, ok := c.m[traceID(1)]; ok {
		t.Fatal("the evicted trace is still remembered")
	}

	// Now let everything age out and evict again: reclaiming tombstones is not
	// a signal.
	old := now.Add(2 * time.Minute)
	before = obs.TailSampleCacheEvicted.Value()
	c.put(traceID(4), true, 1, old)
	c.put(traceID(5), true, 1, old)
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
		c.put(traceID(uint64(i)), i%2 == 0, 1, now.Add(time.Duration(i)*time.Millisecond))
	}
	if c.len() > 8 {
		t.Fatalf("cache holds %d entries, above its cap of 8", c.len())
	}
	if len(c.fifo) > 32 {
		t.Fatalf("the eviction FIFO grew to %d slots for a cap of 8", len(c.fifo))
	}
}

// The decision FIFO is a slice behind a bounded map, and only evict advanced
// its head — which runs only once the map is at its cap. Below the cap, every
// re-decision appended a slot and freed none, so a steady stream of late spans
// for already-decided traces grew it without bound for the process' life.
func TestDecisionCacheFIFODoesNotGrowUnbounded(t *testing.T) {
	c := newDecisionCache(1024, time.Hour)
	now := time.Now()
	var id pcommon.TraceID
	id[0] = 1
	// Re-decide the SAME few traces far more often than the cap, so the map
	// stays tiny and evict never runs.
	for i := 0; i < 100000; i++ {
		id[1] = byte(i % 8)
		c.put(id, true, 1, now)
	}
	if got := c.len(); got > 8 {
		t.Fatalf("map holds %d entries, want <= 8", got)
	}
	if got := len(c.fifo); got > 2*c.len()+compactFloor+8 {
		t.Errorf("fifo holds %d slots for %d live entries — it is not being compacted", got, c.len())
	}
}

// A decision the rate policy REFUSED spent nothing, so the trace must not be
// remembered as charged: the fresh window a straggler opens once the verdict has
// expired would then PEEK a budget the trace never paid, and every span it
// admits leaves without being billed — repeatedly, once per cache TTL, for as
// long as the entry lives.
//
// The observable end of that is the trace after it: the re-decision must SPEND
// what it admits, so a genuinely new trace finds the bucket where the spending
// left it.
func TestARefusedDecisionIsNotRememberedAsCharged(t *testing.T) {
	c := &capture{}
	// Burst 10 (tracehash.NewBucket floors it at the rate), and the whole test
	// runs in well under a millisecond of REAL time — the evaluator's bucket is
	// the one clock a test cannot inject — so refill is under a thousandth of a
	// token and every margin below is a whole one.
	b, clk := newTestBuffer(t, Config{
		Config: rateCfg(10), DecisionWait: "5s", DecisionCacheTTL: "10s",
	}, c)
	ctx := context.Background()
	decide := func(specs ...spanSpec) {
		t.Helper()
		if err := b.ExportTraces(ctx, payload("checkout", specs...)); err != nil {
			t.Fatal(err)
		}
		clk.advance(6 * time.Second)
		b.Sweep(ctx)
	}

	decide(manySpans(1, 8)...) // spends 8 of the 10
	if b.cache.charged(traceID(1)) == 0 {
		t.Fatal("the decision that billed the budget is not remembered as charged")
	}

	decide(manySpans(2, 6)...) // refused: 6 > the 2 tokens left, and nothing is spent
	if got := c.traces()[traceID(2)]; got != 0 {
		t.Fatalf("the over-budget trace exported %d spans", got)
	}
	if b.cache.charged(traceID(2)) != 0 {
		t.Fatal("a trace the rate policy REFUSED is remembered as charged, so its next window admits spans nothing ever billed")
	}

	// Past the TTL a straggler opens a fresh window for trace 2. It fits the two
	// remaining tokens, and it must SPEND one.
	clk.advance(11 * time.Second)
	decide(spanSpec{trace: 2, span: 99, end: 10})
	if got := c.traces()[traceID(2)]; got != 1 {
		t.Fatalf("the straggler exported %d spans, want 1", got)
	}

	// One token left, so a two-span trace no longer fits.
	decide(manySpans(3, 2)...)
	if got := c.traces()[traceID(3)]; got != 0 {
		t.Fatalf("a new trace exported %d spans on a budget the re-decision was supposed to have spent", got)
	}
}
