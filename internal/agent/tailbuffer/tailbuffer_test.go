package tailbuffer

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"sigs.k8s.io/yaml"

	"github.com/JohanLindvall/kubescrape/internal/agent/tailsample"
	"github.com/JohanLindvall/kubescrape/internal/agent/tracesample"
	"github.com/JohanLindvall/kubescrape/internal/obs"
)

// --- helpers ----------------------------------------------------------------

// capture is the downstream exporter: it records what the buffer forwards, and
// can be made to fail.
type capture struct {
	mu      sync.Mutex
	batches []ptrace.Traces
	err     error
	calls   int
}

func (c *capture) ExportTraces(_ context.Context, td ptrace.Traces) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls++
	if c.err != nil {
		return c.err
	}
	c.batches = append(c.batches, td)
	return nil
}

func (c *capture) fail(err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.err = err
}

// spans returns every exported span's (trace id, span id) pair, so a test can
// assert both completeness and the absence of duplicates.
func (c *capture) spans() map[[2]string]int {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := map[[2]string]int{}
	for _, td := range c.batches {
		rss := td.ResourceSpans()
		for i := 0; i < rss.Len(); i++ {
			sss := rss.At(i).ScopeSpans()
			for j := 0; j < sss.Len(); j++ {
				sp := sss.At(j).Spans()
				for k := 0; k < sp.Len(); k++ {
					s := sp.At(k)
					tid, sid := s.TraceID(), s.SpanID()
					out[[2]string{string(tid[:]), string(sid[:])}]++
				}
			}
		}
	}
	return out
}

func (c *capture) count() int { return len(c.spans()) }

// traces returns the distinct trace ids exported.
func (c *capture) traces() map[pcommon.TraceID]int {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := map[pcommon.TraceID]int{}
	for _, td := range c.batches {
		rss := td.ResourceSpans()
		for i := 0; i < rss.Len(); i++ {
			sss := rss.At(i).ScopeSpans()
			for j := 0; j < sss.Len(); j++ {
				sp := sss.At(j).Spans()
				for k := 0; k < sp.Len(); k++ {
					out[sp.At(k).TraceID()]++
				}
			}
		}
	}
	return out
}

func (c *capture) sends() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.calls
}

func traceID(n uint64) pcommon.TraceID {
	var id pcommon.TraceID
	for i := 0; i < 8; i++ {
		id[15-i] = byte(n >> (8 * i))
	}
	return id
}

func spanID(n uint64) pcommon.SpanID {
	var id pcommon.SpanID
	for i := 0; i < 8; i++ {
		id[7-i] = byte(n >> (8 * i))
	}
	return id
}

type spanSpec struct {
	trace  uint64
	span   uint64
	start  int64 // milliseconds
	end    int64
	status ptrace.StatusCode
	attrs  map[string]any
}

// payload builds one pushed batch: one resource, one scope, the given spans.
func payload(svc string, specs ...spanSpec) ptrace.Traces {
	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	rs.Resource().Attributes().PutStr("service.name", svc)
	ss := rs.ScopeSpans().AppendEmpty()
	ss.Scope().SetName("test")
	for _, s := range specs {
		sp := ss.Spans().AppendEmpty()
		sp.SetTraceID(traceID(s.trace))
		sp.SetSpanID(spanID(s.span))
		sp.SetName("op")
		sp.SetStartTimestamp(pcommon.Timestamp(s.start * int64(time.Millisecond)))
		sp.SetEndTimestamp(pcommon.Timestamp(s.end * int64(time.Millisecond)))
		sp.Status().SetCode(s.status)
		for k, v := range s.attrs {
			switch t := v.(type) {
			case string:
				sp.Attributes().PutStr(k, t)
			case int:
				sp.Attributes().PutInt(k, int64(t))
			case bool:
				sp.Attributes().PutBool(k, t)
			}
		}
	}
	return td
}

// clock is the injectable test clock; every test drives it explicitly rather
// than sleeping (the store tests' pattern).
type clock struct {
	mu sync.Mutex
	t  time.Time
}

func newClock() *clock { return &clock{t: time.Unix(1700000000, 0)} }

func (c *clock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *clock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

// alwaysCfg is a policy list that keeps everything, for the tests that are about
// the BUFFER rather than the policies.
func alwaysCfg() tailsample.Config {
	return tailsample.Config{Policies: []tailsample.PolicyConfig{
		{Name: "all", Type: tailsample.TypeAlwaysSample},
	}}
}

// errorsCfg keeps only traces with an ERROR span — the smallest policy list that
// makes the two verdicts distinguishable.
func errorsCfg() tailsample.Config {
	return tailsample.Config{Policies: []tailsample.PolicyConfig{
		{Name: "errors", Type: tailsample.TypeStatusCode,
			StatusCode: &tailsample.StatusCodeConfig{StatusCodes: []string{"ERROR"}}},
	}}
}

func newTestBuffer(t testing.TB, cfg Config, next TracesExporter) (*Buffer, *clock) {
	t.Helper()
	b, err := New(cfg, next, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	c := newClock()
	b.now = c.now
	return b, c
}

// counter reads one obs counter's current value, so a test can assert a DELTA
// (the registry is process-global and every test in this package shares it).
func counter(v interface{ Value() float64 }) float64 { return v.Value() }

// --- the window -------------------------------------------------------------

// The core guarantee: nothing leaves before the window closes, and when it does
// the whole trace leaves together — including the spans that arrived in
// different pushes. Without the buffer each push would be forwarded on arrival.
func TestTraceIsHeldUntilTheWindowThenExportedWhole(t *testing.T) {
	cap := &capture{}
	b, clk := newTestBuffer(t, Config{Config: alwaysCfg(), DecisionWait: "5s"}, cap)
	ctx := context.Background()

	for i := uint64(1); i <= 3; i++ {
		if err := b.ExportTraces(ctx, payload("checkout", spanSpec{trace: 7, span: i, start: 0, end: 10})); err != nil {
			t.Fatalf("push %d: %v", i, err)
		}
		clk.advance(time.Second)
	}
	if cap.sends() != 0 {
		t.Fatalf("the buffer forwarded %d payloads before the decision window closed", cap.sends())
	}
	if got := (Stats{Traces: 1, Spans: 3}); b.Stats() != got {
		t.Fatalf("Stats = %+v, want %+v", b.Stats(), got)
	}

	// Two seconds in — the first span is 3s old, still inside the window.
	b.Sweep(ctx)
	if cap.sends() != 0 {
		t.Fatalf("a trace was decided %v after its first span, before the 5s window", 3*time.Second)
	}

	clk.advance(3 * time.Second) // first span is now 6s old
	b.Sweep(ctx)
	if got, want := cap.sends(), 1; got != want {
		t.Fatalf("sends = %d, want %d (the whole trace must leave in ONE payload)", got, want)
	}
	if got, want := cap.count(), 3; got != want {
		t.Fatalf("exported %d spans, want %d", got, want)
	}
	if b.Stats() != (Stats{}) {
		t.Fatalf("the buffer still holds %+v after the decision", b.Stats())
	}
}

// The other verdict: a trace nothing matches is dropped whole, and its spans are
// counted as the cost of that decision.
func TestUnsampledTraceIsDroppedWhole(t *testing.T) {
	cap := &capture{}
	b, clk := newTestBuffer(t, Config{Config: errorsCfg(), DecisionWait: "5s"}, cap)
	ctx := context.Background()

	before := counter(obs.TailSampleSpans.WithLabelValues("dropped"))
	dropped := counter(obs.TailSampleTraces.WithLabelValues("drop", "none"))

	if err := b.ExportTraces(ctx, payload("checkout",
		spanSpec{trace: 11, span: 1, end: 5}, spanSpec{trace: 11, span: 2, end: 5})); err != nil {
		t.Fatal(err)
	}
	clk.advance(6 * time.Second)
	b.Sweep(ctx)

	if cap.sends() != 0 {
		t.Fatalf("an unsampled trace was exported (%d sends)", cap.sends())
	}
	if got := counter(obs.TailSampleSpans.WithLabelValues("dropped")) - before; got != 2 {
		t.Fatalf("dropped spans counted %v, want 2", got)
	}
	if got := counter(obs.TailSampleTraces.WithLabelValues("drop", "none")) - dropped; got != 1 {
		t.Fatalf("drop{policy=none} counted %v, want 1 (no policy had an opinion)", got)
	}
}

// The deciding policy is a label, not a footnote: an operator has to be able to
// answer "why was this kept".
func TestDecisionIsAttributedToItsPolicy(t *testing.T) {
	cap := &capture{}
	b, clk := newTestBuffer(t, Config{Config: errorsCfg(), DecisionWait: "1s"}, cap)
	ctx := context.Background()

	before := counter(obs.TailSampleTraces.WithLabelValues("keep", "errors"))
	if err := b.ExportTraces(ctx, payload("checkout",
		spanSpec{trace: 12, span: 1, end: 5, status: ptrace.StatusCodeError})); err != nil {
		t.Fatal(err)
	}
	clk.advance(2 * time.Second)
	b.Sweep(ctx)
	if got := counter(obs.TailSampleTraces.WithLabelValues("keep", "errors")) - before; got != 1 {
		t.Fatalf("keep{policy=errors} counted %v, want 1", got)
	}
	if cap.count() != 1 {
		t.Fatalf("exported %d spans, want 1", cap.count())
	}
}

// --- late spans -------------------------------------------------------------

// A span arriving after its trace was KEPT rides out immediately: it belongs to
// a trace the collector has already been told about.
func TestLateSpanFollowsAKeep(t *testing.T) {
	cap := &capture{}
	b, clk := newTestBuffer(t, Config{Config: alwaysCfg(), DecisionWait: "1s"}, cap)
	ctx := context.Background()

	if err := b.ExportTraces(ctx, payload("checkout", spanSpec{trace: 21, span: 1, end: 5})); err != nil {
		t.Fatal(err)
	}
	clk.advance(2 * time.Second)
	b.Sweep(ctx)

	before := counter(obs.TailSampleLate.WithLabelValues("kept"))
	if err := b.ExportTraces(ctx, payload("checkout", spanSpec{trace: 21, span: 2, end: 5})); err != nil {
		t.Fatal(err)
	}
	if got, want := cap.count(), 2; got != want {
		t.Fatalf("exported %d spans, want %d (the late span must be forwarded on arrival, not buffered)", got, want)
	}
	if b.Stats().Spans != 0 {
		t.Fatalf("the late span was buffered instead of forwarded: %+v", b.Stats())
	}
	if got := counter(obs.TailSampleLate.WithLabelValues("kept")) - before; got != 1 {
		t.Fatalf("late{kept} counted %v, want 1", got)
	}
}

// And after a DROP it is dropped, rather than starting a second window that
// could reach the opposite verdict for the same trace.
func TestLateSpanFollowsADrop(t *testing.T) {
	cap := &capture{}
	b, clk := newTestBuffer(t, Config{Config: errorsCfg(), DecisionWait: "1s"}, cap)
	ctx := context.Background()

	if err := b.ExportTraces(ctx, payload("checkout", spanSpec{trace: 22, span: 1, end: 5})); err != nil {
		t.Fatal(err)
	}
	clk.advance(2 * time.Second)
	b.Sweep(ctx)

	before := counter(obs.TailSampleLate.WithLabelValues("dropped"))
	// An ERROR span this time: it WOULD have been kept on its own, which is what
	// makes this a test of the cache rather than of the policy.
	if err := b.ExportTraces(ctx, payload("checkout",
		spanSpec{trace: 22, span: 2, end: 5, status: ptrace.StatusCodeError})); err != nil {
		t.Fatal(err)
	}
	if cap.sends() != 0 {
		t.Fatalf("a late span for a dropped trace was exported")
	}
	if b.Stats().Spans != 0 {
		t.Fatalf("a late span for a dropped trace was buffered: %+v", b.Stats())
	}
	if got := counter(obs.TailSampleLate.WithLabelValues("dropped")) - before; got != 1 {
		t.Fatalf("late{dropped} counted %v, want 1", got)
	}
}

// Past the cache the trace is simply unknown again, and a span for it starts a
// fresh window — the documented cost of bounding the cache.
func TestVerdictExpiresAndTheTraceStartsAFreshWindow(t *testing.T) {
	cap := &capture{}
	b, clk := newTestBuffer(t, Config{Config: alwaysCfg(), DecisionWait: "1s", DecisionCacheTTL: "10s"}, cap)
	ctx := context.Background()

	if err := b.ExportTraces(ctx, payload("checkout", spanSpec{trace: 23, span: 1, end: 5})); err != nil {
		t.Fatal(err)
	}
	clk.advance(2 * time.Second)
	b.Sweep(ctx)

	clk.advance(11 * time.Second) // the verdict has aged out
	if err := b.ExportTraces(ctx, payload("checkout", spanSpec{trace: 23, span: 2, end: 5})); err != nil {
		t.Fatal(err)
	}
	if got := b.Stats(); got.Traces != 1 || got.Spans != 1 {
		t.Fatalf("Stats = %+v, want one buffered trace (an expired verdict must start a fresh window)", got)
	}
}

// The cache's SIZE bound: the oldest verdict is evicted and counted, because a
// span for it will now be judged a second time.
func TestDecisionCacheEvictsUnderItsSizeCap(t *testing.T) {
	c := newDecisionCache(2, time.Minute)
	now := time.Unix(1, 0)
	before := counter(obs.TailSampleCacheEvicted)

	c.put(traceID(1), true, true, now)
	c.put(traceID(2), false, true, now)
	c.put(traceID(3), true, true, now)

	if _, ok := c.get(traceID(1), now); ok {
		t.Fatal("the oldest verdict survived the size cap")
	}
	if keep, ok := c.get(traceID(2), now); !ok || keep {
		t.Fatalf("verdict 2 = (%v,%v), want (false,true)", keep, ok)
	}
	if keep, ok := c.get(traceID(3), now); !ok || !keep {
		t.Fatalf("verdict 3 = (%v,%v), want (true,true)", keep, ok)
	}
	if got := counter(obs.TailSampleCacheEvicted) - before; got != 1 {
		t.Fatalf("cache evictions counted %v, want 1", got)
	}
	if c.len() > 2 {
		t.Fatalf("cache holds %d entries, want <= 2", c.len())
	}
}

// Re-deciding a trace must not let its OLD slot evict the new verdict when the
// FIFO reaches it.
func TestDecisionCacheHandlesARedecidedTrace(t *testing.T) {
	c := newDecisionCache(4, time.Minute)
	now := time.Unix(1, 0)
	c.put(traceID(1), true, true, now)
	c.put(traceID(1), false, true, now) // re-decided
	c.put(traceID(2), true, true, now)
	c.put(traceID(3), true, true, now)
	c.put(traceID(4), true, true, now) // over the cap: evicts the stale slot's owner
	if keep, ok := c.get(traceID(1), now); !ok || keep {
		t.Fatalf("verdict 1 = (%v,%v), want the SECOND verdict (false,true)", keep, ok)
	}
}

// --- the memory bounds ------------------------------------------------------

// maxSpansPerTrace: the trace is decided on what it has, and the rest of it
// follows through the cache rather than being buffered or lost.
func TestMaxSpansPerTraceDecidesEarly(t *testing.T) {
	cap := &capture{}
	b, _ := newTestBuffer(t, Config{Config: alwaysCfg(), DecisionWait: "1m", MaxSpansPerTrace: 3}, cap)
	ctx := context.Background()

	before := counter(obs.TailSampleEarly.WithLabelValues(reasonSpansPerTrace))
	specs := make([]spanSpec, 5)
	for i := range specs {
		specs[i] = spanSpec{trace: 31, span: uint64(i + 1), end: 5}
	}
	if err := b.ExportTraces(ctx, payload("checkout", specs...)); err != nil {
		t.Fatal(err)
	}
	if got := counter(obs.TailSampleEarly.WithLabelValues(reasonSpansPerTrace)) - before; got != 1 {
		t.Fatalf("early{spans_per_trace} counted %v, want 1", got)
	}
	if got, want := cap.count(), 5; got != want {
		t.Fatalf("exported %d spans, want %d (3 decided early, 2 following the verdict)", got, want)
	}
	if got := b.Stats().Spans; got != 0 {
		t.Fatalf("the buffer holds %d spans, want 0 (the per-trace bound must free them)", got)
	}
}

// maxTraces: the OLDEST trace is decided, not the new one, and never more than
// the cap is held.
func TestMaxTracesDecidesTheOldestEarly(t *testing.T) {
	cap := &capture{}
	b, clk := newTestBuffer(t, Config{Config: alwaysCfg(), DecisionWait: "1m", MaxTraces: 2}, cap)
	ctx := context.Background()

	before := counter(obs.TailSampleEarly.WithLabelValues(reasonMaxTraces))
	for i := uint64(1); i <= 3; i++ {
		if err := b.ExportTraces(ctx, payload("checkout", spanSpec{trace: 40 + i, span: i, end: 5})); err != nil {
			t.Fatal(err)
		}
		clk.advance(time.Millisecond)
	}
	if got := counter(obs.TailSampleEarly.WithLabelValues(reasonMaxTraces)) - before; got != 1 {
		t.Fatalf("early{max_traces} counted %v, want 1", got)
	}
	if got := b.Stats().Traces; got != 2 {
		t.Fatalf("the buffer holds %d traces, want the 2 the bound allows", got)
	}
	if got := cap.traces(); len(got) != 1 || got[traceID(41)] != 1 {
		t.Fatalf("exported traces %v, want exactly the OLDEST (41)", got)
	}
}

// maxSpans: the ceiling that actually bounds the process, enforced whichever
// trace the spans belong to.
func TestMaxSpansDecidesOldestUntilUnderTheCeiling(t *testing.T) {
	cap := &capture{}
	b, clk := newTestBuffer(t, Config{Config: alwaysCfg(), DecisionWait: "1m", MaxSpans: 6, MaxSpansPerTrace: 6}, cap)
	ctx := context.Background()

	before := counter(obs.TailSampleEarly.WithLabelValues(reasonMaxSpans))
	// Three traces of three spans each: 9 spans against a ceiling of 6.
	for tr := uint64(1); tr <= 3; tr++ {
		specs := make([]spanSpec, 3)
		for i := range specs {
			specs[i] = spanSpec{trace: 50 + tr, span: tr*10 + uint64(i), end: 5}
		}
		if err := b.ExportTraces(ctx, payload("checkout", specs...)); err != nil {
			t.Fatal(err)
		}
		clk.advance(time.Millisecond)
	}
	if got := counter(obs.TailSampleEarly.WithLabelValues(reasonMaxSpans)) - before; got != 1 {
		t.Fatalf("early{max_spans} counted %v, want 1", got)
	}
	if got := b.Stats().Spans; got > 6 {
		t.Fatalf("the buffer holds %d spans, above the ceiling of 6", got)
	}
	if got := cap.traces(); got[traceID(51)] != 3 {
		t.Fatalf("exported %v, want the oldest trace's 3 spans", got)
	}
}

// The ceiling holds even when a single push is larger than the whole buffer:
// nothing is dropped unjudged, and the buffer does not end up over its bound.
func TestOversizedPushStaysUnderTheCeiling(t *testing.T) {
	cap := &capture{}
	b, _ := newTestBuffer(t, Config{Config: alwaysCfg(), DecisionWait: "1m", MaxSpans: 4, MaxSpansPerTrace: 4}, cap)
	ctx := context.Background()

	specs := make([]spanSpec, 20)
	for i := range specs {
		specs[i] = spanSpec{trace: uint64(60 + i), span: uint64(i), end: 5}
	}
	if err := b.ExportTraces(ctx, payload("checkout", specs...)); err != nil {
		t.Fatal(err)
	}
	if got := b.Stats().Spans; got > 4 {
		t.Fatalf("the buffer holds %d spans, above the ceiling of 4", got)
	}
	// Everything that left was judged (alwaysSample keeps), so nothing was lost.
	if got, want := cap.count()+b.Stats().Spans, 20; got != want {
		t.Fatalf("%d spans accounted for, want %d — the rest were dropped unjudged", got, want)
	}
}

// --- shutdown ---------------------------------------------------------------

// The bound on the contract's exposure: a graceful stop decides everything, so
// only a hard kill loses spans.
func TestFlushDecidesEveryBufferedTrace(t *testing.T) {
	cap := &capture{}
	b, _ := newTestBuffer(t, Config{Config: alwaysCfg(), DecisionWait: "1m"}, cap)
	ctx := context.Background()

	before := counter(obs.TailSampleEarly.WithLabelValues(reasonShutdown))
	for i := uint64(1); i <= 4; i++ {
		if err := b.ExportTraces(ctx, payload("checkout", spanSpec{trace: 70 + i, span: i, end: 5})); err != nil {
			t.Fatal(err)
		}
	}
	if cap.sends() != 0 {
		t.Fatal("traces left before the flush")
	}
	b.Flush(ctx)
	if got, want := cap.count(), 4; got != want {
		t.Fatalf("the flush exported %d spans, want %d", got, want)
	}
	if got := b.Stats(); got != (Stats{}) {
		t.Fatalf("the buffer still holds %+v after the flush", got)
	}
	if got := counter(obs.TailSampleEarly.WithLabelValues(reasonShutdown)) - before; got != 4 {
		t.Fatalf("early{shutdown} counted %v, want 4", got)
	}
}

// --- failures ---------------------------------------------------------------

// A decided trace whose export fails is LOST, and says so: the sender was acked
// when the spans were buffered, so nothing else is holding them.
func TestFailedDecisionExportCountsLost(t *testing.T) {
	cap := &capture{}
	b, clk := newTestBuffer(t, Config{Config: alwaysCfg(), DecisionWait: "1s"}, cap)
	ctx := context.Background()

	if err := b.ExportTraces(ctx, payload("checkout",
		spanSpec{trace: 81, span: 1, end: 5}, spanSpec{trace: 81, span: 2, end: 5})); err != nil {
		t.Fatal(err)
	}
	cap.fail(errors.New("collector down"))
	before := counter(obs.TailSampleSpans.WithLabelValues("lost"))
	clk.advance(2 * time.Second)

	// Cancelled context: the retry must not sit here for the whole backoff, and
	// the shutdown path passes a budgeted one for exactly this reason.
	cctx, cancel := context.WithCancel(ctx)
	cancel()
	b.Sweep(cctx)

	if got := counter(obs.TailSampleSpans.WithLabelValues("lost")) - before; got != 2 {
		t.Fatalf("lost spans counted %v, want 2", got)
	}
}

// A failed forward of ALREADY-DECIDED spans fails the push instead: those spans
// are in the payload the sender still holds, so its retry recovers them.
func TestFailedLateForwardFailsThePush(t *testing.T) {
	cap := &capture{}
	b, clk := newTestBuffer(t, Config{Config: alwaysCfg(), DecisionWait: "1s"}, cap)
	ctx := context.Background()

	if err := b.ExportTraces(ctx, payload("checkout", spanSpec{trace: 82, span: 1, end: 5})); err != nil {
		t.Fatal(err)
	}
	clk.advance(2 * time.Second)
	b.Sweep(ctx)

	cap.fail(errors.New("collector down"))
	err := b.ExportTraces(ctx, payload("checkout", spanSpec{trace: 82, span: 2, end: 5}))
	if err == nil {
		t.Fatal("a failed forward of a late span was acked to the sender; its retry is the only thing that can recover it")
	}
	// A push whose traces are all still assembling is acked regardless: there is
	// nothing to forward and so nothing that can fail.
	if err := b.ExportTraces(ctx, payload("checkout", spanSpec{trace: 83, span: 3, end: 5})); err != nil {
		t.Fatalf("a buffering-only push failed: %v", err)
	}
}

// --- composition ------------------------------------------------------------

// The head sampler runs ABOVE this layer and the two must NEST: a tail
// probabilistic policy at 50% keeps everything a head sampler at probability 0.5
// already kept, rather than halving it again. Both hash the trace id, unsalted,
// with the same threshold arithmetic — if either side changes that, this fails.
func TestNestsWithTheHeadSampler(t *testing.T) {
	const n = 500
	cap := &capture{}
	b, clk := newTestBuffer(t, Config{
		Config: tailsample.Config{Policies: []tailsample.PolicyConfig{
			{Name: "half", Type: tailsample.TypeProbabilistic,
				Probabilistic: &tailsample.ProbabilisticConfig{SamplingPercentage: 50}},
		}},
		DecisionWait: "1s",
	}, cap)
	head := tracesample.New(tracesample.Config{Probability: 0.5}, b)
	ctx := context.Background()

	specs := make([]spanSpec, 0, n)
	for i := uint64(0); i < n; i++ {
		specs = append(specs, spanSpec{trace: i, span: i, end: 5})
	}
	if err := head.ExportTraces(ctx, payload("checkout", specs...)); err != nil {
		t.Fatalf("head sampler: %v", err)
	}
	headKept := b.Stats().Traces
	if headKept == 0 || headKept == n {
		t.Fatalf("the head sampler passed %d of %d traces; this test would prove nothing", headKept, n)
	}
	clk.advance(2 * time.Second)
	b.Flush(ctx)

	if got := len(cap.traces()); got != headKept {
		t.Fatalf("the tail sampler kept %d of the %d traces the head sampler passed — the two stages compound instead of nesting", got, headKept)
	}
}

// Spans of one trace arriving concurrently (the receiver's handler goroutines)
// must all land in the same buffered trace and leave together, exactly once.
// Run with -race.
func TestConcurrentReceive(t *testing.T) {
	const goroutines, per = 8, 50
	cap := &capture{}
	b, clk := newTestBuffer(t, Config{Config: alwaysCfg(), DecisionWait: "1m", MaxSpansPerTrace: 100000}, cap)
	ctx := context.Background()

	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < per; i++ {
				id := uint64(g*per + i)
				// Two traces: one shared by every goroutine, one per span, so the
				// test exercises both the contended entry and map growth.
				err := b.ExportTraces(ctx, payload("checkout",
					spanSpec{trace: 999, span: 1000 + id, end: 5},
					spanSpec{trace: 100000 + id, span: id, end: 5}))
				if err != nil {
					t.Errorf("push: %v", err)
					return
				}
			}
		}(g)
	}
	wg.Wait()

	if got, want := b.Stats().Spans, 2*goroutines*per; got != want {
		t.Fatalf("buffered %d spans, want %d", got, want)
	}
	if got, want := b.Stats().Traces, goroutines*per+1; got != want {
		t.Fatalf("buffered %d traces, want %d", got, want)
	}
	clk.advance(2 * time.Minute)
	b.Flush(ctx)

	spans := cap.spans()
	if got, want := len(spans), 2*goroutines*per; got != want {
		t.Fatalf("exported %d distinct spans, want %d", got, want)
	}
	for k, n := range spans {
		if n != 1 {
			t.Fatalf("span %x exported %d times, want once", k, n)
		}
	}
	if got := cap.traces()[traceID(999)]; got != goroutines*per {
		t.Fatalf("the shared trace exported %d spans, want %d", got, goroutines*per)
	}
}

// --- config -----------------------------------------------------------------

// The section as an operator writes it. The agent config is UnmarshalStrict'ed,
// so a field spelled differently here than in the documentation rejects the
// WHOLE file and the workload does not start — and a duration typed as
// time.Duration would reject "5s" outright, which is the bug this repo has
// already shipped once.
const documentedYAML = `
decisionWait: 5s
maxTraces: 50000
maxSpansPerTrace: 500
maxSpans: 100000
decisionCacheSize: 50000
decisionCacheTTL: 2m
policies:
  - name: errors
    type: statusCode
    statusCode:
      statusCodes: [ERROR]
  - name: slow
    type: latency
    latency:
      threshold: 500ms
  - name: baseline
    type: probabilistic
    probabilistic:
      samplingPercentage: 5
`

func TestConfigDecodesFromDocumentedYAML(t *testing.T) {
	var cfg Config
	if err := yaml.UnmarshalStrict([]byte(documentedYAML), &cfg); err != nil {
		t.Fatalf("the documented tailSampling YAML does not decode: %v", err)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate rejected the documented config: %v", err)
	}
	if !cfg.Enabled() {
		t.Fatal("Enabled = false for a three-policy list")
	}
	if got, want := len(cfg.Policies), 3; got != want {
		t.Fatalf("decoded %d policies, want %d (the embedded policy list must stay flat in the section)", got, want)
	}
	s, err := cfg.settings()
	if err != nil {
		t.Fatalf("settings: %v", err)
	}
	if s.wait != 5*time.Second || s.cacheTTL != 2*time.Minute {
		t.Fatalf("durations resolved to %v/%v, want 5s/2m", s.wait, s.cacheTTL)
	}
	if s.maxTraces != 50000 || s.maxSpansPerTrace != 500 || s.maxSpans != 100000 || s.cacheSize != 50000 {
		t.Fatalf("bounds resolved to %+v", s)
	}
}

func TestConfigDefaults(t *testing.T) {
	cfg := Config{Config: alwaysCfg()}
	s, err := cfg.settings()
	if err != nil {
		t.Fatal(err)
	}
	if s.wait != defaultDecisionWait || s.maxTraces != defaultMaxTraces ||
		s.maxSpansPerTrace != defaultMaxSpansPerTrace || s.maxSpans != defaultMaxSpans ||
		s.cacheSize != defaultCacheSize || s.cacheTTL != defaultCacheTTL {
		t.Fatalf("defaults resolved to %+v", s)
	}
}

func TestConfigValidation(t *testing.T) {
	cases := []struct {
		name string
		cfg  Config
		want string
	}{
		{"bad wait", Config{Config: alwaysCfg(), DecisionWait: "5 seconds"}, "decisionWait"},
		{"zero wait", Config{Config: alwaysCfg(), DecisionWait: "0s"}, "must be greater than zero"},
		{"negative wait", Config{Config: alwaysCfg(), DecisionWait: "-5s"}, "is negative"},
		{"negative bound", Config{Config: alwaysCfg(), MaxTraces: -1}, "negative"},
		{"per-trace above the ceiling", Config{Config: alwaysCfg(), MaxSpans: 10, MaxSpansPerTrace: 100}, "could never fit"},
		{"bad cache ttl", Config{Config: alwaysCfg(), DecisionCacheTTL: "always"}, "decisionCacheTTL"},
		{"knobs without policies", Config{DecisionWait: "5s"}, "no policies"},
		// The cache knobs are settings too: cache-only with no policies is the
		// same silently-samples-nothing misconfiguration as the bound knobs.
		{"cache size without policies", Config{DecisionCacheSize: 50000}, "no policies"},
		{"cache ttl without policies", Config{DecisionCacheTTL: "1m"}, "no policies"},
		{"a bad policy", Config{Config: tailsample.Config{Policies: []tailsample.PolicyConfig{
			{Name: "x", Type: "nonsense"}}}}, "unknown type"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.cfg.Validate()
			if err == nil {
				t.Fatalf("Validate accepted %+v", tc.cfg)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error %q does not mention %q", err, tc.want)
			}
			// Whatever Validate refuses, New must refuse too — the dry run and
			// the real start must not disagree.
			if tc.cfg.Enabled() {
				if _, nerr := New(tc.cfg, &capture{}, nil); nerr == nil {
					t.Fatal("New accepted a config Validate refused")
				}
			}
		})
	}
	// The empty section is not an error: a chart that renders it unconditionally
	// must not fail startup.
	var empty Config
	if err := empty.Validate(); err != nil {
		t.Fatalf("an empty section is an error: %v", err)
	}
	if empty.Enabled() {
		t.Fatal("an empty section reads as enabled")
	}
}

// The decision cadence follows the window, so the two cannot drift.
func TestTickFollowsTheWindow(t *testing.T) {
	for _, tc := range []struct{ wait, tick time.Duration }{
		{5 * time.Second, time.Second},
		{2 * time.Second, 500 * time.Millisecond},
		{50 * time.Millisecond, 100 * time.Millisecond},
		{time.Hour, time.Second},
	} {
		if got := tickFor(tc.wait); got != tc.tick {
			t.Fatalf("tickFor(%v) = %v, want %v", tc.wait, got, tc.tick)
		}
	}
}

// Run decides on its own cadence, without anyone calling Sweep.
func TestRunDecidesOnItsTicker(t *testing.T) {
	cap := &capture{}
	b, clk := newTestBuffer(t, Config{Config: alwaysCfg(), DecisionWait: "100ms"}, cap)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := b.ExportTraces(ctx, payload("checkout", spanSpec{trace: 91, span: 1, end: 5})); err != nil {
		t.Fatal(err)
	}
	clk.advance(time.Second)
	go b.Run(ctx)

	deadline := time.Now().Add(5 * time.Second)
	for cap.sends() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("Run did not decide a due trace within 5s")
		}
		time.Sleep(5 * time.Millisecond)
	}
	if got := cap.count(); got != 1 {
		t.Fatalf("exported %d spans, want 1", got)
	}
}
