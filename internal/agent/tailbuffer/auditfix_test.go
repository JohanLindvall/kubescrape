package tailbuffer

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/JohanLindvall/kubescrape/internal/agent/otlpexport"
	"github.com/JohanLindvall/kubescrape/internal/agent/tailsample"
	"github.com/JohanLindvall/kubescrape/internal/obs"
)

// --- the send's context -----------------------------------------------------

// ctxCapture is a downstream that HONOURS its context, which the shared capture
// deliberately does not. Everything in this section is about what a cancelled or
// an expired context does to a send whose payload nobody else holds a copy of,
// so an exporter that ignores the context would prove nothing.
type ctxCapture struct {
	mu    sync.Mutex
	spans int
	owned bool
}

func (c *ctxCapture) ExportTraces(ctx context.Context, td ptrace.Traces) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.owned = otlpexport.Owned(ctx)
	c.spans += td.SpanCount()
	return nil
}

func (c *ctxCapture) got() (spans int, owned bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.spans, c.owned
}

// A sweep's export must survive the caller's cancellation. By the time the send
// runs, decide() has removed those traces from the buffer and cached their
// verdicts, so the shutdown Flush cannot re-decide them: cancelling the send
// there is not a retry, it is the loss the package doc says only a hard kill
// causes.
func TestDecidedKeepsSurviveTheCallersCancellation(t *testing.T) {
	next := &ctxCapture{}
	b, clk := newTestBuffer(t, Config{Config: alwaysCfg(), DecisionWait: "1s"}, next)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := b.ExportTraces(ctx, payload("checkout", spanSpec{trace: 401, span: 1, end: 5})); err != nil {
		t.Fatal(err)
	}
	clk.advance(2 * time.Second)
	lost0 := counter(obs.TailSampleSpans.WithLabelValues("lost"))

	cancel() // SIGTERM, landing while this sweep is exporting
	b.Sweep(ctx)

	spans, owned := next.got()
	if spans != 1 {
		t.Fatalf("the decided keep was not delivered (%d spans exported): it had already left the buffer, so nothing downstream of here can recover it", spans)
	}
	if !owned {
		t.Fatal("the ownership marker did not survive the detach; a disk buffer would pass the payload through instead of spooling it")
	}
	if lost := counter(obs.TailSampleSpans.WithLabelValues("lost")) - lost0; lost != 0 {
		t.Fatalf("lost spans counted %v, want 0", lost)
	}
}

// Detaching from the caller's CANCELLATION must not detach from its BUDGET: the
// shutdown path hands Flush a deadline precisely so a dead collector cannot hold
// the final pass past the pod's termination grace.
func TestDrainKeepsTheCallersDeadline(t *testing.T) {
	next := &ctxCapture{}
	b, clk := newTestBuffer(t, Config{Config: alwaysCfg(), DecisionWait: "1s"}, next)

	if err := b.ExportTraces(context.Background(), payload("checkout", spanSpec{trace: 402, span: 1, end: 5})); err != nil {
		t.Fatal(err)
	}
	clk.advance(2 * time.Second)

	// A shutdown step whose budget is already spent.
	fctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	start := time.Now()
	b.Flush(fctx)

	if spans, _ := next.got(); spans != 0 {
		t.Fatalf("the send ran on %d spans past a budget that was already gone", spans)
	}
	if d := time.Since(start); d > time.Second {
		t.Fatalf("the flush spent %v retrying past an expired budget", d)
	}
}

// blockingCapture holds its FIRST export until released, so a test can park the
// Run loop inside a sweep while the ticker's next tick becomes ready.
type blockingCapture struct {
	mu      sync.Mutex
	spans   int
	once    sync.Once
	entered chan struct{}
	release chan struct{}
}

func (c *blockingCapture) ExportTraces(_ context.Context, td ptrace.Traces) error {
	c.once.Do(func() {
		close(c.entered)
		<-c.release
	})
	c.mu.Lock()
	defer c.mu.Unlock()
	c.spans += td.SpanCount()
	return nil
}

// Once its context is cancelled, Run must leave the remaining traces to Flush —
// the pass that runs on the shutdown step's own budget. A tick that came ready
// while the previous sweep was exporting is still in the ticker's one-slot
// channel at that moment, and a select with two ready cases picks at random, so
// one pass proves nothing: each iteration here is one coin flip.
func TestRunDoesNotSweepOnceItsContextIsCancelled(t *testing.T) {
	for i := 0; i < 8; i++ {
		next := &blockingCapture{entered: make(chan struct{}), release: make(chan struct{})}
		b, clk := newTestBuffer(t, Config{Config: alwaysCfg(), DecisionWait: "100ms"}, next)
		ctx, cancel := context.WithCancel(context.Background())

		if err := b.ExportTraces(ctx, payload("checkout", spanSpec{trace: uint64(500 + i), span: 1, end: 5})); err != nil {
			t.Fatal(err)
		}
		clk.advance(time.Second)

		done := make(chan struct{})
		go func() { b.Run(ctx); close(done) }()
		<-next.entered // the loop is inside a sweep now, blocked in the export

		// A second due trace, then long enough for the next tick to land in the
		// ticker's one-slot channel while the sweep is still blocked.
		if err := b.ExportTraces(ctx, payload("checkout", spanSpec{trace: uint64(600 + i), span: 2, end: 5})); err != nil {
			t.Fatal(err)
		}
		clk.advance(time.Second)
		time.Sleep(b.tick + 30*time.Millisecond)
		cancel()
		close(next.release)
		<-done

		if got := b.Stats().Traces; got != 1 {
			t.Fatalf("iteration %d: the cancelled Run loop swept once more and decided a trace that main's Flush owns (the buffer holds %d traces)", i, got)
		}
		cancel()
	}
}

// --- decide's scratch -------------------------------------------------------

// decide's scratch is resliced, not cleared, so its tail keeps pdata handles
// into the last decision's trace — one span message plus its group's resource
// attributes each. On a DROP that is the payload the branch below it goes out of
// its way to release.
func TestDecideDoesNotRetainTheDecidedTracesSpans(t *testing.T) {
	next := &capture{}
	b, clk := newTestBuffer(t, Config{Config: errorsCfg(), DecisionWait: "1s"}, next)
	ctx := context.Background()

	// A wide trace with no ERROR span: DROPPED, which is a tail sampler's normal
	// mode and so its steady state.
	specs := make([]spanSpec, 0, 8)
	for i := uint64(1); i <= 8; i++ {
		specs = append(specs, spanSpec{trace: 700, span: i, end: 5})
	}
	if err := b.ExportTraces(ctx, payload("checkout", specs...)); err != nil {
		t.Fatal(err)
	}
	clk.advance(2 * time.Second)
	b.Sweep(ctx)

	// A narrow one behind it: its shorter fill overwrites only the front, so
	// whatever the wide one left beyond that is what stays reachable.
	if err := b.ExportTraces(ctx, payload("checkout", spanSpec{trace: 701, span: 9, end: 5})); err != nil {
		t.Fatal(err)
	}
	clk.advance(2 * time.Second)
	b.Sweep(ctx)

	for i, s := range b.scratch[:cap(b.scratch)] {
		if s != (tailsample.Span{}) {
			t.Fatalf("scratch[%d] of %d still holds a handle into a decided trace", i, cap(b.scratch))
		}
	}
}

// --- the memory budget ------------------------------------------------------

// The maxSpansPerTrace floor can raise a DERIVED ceiling back over the whole
// memory limit. As arms of one switch the guards below the lowering could not
// see that, so the config was accepted with a single log line saying it had been
// lowered "to fit". It is refused now, and it names the field that BINDS:
// lowering maxSpans would change nothing, since maxSpans is not what produced
// the number.
func TestDerivedCeilingFlooredAboveTheWholeLimitIsRefused(t *testing.T) {
	log, out := testLog(t)
	// 128 MiB: a quarter affords 32768 spans, but one trace may hold 150000.
	s := settings{maxSpans: defaultMaxSpans, maxSpansPerTrace: 150_000}
	err := applyMemoryBudget(&s, false, 128<<20, "test", log)
	if err == nil {
		t.Fatalf("a derived ceiling of %d spans (~%d bytes) was accepted under a 128 MiB limit; log was:\n%s",
			s.maxSpans, int64(s.maxSpans)*spanCostBytes, out())
	}
	if !strings.Contains(err.Error(), "maxSpansPerTrace") {
		t.Fatalf("the refusal must name the field that binds: %v", err)
	}
}

// The milder half of the same hole, and the one reachable with a single field at
// the chart's own default limit: the floored ceiling fits the limit but not the
// budget share, so it must be said out loud like any other over-budget ceiling.
func TestDerivedCeilingFlooredAboveTheBudgetShareWarns(t *testing.T) {
	log, out := testLog(t)
	// 512 MiB: a quarter affords 131072 spans, the floor holds it at 150000.
	s := settings{maxSpans: defaultMaxSpans, maxSpansPerTrace: 150_000}
	if err := applyMemoryBudget(&s, false, 512<<20, "test", log); err != nil {
		t.Fatal(err)
	}
	if s.maxSpans != 150_000 {
		t.Fatalf("maxSpans %d, want the floor 150000 (a trace must be able to fit whole)", s.maxSpans)
	}
	if !strings.Contains(out(), "above this workload's memory budget") {
		t.Fatalf("an over-budget derived ceiling was accepted silently; log was:\n%s", out())
	}
}
