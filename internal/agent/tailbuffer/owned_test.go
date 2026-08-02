package tailbuffer

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/JohanLindvall/kubescrape/internal/agent/otlpexport"
	"github.com/JohanLindvall/kubescrape/internal/obs"
)

// ownCapture records, per export, whether the payload was marked as OURS
// (otlpexport.Own) — the seam that decides whether the disk buffer spools it or
// passes it through.
type ownCapture struct {
	mu    sync.Mutex
	owned []bool
}

func (c *ownCapture) ExportTraces(ctx context.Context, _ ptrace.Traces) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.owned = append(c.owned, otlpexport.Owned(ctx))
	return nil
}

func (c *ownCapture) marks() []bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]bool(nil), c.owned...)
}

// A trace decided by the sweep is ours: its sender was acked when the spans were
// buffered and holds nothing, so the payload must be marked for the disk buffer.
// This is the whole of the durability fix at this end.
func TestDecidedKeepsAreOwned(t *testing.T) {
	cap := &ownCapture{}
	b, clk := newTestBuffer(t, Config{Config: alwaysCfg(), DecisionWait: "5s"}, cap)
	ctx := context.Background()
	if err := b.ExportTraces(ctx, payload("checkout", spanSpec{trace: 1, span: 1, end: 10})); err != nil {
		t.Fatal(err)
	}
	if got := cap.marks(); len(got) != 0 {
		t.Fatalf("a purely buffering push exported something: %v", got)
	}
	clk.advance(6 * time.Second)
	b.Sweep(ctx)
	got := cap.marks()
	if len(got) != 1 {
		t.Fatalf("want one export from the sweep, got %d", len(got))
	}
	if !got[0] {
		t.Fatal("a decided keep was exported UNMARKED: the disk buffer would pass it through, and a failed send is unrecoverable loss")
	}
}

// The shutdown flush is the same case — those spans are just as acked.
func TestFlushedKeepsAreOwned(t *testing.T) {
	cap := &ownCapture{}
	b, _ := newTestBuffer(t, Config{Config: alwaysCfg(), DecisionWait: "5s"}, cap)
	ctx := context.Background()
	if err := b.ExportTraces(ctx, payload("checkout", spanSpec{trace: 1, span: 1, end: 10})); err != nil {
		t.Fatal(err)
	}
	b.Flush(ctx)
	got := cap.marks()
	if len(got) != 1 || !got[0] {
		t.Fatalf("the shutdown flush must mark its payload as owned, got %v", got)
	}
}

// A span arriving for an ALREADY-decided trace is different: it is still in the
// payload its sender holds, and a failed send fails the push so the sender's
// retry recovers it. Marking it would ack a sender for data that has not
// shipped — the contract the pass-through rule exists to keep.
func TestLateSpansAreNotOwned(t *testing.T) {
	cap := &ownCapture{}
	b, clk := newTestBuffer(t, Config{Config: alwaysCfg(), DecisionWait: "5s"}, cap)
	ctx := context.Background()
	if err := b.ExportTraces(ctx, payload("checkout", spanSpec{trace: 1, span: 1, end: 10})); err != nil {
		t.Fatal(err)
	}
	clk.advance(6 * time.Second)
	b.Sweep(ctx) // decides KEEP and caches the verdict

	// A straggler for the same trace: forwarded on the receiving goroutine.
	if err := b.ExportTraces(ctx, payload("checkout", spanSpec{trace: 1, span: 2, end: 10})); err != nil {
		t.Fatal(err)
	}
	got := cap.marks()
	if len(got) != 2 {
		t.Fatalf("want the sweep export plus the late forward, got %d", len(got))
	}
	if got[1] {
		t.Fatal("a late span was marked as owned; its sender still holds it and would be acked for an undelivered payload")
	}
}

// A push that makes a BOUND bind carries spans out of the buffer in the same
// send. Those are ours, so the payload is marked even though it arrived on the
// receive path.
func TestEarlyDecidedSpansInAPushAreOwned(t *testing.T) {
	cap := &ownCapture{}
	b, _ := newTestBuffer(t, Config{
		Config: alwaysCfg(), DecisionWait: "5s", MaxSpansPerTrace: 1, MaxSpans: 10,
	}, cap)
	if err := b.ExportTraces(context.Background(), payload("checkout", spanSpec{trace: 1, span: 1, end: 10})); err != nil {
		t.Fatal(err)
	}
	got := cap.marks()
	if len(got) != 1 {
		t.Fatalf("the per-trace bound should have decided and exported in the push, got %d exports", len(got))
	}
	if !got[0] {
		t.Fatal("spans an early decision took out of the buffer were exported unmarked: nobody else holds them")
	}
}

// downstream is a whole otlpexport-shaped collector: logs, metrics and traces,
// with an outage switch. otlpexport.NewBuffered needs an Exporter (logs +
// metrics) that also happens to take traces, which is what the real chain is.
type downstream struct {
	mu    sync.Mutex
	down  bool
	spans int
}

func (d *downstream) ExportLogs(context.Context, plog.Logs) error          { return nil }
func (d *downstream) ExportMetrics(context.Context, pmetric.Metrics) error { return nil }

func (d *downstream) ExportTraces(_ context.Context, td ptrace.Traces) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.down {
		return errors.New("collector is down")
	}
	d.spans += td.SpanCount()
	return nil
}

func (d *downstream) setDown(v bool) { d.mu.Lock(); d.down = v; d.mu.Unlock() }
func (d *downstream) got() int       { d.mu.Lock(); defer d.mu.Unlock(); return d.spans }

// The two halves composed, which is the fix in one test: a collector outage
// during a decision becomes a BACKLOG rather than
// kubescrape_tail_sampling_spans_total{outcome="lost"}. Before the ownership
// marker the same sweep spent its three retries against the dead collector and
// dropped the trace.
func TestDecidedKeepsSurviveACollectorOutageThroughTheDiskBuffer(t *testing.T) {
	coll := &downstream{}
	coll.setDown(true)
	tb, err := otlpexport.OpenBuffer(t.TempDir()+"/traces", 0)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tb.Close() }()
	spool := otlpexport.NewBuffered(coll, nil, nil, tb, 10*time.Millisecond, nil)

	b, clk := newTestBuffer(t, Config{Config: alwaysCfg(), DecisionWait: "5s"}, spool)
	ctx := context.Background()
	// The registry is process-global, so measure the DELTA this test causes.
	lost0 := obs.TailSampleSpans.WithLabelValues("lost").Value()
	if err := b.ExportTraces(ctx, payload("checkout", spanSpec{trace: 1, span: 1, end: 10})); err != nil {
		t.Fatal(err)
	}
	clk.advance(6 * time.Second)
	b.Sweep(ctx) // decides KEEP while the collector is dead

	if lost := obs.TailSampleSpans.WithLabelValues("lost").Value() - lost0; lost != 0 {
		t.Fatalf("the decision was counted lost (%v) even though a disk buffer took it", lost)
	}
	if tb.Bytes() == 0 {
		t.Fatal("the decided trace did not reach the traces spool")
	}

	rctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go spool.Run(rctx)
	coll.setDown(false)
	deadline := time.Now().Add(3 * time.Second)
	for coll.got() == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if coll.got() != 1 {
		t.Fatalf("the spooled trace was not delivered once the collector recovered (got %d spans)", coll.got())
	}
}
