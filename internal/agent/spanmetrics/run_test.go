package spanmetrics

import (
	"context"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

type syncExporter struct {
	mu sync.Mutex
	md []pmetric.Metrics
}

func (c *syncExporter) ExportMetrics(_ context.Context, md pmetric.Metrics) error {
	cp := pmetric.NewMetrics()
	md.CopyTo(cp)
	c.mu.Lock()
	c.md = append(c.md, cp)
	c.mu.Unlock()
	return nil
}

func (c *syncExporter) exports() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.md)
}

// Run must flush once more on shutdown (with a fresh context): spans consumed
// after the last tick would otherwise be silently lost.
func TestRunFinalFlushOnShutdown(t *testing.T) {
	g := New(Config{})
	g.Consume(traces("svc", spanSpec{name: "op", kind: ptrace.SpanKindServer, status: ptrace.StatusCodeOk, dur: 0.01}))

	exp := &syncExporter{}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	// A long interval guarantees the ticker never fires; only the shutdown
	// flush can export.
	go func() {
		defer close(done)
		g.Run(ctx, exp, time.Hour, pcommon.NewResource(), nil)
	}()
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after cancel")
	}
	if got := exp.exports(); got != 1 {
		t.Fatalf("exports after shutdown = %d, want 1 (the final flush)", got)
	}
	if _, ok := findIn(exp.md[0], "traces.span.metrics.calls"); !ok {
		t.Fatal("final flush did not carry the consumed span's metrics")
	}
}

// An idle generator's Export sends nothing (no empty payloads on quiet cycles).
func TestExportIdleSendsNothing(t *testing.T) {
	g := New(Config{})
	exp := &syncExporter{}
	if err := g.Export(context.Background(), exp, pcommon.NewResource()); err != nil {
		t.Fatal(err)
	}
	if got := exp.exports(); got != 0 {
		t.Fatalf("idle Export sent %d payloads, want 0", got)
	}
}

func findIn(md pmetric.Metrics, name string) (pmetric.Metric, bool) {
	rms := md.ResourceMetrics()
	for i := 0; i < rms.Len(); i++ {
		sms := rms.At(i).ScopeMetrics()
		for j := 0; j < sms.Len(); j++ {
			ms := sms.At(j).Metrics()
			for k := 0; k < ms.Len(); k++ {
				if ms.At(k).Name() == name {
					return ms.At(k), true
				}
			}
		}
	}
	return pmetric.Metric{}, false
}

type failNTraces struct {
	fail int
	got  []ptrace.Traces
}

func (f *failNTraces) ExportTraces(_ context.Context, td ptrace.Traces) error {
	if f.fail > 0 {
		f.fail--
		return contextError{}
	}
	f.got = append(f.got, td)
	return nil
}

type contextError struct{}

func (contextError) Error() string { return "transient forward failure" }

// The tap must aggregate only AFTER a successful forward: a transient failure
// surfaces retryable to the sender, whose re-push would otherwise aggregate the
// same spans twice and permanently inflate the cumulative counters.
func TestTapDoesNotDoubleCountOnRetry(t *testing.T) {
	g := New(Config{})
	inner := &failNTraces{fail: 1}
	tp := g.Tap(inner)

	td := traces("svc", spanSpec{name: "op", kind: ptrace.SpanKindServer, status: ptrace.StatusCodeOk, dur: 0.01})
	if err := tp.ExportTraces(context.Background(), td); err == nil {
		t.Fatal("first forward should have failed")
	}
	// Sender retries the SAME batch; this time the forward succeeds.
	if err := tp.ExportTraces(context.Background(), td); err != nil {
		t.Fatal(err)
	}

	exp := &syncExporter{}
	if err := g.Export(context.Background(), exp, pcommon.NewResource()); err != nil {
		t.Fatal(err)
	}
	calls, ok := findIn(exp.md[0], "traces.span.metrics.calls")
	if !ok {
		t.Fatal("calls not exported")
	}
	if v := calls.Sum().DataPoints().At(0).IntValue(); v != 1 {
		t.Fatalf("calls = %d, want 1 (the retried batch double-counted)", v)
	}
	if len(inner.got) != 1 {
		t.Fatalf("forwards delivered = %d, want 1", len(inner.got))
	}
}
