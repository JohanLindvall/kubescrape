package spanmetrics

import (
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

// The span-metrics tap calls Consume with the ingest RPC waiting on it, and
// Consume takes the same series lock renderRED does. Holding that lock across
// the whole payload build would stall every trace push on the tier for the
// render's duration; the chunked snapshot must keep a concurrent Consume from
// blocking for anywhere near the render time. Sibling of servicegraph's
// TestRenderDoesNotStallRecord.
func TestRenderDoesNotStallConsume(t *testing.T) {
	const series = 20000
	g := New(Config{MaxCardinality: series + 1})
	for i := 0; i < series; i++ {
		g.Consume(traces(fmt.Sprintf("svc-%05d", i),
			spanSpec{name: "op", kind: ptrace.SpanKindServer, status: ptrace.StatusCodeOk, dur: 0.01}))
	}

	stop := make(chan struct{})
	done := make(chan struct{})
	var maxStall atomic.Int64
	var calls atomic.Int64
	warm := traces("svc-00000",
		spanSpec{name: "op", kind: ptrace.SpanKindServer, status: ptrace.StatusCodeOk, dur: 0.01})
	go func() {
		defer close(done)
		for {
			select {
			case <-stop:
				return
			default:
			}
			t0 := time.Now()
			g.Consume(warm) // an admitted series: the warm path, one lock hold
			if d := int64(time.Since(t0)); d > maxStall.Load() {
				maxStall.Store(d)
			}
			calls.Add(1)
		}
	}()
	for calls.Load() < 1000 {
		time.Sleep(time.Millisecond)
	}

	start := time.Now()
	md := g.store.Render(pcommon.NewResource(), g.now())
	render := time.Since(start)
	close(stop)
	<-done

	stall := time.Duration(maxStall.Load())
	t.Logf("render of %d series took %v; worst concurrent Consume took %v", series, render, stall)
	if md.ResourceMetrics().Len() == 0 {
		t.Fatal("nothing rendered")
	}
	// Relative so a slow machine moves both together; the absolute floor keeps
	// an unlucky GC pause from failing a fast render.
	if stall > render/2 && stall > 2*time.Millisecond {
		t.Errorf("a concurrent Consume blocked for %v during a %v render: the build is holding the series lock across the whole payload",
			stall, render)
	}
}
