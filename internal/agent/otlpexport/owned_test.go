package otlpexport

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/ptrace"
)

// traceSender is a fakeSender that also takes traces, with an injectable
// failure so the difference between "spooled" and "passed through" is visible:
// a spooled payload is accepted while the collector is down, a passed-through
// one is not.
type traceSender struct {
	fakeSender
	mu       sync.Mutex
	down     bool
	spanName []string
}

func (f *traceSender) ExportTraces(_ context.Context, td ptrace.Traces) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.down {
		return errors.New("collector is down")
	}
	rss := td.ResourceSpans()
	for i := 0; i < rss.Len(); i++ {
		sss := rss.At(i).ScopeSpans()
		for j := 0; j < sss.Len(); j++ {
			spans := sss.At(j).Spans()
			for k := 0; k < spans.Len(); k++ {
				f.spanName = append(f.spanName, spans.At(k).Name())
			}
		}
	}
	return nil
}

func (f *traceSender) setDown(down bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.down = down
}

func (f *traceSender) got() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.spanName...)
}

func tracesWith(name string) ptrace.Traces {
	td := ptrace.NewTraces()
	sp := td.ResourceSpans().AppendEmpty().ScopeSpans().AppendEmpty().Spans().AppendEmpty()
	sp.SetName(name)
	return td
}

func openTraceBuffered(t *testing.T, dir string, send *traceSender) (*Buffered, *Buffer) {
	t.Helper()
	tb, err := OpenBuffer(dir+"/traces", 0)
	if err != nil {
		t.Fatal(err)
	}
	return NewBuffered(send, nil, nil, tb, 10*time.Millisecond, nil), tb
}

// An OWNED traces payload — a tail-sampling decision, whose senders were acked
// when their spans were buffered — is spooled: it is accepted while the
// collector is down and delivered once it comes back. Without the ownership
// check in ExportTraces this passes straight through and the export fails, so
// the accept below is exactly what the feature buys.
func TestOwnedTracesAreSpooled(t *testing.T) {
	send := &traceSender{}
	send.setDown(true)
	b, tb := openTraceBuffered(t, t.TempDir(), send)
	defer func() { _ = tb.Close() }()

	if err := b.ExportTraces(Own(context.Background()), tracesWith("checkout")); err != nil {
		t.Fatalf("an owned traces payload must be accepted by the spool while the collector is down: %v", err)
	}
	if got := send.got(); len(got) != 0 {
		t.Fatalf("the payload reached the collector while it was down: %v", got)
	}
	if tb.Bytes() == 0 {
		t.Fatal("nothing was written to the traces spool")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go b.Run(ctx)
	send.setDown(false)
	waitFor(t, func() bool { return len(send.got()) == 1 }, "the spooled trace to be delivered")
}

// A plain FORWARDED trace keeps the old contract: it is passed through, and a
// failed send comes back to the pushing application, whose SDK retries. Acking
// it into a spool would remove the only other copy.
func TestForwardedTracesArePassedThrough(t *testing.T) {
	send := &traceSender{}
	send.setDown(true)
	b, tb := openTraceBuffered(t, t.TempDir(), send)
	defer func() { _ = tb.Close() }()

	if err := b.ExportTraces(context.Background(), tracesWith("checkout")); err == nil {
		t.Fatal("an unmarked traces payload must fail the push when the collector is down, not be spooled")
	}
	if tb.Bytes() != 0 {
		t.Fatal("an unmarked traces payload was written to the spool; the sender is acked for data that has not shipped")
	}

	send.setDown(false)
	if err := b.ExportTraces(context.Background(), tracesWith("retried")); err != nil {
		t.Fatal(err)
	}
	if got := send.got(); len(got) != 1 || got[0] != "retried" {
		t.Fatalf("pass-through delivery: got %v", got)
	}
}

// The point of the spool: a decided trace survives the process. Enqueue with the
// collector down, close everything, reopen the queue, and it is still there.
func TestOwnedTracesSurviveARestart(t *testing.T) {
	dir := t.TempDir()
	send := &traceSender{}
	send.setDown(true)
	b, tb := openTraceBuffered(t, dir, send)
	if err := b.ExportTraces(Own(context.Background()), tracesWith("checkout")); err != nil {
		t.Fatal(err)
	}
	if err := tb.Close(); err != nil {
		t.Fatal(err)
	}

	send2 := &traceSender{}
	b2, tb2 := openTraceBuffered(t, dir, send2)
	defer func() { _ = tb2.Close() }()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go b2.Run(ctx)
	waitFor(t, func() bool { return len(send2.got()) == 1 }, "the trace spooled by the previous process to be delivered")
	if got := send2.got(); got[0] != "checkout" {
		t.Fatalf("redelivered the wrong span: %v", got)
	}
}

// With no traces spool open, an owned payload still passes through: the marker
// asks for durability where it is available, it does not require it (a tier
// without -buffer-dir must keep working).
func TestOwnedTracesPassThroughWithoutASpool(t *testing.T) {
	send := &traceSender{}
	b := NewBuffered(send, nil, nil, nil, 10*time.Millisecond, nil)
	if err := b.ExportTraces(Own(context.Background()), tracesWith("checkout")); err != nil {
		t.Fatal(err)
	}
	if got := send.got(); len(got) != 1 {
		t.Fatalf("owned payload with no spool must be sent directly: %v", got)
	}
}

// Own is idempotent and Owned is false for a bare context — the marker must not
// be something a caller can half-apply.
func TestOwnedMarker(t *testing.T) {
	ctx := context.Background()
	if Owned(ctx) {
		t.Fatal("a bare context must not be owned")
	}
	o := Own(ctx)
	if !Owned(o) {
		t.Fatal("Own did not mark the context")
	}
	if Own(o) != o {
		t.Fatal("Own must be idempotent (a second call allocated another context)")
	}
}
