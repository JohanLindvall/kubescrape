package otlpexport

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/JohanLindvall/diskqueue"
	"go.opentelemetry.io/collector/pdata/plog"

	"github.com/JohanLindvall/kubescrape/internal/obs"
)

func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// A deliberate Close must not be recovered from. An enqueue that lands after
// shutdown — the agent joins its producers on a budget, so one wedged against a
// dead collector outlives the join — sees the same ErrClosed a dead handle gives,
// and without the latch it reopens the queue behind the caller that closed it:
// a new flock and a new preallocated segment nothing will ever close, while the
// enqueue that triggered the reopen still fails.
func TestCloseIsNotRecoveredFrom(t *testing.T) {
	buf, err := OpenBuffer(t.TempDir(), 0)
	if err != nil {
		t.Fatalf("OpenBuffer: %v", err)
	}
	if err := buf.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	closed, _ := buf.handles()

	if err := buf.add([]byte("a straggler")); !errors.Is(err, diskqueue.ErrClosed) {
		t.Fatalf("add after Close = %v, want ErrClosed", err)
	}
	if q, _ := buf.handles(); q != closed {
		_ = q.Close() // do not leave the leaked queue's flock behind
		t.Fatal("an enqueue after Close reopened the queue: the new one is never closed, and the enqueue failed anyway")
	}
}

// A read on a handle THIS process retired — a recover swap, or the Close above —
// is an expected transition, not a disk failure, and counting it makes the two
// indistinguishable in the one metric an operator watches for a failing disk.
func TestReadOnARetiredHandleIsNotCountedAsAReadFailure(t *testing.T) {
	buf, err := OpenBuffer(t.TempDir(), 0)
	if err != nil {
		t.Fatalf("OpenBuffer: %v", err)
	}
	s := &sink[plog.Logs]{buf: buf, kind: "logs", log: quietLogger()}
	if err := buf.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	before := obs.BufferReadErrors.WithLabelValues("logs", "false").Value()
	s.drainUntilEmpty(context.Background()) // reserves on the retired handle
	if got := obs.BufferReadErrors.WithLabelValues("logs", "false").Value() - before; got != 0 {
		t.Fatalf("a read on a handle we retired counted %v disk-buffer read failures, want 0", got)
	}
}

// The inter-attempt wait is CANCELLABLE and does not sit out its backoff: at
// SIGTERM every draining sink is waiting on one, and the shutdown budget is
// spent on whatever they do next. (The timer is stopped as well —
// otlpexport.Retry's rule — which nothing in Go can observe from here.)
func TestTrySendAbandonsItsBackoffWhenCancelled(t *testing.T) {
	s := &sink[plog.Logs]{
		kind:    "logs",
		log:     quietLogger(),
		backoff: time.Hour, // a wait no test may sit out
		send:    func(context.Context, plog.Logs) error { return context.DeadlineExceeded },
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan sendResult, 1)
	ld := plog.NewLogs()
	// alive=false: the cheap-lap early return must not short-circuit the wait
	// this test is about (and DeadlineExceeded is not a collector response
	// either way).
	go func() { done <- s.trySend(ctx, func(c context.Context) error { return s.send(c, ld) }, false) }()
	cancel()

	select {
	case got := <-done:
		if got != sendCancelled {
			t.Fatalf("trySend = %v, want sendCancelled", got)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("trySend sat out its backoff after the context was cancelled")
	}
}
