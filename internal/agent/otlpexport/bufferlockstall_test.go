package otlpexport

import (
	"context"
	"log/slog"
	"os"
	"testing"
	"time"
)

// blockingHandler stands in for a node whose log collector has stopped reading:
// the write to stderr does not return until something lets it. It is the honest
// model of the failure the rule exists for — a slog call is a handler call plus
// an io.Writer write, and neither is bounded.
type blockingHandler struct {
	entered chan struct{}
	release chan struct{}
}

func (h *blockingHandler) Enabled(context.Context, slog.Level) bool { return true }
func (h *blockingHandler) WithAttrs([]slog.Attr) slog.Handler       { return h }
func (h *blockingHandler) WithGroup(string) slog.Handler            { return h }

func (h *blockingHandler) Handle(context.Context, slog.Record) error {
	select {
	case h.entered <- struct{}{}:
	default:
	}
	<-h.release
	return nil
}

// Buffer.recover reports a failed reopen, and b.mu is the mutex every enqueue,
// every drain iteration and every buffer-stats gauge evaluation reads through
// handles(). If the line is emitted while that WRITE lock is held, one slow log
// consumer stalls every producer on the node for as long as it takes to read —
// during a disk failure, which is exactly when the producers need to learn that
// their batches are being refused.
//
// Reverse-patch check: move the b.log.Error call back inside swap (i.e. under
// b.mu) and this test times out on stats() instead of returning.
func TestRecoverDoesNotLogUnderTheBufferLock(t *testing.T) {
	dir := t.TempDir()
	buf, err := OpenBuffer(dir+"/logs", 0)
	if err != nil {
		t.Fatal(err)
	}
	h := &blockingHandler{entered: make(chan struct{}, 1), release: make(chan struct{})}
	buf.kind, buf.log = "logs", slog.New(h)

	// Make the reopen fail: the queue's directory is replaced by a regular
	// file, so diskqueue.New cannot open it. That is the "the mount went
	// read-only / the directory is gone" case the line reports.
	q, _ := buf.handles()
	if err := os.RemoveAll(dir + "/logs"); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(dir+"/logs", []byte("not a directory"), 0o600); err != nil {
		t.Fatal(err)
	}

	done := make(chan struct{})
	go func() { defer close(done); buf.recover(q) }()

	select {
	case <-h.entered:
	case <-time.After(10 * time.Second):
		close(h.release)
		t.Fatal("the failed reopen was never reported")
	}

	// The log handler is now blocked mid-write. A reader of the same mutex must
	// still get through.
	got := make(chan struct{})
	go func() { defer close(got); buf.stats() }()
	select {
	case <-got:
	case <-time.After(5 * time.Second):
		close(h.release)
		<-done
		t.Fatal("stats() blocked behind a log write: recover is emitting under b.mu, " +
			"so one slow log consumer stalls every enqueue and every drain on this buffer")
	}

	close(h.release)
	<-done
	_ = buf.Close()
}
