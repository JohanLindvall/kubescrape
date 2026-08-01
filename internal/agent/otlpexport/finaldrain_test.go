package otlpexport

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/plog"
)

// Buffered.Run returns the moment its context is cancelled, so every export
// made during shutdown — the tailer's and journald's last flushes, the ingest
// batcher's drain, and the final log-metrics and self-metrics windows, which
// are exported only after those goroutines have joined — lands in the spool
// with nothing left to carry it. FinalDrain is the pass that empties it before
// the process exits; without it that window waits for the next start of this
// pod on this node and is lost outright if the pod never returns or the buffer
// dir is not persistent.
func TestFinalDrainShipsPostShutdownExports(t *testing.T) {
	send := &fakeSender{}
	b, ls, ms := openBuffer(t, t.TempDir(), send, 0)
	defer func() { _ = ls.Close() }()
	defer func() { _ = ms.Close() }()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { b.Run(ctx); close(done) }()

	// Steady state: an export is delivered normally.
	if err := b.ExportMetrics(context.Background(), metricsWith("live")); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return len(send.gotMetrics()) == 1 }, "the pre-shutdown export")

	// SIGTERM: Run stops draining.
	cancel()
	<-done

	// Exports made after that reach only the spool.
	for _, name := range []string{"final_log_metrics", "final_self_metrics"} {
		if err := b.ExportMetrics(context.Background(), metricsWith(name)); err != nil {
			t.Fatal(err)
		}
	}
	if got := len(send.gotMetrics()); got != 1 {
		t.Fatalf("delivered %d payloads before the final drain, want 1 (Run must already be stopped)", got)
	}

	dctx, dcancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer dcancel()
	b.FinalDrain(dctx)

	if got := len(send.gotMetrics()); got != 3 {
		t.Fatalf("delivered %d payloads after FinalDrain, want 3: the shutdown-time exports were left in the spool", got)
	}
}

// FinalDrain must return even when the collector is down, so a dead endpoint
// cannot outlive the pod's termination grace.
func TestFinalDrainIsBounded(t *testing.T) {
	send := &fakeSender{failNext: 1 << 30} // log sends never succeed
	b, ls, ms := openBuffer(t, t.TempDir(), send, 0)
	defer func() { _ = ls.Close() }()
	defer func() { _ = ms.Close() }()

	if err := b.ExportLogs(context.Background(), logsWith("stuck")); err != nil {
		t.Fatal(err)
	}

	dctx, dcancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer dcancel()
	start := time.Now()
	b.FinalDrain(dctx)
	if el := time.Since(start); el > 5*time.Second {
		t.Fatalf("FinalDrain took %v against a dead collector; it must stay bounded by its context", el)
	}
}

// gateSender blocks its FIRST log export until released, so a drain can be
// held mid-cycle on demand.
type gateSender struct {
	fakeSender
	n       atomic.Int32
	entered chan struct{} // closed when that first send starts
	release chan struct{} // closed to let it finish
}

func (g *gateSender) ExportLogs(ctx context.Context, ld plog.Logs) error {
	if g.n.Add(1) == 1 {
		close(g.entered)
		<-g.release
	}
	return g.fakeSender.ExportLogs(ctx, ld)
}

// FinalDrain must be safe to call while Run is still draining. The agent joins
// its producers — Run among them — on a bounded shutdown BUDGET, so cancelling
// Run's context establishes no happens-before with its return: a Run still
// inside a send cycle overlaps the FinalDrain that follows, and the two share
// the queue Reader and the sink's cur/delivered/stuck bookkeeping. Run under
// -race; without the drain token this reports a data race (and can double-read
// the reader), with it FinalDrain simply waits its turn and still ships
// everything.
func TestFinalDrainOverlapsRunSafely(t *testing.T) {
	send := &gateSender{entered: make(chan struct{}), release: make(chan struct{})}
	dir := t.TempDir()
	ls, err := OpenBuffer(dir+"/logs", 0)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ls.Close() }()
	ms, err := OpenBuffer(dir+"/metrics", 0)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ms.Close() }()
	b := NewBuffered(send, ls, ms, 10*time.Millisecond, nil)

	want := []string{"first", "second", "third"}
	for _, name := range want {
		if err := b.ExportLogs(context.Background(), logsWith(name)); err != nil {
			t.Fatal(err)
		}
	}

	runCtx, runCancel := context.WithCancel(context.Background())
	runDone := make(chan struct{})
	go func() { b.Run(runCtx); close(runDone) }()

	<-send.entered // Run is inside a send: mid-cycle, holding a reservation

	// SIGTERM. Run has NOT returned — it is still in the blocked send — which is
	// exactly the state the bounded producer join leaves behind.
	runCancel()

	drainDone := make(chan struct{})
	dctx, dcancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer dcancel()
	go func() { b.FinalDrain(dctx); close(drainDone) }()

	// Give the final drain a window in which it would otherwise be running its
	// own cycle against the same reader and sink state as the blocked Run.
	time.Sleep(100 * time.Millisecond)
	close(send.release)

	<-runDone
	<-drainDone

	got := send.gotLogs()
	if len(got) != len(want) {
		t.Fatalf("delivered %v, want each of %v exactly once", got, want)
	}
	seen := map[string]bool{}
	for _, g := range got {
		seen[g] = true
	}
	for _, w := range want {
		if !seen[w] {
			t.Errorf("batch %q was never delivered", w)
		}
	}
}
