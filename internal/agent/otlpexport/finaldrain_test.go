package otlpexport

import (
	"context"
	"testing"
	"time"
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
