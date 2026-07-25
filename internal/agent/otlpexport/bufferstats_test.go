package otlpexport

import (
	"context"
	"testing"
)

// A filling buffer must be observable BEFORE it starts refusing writes: the
// other buffer metrics are counters that only move once data is already being
// dropped or refused, by which point the tailer is back-pressured.
func TestBufferStatsTrackBacklog(t *testing.T) {
	send := &fakeSender{}
	b, ls, ms := openBuffer(t, t.TempDir(), send, 4096)
	defer func() { _ = ls.Close() }()
	defer func() { _ = ms.Close() }()

	st := b.Stats()
	for _, sig := range []string{"logs", "metrics"} {
		s, ok := st[sig]
		if !ok {
			t.Fatalf("no stats for %q", sig)
		}
		if s.Backlog != 0 {
			t.Errorf("%s backlog = %d on a fresh spool, want 0", sig, s.Backlog)
		}
		if s.Cap != 4096 {
			t.Errorf("%s cap = %d, want the configured 4096 so utilisation is computable", sig, s.Cap)
		}
	}

	// Enqueue without draining: the backlog must grow.
	for i := 0; i < 5; i++ {
		if err := b.ExportLogs(context.Background(), logsWith("x")); err != nil {
			t.Fatal(err)
		}
	}
	got := b.Stats()["logs"]
	if got.Backlog <= 0 {
		t.Fatalf("logs backlog = %d after 5 undrained batches, want > 0", got.Backlog)
	}
	if got.Segments < 1 {
		t.Errorf("logs segments = %d, want at least 1", got.Segments)
	}

	// Draining must bring it back down.
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { b.Run(ctx); close(done) }()
	waitFor(t, func() bool { return b.Stats()["logs"].Backlog == 0 }, "the backlog to drain to zero")
	cancel()
	<-done
}
