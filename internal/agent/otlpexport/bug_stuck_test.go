package otlpexport

import (
	"context"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/JohanLindvall/kubescrape/internal/obs"
	"github.com/JohanLindvall/kubescrape/pkg/spool"
)

// TestBankedRecoveryDeliveriesDoNotDropGoodBatch is the regression test for the
// cross-outage false drop: deliveries banked during an EARLIER recovery (while
// this batch merely sat in the queue) must not be spent against a single later
// failure. The earlier code dropped after >=maxDrainCycles cumulative deliveries
// since first-stuck, regardless of whether this batch was failing across them.
func TestBankedRecoveryDeliveriesDoNotDropGoodBatch(t *testing.T) {
	s := &sink[plog.Logs]{kind: "logs"}
	data := []byte("a perfectly good log batch")

	// Outage: the batch circles the queue, failing every cycle. delivered frozen.
	for i := 0; i < 10; i++ {
		if s.stuckTooLong(data) {
			t.Fatalf("dropped during the outage at cycle %d", i+1)
		}
	}

	// Recovery: the collector accepts THREE other batches from the backlog while
	// this one waits behind them (not attempted, so no stuck cycle spans them).
	s.delivered += 3

	// It then fails ONCE (a blip while the collector is still cold). One failure
	// after a recovery this batch did not participate in is not poison evidence.
	if s.stuckTooLong(data) {
		t.Fatal("ZERO-LOSS BREACH: good batch dropped after a single failure following a recovery " +
			"it merely sat behind — banked deliveries must not spend the poison budget")
	}
}

type flakyThenPermSender struct {
	mu       sync.Mutex
	attempts int
	permFrom int // attempt index (1-based) from which the collector rejects permanently
}

func (s *flakyThenPermSender) ExportLogs(context.Context, plog.Logs) error { return nil }
func (s *flakyThenPermSender) ExportMetrics(context.Context, pmetric.Metrics) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.attempts++
	if s.attempts >= s.permFrom {
		return status.Error(codes.InvalidArgument, "permanent") // IsPermanent == true
	}
	return status.Error(codes.Unavailable, "transient")
}

// TestStuckEntryForgottenOnPermanentRejection guards the stuck-map leak: a batch
// that first goes transient-stuck (creating a stuck-tracking entry) and then
// comes back permanently rejected must have its entry removed, not leaked toward
// the maxStuckTracked cap.
func TestStuckEntryForgottenOnPermanentRejection(t *testing.T) {
	// Transient for the first stuckAfterAttempts sends (→ sendStuck, entry
	// created), permanent thereafter (→ sendRejected on the next drain cycle).
	send := &flakyThenPermSender{permFrom: stuckAfterAttempts + 1}
	dir := t.TempDir()
	ls, err := spool.Open(dir+"/logs", spool.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ls.Close() }()
	ms, err := spool.Open(dir+"/metrics", spool.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ms.Close() }()
	b := NewBuffered(send, ls, ms, time.Millisecond, nil)

	if err := b.ExportMetrics(context.Background(), metricsWith("x")); err != nil {
		t.Fatal(err)
	}
	before := obs.BufferDropped.WithLabelValues("metrics").Value()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { b.Run(ctx); close(done) }()

	waitFor(t, func() bool {
		return obs.BufferDropped.WithLabelValues("metrics").Value()-before == 1
	}, "batch permanently rejected and dropped")
	waitFor(t, func() bool { return ms.Bytes() == 0 }, "spool drained")

	// Stop the drain goroutine before inspecting the sink's unsynchronized map.
	cancel()
	<-done
	if n := len(b.metrics.stuck); n != 0 {
		t.Fatalf("stuck map leaked %d entries after a permanent rejection; forget() was not called on the sendRejected path", n)
	}
}
