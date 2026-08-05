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
	b := NewBuffered(send, ls, ms, nil, time.Millisecond, nil)

	if err := b.ExportMetrics(context.Background(), metricsWith("x")); err != nil {
		t.Fatal(err)
	}
	before := obs.BufferDroppedBatches.WithLabelValues("metrics").Value()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { b.Run(ctx); close(done) }()

	waitFor(t, func() bool {
		return obs.BufferDroppedBatches.WithLabelValues("metrics").Value()-before == 1
	}, "batch permanently rejected and dropped")
	waitFor(t, func() bool { return ms.Bytes() == 0 }, "spool drained")

	// Stop the drain goroutine before inspecting the sink's unsynchronized map.
	cancel()
	<-done
	if n := len(b.metrics.stuck); n != 0 {
		t.Fatalf("stuck map leaked %d entries after a permanent rejection; forget() was not called on the sendRejected path", n)
	}
}

// TestBlipDeliveryThenOutageDoesNotDrop: a single delivery during a blip must
// not let the RESUMED outage's transport failures spend the poison budget — the
// batch was never refused by a live collector. (Laps count only when the
// collector actually RESPONDED to the send: sink.stuckResponded.)
func TestBlipDeliveryThenOutageDoesNotDrop(t *testing.T) {
	s := &sink[plog.Logs]{kind: "logs"} // stuckResponded=false: transport failures
	data := []byte("a good batch")

	for i := 0; i < 5; i++ {
		if s.stuckTooLong(data) {
			t.Fatalf("dropped during the initial outage at lap %d", i+1)
		}
	}
	s.delivered++ // a blip: one other batch gets through
	for i := 0; i < 10; i++ {
		if s.stuckTooLong(data) {
			t.Fatalf("ZERO-LOSS BREACH: dropped during the resumed outage at lap %d after a single blip delivery", i+1)
		}
	}
}

// TestPoisonRespondedLapsDrop pins the drop path: once another batch delivers
// while this one is stuck AND the collector keeps RESPONDING with rejections,
// the poison budget is spent and the batch drops.
func TestPoisonRespondedLapsDrop(t *testing.T) {
	s := &sink[plog.Logs]{kind: "logs", stuckResponded: true} // e.g. ResourceExhausted
	data := []byte("a poison batch")

	if s.stuckTooLong(data) {
		t.Fatal("dropped on first sighting")
	}
	// Every lap must show the collector delivering something ELSE while this
	// batch keeps failing — that is the evidence, and it expires.
	for lap := 1; lap < maxDrainCycles; lap++ {
		s.delivered++
		if s.stuckTooLong(data) {
			t.Fatalf("dropped on lap %d, before the budget was spent", lap)
		}
	}
	s.delivered++
	if !s.stuckTooLong(data) {
		t.Fatal("poison batch not dropped after maxDrainCycles responded laps with concurrent deliveries")
	}
}

// The bug this rule exists to prevent: ONE early delivery must not licence
// every later responded lap to spend budget. A collector under memory pressure
// that answers ResourceExhausted to everything is back-pressuring, not
// rejecting this payload — and dropping then destroys good data during exactly
// the outage the disk buffer exists to survive.
func TestPoisonBudgetNotSpentWithoutConcurrentProgress(t *testing.T) {
	s := &sink[plog.Logs]{kind: "logs", stuckResponded: true}
	data := []byte("a batch stuck behind back-pressure")

	if s.stuckTooLong(data) {
		t.Fatal("dropped on first sighting")
	}
	s.delivered++ // one delivery, early
	if s.stuckTooLong(data) {
		t.Fatal("dropped one lap too early")
	}
	// From here nothing else gets through. However many laps this takes, the
	// batch must survive: there is no evidence the collector would accept it.
	for lap := 0; lap < 10*maxDrainCycles; lap++ {
		if s.stuckTooLong(data) {
			t.Fatalf("dropped on lap %d with no concurrent delivery: that is a back-pressure outage, not poison", lap)
		}
	}
}

// respondedError gates the poison-drop budget: only a collector RESPONSE counts
// as evidence that this payload is bad, because a transport failure says
// nothing about the payload and spending the budget during an outage destroys
// good data. gRPC is the DEFAULT transport, and its arm of that classifier had
// no test at all — the one existing test hard-codes the boolean it stands for.
func TestRespondedErrorGRPCArm(t *testing.T) {
	for _, c := range []struct {
		name string
		err  error
		want bool
	}{
		// Back-pressure and outages: the collector may be down or shedding,
		// which is not evidence about THIS payload.
		{"unavailable", status.Error(codes.Unavailable, "collector restarting"), false},
		{"deadline", status.Error(codes.DeadlineExceeded, "too slow"), false},
		{"canceled", status.Error(codes.Canceled, "client went away"), false},
		// Genuine responses: the collector looked at the payload and refused it.
		{"invalid argument", status.Error(codes.InvalidArgument, "bad payload"), true},
		{"resource exhausted", status.Error(codes.ResourceExhausted, "message too large"), true},
		{"unimplemented", status.Error(codes.Unimplemented, "no logs service"), true},
		{"internal", status.Error(codes.Internal, "collector bug"), true},
		{"unauthenticated", status.Error(codes.Unauthenticated, "bad token"), true},
		// Not a status at all: a dial failure, a DNS miss, a closed connection.
		{"plain error", context.DeadlineExceeded, false},
	} {
		if got := respondedError(c.err); got != c.want {
			t.Errorf("%s: respondedError = %v, want %v", c.name, got, c.want)
		}
	}
}
