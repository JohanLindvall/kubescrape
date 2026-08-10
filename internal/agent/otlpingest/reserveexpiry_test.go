package otlpingest

// kubescrape_ingest_rejected_total means one thing: a push refused because an
// admission bound was REACHED — the in-flight count or the buffered payload
// bytes — answered 429/ResourceExhausted with the payload still in the sender's
// hands. It is what an operator scales a node on. A reclaimed pre-decode
// reservation is not that: the budget had room, and a peer that opened a stream
// and delivered nothing inside the decode window was reaped. Folding the two
// together let one headers-only prober, at zero cost in bytes, drive the metric
// that says this node cannot keep up.
//
// Telling them apart means TWO series, not one condition made invisible: the
// reclaim cancels a peer's stream on a listener nothing authenticates, so
// kubescrape_ingest_reserve_expired_total is asserted here to move on a real
// expiry — a distinguishable cause nothing counts is the same blind spot by a
// different route.

import (
	"context"
	"testing"
	"time"

	"google.golang.org/grpc"

	"github.com/JohanLindvall/kubescrape/internal/obs"
)

func TestReserveWindowExpiryIsNotAnAdmissionRejection(t *testing.T) {
	const window = 300 * time.Millisecond
	// Room for eight full-size reservations, of which the two abandoned
	// streams below take two: nothing here may be refused for want of budget,
	// or the test could not tell the two causes apart.
	s, client, conn := grpcTestServer(t, 8*maxIngestGRPCMessage, window)

	if err := export(t, client); err != nil {
		t.Fatalf("warm-up push: %v", err) // also establishes the connection
	}
	before := obs.IngestRejected.Value()
	expiredBefore := obs.IngestReserveExpired.Value()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for i := range 2 {
		if _, err := conn.NewStream(ctx, &grpc.StreamDesc{ServerStreams: true, ClientStreams: true},
			"/opentelemetry.proto.collector.logs.v1.LogsService/Export"); err != nil {
			t.Fatalf("open stream %d: %v", i, err)
		}
	}

	// Wait for both windows to elapse and the bytes to come back. Polling the
	// METRIC is the point: it is what an operator has, and a reclaim it does not
	// move is a reclaim nobody can see.
	deadline := time.Now().Add(10 * window)
	for obs.IngestReserveExpired.Value()-expiredBefore < 2 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if got := obs.IngestReserveExpired.Value() - expiredBefore; got < 2 {
		t.Fatalf("kubescrape_ingest_reserve_expired_total moved by %v after %v, want the two abandoned "+
			"streams: the reclaim cancels a peer's stream on an unauthenticated listener and must be "+
			"visible to Prometheus, not only to whoever reads the throttled log line", got, 10*window)
	}
	if got := obs.IngestRejected.Value() - before; got != 0 {
		t.Fatalf("kubescrape_ingest_rejected_total moved by %v on reclaimed decode windows; it counts pushes "+
			"refused because an admission bound was reached, and nothing here was refused", got)
	}

	// The receiver is still healthy — the reclaim gave the budget back.
	if err := export(t, client); err != nil {
		t.Fatalf("honest push after the reclaim: %v", err)
	}
	// And the reservations really were the abandoned streams', not the pushes'.
	if got := s.buffer.used.Load(); got != 0 {
		t.Fatalf("%d budget bytes still held after every stream was reaped", got)
	}
}

// The refusal side of the same split: a push shed for want of budget MUST
// count, and must not be silently reclassified along with the expiries.
func TestBudgetRefusalStillCountsAsAnAdmissionRejection(t *testing.T) {
	// One reservation's worth of budget: the second concurrent push is refused
	// by the tap.
	s, client, conn := grpcTestServer(t, maxIngestGRPCMessage, time.Minute)
	if err := export(t, client); err != nil {
		t.Fatalf("warm-up push: %v", err)
	}
	before := obs.IngestRejected.Value()
	expiredBefore := obs.IngestReserveExpired.Value()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// One abandoned stream takes the whole budget (the window is a minute).
	if _, err := conn.NewStream(ctx, &grpc.StreamDesc{ServerStreams: true, ClientStreams: true},
		"/opentelemetry.proto.collector.logs.v1.LogsService/Export"); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for s.buffer.used.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if err := export(t, client); err == nil {
		t.Fatal("a push with no budget left was served")
	}
	if got := obs.IngestRejected.Value() - before; got != 1 {
		t.Fatalf("kubescrape_ingest_rejected_total moved by %v, want 1 for the shed push", got)
	}
	// The split has to hold in both directions, or the two series are one series
	// with two names: a shed push is not a reclaimed decode window.
	if got := obs.IngestReserveExpired.Value() - expiredBefore; got != 0 {
		t.Fatalf("kubescrape_ingest_reserve_expired_total moved by %v on a push shed for want of budget; "+
			"nothing expired here — the window is a minute", got)
	}
}
