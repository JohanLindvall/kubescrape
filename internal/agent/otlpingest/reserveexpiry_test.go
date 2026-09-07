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
	"math"
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
	before := ingestRejectedTotal()
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
	if got := ingestRejectedTotal() - before; got != 0 {
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
	before := ingestRejectedTotal()
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
	if got := ingestRejectedTotal() - before; got != 1 {
		t.Fatalf("kubescrape_ingest_rejected_total moved by %v, want 1 for the shed push", got)
	}
	// The split has to hold in both directions, or the two series are one series
	// with two names: a shed push is not a reclaimed decode window.
	if got := obs.IngestReserveExpired.Value() - expiredBefore; got != 0 {
		t.Fatalf("kubescrape_ingest_reserve_expired_total moved by %v on a push shed for want of budget; "+
			"nothing expired here — the window is a minute", got)
	}
}

// The window has to carry the message the flag authorised.
//
// grpc-go runs the unary interceptor — the only pre-expiry release — once the
// WHOLE message has been received, so the window spans the entire upload, not
// the decode. The reservation SIZE already scales with -ingest-grpc-max-recv-bytes;
// leaving the window fixed turned that flag into a silent per-byte deadline. A
// tier told to accept 64 MiB messages gave a sender 10s to deliver one — about
// 54 Mbit/s per stream — and reaped every push that could not, under a counter
// and a Warn that both say the peer "delivered no message".
func TestReserveWindowScalesWithTheConfiguredMessageCap(t *testing.T) {
	base := reserveWindowFor(maxIngestGRPCMessage)
	if base != grpcReserveWindow {
		t.Errorf("the default cap changed the default window: %v, want %v", base, grpcReserveWindow)
	}
	// A receiver configured for SMALLER messages keeps the full grace: the flag
	// exists to raise the cap, and shortening the window for a lowered one would
	// reap honest senders for a bound nobody asked to tighten.
	if got := reserveWindowFor(maxIngestGRPCMessage / 8); got != grpcReserveWindow {
		t.Errorf("a lowered message cap shortened the window to %v; the floor is %v", got, grpcReserveWindow)
	}
	if got := reserveWindowFor(0); got != grpcReserveWindow {
		t.Errorf("an unset cap yielded %v, want the default %v", got, grpcReserveWindow)
	}

	// The rate is what is held constant, so 16x the message gets ~16x the time.
	big := 16 * maxIngestGRPCMessage // 64 MiB, the documented reason the flag exists
	got := reserveWindowFor(big)
	if want := 16 * grpcReserveWindow; got < want-time.Second || got > want {
		t.Errorf("reserveWindowFor(%d MiB) = %v, want about %v (the same bit rate as the default)",
			big>>20, got, want)
	}
	// However large the flag, the window stays a BOUND: its whole job is to
	// reclaim a pin a peer would otherwise hold for the process' life.
	if got := reserveWindowFor(1 << 40); got != maxReserveWindow {
		t.Errorf("an absurd cap yielded %v, want the %v ceiling", got, maxReserveWindow)
	}

	// And NewServer actually wires it, which is the half a pure-function test
	// cannot see: the constant used to be assigned straight into the field.
	s := NewServer(ServerConfig{MaxRecvBytes: big})
	if s.reserveWindow != got {
		t.Errorf("NewServer wired a %v window for a %d MiB message cap, want %v",
			s.reserveWindow, big>>20, got)
	}
}

// The scaling must not be reachable through an overflow. The trace tier passes
// math.MaxInt32 for an uncapped internal hop, and an operator may pass anything
// at all; a multiply that wrapped would produce a SHORT window, which is the
// exact failure this scaling exists to remove.
func TestReserveWindowNeverUnderflowsOnAnAbsurdMessageCap(t *testing.T) {
	for _, recv := range []int{math.MaxInt32, math.MaxInt / 2, math.MaxInt} {
		if got := reserveWindowFor(recv); got != maxReserveWindow {
			t.Errorf("reserveWindowFor(%d) = %v, want the %v ceiling", recv, got, maxReserveWindow)
		}
	}
	// And a negative one (an unset field reaches NewServer, not this, but the
	// function must not answer a bound with a nonsense duration either way).
	if got := reserveWindowFor(-1); got != grpcReserveWindow {
		t.Errorf("reserveWindowFor(-1) = %v, want the default %v", got, grpcReserveWindow)
	}
}
