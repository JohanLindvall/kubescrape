package tracesample

import (
	"context"
	"testing"
	"time"
)

// A fractional maxSpansPerSecond must throttle, not black-hole. allow() caps
// tokens at burst and requires a whole token, so a burst equal to a rate below
// 1 could never accumulate one: every span was dropped forever — a total trace
// outage rather than the configured cap.
func TestFractionalRateCapStillForwards(t *testing.T) {
	next := &capExporter{}
	s := New(Config{MaxSpansPerSecond: 0.5}, next)
	now := time.Unix(0, 0)
	s.now = func() time.Time { return now }

	// First call primes s.last (no refill has elapsed yet); the initial burst
	// is one whole token, so exactly one span passes.
	if err := s.ExportTraces(context.Background(), payload(10, false, 0)); err != nil {
		t.Fatal(err)
	}
	first := next.spans()

	// Two seconds at 0.5/s refills one more token.
	now = now.Add(2 * time.Second)
	if err := s.ExportTraces(context.Background(), payload(10, false, 0)); err != nil {
		t.Fatal(err)
	}
	total := next.spans()

	if total == 0 {
		t.Fatalf("no spans forwarded at 0.5 spans/s: a fractional rate drops 100%% forever")
	}
	if total <= first {
		t.Fatalf("no refill at 0.5 spans/s: %d spans after 2s, %d before", total, first)
	}
	if total > 4 {
		t.Fatalf("forwarded %d spans, far above the 0.5/s cap", total)
	}
}
