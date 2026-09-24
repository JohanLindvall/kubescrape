package promscrape

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/JohanLindvall/kubescrape/pkg/kubemeta"
)

// One target's short interval must not re-clock every OTHER target: Run ticks
// at the finest requested cadence, and targets without their own interval are
// scraped on every tick.
func TestShortIntervalDoesNotReclockOtherTargets(t *testing.T) {
	srv := serveBody(t, "m 1\n")
	fast := testTarget(srv.URL)
	fast.Interval = "5s"
	plain := testTarget(srv.URL + "/plain")

	exp := &captureExporter{}
	s := New(Config{
		Node: "n1", Interval: time.Minute, Timeout: 5 * time.Second,
		Targets: staticTargets{fast, plain}, Exporter: exp, StartTime: time.Now(),
	})
	if got := s.tickInterval(); got != time.Minute {
		t.Fatalf("tickInterval before any cycle = %v, want -scrape-interval (1m): nothing has asked for a finer cadence yet", got)
	}
	// Drive 12 cycles, as the 5s ticker would inside one configured minute.
	for range 12 {
		s.cycle(context.Background())
	}
	if got := s.tickInterval(); got != 5*time.Second {
		t.Fatalf("tickInterval after the cycles = %v, want the 5s an explicit interval asked for", got)
	}
	if exp.points() > 4 {
		t.Fatalf("%d points: a target with no interval was scraped on every tick, so one 5s monitor re-clocked the whole node", exp.points())
	}
}

// The tick has a one-second FLOOR: a monitor's `interval: 100ms` (or a typo of
// one) may speed up its own target, but it must not spin Run's loop — and with
// it the target-list fetch and the kubelet due checks — ten times a second.
func TestTickIntervalHasAOneSecondFloor(t *testing.T) {
	srv := serveBody(t, "m 1\n")
	tgt := testTarget(srv.URL)
	tgt.Interval = "100ms"
	s := New(Config{
		Node: "n1", Interval: time.Minute, Timeout: 5 * time.Second,
		Targets: staticTargets{tgt}, Exporter: &captureExporter{}, StartTime: time.Now(),
	})
	s.cycle(context.Background())
	if got := s.tickInterval(); got != time.Second {
		t.Fatalf("tickInterval = %v for a 100ms target, want the 1s floor", got)
	}
}

// A non-positive -scrape-interval must not reach time.NewTicker, which panics
// on it — and cmd/kubescrape-agent does not refuse the flag, so Run's guard is
// the only thing between an operator's `-scrape-interval=0` and a crash-looping
// DaemonSet. It means "never": no cycle runs, and Run still returns on cancel.
func TestRunWithNonPositiveIntervalReturnsOnCancel(t *testing.T) {
	for _, iv := range []time.Duration{0, -time.Second} {
		t.Run(iv.String(), func(t *testing.T) {
			src := &countingTargets{}
			s := New(Config{
				Node: "n1", Interval: iv, Timeout: time.Second,
				Targets: src, Exporter: &captureExporter{}, StartTime: time.Now(),
			})
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan struct{})
			go func() {
				defer close(done)
				s.Run(ctx)
			}()
			cancel()
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Fatal("Run did not return after its context was cancelled")
			}
			if n := src.calls.Load(); n != 0 {
				t.Errorf("%d target fetches with -scrape-interval=%v: a non-positive interval must run no cycle", n, iv)
			}
		})
	}
}

// countingTargets serves an empty list and counts the fetches.
type countingTargets struct{ calls atomic.Int64 }

func (c *countingTargets) NodeTargets(context.Context, string) ([]kubemeta.ScrapeTarget, error) {
	c.calls.Add(1)
	return nil, nil
}
