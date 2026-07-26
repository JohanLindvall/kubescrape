package promscrape

import (
	"context"
	"testing"
	"time"
)

// One target's short interval must not re-clock every OTHER target: Run ticks
// at the finest requested cadence, and targets without their own interval are
// scraped on every tick.
func TestProbeTickContagion(t *testing.T) {
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
		t.Logf("tickInterval before any cycle = %v", got)
	}
	// Drive 12 cycles, as the 5s ticker would inside one configured minute.
	for range 12 {
		s.cycle(context.Background())
	}
	t.Logf("tickInterval after cycles = %v", s.tickInterval())
	t.Logf("total exported points = %d (fast target legitimately ~1, plain should be ~1 per cfg.Interval)", exp.points())
	if exp.points() > 4 {
		t.Fatalf("%d points: a target with no interval was scraped on every tick, so one 5s monitor re-clocked the whole node", exp.points())
	}
}
