package promscrape

import (
	"context"
	"testing"
	"time"
)

// A monitor endpoint's own `interval` must be honoured. Collapsing every
// monitor onto one global -scrape-interval silently coarsens targets that
// asked for 10s and multiplies the sample bill of targets that asked for 5m.
func TestPerTargetIntervalRespected(t *testing.T) {
	srv := serveBody(t, "m 1\n")
	exp := &captureExporter{}

	slow := testTarget(srv.URL)
	slow.Interval = "1h" // must be scraped once, then skipped
	fast := testTarget(srv.URL + "/fast")

	s := New(Config{
		Node: "n1", Interval: time.Minute, Timeout: 5 * time.Second,
		Targets: staticTargets{slow}, Exporter: exp, StartTime: time.Now(),
	})
	s.cycle(context.Background())
	s.cycle(context.Background())
	if got := exp.points(); got != 1 {
		t.Fatalf("points = %d, want 1: a target with interval=1h must not be re-scraped in the next cycle", got)
	}

	// A target with NO interval follows the AGENT's interval — it is scheduled
	// like every other target. Leaving such targets unscheduled meant a single
	// monitor asking for 10s re-clocked the whole node, because Run ticks at
	// the finest requested cadence.
	exp2 := &captureExporter{}
	s2 := New(Config{
		Node: "n1", Interval: time.Minute, Timeout: 5 * time.Second,
		Targets: staticTargets{testTarget(srv.URL)}, Exporter: exp2, StartTime: time.Now(),
	})
	s2.cycle(context.Background())
	s2.cycle(context.Background())
	if got := exp2.points(); got != 1 {
		t.Fatalf("points = %d, want 1: a target with no interval follows -scrape-interval, not the tick rate", got)
	}
	_ = fast
}

// An unparseable interval falls back to the default rather than dropping the
// target: the CR is the user's, and losing their metrics over a typo in an
// optional field is worse than scraping at the default cadence.
func TestInvalidIntervalFallsBack(t *testing.T) {
	srv := serveBody(t, "m 1\n")
	exp := &captureExporter{}
	tgt := testTarget(srv.URL)
	tgt.Interval = "10 pancakes"

	s := New(Config{
		Node: "n1", Interval: time.Minute, Timeout: 5 * time.Second,
		Targets: staticTargets{tgt}, Exporter: exp, StartTime: time.Now(),
	})
	s.cycle(context.Background())
	if got := exp.points(); got != 1 {
		t.Fatalf("points = %d, want 1: an invalid interval must not drop the target", got)
	}
}

// A per-target scrapeTimeout is clamped to the target's own interval: a scrape
// outliving its period would overlap the next one.
func TestTimeoutClampedToInterval(t *testing.T) {
	s := New(Config{Node: "n1", Interval: time.Minute, Timeout: 30 * time.Second})
	tgt := testTarget("http://x/metrics")
	tgt.ScrapeTimeout = "45s"
	if got := s.targetTimeout(tgt, 10*time.Second); got != 10*time.Second {
		t.Fatalf("timeout = %v, want it clamped to the 10s interval", got)
	}
	if got := s.targetTimeout(tgt, time.Minute); got != 45*time.Second {
		t.Fatalf("timeout = %v, want the target's own 45s", got)
	}
}
