package promscrape

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/JohanLindvall/kubescrape/pkg/kubemeta"
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

// countingHandler is a slog handler that just tallies records, so a test can
// assert on how often a warnOnce fired.
type countingHandler struct{ n int }

func (h *countingHandler) Enabled(context.Context, slog.Level) bool  { return true }
func (h *countingHandler) Handle(context.Context, slog.Record) error { h.n++; return nil }
func (h *countingHandler) WithAttrs([]slog.Attr) slog.Handler        { return h }
func (h *countingHandler) WithGroup(string) slog.Handler             { return h }

// The per-target warning dedupe must key on the CONFIGURATION, not the URL: the
// URL carries the pod IP, so every pod restart used to re-fire the "once"
// warning AND leak another map entry for the process' whole life.
func TestWarnOnceSurvivesPodRestarts(t *testing.T) {
	h := &countingHandler{}
	s := &Scraper{cfg: Config{Interval: time.Minute, Timeout: 5 * time.Second}, log: slog.New(h)}

	// The same monitor endpoint, re-resolved across 500 pod incarnations: a new
	// pod IP (hence URL) and a new pod name each time, same broken CR field.
	for i := 0; i < 500; i++ {
		tgt := testTarget(fmt.Sprintf("http://10.4.%d.%d:9090/metrics", i/256, i%256))
		tgt.Pod.Name = fmt.Sprintf("dep1-7f9c4b6d5-%05x", i)
		tgt.Pod.UID = fmt.Sprintf("uid-%d", i)
		tgt.Source, tgt.Monitor = "servicemonitor", "monitoring/api"
		tgt.Interval = "10 pancakes"
		if got := s.targetInterval(tgt); got != time.Minute {
			t.Fatalf("targetInterval = %v, want the default", got)
		}
	}
	if h.n != 1 {
		t.Errorf("logged %d warnings across 500 pod incarnations of one monitor, want 1", h.n)
	}
	if s.warned.Len() != 1 {
		t.Errorf("dedupe table holds %d keys, want 1: it grows per pod incarnation", s.warned.Len())
	}

	// A genuinely different problem still gets through: another monitor, and
	// the same monitor with a different bad value.
	other := testTarget("http://10.9.9.9:9090/metrics")
	other.Source, other.Monitor, other.Interval = "servicemonitor", "monitoring/other", "10 pancakes"
	s.targetInterval(other)
	same := testTarget("http://10.9.9.9:9090/metrics")
	same.Source, same.Monitor, same.Interval = "servicemonitor", "monitoring/api", "12 waffles"
	s.targetInterval(same)
	if h.n != 3 {
		t.Errorf("logged %d warnings, want 3: a new monitor and a new bad value are each a new problem", h.n)
	}
}

// A pod-annotation target keys on its WORKLOAD, so a Deployment rollout (new
// ReplicaSet, new pod names, new IPs) is still one warning.
func TestWarnTargetIsStableAcrossRollouts(t *testing.T) {
	a := testTarget("http://10.4.0.1:9090/metrics")
	a.Source, a.Service = "pod", nil
	a.Pod.Name, a.Pod.Owners = "web-6f7b9c-aaaaa", []kubemeta.Owner{
		{Kind: "ReplicaSet", Name: "web-6f7b9c"}, {Kind: "Deployment", Name: "web"},
	}
	b := testTarget("http://10.4.7.9:9090/metrics")
	b.Source, b.Service = "pod", nil
	b.Pod.Name, b.Pod.Owners = "web-11ee22-zzzzz", []kubemeta.Owner{
		{Kind: "ReplicaSet", Name: "web-11ee22"}, {Kind: "Deployment", Name: "web"},
	}
	if warnTarget(a) != warnTarget(b) {
		t.Errorf("warnTarget differs across a rollout: %q vs %q", warnTarget(a), warnTarget(b))
	}
	// A different workload in the same namespace must NOT collide.
	c := b
	c.Pod.Owners = []kubemeta.Owner{{Kind: "Deployment", Name: "api"}}
	if warnTarget(c) == warnTarget(b) {
		t.Errorf("warnTarget collides across workloads: %q", warnTarget(c))
	}
}

// The dedupe table is bounded, so a pathological generator cannot turn a
// diagnostic into an unbounded leak.
func TestWarnOnceTableIsBounded(t *testing.T) {
	s := &Scraper{cfg: Config{Interval: time.Minute}, log: slog.New(&countingHandler{})}
	for i := 0; i < maxWarnKeys*3; i++ {
		s.warnOnce(fmt.Sprintf("k%d", i), "msg")
	}
	if s.warned.Len() > maxWarnKeys {
		t.Errorf("dedupe table holds %d keys, want <= %d", s.warned.Len(), maxWarnKeys)
	}
	if s.warned.Len() == 0 {
		t.Error("dedupe table emptied itself: every warning would re-fire every cycle")
	}
}

// The bound must not turn into a warning STORM. Clearing the table at the cap
// looks bounded ("re-warns once per refill") but is not: with cap+1 distinct
// keys the table re-fills, overflows and clears on every scrape cycle, so all
// of them print every cycle forever — worse than the unbounded map. Reaching
// the cap suppresses instead, and says so once.
func TestWarnOnceDoesNotStormAtTheCap(t *testing.T) {
	h := &countingHandler{}
	s := &Scraper{cfg: Config{Interval: time.Minute}, log: slog.New(h)}

	// Fill past the cap, then replay the same key set over several "cycles".
	for cycle := 0; cycle < 4; cycle++ {
		for i := 0; i < maxWarnKeys+1; i++ {
			s.warnOnce(fmt.Sprintf("k%d", i), "msg")
		}
	}
	// At most the cap's worth of distinct warnings plus the one saturation
	// notice; a clearing table emits (maxWarnKeys+1) * 4.
	if h.n > maxWarnKeys+1 {
		t.Errorf("logged %d warnings for %d distinct keys over 4 cycles; the table is re-warning every cycle", h.n, maxWarnKeys+1)
	}
}

// mutableTargets is a TargetSource whose list the test rewrites between
// cycles, standing in for a monitor edit or deletion.
type mutableTargets struct {
	mu   sync.Mutex
	list []kubemeta.ScrapeTarget
}

func (m *mutableTargets) NodeTargets(context.Context, string) ([]kubemeta.ScrapeTarget, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.list, nil
}

func (m *mutableTargets) set(list ...kubemeta.ScrapeTarget) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.list = list
}

// A SHORTENED interval must take effect at the new cadence, not at the due time
// the OLD one scheduled. The schedule key is stable across the edit, so nothing
// but this clamp pulls the stored time in: an endpoint edited from `interval:
// 2h` to 30s — or a deleted monitor whose target falls back to -scrape-interval
// — went unscraped for the whole residual 2h while every cycle re-committed the
// stale time, recoverable only by restarting the agent.
func TestShortenedIntervalPullsInTheDueTime(t *testing.T) {
	srv := serveBody(t, "m 1\n")
	exp := &captureExporter{}
	slow := testTarget(srv.URL)
	slow.Interval = "2h"
	src := &mutableTargets{list: []kubemeta.ScrapeTarget{slow}}

	s := New(Config{
		Node: "n1", Interval: time.Minute, Timeout: 5 * time.Second,
		Targets: src, Exporter: exp, StartTime: time.Now(),
	})
	s.cycle(context.Background()) // scraped once, next due in 2h

	fast := slow
	fast.Interval = "30s"
	src.set(fast)
	s.cycle(context.Background())

	when, ok := s.dueAt(scheduleKey(fast))
	if !ok {
		t.Fatal("the target lost its schedule slot across the edit")
	}
	if d := time.Until(when); d > 31*time.Second {
		t.Fatalf("next scrape in %v, want at most the new 30s interval", d)
	}
	if got := exp.points(); got != 1 {
		t.Fatalf("points = %d, want 1: the clamp reschedules, it does not scrape early", got)
	}
}

// Two targets can share a URL: the metadata service dedupes them only per POD,
// so two hostNetwork pods on the node's IP with the same annotated port produce
// identical URLs with different pod documents. Keying the schedule on the URL
// alone gave them one slot, so the last one processed in a cycle overwrote the
// other's due time and interval — one target inherited the other's cadence.
func TestSameURLTargetsScheduleIndependently(t *testing.T) {
	srv := serveBody(t, "m 1\n")
	exp := &captureExporter{}

	fast := testTarget(srv.URL)
	fast.Pod.Name, fast.Pod.UID = "pod-a", "uid-a"
	fast.Interval = "10ms"
	slow := testTarget(srv.URL)
	slow.Pod.Name, slow.Pod.UID = "pod-b", "uid-b"
	slow.Source, slow.Monitor, slow.Interval = "servicemonitor", "monitoring/b", "2h"

	s := New(Config{
		Node: "n1", Interval: time.Minute, Timeout: 5 * time.Second,
		Targets: staticTargets{fast, slow}, Exporter: exp, StartTime: time.Now(),
	})
	s.cycle(context.Background())
	if got := exp.points(); got != 2 {
		t.Fatalf("points = %d, want 2: both same-URL targets are scraped", got)
	}
	if scheduleKey(fast) == scheduleKey(slow) {
		t.Fatal("two targets of different pods share one schedule key")
	}

	time.Sleep(15 * time.Millisecond) // past the fast target's own interval
	s.cycle(context.Background())
	if got := exp.points(); got != 3 {
		t.Fatalf("points = %d, want 3: the 10ms target must not inherit the 2h one's schedule", got)
	}
	when, ok := s.dueAt(scheduleKey(slow))
	if !ok {
		t.Fatal("the 2h target lost its schedule slot")
	}
	if d := time.Until(when); d < time.Hour {
		t.Fatalf("the 2h target is next due in %v: it was re-clocked onto the fast one", d)
	}
}

// A non-positive Timeout is not "no timeout": targetTimeout takes the MINIMUM
// of it and the intervals, so an unset (or -scrape-timeout=0) value produced an
// already-expired context and every target plus both kubelet scrapes failed
// with "context deadline exceeded" on every cycle — total metric loss with
// nothing but per-scrape warn logs to name it.
func TestNonPositiveTimeoutIsDefaulted(t *testing.T) {
	for _, tmo := range []time.Duration{0, -5 * time.Second} {
		srv := serveBody(t, "m 1\n")
		exp := &captureExporter{}
		s := New(Config{
			Node: "n1", Interval: time.Minute, Timeout: tmo,
			Targets: staticTargets{testTarget(srv.URL)}, Exporter: exp, StartTime: time.Now(),
		})
		if s.cfg.Timeout != defaultScrapeTimeout {
			t.Fatalf("Timeout %v was not defaulted: %v", tmo, s.cfg.Timeout)
		}
		if got := s.targetTimeout(testTarget(srv.URL), time.Minute); got <= 0 {
			t.Fatalf("effective timeout = %v for a configured %v", got, tmo)
		}
		s.cycle(context.Background())
		if got := exp.points(); got != 1 {
			t.Fatalf("points = %d with Timeout %v, want 1", got, tmo)
		}
	}
}
