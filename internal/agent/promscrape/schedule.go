package promscrape

// Per-target SCHEDULING: the loop's tick (Run, at the finest cadence any target
// asks for), each target's resolved interval and timeout, and the due times
// cycle() consults — the three kubelet scrapes included, under keys that cannot
// collide with a target's.

import (
	"context"
	"time"

	"github.com/JohanLindvall/kubescrape/internal/promdur"
	"github.com/JohanLindvall/kubescrape/pkg/kubemeta"
)

// Run scrapes until ctx is done; the first cycle starts immediately.
//
// The loop ticks at the FINEST cadence any target asks for (never longer than
// max(-scrape-interval, 1s), the 1s being tickInterval's floor): a monitor
// endpoint may set its own `interval`, and each target is scraped only when its
// own period has elapsed (see cycle). With no per-target intervals and a
// -scrape-interval of at least a second this is exactly a -scrape-interval
// ticker. A non-positive -scrape-interval runs no cycle at all and returns
// when ctx is done.
func (s *Scraper) Run(ctx context.Context) {
	tick := s.cfg.Interval
	if tick <= 0 {
		// Same hazard as the log-metrics loop: time.NewTicker panics on a
		// non-positive duration and this comes straight from -scrape-interval.
		// A zero scrape interval means "never", not "crash".
		s.log.Warn("scrape interval is not positive; the scrape loop will not run", "interval", tick)
		<-ctx.Done()
		return
	}
	ticker := time.NewTicker(tick)
	defer ticker.Stop()
	for {
		s.cycle(ctx)
		if want := s.tickInterval(); want != tick {
			tick = want
			ticker.Reset(tick)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// tickInterval is the loop period: the smallest interval any target requested,
// floored at 1s so a nonsense CR cannot spin the scraper.
func (s *Scraper) tickInterval() time.Duration {
	s.dueMu.Lock()
	defer s.dueMu.Unlock()
	out := s.cfg.Interval
	for _, iv := range s.targetIntervals {
		if iv > 0 && iv < out {
			out = iv
		}
	}
	return max(out, time.Second)
}

// parseTargetDuration parses one monitor-supplied duration field (kind names
// it in the warn key, attr in the log line). ok is false for an unparseable or
// non-positive value, which is reported once and left to the caller's fallback
// rather than failing the target — the CR is the user's, and dropping their
// metrics over a typo in an optional field is worse than scraping at the
// default cadence.
//
// The offending VALUE rides on the line but is NOT part of the key: see
// warnOnce. It used to be, so that re-introducing a different typo warned
// again — a real but small diagnostic gain, paid for by letting anyone who can
// edit one ServiceMonitor mint an unbounded number of never-expiring keys (one
// per edit) and permanently saturate the table every other complaint in this
// package shares. One line per (field, target) is what the operator needs; the
// second typo on the same field of the same monitor is now silent until a
// restart, which is the cheap half of that trade.
func (s *Scraper) parseTargetDuration(t kubemeta.ScrapeTarget, kind, attr, value string) (time.Duration, bool) {
	// promdur is the shared prometheus-operator duration parser, and Interval
	// its usability rule (parsed and positive): the metadata service's monitor
	// merge judges the same values through it. Warning once and falling back
	// is THIS caller's reaction to an unusable value, not the rule.
	d, ok := promdur.Interval(value)
	if !ok {
		s.warnOnce(kind+":"+warnTarget(t), "ignoring invalid scrape "+kind+" on target",
			"url", t.URL, "monitor", t.Monitor, attr, clipForLog(value))
		return 0, false
	}
	return d, true
}

// targetInterval resolves a target's effective scrape period: its own
// `interval` when the monitor set a valid one, else the agent's default.
func (s *Scraper) targetInterval(t kubemeta.ScrapeTarget) time.Duration {
	if t.Interval == "" {
		return s.cfg.Interval
	}
	if d, ok := s.parseTargetDuration(t, "interval", "interval", t.Interval); ok {
		return d
	}
	return s.cfg.Interval
}

// targetTimeout resolves a target's per-scrape timeout, clamped to its own
// interval: a scrape outliving its period would overlap the next one.
func (s *Scraper) targetTimeout(t kubemeta.ScrapeTarget, interval time.Duration) time.Duration {
	out := s.cfg.Timeout
	asked := time.Duration(0)
	if t.ScrapeTimeout != "" {
		if d, ok := s.parseTargetDuration(t, "timeout", "scrapeTimeout", t.ScrapeTimeout); ok {
			out, asked = d, d
		}
	}
	// Clamped to the target's interval AND the agent's own: cycle() waits for
	// every scrape it started, and Run only ticks after cycle returns, so one
	// target's long timeout stalls the whole node's scrape loop.
	//
	// The agent's own interval is the constraint a user cannot see from their
	// CR: a monitor at `interval: 5m, scrapeTimeout: 2m` is entirely
	// self-consistent, yet the timeout still collapses to -scrape-interval
	// (30s by default) and a slow exporter is cut off. That is a real
	// limitation of the synchronous cycle, not a misconfiguration — so say so
	// once instead of quietly handing back a fifth of what was asked for.
	got := min(out, interval, s.cfg.Interval)
	if asked > 0 && got < asked {
		// The key is the target's identity alone; the value it asked for is a
		// PARSED duration by now, and rides as an attribute.
		s.warnOnce("timeoutclamp:"+warnTarget(t), "scrape timeout clamped below the monitor's scrapeTimeout; raise -scrape-interval to allow a longer one",
			"url", t.URL, "monitor", t.Monitor,
			"scrapeTimeout", asked, "effective", got, "scrapeInterval", s.cfg.Interval)
	}
	return got
}

// dueNow reports whether the scheduled key is due at now, and records its next
// due time in due — this cycle's schedule, committed by setSchedule or
// setKubeletSchedule. The clock is the caller's (cycle reads it once for the
// kubelet scrapes and once, after the target fetch, for the targets), so each
// of the three rules below is pinned by a test with an explicit now rather than
// by sleeping on the wall clock.
func (s *Scraper) dueNow(due map[string]time.Time, now time.Time, key string, iv time.Duration) bool {
	next, scheduled := s.dueAt(key)
	if scheduled && now.Add(iv/10).Before(next) {
		// Not due: the iv/10 slack absorbs a tick landing slightly early.
		//
		// The carried time was computed from the interval in force when it
		// was set, so a SHORTENED one must pull it in: an endpoint edited
		// from `interval: 2h` to 30s — or a deleted monitor whose target
		// falls back to -scrape-interval — keeps the same schedule key, and
		// nothing else clamps the stored time, so the target went unscraped
		// for the residual 2h while every cycle re-committed it. Lengthening
		// needs no counterpart: it applies at the next due time.
		if n := now.Add(iv); next.After(n) {
			next = n
		}
		due[key] = next
		return false
	}
	if scheduled {
		// Advance from the DUE time, not from now: the tick may land
		// slightly early (iv/10), and folding that slack in every round
		// drifted long-interval targets ~10% faster, permanently.
		if n := next.Add(iv); n.After(now) {
			due[key] = n
		} else {
			due[key] = now.Add(iv) // fell far behind: resynchronise
		}
	} else {
		due[key] = now.Add(iv)
	}
	return true
}

// dueAt returns a target's next scheduled scrape time.
func (s *Scraper) dueAt(url string) (time.Time, bool) {
	s.dueMu.Lock()
	defer s.dueMu.Unlock()
	t, ok := s.due[url]
	return t, ok
}

// setSchedule replaces the per-target schedule with this cycle's.
func (s *Scraper) setSchedule(due map[string]time.Time, intervals map[string]time.Duration) {
	s.dueMu.Lock()
	defer s.dueMu.Unlock()
	s.due, s.targetIntervals = due, intervals
}

// setKubeletSchedule commits only the kubelet entries of a cycle whose target
// list could not be fetched, leaving the target schedule (and the intervals
// derived from it) untouched for the next successful cycle.
func (s *Scraper) setKubeletSchedule(due map[string]time.Time) {
	s.dueMu.Lock()
	defer s.dueMu.Unlock()
	if s.due == nil {
		// The very first cycle can be a failing one — an agent normally starts
		// before the metadata service is reachable — so the schedule map may
		// not exist yet.
		s.due = make(map[string]time.Time, len(kubeletDueKeys))
	}
	for _, k := range kubeletDueKeys {
		if when, ok := due[k]; ok {
			s.due[k] = when
		}
	}
}

// The schedule keys of the three kubelet scrapes, NUL-prefixed so they cannot
// collide with a target URL. cycle() names them individually and
// kubeletDueKeys is what setKubeletSchedule commits: an index into the slice
// would let a fourth scrape (or a removed one) shift the entries under a call
// site the compiler cannot check — which is a panic on a path that runs once
// per cycle on every node.
const (
	dueKeyCadvisor = "\x00cadvisor"
	dueKeyNode     = "\x00node"
	dueKeySummary  = "\x00summary"
)

var kubeletDueKeys = []string{dueKeyCadvisor, dueKeyNode, dueKeySummary}

// scheduleKey identifies one target's schedule slot. The URL alone does NOT:
// the metadata service dedupes same-URL targets only WITHIN a pod, so two
// hostNetwork pods sharing the node's IP with the same annotated port yield
// identical URLs with different pod documents (the case its own sort comment
// names). Sharing a slot let the cycle's last-processed duplicate overwrite the
// other's due time and interval — a 10s endpoint on one collapsed the other
// onto its cadence, or was itself coarsened to the default.
//
// The key always carries all three separators, so it cannot collide with the
// NUL-prefixed kubelet keys even for a target with an empty URL.
func scheduleKey(t kubemeta.ScrapeTarget) string {
	return t.URL + "\x00" + t.Pod.UID + "\x00" + t.Source + "\x00" + t.Monitor
}
