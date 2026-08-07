package promscrape

import (
	"slices"
	"strings"
	"time"

	"github.com/JohanLindvall/kubescrape/pkg/kubemeta"
)

// TargetStatus is one target's LAST scrape outcome, exposed on the agent's
// GET /debug/targets for "which targets exist and which are failing, why" —
// the human-readable counterpart of the health metrics.
type TargetStatus struct {
	Pipeline  string `json:"pipeline"` // targets | cadvisor | node
	URL       string `json:"url"`
	Source    string `json:"source,omitempty"`  // pod | service | servicemonitor (annotation targets)
	Monitor   string `json:"monitor,omitempty"` // ns/name of the ServiceMonitor
	Namespace string `json:"namespace,omitempty"`
	Pod       string `json:"pod,omitempty"`
	Up        bool   `json:"up"`
	// Pending marks a discovered target this process has not scraped yet (its
	// first per-target interval has not elapsed). Up is meaningless while set.
	Pending  bool      `json:"pending,omitempty"`
	Error    string    `json:"error,omitempty"`
	Duration string    `json:"duration,omitempty"`
	Samples  int       `json:"samples,omitempty"`
	Scraped  time.Time `json:"scraped,omitzero"`
}

// CycleStatus is the per-target view after the last completed scrape cycle.
type CycleStatus struct {
	Completed time.Time      `json:"completed"`
	Targets   []TargetStatus `json:"targets"`
}

// publishStatus merges the cycle's outcomes into the per-target snapshot
// (failures first, then by URL — the reader is almost always looking for what
// is broken).
//
// MERGES, not replaces: per-target cadences mean a cycle scrapes only what
// was due, so a snapshot of one cycle's outcomes made a 5-minute-interval
// target's status visible for one cycle in ten and invisible — including its
// standing failure — the rest of the time. A target the schedule still knows
// keeps its LAST outcome (Scraped says how old it is), a discovered-but-
// never-scraped target appears as Pending rather than not at all, and a
// target gone from the list is dropped — unless the list itself failed to
// fetch (targetsOK false), where dropping everything would turn a metadata-
// service blip into an empty debug page precisely while someone is looking.
func (s *Scraper) publishStatus(outcomes []scrapeOutcome, targets []kubemeta.ScrapeTarget, targetsOK bool, completed time.Time) {
	key := func(pipeline, url string) string { return pipeline + "|" + url }
	prev := map[string]TargetStatus{}
	if old := s.status.Load(); old != nil {
		for _, ts := range old.Targets {
			prev[key(ts.Pipeline, ts.URL)] = ts
		}
	}

	st := &CycleStatus{Completed: completed, Targets: make([]TargetStatus, 0, len(outcomes)+len(targets))}
	seen := map[string]bool{}
	for _, o := range outcomes {
		ts := TargetStatus{
			Pipeline: o.pipeline,
			URL:      o.url,
			Up:       o.ok,
			Error:    o.err,
			Duration: o.duration.Round(time.Millisecond).String(),
			Samples:  o.samples,
			Scraped:  completed,
		}
		if o.target != nil {
			ts.Source = o.target.Source
			ts.Monitor = o.target.Monitor
			ts.Namespace = o.target.Pod.Namespace
			ts.Pod = o.target.Pod.Name
		}
		seen[key(o.pipeline, o.url)] = true
		st.Targets = append(st.Targets, ts)
	}

	// Current-but-not-scraped-this-cycle targets: last outcome, or Pending.
	for i := range targets {
		t := &targets[i]
		k := key("targets", t.URL)
		if seen[k] {
			continue
		}
		seen[k] = true
		if ts, ok := prev[k]; ok {
			st.Targets = append(st.Targets, ts)
			continue
		}
		st.Targets = append(st.Targets, TargetStatus{
			Pipeline:  "targets",
			URL:       t.URL,
			Source:    t.Source,
			Monitor:   t.Monitor,
			Namespace: t.Pod.Namespace,
			Pod:       t.Pod.Name,
			Pending:   true,
		})
	}

	// Everything else the previous snapshot knew: the kubelet pipelines
	// (whose endpoints are fixed while enabled, and which scrape on their own
	// cadence), plus — only when this cycle could not fetch the list — the
	// annotation/monitor targets the failure hid.
	for k, ts := range prev {
		if seen[k] {
			continue
		}
		if ts.Pipeline != "targets" || !targetsOK {
			st.Targets = append(st.Targets, ts)
		}
	}

	sortTargets(st.Targets)
	s.status.Store(st)
}

func sortTargets(ts []TargetStatus) {
	// Failures first, then pending, then by URL.
	slices.SortFunc(ts, func(a, b TargetStatus) int {
		aDown, bDown := !a.Up && !a.Pending, !b.Up && !b.Pending
		if aDown != bDown {
			if aDown {
				return -1
			}
			return 1
		}
		if a.Pending != b.Pending {
			if a.Pending {
				return -1
			}
			return 1
		}
		return strings.Compare(a.URL, b.URL)
	})
}

// Status returns the merged per-target outcomes (nil before the first cycle
// finishes).
func (s *Scraper) Status() *CycleStatus {
	return s.status.Load()
}
