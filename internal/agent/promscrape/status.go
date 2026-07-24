package promscrape

import (
	"slices"
	"strings"
	"time"
)

// TargetStatus is one scrape's outcome in the last completed cycle, exposed
// on the agent's GET /debug/targets for "which targets exist and which are
// failing, why" — the human-readable counterpart of the health metrics.
type TargetStatus struct {
	Pipeline  string    `json:"pipeline"` // targets | cadvisor | node
	URL       string    `json:"url"`
	Source    string    `json:"source,omitempty"`  // pod | service | servicemonitor (annotation targets)
	Monitor   string    `json:"monitor,omitempty"` // ns/name of the ServiceMonitor
	Namespace string    `json:"namespace,omitempty"`
	Pod       string    `json:"pod,omitempty"`
	Up        bool      `json:"up"`
	Error     string    `json:"error,omitempty"`
	Duration  string    `json:"duration"`
	Samples   int       `json:"samples"`
	Scraped   time.Time `json:"scraped"`
}

// CycleStatus is the last completed scrape cycle.
type CycleStatus struct {
	Completed time.Time      `json:"completed"`
	Targets   []TargetStatus `json:"targets"`
}

// publishStatus snapshots the cycle's outcomes (failures first, then by URL —
// the reader is almost always looking for what is broken).
func (s *Scraper) publishStatus(outcomes []scrapeOutcome, completed time.Time) {
	st := &CycleStatus{Completed: completed, Targets: make([]TargetStatus, 0, len(outcomes))}
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
		st.Targets = append(st.Targets, ts)
	}
	sortTargets(st.Targets)
	s.status.Store(st)
}

func sortTargets(ts []TargetStatus) {
	// Failures first, then by URL.
	slices.SortFunc(ts, func(a, b TargetStatus) int {
		if a.Up != b.Up {
			if a.Up {
				return 1
			}
			return -1
		}
		return strings.Compare(a.URL, b.URL)
	})
}

// Status returns the last completed cycle's per-target outcomes (nil before
// the first cycle finishes).
func (s *Scraper) Status() *CycleStatus {
	return s.status.Load()
}
