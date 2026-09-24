package promscrape

// What a scrape cycle reports that is NOT a scrape failure (failures.go owns
// those): the discovered-target census and its empty-set warning, a target
// negotiating a format other than the one asked for, and a bounded cache
// running at its cap. (A cadvisor row the metadata service could not place is
// reported beside its one caller, in cadvisorbatch.go.)
// Each exists because the condition it names is otherwise an ABSENCE — no
// scrape, no exemplars, no labels, no counter moving — that nothing else in the
// package would make visible.

import (
	"context"
	"log/slog"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/JohanLindvall/kubescrape/internal/logdedupe"
	"github.com/JohanLindvall/kubescrape/internal/obs"
	"github.com/JohanLindvall/kubescrape/pkg/kubemeta"
)

// emptyTargetsWarnEvery re-states "this node has no scrape targets" at this
// cadence while it stays true. Longer than scrapeFailWarnEvery because it is a
// STANDING condition an operator diagnoses once, not an incident that changes.
const emptyTargetsWarnEvery = 30 * time.Minute

// reportTargetSet publishes the discovered-target count and says something when
// it is interesting: the set changing size (Debug), and the set being EMPTY
// (Warn on the transition, re-warned on a window, Info on recovery).
//
// The empty list is the most common first-run failure and the one that produces
// no other evidence at all — no scrape runs, so no scrape fails, so every
// counter in this package stays flat and /debug/targets is a blank page that
// looks exactly like a healthy agent whose targets happen to be elsewhere. The
// transition/re-warn/recovery shape is cmd/kubescrape's api-server watchdog's.
//
// Called only from a cycle that FETCHED the list: a failed fetch has its own
// Error line, and treating its empty slice as "no targets exist" would blame
// discovery for a metadata-service outage.
func (s *Scraper) reportTargetSet(targets []kubemeta.ScrapeTarget) {
	n := len(targets)
	obs.ScrapeTargets.Set(float64(n))

	if s.lastTargetsSet && n != s.lastTargets && s.log.Enabled(context.Background(), slog.LevelDebug) {
		// Guarded: the counts are field reads, but the by-source breakdown
		// walks the list, and this runs once per cycle on every node.
		s.log.Debug("scrape target set changed",
			"node", s.cfg.Node, "targets", n, "previous", s.lastTargets, "bySource", targetSources(targets))
	}
	s.lastTargets, s.lastTargetsSet = n, true

	switch {
	case n == 0:
		if !s.emptyTargets {
			s.emptyTargets = true
			s.emptyTargetWarn = logdedupe.Throttle{} // a fresh outage says so at once
		}
		if !s.emptyTargetWarn.Allow(emptyTargetsWarnEvery) {
			return
		}
		s.log.Warn("the metadata service returned NO scrape targets for this node, so nothing is being scraped from pods or Services here",
			"node", s.cfg.Node,
			"note", "annotate a pod or Service with prometheus.io/scrape=true, or check that the ServiceMonitor/PodMonitor selects a pod on THIS node and that the metadata service runs -servicemonitors; GET /v1/explain/{namespace}/{pod} on the metadata service says why one pod is not a target")
	case s.emptyTargets:
		s.emptyTargets = false
		s.log.Info("scrape targets discovered for this node", "node", s.cfg.Node, "targets", n)
	}
}

// targetSources is the by-source census the empty/changed report carries: pod
// and service annotations, ServiceMonitors and PodMonitors are discovered by
// three different mechanisms, and which of them produced nothing is most of the
// diagnosis. Built only under a Debug-enabled guard.
func targetSources(targets []kubemeta.ScrapeTarget) string {
	counts := map[string]int{}
	for i := range targets {
		src := targets[i].Source
		if src == "" {
			src = "unknown"
		}
		counts[src]++
	}
	keys := make([]string, 0, len(counts))
	for k := range counts {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	var b strings.Builder
	for i, k := range keys {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(k)
		b.WriteByte('=')
		b.WriteString(strconv.Itoa(counts[k]))
	}
	return b.String()
}

// reportNegotiation says, at Debug, when a target served a format other than
// the one this agent asked for. Nothing FAILS here — the parser reads whatever
// arrived — but the consequence is invisible otherwise: with -scrape-exemplars
// on, a target that answers text/plain simply produces no exemplars, forever,
// and the only evidence is an absence.
//
// One Content-Type header read per scrape, so no Enabled guard is needed; the
// call is skipped entirely once the levels agree.
//
// The header is the TARGET's bytes, bounded only by whatever the transport
// accepted, so it goes on the line through clipForLog — the same rule the
// protobuf refusal beside it follows.
func (s *Scraper) reportNegotiation(t kubemeta.ScrapeTarget, askedProto bool, contentType string, openMetrics bool) {
	if !s.log.Enabled(context.Background(), slog.LevelDebug) {
		return
	}
	switch {
	case askedProto && !strings.Contains(contentType, protoContentType) && !openMetrics:
		s.log.Debug("target negotiated down from the protobuf exposition; native histograms will not be present",
			"url", t.URL, "monitor", t.Monitor, "contentType", clipForLog(contentType))
	case !askedProto && s.cfg.Exemplars && !openMetrics:
		s.log.Debug("target served classic text although OpenMetrics was offered; no exemplars will be scraped from it",
			"url", t.URL, "monitor", t.Monitor, "contentType", clipForLog(contentType))
	}
}

// cacheEvictWarnEvery throttles a bounded-cache eviction notice. Each cache
// carries its OWN gate (Scraper.tlsEvictWarn / relabelEvictWarn) — the caches
// thrash for different reasons and want different remedies, and a shared
// keyless throttle would let the first condition silence the second.
const cacheEvictWarnEvery = 30 * time.Minute

// warnCacheEviction reports a bounded per-target cache running at its cap.
// Nothing is LOST — the entry is rebuilt on demand — which is exactly why it
// needs saying: the symptom is a scrape that gets slower and a node that opens
// a connection per target per cycle, with every counter in this package flat.
//
// gate is the caller's per-cache throttle. Callers must invoke this OUTSIDE the
// lock guarding the cache: rendering and writing a slog record is I/O, and the
// scrape goroutines all contend for that lock.
func (s *Scraper) warnCacheEviction(gate *logdedupe.Throttle, what string, entries int, note string) {
	if !gate.Allow(cacheEvictWarnEvery) {
		return
	}
	s.log.Warn("a bounded scrape cache is at its cap and is evicting; the entries are rebuilt on demand, so this costs work rather than data",
		"cache", what, "entries", entries, "note", note)
}
