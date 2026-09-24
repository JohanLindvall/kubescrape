package promscrape

// The per-scrape METADATA BUDGET: the bound that stops a hung metadata service
// from costing a scrape the data it has already parsed.
//
// Every enriched pipeline here resolves the objects it describes through the
// metadata service while it converts — the cadvisor batcher per cgroup path,
// the summary batcher per pod and container, the splitters per described
// object. Those lookups ran on the SCRAPE's own context, and nothing bounded
// what share of it they could take. A metadata service that REFUSES a
// connection costs nothing (the dial returns instantly and the object falls
// back to its label identity, which is what kubescrape_summary_unresolved_total
// counts), but one that BLACKHOLES — a dropping firewall, a partition, a
// ClusterIP whose backend is gone — parks each lookup until a deadline, and the
// first deadline available was the scrape's. Measured on a live cluster at
// -scrape-interval=15s, every cycle for 23 consecutive cycles:
//
//	scrape failed pipeline=summary  error="rpc error: code = DeadlineExceeded desc = context deadline exceeded"
//	scrape failed pipeline=cadvisor error="context deadline exceeded"
//	/debug/targets: cadvisor up=False dur=15.007s samples=204, summary up=False dur=15.006s samples=372
//
// The samples counts are the point: both scrapes fetched their payload, parsed
// it, built every data point — and then discarded the lot, because the context
// the EXPORT runs in is the one the lookups had just emptied. The kubelet was
// healthy, the collector was healthy, and the node went dark on both pipelines
// for as long as the partition lasted. The /metrics pipeline beside them, which
// resolves nothing, kept succeeding throughout.
//
// So the lookups get an allowance and the rest of the scrape gets the
// remainder. The shape is otlpingest's lookupWaitBudgetFactor, applied to a
// scrape instead of a push: one budget per scrape, charged by measured ELAPSED
// time rather than by requested waits, no single lookup allowed past what
// remains of it, and past it the lookups stop being issued at all — which is
// precisely the refused case, reached by the clock instead of by an RST. The
// object keeps its label identity, the scrape exports, and
// kubescrape_scrape_metadata_budget_exhausted_total says the allowance bound.
// That counter is not redundant with the ones that already exist: an object
// never asked about cannot move kubescrape_metadata_requests_total, and the
// per-object unresolved counters (kubescrape_cadvisor_unresolved_total,
// kubescrape_summary_unresolved_total) say THAT an object went unplaced, not
// that the allowance is why — a refusing service and a spent allowance move
// them identically, and a splitter's shed objects move neither.
//
// Why an allowance rather than giving the export a context of its own: cycle()
// waits for every scrape it starts and Run only ticks after cycle returns, so a
// scrape whose export outlives the scrape budget stretches the whole node's
// cadence — the same reason kubeletTimeout clamps to -scrape-interval. Carving
// the allowance out of the budget keeps the total where it was and hands the
// conversion and the export a share the lookups cannot reach into.

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/JohanLindvall/kubescrape/internal/obs"
)

// metaBudgetDivisor makes the allowance HALF of the scrape's own budget: the
// metadata lookups of one scrape may cost it at most as much as everything else
// put together.
//
// Half rather than something tighter because the allowance has to cover the
// legitimate worst case, which is the FIRST cycle after a restart: nothing is
// in the 1-minute podCache yet, so a 110-pod node issues a few hundred live
// lookups in one conversion. Half of a 15s budget is ~7.5s, which is hundreds
// of round trips against a service answering in single-digit milliseconds —
// while a blackholed one now costs the scrape 7.5s instead of all 15s, and
// costs it nothing at all in data.
const metaBudgetDivisor = 2

// metaBudgetWarnEvery throttles the exhausted-allowance warning. Past the
// allowance EVERY remaining object of the scrape takes that path, and the
// diagnosis is per outage, not per object — otlpingest's lookupBudgetWarnEvery,
// for the same reason. The window is per PIPELINE (metaBudgetSlot).
const metaBudgetWarnEvery = time.Minute

// metaBudgetPipelines are the pipelines whose scrapes carry a metadata
// allowance (every caller of scrapeContext); /metrics resolves nothing and has
// none. Each owns one Scraper.metaBudgetWarn gate, and the extra last slot is
// the fallback for a pipeline added to scrapeContext without being listed
// here — shared, but never a panic.
var metaBudgetPipelines = [...]string{pipelineTargets, pipelineCadvisor, pipelineSummary}

// metaBudgetSlot is the index of pipeline's warning gate in
// Scraper.metaBudgetWarn.
func metaBudgetSlot(pipeline string) int {
	for i, p := range metaBudgetPipelines {
		if p == pipeline {
			return i
		}
	}
	return len(metaBudgetPipelines)
}

// metaBudget is one scrape's metadata allowance. It rides the CONTEXT rather
// than the Scraper (which is shared by every concurrently running scrape) the
// way otlpexport.Own and transform.Handoff ride theirs: the budget belongs to
// the call, and every batcher, splitter and resolver reached from one scrape
// shares exactly the one its entry point attached.
type metaBudget struct {
	limit time.Duration
	// pipeline labels the exhaustion counter; it is the scrape's own pipeline
	// id so an operator can tell which scrape is shedding attribution.
	pipeline string
	// budget is the scrape budget this allowance was carved out of — the
	// MINIMUM of -scrape-timeout and whatever intervals clamp it, never the
	// configured flag. Kept so the warning can name the number the allowance
	// actually relates to (see reportMetaBudget).
	budget time.Duration
	// exhausted latches so the counter moves once per SCRAPE rather than once
	// per shed object.
	exhausted atomic.Bool
	// spent is atomic because a splitter-bearing scrape may resolve on more
	// than one goroutine; the value is nanoseconds of measured elapsed time.
	spent atomic.Int64
	// shed counts the objects that were never asked about once the allowance
	// ran out. It is what makes the warning actionable: "the allowance was
	// spent" says a partition happened, "and 214 objects went out unjoinable"
	// says how much of this node's cadvisor and summary data an operator should
	// not trust to join. Nothing else can report it — a lookup that is never
	// ISSUED moves no request counter, and the cadvisor and summary unresolved
	// counters tally unplaced objects without saying which of them the
	// allowance cost (a splitter's are tallied nowhere).
	shed atomic.Int64
	// shedKeys dedupes shed across the whole SCRAPE, not just one resolution:
	// a batcher clears its per-chunk resource map on every flush and a refused
	// lookup caches nothing, so an object present in several chunks reaches
	// the shed path once per chunk. Allocated lazily on the first shed and
	// touched only on that (cold) path; shedMu because a splitter-bearing
	// scrape may resolve on more than one goroutine.
	shedMu   sync.Mutex
	shedKeys map[string]struct{}
}

// firstShed reports whether key names an object this scrape has not shed yet,
// recording it.
func (b *metaBudget) firstShed(key string) bool {
	b.shedMu.Lock()
	defer b.shedMu.Unlock()
	if _, seen := b.shedKeys[key]; seen {
		return false
	}
	if b.shedKeys == nil {
		b.shedKeys = make(map[string]struct{})
	}
	b.shedKeys[key] = struct{}{}
	return true
}

type metaBudgetKey struct{}

// withMetaBudget carves a scrape's metadata allowance out of its budget. A
// non-positive budget attaches nothing: a Config with no usable timeout has no
// deadline for the lookups to consume in the first place, and an absent budget
// means the lookups are bounded only by the caller's context, which is what
// every caller outside a scrape (FillContainerResource, for
// internal/agent/cgroupstats) still gets.
func withMetaBudget(ctx context.Context, scrapeBudget time.Duration, pipeline string) context.Context {
	if scrapeBudget <= 0 {
		return ctx
	}
	return context.WithValue(ctx, metaBudgetKey{}, &metaBudget{
		budget: scrapeBudget, limit: scrapeBudget / metaBudgetDivisor, pipeline: pipeline,
	})
}

func metaBudgetFrom(ctx context.Context) *metaBudget {
	b, _ := ctx.Value(metaBudgetKey{}).(*metaBudget)
	return b
}

// remaining is what is left of the allowance, negative when overspent (a single
// lookup may overshoot by the slack between its deadline firing and the charge
// landing).
func (b *metaBudget) remaining() time.Duration {
	return b.limit - time.Duration(b.spent.Load())
}

// scrapeContext is the context one scrape runs in: the timeout bounding the
// whole scrape, plus the metadata allowance carved out of it. The three
// ENRICHED entry points (targets, cadvisor, summary) go through here, so they
// cannot drift on which bound applies to what; scrapeNodeMetrics resolves
// nothing and takes a plain timeout.
// The returned cancel also REPORTS a spent allowance, so every scrape entry
// point gets the line from its existing `defer cancel()` and none can forget
// it.
func (s *Scraper) scrapeContext(ctx context.Context, budget time.Duration, pipeline string) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithTimeout(ctx, budget)
	ctx = withMetaBudget(ctx, budget, pipeline)
	return ctx, func() {
		s.reportMetaBudget(ctx)
		cancel()
	}
}

// metaLookup bounds ONE live metadata lookup by what remains of the scrape's
// allowance. The third value reports whether the lookup may be issued at all: a
// false means the allowance is spent and the caller must return unresolved
// WITHOUT asking, since the only thing another blocked round trip can buy is
// more of the export's time.
//
// The release charges the time the lookup actually took and cancels the derived
// context; call it exactly once. Read the RETURNED context's Err to decide
// whether an error is worth caching — it covers both a spent allowance and a
// cancelled scrape, and both mean the service said nothing.
//
// A lookup that resolves quickly — the case the allowance exists to protect —
// charges only what it took, so a scrape's worth of fast lookups never comes
// near the bound. Nothing here reaches the metadata client's own cache, because
// the Scraper's podCache (a minute, against the client's ten seconds) is read
// first and is the longer of the two.
func (s *Scraper) metaLookup(ctx context.Context, obj *objectShed) (context.Context, func(), bool) {
	b := metaBudgetFrom(ctx)
	if b == nil {
		return ctx, func() {}, true
	}
	left := b.remaining()
	if left <= 0 {
		// Counted ONCE per scrape, not per shed object: b.exhausted latches, so
		// a 200-pod node reports one exhausted scrape rather than 200, which is
		// what makes a rate on this comparable with kubescrape_scrapes_total.
		if !b.exhausted.Swap(true) {
			obs.ScrapeMetaBudgetExhausted.WithLabelValues(b.pipeline).Inc()
		}
		// The LINE is emitted when the scrape ends (reportMetaBudget), where
		// the shed count is final; here there is only ever one object's worth
		// of it, and a warn per shed object would take the throttle's atomic on
		// a path a 200-pod node walks 200 times.
		//
		// shed is per OBJECT, which is why obj exists: one cadvisor container
		// row issues TWO lookups (the container id, then its pod), so charging
		// per LOOKUP reported `unattributed=400` for the 200 objects a 200-pod
		// node actually shed — and the inflation factor is between 1x and 2x
		// and not derivable from the line, since rows without a vouched
		// container id and every summary object charge once. The flag covers
		// the two lookups of ONE resolution for free; the scrape-wide key set
		// covers the same object resolved again in a later chunk, which was
		// the same inflation by the chunk count instead.
		switch {
		case obj == nil:
			b.shed.Add(1)
		case !obj.charged:
			obj.charged = true
			if b.firstShed(obj.key()) {
				b.shed.Add(1)
			}
		}
		return ctx, func() {}, false
	}
	// WithTimeout takes the earlier of the two deadlines, so a scrape already
	// close to its own still bounds the lookup.
	lctx, cancel := context.WithTimeout(ctx, left)
	start := time.Now()
	return lctx, func() {
		b.spent.Add(int64(time.Since(start)))
		cancel()
	}, true
}

// reportMetaBudget names the condition once a minute, at the END of a scrape
// that exhausted its allowance — which is the only moment the shed count is
// final, and the count is most of what the line is for.
//
// The counters are the ongoing signal — kubescrape_scrape_metadata_budget_
// exhausted_total counts the scrapes, and on the cadvisor and summary
// pipelines the objects also land in kubescrape_cadvisor_unresolved_total and
// kubescrape_summary_unresolved_total — so what this adds is WHICH
// pipeline is shedding, HOW MANY objects it shed, and the BUDGET the allowance
// was cut from, which together are what an operator needs to decide whether to
// raise the scrape timeout or go and fix the metadata service.
//
// `scrapeBudget` and not the configured -scrape-timeout: the budget is the
// MINIMUM of the timeout and the intervals that clamp it (kubeletTimeout, and
// targetTimeout's min of the target's own interval too), so printing the
// unclamped flag put two numbers on one line that do not relate — with
// -scrape-timeout=60s and -scrape-interval=30s it read `allowance=15s
// scrapeTimeout=60s`, and an operator following this line's own advice raised
// the timeout to 120s and moved the allowance not at all.
func (s *Scraper) reportMetaBudget(ctx context.Context) {
	b := metaBudgetFrom(ctx)
	if b == nil || !b.exhausted.Load() || !s.metaBudgetWarn[metaBudgetSlot(b.pipeline)].Allow(metaBudgetWarnEvery) {
		return
	}
	s.log.Warn("a scrape spent its whole metadata allowance; the objects it had left are exported with their label identity, and the scrape itself still ships",
		"pipeline", b.pipeline, "unattributed", b.shed.Load(),
		"allowance", b.limit, "scrapeBudget", b.budget,
		"scrapeTimeout", s.cfg.Timeout, "scrapeInterval", s.cfg.Interval)
}

// objectShed makes the shed count per OBJECT rather than per LOOKUP: one
// resolution may issue two (the container id, then the pod), and both are the
// same object going out unjoinable. It carries the object's identity as the
// resolution was asked it, so a later chunk resolving the same object again is
// recognised too (metaBudget.shedKeys). Declared on the caller's stack and
// holding only string headers, so this costs no allocation on the enriched
// path; key() is built only on the shed path.
//
// The identity is the OBJECT's, not a lookup's cache key: keyed by lookup, a
// pod-level row whose pod lookup a container row of that pod had already shed
// would not be counted, although it is a different resource going out
// unjoinable.
type objectShed struct {
	containerID, namespace, pod, uid, container string
	charged                                     bool
}

func (o *objectShed) key() string {
	var b []byte
	for _, part := range [...]string{o.containerID, o.namespace, o.pod, o.uid, o.container} {
		b = appendLP(b, part)
	}
	return string(b)
}
