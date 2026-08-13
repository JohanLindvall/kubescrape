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
// That counter is not redundant with the two that already exist: an object
// never asked about cannot move kubescrape_metadata_requests_total, and
// kubescrape_summary_unresolved_total covers only the summary pipeline, so
// without it a cadvisor scrape shedding attribution moves nothing at all.
//
// Why an allowance rather than giving the export a context of its own: cycle()
// waits for every scrape it starts and Run only ticks after cycle returns, so a
// scrape whose export outlives the scrape budget stretches the whole node's
// cadence — the same reason kubeletTimeout clamps to -scrape-interval. Carving
// the allowance out of the budget keeps the total where it was and hands the
// conversion and the export a share the lookups cannot reach into.

import (
	"context"
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
// for the same reason.
const metaBudgetWarnEvery = time.Minute

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
	// exhausted latches so the counter moves once per SCRAPE rather than once
	// per shed object.
	exhausted atomic.Bool
	// spent is atomic because a splitter-bearing scrape may resolve on more
	// than one goroutine; the value is nanoseconds of measured elapsed time.
	spent atomic.Int64
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
	return context.WithValue(ctx, metaBudgetKey{}, &metaBudget{limit: scrapeBudget / metaBudgetDivisor, pipeline: pipeline})
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
// whole scrape, plus the metadata allowance carved out of it. Every scrape
// entry point goes through here, so the three of them cannot drift on which
// bound applies to what.
func (s *Scraper) scrapeContext(ctx context.Context, budget time.Duration, pipeline string) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithTimeout(ctx, budget)
	return withMetaBudget(ctx, budget, pipeline), cancel
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
func (s *Scraper) metaLookup(ctx context.Context) (context.Context, func(), bool) {
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
		s.warnMetaBudget(b.limit)
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

// warnMetaBudget names the condition once a minute. The counters are the
// ongoing signal — kubescrape_scrape_metadata_budget_exhausted_total counts the
// scrapes, and on the summary pipeline the objects also land in
// kubescrape_summary_unresolved_total — so this line exists to name the
// allowance and the timeout it was cut from, which is what an operator needs to
// decide whether to raise the scrape timeout or fix the metadata service.
func (s *Scraper) warnMetaBudget(limit time.Duration) {
	if !s.metaBudgetWarn.Allow(metaBudgetWarnEvery) {
		return
	}
	s.log.Warn("a scrape spent its whole metadata allowance; the objects it had left are exported with their label identity, and the scrape itself still ships",
		"allowance", limit, "scrapeTimeout", s.cfg.Timeout, "scrapeInterval", s.cfg.Interval)
}
