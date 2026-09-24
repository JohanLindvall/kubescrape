package metrics

import (
	"cmp"
	"context"
	"log/slog"
	"slices"
	"strings"
	"time"

	"go.opentelemetry.io/collector/pdata/pmetric"
)

var metricsMarshaler pmetric.ProtoMarshaler

// Run exports the set's metrics to exp every interval until ctx is done. The
// caller should Export once more after every producer has stopped (the
// tailer's shutdown flush feeds the set after Run returns), or the last
// window's samples are lost — series state is not persisted.
func (s *DynamicMetricSet) Run(ctx context.Context, exp Exporter, interval time.Duration, maxBytes int) {
	if interval <= 0 {
		// time.NewTicker PANICS on a non-positive duration, and this interval
		// comes from a flag with no lower bound — so -logs-metrics-interval=0
		// killed the process with a runtime panic instead of doing the obvious
		// thing. Nothing to export on a zero interval; the caller's shutdown
		// flush still runs.
		//
		// SAID OUT LOUD, because the configuration is otherwise indistinguishable
		// from a working one from the outside: every line is still matched and
		// observed (the per-line cost is paid in full), the series still expire,
		// and nothing is ever sent. The effective-config dump prints the interval;
		// this prints the CONSEQUENCE.
		s.logger().Warn("log-derived metrics are observed but never exported: the export interval is zero",
			"interval", interval, "flag", "-logs-metrics-interval", "rules", s.Count)
		<-ctx.Done()
		return
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		s.noteExport(s.export(ctx, exp, maxBytes))
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// noteExport reports one export cycle's outcome as a TRANSITION rather than
// once per interval.
//
// The old shape was one Warn per failed cycle, which on a fleet is one line per
// node per interval for as long as the collector is down — and, worse, it had
// no counterpart: an operator watching the lines stop could not tell a recovery
// from a process that had stopped exporting altogether. So the first failure of
// a run warns immediately, the repeats restate themselves at reWarnInterval
// carrying the cost so far, and the recovery is one Info naming what the outage
// cost. Nothing here is the RATE: the samples that survived are retained (and
// re-offered), and the ones that did not move
// kubescrape_log_metrics_dropped_undelivered_total.
//
// dropped is how many resources this cycle's flushes threw away because the
// collector rejected their chunk PERMANENTLY, and the wording branches on it:
// the retention claim is true of a transient failure and FALSE of a permanent
// rejection, whose samples are already gone and already counted. Saying it
// unconditionally put an Error ("dropping a permanently rejected
// log-metrics chunk") and a Warn ("the undelivered samples are retained")
// about the same cycle in one log, and an operator who read the Warn stopped
// chasing the drop.
func (s *DynamicMetricSet) noteExport(dropped int, err error) {
	s.exportMu.Lock()
	if err != nil {
		now := time.Now()
		_, loud := s.exportOutage.Fail(now, reWarnInterval)
		n, lasted := s.exportOutage.Failures(), s.exportOutage.Lasted(now)
		s.exportMu.Unlock()
		if loud {
			if dropped > 0 {
				s.logger().Warn("exporting log metrics failed; part of the payload was rejected PERMANENTLY and those observations are LOST, "+
					"and any remaining undelivered samples are retained and re-offered",
					"error", err, "failures", n, "outage", lasted, "resources", dropped)
				return
			}
			s.logger().Warn("exporting log metrics failed; the undelivered samples are retained and re-offered",
				"error", err, "failures", n, "outage", lasted)
			return
		}
		s.logger().Debug("exporting log metrics failed", "error", err, "failures", n, "resources", dropped)
		return
	}
	n, lasted, recovered := s.exportOutage.Recover(time.Now())
	s.exportMu.Unlock()
	if recovered {
		s.logger().Info("exporting log metrics succeeded again", "failures", n, "outage", lasted)
	}
}

// logger is the set's logger, defaulting to slog.Default() at the call.
//
// The field is nil unless WithLogger set it — and in a set built literally
// (this package's own tests do it, to reach retain and the export loop without
// compiling a rule set) — and a nil *slog.Logger panics on use, which is a bad
// trade for a line whose whole purpose is diagnostics. Resolving the default
// here rather than at construction is also what keeps a set built before the
// process installs its handler logging through that handler (series.logger).
func (s *DynamicMetricSet) logger() *slog.Logger {
	if s.log == nil {
		return slog.Default()
	}
	return s.log
}

// seriesSamples pairs a series with the samples that belong to one resource.
type seriesSamples struct {
	series  *series
	samples []sample
	// ts is the instant the samples were SNAPSHOTTED, and the timestamp they
	// render at. A retained (previously undelivered) generation keeps its own
	// snapshot time rather than being restamped by the export that re-offers
	// it: one clock for retained and fresh alike meant a still-live cumulative
	// series — re-read by every snapshot — rendered twice under one timestamp
	// with two different values in one payload (a duplicate a Prometheus-
	// lineage backend rejects or ingests order-dependently). With the original
	// time kept, the retained generation is simply the older point of the same
	// series — exactly what at-least-once means for a metric.
	ts time.Time
}

// Export sends the current value of every configured metric as OTLP, grouped
// into one ResourceMetrics per distinct log resource (the line's resource
// attributes become the OTLP resource; the metric's own labels stay on the data
// points). Output is chunked per resource to stay under maxBytes (0 = a single
// payload). Rules sharing a series export it once.
func (s *DynamicMetricSet) Export(ctx context.Context, exp Exporter, maxBytes int) error {
	_, err := s.export(ctx, exp, maxBytes)
	return err
}

// export is Export, additionally reporting how many resources were dropped
// because the collector rejected their chunk PERMANENTLY.
//
// That second return exists for the log line and nothing else: a permanent
// rejection is LOSS, and noteExport's transition Warn used to assert "the
// undelivered samples are retained and re-offered" for every non-nil error —
// so one cycle emitted an Error saying a chunk was dropped and a Warn saying
// the samples were kept, and the Warn is the line the throttle guarantees an
// operator sees first and repeatedly. It is a return value rather than a field
// so a concurrent Export (main.go's shutdown flush runs one beside Run's) can
// never have its outcome narrated by the other's cycle.
func (s *DynamicMetricSet) export(ctx context.Context, exp Exporter, maxBytes int) (int, error) {
	if s == nil {
		return 0, nil
	}
	s.exportMu.Lock()
	defer s.exportMu.Unlock()
	ts := time.Now()
	byResource, order := s.groupByResource(ts)
	// Re-offer whatever a previous export failed to deliver and the store no
	// longer holds. snapshot() is DESTRUCTIVE for exactly those values — it
	// seals aggregation windows, zeroes idled gauges and deletes expired
	// samples — so a failed send used to end those observations' lives. A live
	// series needs no copy: the store still holds it and this snapshot has just
	// re-read it (a failed export hands its consumed flags back instead; see
	// retain). Retaining the rest is what makes this at-least-once, like every
	// other producer in this repo; each retained generation renders at its OWN
	// snapshot time (seriesSamples.ts).
	order = s.mergeRetry(byResource, order)

	md := pmetric.NewMetrics()
	size := 0 // accumulated payload size, tracked incrementally (O(n) not O(n^2))
	// firstErr keeps the first chunk failure but does NOT abort the export: the
	// remaining chunks hold different resources, and failing them too would
	// only widen the outage. Each chunk's samples are retained on ITS OWN
	// failure (below), so nothing is lost by continuing.
	var firstErr error
	// permErr is the first PERMANENT rejection, returned in preference to
	// firstErr: the caller's narration branches on droppedPermanently, and a
	// Warn saying "rejected PERMANENTLY and those observations are LOST" must
	// carry the error that says why, not an earlier chunk's transient one.
	var permErr error
	// chunk names the resources rendered into md since the last flush, so a
	// failed send retains exactly those and not the ones that landed.
	var chunk []string
	// droppedPermanently accumulates what the permanent branch below threw
	// away, for the caller's narration.
	droppedPermanently := 0
	flush := func() {
		if md.ResourceMetrics().Len() == 0 {
			return
		}
		if err := exp.ExportMetrics(ctx, md); err != nil {
			if firstErr == nil {
				firstErr = err
			}
			if s.permanent != nil && s.permanent(err) {
				// A definitive rejection (bad payload, unimplemented, over a
				// receiver's limit) cannot become deliverable: retaining it
				// re-sent the same refused chunk every interval forever while
				// the pile grew. Drop it counted, the way every other producer
				// classifies (the tailer's advanceBatch, the buffer's drain).
				if permErr == nil {
					permErr = err
				}
				n := chunkSamples(byResource, chunk)
				s.drops.addRetained(uint64(n))
				droppedPermanently += len(chunk)
				// THROTTLED: the store keeps every live series, so the next
				// snapshot re-renders the same resources into the same refused
				// chunk — a persisting rejection recurs every interval on every
				// node, which unthrottled was one Error per chunk per interval
				// per node. The counter carries the rate; the line restates the
				// running total. noteExport's Warn is the transition narration
				// beside it, on its own throttle.
				if s.permanentWarn.Allow(reWarnInterval) {
					s.logger().Error("dropping a permanently rejected log-metrics chunk",
						"resources", len(chunk), "samples", n, "dropped", s.drops.Retained(), "error", err)
				} else {
					s.logger().Debug("dropping a permanently rejected log-metrics chunk",
						"resources", len(chunk), "samples", n, "error", err)
				}
			} else {
				s.retain(byResource, chunk)
			}
		}
		md.ResourceMetrics().RemoveIf(func(pmetric.ResourceMetrics) bool { return true })
		chunk = chunk[:0]
		size = 0
	}

	scratch := pmetric.NewMetrics()
	for _, resStr := range order {
		// Render into a scratch payload first so the chunk can be flushed
		// BEFORE it would exceed maxBytes (previously every chunk overflowed
		// the limit by up to one resource, and maxBytes == 0 exported one
		// payload per resource instead of the documented single payload).
		scratch.ResourceMetrics().RemoveIf(func(pmetric.ResourceMetrics) bool { return true })
		rm := scratch.ResourceMetrics().AppendEmpty()
		putLabels(rm.Resource().Attributes(), resStr)
		scope := rm.ScopeMetrics().AppendEmpty()
		setScope(scope, ScopeName)
		for _, ss := range byResource[resStr] {
			renderSeries(scope, ss.series, ss.samples, ss.ts)
		}
		rmSize := metricsMarshaler.MetricsSize(scratch)
		if maxBytes > 0 && size > 0 && size+rmSize > maxBytes {
			flush()
		}
		scratch.ResourceMetrics().MoveAndAppendTo(md.ResourceMetrics())
		chunk = append(chunk, resStr)
		size += rmSize
	}
	flush()
	if permErr != nil {
		return droppedPermanently, permErr
	}
	return droppedPermanently, firstErr
}

// chunkSamples counts the samples a chunk's resources carry — the unit
// kubescrape_log_metrics_dropped_undelivered_total counts in.
func chunkSamples(byResource map[string][]seriesSamples, chunk []string) int {
	n := 0
	for _, resStr := range chunk {
		for _, ss := range byResource[resStr] {
			n += len(ss.samples)
		}
	}
	return n
}

// WithPermanentClassifier installs the export-error classifier (nil = every
// failure is transient and retained). The set cannot import the exporter's own
// IsPermanent — obs sits between the two packages — so the caller injects it,
// the same inversion metaclient uses for Config.Observe.
func WithPermanentClassifier(f func(error) bool) Option {
	return func(c *setConfig) { c.permanent = f }
}

// maxRetainedResources bounds the distinct resources the re-offer buffer
// holds. A collector outage is exactly when this fills, and holding a growing
// pile of dead windows would turn a delivery problem into an OOM — which is the
// failure the retention exists to avoid, arrived at from the other side. Past
// the cap the STALEST whole resources are dropped and counted (see
// stalestRetained), so the loss is visible rather than silent.
const maxRetainedResources = 4096

// maxRetainedSamples bounds the retained SAMPLES across all resources. 50k
// samples is ~20 MB worst case (per-sample struct plus its share of the
// interned label/resource strings), an amount worth holding for a recovery and
// safe to hold through an outage. Only values the store no longer holds are
// retained (see retain), so what fills it is irreplaceable, and past it the
// OLDEST generations go first (evictOldestGenerations).
const maxRetainedSamples = 50_000

// retain handles a transiently failed chunk: it keeps the samples the store
// can no longer produce, so the next Export re-offers them, and hands every
// other sample's consumed flags back to the store.
//
// ONLY FINAL SAMPLES ARE KEPT (sample.final: a grace-deleted series' last
// value, an idled gauge's, the first emission of an aggregation window). A
// live series' sample is a READ of a value the store still holds and re-reads
// at every export, and keeping it cost everything the bound exists to protect:
// one more copy of every live series per failed cycle, so 4000 live series
// filled the 50k-sample cap in about thirteen cycles of an outage, after which
// the eviction threw out whole resources — counting as lost the counters the
// next export delivers in full, and throwing out with them the final values
// that really had no other copy. The price is the intermediate points of a live
// series across an outage (no backfill), which for a cumulative stream costs no
// observation at all. Instead, the live sample gets back what the failed
// snapshot consumed (series.rearm): `initial`, so the baseline zeros ride the
// next export, and `exported`, so a series that goes quiet or expires before a
// delivery is still emitted — as a final sample, which this then keeps.
//
// What is retained is the raw SAMPLES (seriesSamples over the store's sample
// structs), never the rendered pdata: the next Export folds them back in via
// mergeRetry and renders a FRESH pmetric.Metrics. That is what licenses the
// agent to mark this set's exports with transform.Handoff at its call sites
// (cmd/kubescrape-agent — this package cannot import the transform package,
// which reaches it through obs): a payload the transform seam mutated in
// place is never re-offered, so a script cannot run twice over its own
// output. If retention ever starts keeping pdata subtrees and merging them
// into the next payload, those marks become wrong before this comment does.
func (s *DynamicMetricSet) retain(byResource map[string][]seriesSamples, chunk []string) {
	for _, resStr := range chunk {
		for _, ss := range byResource[resStr] {
			if ss.series != nil {
				ss.series.rearm(ss.samples)
			}
			kept := finalSamples(ss.samples)
			if len(kept) == 0 {
				continue
			}
			if s.retryBy == nil {
				s.retryBy = map[string][]seriesSamples{}
			}
			if _, ok := s.retryBy[resStr]; !ok {
				s.retryOrder = append(s.retryOrder, resStr)
			}
			s.retryBy[resStr] = append(s.retryBy[resStr], seriesSamples{series: ss.series, samples: kept, ts: ss.ts})
			s.retainedSamples += len(kept)
		}
	}
	evicted, lastVictim := 0, ""
	for len(s.retryOrder) > maxRetainedResources {
		i := s.stalestRetained()
		victim := s.retryOrder[i]
		s.retryOrder = slices.Delete(s.retryOrder, i, i+1)
		n := 0
		for _, e := range s.retryBy[victim] {
			n += len(e.samples)
		}
		s.retainedSamples -= n
		delete(s.retryBy, victim)
		evicted, lastVictim = evicted+n, victim
	}
	if s.retainedSamples > maxRetainedSamples {
		n, victim := s.evictOldestGenerations()
		evicted += n
		if victim != "" {
			lastVictim = victim
		}
	}
	if evicted == 0 {
		return
	}
	s.drops.addRetained(uint64(evicted))
	// THIS IS LOSS, and it was counted without ever being described: the
	// retention is what makes a failed export at-least-once, so a sample
	// evicted from it is an observation no export will ever carry. The counter
	// (kubescrape_log_metrics_dropped_undelivered_total) says how many; only a
	// line can say which resource and against which of the two bounds — the
	// resource cap and the sample cap bind for different reasons and are tuned
	// separately. Aggregated per retain call and throttled, because a deep
	// outage evicts on every cycle.
	if s.evictWarn.Allow(reWarnInterval) {
		s.logger().Warn("dropping undelivered log-metrics samples: the re-offer buffer is full, so these observations are lost",
			"dropped", evicted, "resource", lastVictim,
			"resources", len(s.retryOrder), "maxResources", maxRetainedResources,
			"samples", s.retainedSamples, "maxSamples", maxRetainedSamples)
	}
}

// finalSamples returns the samples a failed export must keep (sample.final):
// the slice itself when every one is (a retained generation passing through
// again), a filtered copy when only some are, nil when none is.
func finalSamples(samples []sample) []sample {
	n := 0
	for i := range samples {
		if samples[i].final {
			n++
		}
	}
	switch n {
	case 0:
		return nil
	case len(samples):
		return samples
	}
	out := make([]sample, 0, n)
	for i := range samples {
		if samples[i].final {
			out = append(out, samples[i])
		}
	}
	return out
}

// evictOldestGenerations drops retained generations, OLDEST SNAPSHOT FIRST,
// until the pile fits maxRetainedSamples, and reports how many samples it
// dropped and the last resource it took them from. A tie on the snapshot time
// goes to the resource that has gone quietest (its freshest generation oldest),
// then to the resource string, so the choice is deterministic.
//
// Generations, not whole resources: the retention is a sliding window over an
// outage, and dropping a resource's whole pile to make room emptied it — a
// single busy resource lost every retained point at once, about every
// thirteen cycles, rather than its oldest one.
func (s *DynamicMetricSet) evictOldestGenerations() (evicted int, lastVictim string) {
	type gen struct {
		ts, newest time.Time
		res        string
		i          int
	}
	var gens []gen
	for _, res := range s.retryOrder {
		newest := s.newestRetainedTS(res)
		for i, g := range s.retryBy[res] {
			gens = append(gens, gen{ts: g.ts, newest: newest, res: res, i: i})
		}
	}
	slices.SortFunc(gens, func(a, b gen) int {
		return cmp.Or(a.ts.Compare(b.ts), a.newest.Compare(b.newest), strings.Compare(a.res, b.res), cmp.Compare(a.i, b.i))
	})
	gone := map[string][]bool{}
	for _, g := range gens {
		if s.retainedSamples <= maxRetainedSamples {
			break
		}
		marks := gone[g.res]
		if marks == nil {
			marks = make([]bool, len(s.retryBy[g.res]))
			gone[g.res] = marks
		}
		marks[g.i] = true
		n := len(s.retryBy[g.res][g.i].samples)
		s.retainedSamples -= n
		evicted, lastVictim = evicted+n, g.res
	}
	for res, marks := range gone {
		kept := s.retryBy[res][:0]
		for i, g := range s.retryBy[res] {
			if !marks[i] {
				kept = append(kept, g)
			}
		}
		if len(kept) == 0 {
			delete(s.retryBy, res)
			continue
		}
		clear(s.retryBy[res][len(kept):])
		s.retryBy[res] = kept
	}
	s.retryOrder = slices.DeleteFunc(s.retryOrder, func(res string) bool {
		_, ok := s.retryBy[res]
		return !ok
	})
	return evicted, lastVictim
}

// stalestRetained picks the resource-cap eviction victim: the retained resource
// whose most recent generation is oldest — the one that has gone quietest —
// with the resource string breaking ties so the choice is deterministic.
//
// SLICE POSITION IS NOT AGE, which is what this replaced. mergeRetry nils
// retryBy/retryOrder on every Export, so nothing about the previous order
// survives, and retain then rebuilds retryOrder in chunk order — which is
// groupByResource's map-ranged FRESH resources first and mergeRetry's
// retained-only ones appended LAST. Popping index 0 therefore destroyed, every
// interval, a resource that was still producing (picked at random among them)
// while the piles of resources that had gone quiet survived at the tail: the
// exact inverse of the intent, and unrecoverable for the samples the store no
// longer holds — snapshot() seals an aggregating gauge's window and the
// expiry-grace branch emits a sample once before deleting it.
//
// seriesSamples.ts carries the real age. Generations are appended in export
// order (mergeRetry prepends the retained ones before the fresh), so the LAST
// one is a resource's freshest.
func (s *DynamicMetricSet) stalestRetained() int {
	victim := 0
	newest := s.newestRetainedTS(s.retryOrder[0])
	for i := 1; i < len(s.retryOrder); i++ {
		ts := s.newestRetainedTS(s.retryOrder[i])
		if ts.Before(newest) || (ts.Equal(newest) && s.retryOrder[i] < s.retryOrder[victim]) {
			victim, newest = i, ts
		}
	}
	return victim
}

// newestRetainedTS is the snapshot time of a resource's freshest retained
// generation. A resource with none (retain never records an empty group) reads
// as infinitely stale, so it is evicted first rather than being kept forever.
func (s *DynamicMetricSet) newestRetainedTS(res string) time.Time {
	gens := s.retryBy[res]
	if len(gens) == 0 {
		return time.Time{}
	}
	return gens[len(gens)-1].ts
}

// mergeRetry folds previously undelivered samples into this export's grouping
// and clears the retention: whatever is not delivered NOW is retained again by
// flush, so a repeated failure keeps re-offering rather than accumulating twice.
func (s *DynamicMetricSet) mergeRetry(byResource map[string][]seriesSamples, order []string) []string {
	if len(s.retryOrder) == 0 {
		return order
	}
	for _, resStr := range s.retryOrder {
		if _, ok := byResource[resStr]; !ok {
			order = append(order, resStr)
		}
		// Prepended: the undelivered generations are OLDER than what the fresh
		// snapshot just took for the same resource, and each carries its own
		// snapshot ts — per series, points render oldest-first in ascending
		// timestamp order.
		byResource[resStr] = append(s.retryBy[resStr], byResource[resStr]...)
	}
	s.retryBy, s.retryOrder = nil, nil
	s.retainedSamples = 0
	return order
}

// groupByResource snapshots each unique series and buckets its samples by the
// resource they carry, returning resource → [(series, its samples)] plus the
// resource order. ts is the snapshot instant, stamped on every group (see
// seriesSamples.ts).
func (s *DynamicMetricSet) groupByResource(ts time.Time) (map[string][]seriesSamples, []string) {
	byResource := map[string][]seriesSamples{}
	var order []string
	seen := make(map[*series]bool, len(s.rules))
	for _, rule := range s.rules {
		if rule.series.name == "" || seen[rule.series] {
			continue
		}
		seen[rule.series] = true
		perRes := map[string][]sample{}
		for _, samp := range rule.series.snapshot() {
			perRes[samp.resource] = append(perRes[samp.resource], samp)
		}
		for resStr, samps := range perRes {
			if _, ok := byResource[resStr]; !ok {
				order = append(order, resStr)
			}
			byResource[resStr] = append(byResource[resStr], seriesSamples{series: rule.series, samples: samps, ts: ts})
		}
	}
	return byResource, order
}
