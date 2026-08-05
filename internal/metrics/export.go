package metrics

import (
	"context"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

// Exporter sends OTLP metrics; implemented by the agent's otlpexport.Client.
type Exporter interface {
	ExportMetrics(ctx context.Context, md pmetric.Metrics) error
}

var metricsMarshaler pmetric.ProtoMarshaler

// ScopeName is the instrumentation scope of log-derived metrics. Every other
// producer in the repo names its scope; this one shipped an empty one, so its
// series were the only ones a consumer could not attribute to the code that
// made them.
const ScopeName = "github.com/JohanLindvall/kubescrape/internal/metrics"

// RegistryScopeName is the instrumentation scope of the self-metrics Registry
// (the metrics internal/obs registers).
const RegistryScopeName = "github.com/JohanLindvall/kubescrape/internal/obs"

// scopeVersion is the build version stamped on every scope this package emits.
// internal/obs owns BuildVersion but IMPORTS this package, so it pushes the
// value down at init rather than being imported back — that would be a cycle.
// Empty (a test binary, or any importer that is not a kubescrape binary) means
// no version is set at all, which is what an unknown version must look like on
// the wire.
var scopeVersion atomic.Pointer[string]

// SetScopeVersion records the build version to stamp on exported scopes. Called
// once, from obs's init.
func SetScopeVersion(v string) { scopeVersion.Store(&v) }

// setScope names and versions one ScopeMetrics.
func setScope(sm pmetric.ScopeMetrics, name string) {
	sc := sm.Scope()
	sc.SetName(name)
	if v := scopeVersion.Load(); v != nil && *v != "" {
		sc.SetVersion(*v)
	}
}

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
		<-ctx.Done()
		return
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		if err := s.Export(ctx, exp, maxBytes); err != nil {
			s.log.Warn("exporting log metrics failed", "error", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
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
	if s == nil {
		return nil
	}
	s.exportMu.Lock()
	defer s.exportMu.Unlock()
	ts := time.Now()
	byResource, order := s.groupByResource(ts)
	// Re-offer whatever a previous export failed to deliver. snapshot() is
	// DESTRUCTIVE — it seals aggregation windows, zeroes idled samples and
	// deletes expired ones — so for those the store no longer holds the value
	// and a failed send used to end the observation's life. (A plain cumulative
	// series is unaffected either way: the next export re-reads its running
	// total.) Retaining the samples is what makes this at-least-once, like
	// every other producer in this repo; each retained generation renders at
	// its OWN snapshot time (seriesSamples.ts).
	order = s.mergeRetry(byResource, order)

	md := pmetric.NewMetrics()
	size := 0 // accumulated payload size, tracked incrementally (O(n) not O(n^2))
	// firstErr keeps the first chunk failure but does NOT abort the export: the
	// remaining chunks hold different resources, and failing them too would
	// only widen the outage. Each chunk's samples are retained on ITS OWN
	// failure (below), so nothing is lost by continuing.
	var firstErr error
	// chunk names the resources rendered into md since the last flush, so a
	// failed send retains exactly those and not the ones that landed.
	var chunk []string
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
				s.drops.addRetained(uint64(len(chunk)))
				s.log.Error("dropping a permanently rejected log-metrics chunk",
					"resources", len(chunk), "error", err)
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
	return firstErr
}

// WithPermanentClassifier installs the export-error classifier (nil = every
// failure is transient and retained). The set cannot import the exporter's own
// IsPermanent — obs sits between the two packages — so the caller injects it,
// the same inversion metaclient uses for Config.Observe.
func WithPermanentClassifier(f func(error) bool) Option {
	return func(c *setConfig) { c.permanent = f }
}

// maxRetainedResources bounds the undelivered snapshot held for the next
// export. A collector outage is exactly when this fills, and holding a growing
// pile of dead windows would turn a delivery problem into an OOM — which is the
// failure the retention exists to avoid, arrived at from the other side. Past
// the cap the OLDEST retained resources are dropped and counted, so the loss is
// visible rather than silent.
const maxRetainedResources = 4096

// maxRetainedSamples bounds the retained SAMPLES across all resources. The
// resource cap alone never bound on a node — a handful of distinct log
// resources, so a multi-hour outage grew one generation of every live series
// per failed export inside a cap that counts resources. 50k samples is ~20 MB
// worst case (per-sample struct plus its share of the interned label/resource
// strings), an amount worth holding for a recovery and safe to hold through
// an outage.
const maxRetainedSamples = 50_000

// retain keeps a failed chunk's samples so the next Export re-offers them.
func (s *DynamicMetricSet) retain(byResource map[string][]seriesSamples, chunk []string) {
	if s.retryBy == nil {
		s.retryBy = map[string][]seriesSamples{}
	}
	for _, resStr := range chunk {
		ss := byResource[resStr]
		if len(ss) == 0 {
			continue
		}
		if _, ok := s.retryBy[resStr]; !ok {
			s.retryOrder = append(s.retryOrder, resStr)
		}
		s.retryBy[resStr] = append(s.retryBy[resStr], ss...)
		for _, e := range ss {
			s.retainedSamples += len(e.samples)
		}
	}
	for len(s.retryOrder) > maxRetainedResources || (s.retainedSamples > maxRetainedSamples && len(s.retryOrder) > 0) {
		oldest := s.retryOrder[0]
		s.retryOrder = s.retryOrder[1:]
		for _, e := range s.retryBy[oldest] {
			s.retainedSamples -= len(e.samples)
		}
		delete(s.retryBy, oldest)
		s.drops.addRetained(1)
	}
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

// renderSeries appends one metric and the given samples' data points to scope.
func renderSeries(scope pmetric.ScopeMetrics, s *series, samples []sample, ts time.Time) {
	m := scope.Metrics().AppendEmpty()
	m.SetName(s.name)
	m.SetDescription(s.desc)

	switch s.kind {
	case kindHistogram:
		renderHistogram(m, s, samples, ts)
	case kindSummary:
		renderSummary(m, samples, ts)
	case kindGauge:
		renderNumber(m.SetEmptyGauge().DataPoints(), samples, ts, false)
	default: // counter
		sum := m.SetEmptySum()
		sum.SetIsMonotonic(true)
		sum.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
		renderNumber(sum.DataPoints(), samples, ts, true)
	}
}

// startOf renders a sample's start-of-accumulation stamp (sample.start, epoch
// seconds) as an OTLP timestamp. A sample that somehow never went through
// admit falls back to ts, which is the old always-a-reset behaviour — wrong,
// but never AHEAD of the point's own timestamp, which some backends reject
// outright.
func startOf(s sample, ts time.Time) pcommon.Timestamp {
	if s.start <= 0 {
		return pcommon.Timestamp(ts.UnixNano())
	}
	return pcommon.Timestamp(time.Unix(s.start, 0).UnixNano())
}

// renderNumber writes gauge or counter samples as number data points. Counters
// additionally emit two synthetic zero points before a series' first real
// point so downstream rate() has a baseline (one minute is too short given
// timestamp normalization — Mimir takes the max value for a counter).
//
// A correct StartTimeUnixNano does NOT replace those zeros. It is advisory
// metadata that the Prometheus-lineage backends this ships into discard unless
// created-timestamp injection is explicitly enabled, and rate()/increase()
// need two real SAMPLES either way — a start timestamp cannot be the second
// one. What it does fix is the zeros themselves: they used to stamp their own
// timestamp as their start, i.e. announce a reset one and two minutes back, so
// the whole baseline they exist to provide was the thing a delta consumer
// threw away. All three points now carry the stream's single start, which
// counterBaselineSeconds keeps strictly below the earliest of them.
func renderNumber(dps pmetric.NumberDataPointSlice, samples []sample, ts time.Time, counter bool) {
	now := pcommon.Timestamp(ts.UnixNano())
	for _, s := range samples {
		start := startOf(s, ts)
		if counter && s.initial {
			for back := 2; back >= 1; back-- {
				prev := pcommon.Timestamp(ts.Add(time.Duration(-back) * time.Minute).UnixNano())
				zero := dps.AppendEmpty()
				zero.SetDoubleValue(0)
				zero.SetStartTimestamp(start)
				zero.SetTimestamp(prev)
				putLabels(zero.Attributes(), s.labels)
			}
		}
		dp := dps.AppendEmpty()
		dp.SetDoubleValue(s.value)
		dp.SetStartTimestamp(start)
		dp.SetTimestamp(now)
		putLabels(dp.Attributes(), s.labels)
	}
}

// renderSummary writes summary samples as OTLP summary data points carrying the
// running count and sum (no quantiles).
func renderSummary(m pmetric.Metric, samples []sample, ts time.Time) {
	now := pcommon.Timestamp(ts.UnixNano())
	dps := m.SetEmptySummary().DataPoints()
	for _, s := range samples {
		dp := dps.AppendEmpty()
		dp.SetStartTimestamp(startOf(s, ts))
		dp.SetTimestamp(now)
		dp.SetCount(s.count)
		dp.SetSum(s.value)
		putLabels(dp.Attributes(), s.labels)
	}
}

// histGroup is one label set's regrouped histogram: the series store keeps a
// histogram as one stream per bucket (the labels plus "le"), and both readers
// of that layout — Dump and this OTLP render — need it back as one point per
// label set.
type histGroup struct {
	lbls labels
	key  string // canonical label string without "le" (putLabels-ready)
	// first is the group's first sample in input order. Every bucket stream of
	// one label set is admitted together (observe's admission is
	// all-or-nothing), so whichever arrives first carries the point's start.
	first sample
	// buckets is the CUMULATIVE count per bound, +Inf excluded: each stream's
	// count already includes every lower bucket's, because observe records
	// into every bucket the value fits.
	buckets []uint64
	// count/sum are the +Inf stream's totals — the point's observation count
	// and their sum, exactly the Prometheus histogram shape.
	count uint64
	sum   float64
	// hasInf records that the +Inf stream was present, so a render can leave
	// the optional sum/count unset when it somehow was not (unreachable given
	// all-or-nothing admission; defensive).
	hasInf bool
}

// regroupHistogram decodes the store's bucket-stream layout into one group per
// label set, in first-seen order. Consumed by BOTH renderHistogram (which
// converts the cumulative buckets to OTLP's absolute counts) and dumpSeries
// (which keeps them cumulative, the Dump contract).
//
// Keyed by the canonical label STRING, not lbls.hash(): the series store
// defends 64-bit collisions with a check hash, and dropping that discipline
// here would silently merge two label sets' buckets into one corrupted point.
// The string is exact and already needed for the label rendering.
//
// PURE READ, by contract: nothing in the samples is mutated — Dump serves a
// live Registry through this beside the push path (TestDumpNonMutating), so
// it must never seal, clear or spend anything.
func regroupHistogram(samples []sample, nBounds int) []*histGroup {
	groups := map[string]*histGroup{}
	var out []*histGroup
	for _, samp := range samples {
		lbls, err := parseLabels(samp.labels)
		if err != nil {
			continue
		}
		lbls = lbls.without(leLabel)
		key := lbls.String()
		g := groups[key]
		if g == nil {
			g = &histGroup{lbls: lbls, key: key, first: samp, buckets: make([]uint64, nBounds)}
			groups[key] = g
			out = append(out, g)
		}
		switch {
		case samp.bucket == nBounds: // the +Inf stream carries the totals
			g.count, g.sum, g.hasInf = samp.count, samp.value, true
		case samp.bucket < nBounds:
			g.buckets[samp.bucket] = samp.count
		}
	}
	return out
}

// renderHistogram writes one cumulative OTLP histogram point per regrouped
// label set (regroupHistogram), converting the stored cumulative bucket
// counts to the absolute per-bucket counts OTLP wants (accumulateBuckets).
func renderHistogram(m pmetric.Metric, s *series, samples []sample, ts time.Time) {
	now := pcommon.Timestamp(ts.UnixNano())
	bounds := s.buckets[:len(s.buckets)-1] // drop +Inf

	hist := m.SetEmptyHistogram()
	hist.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)

	for _, g := range regroupHistogram(samples, len(bounds)) {
		dp := hist.DataPoints().AppendEmpty()
		dp.SetStartTimestamp(startOf(g.first, ts))
		dp.SetTimestamp(now)
		putLabels(dp.Attributes(), g.key)
		dp.ExplicitBounds().FromRaw(bounds)
		if g.hasInf {
			dp.SetSum(g.sum)
			dp.SetCount(g.count)
		}
		dp.BucketCounts().FromRaw(accumulateBuckets(g))
	}
}

// accumulateBuckets converts a group's cumulative bucket counts into the
// absolute per-bucket counts OTLP wants: a value counted in its bucket was
// also counted in every higher one, so each slot is its cumulative count
// minus the previous bound's, and the +Inf slot is the total minus the last
// bound's.
func accumulateBuckets(g *histGroup) []uint64 {
	counts := make([]uint64, len(g.buckets)+1)
	var prev uint64
	for i, c := range g.buckets {
		counts[i] = c - prev
		prev = c
	}
	counts[len(g.buckets)] = g.count - prev
	return counts
}

// putLabels parses a serialized label set and copies its pairs into a pdata map.
func putLabels(dst pcommon.Map, serialized string) {
	lbls, _ := parseLabels(serialized)
	dst.EnsureCapacity(len(lbls))
	for _, e := range lbls {
		if e.key != "" && e.value != "" {
			dst.PutStr(e.key, e.value)
		}
	}
}
