package metrics

// The OTLP output half BOTH products share: the Exporter they send through, the
// instrumentation scope they stamp, the per-series render (renderSeries and the
// per-kind helpers under it) and the cadence their export-failure lines restate
// at. DynamicMetricSet's export loop and retention live in export.go and the
// Registry's in registry.go; both build their payload through renderSeries, so a
// change here changes the self-metrics wire format and the log-derived one alike.

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

// reWarnInterval is how often a persisting export or retention failure
// restates itself. Long against the export interval (so a node contributes at
// most a line per five minutes to a fleet-wide outage) and short against an
// operator's attention.
const reWarnInterval = 5 * time.Minute

// renderSeries appends the given samples' data points to scope, reusing the
// Metric an earlier call for the same name already created: a retained
// (undelivered) generation and the fresh snapshot of one series render into
// ONE Metric, because two same-named Metrics in one ScopeMetrics violate
// OTLP's one-metric-per-name rule and a strict consumer may reject the chunk
// or dedupe order-dependently. Generations arrive oldest-first (mergeRetry
// prepends), so a series' points stay in ascending timestamp order. The
// linear name scan is bounded by the configured metric count.
func renderSeries(scope pmetric.ScopeMetrics, s *series, samples []sample, ts time.Time) {
	var m pmetric.Metric
	found := false
	ms := scope.Metrics()
	for i := 0; i < ms.Len(); i++ {
		if ms.At(i).Name() == s.name {
			m, found = ms.At(i), true
			break
		}
	}
	if !found {
		m = ms.AppendEmpty()
		m.SetName(s.name)
		m.SetDescription(s.desc)
	}

	switch s.kind {
	case kindHistogram:
		renderHistogram(m, s, samples, ts)
	case kindSummary:
		renderSummary(m, samples, ts)
	case kindGauge:
		if m.Type() != pmetric.MetricTypeGauge {
			m.SetEmptyGauge()
		}
		renderNumber(m.Gauge().DataPoints(), samples, ts, false)
	default: // counter
		if m.Type() != pmetric.MetricTypeSum {
			sum := m.SetEmptySum()
			sum.SetIsMonotonic(true)
			sum.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
		}
		renderNumber(m.Sum().DataPoints(), samples, ts, true)
	}
}

// startOf renders a sample's start-of-accumulation stamp (sample.start, epoch
// seconds) as an OTLP timestamp, BOUNDED BY the point timestamp it is about to
// be stamped beside. A sample that somehow never went through admit falls back
// to ts, which is the old always-a-reset behaviour — wrong, but never AHEAD of
// the point's own timestamp. (A counter's synthetic baseline zeros are not
// bounds for it: renderNumber drops any zero that would fall at or before the
// start instead of moving the start back to meet it.)
//
// The clamp exists for one narrow race, the same one internal/agent/cumagg's
// ClampStart was written for (this package cannot call it: cumagg imports
// transform, which imports obs, which imports this package). Both exports read
// their clock ONCE, before the first snapshot (DynamicMetricSet.Export and
// Registry.Export), so a sample ADMITTED in that window — its start stamped from a later read of
// the coarse clock, on a producer goroutine — renders StartTimestamp >
// Timestamp on its very first export. COUNTERS were mostly safe on their own
// (counterBaselineSeconds backdates the stream up to three minutes), which is
// why this went unseen; a gauge, histogram or summary stamps the admission
// instant and inverts. OTLP requires a cumulative point's start at or before its time,
// and a consumer may reject the payload or read the inversion as a reset.
//
// Clamping the STAMP is the OTLP first-point spelling of "the stream began
// now"; sample.start itself is untouched, so the true start renders from the
// next export on, whose ts lies past it.
func startOf(s sample, ts time.Time) pcommon.Timestamp {
	now := pcommon.Timestamp(ts.UnixNano())
	if s.start <= 0 {
		return now
	}
	if start := pcommon.Timestamp(time.Unix(s.start, 0).UnixNano()); start < now {
		return start
	}
	return now
}

// baselineBack is how far before the export the OLDER of a counter's two
// synthetic zeros is stamped (the younger is at half of it).
const baselineBack = 2 * time.Minute

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
// threw away. All three points now carry the stream's single start, and no
// zero is ever stamped at or before it (baselineZeros).
func renderNumber(dps pmetric.NumberDataPointSlice, samples []sample, ts time.Time, counter bool) {
	now := pcommon.Timestamp(ts.UnixNano())
	for _, s := range samples {
		start := startOf(s, ts)
		if counter && s.initial {
			baselineZeros(dps, s.labels, start, ts)
		}
		dp := dps.AppendEmpty()
		dp.SetDoubleValue(s.value)
		dp.SetStartTimestamp(start)
		dp.SetTimestamp(now)
		putLabels(dp.Attributes(), s.labels)
	}
}

// baselineZeros appends a first counter point's synthetic zeros, at
// baselineBack and baselineBack/2 before ts — but only those strictly AFTER the
// stream's start, since a zero at or before it claims a value for time the
// stream does not cover. The start is floored at the series' construction
// (streamStart), so in a process's first three minutes one or both regular
// slots can fall before it; then a single zero goes one second after the start
// instead (or halfway to ts, when ts is closer than that), strictly between the
// two. That zero can sit well under a minute before the first real point —
// close enough that timestamp normalization may fold the two together — which
// is the accepted cost: the alternative is a zero stamped BEFORE the previous
// process's final samples of the same series, which a backend either refuses
// outright or stores as a fake reset that increase() counts the pre-restart
// total through a second time.
func baselineZeros(dps pmetric.NumberDataPointSlice, labels string, start pcommon.Timestamp, ts time.Time) {
	zero := func(at pcommon.Timestamp) {
		dp := dps.AppendEmpty()
		dp.SetDoubleValue(0)
		dp.SetStartTimestamp(start)
		dp.SetTimestamp(at)
		putLabels(dp.Attributes(), labels)
	}
	emitted := false
	for _, back := range [...]time.Duration{baselineBack, baselineBack / 2} {
		if at := pcommon.Timestamp(ts.Add(-back).UnixNano()); at > start {
			zero(at)
			emitted = true
		}
	}
	if end := pcommon.Timestamp(ts.UnixNano()); !emitted && end > start {
		if at := start + min(pcommon.Timestamp(time.Second), (end-start)/2); at > start && at < end {
			zero(at)
		}
	}
}

// renderSummary writes summary samples as OTLP summary data points carrying the
// running count and sum (no quantiles).
func renderSummary(m pmetric.Metric, samples []sample, ts time.Time) {
	now := pcommon.Timestamp(ts.UnixNano())
	// Reuse an earlier generation's shape — SetEmptySummary would wipe its
	// points (see renderSeries).
	if m.Type() != pmetric.MetricTypeSummary {
		m.SetEmptySummary()
	}
	dps := m.Summary().DataPoints()
	for _, s := range samples {
		dp := dps.AppendEmpty()
		dp.SetStartTimestamp(startOf(s, ts))
		dp.SetTimestamp(now)
		dp.SetCount(s.count)
		dp.SetSum(s.value)
		putLabels(dp.Attributes(), s.labels)
	}
}

// renderHistogram writes one cumulative OTLP histogram point per sample — a
// histogram sample IS one label set's whole distribution (sample.counts) —
// converting the stored cumulative bucket counts to the absolute per-bucket
// counts OTLP wants (absoluteBuckets).
func renderHistogram(m pmetric.Metric, s *series, samples []sample, ts time.Time) {
	now := pcommon.Timestamp(ts.UnixNano())

	// Reuse an earlier generation's shape — SetEmpty* would wipe its points
	// (see renderSeries).
	if m.Type() != pmetric.MetricTypeHistogram {
		m.SetEmptyHistogram().SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
	}
	hist := m.Histogram()

	for _, samp := range samples {
		dp := hist.DataPoints().AppendEmpty()
		dp.SetStartTimestamp(startOf(samp, ts))
		dp.SetTimestamp(now)
		putLabels(dp.Attributes(), samp.labels)
		dp.ExplicitBounds().FromRaw(s.bounds) // FromRaw copies
		dp.SetSum(samp.value)
		dp.SetCount(samp.count)
		dp.BucketCounts().FromRaw(absoluteBuckets(samp.counts, samp.count))
	}
}

// absoluteBuckets converts a sample's cumulative bucket counts into the
// absolute per-bucket counts OTLP wants: a value counted in its bucket was
// also counted in every higher one, so each slot is its cumulative count
// minus the previous bound's, and the +Inf slot is the total minus the last
// bound's. total >= counts[last] by construction — both start at zero in
// admit and record is their only writer, incrementing them together — so the
// unsigned subtraction cannot underflow (the partial-family case the old per-bucket
// layout had to defend against is unrepresentable in one sample).
func absoluteBuckets(counts []uint64, total uint64) []uint64 {
	out := make([]uint64, len(counts)+1)
	var prev uint64
	for i, c := range counts {
		out[i] = c - prev
		prev = c
	}
	out[len(counts)] = total - prev
	return out
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
