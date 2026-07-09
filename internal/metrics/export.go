package metrics

import (
	"context"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

// Exporter sends OTLP metrics; implemented by the agent's otlpexport.Client.
type Exporter interface {
	ExportMetrics(ctx context.Context, md pmetric.Metrics) error
}

var metricsMarshaler pmetric.ProtoMarshaler

// Run exports the set's metrics to exp every interval until ctx is done.
func (s *DynamicMetricSet) Run(ctx context.Context, exp Exporter, interval time.Duration, maxBytes int) {
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

// Export sends the current value of every configured metric as OTLP, in chunks
// kept under maxBytes (0 = a single payload). Rules sharing a series export it
// once.
func (s *DynamicMetricSet) Export(ctx context.Context, exp Exporter, maxBytes int) error {
	if s == nil {
		return nil
	}
	ts := time.Now()
	md := pmetric.NewMetrics()
	scope := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty()

	flush := func(force bool) error {
		if scope.Metrics().Len() == 0 {
			return nil
		}
		if !force && maxBytes > 0 && metricsMarshaler.MetricsSize(md) < maxBytes {
			return nil
		}
		err := exp.ExportMetrics(ctx, md)
		scope.Metrics().RemoveIf(func(pmetric.Metric) bool { return true })
		return err
	}

	seen := make(map[*series]bool, len(s.rules))
	for _, rule := range s.rules {
		if rule.series.name == "" || seen[rule.series] {
			continue
		}
		seen[rule.series] = true
		renderSeries(scope, rule.series, ts)
		if err := flush(false); err != nil {
			return err
		}
	}
	return flush(true)
}

// renderSeries appends one metric and its data points to scope.
func renderSeries(scope pmetric.ScopeMetrics, s *series, ts time.Time) {
	samples := s.snapshot()

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

// renderNumber writes gauge or counter samples as number data points. Counters
// additionally emit two synthetic zero points before a series' first real
// point so downstream rate() has a baseline (one minute is too short given
// timestamp normalization — Mimir takes the max value for a counter).
func renderNumber(dps pmetric.NumberDataPointSlice, samples []sample, ts time.Time, counter bool) {
	now := pcommon.Timestamp(ts.UnixNano())
	for _, s := range samples {
		if counter && s.initial {
			for back := 2; back >= 1; back-- {
				prev := pcommon.Timestamp(ts.Add(time.Duration(-back) * time.Minute).UnixNano())
				zero := dps.AppendEmpty()
				zero.SetDoubleValue(0)
				zero.SetStartTimestamp(prev)
				zero.SetTimestamp(prev)
				putLabels(zero.Attributes(), s.labels)
			}
		}
		dp := dps.AppendEmpty()
		dp.SetDoubleValue(s.value)
		dp.SetStartTimestamp(now)
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
		dp.SetStartTimestamp(now)
		dp.SetTimestamp(now)
		dp.SetCount(s.count)
		dp.SetSum(s.value)
		putLabels(dp.Attributes(), s.labels)
	}
}

// renderHistogram regroups a histogram's per-bucket samples (keyed by their
// labels without "le") into cumulative OTLP histogram points, converting the
// stored cumulative bucket counts to the absolute per-bucket counts OTLP wants.
func renderHistogram(m pmetric.Metric, s *series, samples []sample, ts time.Time) {
	now := pcommon.Timestamp(ts.UnixNano())
	bounds := s.buckets[:len(s.buckets)-1] // drop +Inf

	hist := m.SetEmptyHistogram()
	hist.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)

	points := map[uint64]pmetric.HistogramDataPoint{}
	counts := map[uint64][]uint64{}
	for _, sample := range samples {
		lbls, _ := parseLabels(sample.labels)
		lbls = lbls.without(leLabel)
		key := lbls.hash()

		dp, ok := points[key]
		if !ok {
			dp = hist.DataPoints().AppendEmpty()
			dp.SetStartTimestamp(now)
			dp.SetTimestamp(now)
			putLabels(dp.Attributes(), lbls.String())
			dp.ExplicitBounds().FromRaw(bounds)
			points[key] = dp
			counts[key] = make([]uint64, len(bounds)+1)
		}
		accumulateBucket(counts[key], sample)
		if sample.bucket == len(bounds) { // the +Inf bucket carries totals
			dp.SetSum(sample.value)
			dp.SetCount(sample.count)
		}
	}
	for key, dp := range points {
		dp.BucketCounts().FromRaw(counts[key])
	}
}

// accumulateBucket converts one cumulative bucket sample into absolute counts:
// the value belongs to its bucket but was also counted in every higher one, so
// subtract it from the next.
func accumulateBucket(counts []uint64, s sample) {
	if s.count == 0 {
		return
	}
	if s.bucket < len(counts)-1 {
		counts[s.bucket+1] -= s.count
	}
	counts[s.bucket] += s.count
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
