package otlpsplit

import (
	"fmt"
	"testing"

	"go.opentelemetry.io/collector/pdata/pmetric"
)

// emptyMetricNames names every metric in md carrying zero data points.
//
// Deliberately written out here rather than imported from the repo's shared
// internal/pdatacheck helper: this is a pkg/ package, and an import of
// internal/ — even from a _test file — is the first step of the mistake that
// would break every external consumer of it. Ten lines is a cheap boundary.
func emptyMetricNames(md pmetric.Metrics) []string {
	var out []string
	rms := md.ResourceMetrics()
	for i := 0; i < rms.Len(); i++ {
		sms := rms.At(i).ScopeMetrics()
		for j := 0; j < sms.Len(); j++ {
			ms := sms.At(j).Metrics()
			for k := 0; k < ms.Len(); k++ {
				m := ms.At(k)
				n := 0
				switch m.Type() {
				case pmetric.MetricTypeGauge:
					n = m.Gauge().DataPoints().Len()
				case pmetric.MetricTypeSum:
					n = m.Sum().DataPoints().Len()
				case pmetric.MetricTypeHistogram:
					n = m.Histogram().DataPoints().Len()
				case pmetric.MetricTypeExponentialHistogram:
					n = m.ExponentialHistogram().DataPoints().Len()
				case pmetric.MetricTypeSummary:
					n = m.Summary().DataPoints().Len()
				}
				if n == 0 {
					out = append(out, fmt.Sprintf("part metric %q (type %s)", m.Name(), m.Type()))
				}
			}
		}
	}
	return out
}

// Splitting is where an empty metric could be MANUFACTURED rather than merely
// forwarded: an over-cap metric family is split further by DATA POINT, and the
// shell is recreated in each part to carry the points assigned to it. A shell
// emitted for a part that received no point would be a metric with no data
// points that no producer ever built and nothing downstream would reject.
//
// The caps are chosen to land the split at every granularity the function has
// — by resource, by metric, by data point, and a single leaf over the cap
// going alone — because the by-point arm is only exercised by a cap tight
// enough to cut inside one family.
func TestSplittingNeverManufacturesAnEmptyMetric(t *testing.T) {
	for _, shape := range []struct {
		name string
		md   func() pmetric.Metrics
	}{
		{"gauges", func() pmetric.Metrics { return manyPointMetrics(pmetric.MetricTypeGauge) }},
		{"sums", func() pmetric.Metrics { return manyPointMetrics(pmetric.MetricTypeSum) }},
		{"histograms", func() pmetric.Metrics { return manyPointMetrics(pmetric.MetricTypeHistogram) }},
		{"summaries", func() pmetric.Metrics { return manyPointMetrics(pmetric.MetricTypeSummary) }},
		{"exp_histograms", func() pmetric.Metrics {
			return manyPointMetrics(pmetric.MetricTypeExponentialHistogram)
		}},
	} {
		for _, cap := range []int{64, 300, 900, 2048, 8192, 1 << 20} {
			t.Run(fmt.Sprintf("%s/cap=%d", shape.name, cap), func(t *testing.T) {
				in := shape.md()
				parts := Metrics(in, cap)
				if len(parts) == 0 {
					t.Fatal("a non-empty input must never yield zero parts")
				}
				points := 0
				for _, p := range parts {
					if bad := emptyMetricNames(p); len(bad) > 0 {
						t.Errorf("split produced empty metrics: %v", bad)
					}
					points += p.DataPointCount()
				}
				// Nothing may be lost on the way either, or "no empties" could
				// be satisfied by dropping data.
				if want := in.DataPointCount(); points != want {
					t.Errorf("data points across parts = %d, want %d", points, want)
				}
			})
		}
	}
}

// manyPointMetrics builds two resources x two scopes x three metrics of the
// given type, each carrying eight data points with attributes fat enough that
// a tight cap has to cut inside a single family.
func manyPointMetrics(kind pmetric.MetricType) pmetric.Metrics {
	md := pmetric.NewMetrics()
	for r := 0; r < 2; r++ {
		rm := md.ResourceMetrics().AppendEmpty()
		rm.Resource().Attributes().PutStr("service.name", fmt.Sprintf("svc-%d", r))
		rm.Resource().Attributes().PutStr("k8s.pod.name", fmt.Sprintf("pod-%d-aaaaaaaaaaaaaaaaaaaa", r))
		for s := 0; s < 2; s++ {
			sm := rm.ScopeMetrics().AppendEmpty()
			sm.Scope().SetName(fmt.Sprintf("scope-%d", s))
			for n := 0; n < 3; n++ {
				m := sm.Metrics().AppendEmpty()
				m.SetName(fmt.Sprintf("metric_%d_%d_%d", r, s, n))
				m.SetDescription("a description long enough to matter to the byte estimate")
				m.SetUnit("By")
				for p := 0; p < 8; p++ {
					putPoint(m, kind, p)
				}
			}
		}
	}
	return md
}

func putPoint(m pmetric.Metric, kind pmetric.MetricType, p int) {
	attr := func(a interface{ PutStr(string, string) }) {
		a.PutStr("label", fmt.Sprintf("value-%d-padded-out-so-points-are-not-tiny", p))
	}
	switch kind {
	case pmetric.MetricTypeGauge:
		if m.Type() != kind {
			m.SetEmptyGauge()
		}
		dp := m.Gauge().DataPoints().AppendEmpty()
		dp.SetDoubleValue(float64(p))
		attr(dp.Attributes())
	case pmetric.MetricTypeSum:
		if m.Type() != kind {
			m.SetEmptySum()
		}
		dp := m.Sum().DataPoints().AppendEmpty()
		dp.SetDoubleValue(float64(p))
		attr(dp.Attributes())
	case pmetric.MetricTypeHistogram:
		if m.Type() != kind {
			m.SetEmptyHistogram()
		}
		dp := m.Histogram().DataPoints().AppendEmpty()
		dp.SetCount(uint64(p))
		dp.ExplicitBounds().FromRaw([]float64{1, 2, 5})
		dp.BucketCounts().FromRaw([]uint64{1, 1, 1, 1})
		attr(dp.Attributes())
	case pmetric.MetricTypeSummary:
		if m.Type() != kind {
			m.SetEmptySummary()
		}
		dp := m.Summary().DataPoints().AppendEmpty()
		dp.SetCount(uint64(p))
		q := dp.QuantileValues().AppendEmpty()
		q.SetQuantile(0.99)
		q.SetValue(float64(p))
		attr(dp.Attributes())
	case pmetric.MetricTypeExponentialHistogram:
		if m.Type() != kind {
			m.SetEmptyExponentialHistogram()
		}
		dp := m.ExponentialHistogram().DataPoints().AppendEmpty()
		dp.SetCount(uint64(p))
		dp.SetScale(2)
		dp.Positive().BucketCounts().FromRaw([]uint64{1, 2, 3})
		attr(dp.Attributes())
	}
}
