package pdatacheck

import (
	"strings"
	"testing"

	"go.opentelemetry.io/collector/pdata/pmetric"
)

// The detector is what several producers' tests assert through, so a bug in it
// — returning nil unconditionally being the obvious one — would make every one
// of those tests pass vacuously while the invariant they name went unchecked.
// It is pinned in both directions: it must FLAG an empty metric of every type
// the wire can carry, and it must not flag a populated one of any of them.
func TestEmptyMetricsFlagsEveryTypeAndOnlyWhenEmpty(t *testing.T) {
	types := []struct {
		name string
		set  func(m pmetric.Metric)
		put  func(m pmetric.Metric)
	}{
		{"gauge",
			func(m pmetric.Metric) { m.SetEmptyGauge() },
			func(m pmetric.Metric) { m.Gauge().DataPoints().AppendEmpty().SetIntValue(1) }},
		{"sum",
			func(m pmetric.Metric) { m.SetEmptySum() },
			func(m pmetric.Metric) { m.Sum().DataPoints().AppendEmpty().SetIntValue(1) }},
		{"histogram",
			func(m pmetric.Metric) { m.SetEmptyHistogram() },
			func(m pmetric.Metric) { m.Histogram().DataPoints().AppendEmpty().SetCount(1) }},
		{"exp_histogram",
			func(m pmetric.Metric) { m.SetEmptyExponentialHistogram() },
			func(m pmetric.Metric) { m.ExponentialHistogram().DataPoints().AppendEmpty().SetCount(1) }},
		{"summary",
			func(m pmetric.Metric) { m.SetEmptySummary() },
			func(m pmetric.Metric) { m.Summary().DataPoints().AppendEmpty().SetCount(1) }},
		// An untyped metric — a descriptor a sender created and never recorded
		// into — has no data points by definition and must flag too.
		{"untyped", func(pmetric.Metric) {}, nil},
	}

	for _, tc := range types {
		t.Run(tc.name+"/empty_is_flagged", func(t *testing.T) {
			md := pmetric.NewMetrics()
			m := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
			m.SetName("m_" + tc.name)
			tc.set(m)

			got := EmptyMetrics(md)
			if len(got) != 1 {
				t.Fatalf("EmptyMetrics = %v, want exactly one entry", got)
			}
			if !strings.Contains(got[0], "m_"+tc.name) {
				t.Errorf("entry %q does not name the metric", got[0])
			}
			if n := MetricPointCount(m); n != 0 {
				t.Errorf("MetricPointCount = %d, want 0", n)
			}
		})

		if tc.put == nil {
			continue // an untyped metric cannot be populated
		}
		t.Run(tc.name+"/populated_is_not_flagged", func(t *testing.T) {
			md := pmetric.NewMetrics()
			m := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
			m.SetName("m_" + tc.name)
			tc.set(m)
			tc.put(m)

			if got := EmptyMetrics(md); len(got) != 0 {
				t.Errorf("EmptyMetrics = %v, want none", got)
			}
			if n := MetricPointCount(m); n != 1 {
				t.Errorf("MetricPointCount = %d, want 1", n)
			}
		})
	}
}

// The path qualification is what makes a failure actionable in a payload
// holding hundreds of metrics, so it is asserted rather than assumed.
func TestEmptyMetricsReportsWhereTheMetricIs(t *testing.T) {
	md := pmetric.NewMetrics()
	md.ResourceMetrics().AppendEmpty() // rm[0]: no scopes
	rm := md.ResourceMetrics().AppendEmpty()
	rm.ScopeMetrics().AppendEmpty() // sm[0]: no metrics
	sm := rm.ScopeMetrics().AppendEmpty()
	good := sm.Metrics().AppendEmpty()
	good.SetName("good")
	good.SetEmptyGauge().DataPoints().AppendEmpty().SetIntValue(1)
	bad := sm.Metrics().AppendEmpty()
	bad.SetName("bad")
	bad.SetEmptySum()

	got := EmptyMetrics(md)
	if len(got) != 1 {
		t.Fatalf("EmptyMetrics = %v, want exactly one entry", got)
	}
	if !strings.Contains(got[0], "rm[1]/sm[1]/metric[1]") || !strings.Contains(got[0], `"bad"`) {
		t.Errorf("entry %q does not locate the metric as rm[1]/sm[1]/metric[1] \"bad\"", got[0])
	}
}

// A clean payload returns an EMPTY slice, which is what lets every caller read
// `if len(names) > 0`.
func TestACleanPayloadReturnsNothing(t *testing.T) {
	md := pmetric.NewMetrics()
	m := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
	m.SetName("ok")
	m.SetEmptyGauge().DataPoints().AppendEmpty().SetDoubleValue(1)
	if got := EmptyMetrics(md); len(got) != 0 {
		t.Errorf("EmptyMetrics = %v, want none", got)
	}
	if got := EmptyMetrics(pmetric.NewMetrics()); len(got) != 0 {
		t.Errorf("EmptyMetrics(empty payload) = %v, want none", got)
	}
}
