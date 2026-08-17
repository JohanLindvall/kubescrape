package cumagg

import (
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

// SumMetric and HistMetric append a metric's SHELL and hand back a data-point
// slice the caller may never write into, so "do not create a metric you have
// nothing to put in" is a contract each aggregator keeps by its own
// arithmetic — spanmetrics by returning before it makes its three shells,
// servicegraph by computing anyClient/anyServer across the whole snapshot
// first. Store.Render owes the guarantee regardless of whether the next
// aggregator built on this store remembers, because a metric with no data
// points is legal OTLP that nothing downstream rejects.
//
// The optimistic caller is simulated directly: shells created before the
// series are known, only one of which gets a point.
func TestRenderDropsShellsTheAggregatorLeftEmpty(t *testing.T) {
	h := &harness{now: time.Unix(1_700_000_000, 0)}
	h.store = NewStore(Options[*series]{
		Scope:     "test",
		Name:      "test metrics",
		Dropped:   &h.dropped,
		Evicted:   &h.evicted,
		Now:       func() time.Time { return h.now },
		NewSeries: func() *series { return &series{} },
		Render: func(sm pmetric.ScopeMetrics, now time.Time) {
			// Every shape a caller could leave behind, created before it is
			// known whether anything will be recorded into them.
			populated := SumMetric(sm, "calls", "", "")
			SumMetric(sm, "forgotten_sum", "", "")
			HistMetric(sm, "forgotten_hist", "")
			sm.Metrics().AppendEmpty().SetName("forgotten_untyped")

			p := populated.AppendEmpty()
			p.SetStartTimestamp(pcommon.NewTimestampFromTime(now))
			p.SetIntValue(7)
		},
		ResetExemplars: (*series).reset,
	})

	md := h.store.Render(pcommon.NewResource(), h.now)

	var names []string
	rms := md.ResourceMetrics()
	for i := 0; i < rms.Len(); i++ {
		sms := rms.At(i).ScopeMetrics()
		for j := 0; j < sms.Len(); j++ {
			ms := sms.At(j).Metrics()
			for k := 0; k < ms.Len(); k++ {
				m := ms.At(k)
				if noDataPoints(m) {
					t.Errorf("metric %q shipped with no data points", m.Name())
				}
				names = append(names, m.Name())
			}
		}
	}
	if len(names) != 1 || names[0] != "calls" {
		t.Errorf("rendered metrics = %v, want exactly [calls]", names)
	}
}

// A render whose shells are ALL empty must yield nothing at all, not an
// envelope: Export short-circuits on a payload with no resources, and that is
// what keeps a quiet interval off the wire entirely.
func TestARenderThatFillsNoShellSendsNothing(t *testing.T) {
	h := &harness{now: time.Unix(1_700_000_000, 0)}
	h.store = NewStore(Options[*series]{
		Scope:     "test",
		Name:      "test metrics",
		Dropped:   &h.dropped,
		Evicted:   &h.evicted,
		Now:       func() time.Time { return h.now },
		NewSeries: func() *series { return &series{} },
		Render: func(sm pmetric.ScopeMetrics, _ time.Time) {
			SumMetric(sm, "calls", "", "")
			HistMetric(sm, "duration", "")
		},
		ResetExemplars: (*series).reset,
	})

	md := h.store.Render(pcommon.NewResource(), h.now)
	if got := md.ResourceMetrics().Len(); got != 0 {
		t.Errorf("resources = %d, want 0 (an all-empty render must send nothing)", got)
	}
	if got := md.DataPointCount(); got != 0 {
		t.Errorf("data points = %d, want 0", got)
	}
}

// noDataPoints is what the prune turns on, so it is pinned in both directions
// rather than trusted: a wrong answer either way is invisible in production —
// too eager silently deletes real measurements, too lax ships the empties this
// whole seam exists to stop.
func TestNoDataPointsAnswersForEveryMetricType(t *testing.T) {
	for _, tc := range []struct {
		name     string
		empty    func(m pmetric.Metric)
		populate func(m pmetric.Metric)
	}{
		{"gauge", func(m pmetric.Metric) { m.SetEmptyGauge() },
			func(m pmetric.Metric) { m.Gauge().DataPoints().AppendEmpty() }},
		{"sum", func(m pmetric.Metric) { m.SetEmptySum() },
			func(m pmetric.Metric) { m.Sum().DataPoints().AppendEmpty() }},
		{"histogram", func(m pmetric.Metric) { m.SetEmptyHistogram() },
			func(m pmetric.Metric) { m.Histogram().DataPoints().AppendEmpty() }},
		{"exp_histogram", func(m pmetric.Metric) { m.SetEmptyExponentialHistogram() },
			func(m pmetric.Metric) { m.ExponentialHistogram().DataPoints().AppendEmpty() }},
		{"summary", func(m pmetric.Metric) { m.SetEmptySummary() },
			func(m pmetric.Metric) { m.Summary().DataPoints().AppendEmpty() }},
		{"untyped", func(pmetric.Metric) {}, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ms := pmetric.NewMetricSlice()
			m := ms.AppendEmpty()
			tc.empty(m)
			if !noDataPoints(m) {
				t.Errorf("noDataPoints on an empty %s = false, want true", tc.name)
			}
			if tc.populate == nil {
				return
			}
			tc.populate(m)
			if noDataPoints(m) {
				t.Errorf("noDataPoints on a populated %s = true, want false", tc.name)
			}
		})
	}
}
