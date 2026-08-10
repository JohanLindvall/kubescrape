package metrics

// A cumulative point's StartTimeUnixNano must never be AFTER its own
// TimeUnixNano. Both exports here read their clock once, before the first
// snapshot, so a sample admitted in that window carries a start stamped from a
// LATER clock read on a producer goroutine — see startOf, and
// internal/agent/cumagg's ClampStart, which is the same fix on the two
// aggregators this package could not be reached from.
//
// Counters hid it: streamStart backdates their stream three minutes
// (counterBaselineSeconds), so the inversion needs a window three minutes wide.
// A gauge, histogram or summary stamps the admission instant, so a window of
// microseconds is enough — which is why these tests pin every kind, not the one
// that broke.
//
// The window is reproduced here by pinning the series clock AHEAD of the wall
// clock the exports read rather than by racing it: the invariant is what is
// under test, and a microsecond race cannot be asserted on.

import (
	"context"
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

// forEachPointStart visits (start, timestamp) of every data point of every
// kind — number, histogram and summary alike, which the number-only helper in
// starttime_test.go does not reach.
func forEachPointStart(exp *capExporter, fn func(name string, start, ts pcommon.Timestamp)) {
	for _, md := range exp.md {
		rms := md.ResourceMetrics()
		for i := 0; i < rms.Len(); i++ {
			sms := rms.At(i).ScopeMetrics()
			for j := 0; j < sms.Len(); j++ {
				ms := sms.At(j).Metrics()
				for k := 0; k < ms.Len(); k++ {
					m := ms.At(k)
					switch m.Type() {
					case pmetric.MetricTypeSum:
						dps := m.Sum().DataPoints()
						for p := 0; p < dps.Len(); p++ {
							fn(m.Name(), dps.At(p).StartTimestamp(), dps.At(p).Timestamp())
						}
					case pmetric.MetricTypeGauge:
						dps := m.Gauge().DataPoints()
						for p := 0; p < dps.Len(); p++ {
							fn(m.Name(), dps.At(p).StartTimestamp(), dps.At(p).Timestamp())
						}
					case pmetric.MetricTypeHistogram:
						dps := m.Histogram().DataPoints()
						for p := 0; p < dps.Len(); p++ {
							fn(m.Name(), dps.At(p).StartTimestamp(), dps.At(p).Timestamp())
						}
					case pmetric.MetricTypeSummary:
						dps := m.Summary().DataPoints()
						for p := 0; p < dps.Len(); p++ {
							fn(m.Name(), dps.At(p).StartTimestamp(), dps.At(p).Timestamp())
						}
					}
				}
			}
		}
	}
}

// A sample admitted after the export read its clock must not render a start
// ahead of the point it is stamped beside: OTLP requires a cumulative point's
// start at or before its time, and a consumer may reject the payload or read
// the inversion as a reset of a series that never reset.
func TestStartNeverExceedsThePointTimestamp(t *testing.T) {
	// The series clock runs an hour ahead of the wall clock Export reads, which
	// is the admitted-after-ts window widened until it is deterministic.
	setTimeForTest(time.Now().Add(time.Hour))
	defer testEpoch.Store(0)

	set, err := newTestSet([]Dynamic{
		{Name: "lines_total", Type: CounterType, Value: "1", Match: []string{"m=1"}},
		{Name: "live_gauge", Type: GaugeType, Value: "v", Match: []string{"m=1"}},
		{Name: "dur_hist", Type: HistogramType, Value: "v", Match: []string{"m=1"}, Buckets: []float64{1, 10}},
		{Name: "dur_summary", Type: SummaryType, Value: "v", Match: []string{"m=1"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	set.Add(valuesFrom(map[string]string{"v": "3"}), labelsFrom(map[string]string{"m": "1"}), noRes(), "")

	exp := &capExporter{}
	if err := set.Export(context.Background(), exp, 0); err != nil {
		t.Fatal(err)
	}

	points := 0
	forEachPointStart(exp, func(name string, start, ts pcommon.Timestamp) {
		points++
		if start > ts {
			t.Errorf("%s: start %d is AFTER the point timestamp %d — an inverted cumulative point, "+
				"which OTLP forbids and a consumer may reject or read as a reset", name, start, ts)
		}
	})
	if points == 0 {
		t.Fatal("no data points exported")
	}
}

// The self-metrics Registry renders through the same startOf and reads its
// clock in the same place (Registry.Export), so it gets the same guarantee.
func TestRegistryStartNeverExceedsThePointTimestamp(t *testing.T) {
	setTimeForTest(time.Now().Add(time.Hour))
	defer testEpoch.Store(0)

	r := NewRegistry()
	// A histogram, because it is a non-counter: the counter's three-minute
	// backdate would hide the inversion at anything under that offset.
	h := r.HistogramVec("kubescrape_test_seconds", "help", []float64{1, 10}, "kind")
	h.WithLabelValues("x").Observe(2)
	g := r.Gauge("kubescrape_test_gauge", "help")
	g.Set(1)

	// A Registry series takes the coarse clock, not the injected one, so pin
	// each one's clock directly — the field newSeries fills from seriesSpec.
	for _, s := range r.series {
		s.now = testNow
		s.mu.Lock()
		for _, samp := range s.db {
			samp.start = testNow()
			samp.when = testNow()
		}
		s.mu.Unlock()
	}

	exp := &capExporter{}
	if err := r.Export(context.Background(), exp, pcommon.NewResource()); err != nil {
		t.Fatal(err)
	}
	points := 0
	forEachPointStart(exp, func(name string, start, ts pcommon.Timestamp) {
		points++
		if start > ts {
			t.Errorf("%s: self-metric start %d is AFTER the point timestamp %d", name, start, ts)
		}
	})
	if points == 0 {
		t.Fatal("no data points exported")
	}
}
