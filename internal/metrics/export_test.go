package metrics

import (
	"context"
	"strconv"
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/pmetric"
)

// capExporter records exported metrics payloads.
type capExporter struct{ md []pmetric.Metrics }

func (c *capExporter) ExportMetrics(_ context.Context, md pmetric.Metrics) error {
	cp := pmetric.NewMetrics()
	md.CopyTo(cp)
	c.md = append(c.md, cp)
	return nil
}

// find returns the last-exported metric with the given name.
func (c *capExporter) find(name string) (pmetric.Metric, bool) {
	for _, md := range c.md {
		rms := md.ResourceMetrics()
		for i := 0; i < rms.Len(); i++ {
			sms := rms.At(i).ScopeMetrics()
			for j := 0; j < sms.Len(); j++ {
				ms := sms.At(j).Metrics()
				for k := 0; k < ms.Len(); k++ {
					if ms.At(k).Name() == name {
						return ms.At(k), true
					}
				}
			}
		}
	}
	return pmetric.Metric{}, false
}

func labelsFrom(m map[string]string) func(string) string {
	return func(k string) string { return m[k] }
}

func valuesFrom(m map[string]string) func(string) float64 {
	return func(k string) float64 {
		f, _ := strconv.ParseFloat(m[k], 64)
		return f
	}
}

func exportOne(t *testing.T, set *DynamicMetricSet, name string) pmetric.Metric {
	t.Helper()
	exp := &capExporter{}
	if err := set.Export(context.Background(), exp, 0); err != nil {
		t.Fatal(err)
	}
	m, ok := exp.find(name)
	if !ok {
		t.Fatalf("%s not exported", name)
	}
	return m
}

func TestDynamicCounter(t *testing.T) {
	setTimeForTest(time.Unix(1_700_000_000, 0))
	defer testEpoch.Store(0)

	set, err := NewDynamicMetricSet([]Dynamic{{
		Name:   "test_http_requests_total",
		Type:   CounterType,
		Value:  "1",
		Match:  []string{"level=info"},
		Labels: []string{"status=$http_status", "method"},
	}})
	if err != nil {
		t.Fatal(err)
	}

	set.Add(nil, labelsFrom(map[string]string{"level": "info", "http_status": "200", "method": "GET"}), "")
	set.Add(nil, labelsFrom(map[string]string{"level": "info", "http_status": "500", "method": "GET"}), "")
	set.Add(nil, labelsFrom(map[string]string{"level": "debug", "http_status": "200"}), "")

	m := exportOne(t, set, "test_http_requests_total")
	if m.Type() != pmetric.MetricTypeSum || !m.Sum().IsMonotonic() {
		t.Fatalf("type = %v", m.Type())
	}
	// Two distinct series (status 200 and 500), each counted once (synthetic
	// zero points aside — count real points by value).
	series := map[string]float64{}
	dps := m.Sum().DataPoints()
	for i := 0; i < dps.Len(); i++ {
		dp := dps.At(i)
		st, _ := dp.Attributes().Get("status")
		if dp.DoubleValue() > 0 {
			series[st.Str()] = dp.DoubleValue()
		}
	}
	if series["200"] != 1 || series["500"] != 1 {
		t.Fatalf("series = %v (want 200:1 500:1)", series)
	}
}

func TestDynamicSummary(t *testing.T) {
	setTimeForTest(time.Unix(1_700_000_200, 0))
	defer testEpoch.Store(0)

	set, err := NewDynamicMetricSet([]Dynamic{{
		Name:   "test_bytes_summary",
		Type:   SummaryType,
		Value:  "bytes",
		Match:  []string{"op=write"},
		Labels: []string{"op"},
	}})
	if err != nil {
		t.Fatal(err)
	}
	for _, b := range []string{"10", "20", "30"} { // count 3, sum 60
		set.Add(valuesFrom(map[string]string{"bytes": b}),
			labelsFrom(map[string]string{"op": "write", "bytes": b}), "")
	}

	m := exportOne(t, set, "test_bytes_summary")
	if m.Type() != pmetric.MetricTypeSummary {
		t.Fatalf("type = %v", m.Type())
	}
	dps := m.Summary().DataPoints()
	if dps.Len() != 1 {
		t.Fatalf("data points = %d", dps.Len())
	}
	dp := dps.At(0)
	if dp.Count() != 3 {
		t.Errorf("count = %d, want 3", dp.Count())
	}
	if dp.Sum() != 60 {
		t.Errorf("sum = %v, want 60", dp.Sum())
	}
}

func TestDynamicHistogram(t *testing.T) {
	setTimeForTest(time.Unix(1_700_000_100, 0))
	defer testEpoch.Store(0)

	set, err := NewDynamicMetricSet([]Dynamic{{
		Name:    "test_latency_seconds",
		Type:    HistogramType,
		Value:   "duration",
		Buckets: []float64{0.1, 0.5, 1},
		Match:   []string{"op=query"},
		Labels:  []string{"op"},
	}})
	if err != nil {
		t.Fatal(err)
	}
	// Observations 0.05, 0.3, 0.7 → cumulative buckets le0.1:1, le0.5:2, le1:3.
	for _, d := range []string{"0.05", "0.3", "0.7"} {
		set.Add(valuesFrom(map[string]string{"duration": d}),
			labelsFrom(map[string]string{"op": "query", "duration": d}), "")
	}

	m := exportOne(t, set, "test_latency_seconds")
	if m.Type() != pmetric.MetricTypeHistogram {
		t.Fatalf("type = %v", m.Type())
	}
	dps := m.Histogram().DataPoints()
	if dps.Len() != 1 {
		t.Fatalf("data points = %d", dps.Len())
	}
	dp := dps.At(0)
	if dp.Count() != 3 {
		t.Errorf("count = %d, want 3", dp.Count())
	}
	if dp.Sum() < 1.04 || dp.Sum() > 1.06 {
		t.Errorf("sum = %v, want ~1.05", dp.Sum())
	}
	// Absolute (non-cumulative) bucket counts: [le0.1, le0.5, le1, +Inf].
	got := dp.BucketCounts().AsRaw()
	if len(got) != 4 || got[0] != 1 || got[1] != 1 || got[2] != 1 || got[3] != 0 {
		t.Errorf("bucket counts = %v, want [1 1 1 0]", got)
	}
}

func TestSharedSeriesByName(t *testing.T) {
	setTimeForTest(time.Unix(1_700_000_300, 0))
	defer testEpoch.Store(0)

	// Two rules feeding the same metric name must share one series and export
	// once.
	set, err := NewDynamicMetricSet([]Dynamic{
		{Name: "events_total", Type: CounterType, Value: "1", Match: []string{"kind=a"}, Labels: []string{"kind"}},
		{Name: "events_total", Type: CounterType, Value: "1", Match: []string{"kind=b"}, Labels: []string{"kind"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	set.Add(nil, labelsFrom(map[string]string{"kind": "a"}), "")
	set.Add(nil, labelsFrom(map[string]string{"kind": "b"}), "")

	exp := &capExporter{}
	if err := set.Export(context.Background(), exp, 0); err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, md := range exp.md {
		rms := md.ResourceMetrics()
		for i := 0; i < rms.Len(); i++ {
			sms := rms.At(i).ScopeMetrics()
			for j := 0; j < sms.Len(); j++ {
				ms := sms.At(j).Metrics()
				for k := 0; k < ms.Len(); k++ {
					if ms.At(k).Name() == "events_total" {
						count++
					}
				}
			}
		}
	}
	if count != 1 {
		t.Errorf("events_total exported %d times, want 1", count)
	}
}
