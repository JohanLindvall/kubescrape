package metrics

import (
	"context"
	"errors"
	"strconv"
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

// TestEmptyResourceExport: lines with an EMPTY resource map export under their
// own ResourceMetrics with zero attributes, distinct from a populated resource.
func TestEmptyResourceExport(t *testing.T) {
	setTimeForTest(time.Unix(1_700_700_000, 0))
	defer testEpoch.Store(0)

	set, err := NewDynamicMetricSet([]Dynamic{{
		Name: "lines_total", Type: CounterType, Value: "1", Match: []string{"m=1"},
	}})
	if err != nil {
		t.Fatal(err)
	}
	set.Add(nil, labelsFrom(map[string]string{"m": "1"}), noRes(), "")
	set.Add(nil, labelsFrom(map[string]string{"m": "1"}), res(map[string]string{"k8s.pod.name": "p"}), "")

	exp := &capExporter{}
	if err := set.Export(context.Background(), exp, 0); err != nil {
		t.Fatal(err)
	}
	emptyRMs, podRMs, totalRMs := 0, 0, 0
	for _, md := range exp.md {
		rms := md.ResourceMetrics()
		for i := 0; i < rms.Len(); i++ {
			totalRMs++
			switch rms.At(i).Resource().Attributes().Len() {
			case 0:
				emptyRMs++
			default:
				podRMs++
			}
		}
	}
	if totalRMs != 2 || emptyRMs != 1 || podRMs != 1 {
		t.Fatalf("ResourceMetrics: total %d empty %d pod %d, want 2/1/1", totalRMs, emptyRMs, podRMs)
	}
}

// --- Angle 5: valueRegexp ----------------------------------------------------

// flakyExporter fails the first chunk and records the rest.
type flakyExporter struct {
	calls int
	names []string
}

func (f *flakyExporter) ExportMetrics(_ context.Context, md pmetric.Metrics) error {
	f.calls++
	if f.calls == 1 {
		return errors.New("collector unavailable")
	}
	rms := md.ResourceMetrics()
	for i := 0; i < rms.Len(); i++ {
		if v, ok := rms.At(i).Resource().Attributes().Get("k8s.pod.name"); ok {
			f.names = append(f.names, v.Str())
		}
	}
	return nil
}

// TestExportContinuesPastFailedChunk: snapshot() has already sealed the
// aggregation windows and cleared the counters' initial flag by the time the
// first chunk is sent, so abandoning the remaining chunks on a failure would
// discard observations that no longer exist in the store. The export must keep
// going and report the first error.
func TestExportContinuesPastFailedChunk(t *testing.T) {
	set, err := NewDynamicMetricSet([]Dynamic{{
		Name:  "test_lines_total",
		Type:  CounterType,
		Value: "1",
	}})
	if err != nil {
		t.Fatal(err)
	}
	// Three distinct resources -> three chunks at a 1-byte chunk limit.
	for _, pod := range []string{"pod-a", "pod-b", "pod-c"} {
		set.Add(nil, labelsFrom(nil), res(map[string]string{"k8s.pod.name": pod}), "")
	}

	exp := &flakyExporter{}
	if err := set.Export(context.Background(), exp, 1); err == nil {
		t.Fatal("Export returned nil; the failed chunk must be reported")
	}
	if exp.calls != 3 {
		t.Fatalf("exporter called %d times, want 3: a failing chunk must not abandon the rest", exp.calls)
	}
	if len(exp.names) != 2 {
		t.Fatalf("delivered %v, want the 2 resources after the failing one", exp.names)
	}
}

// resourceOf returns the metric's exported resource attributes for the given
// metric name (the first ResourceMetrics containing it).
func resourceOf(t *testing.T, exp *capExporter, name string) map[string]any {
	t.Helper()
	for _, md := range exp.md {
		rms := md.ResourceMetrics()
		for i := 0; i < rms.Len(); i++ {
			rm := rms.At(i)
			sms := rm.ScopeMetrics()
			for j := 0; j < sms.Len(); j++ {
				ms := sms.At(j).Metrics()
				for k := 0; k < ms.Len(); k++ {
					if ms.At(k).Name() == name {
						return rm.Resource().Attributes().AsRaw()
					}
				}
			}
		}
	}
	t.Fatalf("%s not exported", name)
	return nil
}

func TestLogResourceBecomesResourceAttrs(t *testing.T) {
	setTimeForTest(time.Unix(1_700_200_300, 0))
	defer testEpoch.Store(0)

	// The log line's resource attributes become the metric's OTLP resource; the
	// metric's own DSL labels stay on the data points. Two pods → two resources.
	set, err := NewDynamicMetricSet([]Dynamic{{
		Name:   "http_requests_total",
		Type:   CounterType,
		Value:  "1",
		Match:  []string{"level=info"},
		Labels: []string{"status=$http_status"},
	}})
	if err != nil {
		t.Fatal(err)
	}
	set.Add(nil, labelsFrom(map[string]string{"level": "info", "http_status": "200"}),
		res(map[string]string{"k8s.pod.name": "pod-a", "k8s.namespace.name": "ns"}), "")
	set.Add(nil, labelsFrom(map[string]string{"level": "info", "http_status": "500"}),
		res(map[string]string{"k8s.pod.name": "pod-b", "k8s.namespace.name": "ns"}), "")

	exp := &capExporter{}
	if err := set.Export(context.Background(), exp, 0); err != nil {
		t.Fatal(err)
	}
	// Two distinct resources, one per pod.
	pods := map[string]bool{}
	statusOnResource := false
	for _, md := range exp.md {
		rms := md.ResourceMetrics()
		for i := 0; i < rms.Len(); i++ {
			ra := rms.At(i).Resource().Attributes().AsRaw()
			if p, ok := ra["k8s.pod.name"]; ok {
				pods[p.(string)] = true
			}
			if _, ok := ra["status"]; ok {
				statusOnResource = true
			}
		}
	}
	if !pods["pod-a"] || !pods["pod-b"] {
		t.Errorf("pods on resources = %v, want pod-a and pod-b", pods)
	}
	if statusOnResource {
		t.Error("status (a metric label) leaked onto the resource")
	}
	m := exportOne(t, set, "http_requests_total")
	statusOnDP := false
	dps := m.Sum().DataPoints()
	for i := 0; i < dps.Len(); i++ {
		if _, ok := dps.At(i).Attributes().Get("status"); ok {
			statusOnDP = true
		}
	}
	if !statusOnDP {
		t.Error("status not present as a data-point attribute")
	}
}

func TestResourceLabelsLiftedToResource(t *testing.T) {
	setTimeForTest(time.Unix(1_700_200_400, 0))
	defer testEpoch.Store(0)

	// resourceLabels move a log-derived label onto the resource; labels stay on
	// the data point.
	set, err := NewDynamicMetricSet([]Dynamic{{
		Name:           "reqs_total",
		Type:           CounterType,
		Value:          "1",
		Match:          []string{"level=info"},
		Labels:         []string{"status=$http_status"},
		ResourceLabels: []string{"tenant=$tenant"},
	}})
	if err != nil {
		t.Fatal(err)
	}
	set.Add(nil, labelsFrom(map[string]string{"level": "info", "http_status": "200", "tenant": "acme"}),
		res(map[string]string{"k8s.pod.name": "pod-a"}), "")

	exp := &capExporter{}
	if err := set.Export(context.Background(), exp, 0); err != nil {
		t.Fatal(err)
	}
	ra := resourceOf(t, exp, "reqs_total")
	if ra["tenant"] != "acme" {
		t.Errorf("tenant not lifted to resource: %v", ra)
	}
	if ra["k8s.pod.name"] != "pod-a" {
		t.Errorf("log resource attribute missing: %v", ra)
	}
	if _, ok := ra["status"]; ok {
		t.Error("status (data-point label) leaked onto the resource")
	}
}

// noRes is an empty resource for tests that don't exercise resource grouping.
func noRes() pcommon.Map { return pcommon.NewMap() }

// res builds a resource map from key-value pairs.
func res(m map[string]string) pcommon.Map {
	r := pcommon.NewMap()
	for k, v := range m {
		r.PutStr(k, v)
	}
	return r
}

// capExporter records exported metrics payloads.
type capExporter struct{ md []pmetric.Metrics }

func (c *capExporter) ExportMetrics(_ context.Context, md pmetric.Metrics) error {
	cp := pmetric.NewMetrics()
	md.CopyTo(cp)
	c.md = append(c.md, cp)
	return nil
}

// find returns the last-exported metric with the given name (searching
// newest payload first, so multi-export tests see the current value — the
// first-match order once masked a CounterFunc double-count).
func (c *capExporter) find(name string) (pmetric.Metric, bool) {
	for x := len(c.md) - 1; x >= 0; x-- {
		md := c.md[x]
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

func valuesFrom(m map[string]string) func(string) (float64, bool) {
	return func(k string) (float64, bool) {
		s, ok := m[k]
		if !ok {
			return 0, false
		}
		f, err := strconv.ParseFloat(s, 64)
		return f, err == nil
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

	set.Add(nil, labelsFrom(map[string]string{"level": "info", "http_status": "200", "method": "GET"}), noRes(), "")
	set.Add(nil, labelsFrom(map[string]string{"level": "info", "http_status": "500", "method": "GET"}), noRes(), "")
	set.Add(nil, labelsFrom(map[string]string{"level": "debug", "http_status": "200"}), noRes(), "")

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
			labelsFrom(map[string]string{"op": "write", "bytes": b}), noRes(), "")
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
			labelsFrom(map[string]string{"op": "query", "duration": d}), noRes(), "")
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
	set.Add(nil, labelsFrom(map[string]string{"kind": "a"}), noRes(), "")
	set.Add(nil, labelsFrom(map[string]string{"kind": "b"}), noRes(), "")

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
