package metrics

import (
	"context"
	"testing"
	"time"
)

// gaugeValue reads the single gauge data point's value for the given label.
func gaugeValue(t *testing.T, set *DynamicMetricSet, name, labelKey, labelVal string) (float64, bool) {
	t.Helper()
	m := exportOne(t, set, name)
	dps := m.Gauge().DataPoints()
	for i := 0; i < dps.Len(); i++ {
		dp := dps.At(i)
		if labelKey == "" {
			return dp.DoubleValue(), true
		}
		if v, ok := dp.Attributes().Get(labelKey); ok && v.Str() == labelVal {
			return dp.DoubleValue(), true
		}
	}
	return 0, false
}

func TestGaugeActions(t *testing.T) {
	cases := []struct {
		action string
		value  string // "" means none (inc/dec)
		want   float64
	}{
		{"inc", "", 3},        // three matching lines, +1 each
		{"dec", "", -3},       // -1 each
		{"add", "amount", 60}, // 10+20+30
		{"sub", "amount", -60},
		{"set", "amount", 30}, // last value wins
	}
	for _, c := range cases {
		t.Run(c.action, func(t *testing.T) {
			setTimeForTest(time.Unix(1_700_200_000, 0))
			defer testEpoch.Store(0)

			set, err := NewDynamicMetricSet([]Dynamic{{
				Name:   "g",
				Type:   GaugeType,
				Action: c.action,
				Value:  c.value,
				Match:  []string{"m=1"},
			}})
			if err != nil {
				t.Fatal(err)
			}
			for _, a := range []string{"10", "20", "30"} {
				set.Add(valuesFrom(map[string]string{"amount": a}),
					labelsFrom(map[string]string{"m": "1", "amount": a}), noRes(), "")
			}
			got, ok := gaugeValue(t, set, "g", "", "")
			if !ok || got != c.want {
				t.Fatalf("%s gauge = %v (ok=%v), want %v", c.action, got, ok, c.want)
			}
		})
	}
}

func TestActionOnNonGaugeErrors(t *testing.T) {
	if _, err := NewDynamicMetricSet([]Dynamic{{Name: "c", Type: CounterType, Action: "inc"}}); err == nil {
		t.Error("action on a counter: want error")
	}
}

func TestValueRegexp(t *testing.T) {
	setTimeForTest(time.Unix(1_700_200_100, 0))
	defer testEpoch.Store(0)

	// Pull a number out of an unstructured line.
	set, err := NewDynamicMetricSet([]Dynamic{{
		Name:        "latency_seconds_total",
		Type:        CounterType,
		ValueRegexp: `latency=([0-9.]+)s`,
		MatchRegexp: []string{"__line__=^GET"},
	}})
	if err != nil {
		t.Fatal(err)
	}
	set.Add(nil, nil, noRes(), "GET /x latency=0.25s ok")
	set.Add(nil, nil, noRes(), "GET /y latency=0.75s ok")
	set.Add(nil, nil, noRes(), "POST /z latency=9.9s ok") // __line__ has no GET → filtered

	m := exportOne(t, set, "latency_seconds_total")
	var total float64
	dps := m.Sum().DataPoints()
	for i := 0; i < dps.Len(); i++ {
		total += dps.At(i).DoubleValue()
	}
	if total != 1.0 { // 0.25 + 0.75, POST excluded
		t.Errorf("sum = %v, want 1.0", total)
	}
}

func TestValueAndValueRegexpConflict(t *testing.T) {
	_, err := NewDynamicMetricSet([]Dynamic{{
		Name: "x", Type: CounterType, Value: "a", ValueRegexp: "b",
	}})
	if err == nil {
		t.Error("value + valueRegexp: want error")
	}
}

func TestLineSelector(t *testing.T) {
	setTimeForTest(time.Unix(1_700_200_200, 0))
	defer testEpoch.Store(0)

	set, err := NewDynamicMetricSet([]Dynamic{{
		Name:        "panics_total",
		Type:        CounterType,
		Value:       "1",
		MatchRegexp: []string{`__line__=panic:`},
	}})
	if err != nil {
		t.Fatal(err)
	}
	set.Add(nil, nil, noRes(), "goroutine 1 panic: nil deref")
	set.Add(nil, nil, noRes(), "all good here")

	m := exportOne(t, set, "panics_total")
	var total float64
	dps := m.Sum().DataPoints()
	for i := 0; i < dps.Len(); i++ {
		if v := dps.At(i).DoubleValue(); v > 0 {
			total += v
		}
	}
	if total != 1 {
		t.Errorf("panics = %v, want 1", total)
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
