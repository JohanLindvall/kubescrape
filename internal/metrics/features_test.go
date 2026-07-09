package metrics

import (
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
			}}, WithStreamLabels(nil))
			if err != nil {
				t.Fatal(err)
			}
			for _, a := range []string{"10", "20", "30"} {
				set.Add(valuesFrom(map[string]string{"amount": a}),
					labelsFrom(map[string]string{"m": "1", "amount": a}), "")
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
	}}, WithStreamLabels(nil))
	if err != nil {
		t.Fatal(err)
	}
	set.Add(nil, nil, "GET /x latency=0.25s ok")
	set.Add(nil, nil, "GET /y latency=0.75s ok")
	set.Add(nil, nil, "POST /z latency=9.9s ok") // __line__ has no GET → filtered

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
	}}, WithStreamLabels(nil))
	if err != nil {
		t.Fatal(err)
	}
	set.Add(nil, nil, "goroutine 1 panic: nil deref")
	set.Add(nil, nil, "all good here")

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

func TestStreamLabelsAutomaticAndOverride(t *testing.T) {
	setTimeForTest(time.Unix(1_700_200_300, 0))
	defer testEpoch.Store(0)

	// Default stream labels are on: a resource-attribute lookup for
	// k8s.pod.name is promoted to a label automatically. The second metric
	// overrides with an empty list to aggregate across pods.
	set, err := NewDynamicMetricSet([]Dynamic{
		{Name: "per_pod_total", Type: CounterType, Value: "1", Match: []string{"m=1"}},
		{Name: "agg_total", Type: CounterType, Value: "1", Match: []string{"m=1"}, StreamLabels: &[]string{}},
	})
	if err != nil {
		t.Fatal(err)
	}
	resource := func(k string) string {
		if k == "k8s.pod.name" {
			return "pod-a"
		}
		return ""
	}
	set.Add(nil, resource, `{"m":1}`)

	perPod := exportOne(t, set, "per_pod_total")
	found := false
	dps := perPod.Sum().DataPoints()
	for i := 0; i < dps.Len(); i++ {
		if v, ok := dps.At(i).Attributes().Get("k8s.pod.name"); ok && v.Str() == "pod-a" {
			found = true
		}
	}
	if !found {
		t.Error("per_pod_total missing automatic k8s.pod.name stream label")
	}

	agg := exportOne(t, set, "agg_total")
	adps := agg.Sum().DataPoints()
	for i := 0; i < adps.Len(); i++ {
		if _, ok := adps.At(i).Attributes().Get("k8s.pod.name"); ok {
			t.Error("agg_total should have no pod label (override to aggregate)")
		}
	}
}

func TestExplicitLabelOverridesStreamLabel(t *testing.T) {
	setTimeForTest(time.Unix(1_700_200_400, 0))
	defer testEpoch.Store(0)

	// An explicit label with the same name as a stream label wins.
	set, err := NewDynamicMetricSet([]Dynamic{{
		Name:   "t",
		Type:   CounterType,
		Value:  "1",
		Match:  []string{"m=1"},
		Labels: []string{"k8s.pod.name=fixed"},
	}}, WithStreamLabels([]string{"k8s.pod.name"}))
	if err != nil {
		t.Fatal(err)
	}
	set.Add(nil, func(k string) string {
		if k == "k8s.pod.name" {
			return "from-resource"
		}
		return ""
	}, `{"m":1}`)

	m := exportOne(t, set, "t")
	dps := m.Sum().DataPoints()
	for i := 0; i < dps.Len(); i++ {
		if v, ok := dps.At(i).Attributes().Get("k8s.pod.name"); ok && v.Str() != "fixed" {
			t.Errorf("k8s.pod.name = %q, want fixed (explicit label wins)", v.Str())
		}
	}
}
