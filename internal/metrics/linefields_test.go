package metrics

import (
	"context"
	"testing"
	"time"
)

// With no caller-provided lookup/values, labels, the observed value and the
// match selector all resolve from the JSON line itself.
func TestLineFieldsJSON(t *testing.T) {
	setTimeForTest(time.Unix(1_700_100_000, 0))
	defer testEpoch.Store(0)

	set, err := NewDynamicMetricSet([]Dynamic{{
		Name:   "bytes_written_total",
		Type:   SummaryType,
		Value:  "bytes",
		Match:  []string{"level=info"},
		Labels: []string{"route=$http.route"}, // nested JSON path
	}})
	if err != nil {
		t.Fatal(err)
	}
	// nil lookup + nil values: everything must come off the line.
	set.Add(nil, nil, `{"level":"info","bytes":128,"http":{"route":"/api"}}`)
	set.Add(nil, nil, `{"level":"info","bytes":256,"http":{"route":"/api"}}`)
	set.Add(nil, nil, `{"level":"debug","bytes":999,"http":{"route":"/api"}}`) // filtered out

	m := exportOne(t, set, "bytes_written_total")
	dps := m.Summary().DataPoints()
	if dps.Len() != 1 {
		t.Fatalf("data points = %d", dps.Len())
	}
	dp := dps.At(0)
	if route, _ := dp.Attributes().Get("route"); route.Str() != "/api" {
		t.Errorf("route = %q, want /api", route.Str())
	}
	if dp.Count() != 2 || dp.Sum() != 384 {
		t.Errorf("count/sum = %d/%v, want 2/384", dp.Count(), dp.Sum())
	}
}

func TestLineFieldsLogfmt(t *testing.T) {
	setTimeForTest(time.Unix(1_700_100_100, 0))
	defer testEpoch.Store(0)

	set, err := NewDynamicMetricSet([]Dynamic{{
		Name:   "requests_total",
		Type:   CounterType,
		Value:  "1",
		Match:  []string{"status=200"},
		Labels: []string{"method"},
	}})
	if err != nil {
		t.Fatal(err)
	}
	set.Add(nil, nil, `level=info method=GET status=200 msg="ok"`)
	set.Add(nil, nil, `level=info method=GET status=500 msg="err"`) // filtered

	m := exportOne(t, set, "requests_total")
	dps := m.Sum().DataPoints()
	got := map[string]float64{}
	for i := 0; i < dps.Len(); i++ {
		dp := dps.At(i)
		if dp.DoubleValue() > 0 {
			meth, _ := dp.Attributes().Get("method")
			got[meth.Str()] = dp.DoubleValue()
		}
	}
	if got["GET"] != 1 {
		t.Errorf("GET count = %v, want 1", got["GET"])
	}
}

// A caller-provided lookup (record/resource attributes) takes precedence over
// the line for the same key.
func TestCallerLookupWinsOverLine(t *testing.T) {
	setTimeForTest(time.Unix(1_700_100_200, 0))
	defer testEpoch.Store(0)

	set, err := NewDynamicMetricSet([]Dynamic{{
		Name:   "hits_total",
		Type:   CounterType,
		Value:  "1",
		Match:  []string{"level=info"},
		Labels: []string{"pod=$k8s.pod.name"},
	}})
	if err != nil {
		t.Fatal(err)
	}
	lookup := func(k string) string {
		if k == "k8s.pod.name" {
			return "from-resource"
		}
		return ""
	}
	set.Add(nil, lookup, `{"level":"info","k8s.pod.name":"from-line"}`)

	exp := &capExporter{}
	if err := set.Export(context.Background(), exp, 0); err != nil {
		t.Fatal(err)
	}
	m, _ := exp.find("hits_total")
	dps := m.Sum().DataPoints()
	found := ""
	for i := 0; i < dps.Len(); i++ {
		if v, ok := dps.At(i).Attributes().Get("pod"); ok {
			found = v.Str()
		}
	}
	if found != "from-resource" {
		t.Errorf("pod = %q, want from-resource (caller lookup wins)", found)
	}
}
