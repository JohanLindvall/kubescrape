package metrics

import (
	"strings"
	"testing"
)

// TestHistogramDefaultBucketsCardinalityGuard is the regression test for the
// silent-data-loss bug: a histogram with no explicit buckets gets the 14 default
// buckets (15 streams incl. +Inf), so a maxCardinality below that admits nothing
// (all-or-nothing histogram admission). The compile guard checked the RAW bucket
// count (1 when empty) and let such a config through, producing zero data with
// no error. It must now reject at compile time against the EFFECTIVE count.
func TestHistogramDefaultBucketsCardinalityGuard(t *testing.T) {
	_, err := NewDynamicMetricSet([]Dynamic{{
		Name: "h", Type: HistogramType, Value: "v", MaxCardinality: 5,
	}})
	if err == nil {
		t.Fatal("default-bucket histogram with maxCardinality 5 compiled — it would silently admit nothing")
	}
	if !strings.Contains(err.Error(), "bucket streams") {
		t.Fatalf("unexpected error: %v", err)
	}
	// An adequate cap (default buckets = 15 streams) compiles, and so does an
	// explicit small bucket set within the cap.
	if _, err := NewDynamicMetricSet([]Dynamic{{
		Name: "h", Type: HistogramType, Value: "v", MaxCardinality: 1500,
	}}); err != nil {
		t.Fatalf("adequate maxCardinality rejected: %v", err)
	}
	if _, err := NewDynamicMetricSet([]Dynamic{{
		Name: "h", Type: HistogramType, Value: "v", Buckets: []float64{1, 5}, MaxCardinality: 10,
	}}); err != nil {
		t.Fatalf("explicit 3-stream histogram under a cap of 10 rejected: %v", err)
	}
}

// TestRuleRequiresValueSource: a rule whose action must read a value but names
// no value source (no value, no valueRegexp) records nothing on every line — a
// silent misconfiguration now rejected at compile time. Gauge inc/dec/count
// tally lines and legitimately need no value.
func TestRuleRequiresValueSource(t *testing.T) {
	for _, d := range []Dynamic{
		{Name: "c", Type: CounterType},
		{Name: "s", Type: SummaryType},
		{Name: "g", Type: GaugeType, Action: "add"},
	} {
		if _, err := NewDynamicMetricSet([]Dynamic{d}); err == nil {
			t.Fatalf("%s %q with no value source compiled — it would record nothing", d.Type, d.Action)
		}
	}
	// Gauge inc/dec/count and any rule with a value source compile.
	for _, d := range []Dynamic{
		{Name: "gi", Type: GaugeType, Action: "inc"},
		{Name: "gc", Type: GaugeType, Action: "count"},
		{Name: "cv", Type: CounterType, Value: "v"},
		{Name: "cr", Type: CounterType, ValueRegexp: `took (\d+)ms`},
	} {
		if _, err := NewDynamicMetricSet([]Dynamic{d}); err != nil {
			t.Fatalf("%s %q wrongly rejected: %v", d.Type, d.Action, err)
		}
	}
}
