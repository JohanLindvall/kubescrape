// Tests for rule compilation and validation (compile.go).
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

// Rules sharing a metric name must agree on histogram buckets: the second
// rule's buckets would otherwise be silently ignored (the first rule's series
// wins), observing into bounds the config never declared.
func TestSharedNameConflictingBucketsRejected(t *testing.T) {
	_, err := NewDynamicMetricSet([]Dynamic{
		{Name: "h", Type: HistogramType, Value: "v", Buckets: []float64{1, 2, 3}},
		{Name: "h", Type: HistogramType, Value: "v", Buckets: []float64{10, 20}},
	})
	if err == nil {
		t.Fatal("conflicting buckets on a shared metric name compiled")
	}
	// Agreeing (or unset) buckets still share the series.
	if _, err := NewDynamicMetricSet([]Dynamic{
		{Name: "h", Type: HistogramType, Value: "v", Buckets: []float64{1, 2, 3}},
		{Name: "h", Type: HistogramType, Value: "w", Buckets: []float64{1, 2, 3}},
		{Name: "h", Type: HistogramType, Value: "x"},
	}); err != nil {
		t.Fatalf("agreeing shared histogram rejected: %v", err)
	}
}

func TestActionOnNonGaugeErrors(t *testing.T) {
	if _, err := NewDynamicMetricSet([]Dynamic{{Name: "c", Type: CounterType, Action: "inc"}}); err == nil {
		t.Error("action on a counter: want error")
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

func TestParseLabelForms(t *testing.T) {
	get := func(m map[string]string) func(string) string {
		return func(k string) string { return m[k] }
	}
	ctx := map[string]string{"status": "503", "path": "/a/b"}

	cases := map[string]string{
		"method":              "",      // bare key: reads itself (missing → "")
		"code=$status":        "503",   // passthrough
		"lit=fixed":           "fixed", // literal
		"class=$status(_xx)":  "5xx",   // pattern: keep first char, mask rest
		"masked=$status/0/_/": "5_3",   // regex replace: 0 → _
	}
	for spec, want := range cases {
		lt, err := parseLabelTemplate(spec, "")
		if err != nil {
			t.Fatalf("parseLabelTemplate(%q): %v", spec, err)
		}
		if got := lt.get(get(ctx)); got != want {
			t.Errorf("parseLabelTemplate(%q).get = %q, want %q", spec, got, want)
		}
	}

	if _, err := parseLabelTemplate("=nope", ""); err == nil {
		t.Error("invalid label: want error")
	}
}

func TestLabelPrefix(t *testing.T) {
	lt, err := parseLabelTemplate("status=$http_status", "http_")
	if err != nil {
		t.Fatal(err)
	}
	if lt.setKey != "http_status" {
		t.Errorf("setKey = %q, want http_status", lt.setKey)
	}
}

func TestInvalidType(t *testing.T) {
	if _, err := NewDynamicMetricSet([]Dynamic{{Name: "x", Type: "bogus"}}); err == nil {
		t.Error("invalid type: want error")
	}
}

func TestBucketsOnlyForHistogram(t *testing.T) {
	if _, err := NewDynamicMetricSet([]Dynamic{{Name: "x", Type: CounterType, Buckets: []float64{1}}}); err == nil {
		t.Error("buckets on a counter: want error")
	}
}

// A zero or negative maxAge would mark every sample idle on every export,
// silently turning counters into per-interval deltas; reject at load.
func TestNonPositiveMaxAgeRejected(t *testing.T) {
	for _, age := range []string{"0s", "-1h"} {
		if _, err := NewDynamicMetricSet([]Dynamic{{Name: "x", Type: CounterType, Value: "1", MaxAge: age}}); err == nil {
			t.Errorf("maxAge %q: want error", age)
		}
	}
}

// Only `\/` and `\\` are DSL escapes in the /pattern/replacement/ form; any
// other backslash sequence must reach the regex compiler intact — the escape
// branch used to eat EVERY backslash, silently compiling `error (d+)` from
// `error (\d+)` so the replace never fired.
func TestRegexpReplaceKeepsRegexEscapes(t *testing.T) {
	cases := []struct {
		in, pattern, repl string
	}{
		{`/error (\d+)/e$1/`, `error (\d+)`, "e$1"},
		{`/a\/b/x/`, "a/b", "x"},
		{`/a\\d/y/`, `a\d`, "y"},
		{`/\s+/ /`, `\s+`, " "},
	}
	for _, c := range cases {
		p, r, err := parseRegexpReplace(c.in)
		if err != nil {
			t.Fatalf("%s: %v", c.in, err)
		}
		if p != c.pattern || r != c.repl {
			t.Errorf("%s: got (%q, %q), want (%q, %q)", c.in, p, r, c.pattern, c.repl)
		}
	}
}

// A mask on a missing source field must drop the label, not fabricate one
// from the mask's literal characters ("_xx" buckets for lines without the
// field) — matching the plain passthrough's behavior.
func TestMaskPatternMissingFieldDropsLabel(t *testing.T) {
	if got := maskPattern("", "_xx"); got != "" {
		t.Fatalf("maskPattern(missing) = %q, want empty", got)
	}
	if got := maskPattern("404", "_xx"); got != "4xx" {
		t.Fatalf("maskPattern present = %q, want 4xx", got)
	}
}
