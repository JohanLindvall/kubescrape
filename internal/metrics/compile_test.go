// Tests for rule compilation and validation (compile.go).
package metrics

import (
	"strconv"
	"strings"
	"testing"

	"go.opentelemetry.io/collector/pdata/pcommon"
)

// TestHistogramCardinalityCountsLabelSets pins the UNIT of maxCardinality: it
// caps distinct label combinations, not live samples. The store keys samples
// per bucket stream, so comparing the raw map size against the cap divided a
// histogram's configured cap by its bucket count behind the user's back — a
// default-bucket histogram at maxCardinality 10000 admitted 666 label sets
// while the config, the README and the warning all said 10000.
func TestHistogramCardinalityCountsLabelSets(t *testing.T) {
	const want = 5
	set, err := NewDynamicMetricSet([]Dynamic{{
		Name: "h", Type: HistogramType, Value: "v", Labels: []string{"id"},
		MaxCardinality: want,
	}})
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	// Ten distinct label sets against a cap of five: exactly five must land,
	// each with its full 15-stream distribution.
	for i := range 10 {
		id := strconv.Itoa(i)
		set.Add(func(k string) (float64, bool) { return 1, k == "v" },
			func(k string) string {
				if k == "id" {
					return id
				}
				return ""
			}, pcommon.NewMap(), "")
	}
	streams := len(defaultBuckets) + 1
	ser := set.rules[0].series
	if got := len(ser.db) / streams; got != want {
		t.Fatalf("admitted %d label combinations (%d samples / %d streams), want %d",
			got, len(ser.db), streams, want)
	}
}

// A histogram whose label-set cap times its bucket count would outgrow the
// store's live-sample budget is rejected at compile time — loudly, rather than
// by silently admitting fewer label sets than configured.
func TestHistogramStreamBudgetGuard(t *testing.T) {
	buckets := make([]float64, 200)
	for i := range buckets {
		buckets[i] = float64(i)
	}
	_, err := NewDynamicMetricSet([]Dynamic{{
		Name: "h", Type: HistogramType, Value: "v", Buckets: buckets, MaxCardinality: 10000,
	}})
	if err == nil {
		t.Fatal("10000 label sets x 201 buckets compiled — that is 2M live samples")
	}
	if !strings.Contains(err.Error(), "live samples") {
		t.Fatalf("unexpected error: %v", err)
	}
	// Lowering the cardinality to fit is accepted.
	if _, err := NewDynamicMetricSet([]Dynamic{{
		Name: "h", Type: HistogramType, Value: "v", Buckets: buckets, MaxCardinality: 500,
	}}); err != nil {
		t.Fatalf("500 x 201 = 100500 samples rejected: %v", err)
	}
	// A small cap on a default-bucket histogram is fine — it means few label
	// sets, not "nothing can ever be admitted".
	if _, err := NewDynamicMetricSet([]Dynamic{{
		Name: "h", Type: HistogramType, Value: "v", MaxCardinality: 5,
	}}); err != nil {
		t.Fatalf("maxCardinality 5 rejected: %v", err)
	}
}

// maxStreamCap is written as a literal because defaultBuckets is a slice; keep
// the two in step.
func TestStreamCapMatchesDefaultHistogram(t *testing.T) {
	if got := maxCardinalityCap * (len(defaultBuckets) + 1); got != maxStreamCap {
		t.Fatalf("maxStreamCap = %d, want %d (maxCardinalityCap x default streams)", maxStreamCap, got)
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
