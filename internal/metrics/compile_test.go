// Tests for rule compilation and validation (compile.go).
package metrics

import (
	"strconv"
	"strings"
	"testing"
	"unicode/utf8"

	"go.opentelemetry.io/collector/pdata/pcommon"

	"github.com/JohanLindvall/kubescrape/internal/logline"
)

// TestHistogramCardinalityCountsLabelSets pins the UNIT of maxCardinality: it
// caps distinct label combinations, not live samples. The store keys samples
// per bucket stream, so comparing the raw map size against the cap divided a
// histogram's configured cap by its bucket count behind the user's back — a
// default-bucket histogram at maxCardinality 10000 admitted 666 label sets
// while the config, the README and the warning all said 10000.
func TestHistogramCardinalityCountsLabelSets(t *testing.T) {
	const want = 5
	set, err := newTestSet([]Dynamic{{
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
		set.Add(func(k string) (float64, bool, bool) { return 1, k == "v", k == "v" },
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
	_, err := newTestSet([]Dynamic{{
		Name: "h", Type: HistogramType, Value: "v", Buckets: buckets, MaxCardinality: 10000,
	}})
	if err == nil {
		t.Fatal("10000 label sets x 201 buckets compiled — that is 2M live samples")
	}
	if !strings.Contains(err.Error(), "live samples") {
		t.Fatalf("unexpected error: %v", err)
	}
	// Lowering the cardinality to fit is accepted.
	if _, err := newTestSet([]Dynamic{{
		Name: "h", Type: HistogramType, Value: "v", Buckets: buckets, MaxCardinality: 500,
	}}); err != nil {
		t.Fatalf("500 x 201 = 100500 samples rejected: %v", err)
	}
	// A small cap on a default-bucket histogram is fine — it means few label
	// sets, not "nothing can ever be admitted".
	if _, err := newTestSet([]Dynamic{{
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
		if _, err := newTestSet([]Dynamic{d}); err == nil {
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
		if _, err := newTestSet([]Dynamic{d}); err != nil {
			t.Fatalf("%s %q wrongly rejected: %v", d.Type, d.Action, err)
		}
	}
}

// Rules sharing a metric name must agree on histogram buckets: the second
// rule's buckets would otherwise be silently ignored (the first rule's series
// wins), observing into bounds the config never declared.
func TestSharedNameConflictingBucketsRejected(t *testing.T) {
	_, err := newTestSet([]Dynamic{
		{Name: "h", Type: HistogramType, Value: "v", Buckets: []float64{1, 2, 3}},
		{Name: "h", Type: HistogramType, Value: "v", Buckets: []float64{10, 20}},
	})
	if err == nil {
		t.Fatal("conflicting buckets on a shared metric name compiled")
	}
	// Agreeing (or unset) buckets still share the series.
	if _, err := newTestSet([]Dynamic{
		{Name: "h", Type: HistogramType, Value: "v", Buckets: []float64{1, 2, 3}},
		{Name: "h", Type: HistogramType, Value: "w", Buckets: []float64{1, 2, 3}},
		{Name: "h", Type: HistogramType, Value: "x"},
	}); err != nil {
		t.Fatalf("agreeing shared histogram rejected: %v", err)
	}
}

// The same holds for every other shape field a later rule can declare and the
// shared series cannot honour. A second rule asking for a tight maxCardinality
// because ITS label set is the riskier one used to start cleanly and admit label
// sets up to the first rule's cap — the memory blow-up the field was set to
// prevent, with nothing said about it.
func TestSharedNameConflictingLimitsRejected(t *testing.T) {
	base := Dynamic{
		Name: "m", Type: CounterType, Value: "1",
		MaxCardinality: 5000, MaxAge: "1h", Description: "the first rule's",
	}
	for _, tc := range []struct {
		name   string
		second Dynamic
	}{
		{"maxCardinality", Dynamic{Name: "m", Type: CounterType, Value: "1", MaxCardinality: 100}},
		{"maxAge", Dynamic{Name: "m", Type: CounterType, Value: "1", MaxAge: "1s"}},
		{"description", Dynamic{Name: "m", Type: CounterType, Value: "1", Description: "the second rule's"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			set, err := newTestSet([]Dynamic{base, tc.second})
			if err == nil {
				t.Fatalf("a conflicting %s compiled; the shared series keeps the first rule's %s (maxSize=%d expiration=%d desc=%q)",
					tc.name, tc.name, set.rules[0].series.maxSize, set.rules[0].series.expiration, set.rules[0].series.desc)
			}
			if !strings.Contains(err.Error(), tc.name) {
				t.Errorf("error must name the field: %v", err)
			}
		})
	}

	// Unset means "whatever the name already has", and an equal declaration —
	// however it is spelled — is not a conflict.
	if _, err := newTestSet([]Dynamic{
		base,
		{Name: "m", Type: CounterType, Value: "1"},
		{Name: "m", Type: CounterType, Value: "1", MaxCardinality: 5000, MaxAge: "60m", Description: "the first rule's"},
	}); err != nil {
		t.Fatalf("agreeing shared rules rejected: %v", err)
	}
}

// Two configs that used to compile cleanly and then do nothing, both invited by
// the DSL the metrics engine shares with logs.rules: __severity__ is resolved
// only by the rules tier (a metric naming it is permanently absent) and
// __line__ is resolved by the LABEL tier but not the value one (a metric naming
// it as its value records nothing). Silence is the defect — an operator reads a
// clean start as a working rule.
func TestKeysTheMetricsEngineCannotResolveAreRejected(t *testing.T) {
	for _, tc := range []struct {
		name string
		d    Dynamic
	}{
		{"match", Dynamic{Name: "m", Type: CounterType, Value: "1", Match: []string{logline.SeverityKey + "=error"}}},
		{"matchRegexp", Dynamic{Name: "m", Type: CounterType, Value: "1", MatchRegexp: []string{logline.SeverityKey + "=err.*"}}},
		{"label", Dynamic{Name: "m", Type: CounterType, Value: "1", Labels: []string{"sev=$" + logline.SeverityKey}}},
		{"resourceLabel", Dynamic{Name: "m", Type: CounterType, Value: "1", ResourceLabels: []string{"sev=$" + logline.SeverityKey}}},
		{"severity value", Dynamic{Name: "m", Type: GaugeType, Value: logline.SeverityKey}},
		{"line value", Dynamic{Name: "m", Type: GaugeType, Value: logline.LineKey}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			set, err := newTestSet([]Dynamic{tc.d})
			if err == nil {
				set.Add(nil, nil, pcommon.NewMap(), `{"level":"error","amount":42}`)
				t.Fatalf("compiled, and observed %d samples from a matching line", len(set.rules[0].series.db))
			}
		})
	}

	// The keys each tier DOES resolve stay working: __line__ as a label and as
	// a selector, and a real line field as the value.
	set, err := newTestSet([]Dynamic{{
		Name: "m", Type: GaugeType, Value: "amount",
		MatchRegexp: []string{logline.LineKey + "=amount"},
		Labels:      []string{"body=$" + logline.LineKey},
	}})
	if err != nil {
		t.Fatalf("__line__ as a selector/label rejected: %v", err)
	}
	set.Add(nil, nil, pcommon.NewMap(), `{"level":"error","amount":42}`)
	if n := len(set.rules[0].series.db); n != 1 {
		t.Fatalf("observed %d samples, want 1", n)
	}
}

func TestActionOnNonGaugeErrors(t *testing.T) {
	if _, err := newTestSet([]Dynamic{{Name: "c", Type: CounterType, Action: "inc"}}); err == nil {
		t.Error("action on a counter: want error")
	}
}

func TestValueAndValueRegexpConflict(t *testing.T) {
	_, err := newTestSet([]Dynamic{{
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
	if _, err := newTestSet([]Dynamic{{Name: "x", Type: "bogus"}}); err == nil {
		t.Error("invalid type: want error")
	}
}

func TestBucketsOnlyForHistogram(t *testing.T) {
	if _, err := newTestSet([]Dynamic{{Name: "x", Type: CounterType, Buckets: []float64{1}}}); err == nil {
		t.Error("buckets on a counter: want error")
	}
}

// A zero or negative maxAge would mark every sample idle on every export,
// silently turning counters into per-interval deltas; reject at load.
//
// A MALFORMED one must also name the field: this was the one duration parser
// in the repo that returned time.ParseDuration's bare error, so
// `maxAge: 5 minutes` failed startup with `time: unknown unit " " in duration
// "5 minutes"` and nothing pointing at which of a config's metrics carried it.
// config.Duration names the field and the value in every error.
func TestNonPositiveMaxAgeRejected(t *testing.T) {
	for _, age := range []string{"0s", "-1h"} {
		if _, err := newTestSet([]Dynamic{{Name: "x", Type: CounterType, Value: "1", MaxAge: age}}); err == nil {
			t.Errorf("maxAge %q: want error", age)
		}
	}
	_, err := newTestSet([]Dynamic{{Name: "x", Type: CounterType, Value: "1", MaxAge: "5 minutes"}})
	if err == nil {
		t.Fatal("a malformed maxAge must be rejected")
	}
	if !strings.Contains(err.Error(), "maxAge") || !strings.Contains(err.Error(), "5 minutes") {
		t.Errorf("error must name the field and the value: %v", err)
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

// A LITERAL value cannot carry a transform: the constant arm wins, so
// `class=status(_xx)` compiled to the constant label class="status" and
// `path=api/x/y/` to path="api" — one merged series, wrong forever, from a
// config whose only defect is a forgotten '$'. Refuse it, and keep the bare
// form (which promotes to a key) working.
func TestLiteralValueWithTransformRejected(t *testing.T) {
	for _, spec := range []string{"class=status(_xx)", "path=api/x/y/"} {
		_, err := parseLabelTemplate(spec, "")
		if err == nil {
			t.Fatalf("parseLabelTemplate(%q) compiled; want an error", spec)
		}
		if !strings.Contains(err.Error(), spec) {
			t.Errorf("error must name the label spec: %v", err)
		}
	}
	// The same shapes without a set= are bare keys that read themselves.
	for spec, want := range map[string]string{"status(_xx)": "5xx", "status/0/_/": "5_3"} {
		lt, err := parseLabelTemplate(spec, "")
		if err != nil {
			t.Fatalf("parseLabelTemplate(%q): %v", spec, err)
		}
		if got := lt.get(func(string) string { return "503" }); got != want {
			t.Errorf("parseLabelTemplate(%q).get = %q, want %q", spec, got, want)
		}
	}
	// And through the compiler, where an operator meets it.
	if _, err := newTestSet([]Dynamic{{
		Name: "x", Type: CounterType, Value: "1", Labels: []string{"class=status(_xx)"},
	}}); err == nil {
		t.Fatal("a literal value with a mask compiled into a rule")
	}
}

// The fast path's ASCII probe must read no more of the value than the overlay
// itself does. Probing the WHOLE value made a mask cost time proportional to the
// LINE — `class=$__line__(_xx)` over a 1 MiB entry burned ~300us of the single
// sweep goroutine per record — while the loop it guards reads at most
// len(pattern) bytes. Non-ASCII beyond that prefix cannot change the overlay, so
// it must not push the value onto the slow path either.
func TestMaskPatternProbeIsBoundedByThePattern(t *testing.T) {
	long := strings.Repeat("5", 1<<10) + "é"
	if !asciiOverlay(long, "_xx") {
		t.Error("a non-ASCII byte past the mask's reach refused the byte overlay: the probe scans the whole value")
	}
	// Bounded, but never narrower than the overlay: a rune straddling the cut
	// ends the prefix on a continuation byte and belongs to the rune loop.
	if asciiOverlay("é50", "_xx") {
		t.Error("a value whose FIRST rune is multi-byte took the byte overlay")
	}
	// The two paths agree wherever both are legal.
	for _, c := range []struct{ value, pattern string }{
		{long, "_xx"}, {"503", "_xx"}, {"é50", "_xx"}, {"5", "___"},
	} {
		if got, want := maskPattern(c.value, c.pattern), maskRunes(c.value, c.pattern); got != want {
			t.Errorf("maskPattern(%.8q, %q) = %q, rune step gives %q", c.value, c.pattern, got, want)
		}
	}
}

// maskRunes is the unconditional rune-stepped overlay — maskPattern's slow path,
// spelled out so the test can compare the fast path against it.
func maskRunes(value, pattern string) string {
	out := make([]byte, 0, len(pattern)+len(value))
	rest := value
	for _, pr := range pattern {
		vr, size := utf8.DecodeRuneInString(rest)
		rest = rest[size:]
		if pr == '_' && size > 0 {
			out = utf8.AppendRune(out, vr)
			continue
		}
		out = utf8.AppendRune(out, pr)
	}
	return string(out)
}

// The mask steps by RUNE. Byte indexing split a multi-byte rune straddling a
// boundary, and that value is hashed, retained and written to a pcommon.Map —
// an invalid-UTF-8 string in the marshaled payload, which a strict protobuf
// receiver rejects PERMANENTLY, dropping every metric sharing that resource.
func TestMaskPatternIsRuneAware(t *testing.T) {
	for _, c := range []struct{ value, pattern, want string }{
		{"é50", "_xx", "éxx"},
		{"日本語", "__x", "日本x"},
		{"é50", "___", "é50"},
		{"5", "_xx", "5xx"},
		{"5", "___", "5__"},  // value exhausted: the mask stays literal
		{"ok", "_é_", "oé_"}, // the literal 'é' consumes 'k'; value exhausted after it
	} {
		got := maskPattern(c.value, c.pattern)
		if got != c.want {
			t.Errorf("maskPattern(%q, %q) = %q, want %q", c.value, c.pattern, got, c.want)
		}
		if !utf8.ValidString(got) {
			t.Errorf("maskPattern(%q, %q) = %q, which is not valid UTF-8", c.value, c.pattern, got)
		}
	}
}
