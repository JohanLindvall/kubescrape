package metrics

import (
	"context"
	"strings"
	"testing"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

// fuzzMetricSet compiles a rule set that references line fields in every way
// the DSL allows: JSON dotted paths, logfmt keys, the __line__ synthetic key,
// value extraction (field, "1", regexp), masks, regex replaces, selectors and
// resource labels — so fuzzed line content flows through keyIndex/lineFields,
// the JSON GetPaths/unsafe-string path and the logfmt scanner.
func fuzzMetricSet(t testing.TB) *DynamicMetricSet {
	set, err := NewDynamicMetricSet([]Dynamic{
		{
			Name: "lines_total", Type: CounterType, Value: "1",
			Labels: []string{"level=$level", "status=$http_status(_xx)", "nested=$a.b.c", "path=$path/\\/api\\/v[0-9]+/api/"},
		},
		{
			Name: "latency", Type: HistogramType, Value: "latency_ms",
			Buckets: []float64{1, 10, 100},
			Labels:  []string{"method=$method"},
		},
		{
			Name: "gauge_avg", Type: GaugeType, Action: "avg", Value: "value",
			Match:          []string{"level!=debug"},
			ResourceLabels: []string{"tenant=$tenant"},
		},
		{
			Name: "extracted", Type: SummaryType, ValueRegexp: `took ([0-9.]+)ms`,
			MatchRegexp: []string{"__line__=took"},
			Labels:      []string{"line_head=$__line__(____)"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	return set
}

type discardExporter struct{}

func (discardExporter) ExportMetrics(context.Context, pmetric.Metrics) error { return nil }

// FuzzLineFields runs arbitrary lines through the JSON/logfmt line-field
// extraction and the whole observe/export pipeline. Invariants: no panics
// (including in the unsafe read-only string views handed to the JSON scanner)
// and a clean OTLP render of whatever series the lines produced.
func FuzzLineFields(f *testing.F) {
	seeds := []string{
		`{"level":"info","msg":"handled","http_status":200,"latency_ms":42.5,"method":"GET","path":"/api/v1/orders","a":{"b":{"c":"deep"}},"tenant":"acme","value":3}`,
		`{"level":"info","a":{"b":{"c":[1,2]}},"http_status":"501"}`,
		`{"level":123,"value":true,"latency_ms":"12"}`,
		`level=warn msg="logfmt line" http_status=503 latency_ms=7 value=1.5 tenant=t1`,
		`took 12.5ms to render`,
		`{"unterminated":`,
		`{"dup":1,"dup":2,"nested":{"deep":{"deeper":{"deepest":null}}}}`,
		"{\"k\":\"\x00\xff invalid utf8\"}",
		`{"a.b.c":"flat-dotted-key"}`,
		`a=b c= =d e="unclosed`,
		`{"a":{"b":{"c":1e309}},"latency_ms":-0.0,"value":9999999999999999999999}`,
		"", " ", "=", "{}", "{", "\x00\x01\x02",
		`{"level":"info","value":"NaN","latency_ms":"Inf"}`,
		strings.Repeat(`{"a":`, 200) + "1" + strings.Repeat("}", 200),
	}
	for _, s := range seeds {
		f.Add(s)
	}
	set := fuzzMetricSet(f)
	res := pcommon.NewMap()
	res.PutStr("service.name", "fuzz")
	res.PutStr("k8s.pod.name", "fuzz-0")
	bound := set.Bind(res)

	f.Fuzz(func(t *testing.T, line string) {
		// Twice: once through the bound-resource path, once plain, so pooled
		// addContext reuse across lines is exercised.
		bound.Add(nil, nil, line)
		set.Add(nil, func(key string) string {
			if key == "tenant" {
				return "" // force fallthrough to line fields
			}
			return ""
		}, res, line)
		if err := set.Export(context.Background(), discardExporter{}, 1<<10); err != nil {
			t.Fatalf("export: %v", err)
		}
	})
}

// sanitizeLabelKey restricts a fuzzed key to characters that survive the
// serialized form: parseLabels cuts keys at '=', trims surrounding space, and
// cannot represent quotes/backslashes/newlines/commas in keys (values may
// contain anything — they are escaped; keys are not, by design).
func sanitizeLabelKey(k string) string {
	k = strings.Map(func(r rune) rune {
		switch r {
		case '=', ',', '"', '\\', '\n', '{', '}':
			return -1
		}
		return r
	}, k)
	return strings.TrimSpace(k)
}

// FuzzLabelsParse checks that String/parseLabels round-trip arbitrary label
// sets (keys sanitized to the representable alphabet, values fully arbitrary)
// and that parseLabels never panics on arbitrary input.
func FuzzLabelsParse(f *testing.F) {
	f.Add("app", "web", "ns", "prod", "z", "1", `{a="1", b="2"}`)
	f.Add("k", `quote " backslash \ newline`+"\n", "k2", "a,b=c", "k3", `\`, `{`)
	f.Add("a", "", "", "v", " spaced ", "x", `{k="v\n\\\""}`)
	f.Add("k", `"`, "k2", `\n`, "k3", ",", "not-a-label-string")
	f.Add("\xff\xfe", "\x00", "é", "ü", "k", "v", `{k=}`)

	f.Fuzz(func(t *testing.T, k1, v1, k2, v2, k3, v3, arbitrary string) {
		// parseLabels must never panic on arbitrary input.
		if ls, err := parseLabels(arbitrary); err == nil {
			_ = ls.String() // and its result must serialize without panicking
			_ = ls.hash()
		}

		var l labels
		for _, p := range []kv{{k1, v1}, {k2, v2}, {k3, v3}} {
			l = l.set(sanitizeLabelKey(p.key), p.value)
		}

		s := l.String()
		got, err := parseLabels(s)
		if err != nil {
			t.Fatalf("parseLabels(%q) failed: %v (labels %+v)", s, err, l)
		}
		want := append(labels(nil), l...)
		// String drops empty values; set() already dropped empty keys/values.
		// Compare as key-sorted sets (both sides sorted by key; keys are unique
		// because set() replaces).
		sortByKey(want)
		sortByKey(got)
		if len(got) != len(want) {
			t.Fatalf("round-trip length: got %v want %v (serialized %q)", got, want, s)
		}
		for i := range want {
			if got[i] != want[i] {
				t.Fatalf("round-trip mismatch at %d: got %+v want %+v (serialized %q)", i, got[i], want[i], s)
			}
		}

		// hash must be order-independent and stable across the round-trip.
		if got.hash() != want.hash() || got.checkAccum() != want.checkAccum() {
			t.Fatalf("hash not stable across round-trip (serialized %q)", s)
		}
	})
}

func sortByKey(l labels) {
	for i := 1; i < len(l); i++ {
		for j := i; j > 0 && l[j].key < l[j-1].key; j-- {
			l[j], l[j-1] = l[j-1], l[j]
		}
	}
}
