package metrics

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
	"unsafe"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"sigs.k8s.io/yaml"
)

func TestValueRegexpWholeMatchAndBadCapture(t *testing.T) {
	setTimeForTest(time.Unix(1_700_800_000, 0))
	defer testEpoch.Store(0)

	// No capture group: the whole match is the value.
	set, err := NewDynamicMetricSet([]Dynamic{
		{Name: "whole_total", Type: CounterType, ValueRegexp: `[0-9]+\.[0-9]+`},
		{Name: "bad_total", Type: CounterType, ValueRegexp: `id=([a-z]+)`},
	})
	if err != nil {
		t.Fatal(err)
	}
	beforeNaN := DroppedNaN()
	set.Add(nil, nil, noRes(), "latency 12.5 seconds")
	set.Add(nil, nil, noRes(), "id=abc done") // matches, capture non-numeric -> skipped

	m := exportOne(t, set, "whole_total")
	var total float64
	dps := m.Sum().DataPoints()
	for i := 0; i < dps.Len(); i++ {
		total += dps.At(i).DoubleValue()
	}
	if total != 12.5 {
		t.Errorf("whole-match total = %v, want 12.5", total)
	}
	exp := &capExporter{}
	if err := set.Export(context.Background(), exp, 0); err != nil {
		t.Fatal(err)
	}
	if _, ok := exp.find("bad_total"); ok {
		t.Error("non-numeric capture produced a series; the line must be skipped")
	}
	if got := DroppedNaN() - beforeNaN; got != 0 {
		t.Errorf("droppedNaN delta = %d, want 0 (skip, not NaN admission)", got)
	}
}

// TestLineKeyMultilineBody: __line__ selectors/labels and valueRegexp against a
// body with embedded newlines; a newline-carrying label value must round-trip
// the serialized label form into the exported attribute intact.
func TestLineKeyMultilineBody(t *testing.T) {
	setTimeForTest(time.Unix(1_700_800_100, 0))
	defer testEpoch.Store(0)

	set, err := NewDynamicMetricSet([]Dynamic{{
		Name:        "panic_lines",
		Type:        GaugeType,
		Action:      "count",
		Match:       nil,
		MatchRegexp: []string{`__line__=(?s)panic:.*goroutine`},
		Labels:      []string{"head=$__line__(______)"},
	}})
	if err != nil {
		t.Fatal(err)
	}
	body := "panic: boom\ngoroutine 1 [running]:\nmain.main()"
	set.Add(nil, nil, noRes(), body)

	m := exportOne(t, set, "panic_lines")
	dps := m.Gauge().DataPoints()
	if dps.Len() != 1 {
		t.Fatalf("data points = %d", dps.Len())
	}
	if v := dps.At(0).DoubleValue(); v != 1 {
		t.Fatalf("count = %v, want 1", v)
	}
	if head, ok := dps.At(0).Attributes().Get("head"); !ok || head.Str() != "panic:" {
		t.Fatalf("head = %q, want %q", head.Str(), "panic:")
	}

	// And a raw newline INSIDE a label value survives serialize/parse/export.
	set2, err := NewDynamicMetricSet([]Dynamic{{
		Name: "nl_total", Type: CounterType, Value: "1", Labels: []string{"tail=$t"},
	}})
	if err != nil {
		t.Fatal(err)
	}
	set2.Add(nil, labelsFrom(map[string]string{"t": "a\nb\"c\\d"}), noRes(), "")
	m2 := exportOne(t, set2, "nl_total")
	found := false
	dps2 := m2.Sum().DataPoints()
	for i := 0; i < dps2.Len(); i++ {
		if v, ok := dps2.At(i).Attributes().Get("tail"); ok {
			found = true
			if v.Str() != "a\nb\"c\\d" {
				t.Fatalf("tail = %q, want %q", v.Str(), "a\nb\"c\\d")
			}
		}
	}
	if !found {
		t.Fatal("tail label missing")
	}
}

// --- Angle 6: unsafe line-field views must not be retained -------------------

// TestNoAliasRetentionAfterLineBufferReuse: linefields hands out strings that
// may alias the log line (lightning's UnescapeString aliases its input when the
// string has no escapes). If any such string were RETAINED by a series, a
// caller reusing its line buffer would corrupt exported label values. Series
// storage must copy (labels.String / resourceString) before the line dies.
func TestNoAliasRetentionAfterLineBufferReuse(t *testing.T) {
	setTimeForTest(time.Unix(1_700_900_000, 0))
	defer testEpoch.Store(0)

	set, err := NewDynamicMetricSet([]Dynamic{{
		Name:           "aliased_total",
		Type:           CounterType,
		Value:          "1",
		Match:          []string{"level=info"},
		Labels:         []string{"user=$user"},
		ResourceLabels: []string{"tenant=$tenant"},
	}})
	if err != nil {
		t.Fatal(err)
	}

	buf := []byte(`{"level":"info","user":"alice","tenant":"acme"}`)
	line := unsafe.String(&buf[0], len(buf)) // simulates a reused read buffer
	set.Add(nil, nil, res(map[string]string{"k8s.pod.name": "p"}), line)

	for i := range buf { // the "buffer" is reused for the next read
		buf[i] = 'X'
	}

	exp := &capExporter{}
	if err := set.Export(context.Background(), exp, 0); err != nil {
		t.Fatal(err)
	}
	m, ok := exp.find("aliased_total")
	if !ok {
		t.Fatal("metric missing")
	}
	dps := m.Sum().DataPoints()
	userOK := false
	for i := 0; i < dps.Len(); i++ {
		if v, ok := dps.At(i).Attributes().Get("user"); ok {
			if v.Str() != "alice" {
				t.Fatalf("user label = %q, want alice: series retained an aliased line view", v.Str())
			}
			userOK = true
		}
	}
	if !userOK {
		t.Fatal("user label missing")
	}
	for _, md := range exp.md {
		rms := md.ResourceMetrics()
		for i := 0; i < rms.Len(); i++ {
			if v, ok := rms.At(i).Resource().Attributes().Get("tenant"); ok && v.Str() != "acme" {
				t.Fatalf("tenant resource label = %q, want acme: aliased retention", v.Str())
			}
		}
	}
}

// --- Angle 7: registry concurrency -------------------------------------------

// --- Angle 8: value extraction vs the counting gauge actions ------------------
//
// TestValueRegexpFiltersCountingActions: valueRegexp is documented as
// "a line that does not match is skipped" — it is both an extractor AND a
// filter. needsValue() returns false for gauge inc/dec/count, so observe used
// to skip readValue entirely and the regexp was NEVER evaluated: every line
// passing the selectors was counted, including lines the valueRegexp rejects.
// Regression guard for the fix: valueRe is evaluated as a filter even for
// counting actions.
func TestValueRegexpFiltersCountingActions(t *testing.T) {
	setTimeForTest(time.Unix(1_701_000_000, 0))
	defer testEpoch.Store(0)

	set, err := NewDynamicMetricSet([]Dynamic{
		{Name: "errs_inc", Type: GaugeType, Action: "inc", ValueRegexp: `code=(\d+)`},
		{Name: "errs_count", Type: GaugeType, Action: "count", ValueRegexp: `code=(\d+)`},
	})
	if err != nil {
		t.Fatal(err)
	}
	set.Add(nil, nil, noRes(), "all good, nothing to see")     // no code= — must be skipped
	set.Add(nil, nil, noRes(), "request failed code=500 oops") // matches

	for _, name := range []string{"errs_inc", "errs_count"} {
		m := exportOne(t, set, name)
		dps := m.Gauge().DataPoints()
		if dps.Len() != 1 {
			t.Fatalf("%s: data points = %d, want 1", name, dps.Len())
		}
		if v := dps.At(0).DoubleValue(); v != 1 {
			t.Errorf("%s = %v, want 1: the line not matching valueRegexp must be skipped, but the counting actions never evaluate it", name, v)
		}
	}
}

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
	set.Add(nil, nil, noRes(), `{"level":"info","bytes":128,"http":{"route":"/api"}}`)
	set.Add(nil, nil, noRes(), `{"level":"info","bytes":256,"http":{"route":"/api"}}`)
	set.Add(nil, nil, noRes(), `{"level":"debug","bytes":999,"http":{"route":"/api"}}`) // filtered out

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
	set.Add(nil, nil, noRes(), `level=info method=GET status=200 msg="ok"`)
	set.Add(nil, nil, noRes(), `level=info method=GET status=500 msg="err"`) // filtered

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
	set.Add(nil, lookup, noRes(), `{"level":"info","k8s.pod.name":"from-line"}`)

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

func TestAddConcurrent(t *testing.T) {
	// Exercise the pooled per-line contexts (bound closures, scratch reuse)
	// under concurrency; run with -race to verify no shared state leaks
	// between goroutines.
	setTimeForTest(time.Unix(1_700_100_300, 0))
	defer testEpoch.Store(0)
	set, err := NewDynamicMetricSet([]Dynamic{{
		Name:   "reqs_total",
		Type:   CounterType,
		Value:  "1",
		Match:  []string{"level=info"},
		Labels: []string{"status=$status"},
	}})
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			status := fmt.Sprintf("%d", 200+g)
			lookup := func(k string) string {
				switch k {
				case "level":
					return "info"
				case "status":
					return status
				}
				return ""
			}
			for i := 0; i < 500; i++ {
				set.Add(nil, lookup, noRes(), "")
			}
		}(g)
	}
	wg.Wait()

	m := exportOne(t, set, "reqs_total")
	var total float64
	real := map[string]bool{} // fresh counters also emit synthetic zero baseline points
	dps := m.Sum().DataPoints()
	for i := 0; i < dps.Len(); i++ {
		dp := dps.At(i)
		total += dp.DoubleValue()
		switch dp.DoubleValue() {
		case 500:
			v, _ := dp.Attributes().Get("status")
			real[v.Str()] = true
		case 0: // baseline
		default:
			t.Errorf("data point value = %v, want 0 (baseline) or 500", dp.DoubleValue())
		}
	}
	if total != 8*500 {
		t.Errorf("total = %v, want %d", total, 8*500)
	}
	if len(real) != 8 {
		t.Errorf("series = %d (%v), want 8 (one per status)", len(real), real)
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

// LoadDynamicMetrics loads a standalone config file. Production config arrives solely
// through the unified agent config (cmd/kubescrape-agent -config); this
// loader survives only for the strict-YAML parse/validate tests here.
func LoadDynamicMetrics(path string) ([]Dynamic, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var cfg DynamicConfig
	if err := yaml.UnmarshalStrict(data, &cfg); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	return cfg.Metrics, nil
}

// Add is the unbound test/bench convenience over add: production exclusively
// binds a resource first (Bind + BoundResource.Add, which hashes the resource
// once per flush); the per-call resourceAccum here would be a hot-path
// regression outside tests.
func (s *DynamicMetricSet) Add(values func(string) (float64, bool), lookup func(string) string, resource pcommon.Map, line string) {
	if s == nil || len(s.rules) == 0 {
		return
	}
	s.add(values, lookup, resource, resourceAccum(resource), line)
}

func TestLoadDynamicMetrics(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "m.yaml")
	_ = os.WriteFile(path, []byte(`metrics:
  - name: http_requests_total
    type: counter
    value: "1"
    match: ["level=error"]
    matchRegexp: ["msg=timeout"]
    labels: ["status=$http_status", "method"]
    maxCardinality: 5000
    maxAge: 1h
`), 0o644)

	dyn, err := LoadDynamicMetrics(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(dyn) != 1 {
		t.Fatalf("metrics = %d", len(dyn))
	}
	d := dyn[0]
	if d.Name != "http_requests_total" || d.Value != "1" || d.MaxCardinality != 5000 {
		t.Errorf("dynamic = %+v", d)
	}
	if len(d.Match) != 1 || len(d.MatchRegexp) != 1 || len(d.Labels) != 2 {
		t.Errorf("selectors/labels = %+v", d)
	}
	if _, err := NewDynamicMetricSet(dyn); err != nil {
		t.Fatalf("building set: %v", err)
	}

	if _, err := LoadDynamicMetrics(filepath.Join(dir, "nope.yaml")); err == nil {
		t.Error("missing file: want error")
	}
}
