package metrics

import (
	"os"
	"path/filepath"
	"testing"

	"fmt"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"sigs.k8s.io/yaml"
)

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
