package promscrape

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"sigs.k8s.io/yaml"

	"github.com/JohanLindvall/kubescrape/internal/obs"
)

// LoadMetricsConfig loads a standalone config file. Production config arrives solely
// through the unified agent config (cmd/kubescrape-agent -config); this
// loader survives only for the strict-YAML parse/validate tests here.
func LoadMetricsConfig(path string) (*MetricFilters, []*Splitter, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, nil, err
	}
	var cfg MetricsConfig
	if err := yaml.UnmarshalStrict(data, &cfg); err != nil {
		return nil, nil, fmt.Errorf("%s: %w", path, err)
	}
	filters, err := NewMetricFilters(cfg.Pipelines)
	if err != nil {
		return nil, nil, fmt.Errorf("%s: %w", path, err)
	}
	splitters, err := NewSplitters(cfg.Splitters)
	if err != nil {
		return nil, nil, fmt.Errorf("%s: %w", path, err)
	}
	return filters, splitters, nil
}

// keepReference is the unmemoized definition of a filter's verdict — the rules
// in order, the first whose name regex and label matchers all accept the series
// deciding, a series no rule matches kept — which the memoizing session must
// agree with. It lives here and not in filter.go because no pipeline runs it:
// every one filters through session(), and a production copy was a second,
// unexercised spelling of first-match-wins. Safe on a nil filter.
func keepReference(f *MetricFilter, name string, labels []Label) bool {
	if f == nil {
		return true
	}
	for _, r := range f.rules {
		if r.name != nil && !r.name.MatchString(name) {
			continue
		}
		matched := true
		for _, m := range r.labels {
			if !m.re.MatchString(labelValue(labels, m.name)) {
				matched = false
				break
			}
		}
		if matched {
			return !r.drop
		}
	}
	return true
}

func mustFilters(t *testing.T, pipelines map[string][]FilterRule) *MetricFilters {
	t.Helper()
	f, err := NewMetricFilters(pipelines)
	if err != nil {
		t.Fatal(err)
	}
	return f
}

func TestMetricFilterRules(t *testing.T) {
	f := mustFilters(t, map[string][]FilterRule{
		"all": {
			{Action: "keep", Metrics: `envoy_requests_total`},
			{Action: "drop", Metrics: `(envoy_|otelcol_).+`},
		},
		"cadvisor": {
			{Action: "keep", Metrics: `container_network_.+`, Labels: map[string]string{"interface": "eth0"}},
			{Action: "drop", Metrics: `container_network_.+`},
		},
	})

	targets := f.filterFor(pipelineTargets).session()
	cases := []struct {
		name   string
		labels []Label
		keep   bool
	}{
		{"envoy_requests_total", nil, true},       // keep exception beats the drop
		{"envoy_cluster_upstream_rq", nil, false}, // dropped by prefix
		{"otelcol_receiver_accepted", nil, false}, // dropped by prefix
		{"http_requests_total", nil, true},        // no rule matches -> keep
		{"container_network_receive", nil, true},  // cadvisor rule not in targets
	}
	for _, c := range cases {
		if got := targets.Keep(c.name, c.labels); got != c.keep {
			t.Errorf("targets %s: keep=%v, want %v", c.name, got, c.keep)
		}
	}

	cad := f.filterFor(pipelineCadvisor).session()
	if !cad.Keep("container_network_receive_bytes_total", []Label{{Name: "interface", Value: "eth0"}}) {
		t.Error("eth0 network series must survive")
	}
	if cad.Keep("container_network_receive_bytes_total", []Label{{Name: "interface", Value: "cali123"}}) {
		t.Error("non-eth0 network series must be dropped")
	}
	// Missing label matches against "".
	if cad.Keep("container_network_receive_bytes_total", nil) {
		t.Error("network series without interface label must be dropped")
	}
	// The "all" rules apply to cadvisor too.
	if cad.Keep("otelcol_x", nil) {
		t.Error("all-pipeline drop must apply to cadvisor")
	}
}

func TestMetricFilterValidation(t *testing.T) {
	if _, err := NewMetricFilters(map[string][]FilterRule{
		"bogus": {{Action: "drop"}},
	}); err == nil {
		t.Fatal("unknown pipeline must error")
	}
	if _, err := NewMetricFilters(map[string][]FilterRule{
		"all": {{Action: "nuke"}},
	}); err == nil {
		t.Fatal("unknown action must error")
	}
	if _, err := NewMetricFilters(map[string][]FilterRule{
		"all": {{Action: "drop", Metrics: "("}},
	}); err == nil {
		t.Fatal("invalid regex must error")
	}
	if f := mustFilters(t, nil); f != nil {
		t.Fatal("nil config must compile to nil filters")
	}
}

// Every scrape pipeline in filterPipelineNames is registered by the list alone:
// a rule under its name reaches filterFor(name), and reaches NO other
// pipeline. The second half is the regression that storing only the non-nil
// filters would introduce — filterFor falls back to the targets filter for a
// name it has no entry for, so a targets-only drop would silently apply to
// cadvisor, node and summary as well.
func TestEveryFilterPipelineGetsOnlyItsOwnRules(t *testing.T) {
	for _, p := range filterPipelineNames {
		if p == filterAllPipeline {
			continue
		}
		f := mustFilters(t, map[string][]FilterRule{p: {{Action: "drop", Metrics: `probe_.+`}}})
		for _, q := range filterPipelineNames {
			if q == filterAllPipeline {
				continue
			}
			kept := f.filterFor(q).session().Keep("probe_total", nil)
			if q == p && kept {
				t.Errorf("a %q rule does not reach filterFor(%q): the name is listed but never compiled", p, q)
			}
			if q != p && !kept {
				t.Errorf("a %q-only drop rule was applied to the %q pipeline", p, q)
			}
		}
	}
	// The concrete shape of the regression: a targets-only rule, cadvisor's
	// own series.
	f := mustFilters(t, map[string][]FilterRule{pipelineTargets: {{Action: "drop", Metrics: `container_.+`}}})
	if !f.filterFor(pipelineCadvisor).session().Keep("container_cpu_usage_seconds_total", nil) {
		t.Error("a targets-only drop rule was applied to the cadvisor pipeline")
	}
}

func TestLoadMetricsConfig(t *testing.T) {
	path := filepath.Join(t.TempDir(), "metrics.yaml")
	if err := os.WriteFile(path, []byte(`
pipelines:
  all:
    - action: drop
      metrics: 'go_.+'
splitters:
  - match:
      podLabels: {app.kubernetes.io/name: kube-state-metrics}
    rules:
      - metrics: 'kube_pod_.+'
        groupBy: {namespace: k8s.namespace.name, pod: k8s.pod.name}
        enrich: true
`), 0o600); err != nil {
		t.Fatal(err)
	}
	f, sp, err := LoadMetricsConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if f.filterFor(pipelineTargets).session().Keep("go_threads", nil) {
		t.Fatal("go_threads must be dropped")
	}
	if len(sp) != 1 {
		t.Fatalf("splitters = %d", len(sp))
	}
	if err := os.WriteFile(path, []byte("nonsense: true\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := LoadMetricsConfig(path); err == nil {
		t.Fatal("unknown fields must error")
	}
}

func TestScrapeWithFilter(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprint(w, "keep_me 1\ndrop_me 2\n# TYPE hist histogram\nhist_bucket{le=\"+Inf\"} 3\nhist_count 3\nhist_sum 1.5\n")
	}))
	defer srv.Close()

	exp := &captureExporter{}
	s := New(Config{
		Node: "node1", Interval: time.Hour, Timeout: 5 * time.Second,
		Targets: staticTargets{testTarget(srv.URL)}, Exporter: exp, StartTime: time.Now(),
		Filters: mustFilters(t, map[string][]FilterRule{
			"targets": {{Action: "drop", Metrics: `drop_me|hist_.+`}},
		}),
	})
	if _, err := s.scrapeTarget(context.Background(), testTarget(srv.URL), s.cfg.Timeout); err != nil {
		t.Fatal(err)
	}
	metrics := exp.batches[0].ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics()
	if metrics.Len() != 1 || metrics.At(0).Name() != "keep_me" {
		var names []string
		for i := 0; i < metrics.Len(); i++ {
			names = append(names, metrics.At(i).Name())
		}
		t.Fatalf("metrics = %v, want only keep_me", names)
	}
}

// The memoizing session must agree with the unmemoized reference on ordering
// and label-conditional rules, including repeated names (the cached path).
func TestFilterSession(t *testing.T) {
	f, err := newMetricFilter([]FilterRule{
		{Action: "keep", Metrics: "container_network_.+", Labels: map[string]string{"interface": "eth0"}},
		{Action: "drop", Metrics: "container_network_.+"},
		{Action: "drop", Metrics: "(go_|process_).+"},
	})
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name   string
		labels []Label
		want   bool
	}{
		{"container_network_receive_bytes_total", []Label{{Name: "interface", Value: "eth0"}}, true},
		{"container_network_receive_bytes_total", []Label{{Name: "interface", Value: "lo"}}, false},
		{"container_network_receive_bytes_total", []Label{{Name: "interface", Value: "eth0"}}, true}, // cached name
		{"go_goroutines", nil, false},
		{"http_requests_total", nil, true},
		{"http_requests_total", nil, true}, // cached name
	}
	fs := f.session()
	for _, c := range cases {
		if got := fs.Keep(c.name, c.labels); got != c.want {
			t.Errorf("session Keep(%q, %v) = %v, want %v", c.name, c.labels, got, c.want)
		}
		if got := keepReference(f, c.name, c.labels); got != c.want {
			t.Errorf("reference Keep(%q, %v) = %v, want %v", c.name, c.labels, got, c.want)
		}
	}

	// Nil filter, and a rule count past one bitset word.
	var nilf *MetricFilter
	if !nilf.session().Keep("anything", nil) {
		t.Error("nil filter session must keep")
	}
	many := make([]FilterRule, 65)
	for i := range many {
		many[i] = FilterRule{Action: "drop", Metrics: fmt.Sprintf("rule%d_.+", i)}
	}
	big, err := newMetricFilter(many)
	if err != nil {
		t.Fatal(err)
	}
	bs := big.session()
	if bs.offsets == nil || bs.words != 2 {
		t.Errorf("65 rules gave offsets=%v words=%d, want a live memo of 2 words", bs.offsets != nil, bs.words)
	}
	if bs.Keep("rule7_x", nil) || !bs.Keep("other", nil) {
		t.Error("multi-word mask verdicts wrong")
	}
	if bs.Keep("rule64_x", nil) {
		t.Error("the 65th rule (the second bitset word) did not decide")
	}
}

// Filtering happens between the parse and the conversion, and used to be
// entirely unmeasured: kubescrape_scrape_samples_total documents itself as the
// count BEFORE filtering, kubescrape_scrapes_total reports success, and the
// converter simply never sees what the filter refused. A keep rule whose regex
// stopped matching — or a monitor's metricRelabelings dropping everything —
// therefore emptied a whole pipeline with every metric on the dashboard green.
func TestScrapeFilterDropsAreCounted(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprint(w, "keep_me 1\ndrop_me 2\ndrop_me_too 3\n")
	}))
	defer srv.Close()

	exp := &captureExporter{}
	s := New(Config{
		Node: "node1", Interval: time.Hour, Timeout: 5 * time.Second,
		Targets: staticTargets{testTarget(srv.URL)}, Exporter: exp, StartTime: time.Now(),
		Filters: mustFilters(t, map[string][]FilterRule{
			"targets": {{Action: "drop", Metrics: `drop_me.*`}},
		}),
	})

	before := obs.ScrapeSamplesDropped.WithLabelValues("targets", "filter").Value()
	beforeRelabel := obs.ScrapeSamplesDropped.WithLabelValues("targets", "relabel").Value()
	if _, err := s.scrapeTarget(context.Background(), testTarget(srv.URL), s.cfg.Timeout); err != nil {
		t.Fatal(err)
	}
	if got := obs.ScrapeSamplesDropped.WithLabelValues("targets", "filter").Value() - before; got != 2 {
		t.Fatalf("filter-dropped samples = %v, want 2 — scraped minus dropped is what reaches the collector", got)
	}
	if got := obs.ScrapeSamplesDropped.WithLabelValues("targets", "relabel").Value() - beforeRelabel; got != 0 {
		t.Fatalf("relabel reason moved by %v with no relabelings configured", got)
	}
}

// A scrape whose filter keeps everything must leave the counter alone, or the
// rate is unreadable in the healthy case.
func TestScrapeNoFilterDropsNoCount(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprint(w, "keep_me 1\nkeep_me_too 2\n")
	}))
	defer srv.Close()

	exp := &captureExporter{}
	s := New(Config{
		Node: "node1", Interval: time.Hour, Timeout: 5 * time.Second,
		Targets: staticTargets{testTarget(srv.URL)}, Exporter: exp, StartTime: time.Now(),
	})
	before := obs.ScrapeSamplesDropped.WithLabelValues("targets", "filter").Value()
	if _, err := s.scrapeTarget(context.Background(), testTarget(srv.URL), s.cfg.Timeout); err != nil {
		t.Fatal(err)
	}
	if got := obs.ScrapeSamplesDropped.WithLabelValues("targets", "filter").Value() - before; got != 0 {
		t.Fatalf("counter moved by %v with nothing filtered", got)
	}
}
