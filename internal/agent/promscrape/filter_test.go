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
)

func mustFilters(t *testing.T, cfg *FilterConfig) *MetricFilters {
	t.Helper()
	f, err := NewMetricFilters(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return f
}

func TestMetricFilterRules(t *testing.T) {
	f := mustFilters(t, &FilterConfig{Pipelines: map[string][]FilterRule{
		"all": {
			{Action: "keep", Metrics: `envoy_requests_total`},
			{Action: "drop", Metrics: `(envoy_|otelcol_).+`},
		},
		"cadvisor": {
			{Action: "keep", Metrics: `container_network_.+`, Labels: map[string]string{"interface": "eth0"}},
			{Action: "drop", Metrics: `container_network_.+`},
		},
	}})

	targets := f.filterFor(pipelineTargets)
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

	cad := f.filterFor(pipelineCadvisor)
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
	if _, err := NewMetricFilters(&FilterConfig{Pipelines: map[string][]FilterRule{
		"bogus": {{Action: "drop"}},
	}}); err == nil {
		t.Fatal("unknown pipeline must error")
	}
	if _, err := NewMetricFilters(&FilterConfig{Pipelines: map[string][]FilterRule{
		"all": {{Action: "nuke"}},
	}}); err == nil {
		t.Fatal("unknown action must error")
	}
	if _, err := NewMetricFilters(&FilterConfig{Pipelines: map[string][]FilterRule{
		"all": {{Action: "drop", Metrics: "("}},
	}}); err == nil {
		t.Fatal("invalid regex must error")
	}
	if f := mustFilters(t, nil); f != nil {
		t.Fatal("nil config must compile to nil filters")
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
	if f.filterFor(pipelineTargets).Keep("go_threads", nil) {
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
		Filters: mustFilters(t, &FilterConfig{Pipelines: map[string][]FilterRule{
			"targets": {{Action: "drop", Metrics: `drop_me|hist_.+`}},
		}}),
	})
	if _, err := s.scrapeTarget(context.Background(), testTarget(srv.URL)); err != nil {
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

// The memoizing session must agree with the direct Keep on ordering and
// label-conditional rules, including repeated names (the cached path).
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
		if got := f.Keep(c.name, c.labels); got != c.want {
			t.Errorf("direct Keep(%q, %v) = %v, want %v", c.name, c.labels, got, c.want)
		}
	}

	// Nil filter and >64-rule fallback keep working.
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
	if bs.masks != nil {
		t.Error(">64 rules must disable the memo")
	}
	if bs.Keep("rule7_x", nil) || !bs.Keep("other", nil) {
		t.Error(">64-rule fallback verdicts wrong")
	}
}
