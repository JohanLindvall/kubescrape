package promscrape

import (
	"fmt"
	"math/bits"
	"os"
	"regexp"
	"sort"

	"sigs.k8s.io/yaml"
)

// FilterConfig declares which scraped series are exported. It is loaded from
// YAML (-metrics-filter-config):
//
//	pipelines:
//	  all:                    # prepended to every pipeline's rules
//	    - action: keep        # exceptions go before the drop they punch through
//	      metrics: 'envoy_cluster_upstream_rq_total|envoy_requests_total'
//	    - action: drop
//	      metrics: '(envoy_|otelcol_|prometheus_).+'
//	  cadvisor:
//	    - action: keep
//	      metrics: 'container_network_(receive|transmit)_bytes_total'
//	      labels:
//	        interface: 'eth0'
//	    - action: drop
//	      metrics: 'container_network_.+'
//
// Rules are evaluated in order (the "all" list first, then the pipeline's
// own list); the first matching rule decides. A series with no matching rule
// is kept. Regexes are fully anchored; a rule matches when the series name
// matches `metrics` (empty = any) and every `labels` entry matches the
// series' label value (a missing label matches against "").
//
// Filtering happens on the scraped series names (e.g. `foo_bucket`), before
// histogram/summary grouping — dropping only some component series of a
// family yields a partial family, exactly as with Prometheus relabeling.
type FilterConfig struct {
	Pipelines map[string][]FilterRule `json:"pipelines,omitempty"`
}

// FilterRule is one keep/drop decision.
type FilterRule struct {
	// Action is "keep" or "drop".
	Action string `json:"action"`
	// Metrics is an anchored regex on the series name; empty matches any.
	Metrics string `json:"metrics,omitempty"`
	// Labels maps label names to anchored regexes; all must match.
	Labels map[string]string `json:"labels,omitempty"`
}

// filterPipelineNames are the sections accepted under pipelines ("all" plus
// the scrape pipelines).
var filterPipelineNames = []string{"all", "targets", "cadvisor", "node"}

// MetricFilters holds the compiled per-pipeline series filters; nil (or a
// nil field) keeps everything.
type MetricFilters struct {
	Targets  *MetricFilter
	Cadvisor *MetricFilter
	Node     *MetricFilter
}

// MetricsConfig is the `metrics` section of the agent config: per-pipeline
// series filters plus target splitters.
type MetricsConfig struct {
	Pipelines map[string][]FilterRule `json:"pipelines,omitempty"`
	Splitters []SplitterConfig        `json:"splitters,omitempty"`
}

// LoadMetricsConfig reads and compiles the metrics config file.
func LoadMetricsConfig(path string) (*MetricFilters, []*Splitter, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, nil, err
	}
	var cfg MetricsConfig
	if err := yaml.UnmarshalStrict(data, &cfg); err != nil {
		return nil, nil, fmt.Errorf("%s: %w", path, err)
	}
	filters, err := NewMetricFilters(&FilterConfig{Pipelines: cfg.Pipelines})
	if err != nil {
		return nil, nil, fmt.Errorf("%s: %w", path, err)
	}
	splitters, err := NewSplitters(cfg.Splitters)
	if err != nil {
		return nil, nil, fmt.Errorf("%s: %w", path, err)
	}
	return filters, splitters, nil
}

// NewMetricFilters compiles a FilterConfig.
func NewMetricFilters(cfg *FilterConfig) (*MetricFilters, error) {
	if cfg == nil {
		return nil, nil
	}
	for name := range cfg.Pipelines {
		ok := false
		for _, want := range filterPipelineNames {
			if name == want {
				ok = true
			}
		}
		if !ok {
			return nil, fmt.Errorf("unknown pipeline %q (want one of all, targets, cadvisor, node)", name)
		}
	}
	compile := func(pipeline string) (*MetricFilter, error) {
		rules := append(append([]FilterRule(nil), cfg.Pipelines["all"]...), cfg.Pipelines[pipeline]...)
		return newMetricFilter(rules)
	}
	var out MetricFilters
	var err error
	if out.Targets, err = compile("targets"); err != nil {
		return nil, err
	}
	if out.Cadvisor, err = compile("cadvisor"); err != nil {
		return nil, err
	}
	if out.Node, err = compile("node"); err != nil {
		return nil, err
	}
	if out.Targets == nil && out.Cadvisor == nil && out.Node == nil {
		return nil, nil
	}
	return &out, nil
}

// filterFor picks the filter for a pipeline; nil keeps everything.
func (f *MetricFilters) filterFor(pipeline string) *MetricFilter {
	if f == nil {
		return nil
	}
	switch pipeline {
	case pipelineCadvisor:
		return f.Cadvisor
	case pipelineNode:
		return f.Node
	default:
		return f.Targets
	}
}

// MetricFilter is an ordered first-match-wins series filter.
type MetricFilter struct {
	rules []compiledRule
}

type compiledRule struct {
	drop   bool
	name   *regexp.Regexp // nil matches any
	labels []labelMatcher // all must match
}

type labelMatcher struct {
	name string
	re   *regexp.Regexp
}

func newMetricFilter(rules []FilterRule) (*MetricFilter, error) {
	if len(rules) == 0 {
		return nil, nil
	}
	f := &MetricFilter{rules: make([]compiledRule, 0, len(rules))}
	for i, r := range rules {
		var cr compiledRule
		switch r.Action {
		case "drop":
			cr.drop = true
		case "keep":
		default:
			return nil, fmt.Errorf("rule %d: action %q (want keep or drop)", i, r.Action)
		}
		if r.Metrics != "" {
			re, err := regexp.Compile("^(?:" + r.Metrics + ")$")
			if err != nil {
				return nil, fmt.Errorf("rule %d metrics: %w", i, err)
			}
			cr.name = re
		}
		names := make([]string, 0, len(r.Labels))
		for name := range r.Labels {
			names = append(names, name)
		}
		sort.Strings(names)
		for _, name := range names {
			re, err := regexp.Compile("^(?:" + r.Labels[name] + ")$")
			if err != nil {
				return nil, fmt.Errorf("rule %d label %q: %w", i, name, err)
			}
			cr.labels = append(cr.labels, labelMatcher{name: name, re: re})
		}
		f.rules = append(f.rules, cr)
	}
	return f, nil
}

// Keep reports whether a series passes the filter. Safe on a nil receiver.
func (f *MetricFilter) Keep(name string, labels []Label) bool {
	if f == nil {
		return true
	}
	for _, r := range f.rules {
		if r.name != nil && !r.name.MatchString(name) {
			continue
		}
		if !r.labelsMatch(labels) {
			continue
		}
		return !r.drop
	}
	return true
}

// session returns a per-scrape memoizing view of the filter: the set of rules
// whose NAME regex matches is cached per series name (a bitmask), so a family
// of thousands of series pays the regex walk once. Safe on a nil receiver;
// the returned session is single-goroutine (one per scrape), keeping the
// shared MetricFilter immutable.
func (f *MetricFilter) session() *filterSession {
	if f == nil || len(f.rules) > 64 {
		return &filterSession{f: f} // no memo; fall back to direct Keep
	}
	return &filterSession{f: f, masks: make(map[string]uint64, 64)}
}

type filterSession struct {
	f     *MetricFilter
	masks map[string]uint64 // name -> bitmask of name-matching rules
}

func (s *filterSession) Keep(name string, labels []Label) bool {
	if s.f == nil {
		return true
	}
	if s.masks == nil {
		return s.f.Keep(name, labels)
	}
	mask, ok := s.masks[name]
	if !ok {
		for i, r := range s.f.rules {
			if r.name == nil || r.name.MatchString(name) {
				mask |= 1 << i
			}
		}
		s.masks[name] = mask
	}
	for mask != 0 {
		i := bits.TrailingZeros64(mask)
		mask &^= 1 << i
		if r := &s.f.rules[i]; r.labelsMatch(labels) {
			return !r.drop
		}
	}
	return true
}

func (r *compiledRule) labelsMatch(labels []Label) bool {
	for _, m := range r.labels {
		value := ""
		for _, l := range labels {
			if l.Name == m.name {
				value = l.Value
				break
			}
		}
		if !m.re.MatchString(value) {
			return false
		}
	}
	return true
}
