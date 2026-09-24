package promscrape

import (
	"fmt"
	"math/bits"
	"regexp"
	"slices"
	"strings"
)

// FilterRule is one keep/drop decision.
type FilterRule struct {
	// Action is "keep" or "drop".
	Action string `json:"action"`
	// Metrics is an anchored regex on the series name; empty matches any.
	Metrics string `json:"metrics,omitempty"`
	// Labels maps label names to anchored regexes; all must match.
	Labels map[string]string `json:"labels,omitempty"`
}

// filterAllPipeline is the section whose rules are prepended to every
// pipeline's own; it names no pipeline of its own.
const filterAllPipeline = "all"

// filterPipelineNames are the sections accepted under pipelines: "all" plus
// every scrape pipeline, spelled with the pipeline constants so there is ONE
// list. NewMetricFilters compiles a filter for each name here and filterFor
// looks it up by the same name, so adding a pipeline to this list is the whole
// registration — there is no second switch to forget.
//
// pipelineSummary is the kubelet /stats/summary pipeline. Its rules see the
// OTLP metric name and the DATA POINT attributes as labels, so a rule can select
// on k8s.volume.name or fs.type — which is this pipeline's cardinality lever,
// most of its series being one pod's volumes.
var filterPipelineNames = []string{filterAllPipeline, pipelineTargets, pipelineCadvisor, pipelineNode, pipelineSummary}

// MetricFilters holds the compiled per-pipeline series filters; nil (or a
// nil filter for a pipeline) keeps everything.
type MetricFilters struct {
	// byPipeline holds an entry for EVERY scrape pipeline in
	// filterPipelineNames, nil included. The nil entries are load-bearing:
	// filterFor falls back to the targets filter only for a name it has no
	// entry for, so storing only the non-nil filters would hand a known
	// pipeline with no rules of its own the TARGETS pipeline's rules.
	byPipeline map[string]*MetricFilter
}

// MetricsConfig is the `metrics` section of the agent config: per-pipeline
// series filters plus target splitters.
//
// Pipelines declares which scraped series are exported:
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
//
// Native histograms (the protobuf exposition, -scrape-native-histograms) are
// the exception because they have no component series: a rule, a
// metricRelabelings `__name__` match and a splitter rule's `metrics` all see
// the FAMILY name (`foo`) once per point, as Prometheus' own relabeling does.
// A `foo_bucket` rule therefore does not reach a native `foo`, and a rule
// matching `foo` keeps or drops the whole point.
type MetricsConfig struct {
	Pipelines map[string][]FilterRule `json:"pipelines,omitempty"`
	Splitters []SplitterConfig        `json:"splitters,omitempty"`
}

// compileAnchored compiles a configured regex FULLY ANCHORED, Prometheus'
// rule for every regex a filter or splitter rule takes. The non-capturing group
// is load-bearing, not cosmetic: a bare "^" + x + "$" anchors `a|b` as `^a|b$`,
// which matches any name merely starting with a or ending with b, so a keep or
// drop rule silently over-matches. (Relabel rules take the same wrap through
// kubemeta.CompileRelabelRegex, which adds their empty-regex default.)
func compileAnchored(expr string) (*regexp.Regexp, error) {
	return regexp.Compile("^(?:" + expr + ")$")
}

// NewMetricFilters compiles the per-pipeline rules of a MetricsConfig (see
// MetricsConfig.Pipelines). An empty map compiles to nil: keep everything.
func NewMetricFilters(pipelines map[string][]FilterRule) (*MetricFilters, error) {
	for name := range pipelines {
		if !slices.Contains(filterPipelineNames, name) {
			// Derived from the list rather than spelled out beside it: the two had
			// to be edited together, which is exactly the pairing a new pipeline
			// forgets.
			return nil, fmt.Errorf("unknown pipeline %q (want one of %s)", name, strings.Join(filterPipelineNames, ", "))
		}
	}
	out := &MetricFilters{byPipeline: make(map[string]*MetricFilter, len(filterPipelineNames)-1)}
	anyRules := false
	for _, pipeline := range filterPipelineNames {
		if pipeline == filterAllPipeline {
			continue
		}
		f, err := newMetricFilter(slices.Concat(pipelines[filterAllPipeline], pipelines[pipeline]))
		if err != nil {
			return nil, err
		}
		out.byPipeline[pipeline] = f // nil included; see byPipeline
		anyRules = anyRules || f != nil
	}
	if !anyRules {
		return nil, nil
	}
	return out, nil
}

// filterFor picks the filter for a pipeline; nil keeps everything. A name that
// is not a filterable pipeline gets the targets filter.
func (f *MetricFilters) filterFor(pipeline string) *MetricFilter {
	if f == nil {
		return nil
	}
	if mf, ok := f.byPipeline[pipeline]; ok {
		return mf
	}
	return f.byPipeline[pipelineTargets]
}

// MetricFilter is an ordered first-match-wins series filter. It is immutable
// and shared; every pipeline evaluates it through a per-scrape session
// (session), which is the one definition of its verdict.
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
			re, err := compileAnchored(r.Metrics)
			if err != nil {
				return nil, fmt.Errorf("rule %d metrics: %w", i, err)
			}
			cr.name = re
		}
		names := make([]string, 0, len(r.Labels))
		for name := range r.Labels {
			names = append(names, name)
		}
		slices.Sort(names)
		for _, name := range names {
			re, err := compileAnchored(r.Labels[name])
			if err != nil {
				return nil, fmt.Errorf("rule %d label %q: %w", i, name, err)
			}
			cr.labels = append(cr.labels, labelMatcher{name: name, re: re})
		}
		f.rules = append(f.rules, cr)
	}
	return f, nil
}

// session returns a per-scrape memoizing view of the filter: the set of rules
// whose NAME regex matches is cached per series name (a bitset), so a family
// of thousands of series pays the regex walk once. Safe on a nil receiver;
// the returned session is single-goroutine (one per scrape), keeping the
// shared MetricFilter immutable.
//
// The bitset is a WORD SLICE and not a uint64, so there is no rule count at
// which the memo silently disappears. It used to: past 64 rules — the width of
// the mask, reached by 40 shared `all` rules plus 25 pipeline ones with neither
// list looking large, since MetricFilters concatenates them — session() returned
// a memo-less view and BOTH memos vanished with it (the label memo is created
// only beside the mask one), so every sample of a 100k-series target re-ran the
// whole anchored-regex chain on the scrape goroutine cycle() waits for. Nothing
// warned, no metric told the two modes apart, and the operator saw only a node
// whose cycles got slower after one added rule.
func (f *MetricFilter) session() *filterSession {
	if f == nil {
		return &filterSession{} // nothing to memoize; Keep answers true
	}
	words := (len(f.rules) + 63) / 64
	s := &filterSession{f: f, words: words, offsets: make(map[string]int, 64), scratch: make([]uint64, words)}
	for _, r := range f.rules {
		if len(r.labels) > 0 {
			s.lblMatch = make(map[lblMatchKey]bool, 64)
			break
		}
	}
	return s
}

// maxMemoBytes bounds the KEY TEXT the per-scrape memos below retain. Their
// entry caps (maxTrackedFamilies, maxInternedValues) bound how MANY names and
// values are remembered, and a count is not a memory bound: both keys are text
// the TARGET chooses, capped only by the parser's line bound. 100k series names
// of 16 KiB — a body that gzips to ~2 MiB, since the amplifier is the
// compression — would retain 1.6 GB here, the sibling of the promparse TYPE
// table's own byte bound (maxTypeBytes) and reachable whenever a `metrics`
// filter is configured. Past the budget the memo simply stops growing: a later
// name pays the regex walk the memo would have saved, never a different
// verdict.
const maxMemoBytes = 1 << 20

// memoBudget is one per-scrape memo holder's byte budget (see maxMemoBytes).
// admit checks BOTH of a memo's bounds — its own entry cap and the holder's
// shared key-text budget — and charges on success, so no site can check the
// budget without charging it, or charge it without checking.
type memoBudget struct{ bytes int }

// admit reports whether a memo holding entries (capped at maxEntries) may take
// one more entry retaining n bytes, and charges n when it may. A refusal is
// never a different verdict, only a lookup the memo will not save next time.
func (m *memoBudget) admit(entries, maxEntries, n int) bool {
	if entries >= maxEntries || m.bytes+n > maxMemoBytes {
		return false
	}
	m.bytes += n
	return true
}

type filterSession struct {
	f *MetricFilter
	// words is the bitset width in uint64s — one per 64 rules, so every rule
	// count is memoizable. offsets maps a series name to the start of its
	// words-long run in maskWords; ONE flat backing slice rather than a slice
	// per name keeps a memo hit a plain reslice (no allocation, which
	// TestFilterSessionAllocationBudget pins) and a miss an amortized append.
	words     int
	offsets   map[string]int
	maskWords []uint64
	// scratch holds the mask of a name the budget refused to memoize, so the
	// over-budget path still costs no allocation.
	scratch  []uint64
	lblMatch map[lblMatchKey]bool
	// budget is what both memos hold; see maxMemoBytes. One budget for the
	// session, because it is the session's retained heap that matters and
	// either memo alone can spend it. A name's mask words are charged with its
	// key text: at 1000 rules a bitset is 128 bytes, so a count cap alone
	// would not bound them.
	budget memoBudget
}

// lblMatchKey memoizes one label matcher's verdict on one value: label values
// repeat heavily within a scrape (bucket boundaries, namespaces, pod names),
// so each distinct (matcher, value) pair pays the regex once per scrape.
type lblMatchKey struct {
	re    *regexp.Regexp
	value string
}

// Keep reports whether a series passes the filter: the first rule whose name
// regex and label matchers all accept the series decides, and a series no rule
// matches is kept.
func (s *filterSession) Keep(name string, labels []Label) bool {
	if s.f == nil {
		return true
	}
	mask := s.mask(name)
	for w, word := range mask {
		for word != 0 {
			i := w*64 + bits.TrailingZeros64(word)
			word &= word - 1
			if r := &s.f.rules[i]; s.labelsMatch(r, labels) {
				return !r.drop
			}
		}
	}
	return true
}

// mask returns the bitset of rules whose NAME regex matches, memoized per name.
// The returned slice aliases session-owned memory and is valid until the next
// call — every reader consumes it before it returns.
func (s *filterSession) mask(name string) []uint64 {
	if off, ok := s.offsets[name]; ok {
		return s.maskWords[off : off+s.words]
	}
	dst := s.scratch
	if s.budget.admit(len(s.offsets), maxTrackedFamilies, len(name)+s.words*8) {
		off := len(s.maskWords)
		for range s.words {
			s.maskWords = append(s.maskWords, 0)
		}
		dst = s.maskWords[off : off+s.words]
		s.offsets[name] = off
	} else {
		clear(dst)
	}
	for i, r := range s.f.rules {
		if r.name == nil || r.name.MatchString(name) {
			dst[i/64] |= 1 << (i % 64)
		}
	}
	return dst
}

// labelsMatch reports whether every label matcher of r accepts the sample's
// value for its label (a missing label matches against ""), with the session's
// memo in front of each matcher's regex.
func (s *filterSession) labelsMatch(r *compiledRule, labels []Label) bool {
	for i := range r.labels {
		m := &r.labels[i]
		value := labelValue(labels, m.name)
		key := lblMatchKey{re: m.re, value: value}
		matched, ok := s.lblMatch[key]
		if !ok {
			matched = m.re.MatchString(value)
			if s.budget.admit(len(s.lblMatch), maxInternedValues, len(value)) {
				s.lblMatch[key] = matched
			}
		}
		if !matched {
			return false
		}
	}
	return true
}
