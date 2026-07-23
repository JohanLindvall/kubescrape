package metrics

import (
	"fmt"
	"log/slog"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"

	"github.com/JohanLindvall/kubescrape/internal/logline"
)

// Metric type names, as written in the `type` field of a Dynamic.
const (
	CounterType   = "counter"
	GaugeType     = "gauge"
	HistogramType = "histogram"
	SummaryType   = "summary"
)

const (
	defaultMaxAge     = 24 * time.Hour
	maxMaxAge         = 24 * time.Hour
	maxCardinalityCap = 10000
)

// Dynamic declares one metric derived from log lines: which lines it matches,
// the labels it carries and the value it observes. It is loaded from YAML (see
// LoadDynamicMetrics / DynamicConfig).
type Dynamic struct {
	// Name and Description are the metric's OTLP name and description.
	Name        string `json:"name"`
	Description string `json:"description,omitempty"`
	// Type is counter (default), gauge, histogram or summary.
	Type string `json:"type,omitempty"`
	// Action, for a gauge, selects how each observation folds: set (default,
	// last value wins), inc (+1), dec (-1), add (+value), sub (-value), or a
	// windowed aggregation min/max/avg/first/sum/count/stddev/range/delta.
	// Aggregations emit the aggregate on every export and keep it while no new
	// value arrives; the first value after an export starts a fresh window.
	// count only tallies matching lines (no value). Ignored for other types,
	// which always accumulate.
	Action string `json:"action,omitempty"`
	// Value names the numeric field to observe, or "1" to count matching lines.
	Value string `json:"value,omitempty"`
	// ValueRegexp instead extracts the observed value from the raw line via a
	// regex: capture group 1 (or the whole match) is parsed as a float, and a
	// line that does not match is skipped. Use it to pull a number out of an
	// unstructured line. Mutually exclusive with Value.
	ValueRegexp string `json:"valueRegexp,omitempty"`
	// Match are exact label selectors (key=value, or key!=value to negate);
	// MatchRegexp match the value against a regex. A line must satisfy all of
	// them. In the tailer selectors resolve against the log line's own fields
	// and the resource attributes (k8s metadata) alike. The synthetic key
	// `__line__` matches against the whole raw line.
	Match       []string `json:"match,omitempty"`
	MatchRegexp []string `json:"matchRegexp,omitempty"`
	// Labels are the metric's data-point labels, each `set=$get`: a literal
	// (set=value), a passthrough (set=$key), a masking pattern (set=$key(_xx))
	// or a regex replace (set=$key/re/repl/). A bare `key` both sets and reads
	// itself. In the regex form only `\/` and `\\` are DSL escapes (a literal
	// slash/backslash); every other backslash sequence passes through to the
	// regex engine unchanged, so `\d`, `\s` etc. work as written. A mask on a
	// line missing the source field drops the label (like the passthrough).
	Labels []string `json:"labels,omitempty"`
	// ResourceLabels are labels lifted onto the metric's OTLP resource instead
	// of its data points (same DSL as Labels). Use this to make a log-derived
	// attribute a resource attribute; the log line's own resource attributes are
	// always on the resource.
	ResourceLabels []string `json:"resourceLabels,omitempty"`
	// Buckets are the histogram boundaries (histogram type only).
	Buckets []float64 `json:"buckets,omitempty"`
	// MaxCardinality caps unique label combinations (default/hard cap 10000);
	// MaxAge expires idle series (a Go duration, default/cap 24h).
	MaxCardinality int    `json:"maxCardinality,omitempty"`
	MaxAge         string `json:"maxAge,omitempty"`
	// LabelPrefix is prepended to every set label name.
	LabelPrefix string `json:"labelPrefix,omitempty"`
}

// DynamicConfig is the log-derived-metrics config shape (the `logMetrics`
// section of the unified agent config, or a standalone file).
type DynamicConfig struct {
	Metrics []Dynamic `json:"metrics"`
}

// labelTemplate produces one metric label from a line's fields. getKey is the
// line field it reads ("" for a literal), recorded so the set knows which line
// fields to parse.
type labelTemplate struct {
	setKey string
	getKey string
	get    func(lookup func(string) string) string
}

// metricRule is a compiled Dynamic: a series plus the match/label logic that
// feeds it.
type metricRule struct {
	series    *series
	match     *logline.Selectors
	labels    []labelTemplate // data-point labels
	resLabels []labelTemplate // labels lifted onto the resource
	value     string
	valueRe   *regexp.Regexp // extracts the value from the raw line, if set
}

// needsValue reports whether the rule must read a value (skip the line when it
// is absent/zero). Gauge inc/dec/count only tally lines and need none.
func (r *metricRule) needsValue() bool {
	if r.series.kind == kindGauge {
		switch r.series.action {
		case actionInc, actionDec, actionCount:
			return false
		}
	}
	return true
}

// readValue resolves the observed value and whether the line should be
// recorded. It comes from ValueRegexp (a capture off the raw line), the "1"
// count, or a numeric field via values.
func (r *metricRule) readValue(values func(string) (float64, bool), line string) (float64, bool) {
	if r.valueRe != nil {
		m := r.valueRe.FindStringSubmatch(line)
		if m == nil {
			return 0, false
		}
		s := m[0]
		if len(m) > 1 {
			s = m[1]
		}
		f, err := strconv.ParseFloat(s, 64)
		return f, err == nil
	}
	if r.value == "1" {
		return 1, true
	}
	if values == nil {
		return 0, false
	}
	return values(r.value) // presence-based: a legitimate 0 records
}

// observe evaluates the rule against one line and records an observation when
// it matches. buf/rbuf are reused for the data-point and resource label sets and
// returned (set may grow them). resAccum is the hash of res (the line's resource
// attributes), computed once by the caller.
func (r *metricRule) observe(values func(string) (float64, bool), lookup func(string) string, res pcommon.Map, resAccum resKey, line string, ctx *logline.MatchContext, buf, rbuf labels) (labels, labels) {
	if !r.match.Match(lookup, ctx) {
		return buf, rbuf
	}
	var value float64
	if r.needsValue() {
		v, ok := r.readValue(values, line)
		if !ok {
			return buf, rbuf
		}
		value = v
	} else if r.valueRe != nil {
		// ValueRegexp both extracts and FILTERS ("a line that does not match is
		// skipped"). inc/dec/count ignore the extracted value, but the filter
		// still applies — otherwise they would tally lines the regex rejects.
		if _, ok := r.readValue(values, line); !ok {
			return buf, rbuf
		}
	}
	buf = buf[:0]
	for _, lt := range r.labels {
		buf = buf.set(lt.setKey, lt.get(lookup))
	}
	rbuf = rbuf[:0]
	for _, lt := range r.resLabels {
		rbuf = rbuf.set(lt.setKey, lt.get(lookup))
	}
	r.series.observe(buf, value, resAccum, res, rbuf)
	return buf, rbuf
}

// gaugeAction resolves the fold action for a metric, validating that non-gauge
// metrics do not set one.
func (d *Dynamic) gaugeAction(kind seriesKind) (gaugeAction, error) {
	if d.Action == "" {
		return actionSet, nil
	}
	if kind != kindGauge {
		return 0, fmt.Errorf("action is only valid for gauge metrics (got %q)", d.Type)
	}
	switch strings.ToLower(d.Action) {
	case "set":
		return actionSet, nil
	case "inc":
		return actionInc, nil
	case "dec":
		return actionDec, nil
	case "add":
		return actionAdd, nil
	case "sub":
		return actionSub, nil
	case "min":
		return actionMin, nil
	case "max":
		return actionMax, nil
	case "avg":
		return actionAvg, nil
	case "first":
		return actionFirst, nil
	case "sum":
		return actionSum, nil
	case "count":
		return actionCount, nil
	case "stddev":
		return actionStddev, nil
	case "range":
		return actionRange, nil
	case "delta":
		return actionDelta, nil
	default:
		return 0, fmt.Errorf("invalid gauge action: %s", d.Action)
	}
}

// setConfig holds NewDynamicMetricSet options.
type setConfig struct {
	namePrefix string
	log        *slog.Logger
}

// Option configures a DynamicMetricSet.
type Option func(*setConfig)

// WithNamePrefix prepends prefix to every metric name.
func WithNamePrefix(prefix string) Option {
	return func(c *setConfig) { c.namePrefix = prefix }
}

// WithLogger sets the logger used for cardinality warnings.
func WithLogger(l *slog.Logger) Option {
	return func(c *setConfig) {
		if l != nil {
			c.log = l
		}
	}
}

// DynamicMetricSet is a set of log-derived metrics evaluated per line.
type DynamicMetricSet struct {
	rules []*metricRule
	keys  logline.KeyIndex
	pool  sync.Pool
	log   *slog.Logger
	// Count is the number of configured rules.
	Count int
}

// addContext is the per-line scratch state pooled across Add calls. labelFn
// and valueFn are bound once at construction (closing over the context) so a
// line's evaluation allocates no closures; the per-line inputs live in the
// set/values/lookup/raw fields.
type addContext struct {
	ctx  logline.MatchContext
	buf  labels // data-point labels
	rbuf labels // resource labels
	line logline.Fields

	set     *DynamicMetricSet
	values  func(string) (float64, bool)
	lookup  func(string) string
	raw     string
	labelFn func(string) string
	valueFn func(string) (float64, bool)
}

// labelLookup resolves a label key: the synthetic __line__ key is the whole
// raw line; otherwise the caller's own lookup (record/resource attributes)
// wins and the line's parsed fields are the fallback.
func (ac *addContext) labelLookup(key string) string {
	if key == logline.LineKey {
		return ac.raw
	}
	if ac.lookup != nil {
		if v := ac.lookup(key); v != "" {
			return v
		}
	}
	return ac.set.keys.Get(&ac.line, key)
}

// valueLookup resolves a numeric key the same way.
func (ac *addContext) valueLookup(key string) (float64, bool) {
	if ac.values != nil {
		if v, ok := ac.values(key); ok {
			return v, true
		}
	}
	raw := ac.set.keys.Get(&ac.line, key)
	if raw == "" {
		return 0, false
	}
	f, err := strconv.ParseFloat(raw, 64)
	return f, err == nil
}

// NewDynamicMetricSet compiles a metric specification into an evaluatable set.
// Rules sharing a metric name share one underlying series.
func NewDynamicMetricSet(metrics []Dynamic, opts ...Option) (*DynamicMetricSet, error) {
	cfg := setConfig{log: slog.Default()}
	for _, opt := range opts {
		opt(&cfg)
	}

	set := &DynamicMetricSet{log: cfg.log}
	set.pool = sync.Pool{New: func() any {
		ac := &addContext{buf: make(labels, 0, 16), rbuf: make(labels, 0, 8), set: set}
		// Bind the lookup closures once; per-line state flows through fields.
		ac.labelFn = ac.labelLookup
		ac.valueFn = ac.valueLookup
		return ac
	}}
	byName := map[string]*series{}
	for i := range metrics {
		rule, err := compileRule(&metrics[i], &cfg, byName)
		if err != nil {
			return nil, err
		}
		set.rules = append(set.rules, rule)
	}
	set.keys = buildKeyIndex(set.rules)
	set.Count = len(set.rules)
	return set, nil
}

// BoundResource is a DynamicMetricSet bound to one resource, with the
// resource's hash precomputed — use it to Add many lines sharing the same
// resource attributes (e.g. all records of one file in a flush) without
// re-hashing the resource per line.
type BoundResource struct {
	set   *DynamicMetricSet
	res   pcommon.Map
	accum resKey
}

// Bind precomputes the per-resource state for repeated Adds. Safe on a nil
// set (Add becomes a no-op).
func (s *DynamicMetricSet) Bind(resource pcommon.Map) BoundResource {
	b := BoundResource{set: s, res: resource}
	if s != nil && len(s.rules) > 0 {
		b.accum = resourceAccum(resource)
	}
	return b
}

// Add evaluates every rule against one line, as DynamicMetricSet.Add.
func (b BoundResource) Add(values func(string) (float64, bool), lookup func(string) string, line string) {
	if b.set == nil || len(b.set.rules) == 0 {
		return
	}
	b.set.add(values, lookup, b.res, b.accum, line)
}

func (s *DynamicMetricSet) add(values func(string) (float64, bool), lookup func(string) string, resource pcommon.Map, resAccum resKey, line string) {
	ac := s.pool.Get().(*addContext)
	ac.ctx.Reset()
	ac.line.Reset(line)
	ac.values, ac.lookup, ac.raw = values, lookup, line

	for _, rule := range s.rules {
		ac.buf, ac.rbuf = rule.observe(ac.valueFn, ac.labelFn, resource, resAccum, line, &ac.ctx, ac.buf, ac.rbuf)
	}
	ac.values, ac.lookup, ac.raw = nil, nil, "" // do not retain caller state in the pool
	s.pool.Put(ac)
}
