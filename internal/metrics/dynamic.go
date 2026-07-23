package metrics

import (
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"slices"
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
	// itself.
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

// kind resolves the metric type name to a seriesKind.
func (d *Dynamic) kind() (seriesKind, error) {
	switch strings.ToLower(d.Type) {
	case "", CounterType:
		return kindCounter, nil
	case GaugeType:
		return kindGauge, nil
	case HistogramType:
		return kindHistogram, nil
	case SummaryType:
		return kindSummary, nil
	default:
		return 0, fmt.Errorf("invalid metric type: %s", d.Type)
	}
}

// maxAge parses and clamps the expiration duration.
func (d *Dynamic) maxAge() (time.Duration, error) {
	if d.MaxAge == "" {
		return defaultMaxAge, nil
	}
	age, err := time.ParseDuration(d.MaxAge)
	if err != nil {
		return 0, err
	}
	if age <= 0 {
		// A zero/negative expiration would mark every sample idle on every
		// export, silently turning counters into per-interval deltas.
		return 0, fmt.Errorf("maxAge must be positive: %s", d.MaxAge)
	}
	return min(age, maxMaxAge), nil
}

// validateBuckets checks that histogram bounds increase and that non-histograms
// declare none.
func (d *Dynamic) validateBuckets(kind seriesKind) error {
	if kind != kindHistogram {
		if len(d.Buckets) > 0 {
			return errors.New("buckets can only be set for histogram metrics")
		}
		return nil
	}
	for i := 1; i < len(d.Buckets); i++ {
		if d.Buckets[i] <= d.Buckets[i-1] {
			return fmt.Errorf("buckets must be increasing: %v", d.Buckets)
		}
	}
	return nil
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

// compileRule builds one rule, reusing (or creating) the named series in shared.
func compileRule(d *Dynamic, cfg *setConfig, shared map[string]*series) (*metricRule, error) {
	kind, err := d.kind()
	if err != nil {
		return nil, err
	}
	action, err := d.gaugeAction(kind)
	if err != nil {
		return nil, err
	}
	if err := d.validateBuckets(kind); err != nil {
		return nil, err
	}
	age, err := d.maxAge()
	if err != nil {
		return nil, err
	}

	rule := &metricRule{value: d.Value}
	if d.ValueRegexp != "" {
		if d.Value != "" {
			return nil, errors.New("value and valueRegexp are mutually exclusive")
		}
		if rule.valueRe, err = regexp.Compile(d.ValueRegexp); err != nil {
			return nil, fmt.Errorf("invalid valueRegexp: %w", err)
		}
	}
	if rule.match, err = logline.ParseSelectors(d.Match, d.MatchRegexp); err != nil {
		return nil, err
	}
	if rule.labels, err = parseLabelTemplates(d.Labels, d.LabelPrefix); err != nil {
		return nil, err
	}
	if rule.resLabels, err = parseLabelTemplates(d.ResourceLabels, d.LabelPrefix); err != nil {
		return nil, err
	}

	name := cfg.namePrefix + d.Name
	if name == "" {
		return nil, fmt.Errorf("metric rule has no name")
	}
	if kind == kindHistogram {
		// Guard against the EFFECTIVE bucket count: an empty Buckets is replaced by
		// defaultBuckets in newSeries, so checking len(d.Buckets)+1 (==1 when empty)
		// would pass a maxCardinality below the real stream count and then admit
		// nothing at all (the all-or-nothing histogram pre-pass rejects every
		// observation) — silent total data loss instead of this compile-time error.
		streams := len(d.Buckets) + 1
		if len(d.Buckets) == 0 {
			streams = len(defaultBuckets) + 1
		}
		if cap := cardinalityCap(d.MaxCardinality); cap < streams {
			return nil, fmt.Errorf("metric %q: maxCardinality %d is below the histogram's %d bucket streams — nothing could ever be admitted", d.Name, cap, streams)
		}
	}
	if existing, ok := shared[name]; ok {
		if existing.kind != kind || existing.action != action {
			return nil, fmt.Errorf("metric %q declared with conflicting type/action", d.Name)
		}
		// A second rule's buckets/maxCardinality/maxAge would be silently
		// ignored (the first rule's series wins) — reject a conflicting
		// histogram bucket declaration like a conflicting type.
		if kind == kindHistogram && len(d.Buckets) > 0 && !slices.Equal(existing.buckets[:len(existing.buckets)-1], d.Buckets) {
			return nil, fmt.Errorf("metric %q declared with conflicting buckets", d.Name)
		}
		rule.series = existing
	} else {
		rule.series = newSeries(seriesSpec{
			name:       name,
			desc:       d.Description,
			kind:       kind,
			action:     action,
			maxSize:    cardinalityCap(d.MaxCardinality),
			expiration: age,
			buckets:    d.Buckets,
			log:        cfg.log,
		})
		shared[name] = rule.series
	}
	// A rule that must read a value but names no value source (no `value`, no
	// `valueRegexp`) resolves the empty key to nothing on every line and records
	// zero data — a silent misconfiguration. Reject it at compile time. Gauge
	// inc/dec/count tally lines and legitimately need no value.
	if rule.needsValue() && rule.value == "" && rule.valueRe == nil {
		return nil, fmt.Errorf("metric %q: type %q needs a value source — set `value` or `valueRegexp`", d.Name, d.Type)
	}
	return rule, nil
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

// cardinalityCap resolves the configured MaxCardinality: unset (or negative)
// means the documented default of maxCardinalityCap — never unlimited, since
// the cap is the defense against a high-cardinality label (request id, user
// id) exhausting the node agent's memory.
func cardinalityCap(configured int) int {
	if configured <= 0 {
		return maxCardinalityCap
	}
	return min(configured, maxCardinalityCap)
}

// parseLabelTemplates compiles a list of label specs.
func parseLabelTemplates(specs []string, setPrefix string) ([]labelTemplate, error) {
	if len(specs) == 0 {
		return nil, nil
	}
	out := make([]labelTemplate, 0, len(specs))
	for _, spec := range specs {
		lt, err := parseLabelTemplate(spec, setPrefix)
		if err != nil {
			return nil, err
		}
		out = append(out, lt)
	}
	return out, nil
}

// labelTemplateRe captures the four label forms: an optional `set=`, a `$get`
// or literal value, and an optional `(pattern)` or `/regex/`.
var labelTemplateRe = regexp.MustCompile(`^((?P<set>[a-zA-Z_:][a-zA-Z0-9_:.]*)=)?(?P<get>\$?[a-zA-Z_:][a-zA-Z0-9_:.]*)(\((?P<pat>[^)]+)\)|(?P<re>/.+))?$`)

// parseLabelTemplate compiles one `set=$get` label spec. setPrefix, when set,
// is prepended to the label's name.
func parseLabelTemplate(spec, setPrefix string) (labelTemplate, error) {
	match := labelTemplateRe.FindStringSubmatch(spec)
	if match == nil {
		return labelTemplate{}, fmt.Errorf("invalid label: %s", spec)
	}
	var lt labelTemplate
	getKey, value, pattern, reSpec := "", "", "", ""
	for i, name := range labelTemplateRe.SubexpNames() {
		if match[i] == "" {
			continue
		}
		switch name {
		case "set":
			lt.setKey = match[i]
		case "get":
			if strings.HasPrefix(match[i], "$") {
				getKey = match[i][1:]
			} else {
				value = match[i]
			}
		case "pat":
			pattern = match[i]
		case "re":
			reSpec = match[i]
		}
	}
	// Bare `key`: sets and reads itself.
	if value != "" && getKey == "" && lt.setKey == "" {
		getKey, lt.setKey, value = value, value, ""
	}
	if setPrefix != "" && lt.setKey != "" {
		lt.setKey = setPrefix + lt.setKey
	}
	if lt.setKey == "" || (value == "" && getKey == "") {
		return labelTemplate{}, fmt.Errorf("invalid label: %s", spec)
	}

	get, err := labelGetter(getKey, value, pattern, reSpec, spec)
	if err != nil {
		return labelTemplate{}, err
	}
	lt.get = get
	lt.getKey = getKey
	return lt, nil
}

// labelGetter builds the value-producing closure for a label template from
// whichever of the four forms is present.
func labelGetter(getKey, value, pattern, reSpec, spec string) (func(func(string) string) string, error) {
	switch {
	case value != "":
		return func(func(string) string) string { return value }, nil
	case pattern != "":
		return func(lookup func(string) string) string {
			return maskPattern(lookup(getKey), pattern)
		}, nil
	case reSpec != "":
		pat, repl, err := parseRegexpReplace(reSpec)
		if err != nil {
			return nil, fmt.Errorf("invalid label re %q: %w", spec, err)
		}
		re, err := regexp.Compile(pat)
		if err != nil {
			return nil, fmt.Errorf("invalid label re %q: %w", spec, err)
		}
		return func(lookup func(string) string) string {
			return re.ReplaceAllString(lookup(getKey), repl)
		}, nil
	default:
		return func(lookup func(string) string) string { return lookup(getKey) }, nil
	}
}

// maskPattern overlays value onto pattern: each '_' in pattern is replaced by
// the corresponding character of value, other characters are kept. So value
// "503" with pattern "_xx" yields "5xx".
func maskPattern(value, pattern string) string {
	if pattern == "" {
		return ""
	}
	out := make([]byte, len(pattern))
	for i := 0; i < len(pattern); i++ {
		if pattern[i] == '_' && i < len(value) {
			out[i] = value[i]
		} else {
			out[i] = pattern[i]
		}
	}
	return string(out)
}

// parseRegexpReplace splits a `/pattern/replacement/` spec, honouring
// backslash escapes.
func parseRegexpReplace(in string) (pattern, replacement string, err error) {
	if !strings.HasPrefix(in, "/") {
		return "", "", errors.New("must start with '/'")
	}
	var parts []string
	var buf strings.Builder
	var escaped bool
	for _, ch := range in[1:] {
		switch {
		case escaped:
			buf.WriteRune(ch)
			escaped = false
		case ch == '\\':
			escaped = true
		case ch == '/':
			parts = append(parts, buf.String())
			buf.Reset()
		default:
			buf.WriteRune(ch)
		}
	}
	if escaped {
		return "", "", errors.New("dangling backslash at end")
	}
	if buf.Len() > 0 || len(parts) < 2 {
		parts = append(parts, buf.String())
	}
	if len(parts) != 2 {
		return "", "", errors.New("must be in the form /pattern/replacement/")
	}
	return parts[0], parts[1], nil
}

// buildKeyIndex collects the distinct field keys referenced across rules:
// label getters, the observed value, and selector labels.
func buildKeyIndex(rules []*metricRule) logline.KeyIndex {
	ki := logline.NewKeyIndex()
	for _, r := range rules {
		ki.Add(r.value)
		for _, lt := range r.labels {
			ki.Add(lt.getKey)
		}
		for _, lt := range r.resLabels {
			ki.Add(lt.getKey)
		}
		for _, key := range r.match.LabelKeys() {
			ki.Add(key)
		}
	}
	return ki
}
