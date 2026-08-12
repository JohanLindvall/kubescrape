package metrics

import (
	"fmt"
	"log/slog"
	"maps"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/zeebo/xxh3"
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
	// maxStreamCap bounds a histogram's BUCKET SLOTS (label sets x buckets):
	// each live sample carries a counts entry per bucket, and every export
	// renders one bucket series per slot, so the product is what sizes both
	// the store and the payload. It is exactly the default histogram at the
	// hard cardinality cap (10000 label sets x 15 buckets incl. +Inf);
	// anything wider is a deliberate trade the operator must spell out by
	// lowering maxCardinality.
	// (defaultBuckets is a slice, so len() is not a constant expression;
	// TestStreamCapMatchesDefaultHistogram pins the two together.)
	maxStreamCap = maxCardinalityCap * 15
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
	// windowed aggregation min/max/avg/sum/count.
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

// ValueFunc resolves a metric's observed VALUE key against the caller's
// attributes (the tailer's record/resource ranking, logchain.Resolver.ValueFn).
//
// It is three-valued because two could not say WHY a lookup missed, and the
// answer decides whether the LINE is read: ok is a numeric hit, present says
// the attributes held the key at all — in the terms this tier uses everywhere
// (the first PRESENT rank, rendering non-empty; see logline.ResolveKey) — so a
// present-but-non-numeric attribute is a miss for the key rather than
// permission to look further down. Only the walk that resolved the value knows
// it without walking the ranks again.
type ValueFunc func(key string) (v float64, ok, present bool)

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
func (r *metricRule) observe(values func(string) (float64, bool), lookup func(string) string, res pcommon.Map, resAccum xxh3.Uint128, line string, ctx *logline.MatchContext, buf, rbuf labels) (labels, labels) {
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
		//
		// Gate on the MATCH alone, not on readValue: readValue also demands the
		// capture parse as a float, so a regex used as a pure content filter
		// (`action: inc, valueRegexp: ERROR`) or one capturing a non-numeric
		// group rejected every line it matched. The rule compiled without error
		// (inc/dec/count need no value source) and then silently reported
		// nothing forever.
		if !r.valueRe.MatchString(line) {
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
	case "sum":
		return actionSum, nil
	case "count":
		return actionCount, nil
	default:
		return 0, fmt.Errorf("invalid gauge action %q (want set, inc, dec, add, sub, min, max, avg, sum or count)", d.Action)
	}
}

// setConfig holds NewDynamicMetricSet options.
type setConfig struct {
	namePrefix string
	log        *slog.Logger
	// drops is the set's refusal counters, shared by every series it compiles.
	drops *drops
	// permanent classifies export errors (see WithPermanentClassifier).
	permanent func(error) bool
	// now overrides the coarse clock (tests only; see withClock).
	now func() int64
}

// Option configures a DynamicMetricSet.
type Option func(*setConfig)

// WithNamePrefix prepends prefix to every metric name.
func WithNamePrefix(prefix string) Option {
	return func(c *setConfig) { c.namePrefix = prefix }
}

// withClock overrides the coarse ten-second clock every series reads. Test-only
// (it is unexported): production always takes the package clock, which is what
// keeps the observe path free of the per-observation atomic load a
// process-global test override used to cost it.
func withClock(now func() int64) Option {
	return func(c *setConfig) { c.now = now }
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
	// drops counts the observations this set REFUSED. Per set, not per
	// process: obs registers getters over it (RegisterLogMetricsDrops), the
	// way it does for the disk buffer's stats and the self-metadata gauge.
	drops drops
	// Count is the number of configured rules.
	Count int
	// retryBy/retryOrder hold previous exports' UNDELIVERED samples, keyed by
	// resource, so the next Export re-offers them at their original snapshot
	// times. snapshot() is destructive — it seals aggregation windows, zeroes
	// idled samples and deletes expired ones — so without this a failed send
	// ended those observations' lives. Bounded by maxRetainedResources AND
	// maxRetainedSamples (retainedSamples is the running total); permanently
	// rejected chunks are dropped counted instead of retained (permanent).
	// Guarded by exportMu: the agent runs Export from one goroutine, but the
	// FINAL export joins the run loop through a BUDGETED wait, and on a blown
	// budget the two overlap — the old single-goroutine comment promised more
	// than that wiring delivers, and the mutex is uncontended everywhere else.
	exportMu        sync.Mutex
	permanent       func(error) bool
	retryBy         map[string][]seriesSamples
	retryOrder      []string
	retainedSamples int
}

// DroppedCapped counts observations this set rejected because a metric's
// label-set cardinality cap was reached.
func (s *DynamicMetricSet) DroppedCapped() uint64 { return s.drops.Capped() }

// DroppedUndelivered counts undelivered resources dropped because the re-offer
// buffer filled (maxRetainedResources / maxRetainedSamples) or because the
// collector rejected them PERMANENTLY (WithPermanentClassifier). Unlike a
// transiently failed export, which is retried, these observations are gone.
func (s *DynamicMetricSet) DroppedUndelivered() uint64 { return s.drops.Retained() }

// DroppedCappedByMetric reports this set's cap-refused observations per metric
// name — the only form an operator can act on, since the cap frees slots only
// through idleness.
func (s *DynamicMetricSet) DroppedCappedByMetric() map[string]float64 {
	return s.drops.CappedByMetric()
}

// DroppedNaN counts observations this set rejected because the extracted value
// was not finite (NaN or +/-Inf).
func (s *DynamicMetricSet) DroppedNaN() uint64 { return s.drops.NaN() }

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
	values  ValueFunc
	lookup  func(string) string
	raw     string
	labelFn func(string) string
	valueFn func(string) (float64, bool)
}

// labelLookup delegates to logline.ResolveKey — the one attrs-then-line-fields
// resolution tier, shared with the rules engine's filterCtx.resolve.
func (ac *addContext) labelLookup(key string) string {
	return logline.ResolveKey(key, ac.raw, ac.lookup, &ac.set.keys, &ac.line)
}

// valueLookup resolves a numeric key the same way: the caller's attributes
// first, the line's own fields as the fallback — and it must fall through on
// exactly the same condition the LABEL tier does, or one metric's label names
// one attribute while its observed value comes from another.
//
// present is why ValueFunc is three-valued: absent and present-but-non-numeric
// are both "no number", and only the first may read the line. Asking the LABEL
// closure instead walked every attribute rank a second time, in full, on every
// line whose value key lives on the line — which is the common case, since a
// value key that is an attribute answers on the first walk.
func (ac *addContext) valueLookup(key string) (float64, bool) {
	if ac.values != nil {
		v, ok, present := ac.values(key)
		if ok {
			return v, true
		}
		if present {
			return 0, false
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

	set := &DynamicMetricSet{log: cfg.log, permanent: cfg.permanent}
	cfg.drops = &set.drops
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
	accum xxh3.Uint128
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
func (b BoundResource) Add(values ValueFunc, lookup func(string) string, line string) {
	if b.set == nil || len(b.set.rules) == 0 {
		return
	}
	b.set.add(values, lookup, b.res, b.accum, line)
}

func (s *DynamicMetricSet) add(values ValueFunc, lookup func(string) string, resource pcommon.Map, resAccum xxh3.Uint128, line string) {
	ac := s.pool.Get().(*addContext)
	ac.ctx.Reset()
	ac.line.Reset(line)
	ac.values, ac.lookup, ac.raw = values, lookup, line

	for _, rule := range s.rules {
		ac.buf, ac.rbuf = rule.observe(ac.valueFn, ac.labelFn, resource, resAccum, line, &ac.ctx, ac.buf, ac.rbuf)
	}
	ac.values, ac.lookup, ac.raw = nil, nil, "" // do not retain caller state in the pool
	// Drop every reference into the line before pooling: the Fields holds the
	// line string + its raws/vals views (up to one ~1 MiB multiline body), and
	// buf/rbuf label values alias it too (a __line__ label IS the whole line).
	// Clearing lookup state alone left the last line pinned until the next Add.
	// clear() only, no alloc — the per-line budget tests must stay 0/1-alloc.
	ac.line.Release()
	clear(ac.buf[:cap(ac.buf)])
	clear(ac.rbuf[:cap(ac.rbuf)])
	s.pool.Put(ac)
}

// EmitDirect records one observation into a DECLARED metric, bypassing the
// line matching — the transform-script bridge (emit_metric). The metric must
// exist in the logMetrics config: declaration is where its type, action,
// buckets and cardinality cap live, and an undeclared name is a script bug
// reported as a script error rather than a silently minted unbounded series.
// The observation lands in the rule's own series under all the usual caps;
// labels are applied in sorted-key order for a deterministic identity. Safe
// on a nil set (returns an error naming the reason).
//
// THE RESOURCE IS READ AT CALL TIME, and that is the contract — not an
// accident of where the bridge happens to hold a handle. A transform script
// mutates resource attributes in place, so a script that edits the resource
// between two emit_metric calls puts the two observations in two different
// series; one that edits it first puts both under the edited one. The
// alternative — snapshotting the resource once when the batch is handed to the
// script — was rejected for two reasons: every other read a script makes
// (item.resource[k], the route glob, the attributes the payload finally ships
// under) sees the CURRENT value, so a single verb reading a stale one is the
// surprise; and a script that enriches the resource precisely so its metric
// carries the enrichment would then be silently ignored. It is deterministic
// either way — the script is hermetic and its statements are ordered — so the
// rule is "the resource as of this call", and it is pinned by test.
//
// Nothing is retained: the attributes are rendered into the sample's identity
// here (admit), so a later mutation of the caller's map cannot rewrite a series
// already created.
//
// At-least-once caveat, same as every producer's: a retried batch re-runs
// its script, so a transient export failure re-emits.
func (s *DynamicMetricSet) EmitDirect(name string, value float64, lbls map[string]string, resource pcommon.Map) error {
	if s == nil {
		return fmt.Errorf("emit_metric %q: no logMetrics section is configured", name)
	}
	for _, r := range s.rules {
		if r.series.name != name {
			continue
		}
		// The one runtime "le" check. Every other door into the store has
		// static label names (config specs, checked by rejectHistogramLe;
		// registry label sets, which come from code), but a script's label map
		// is arbitrary — and an "le" on a histogram would split the
		// distribution into one sample per value, each rendering its own full
		// bucket set. An error rather than a silent drop, matching how every
		// other mistake in emit_metric is reported.
		if _, ok := lbls[leLabel]; ok && r.series.kind == kindHistogram {
			return fmt.Errorf("emit_metric %q: a histogram may not set a label named %q — "+
				"it is the bucket-bound label generated from the histogram's own buckets", name, leLabel)
		}
		var buf labels
		for _, k := range slices.Sorted(maps.Keys(lbls)) {
			buf = buf.set(k, lbls[k])
		}
		r.series.observe(buf, value, uniqueResourceAccum(resource), resource, nil)
		return nil
	}
	return fmt.Errorf("emit_metric %q: no logMetrics rule declares this metric", name)
}

// uniqueResourceAccum folds a resource's attributes the way resourceString
// RENDERS them — last-wins per key — instead of one term per map ENTRY.
//
// resourceAccum sums a term per entry, which is right for every agent-built
// resource (they come from a Go map and cannot repeat a key) and wrong for one
// that arrived on the wire: OTLP encodes attributes as a repeated KeyValue and
// pdata does not dedupe on decode. {k=v, k=v} then hashes DISTINCT from {k=v}
// while both render {k="v"}, so the two live samples put byte-identical data
// points in one exported Metric; and {k=p, k=q} hashes IDENTICAL to {k=q, k=p}
// in both accumulators — a sum cannot tell a multiset from its permutation —
// so two senders' observations merge under whichever resource string was
// frozen at admit. Neither moves a counter: the collision check is a
// projection of the same pairs and agrees.
//
// EmitDirect is the door such a resource reaches the store through: a transform
// script's emit_metric over an ingested METRICS or TRACES payload, which
// otlpingest's dedupeResourceKeys does not cover (that runs on the log chain
// only, and before the transform seam). It is a cold per-script-call path that
// already allocates, so it pays the extra pass; Bind's per-flush hot path,
// whose resources are all agent-built, is untouched.
func uniqueResourceAccum(res pcommon.Map) xxh3.Uint128 {
	ls := make(labels, 0, res.Len())
	res.Range(func(k string, v pcommon.Value) bool {
		ls = ls.set(k, v.AsString())
		return true
	})
	var rk xxh3.Uint128
	for _, e := range ls {
		// set() already truncated the value, which is exactly what
		// resourceAccum's truncLabelCut reslice achieves: the hashed identity
		// must be the rendered one.
		hk, hv := strHash(e.key), strHash(e.value)
		rk = add128(rk, combineResHash(hk, hv))
	}
	return rk
}
