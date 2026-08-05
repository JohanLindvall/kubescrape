// Package spanmetrics derives RED (Request/Error/Duration) metrics from ingested
// OTLP trace spans, following the OpenTelemetry spanmetrics conventions: a
// monotonic `calls` counter, a `size` counter (span bytes), and a `duration`
// histogram (seconds, with trace-id exemplars), dimensioned by service.name /
// span.name / span.kind / status.code plus configurable extra attributes.
//
// It plugs into the agent's OTLP-ingest traces path as a TracesExporter tap —
// spans are aggregated as a side effect and still forwarded — and the metrics
// are exported over OTLP on an interval like every other agent metric. Each span
// is aggregated independently, so a node-local agent (which only sees the spans
// pushed to its node) still produces correct per-service RED metrics; Prometheus
// sums the cumulative counters across agents. (Service-graph edge metrics, which
// require pairing a request's client and server spans, are deliberately NOT
// derived here — those two spans usually land on different nodes' agents, so a
// single agent never sees both halves.)
//
// The generator is a self-contained cumulative aggregator (not the shared
// metrics.Registry): exemplars are a histogram-data-point feature the Registry
// cannot express, and owning the aggregation also gives the size counter and
// units a single coherent home. The state machine underneath — admission under
// a cardinality cap, the observed/rendered/delivered gate, stale eviction and
// the export loop — is agent/cumagg, shared with agent/servicegraph, which
// needs exactly the same decisions and once made two of them differently. What
// stays here is what is this aggregator's own: the metric names, the dimension
// set and the per-span aggregate.
package spanmetrics

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/JohanLindvall/kubescrape/internal/agent/cumagg"
	"github.com/JohanLindvall/kubescrape/internal/obs"
)

const scopeName = "github.com/JohanLindvall/kubescrape/agent/spanmetrics"

// Built-in dimension label names (OTel-style dotted keys; the exporter renders
// them to Prometheus as service_name, span_name, …).
const (
	dimService = "service.name"
	dimSpan    = "span.name"
	dimKind    = "span.kind"
	dimStatus  = "status.code"
)

// builtinDims are the fixed dimension label names, in the order observe/dims emit
// them (extra configured dimensions follow).
var builtinDims = []string{dimService, dimSpan, dimKind, dimStatus}

// builtins is the collision guard for configured dimensions; see cumagg.Builtins
// for what a colliding name does and why the check runs at construction here.
var builtins = cumagg.NewBuiltins(builtinDims...)

// defaultBuckets are the classic spanmetrics latency boundaries in SECONDS.
var defaultBuckets = []float64{0.002, 0.004, 0.006, 0.008, 0.01, 0.05, 0.1, 0.2, 0.4, 0.8, 1, 1.4, 2, 5, 10, 15}

const (
	defaultNamePrefix     = "traces.span.metrics"
	defaultMaxCardinality = 20000
	// defaultStaleAfter drops a series whose dimensions have not been seen for
	// this long. Long enough that a slow-but-live endpoint keeps reporting,
	// short enough that a burst of one-off span names releases its cardinality
	// slots within one alerting window.
	defaultStaleAfter = 15 * time.Minute
)

// maxDimBytes bounds one dimension value; see cumagg.MaxLabelBytes for why.
const maxDimBytes = cumagg.MaxLabelBytes

// Exporter sends one OTLP metrics payload; satisfied by otlpexport.Client.
type Exporter = cumagg.Exporter

// Config tunes the generator. The zero value is valid and uses the defaults.
type Config struct {
	// NamePrefix prefixes the span-metric names (default "traces.span.metrics",
	// giving .calls, .size and .duration).
	NamePrefix string `json:"namePrefix,omitempty"`
	// Buckets are the duration histogram boundaries in SECONDS (default: the
	// spanmetrics latency buckets).
	Buckets []float64 `json:"buckets,omitempty"`
	// Dimensions are extra span (falling back to resource) attribute keys to add
	// as labels, beyond the four built-ins. A missing attribute yields "".
	Dimensions []string `json:"dimensions,omitempty"`
	// MaxCardinality caps the number of distinct dimension tuples; over the cap,
	// spans are dropped and counted (default 20000, 0 = default).
	MaxCardinality int `json:"maxCardinality,omitempty"`
	// Exemplars attaches a trace/span-id exemplar (one per latency bucket, reset
	// each export) to the duration histogram. nil defaults to true.
	Exemplars *bool `json:"exemplars,omitempty"`
	// StaleAfter evicts a series whose dimensions have not been observed for
	// this long (a Go duration such as "15m"; empty = 15m, "0" disables
	// eviction and keeps every series for the process' life). A negative value
	// is refused — see cumagg.ParseStaleAfter.
	//
	// A STRING, not a time.Duration, for the same reason as traceSampling's
	// keepSlowerThan: the config is decoded through sigs.k8s.io/yaml ->
	// encoding/json, which only accepts a raw nanosecond integer for a
	// time.Duration, so the documented "15m" spelling would fail to decode.
	StaleAfter string `json:"staleAfter,omitempty"`
}

// staleAfter parses StaleAfter (empty = the default, "0" disables eviction, a
// negative value is an error).
func (c Config) staleAfter() (time.Duration, error) {
	return cumagg.ParseStaleAfter("traceMetrics.staleAfter", c.StaleAfter, defaultStaleAfter)
}

// Validate reports a malformed config so a bad value can fail startup with a
// clear message (New itself falls back to the default, never refusing to
// aggregate).
func (c Config) Validate() error {
	if _, err := c.staleAfter(); err != nil {
		return err
	}
	if c.MaxCardinality < 0 {
		return fmt.Errorf("maxCardinality must not be negative")
	}
	// The same check servicegraph.Config.Validate applies to its identical
	// histogramBuckets field. Without it, boundsOrDefault sorts but never
	// de-duplicates, so `buckets: [0.1, 0.1, 1]` shipped an OTLP histogram
	// whose ExplicitBounds are not strictly increasing — which the spec
	// requires — giving a bucket that can never receive a value and a
	// backend that either rejects the metric or renders a nonsense
	// distribution. The two aggregators are the same shape and had drifted on
	// exactly this kind of rule before (see cumagg.ParseStaleAfter).
	var prev float64
	for i, b := range c.Buckets {
		if b <= 0 {
			return fmt.Errorf("buckets[%d] = %v (want > 0, in seconds)", i, b)
		}
		if i > 0 && b <= prev {
			return fmt.Errorf("buckets must be strictly increasing (%v after %v)", b, prev)
		}
		prev = b
	}
	return nil
}

// Generator aggregates spans into calls/size/duration metrics. Safe for
// concurrent Consume from the ingest goroutines.
type Generator struct {
	prefix    string
	names     []string // full dimension label names (built-ins + extras), in order
	extra     []string
	bounds    []float64 // histogram bucket bounds, ascending, seconds
	exemplars bool
	now       func() time.Time

	store *cumagg.Store[*spanSeries]
}

type spanSeries struct {
	// Meta is the shared bookkeeping: creation time (the cumulative start
	// timestamp), last observation and the observed/rendered/delivered state
	// eviction is gated on.
	cumagg.Meta
	dims    []string // dimension values, aligned with Generator.names
	calls   uint64
	size    int64
	count   uint64
	sum     float64
	buckets []uint64          // len(bounds)+1
	ex      []cumagg.Exemplar // nil until an exemplar is recorded; one latest per bucket
}

// resetExemplars is the store's after-delivery hook (a failed send keeps them).
func (s *spanSeries) resetExemplars() { cumagg.ClearExemplars(s.ex) }

// clock reads the injectable now through the Generator, so a test that replaces
// g.now after New is also replacing the clock the store exports on.
func (g *Generator) clock() time.Time { return g.now() }

// New builds a generator from cfg.
func New(cfg Config) *Generator {
	prefix := cfg.NamePrefix
	if prefix == "" {
		prefix = defaultNamePrefix
	}
	maxCard := cfg.MaxCardinality
	if maxCard <= 0 {
		maxCard = defaultMaxCardinality
	}
	ex := true
	if cfg.Exemplars != nil {
		ex = *cfg.Exemplars
	}
	// Drop configured dimensions that repeat a built-in (or each other). This is
	// the ONE place this generator's label names are decided, which is why the
	// guard runs here; cumagg.Builtins holds the rule and the bug it prevents.
	names := append([]string(nil), builtinDims...)
	names = append(names, builtins.Filter(cfg.Dimensions)...)
	stale, _ := cfg.staleAfter() // an unparseable value falls back to the default; Validate reports it
	g := &Generator{
		prefix:    prefix,
		names:     names,
		extra:     names[len(builtinDims):], // the configured dimensions, aliased (never diverges from names)
		bounds:    boundsOrDefault(cfg.Buckets),
		exemplars: ex,
		now:       time.Now,
	}
	g.store = cumagg.NewStore(cumagg.Options[*spanSeries]{
		Scope:          scopeName,
		Name:           "span metrics",
		MaxCardinality: maxCard,
		StaleAfter:     stale,
		Dropped:        obs.SpanMetricsDropped,
		Evicted:        obs.SpanMetricsEvicted,
		Now:            g.clock,
		NewSeries:      func() *spanSeries { return &spanSeries{} },
		Render:         g.renderRED,
		ResetExemplars: (*spanSeries).resetExemplars,
	})
	return g
}

// boundsOrDefault returns a sorted copy of b, or the default buckets when empty.
func boundsOrDefault(b []float64) []float64 {
	if len(b) == 0 {
		b = defaultBuckets
	}
	out := append([]float64(nil), b...)
	sort.Float64s(out)
	return out
}

// Consume aggregates every span in td (called on the ingest goroutines, so it is
// safe for concurrent use). It never mutates td.
func (g *Generator) Consume(td ptrace.Traces) {
	// One clock read per BATCH, not per span: last-seen only feeds staleness
	// eviction (minutes), and the hot path must stay allocation- and
	// syscall-free per span.
	now := g.now()
	rss := td.ResourceSpans()
	for i := 0; i < rss.Len(); i++ {
		rs := rss.At(i)
		resAttrs := rs.Resource().Attributes()
		svc := cumagg.AttrStr(resAttrs, dimService)
		sss := rs.ScopeSpans()
		for j := 0; j < sss.Len(); j++ {
			spans := sss.At(j).Spans()
			for k := 0; k < spans.Len(); k++ {
				g.observe(spans.At(k), resAttrs, svc, now)
			}
		}
	}
}

func (g *Generator) observe(span ptrace.Span, resAttrs pcommon.Map, svc string, now time.Time) {
	// Build the map key on the stack (it does not escape → the lookup inside
	// AdmitLocked allocates nothing for a warm series). A key over keyScratch
	// bytes falls back to a one-off heap grow.
	//
	// Every part is cut at exactly the length dims() cuts the values it RENDERS
	// to (cumagg.Retain, which differs only in cloning what it keeps); see
	// cumagg.Trunc for why keying on the untruncated value is a duplicate
	// series. Kind and status are closed enum spellings, truncated nowhere.
	var keyScratch [256]byte
	key := keyScratch[:0]
	key = cumagg.AppendKeyPart(key, cumagg.Trunc(svc))
	key = cumagg.AppendKeyPart(key, cumagg.Trunc(span.Name()))
	key = cumagg.AppendKeyPart(key, span.Kind().String())
	key = cumagg.AppendKeyPart(key, span.Status().Code().String())
	for _, k := range g.extra {
		v := cumagg.AttrStr(span.Attributes(), k)
		if v == "" {
			v = cumagg.AttrStr(resAttrs, k) // fall back to the resource
		}
		key = cumagg.AppendKeyPart(key, cumagg.Trunc(v))
	}
	d := cumagg.SpanSeconds(span)
	sz := spanSize(span)
	idx := cumagg.BucketIndex(g.bounds, d)

	g.store.Lock()
	s, fresh, ok := g.store.AdmitLocked(key, now)
	if !ok {
		g.store.Unlock() // over the cardinality cap; AdmitLocked counted it
		return
	}
	if fresh {
		s.dims = g.dims(span, resAttrs, svc)
		s.buckets = make([]uint64, len(g.bounds)+1)
	}
	s.calls++
	s.size += sz
	s.count++
	s.sum += d
	s.buckets[idx]++
	if g.exemplars {
		s.ex = cumagg.RecordExemplar(s.ex, len(g.bounds)+1, idx, d,
			span.EndTimestamp(), span.TraceID(), span.SpanID())
	}
	g.store.ObservedLocked(s, now)
	g.store.Unlock()
}

// dims materializes the dimension values for a new series (cold path). Values
// are retained for the series' life, so they are cloned where they were cut
// (cumagg.Retain) rather than left pointing into the sender's payload.
func (g *Generator) dims(span ptrace.Span, resAttrs pcommon.Map, svc string) []string {
	vals := make([]string, 0, len(g.names))
	vals = append(vals, cumagg.Retain(svc), cumagg.Retain(span.Name()),
		span.Kind().String(), span.Status().Code().String())
	for _, k := range g.extra {
		v := cumagg.AttrStr(span.Attributes(), k)
		if v == "" {
			v = cumagg.AttrStr(resAttrs, k)
		}
		vals = append(vals, cumagg.Retain(v))
	}
	return vals
}

// Run exports every interval until ctx is done, then once more on a detached
// context (cumagg.Store.Run).
func (g *Generator) Run(ctx context.Context, exp Exporter, interval time.Duration, res pcommon.Resource, log *slog.Logger) {
	g.store.Run(ctx, exp, interval, res, log)
}

// Export renders the current cumulative aggregate under res and sends it once.
// Exemplars are cleared only after a SUCCESSFUL send (recent-evidence semantics
// per delivered export): a failed send keeps them for the next attempt instead
// of wiping them unseen.
func (g *Generator) Export(ctx context.Context, exp Exporter, res pcommon.Resource) error {
	return g.store.Export(ctx, exp, res)
}

// renderRED writes the three RED metrics for every live series. It is the
// store's Render callback.
//
// Unlike agent/servicegraph's, it builds the payload UNDER the series lock: the
// two aggregators' receive paths differ in what a stall costs (there, Record is
// called from inside the pairing store's own mutex, so a long hold stops every
// shard goroutine), and the lock strategy is deliberately the caller's to pick.
func (g *Generator) renderRED(sm pmetric.ScopeMetrics, now time.Time) {
	g.store.Lock()
	defer g.store.Unlock()
	g.store.EvictLocked(now)
	if g.store.CountLocked() == 0 {
		return
	}
	ts := pcommon.NewTimestampFromTime(now)
	calls := cumagg.SumMetric(sm, g.prefix+".calls", "Count of spans observed, by dimensions.", "")
	size := cumagg.SumMetric(sm, g.prefix+".size", "Total size of spans observed, in bytes.", "By")
	dur := cumagg.HistMetric(sm, g.prefix+".duration", "Span duration in seconds, by dimensions.")
	g.store.EachLocked(func(s *spanSeries) {
		g.store.MarkRenderedLocked(s)
		start := pcommon.NewTimestampFromTime(s.Start)
		cp := calls.AppendEmpty()
		putDims(cp.Attributes(), g.names, s.dims)
		cp.SetStartTimestamp(start)
		cp.SetTimestamp(ts)
		cp.SetIntValue(int64(s.calls))

		zp := size.AppendEmpty()
		putDims(zp.Attributes(), g.names, s.dims)
		zp.SetStartTimestamp(start)
		zp.SetTimestamp(ts)
		zp.SetIntValue(s.size)

		hp := dur.AppendEmpty()
		putDims(hp.Attributes(), g.names, s.dims)
		hp.SetStartTimestamp(start)
		hp.SetTimestamp(ts)
		hp.SetCount(s.count)
		hp.SetSum(s.sum)
		hp.ExplicitBounds().FromRaw(g.bounds)
		hp.BucketCounts().FromRaw(s.buckets)
		for i := range s.ex {
			if s.ex[i].Set {
				cumagg.PutExemplar(hp.Exemplars(), s.ex[i])
			}
		}
	})
}

// Tap returns a TracesExporter that feeds each batch through Consume and then
// forwards it to inner. The generator observes ENRICHED spans because the ingest
// server enriches in place before calling the exporter.
func (g *Generator) Tap(inner TracesExporter) TracesExporter {
	return &tap{gen: g, inner: inner}
}

// TracesExporter forwards traces onward (structurally identical to the ingest
// server's own interface, so a tap satisfies it too).
type TracesExporter interface {
	ExportTraces(ctx context.Context, td ptrace.Traces) error
}

type tap struct {
	gen   *Generator
	inner TracesExporter
}

func (t *tap) ExportTraces(ctx context.Context, td ptrace.Traces) error {
	// Forward FIRST, aggregate only on success: a transient failure surfaces to
	// the sender as retryable, and the re-pushed batch would otherwise aggregate
	// twice — permanently inflating the cumulative counters across every outage
	// or back-pressure window. (A retry after a lost ack still double-counts;
	// that is the unavoidable at-least-once residue.)
	if err := t.inner.ExportTraces(ctx, td); err != nil {
		return err
	}
	t.gen.Consume(td)
	return nil
}

// --- span sizing ---

// spanSize approximates the span's OTLP encoded byte size (name + ids +
// attributes + events + links) — a cheap, allocation-free size signal for the
// size counter, not the exact proto size.
func spanSize(span ptrace.Span) int64 {
	n := int64(len(span.Name()) + 24) // name + trace id (16) + span id (8)
	n += attrsSize(span.Attributes())
	events := span.Events()
	for i := 0; i < events.Len(); i++ {
		e := events.At(i)
		n += int64(len(e.Name())) + attrsSize(e.Attributes())
	}
	links := span.Links()
	for i := 0; i < links.Len(); i++ {
		n += 24 + attrsSize(links.At(i).Attributes())
	}
	return n
}

func attrsSize(m pcommon.Map) int64 {
	var n int64
	m.Range(func(k string, v pcommon.Value) bool {
		n += int64(len(k) + valueSize(v))
		return true
	})
	return n
}

// valueSize estimates an attribute value's byte size without allocating (AsString
// would format non-string values onto the heap).
func valueSize(v pcommon.Value) int {
	switch v.Type() {
	case pcommon.ValueTypeStr:
		return len(v.Str())
	case pcommon.ValueTypeBytes:
		return v.Bytes().Len()
	case pcommon.ValueTypeBool:
		return 1
	default: // int, double, empty, slice, map
		return 8
	}
}

func putDims(a pcommon.Map, names, dims []string) {
	for i, name := range names {
		if i < len(dims) {
			a.PutStr(name, dims[i])
		}
	}
}
