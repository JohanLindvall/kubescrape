// Package spanmetrics derives RED (Request/Error/Duration) metrics from ingested
// OTLP trace spans, following the OpenTelemetry spanmetrics conventions: a
// monotonic `calls` counter, a `size` counter (span bytes), and a `duration`
// histogram (seconds, with trace-id exemplars), dimensioned by service.name /
// span.name / span.kind / status.code plus configurable extra attributes. With
// ServiceGraphs it additionally derives service-graph edge metrics (see
// servicegraph.go).
//
// It plugs into the agent's OTLP-ingest traces path as a TracesExporter tap —
// spans are aggregated as a side effect and still forwarded — and the metrics
// are exported over OTLP on an interval like every other agent metric.
//
// The generator is a self-contained cumulative aggregator (not the shared
// metrics.Registry): exemplars are a histogram-data-point feature the Registry
// cannot express, and owning the aggregation also gives the size counter, units,
// and the service-graph pairing a single coherent home. A cardinality cap bounds
// the data-driven label sets.
package spanmetrics

import (
	"context"
	"log/slog"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"

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

// defaultBuckets are the classic spanmetrics latency boundaries in SECONDS.
var defaultBuckets = []float64{0.002, 0.004, 0.006, 0.008, 0.01, 0.05, 0.1, 0.2, 0.4, 0.8, 1, 1.4, 2, 5, 10, 15}

const (
	defaultNamePrefix     = "traces.span.metrics"
	defaultMaxCardinality = 20000
)

// Exporter sends one OTLP metrics payload; satisfied by otlpexport.Client.
type Exporter interface {
	ExportMetrics(ctx context.Context, md pmetric.Metrics) error
}

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
	// MaxCardinality caps the number of distinct dimension tuples (and, for
	// service graphs, pending half-edges and edges); over the cap, spans/edges
	// are dropped and counted (default 20000, 0 = default).
	MaxCardinality int `json:"maxCardinality,omitempty"`
	// Exemplars attaches a trace/span-id exemplar (one per latency bucket, reset
	// each export) to the duration histogram. nil defaults to true.
	Exemplars *bool `json:"exemplars,omitempty"`
	// ServiceGraphs additionally derives service-graph edge metrics (request and
	// error counts, client/server latency per client→server pair) by pairing a
	// client span with its child server span.
	ServiceGraphs bool `json:"serviceGraphs,omitempty"`
	// ServiceGraphBuckets are the edge-latency histogram boundaries in SECONDS
	// (default: the span-metrics latency buckets).
	ServiceGraphBuckets []float64 `json:"serviceGraphBuckets,omitempty"`
}

// Generator aggregates spans into calls/size/duration metrics (and optional
// service-graph edges). Safe for concurrent Consume from the ingest goroutines.
type Generator struct {
	prefix    string
	names     []string // full dimension label names (built-ins + extras), in order
	extra     []string
	bounds    []float64 // histogram bucket bounds, ascending, seconds
	maxCard   int
	exemplars bool

	mu     sync.Mutex
	series map[string]*spanSeries
	start  time.Time

	sg *serviceGraph // nil unless ServiceGraphs
}

type spanSeries struct {
	dims    []string // dimension values, aligned with Generator.names
	calls   uint64
	size    int64
	count   uint64
	sum     float64
	buckets []uint64   // len(bounds)+1
	ex      []exemplar // nil until an exemplar is recorded; one latest per bucket
}

type exemplar struct {
	set     bool
	value   float64
	ts      pcommon.Timestamp
	traceID pcommon.TraceID
	spanID  pcommon.SpanID
}

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
	names := append([]string{dimService, dimSpan, dimKind, dimStatus}, cfg.Dimensions...)
	g := &Generator{
		prefix:    prefix,
		names:     names,
		extra:     append([]string(nil), cfg.Dimensions...),
		bounds:    boundsOrDefault(cfg.Buckets),
		maxCard:   maxCard,
		exemplars: ex,
		series:    make(map[string]*spanSeries),
		start:     time.Now(),
	}
	if cfg.ServiceGraphs {
		g.sg = newServiceGraph(boundsOrDefault(cfg.ServiceGraphBuckets), maxCard)
	}
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
	rss := td.ResourceSpans()
	for i := 0; i < rss.Len(); i++ {
		rs := rss.At(i)
		resAttrs := rs.Resource().Attributes()
		svc := attrStr(resAttrs, dimService)
		sss := rs.ScopeSpans()
		for j := 0; j < sss.Len(); j++ {
			spans := sss.At(j).Spans()
			for k := 0; k < spans.Len(); k++ {
				span := spans.At(k)
				g.observe(span, resAttrs, svc)
				if g.sg != nil {
					g.sg.consume(span, svc)
				}
			}
		}
	}
}

func (g *Generator) observe(span ptrace.Span, resAttrs pcommon.Map, svc string) {
	vals := make([]string, 0, len(g.names))
	vals = append(vals, svc, span.Name(), span.Kind().String(), span.Status().Code().String())
	for _, key := range g.extra {
		v := attrStr(span.Attributes(), key)
		if v == "" {
			v = attrStr(resAttrs, key) // fall back to the resource
		}
		vals = append(vals, v)
	}
	d := durationSeconds(span)
	sz := spanSize(span)
	idx := bucketIndex(g.bounds, d)

	g.mu.Lock()
	s := g.admit(vals)
	if s == nil {
		g.mu.Unlock()
		obs.SpanMetricsDropped.Inc()
		return
	}
	s.calls++
	s.size += sz
	s.count++
	s.sum += d
	s.buckets[idx]++
	if g.exemplars {
		if tid := span.TraceID(); !tid.IsEmpty() {
			if s.ex == nil {
				s.ex = make([]exemplar, len(g.bounds)+1)
			}
			s.ex[idx] = exemplar{set: true, value: d, ts: span.EndTimestamp(), traceID: tid, spanID: span.SpanID()}
		}
	}
	g.mu.Unlock()
}

// admit returns the series for vals, creating it while under the cardinality cap;
// nil when the cap is reached. The caller holds g.mu. vals is fresh per span, so
// it is retained as the series' dimension values without a copy.
func (g *Generator) admit(vals []string) *spanSeries {
	key := tupleKey(vals)
	if s, ok := g.series[key]; ok {
		return s
	}
	if len(g.series) >= g.maxCard {
		return nil
	}
	s := &spanSeries{dims: vals, buckets: make([]uint64, len(g.bounds)+1)}
	g.series[key] = s
	return s
}

// Run exports every interval until ctx is done, then once more.
func (g *Generator) Run(ctx context.Context, exp Exporter, interval time.Duration, res pcommon.Resource, log *slog.Logger) {
	if log == nil {
		log = slog.Default()
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			fctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			if err := g.Export(fctx, exp, res); err != nil {
				log.Warn("final span-metrics export failed", "error", err)
			}
			cancel()
			return
		case <-ticker.C:
			if err := g.Export(ctx, exp, res); err != nil {
				log.Warn("exporting span metrics failed", "error", err)
			}
		}
	}
}

// Export renders the current cumulative aggregate under res and sends it once.
func (g *Generator) Export(ctx context.Context, exp Exporter, res pcommon.Resource) error {
	md := g.render(res, time.Now())
	if md.ResourceMetrics().Len() == 0 {
		return nil
	}
	return exp.ExportMetrics(ctx, md)
}

func (g *Generator) render(res pcommon.Resource, now time.Time) pmetric.Metrics {
	empty := pmetric.NewMetrics()
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	res.CopyTo(rm.Resource())
	sm := rm.ScopeMetrics().AppendEmpty()
	sm.Scope().SetName(scopeName)
	start := pcommon.NewTimestampFromTime(g.start)
	ts := pcommon.NewTimestampFromTime(now)

	g.renderRED(sm, start, ts)
	if g.sg != nil {
		g.sg.appendMetrics(sm, start, ts, now)
	}
	if sm.Metrics().Len() == 0 {
		return empty // nothing to send this cycle
	}
	return md
}

func (g *Generator) renderRED(sm pmetric.ScopeMetrics, start, ts pcommon.Timestamp) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if len(g.series) == 0 {
		return
	}
	calls := sumMetric(sm, g.prefix+".calls", "Count of spans observed, by dimensions.", "")
	size := sumMetric(sm, g.prefix+".size", "Total size of spans observed, in bytes.", "By")
	dur := histMetric(sm, g.prefix+".duration", "Span duration in seconds, by dimensions.")
	for _, s := range g.series {
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
			if !s.ex[i].set {
				continue
			}
			e := hp.Exemplars().AppendEmpty()
			e.SetDoubleValue(s.ex[i].value)
			e.SetTimestamp(s.ex[i].ts)
			e.SetTraceID(s.ex[i].traceID)
			e.SetSpanID(s.ex[i].spanID)
			s.ex[i].set = false // exemplars are recent evidence: reset each export
		}
	}
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
	t.gen.Consume(td)
	return t.inner.ExportTraces(ctx, td)
}

// --- shared helpers ---

func attrStr(m pcommon.Map, key string) string {
	if v, ok := m.Get(key); ok {
		return v.AsString()
	}
	return ""
}

func durationSeconds(span ptrace.Span) float64 {
	end, start := span.EndTimestamp(), span.StartTimestamp()
	if end <= start {
		return 0 // unset or clock-skewed end: a negative duration is meaningless
	}
	return float64(end-start) / float64(time.Second)
}

// bucketIndex is the index of the first bound >= v, or the +Inf overflow bucket.
func bucketIndex(bounds []float64, v float64) int {
	for i, b := range bounds {
		if v <= b {
			return i
		}
	}
	return len(bounds)
}

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

// sumMetric appends a monotonic cumulative Sum metric shell and returns its data
// point slice.
func sumMetric(sm pmetric.ScopeMetrics, name, desc, unit string) pmetric.NumberDataPointSlice {
	m := sm.Metrics().AppendEmpty()
	m.SetName(name)
	m.SetDescription(desc)
	if unit != "" {
		m.SetUnit(unit)
	}
	s := m.SetEmptySum()
	s.SetIsMonotonic(true)
	s.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
	return s.DataPoints()
}

// histMetric appends a cumulative Histogram metric shell (seconds) and returns
// its data point slice.
func histMetric(sm pmetric.ScopeMetrics, name, desc string) pmetric.HistogramDataPointSlice {
	m := sm.Metrics().AppendEmpty()
	m.SetName(name)
	m.SetDescription(desc)
	m.SetUnit("s")
	h := m.SetEmptyHistogram()
	h.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
	return h.DataPoints()
}

// tupleKey is a collision-proof key for the cardinality map: values are
// length-prefixed so ("a","bc") and ("ab","c") never collide.
func tupleKey(vals []string) string {
	n := 0
	for _, v := range vals {
		n += len(v) + 4 // value + ':' + up to a few length digits
	}
	var sb strings.Builder
	sb.Grow(n) // one allocation instead of the builder's growth reallocs
	for _, v := range vals {
		sb.WriteString(strconv.Itoa(len(v)))
		sb.WriteByte(':')
		sb.WriteString(v)
	}
	return sb.String()
}
