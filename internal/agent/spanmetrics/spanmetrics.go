// Package spanmetrics derives RED (Request/Error/Duration) metrics from ingested
// OTLP trace spans, following the OpenTelemetry spanmetrics conventions: a
// monotonic `calls` counter and a `duration` histogram, dimensioned by
// service.name / span.name / span.kind / status.code (plus configurable extra
// attributes). It plugs into the agent's OTLP-ingest traces path as a
// TracesExporter tap — spans are aggregated as a side effect and still
// forwarded — and the aggregated metrics are exported over OTLP on an interval
// like every other agent metric.
//
// Aggregation reuses metrics.Registry (cumulative counters, histogram bucketing,
// OTLP rendering); this package adds the span→dimension extraction and a
// cardinality cap so data-driven label sets cannot grow memory without bound.
package spanmetrics

import (
	"context"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/JohanLindvall/kubescrape/internal/metrics"
	"github.com/JohanLindvall/kubescrape/internal/obs"
)

// Built-in dimension label names (kept as OTel-style dotted keys; the exporter
// renders them to Prometheus as service_name, span_name, … via target_info).
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

// Config tunes the generator. The zero value is valid and uses the defaults.
type Config struct {
	// NamePrefix prefixes the two metric names (default "traces.span.metrics",
	// giving traces.span.metrics.calls and traces.span.metrics.duration).
	NamePrefix string `json:"namePrefix,omitempty"`
	// Buckets are the duration histogram boundaries in SECONDS (default: the
	// spanmetrics latency buckets).
	Buckets []float64 `json:"buckets,omitempty"`
	// Dimensions are extra span (falling back to resource) attribute keys to add
	// as labels, beyond the four built-ins. A missing attribute yields "".
	Dimensions []string `json:"dimensions,omitempty"`
	// MaxCardinality caps the number of distinct dimension tuples; spans whose
	// tuple would exceed it are dropped and counted (default 20000, 0 = default).
	MaxCardinality int `json:"maxCardinality,omitempty"`
}

// Generator aggregates spans into calls/duration metrics.
type Generator struct {
	reg      *metrics.Registry
	calls    *metrics.RegCounterVec
	duration *metrics.RegHistogramVec
	extra    []string // configured extra dimension keys, in label order
	maxCard  int

	mu   sync.Mutex
	seen map[string]struct{} // distinct dimension tuples admitted (bounds cardinality)
}

// New builds a generator from cfg.
func New(cfg Config) *Generator {
	prefix := cfg.NamePrefix
	if prefix == "" {
		prefix = defaultNamePrefix
	}
	buckets := cfg.Buckets
	if len(buckets) == 0 {
		buckets = defaultBuckets
	}
	maxCard := cfg.MaxCardinality
	if maxCard <= 0 {
		maxCard = defaultMaxCardinality
	}
	// Label order: the four built-ins, then the configured extras.
	names := append([]string{dimService, dimSpan, dimKind, dimStatus}, cfg.Dimensions...)
	reg := metrics.NewRegistry()
	return &Generator{
		reg:      reg,
		calls:    reg.CounterVec(prefix+".calls", "Count of spans observed, by dimensions.", names...),
		duration: reg.HistogramVec(prefix+".duration", "Span duration in seconds, by dimensions.", buckets, names...),
		extra:    append([]string(nil), cfg.Dimensions...),
		maxCard:  maxCard,
		seen:     make(map[string]struct{}),
	}
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
				g.observe(spans.At(k), resAttrs, svc)
			}
		}
	}
}

func (g *Generator) observe(span ptrace.Span, resAttrs pcommon.Map, svc string) {
	vals := make([]string, 0, 4+len(g.extra))
	vals = append(vals, svc, span.Name(), span.Kind().String(), span.Status().Code().String())
	for _, key := range g.extra {
		v := attrStr(span.Attributes(), key)
		if v == "" {
			v = attrStr(resAttrs, key) // fall back to the resource
		}
		vals = append(vals, v)
	}
	if !g.admit(vals) {
		obs.SpanMetricsDropped.Inc()
		return
	}
	g.calls.WithLabelValues(vals...).Inc()
	g.duration.WithLabelValues(vals...).Observe(durationSeconds(span))
}

// admit reports whether a tuple may be recorded, enforcing the cardinality cap.
// An already-seen tuple always passes; a new one passes only while under the cap.
func (g *Generator) admit(vals []string) bool {
	key := tupleKey(vals)
	g.mu.Lock()
	defer g.mu.Unlock()
	if _, ok := g.seen[key]; ok {
		return true
	}
	if len(g.seen) >= g.maxCard {
		return false
	}
	g.seen[key] = struct{}{}
	return true
}

// Run exports the aggregated metrics under res every interval until ctx is done,
// then once more. Mirrors metrics.Registry.Run / the agent's self-metrics.
func (g *Generator) Run(ctx context.Context, exp metrics.Exporter, interval time.Duration, res pcommon.Resource, log *slog.Logger) {
	g.reg.Run(ctx, exp, interval, res, log)
}

// Export renders the current cumulative aggregate under res and sends it once.
func (g *Generator) Export(ctx context.Context, exp metrics.Exporter, res pcommon.Resource) error {
	return g.reg.Export(ctx, exp, res)
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

// tupleKey is a collision-proof key for the cardinality set: values are
// length-prefixed so ("a","bc") and ("ab","c") never collide.
func tupleKey(vals []string) string {
	var sb strings.Builder
	for _, v := range vals {
		sb.WriteString(strconv.Itoa(len(v)))
		sb.WriteByte(':')
		sb.WriteString(v)
	}
	return sb.String()
}
