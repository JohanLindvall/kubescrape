// Package spanmetrics derives RED (Request/Error/Duration) metrics from ingested
// OTLP trace spans, following the OpenTelemetry spanmetrics conventions: a
// monotonic `calls` counter, a `size` counter (span bytes), and a `duration`
// histogram (seconds, with trace-id exemplars), dimensioned by service.name /
// span.name / span.kind / status.code plus configurable extra attributes.
//
// It runs as a TracesExporter tap in the -service-graph trace tier's owner
// chain (cmd/kubescrape-agent's buildOwnerChain: below the pairing tap, above
// the samplers) — spans are aggregated as a side effect and still forwarded —
// and the metrics are exported over OTLP on an interval like every other agent
// metric. The tap sits on the shard that OWNS a span's trace, after the
// internal re-shard hop, so each span is counted ONCE however many hops it
// took; and each span is aggregated independently, so the cumulative counters
// sum across SHARDS. Being above the samplers it sees 100% of spans: the RED
// metrics describe the traffic, not the sampled subset. (Service-graph edge
// metrics are agent/servicegraph's, in the same owner chain: an edge needs a
// request's client and server spans in ONE process, which is what routing by
// trace id buys and what a per-span aggregate does not need.)
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
	"errors"
	"log/slog"
	"slices"
	"sort"
	"sync"
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
//
// It holds one name the aggregator does not write itself: `le`, the bucket
// label the OTLP -> Prometheus mapping generates for the duration histogram. A
// dimension of that name lands on the histogram's points and collides with it
// downstream — the same refusal internal/metrics makes for a log-derived
// histogram's labels.
var builtins = cumagg.NewBuiltins(append(slices.Clone(builtinDims), "le")...)

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

// keyScratchBytes sizes observe's per-call stack buffer for the series key. A
// key that outgrows it is re-allocated on the heap for EVERY span, not once, so
// it must hold the key the built-ins alone can produce at the truncation limit:
// a service name and a span name of cumagg.MaxLabelBytes each (some database
// instrumentations name a span after its statement text, so a span name AT the
// cut is ordinary), each behind a two-byte length prefix, plus the two enum
// spellings. It was 256, which one such name overran. The rest is headroom for
// short configured dimensions. A configured dimension carrying a long value
// can still overrun it — a per-span allocation that only a deployment which
// configured dimensions can incur, where the built-ins alone never do.
// builtinKeyMax is the floor, enforced at compile time below.
const (
	keyScratchBytes = 640
	builtinKeyMax   = 2*(2+cumagg.MaxLabelBytes) +
		(1 + len("SPAN_KIND_UNSPECIFIED")) + (1 + len("STATUS_CODE_UNSET"))
)

// A keyScratchBytes below builtinKeyMax fails to compile (a negative array
// length).
var _ [keyScratchBytes - builtinKeyMax]struct{}

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
	// MaxCardinality caps the number of distinct dimension tuples (default
	// 20000, 0 = default). A span whose tuple is NEW over the cap is not
	// aggregated, and is counted (kubescrape_span_metrics_dropped_total); it
	// is still forwarded, and existing tuples keep reporting, because these
	// are cumulative series.
	MaxCardinality int `json:"maxCardinality,omitempty"`
	// Exemplars attaches a trace/span-id exemplar (one per latency bucket, the
	// latest wins, reset after each DELIVERED export — a failed send keeps them
	// for the retry) to the duration histogram. nil defaults to true. Only a
	// span ExemplarKeep admits anchors one.
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

	// ExemplarKeep, when set, is asked whether a span will be EXPORTED before
	// it may become an exemplar, and a span it refuses records none (it is
	// still counted: the RED metrics are the whole traffic). An exemplar is a
	// link — a trace id a UI looks up — and this generator sits ABOVE the trace
	// tier's samplers precisely so it sees 100% of spans, so without the
	// predicate a traceSampling probability of p left about 1-p of the
	// duration histogram's exemplars naming a trace that was never shipped: a
	// dead link on the panel an operator clicks through from.
	//
	// The tier wires the head sampler's own per-span decision
	// (tracesample.Sampler.SpanKept: the probability plus the keepErrors /
	// keepSlowerThan guard rails, so an error or slow fragment that ships still
	// anchors one; the rate cap is not predictable per span and is left out).
	// The TAIL sampler's verdict cannot be predicted at all — it is taken
	// seconds later over the whole trace — so under tailSampling an exemplar
	// can still name a trace it dropped.
	//
	// It runs once per span, before the series lock is taken; it must be safe
	// for concurrent use and must not allocate (TestConsumeAllocationBudget).
	// json:"-" — wiring, not config, like Logger below.
	ExemplarKeep func(ptrace.Span) bool `json:"-"`

	// Logger reports what New decides for itself: a fallback taken (and, at
	// Debug, a dimension dropped — the Warn for that is DimensionWarnings').
	// json:"-" — it is wiring, not config (the same shape as
	// tailsample.Config.Script), and nil means slog.Default(), which both
	// binaries install as the logfmt handler before anything runs.
	Logger *slog.Logger `json:"-"`
}

// staleAfterField is StaleAfter's config path, for the refusal and the fallback
// warning alike.
const staleAfterField = "traceMetrics.staleAfter"

// staleAfter parses StaleAfter (empty = the default, "0" disables eviction, a
// negative value is an error).
func (c Config) staleAfter() (time.Duration, error) {
	return cumagg.ParseStaleAfter(staleAfterField, c.StaleAfter, defaultStaleAfter)
}

// DimensionWarnings is one sentence per configured dimension New will drop
// (empty, a built-in, a repeat), and nothing for a clean list. Pure, so
// cmd/kubescrape-agent's configWarnings can say it from -check-config and a
// real start alike; New itself only logs the drops at Debug, or a start would
// print each line twice.
func (c Config) DimensionWarnings() []string {
	return builtins.DimensionWarnings(dimensionsField, c.Dimensions)
}

// dimensionsField is Dimensions' config path, for the warning and New's trace.
const dimensionsField = "traceMetrics.dimensions"

// Validate reports a malformed config so a bad value can fail startup with a
// clear message (New itself falls back to the default, never refusing to
// aggregate).
func (c Config) Validate() error {
	if _, err := c.staleAfter(); err != nil {
		return err
	}
	if c.MaxCardinality < 0 {
		return errors.New("maxCardinality must not be negative")
	}
	// Shared with servicegraph's identical histogramBuckets check —
	// cumagg.ValidateBuckets carries the why (non-increasing ExplicitBounds
	// violate the OTLP spec, and boundsOrDefault sorts but never
	// de-duplicates).
	return cumagg.ValidateBuckets("buckets", c.Buckets)
}

// Generator aggregates spans into calls/size/duration metrics. Safe for
// concurrent Consume from the ingest goroutines.
type Generator struct {
	prefix    string
	names     []string // full dimension label names (built-ins + extras), in order
	extra     []string
	bounds    []float64 // histogram bucket bounds, ascending, seconds
	exemplars bool
	// exemplarKeep is Config.ExemplarKeep: nil keeps every exemplar.
	exemplarKeep func(ptrace.Span) bool
	now          func() time.Time

	store *cumagg.Store[*spanSeries]

	// renderMu serializes renders so the snapshot scratch can be REUSED across
	// them. Lock order is renderMu BEFORE the store's mutex; nothing ever takes
	// renderMu while holding the store lock.
	renderMu sync.Mutex
	// snaps is the render scratch: the series' values copied out under the
	// store lock in chunks, then read lock-free (cumagg.Snapshotter).
	snaps cumagg.Snapshotter[*spanSeries, spanSnapshot]
}

// spanSnapshot is one series' state as of the instant the render read it. dims
// is ALIASED (built once at admission, never mutated); the histogram is a COPY,
// because Consume writes it under the mutex the build runs without.
type spanSnapshot struct {
	dims  []string
	calls uint64
	size  int64
	start time.Time
	dur   cumagg.HistSnap
}

type spanSeries struct {
	// Meta is the shared bookkeeping: creation time (the cumulative start
	// timestamp), last observation and the observed/rendered/delivered state
	// eviction is gated on.
	cumagg.Meta
	dims  []string // dimension values, aligned with Generator.names
	calls uint64
	size  int64
	dur   cumagg.Hist // the duration histogram, len(bounds)+1 buckets
}

// resetExemplars is the store's after-delivery hook (a failed send keeps them).
func (s *spanSeries) resetExemplars() { s.dur.ClearExemplars() }

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
	log := cfg.Logger
	if log == nil {
		log = slog.Default()
	}
	// Drop configured dimensions that are empty, repeat a built-in or repeat
	// each other. This is the ONE place this generator's label names are
	// decided, which is why the guard runs here; cumagg.Builtins holds the rule,
	// the bug it prevents, and why a drop is only a Debug line here (the
	// operator's WARNING is Config.DimensionWarnings', emitted by configWarnings
	// on -check-config and every start alike).
	names := append([]string(nil), builtinDims...)
	names = append(names, builtins.Configure(dimensionsField, cfg.Dimensions, log)...)
	// A bad value falls back to the default, and says so (cumagg.ResolveStaleAfter).
	stale := cumagg.ResolveStaleAfter(staleAfterField, cfg.StaleAfter, defaultStaleAfter, log)
	g := &Generator{
		prefix:       prefix,
		names:        names,
		extra:        names[len(builtinDims):], // the configured dimensions, aliased (never diverges from names)
		bounds:       boundsOrDefault(cfg.Buckets),
		exemplars:    ex,
		exemplarKeep: cfg.ExemplarKeep,
		now:          time.Now,
	}
	nb := len(g.bounds) + 1
	g.snaps = cumagg.Snapshotter[*spanSeries, spanSnapshot]{
		CopyLocked: copySpanSeries,
		Fit:        func(e *spanSnapshot) { e.dur.Fit(nb) },
		Release:    func(e *spanSnapshot) { e.dims = nil },
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
//
// The series mutex is taken PER SPAN, and a chunked hold (fold 64 spans per
// acquisition) was tried and backed out: the uncontended lock/unlock pair is
// about a seventh of what folding a span costs, but neither the serial nor the
// parallel benchmark could resolve a difference (interleaved n=8 and n=12, every
// row "~", geomean -0.1% and -2.0% against a ±30-46% spread). A longer hold also
// works against the reason the render is chunked — the batch size is the
// SENDER's choice — so it bought contested noise at the price of a real
// invariant.
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
	// AdmitLocked allocates nothing for a warm series). A key longer than
	// keyScratchBytes spills to the heap on EVERY call, not once — the scratch
	// is per call — so the size is chosen to hold the built-ins at their
	// truncation limit; see keyScratchBytes.
	//
	// Every part is cut at exactly the length dims() cuts the values it RENDERS
	// to (cumagg.Retain, which differs only in cloning what it keeps); see
	// cumagg.Trunc for why keying on the untruncated value is a duplicate
	// series. Kind and status are closed enum spellings, truncated nowhere.
	var keyScratch [keyScratchBytes]byte
	key := keyScratch[:0]
	key = cumagg.AppendKeyPart(key, cumagg.Trunc(svc))
	key = cumagg.AppendKeyPart(key, cumagg.Trunc(span.Name()))
	key = cumagg.AppendKeyPart(key, kindStr(span.Kind()))
	key = cumagg.AppendKeyPart(key, statusStr(span.Status().Code()))
	for _, k := range g.extra {
		// Appended from the attribute VALUE, not from its rendered string: an
		// Int dimension (http.response.status_code) is formatted straight into
		// the stack key, where rendering it to a string first cost an allocation
		// per span. cumagg.AppendValueKeyPart is byte-identical to appending the
		// rendered string (cumagg.DimStr, which dims() writes), and both resolve
		// through cumagg.DimValue, so the key still agrees with the label.
		if v, ok := cumagg.DimValue(span.Attributes(), resAttrs, k); ok {
			key = cumagg.AppendValueKeyPart(key, v)
		} else {
			key = cumagg.AppendKeyPart(key, "")
		}
	}
	d := cumagg.SpanSeconds(span)
	sz := spanSize(span)
	idx := cumagg.BucketIndex(g.bounds, d)
	// Decided before the lock: the predicate reads only the span.
	ex := g.exemplars && (g.exemplarKeep == nil || g.exemplarKeep(span))

	g.store.Lock()
	s, fresh, ok := g.store.AdmitLocked(key, now)
	if !ok {
		g.store.Unlock() // over the cardinality cap; AdmitLocked counted it
		return
	}
	if fresh {
		s.dims = g.dims(span, resAttrs, svc)
	}
	s.calls++
	s.size += sz
	// The first observation allocates the buckets, which is this admission
	// path, never the warm one.
	nb := len(g.bounds) + 1
	s.dur.Observe(idx, nb, d)
	if ex {
		s.dur.SetExemplar(idx, nb, d, span.EndTimestamp(), span.TraceID(), span.SpanID())
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
		kindStr(span.Kind()), statusStr(span.Status().Code()))
	for _, k := range g.extra {
		vals = append(vals, cumagg.Retain(cumagg.DimStr(span.Attributes(), resAttrs, k)))
	}
	return vals
}

// kindNames and statusNames are the span.kind and status.code label VALUES: the
// OTLP proto enum spellings, exactly as the OpenTelemetry Collector's
// spanmetrics connector writes them (traceutil.SpanKindStr / StatusCodeStr) and
// as Grafana Tempo's metrics-generator does. pdata's own String() methods spell
// them "Server" and "Error", which is what this package used to render — and a
// Jaeger SPM view, a connector-shaped dashboard or an alert copied from either
// then matched nothing, on the one pair of labels those consumers all select
// by. Indexed by the enum's value, so the spelling costs a bounds check and a
// load on the per-span path, and a value outside the enum renders "" (the
// connector's answer too) rather than a number nobody queries for.
var (
	kindNames = [...]string{
		ptrace.SpanKindUnspecified: "SPAN_KIND_UNSPECIFIED",
		ptrace.SpanKindInternal:    "SPAN_KIND_INTERNAL",
		ptrace.SpanKindServer:      "SPAN_KIND_SERVER",
		ptrace.SpanKindClient:      "SPAN_KIND_CLIENT",
		ptrace.SpanKindProducer:    "SPAN_KIND_PRODUCER",
		ptrace.SpanKindConsumer:    "SPAN_KIND_CONSUMER",
	}
	statusNames = [...]string{
		ptrace.StatusCodeUnset: "STATUS_CODE_UNSET",
		ptrace.StatusCodeOk:    "STATUS_CODE_OK",
		ptrace.StatusCodeError: "STATUS_CODE_ERROR",
	}
)

// kindStr is the span.kind label value; see kindNames. observe's key and dims'
// label read it through this one function, so the two cannot disagree.
func kindStr(k ptrace.SpanKind) string {
	if uint32(k) < uint32(len(kindNames)) {
		return kindNames[k]
	}
	return ""
}

// statusStr is the status.code label value; see kindNames.
func statusStr(c ptrace.StatusCode) string {
	if uint32(c) < uint32(len(statusNames)) {
		return statusNames[c]
	}
	return ""
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
// It snapshots the series' values under the store lock in CHUNKS and builds the
// pdata payload LOCK-FREE, exactly as agent/servicegraph does — because the
// span-metrics tap calls Consume with the ingest RPC waiting on it (Tap forwards
// then Consumes, and Consume takes this same lock per span), so holding the lock
// across the whole cardinality-cap build stalls every trace-push on the tier
// for the render's duration (measured ~89 ms at the 20k cap). Chunking bounds
// one stall to one chunk-sized copy rather than the whole build.
// TestRenderDoesNotStallConsume holds that end to end — at the 20k cap, a
// concurrent Consume must not block for more than half the render (with a 2 ms
// floor for a GC pause) — and the chunking itself is cumagg.Snapshotter's,
// pinned there.
func (g *Generator) renderRED(sm pmetric.ScopeMetrics, now time.Time) {
	g.renderMu.Lock()
	defer g.renderMu.Unlock()

	snap := g.snaps.Take(g.store, now)
	if len(snap) == 0 {
		return
	}

	ts := pcommon.NewTimestampFromTime(now)
	calls := cumagg.SumMetric(sm, g.prefix+".calls", "Count of spans observed, by dimensions.", "")
	size := cumagg.SumMetric(sm, g.prefix+".size", "Total size of spans observed, in bytes.", "By")
	dur := cumagg.HistMetric(sm, g.prefix+".duration", "Span duration in seconds, by dimensions.")
	for i := range snap {
		s := &snap[i]
		// Clamped: a series admitted between Export's clock read and the
		// snapshot's lock hold carries a Start past ts, and stamping it
		// unclamped is an inverted cumulative interval on its first export
		// (cumagg.ClampStart has the full race).
		start := cumagg.ClampStart(pcommon.NewTimestampFromTime(s.start), ts)
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
		cumagg.PutHistPoint(hp, &s.dur, g.bounds, start, ts)
	}
}

// copySpanSeries is the snapshot's per-series copy (cumagg.Snapshotter's
// CopyLocked): under the store lock, the series already marked rendered.
func copySpanSeries(e *spanSnapshot, s *spanSeries) {
	e.dims = s.dims // aliased: built once at admission, never mutated
	e.calls, e.size, e.start = s.calls, s.size, s.Start
	e.dur.CopyFrom(&s.dur)
}

// Tap returns a TracesExporter that forwards each batch to inner FIRST and runs
// it through Consume only once that succeeded (see tap.ExportTraces for why the
// order matters). The generator observes ENRICHED spans because the tier's
// entry shard enriches in place before the batch reaches the owner chain.
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
//
// A slice or a map is sized by its CONTENTS. It used to be charged a flat 8
// bytes like a scalar, which undercounted exactly the largest attributes a span
// carries — semconv's captured headers (http.request.header.<name>) are
// string[] — so a 1 KiB header value added 8 bytes to a counter that says it
// totals span bytes. The recursion is bounded: the depth of a pushed value is
// capped at the receive seam (otlpingest's nesting guard, on the application
// ports and the internal hop alike), and it allocates nothing.
func valueSize(v pcommon.Value) int {
	switch v.Type() {
	case pcommon.ValueTypeStr:
		return len(v.Str())
	case pcommon.ValueTypeBytes:
		return v.Bytes().Len()
	case pcommon.ValueTypeBool:
		return 1
	case pcommon.ValueTypeSlice:
		s := v.Slice()
		n := 0
		for i := 0; i < s.Len(); i++ {
			n += valueSize(s.At(i))
		}
		return n
	case pcommon.ValueTypeMap:
		return int(attrsSize(v.Map()))
	default: // int, double, empty
		return 8
	}
}

func putDims(a pcommon.Map, names, dims []string) {
	// Pre-sized: appending grows the map 1 -> 2 -> 4 -> ..., and this runs three
	// times per series per export — measured at 120,000 allocations and 7.7 MB
	// of garbage per export at the 20,000-series default without it.
	// servicegraph's putLabels does the same.
	a.EnsureCapacity(min(len(names), len(dims)))
	for i, name := range names {
		if i < len(dims) {
			a.PutStr(name, dims[i])
		}
	}
}
