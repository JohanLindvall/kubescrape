package servicegraph

import (
	"context"
	"log/slog"
	"slices"
	"sync"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/JohanLindvall/kubescrape/internal/agent/cumagg"
	"github.com/JohanLindvall/kubescrape/internal/obs"
)

const scopeName = "github.com/JohanLindvall/kubescrape/agent/servicegraph"

// The four metric names and the label names below are Grafana Tempo's VERBATIM
// (modules/generator/processor/servicegraphs/servicegraphs.go).
// Grafana's Service Graph view queries these exact strings; a name that reads
// better renders in nothing. Nothing here is ours to improve.
//
// # How these names survive the OTLP -> Prometheus translation
//
// What has to be exact is the series name IN PROMETHEUS, not the one on the
// wire — and between the two sits a translation whose rules add suffixes. The
// OTLP names are therefore spelled so that every mapping an operator can end up
// with produces the contract strings:
//
//   - Suffixes ON (the default of the collector's prometheus and
//     prometheusremotewrite exporters, of Prometheus' own OTLP receiver and of
//     Mimir's): the name is sanitized to [A-Za-z0-9_:], the UCUM unit is
//     appended as a Prometheus base unit, and a monotonic Sum gets `_total`.
//     Both suffix rules are IDEMPOTENT — verified against
//     github.com/prometheus/otlptranslator v1.0.0, metric_namer.go, which is the
//     shared implementation behind all of the above:
//     `addUnitTokens` drops the unit suffix when
//     `slices.Contains(nameTokens, "seconds")` (unitMap["s"] == "seconds"), and
//     `normalizeName` does `append(removeItem(nameTokens, "total"), "total")`
//     rather than appending blindly. The UTF-8 strategies take the other branch,
//     `buildMetricName`, which reaches the same place by
//     `trimSuffixAndDelimiter(name, "total")` + re-append and by an explicit
//     `!strings.HasSuffix(name, mainUnitSuffix)` guard. A name that already
//     reads `..._request_server_seconds` with unit `s`, or `..._request_total`
//     on a monotonic Sum, therefore comes out unchanged either way.
//   - Suffixes OFF (`add_metric_suffixes: false`, and the `NoTranslation`
//     strategy): the name passes through sanitization only.
//
// Both suffix rules key off the OTLP metric SHAPE, not the name, which is why
// the counters must be monotonic cumulative Sums and the histograms must carry
// unit `s`: those are the inputs the idempotence above depends on, and
// TestMetricNamesAndLabelsAreTempoVerbatim pins them for that reason. (The
// shells themselves are cumagg.SumMetric / cumagg.HistMetric, which is where
// that shape is fixed for both of the agent's aggregators; the NAMES stay
// here, because only this one answers to Grafana.)
//
// The alternative — emitting the OTel-shaped `traces.service_graph.request` and
// letting the translation append `_total`/`_seconds`, which is what the sibling
// agent/spanmetrics does — works for the first case ONLY. Under suffixes-off it
// yields `traces_service_graph_request`, which the Service Graph view matches
// nothing against, and whether that flag is set lives in the operator's
// collector config, where this package cannot see it or fail on it. Wire
// compatibility is the entire reason this package exists, so it is spelled out
// in the name rather than made contingent on a downstream setting. (This is the
// deliberate exception to the repo's OTel-dotted metric naming; see the
// spanmetrics package comment for the rule it breaks.)
//
// The unit is still SET on the two histograms even though it contributes
// nothing to the name: it is what an OTLP-native consumer displays, and per the
// contains-rule above a spec-following translator finds `seconds` among the
// tokens and appends nothing. A translator that appended unconditionally would
// produce `_seconds_seconds` — none does, and dropping the unit to guard
// against a hypothetical one would lose real information from every consumer
// that reads it.
const (
	// metricRequests counts calls on the edge (Prometheus counter; the `_total`
	// is in the name and the translation's remove-then-append leaves it alone).
	metricRequests = "traces_service_graph_request_total"
	// metricFailed counts the subset that failed on either side.
	metricFailed = "traces_service_graph_request_failed_total"
	// metricServerSeconds / metricClientSeconds are the two sides' own measured
	// latency, in seconds, as separate histograms (see Edge for why they are not
	// one).
	metricServerSeconds = "traces_service_graph_request_server_seconds"
	metricClientSeconds = "traces_service_graph_request_client_seconds"
)

// Label names. `connection_type` is emitted on every series INCLUDING the empty
// value, exactly as Tempo does — an empty label value is indistinguishable from
// an absent one once Prometheus ingests it, so the always-present spelling
// costs nothing and keeps a `connection_type=""` selector working.
//
// `virtual_node` is emitted only when a side WAS synthesized, for the same
// reason read the other way round: an empty value would not survive to
// Prometheus anyway, so the common edge does not carry the attribute at all.
const (
	labelClient         = "client"
	labelServer         = "server"
	labelConnectionType = "connection_type"
	labelVirtualNode    = "virtual_node"
)

// builtinLabels are the label names Record owns. A configured dimension that
// collides with one of them is DROPPED rather than rendered; cumagg.Builtins
// holds the rule, the bug it prevents (agent/spanmetrics shipped it through
// `dimensions: ["span.name"]`) and why this package tests each name as it
// ARRIVES while spanmetrics tests its configured list once.
var builtinLabels = cumagg.NewBuiltins(labelClient, labelServer, labelConnectionType, labelVirtualNode)

// keyScratchBytes sizes RecordAt's per-call stack buffer for the series key. A
// key that outgrows it is re-allocated on the heap on EVERY completed request,
// not once — and inside the pairing store's mutex, since RecordAt runs there —
// so it must hold the key the built-in labels alone can produce at the
// truncation limit: a client and a server name of cumagg.MaxLabelBytes each,
// each behind a two-byte length prefix, plus the longest connection type and
// virtual-node spelling. It was 256, which one long service name overran. The
// rest is headroom for short configured dimensions; a dimension carrying a
// long value can still overrun it, which only a deployment that configured
// dimensions can incur. builtinKeyMax is the floor, enforced at compile time
// below.
const (
	keyScratchBytes = 640
	builtinKeyMax   = 2*(2+cumagg.MaxLabelBytes) +
		(1 + len(ConnectionMessagingSystem)) + (1 + len(virtualNodeServer))
)

// A keyScratchBytes below builtinKeyMax fails to compile (a negative array
// length).
var _ [keyScratchBytes - builtinKeyMax]struct{}

// Exporter sends one OTLP metrics payload; satisfied by otlpexport.Client.
type Exporter = cumagg.Exporter

// Registry aggregates completed edges into the Tempo-compatible cumulative
// series above and renders them to OTLP on an interval. It implements the
// store's edge sink (`RecordAt(Edge, time.Time)`) and is safe for concurrent
// RecordAt from the ingest goroutines.
//
// It is a self-contained aggregator rather than metrics.Registry for the same
// reason agent/spanmetrics is: the shared registry has no way to express a
// per-series start timestamp, which is how a cumulative reset after a stale
// eviction is spelled, and no way to hold two histograms plus two counters
// under one label set without four independent cardinality budgets. The
// bookkeeping the two of them DO share — admission under the cardinality cap,
// the observed/rendered/delivered gate, stale eviction, the export loop — is
// agent/cumagg.
type Registry struct {
	bounds    []float64 // histogram bounds, ascending, seconds
	exemplars bool
	// exemplarKeep is SetExemplarKeep's predicate (nil keeps every exemplar).
	// Guarded by the store's mutex: Record reads it inside the hold it already
	// takes, so the setter is safe at any time, not only before the first edge.
	exemplarKeep func(pcommon.TraceID) bool
	now          func() time.Time

	store *cumagg.Store[*edgeSeries]

	// renderMu serializes renders so snaps can be REUSED across them. Exports
	// DO overlap in production — cmd/kubescrape-agent's shutdown path makes a
	// final Export beside the one Run makes on its detached context — and those
	// are serialized by cumagg.Store's exportGate, which holds for the whole
	// render+send+mark. renderMu is the scratch's own lock, and it is what
	// covers a render that does not go through Export (render, the store's
	// Render — tests call both directly): two renders sharing one scratch
	// buffer would be a data race on it. Lock order is renderMu before the
	// store's mutex; nothing ever takes renderMu while holding it.
	renderMu sync.Mutex
	// snaps is the render scratch: the series' values copied out under the
	// store lock, in chunks, so the pdata payload can be built WITHOUT it
	// (cumagg.Snapshotter). Reused because it is otherwise tens of megabytes of
	// garbage per export at the cardinality cap.
	snaps cumagg.Snapshotter[*edgeSeries, edgeSnapshot]
}

// edgeSnapshot is one series' state as of the instant the render read it. The
// label slice is ALIASED (it is built once at admission and never mutated); the
// two histograms are COPIES, because Record writes them under the mutex the
// render has just let go of.
type edgeSnapshot struct {
	labels         []edgeLabel
	requests       uint64
	failed         uint64
	start          time.Time
	client, server cumagg.HistSnap
}

// edgeSeries is one (client, server, connection_type, dimensions...) tuple's
// cumulative state.
type edgeSeries struct {
	// Meta is the shared bookkeeping: creation time (a re-created series' fresh
	// start timestamp is how OTLP spells the cumulative reset), last
	// observation, and the observed/rendered/delivered state eviction is gated
	// on.
	cumagg.Meta
	labels   []edgeLabel // rendered attribute set, built once when the series is admitted
	requests uint64
	failed   uint64
	// client and server are the two sides' own latency histograms. A side
	// stays unobserved (nil buckets, see cumagg.Hist) until it is first seen: a
	// virtual-node edge never has a server half.
	client cumagg.Hist
	server cumagg.Hist
}

// resetExemplars is the store's after-delivery hook: BOTH sides, and only once
// the payload carrying them was acked (a failed send keeps them for the retry).
func (s *edgeSeries) resetExemplars() {
	s.client.ClearExemplars()
	s.server.ClearExemplars()
}

type edgeLabel struct{ name, value string }

// clock reads the injectable now through the Registry, so a test that replaces
// r.now after NewRegistry is also replacing the clock the store exports on.
func (r *Registry) clock() time.Time { return r.now() }

// NewRegistry builds the aggregator from cfg (Tempo's defaults fill the zero
// value). log may be nil.
func NewRegistry(cfg Config, log *slog.Logger) *Registry {
	if log == nil {
		log = slog.Default()
	}
	cfg = cfg.withDefaults()
	bounds := slices.Clone(cfg.HistogramBuckets)
	slices.Sort(bounds) // Validate demands ascending; New never refuses to aggregate over it
	ex := true          // see Config.Exemplars
	if cfg.Exemplars != nil {
		ex = *cfg.Exemplars
	}
	// An invalid staleAfter falls back to the default, and says so
	// (cumagg.ResolveStaleAfter — one arm for both aggregators, which had
	// written it twice). "0" resolves to 0 here, which is the disable branch in
	// cumagg's eviction.
	stale := cumagg.ResolveStaleAfter(staleAfterField, cfg.StaleAfter, DefaultStaleAfter, log)
	r := &Registry{bounds: bounds, exemplars: ex, now: time.Now}
	nb := len(bounds) + 1
	r.snaps = cumagg.Snapshotter[*edgeSeries, edgeSnapshot]{
		CopyLocked: copyEdgeSeries,
		Fit: func(e *edgeSnapshot) {
			e.client.Fit(nb)
			e.server.Fit(nb)
		},
		Release: func(e *edgeSnapshot) { e.labels = nil },
	}
	r.store = cumagg.NewStore(cumagg.Options[*edgeSeries]{
		Scope:          scopeName,
		Name:           "service-graph metrics",
		MaxCardinality: cfg.MaxCardinality,
		StaleAfter:     stale,
		// Dropped, never silent: the cap is one of the three bounds the config
		// exposes, and an operator has to be able to see it bind — and to tell
		// it apart from the sibling aggregator's cap, which is why the counters
		// are per-aggregator rather than one shared series.
		Dropped:        obs.ServiceGraphDropped,
		Evicted:        obs.ServiceGraphEvicted,
		Now:            r.clock,
		NewSeries:      func() *edgeSeries { return &edgeSeries{} },
		Render:         r.renderEdges,
		ResetExemplars: (*edgeSeries).resetExemplars,
	})
	return r
}

// SetExemplarKeep installs the predicate asked, per completed edge, whether its
// trace will be EXPORTED; an edge it refuses records no exemplar (it is still
// counted — the graph is the whole traffic). nil removes it.
//
// An exemplar is a link to a trace, and this aggregator sits ABOVE the trace
// tier's samplers so it can count every request: without the predicate a
// traceSampling probability of p left about 1-p of both duration histograms'
// exemplars naming a trace that was never shipped. The tier wires the head
// sampler's trace-level decision (tracesample.Sampler.TraceKept) — trace-level
// because an Edge carries no span status or duration, so a trace whose error
// fragments the guard rails rescued reads as dropped: the safe direction, a
// missing link rather than a dead one. The tail sampler's verdict cannot be
// predicted here, so under tailSampling an exemplar can still name a trace it
// dropped.
//
// A setter rather than a Config field because the tier builds this Registry
// before the sampler that answers the question. It takes the series lock, so
// it is safe at any time. The predicate runs inside that lock on every
// completed request: it must be cheap and must not allocate
// (TestRecordIsAllocationFree).
func (r *Registry) SetExemplarKeep(keep func(pcommon.TraceID) bool) {
	r.store.Lock()
	r.exemplarKeep = keep
	r.store.Unlock()
}

// Record aggregates one hand-built edge at the Registry's own clock. The pairing
// store does not call it: it calls RecordAt with the clock it already holds.
func (r *Registry) Record(e Edge) { r.RecordAt(e, r.now()) }

// RecordAt aggregates one completed edge observed at now. Called from the
// pairing store, which runs on the ingest goroutines.
//
// now is the pairing pass's own clock read — the batch time Consume took, or
// the sweep's — and not a fresh one. This runs INSIDE the pairing store's mutex
// (upsert/sweep -> emit -> RecordAt), once per completed request, so a clock
// read here was a syscall-priced stall (a quarter of Record's cost, measured)
// paid while every concurrent Consume on the shard waited; and it measured
// nothing the batch time does not: both are "the shard's clock at pairing
// time", the one the exemplar timestamp below is defined as.
func (r *Registry) RecordAt(e Edge, now time.Time) {

	// e.Dimensions arrives in an order that is already a function of the SET
	// (see joinDims), so the key is built by walking it — no scratch, no sort.
	// It used to iterate a map into a stack array and sort that on EVERY
	// completed request, because a map has no order and an unsorted key mints a
	// new series per permutation; the ordering now falls out of where the pairs
	// are built.
	//
	// cumagg.Trunc is applied again HERE, on the values that go into the key,
	// even though the processor already truncates what it puts on an Edge. It is
	// a reslice, so the warm path pays nothing, and it removes a class of bug
	// rather than a cost: a promoted virtual-node edge takes its far-side name
	// from a peer attribute after truncation, and truncating the value but not
	// the key is what let spanmetrics hold two series that rendered one
	// byte-identical attribute set (a duplicate series in a single payload — a
	// conflict downstream, not extra detail). The RETAINED copies are cut by
	// cumagg.Retain instead (see edgeLabels): the key is consumed by the map
	// lookup, a label is held for the series' life.
	client := cumagg.Trunc(e.ClientService)
	server := cumagg.Trunc(e.ServerService)

	var keyScratch [keyScratchBytes]byte // see keyScratchBytes: a spill costs every call
	key := keyScratch[:0]
	key = cumagg.AppendKeyPart(key, client)
	key = cumagg.AppendKeyPart(key, server)
	key = cumagg.AppendKeyPart(key, string(e.Connection))
	key = cumagg.AppendKeyPart(key, e.VirtualNode)
	for _, d := range e.Dimensions {
		if builtinLabels.Has(d.Name) {
			continue // see builtinLabels
		}
		key = cumagg.AppendKeyPart(key, d.Name)
		key = cumagg.AppendKeyPart(key, cumagg.Trunc(d.Value))
	}

	r.store.Lock()
	s, fresh, ok := r.store.AdmitLocked(key, now)
	if !ok {
		r.store.Unlock() // over the cardinality cap; AdmitLocked counted it
		return
	}
	if fresh {
		s.labels = edgeLabels(e)
	}
	s.requests++
	if e.Failed {
		s.failed++
	}
	// A side that never arrived is not observed — its absence is the point of a
	// virtual-node edge, and a zero would read as a zero-latency call.
	//
	// The exemplar's timestamp is the SHARD's clock at pairing time, not either
	// span's end: the two spans are measured by processes whose clocks are not
	// synchronised (the very reason the two sides are separate histograms), so
	// one local reading places both sides' evidence consistently on the time
	// axis. Tempo stamps its exemplars the same way, and pairing is at most one
	// Wait behind the request.
	nb := len(r.bounds) + 1
	ts := pcommon.NewTimestampFromTime(now)
	ex := r.exemplars && (r.exemplarKeep == nil || r.exemplarKeep(e.TraceID))
	if e.HaveClient {
		i := cumagg.BucketIndex(r.bounds, e.ClientSeconds)
		s.client.Observe(i, nb, e.ClientSeconds)
		if ex {
			s.client.SetExemplar(i, nb, e.ClientSeconds, ts, e.TraceID, e.ClientSpanID)
		}
	}
	if e.HaveServer {
		i := cumagg.BucketIndex(r.bounds, e.ServerSeconds)
		s.server.Observe(i, nb, e.ServerSeconds)
		if ex {
			s.server.SetExemplar(i, nb, e.ServerSeconds, ts, e.TraceID, e.ServerSpanID)
		}
	}
	r.store.ObservedLocked(s, now)
	r.store.Unlock()
}

// edgeLabels materializes the attribute set for a newly admitted series (cold
// path). It COPIES every string out of e, which is what lets the Edge borrow
// its dimension slice from the pairing store: nothing of the Edge outlives this
// call. It cuts the values itself, from the ORIGINALS rather than from Record's
// key-side truncations — those are reslices, and re-truncating one is a no-op
// that would leave the whole sender-controlled string pinned by the series.
func edgeLabels(e Edge) []edgeLabel {
	out := make([]edgeLabel, 0, 4+len(e.Dimensions))
	out = append(out,
		edgeLabel{labelClient, cumagg.Retain(e.ClientService)},
		edgeLabel{labelServer, cumagg.Retain(e.ServerService)},
		edgeLabel{labelConnectionType, string(e.Connection)})
	if e.VirtualNode != "" {
		out = append(out, edgeLabel{labelVirtualNode, e.VirtualNode})
	}
	for _, d := range e.Dimensions {
		if builtinLabels.Has(d.Name) {
			continue // see builtinLabels
		}
		out = append(out, edgeLabel{d.Name, cumagg.Retain(d.Value)})
	}
	return out
}

// Run exports every interval until ctx is done, then once more on a detached
// context (cumagg.Store.Run).
func (r *Registry) Run(ctx context.Context, exp Exporter, interval time.Duration, res pcommon.Resource, log *slog.Logger) {
	r.store.Run(ctx, exp, interval, res, log)
}

// Export renders the current cumulative aggregate under res and sends it once.
func (r *Registry) Export(ctx context.Context, exp Exporter, res pcommon.Resource) error {
	return r.store.Export(ctx, exp, res)
}

// copyEdgeSeries is the snapshot's per-series copy (cumagg.Snapshotter's
// CopyLocked): under the store lock, the series already marked rendered.
func copyEdgeSeries(e *edgeSnapshot, s *edgeSeries) {
	e.labels = s.labels
	e.requests, e.failed, e.start = s.requests, s.failed, s.Start
	e.client.CopyFrom(&s.client)
	e.server.CopyFrom(&s.server)
}

// renderEdges is the store's Render callback: it writes the four Tempo metrics
// for every live series. It takes renderMu (never held while the store's mutex
// is) because the snapshot scratch is reused across renders.
//
// Only the snapshot (cumagg.Snapshotter.Take) holds the store's mutex, in
// chunks; the build below runs without it, and that is a receive-path matter,
// not just a slow export: RecordAt is called by the pairing store from INSIDE
// its own mutex (store.upsert -> emit -> sink.RecordAt), so every millisecond
// the series lock is held is a millisecond in which no shard goroutine can
// Consume a span. At the cardinality cap the payload build is tens of
// milliseconds, once per export interval, and the whole of it used to land on
// the ingest path: a 46.7 ms Record stall inside a 46.7 ms render, now 1.6 ms
// (TestRenderDoesNotStallRecord).
func (r *Registry) renderEdges(sm pmetric.ScopeMetrics, now time.Time) {
	r.renderMu.Lock()
	defer r.renderMu.Unlock()

	snap := r.snaps.Take(r.store, now)
	if len(snap) == 0 {
		return
	}

	// Which histograms exist is decided BEFORE any point is appended, so the
	// payload's metric order is fixed rather than a function of which series the
	// map happened to yield first.
	var anyClient, anyServer bool
	for i := range snap {
		anyClient = anyClient || snap[i].client.Present
		anyServer = anyServer || snap[i].server.Present
		if anyClient && anyServer {
			break
		}
	}

	ts := pcommon.NewTimestampFromTime(now)
	// No unit on the counters: these are counts, and the `_total` the Prometheus
	// mapping wants is already in the name (see the naming comment above).
	requests := cumagg.SumMetric(sm, metricRequests, "Total count of requests between two nodes.", "")
	failed := cumagg.SumMetric(sm, metricFailed, "Total count of failed requests between two nodes.", "")
	var server, client pmetric.HistogramDataPointSlice
	if anyServer {
		server = cumagg.HistMetric(sm, metricServerSeconds, "Time for a request between two nodes as seen from the server.")
	}
	if anyClient {
		client = cumagg.HistMetric(sm, metricClientSeconds, "Time for a request between two nodes as seen from the client.")
	}

	for i := range snap {
		s := &snap[i]
		// Clamped: a series admitted between Export's clock read and the
		// snapshot's first lock hold carries a Start past ts, and stamping it
		// unclamped is an inverted cumulative interval on the series' first
		// export (cumagg.ClampStart has the full race).
		start := cumagg.ClampStart(pcommon.NewTimestampFromTime(s.start), ts)

		rp := requests.AppendEmpty()
		putLabels(rp.Attributes(), s.labels)
		rp.SetStartTimestamp(start)
		rp.SetTimestamp(ts)
		rp.SetIntValue(int64(s.requests))

		// The failed counter is emitted for EVERY edge, at zero when nothing has
		// failed — Tempo creates its child series only on the first failure, so
		// the error-rate ratio (failed / total) is undefined for exactly the
		// edges that are healthy. A present zero costs one data point and makes
		// the ratio total.
		fp := failed.AppendEmpty()
		putLabels(fp.Attributes(), s.labels)
		fp.SetStartTimestamp(start)
		fp.SetTimestamp(ts)
		fp.SetIntValue(int64(s.failed))

		if s.server.Present {
			putHist(server.AppendEmpty(), s.labels, &s.server, r.bounds, start, ts)
		}
		if s.client.Present {
			putHist(client.AppendEmpty(), s.labels, &s.client, r.bounds, start, ts)
		}
	}
}

func putLabels(a pcommon.Map, labels []edgeLabel) {
	a.EnsureCapacity(len(labels))
	for _, l := range labels {
		// Dimension names arrive as the store spelled them; a dotted OTel key
		// (client_http.method) sanitizes to the Prometheus label Tempo emits
		// (client_http_method) in the same translation that fixes the metric
		// names, so it is not re-spelled here.
		a.PutStr(l.name, l.value)
	}
}

// putHist writes one side's histogram point. Its exemplars are one per occupied
// bucket, in bucket order — the snapshot already dropped the unset slots — and
// their id is THIS side's own span (see Edge.ClientSpanID), so the evidence
// attached to a latency explains the latency it is attached to rather than the
// other half of the request.
func putHist(p pmetric.HistogramDataPoint, labels []edgeLabel, h *cumagg.HistSnap, bounds []float64, start, ts pcommon.Timestamp) {
	putLabels(p.Attributes(), labels)
	cumagg.PutHistPoint(p, h, bounds, start, ts)
}
