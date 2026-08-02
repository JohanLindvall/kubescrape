package servicegraph

import (
	"context"
	"log/slog"
	"slices"
	"strconv"
	"sync"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"

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
// TestMetricNamesAndLabelsAreTempoVerbatim pins them for that reason.
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
// collides with one of them is DROPPED rather than rendered: the attributes go
// into one pcommon.Map, so a colliding key would overwrite the real client or
// server and two different edges would render byte-identical label sets while
// the series key still held them apart — a duplicate series in one payload,
// which is a conflict downstream, not extra detail. agent/spanmetrics shipped
// exactly that bug through its `dimensions: ["span.name"]` path; this is the
// same guard, applied where the labels are built rather than where they are
// configured (the dimension names arrive on the Edge, not from cfg).
var builtinLabels = map[string]bool{
	labelClient:         true,
	labelServer:         true,
	labelConnectionType: true,
	labelVirtualNode:    true,
}

// Exporter sends one OTLP metrics payload; satisfied by otlpexport.Client. The
// signature mirrors agent/spanmetrics so both generators wire up identically.
type Exporter interface {
	ExportMetrics(ctx context.Context, md pmetric.Metrics) error
}

// Registry aggregates completed edges into the Tempo-compatible cumulative
// series above and renders them to OTLP on an interval. It implements the
// store's edge sink (`Record(Edge)`) and is safe for concurrent Record from the
// ingest goroutines.
//
// It is a self-contained aggregator rather than metrics.Registry for the same
// reason agent/spanmetrics is: the shared registry has no way to express a
// per-series start timestamp, which is how a cumulative reset after a stale
// eviction is spelled, and no way to hold two histograms plus two counters
// under one label set without four independent cardinality budgets.
type Registry struct {
	bounds     []float64 // histogram bounds, ascending, seconds
	maxCard    int
	staleAfter time.Duration // 0 disables eviction
	exemplars  bool
	now        func() time.Time

	mu     sync.Mutex
	series map[string]*edgeSeries

	// renderMu serializes renders so snap can be REUSED across them. Renders
	// come from one goroutine in production (Run's loop, then its final export),
	// but Export is exported and tests call it directly, and two renders sharing
	// one scratch buffer would be a data race on it. Lock order is renderMu
	// before mu; nothing ever takes renderMu while holding mu.
	renderMu sync.Mutex
	// snap is the render scratch: the series' values copied out under mu so the
	// pdata payload can be built WITHOUT it. Reused because it is otherwise tens
	// of megabytes of garbage per export at the cardinality cap. snapPtrs is the
	// series order that copy walks, taken in one cheap pass so the copy itself
	// can release and retake the mutex between chunks.
	snap     []edgeSnapshot
	snapPtrs []*edgeSeries
}

// snapChunk is how many series one snapshot lock-hold copies. It trades the
// number of acquisitions (cheap, uncontended most of the time) against the
// length of one stall on the receive path, which is what actually hurts: at the
// 20k cardinality cap the whole copy is ~5 ms, and this makes the longest hold
// about a fortieth of that.
const snapChunk = 512

// edgeSnapshot is one series' state as of the instant the render read it. The
// label slice is ALIASED (it is built once at admission and never mutated); the
// bucket and exemplar slices are COPIES, because Record writes them under the
// mutex the render has just let go of.
type edgeSnapshot struct {
	labels         []edgeLabel
	requests       uint64
	failed         uint64
	start          time.Time
	client, server histSnapshot
}

type histSnapshot struct {
	present bool // the side was observed at all (buckets != nil)
	count   uint64
	sum     float64
	buckets []uint64
	// ex holds only the exemplars that are SET, in bucket order — which is
	// exactly what putHist renders, and typically one or two per side per
	// interval rather than one slot per bucket. Copying the full per-bucket
	// array instead put ~34 MB of memcpy under the series mutex at the
	// cardinality cap, which is the stall this snapshot exists to remove.
	ex []exemplar
}

// copyFrom copies one side's aggregate, reusing this snapshot's slices. The
// caller holds r.mu: everything read here is written by Record under it.
func (h *histSnapshot) copyFrom(src *histAgg) {
	h.present = src.buckets != nil
	h.count, h.sum = src.count, src.sum
	h.buckets = append(h.buckets[:0], src.buckets...)
	h.ex = h.ex[:0]
	for i := range src.ex {
		if src.ex[i].set {
			h.ex = append(h.ex, src.ex[i])
		}
	}
}

// edgeSeries is one (client, server, connection_type, dimensions...) tuple's
// cumulative state.
type edgeSeries struct {
	labels   []edgeLabel // rendered attribute set, built once when the series is admitted
	requests uint64
	failed   uint64
	client   histAgg
	server   histAgg
	// start is when this series was created. A series re-created after an
	// eviction restarts its counters from zero, and a FRESH start timestamp is
	// how OTLP spells that reset — reusing the old one reads downstream as a
	// counter jumping backwards.
	start time.Time
	// lastSeen is the last observation; state says whether the CURRENT values
	// reached the collector. Eviction needs both: dropping a series whose last
	// observations no DELIVERED export carried would destroy them unseen (the
	// export interval may legally exceed staleAfter, and an export can fail).
	lastSeen time.Time
	state    seriesState
}

type edgeLabel struct{ name, value string }

// histAgg is one side's latency histogram. buckets stays nil until that side is
// first observed: a virtual-node edge never has a server half, and rendering a
// zero-count point for it would put a latency series on the graph for a
// measurement nobody took.
type histAgg struct {
	count   uint64
	sum     float64
	buckets []uint64 // len(bounds)+1 once observed
	// ex holds at most one exemplar per bucket — the latest observed since the
	// last DELIVERED export — and stays nil until one is recorded (exemplars
	// off, or an edge whose spans carried no trace id).
	ex []exemplar
}

// exemplar is the evidence kept for one latency bucket: a request that landed
// in it, so the histogram links to the trace that explains it.
type exemplar struct {
	set     bool
	value   float64
	ts      pcommon.Timestamp
	traceID pcommon.TraceID
	spanID  pcommon.SpanID
}

// observe folds one measurement in and returns its bucket, so the caller can
// attach an exemplar to that same bucket without walking the bounds twice.
func (h *histAgg) observe(bounds []float64, v float64) int {
	if h.buckets == nil {
		h.buckets = make([]uint64, len(bounds)+1)
	}
	h.count++
	h.sum += v
	idx := bucketIndex(bounds, v)
	h.buckets[idx]++
	return idx
}

// setExemplar keeps this sample as the bucket's exemplar, replacing whatever
// was there: the newest is the one an operator looking at a live graph wants,
// and one per bucket bounds the cost at len(bounds)+1 per side per edge.
//
// A span with no trace id is skipped rather than emitted with a zero one: an
// exemplar whose trace cannot be looked up is a dead link in the UI, and the
// spanmetrics generator skips the same case for the same reason.
func (h *histAgg) setExemplar(idx, nbuckets int, v float64, ts pcommon.Timestamp, tid pcommon.TraceID, sid pcommon.SpanID) {
	if tid.IsEmpty() {
		return
	}
	if h.ex == nil {
		h.ex = make([]exemplar, nbuckets)
	}
	h.ex[idx] = exemplar{set: true, value: v, ts: ts, traceID: tid, spanID: sid}
}

// seriesState tracks a series' values from observation to delivery, so eviction
// only ever drops values the collector has acked (agent/spanmetrics'
// reportState, under a name that cannot collide with the pairing store's own).
type seriesState uint8

const (
	seriesObserved  seriesState = iota // new values since the last render
	seriesRendered                     // rendered into a payload; delivery unknown
	seriesDelivered                    // a payload carrying them was acked
)

// NewRegistry builds the aggregator from cfg (Tempo's defaults fill the zero
// value).
func NewRegistry(cfg Config) *Registry {
	cfg = cfg.withDefaults()
	bounds := slices.Clone(cfg.HistogramBuckets)
	slices.Sort(bounds) // Validate demands ascending; New never refuses to aggregate over it
	ex := true          // see Config.Exemplars
	if cfg.Exemplars != nil {
		ex = *cfg.Exemplars
	}
	// An unparseable staleAfter falls back to the default; Config.Validate is
	// what reports it (and -check-config runs that), so New never refuses to
	// aggregate. "0" resolves to 0 here, which is the disable branch in
	// evictLocked.
	stale, _ := cfg.staleAfter()
	return &Registry{
		bounds:     bounds,
		maxCard:    cfg.MaxCardinality,
		staleAfter: stale,
		exemplars:  ex,
		now:        time.Now,
		series:     make(map[string]*edgeSeries),
	}
}

// Record aggregates one completed edge. Called from the pairing store, which
// runs on the ingest goroutines.
func (r *Registry) Record(e Edge) {
	now := r.now()

	// e.Dimensions arrives in an order that is already a function of the SET
	// (see joinDims), so the key is built by walking it — no scratch, no sort.
	// It used to iterate a map into a stack array and sort that on EVERY
	// completed request, because a map has no order and an unsorted key mints a
	// new series per permutation; the ordering now falls out of where the pairs
	// are built.
	//
	// truncDimValue (processor.go) is applied again HERE, on the values that go
	// into the key, even though the processor already truncates what it puts on
	// an Edge. It is a reslice, so the warm path pays nothing, and it removes a
	// class of bug rather than a cost: a promoted virtual-node edge takes its
	// far-side name from a peer attribute after truncation, and truncating the
	// value but not the key is what let spanmetrics hold two series that
	// rendered one byte-identical attribute set (a duplicate series in a single
	// payload — a conflict downstream, not extra detail). The RETAINED copies
	// are cut by retainDimValue instead (see edgeLabels): the key is consumed by
	// the map lookup, a label is held for the series' life.
	client := truncDimValue(e.ClientService)
	server := truncDimValue(e.ServerService)

	var keyScratch [256]byte
	key := keyScratch[:0]
	key = appendEdgeKeyPart(key, client)
	key = appendEdgeKeyPart(key, server)
	key = appendEdgeKeyPart(key, string(e.Connection))
	key = appendEdgeKeyPart(key, e.VirtualNode)
	for _, d := range e.Dimensions {
		if builtinLabels[d.Name] {
			continue // see builtinLabels
		}
		key = appendEdgeKeyPart(key, d.Name)
		key = appendEdgeKeyPart(key, truncDimValue(d.Value))
	}

	r.mu.Lock()
	s, ok := r.series[string(key)]
	if !ok {
		if len(r.series) >= r.maxCard {
			r.mu.Unlock()
			// Dropped, never silent: the cap is one of the three bounds the
			// config exposes, and an operator has to be able to see it bind.
			obs.ServiceGraphDropped.Inc()
			return
		}
		s = &edgeSeries{labels: edgeLabels(e), start: now}
		r.series[string(key)] = s
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
	if e.HaveClient {
		i := s.client.observe(r.bounds, e.ClientSeconds)
		if r.exemplars {
			s.client.setExemplar(i, nb, e.ClientSeconds, ts, e.TraceID, e.ClientSpanID)
		}
	}
	if e.HaveServer {
		i := s.server.observe(r.bounds, e.ServerSeconds)
		if r.exemplars {
			s.server.setExemplar(i, nb, e.ServerSeconds, ts, e.TraceID, e.ServerSpanID)
		}
	}
	s.lastSeen = now
	s.state = seriesObserved
	r.mu.Unlock()
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
		edgeLabel{labelClient, retainDimValue(e.ClientService)},
		edgeLabel{labelServer, retainDimValue(e.ServerService)},
		edgeLabel{labelConnectionType, string(e.Connection)})
	if e.VirtualNode != "" {
		out = append(out, edgeLabel{labelVirtualNode, e.VirtualNode})
	}
	for _, d := range e.Dimensions {
		if builtinLabels[d.Name] {
			continue // see builtinLabels
		}
		out = append(out, edgeLabel{d.Name, retainDimValue(d.Value)})
	}
	return out
}

// Run exports every interval until ctx is done, then once more. A non-positive
// interval falls back to one minute (NewTicker would panic). Mirrors
// agent/spanmetrics' Run so the two wire up the same way.
func (r *Registry) Run(ctx context.Context, exp Exporter, interval time.Duration, res pcommon.Resource, log *slog.Logger) {
	if log == nil {
		log = slog.Default()
	}
	if interval <= 0 {
		interval = time.Minute
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			// The final export runs on a detached context: ctx is already done,
			// and the last window's edges are as real as any other.
			fctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			if err := r.Export(fctx, exp, res); err != nil {
				log.Warn("final service-graph export failed", "error", err)
			}
			cancel()
			return
		case <-ticker.C:
			if err := r.Export(ctx, exp, res); err != nil {
				log.Warn("exporting service-graph metrics failed", "error", err)
			}
		}
	}
}

// Export renders the current cumulative aggregate under res and sends it once.
func (r *Registry) Export(ctx context.Context, exp Exporter, res pcommon.Resource) error {
	md := r.render(res, r.now())
	if md.ResourceMetrics().Len() == 0 {
		return nil
	}
	if err := exp.ExportMetrics(ctx, md); err != nil {
		return err
	}
	r.afterDelivered()
	return nil
}

// afterDelivered records that the rendered values reached the collector (only
// those may later be evicted) and clears every recorded exemplar. A series
// OBSERVED between render and this call is back in seriesObserved and is
// deliberately not marked: its new values must still be exported before
// eviction may touch them.
//
// The exemplar reset is keyed on DELIVERY, not on rendering: a failed send
// keeps the exemplars for the retry rather than wiping evidence no collector
// ever saw. (One recorded between render and this call is dropped unseen —
// the one-interval recency window an exemplar has by nature, and the same
// trade agent/spanmetrics makes.)
func (r *Registry) afterDelivered() {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, s := range r.series {
		if s.state == seriesRendered {
			s.state = seriesDelivered
		}
		for i := range s.client.ex {
			s.client.ex[i].set = false
		}
		for i := range s.server.ex {
			s.server.ex[i].set = false
		}
	}
}

func (r *Registry) render(res pcommon.Resource, now time.Time) pmetric.Metrics {
	r.renderMu.Lock()
	defer r.renderMu.Unlock()
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	res.CopyTo(rm.Resource())
	sm := rm.ScopeMetrics().AppendEmpty()
	sm.Scope().SetName(scopeName)

	r.renderEdges(sm, now)
	if sm.Metrics().Len() == 0 {
		return pmetric.NewMetrics() // nothing to send this cycle
	}
	return md
}

// growSnap makes the render scratch big enough for n series, with each entry's
// bucket and exemplar slices already allocated, so the copy under the mutex is
// pure memmove. Called without the mutex; the scratch belongs to renderMu.
func (r *Registry) growSnap(n int) {
	if cap(r.snap) < n {
		grown := make([]edgeSnapshot, n)
		copy(grown, r.snap)
		r.snap = grown
	} else {
		r.snap = r.snap[:n]
	}
	nb := len(r.bounds) + 1
	for i := range r.snap {
		e := &r.snap[i]
		if cap(e.client.buckets) < nb {
			e.client.buckets = make([]uint64, 0, nb)
			e.client.ex = make([]exemplar, 0, nb)
		}
		if cap(e.server.buckets) < nb {
			e.server.buckets = make([]uint64, 0, nb)
			e.server.ex = make([]exemplar, 0, nb)
		}
	}
}

// snapshot copies every series' current values out under the mutex and marks
// them rendered. It is the ONLY part of a render that holds r.mu.
//
// The build below used to run under it, and that is a receive-path stall, not
// just a slow export: Record is called by the pairing store from INSIDE its own
// mutex (store.upsert -> emit -> sink.Record), so every millisecond this lock is
// held is a millisecond in which no shard goroutine can Consume a span. At the
// cardinality cap the payload build is tens of milliseconds, once per export
// interval, and the whole of it used to land on the ingest path: a 46.7 ms
// Record stall inside a 46.7 ms render, now 1.6 ms
// (TestRenderDoesNotStallRecord).
func (r *Registry) snapshot(now time.Time) []edgeSnapshot {
	// Hold 1: evict, then take the series POINTERS. One pointer write each, so
	// this is the cheapest pass that can exist over a map — and it is what lets
	// the expensive part be chunked, because a slice can be walked across lock
	// releases and a map cannot.
	r.mu.Lock()
	r.evictLocked(now)
	ptrs := r.snapPtrs[:0]
	for _, s := range r.series {
		ptrs = append(ptrs, s)
	}
	r.snapPtrs = ptrs
	r.mu.Unlock()

	// Sizing (and, on the first render, two slice allocations per series) stays
	// outside the lock; later renders reuse the whole scratch.
	r.growSnap(len(ptrs))
	snap := r.snap

	// Holds 2..n: the values, in chunks. A series' fields are written by Record
	// under the mutex, so the copy has to hold it — but only for a chunk at a
	// time, which bounds one stall at a few hundred microseconds instead of the
	// whole cardinality cap. A Record landing between chunks is free to run and
	// puts that series back in seriesObserved, exactly as one landing between
	// the render and afterDelivered always could.
	for start := 0; start < len(ptrs); start += snapChunk {
		end := min(start+snapChunk, len(ptrs))
		r.mu.Lock()
		for i := start; i < end; i++ {
			s := ptrs[i]
			s.state = seriesRendered
			e := &snap[i]
			e.labels = s.labels
			e.requests, e.failed, e.start = s.requests, s.failed, s.start
			e.client.copyFrom(&s.client)
			e.server.copyFrom(&s.server)
		}
		r.mu.Unlock()
	}
	// Series ADMITTED during the walk are simply not in this payload; their
	// cumulative values ride the next one.
	clear(ptrs) // do not pin evicted series until the next render
	return snap[:len(ptrs)]
}

func (r *Registry) renderEdges(sm pmetric.ScopeMetrics, now time.Time) {
	snap := r.snapshot(now)
	if len(snap) == 0 {
		return
	}

	// Which histograms exist is decided BEFORE any point is appended, so the
	// payload's metric order is fixed rather than a function of which series the
	// map happened to yield first.
	var anyClient, anyServer bool
	for i := range snap {
		anyClient = anyClient || snap[i].client.present
		anyServer = anyServer || snap[i].server.present
		if anyClient && anyServer {
			break
		}
	}

	ts := pcommon.NewTimestampFromTime(now)
	requests := sumMetric(sm, metricRequests, "Total count of requests between two nodes.")
	failed := sumMetric(sm, metricFailed, "Total count of failed requests between two nodes.")
	var server, client pmetric.HistogramDataPointSlice
	if anyServer {
		server = histMetric(sm, metricServerSeconds, "Time for a request between two nodes as seen from the server.")
	}
	if anyClient {
		client = histMetric(sm, metricClientSeconds, "Time for a request between two nodes as seen from the client.")
	}

	for i := range snap {
		s := &snap[i]
		start := pcommon.NewTimestampFromTime(s.start)

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

		if s.server.present {
			putHist(server.AppendEmpty(), s.labels, &s.server, r.bounds, start, ts)
		}
		if s.client.present {
			putHist(client.AppendEmpty(), s.labels, &s.client, r.bounds, start, ts)
		}
	}
}

// evictLocked drops series not observed within staleAfter. Without it the
// cardinality cap is a ONE-WAY LATCH: one burst of short-lived services (a job
// that runs a thousand pods with distinct service names) permanently blinds the
// graph for everything that starts afterwards, and the dead edges render into
// every export forever. A cumulative counter that stops being reported is the
// standard staleness signal downstream. Caller holds the mutex.
func (r *Registry) evictLocked(now time.Time) {
	if r.staleAfter <= 0 { // eviction disabled
		return
	}
	for k, s := range r.series {
		// Only a series whose current values a DELIVERED export carried may go:
		// an export interval longer than staleAfter — or a failed export — must
		// not destroy observations unseen.
		if s.state == seriesDelivered && now.Sub(s.lastSeen) > r.staleAfter {
			delete(r.series, k)
			obs.ServiceGraphEvicted.Inc()
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

func putHist(p pmetric.HistogramDataPoint, labels []edgeLabel, h *histSnapshot, bounds []float64, start, ts pcommon.Timestamp) {
	putLabels(p.Attributes(), labels)
	p.SetStartTimestamp(start)
	p.SetTimestamp(ts)
	p.SetCount(h.count)
	p.SetSum(h.sum)
	p.ExplicitBounds().FromRaw(bounds)
	p.BucketCounts().FromRaw(h.buckets)
	// One exemplar per occupied bucket, in bucket order — the snapshot already
	// dropped the unset slots. The id is THIS side's own span (see
	// Edge.ClientSpanID), so the evidence attached to a latency explains the
	// latency it is attached to rather than the other half of the request.
	for i := range h.ex {
		e := p.Exemplars().AppendEmpty()
		e.SetDoubleValue(h.ex[i].value)
		e.SetTimestamp(h.ex[i].ts)
		e.SetTraceID(h.ex[i].traceID)
		e.SetSpanID(h.ex[i].spanID)
	}
}

// sumMetric appends a monotonic cumulative Sum shell. No unit is set: these are
// counts, and the `_total` the Prometheus mapping wants is already in the name
// (see the naming comment above).
func sumMetric(sm pmetric.ScopeMetrics, name, desc string) pmetric.NumberDataPointSlice {
	m := sm.Metrics().AppendEmpty()
	m.SetName(name)
	m.SetDescription(desc)
	s := m.SetEmptySum()
	s.SetIsMonotonic(true)
	s.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
	return s.DataPoints()
}

// histMetric appends a cumulative Histogram shell in seconds.
func histMetric(sm pmetric.ScopeMetrics, name, desc string) pmetric.HistogramDataPointSlice {
	m := sm.Metrics().AppendEmpty()
	m.SetName(name)
	m.SetDescription(desc)
	m.SetUnit("s")
	h := m.SetEmptyHistogram()
	h.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
	return h.DataPoints()
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

// appendEdgeKeyPart appends one length-prefixed value to a map key so distinct
// tuples never collide (("a","bc") vs ("ab","c")).
func appendEdgeKeyPart(dst []byte, v string) []byte {
	dst = strconv.AppendInt(dst, int64(len(v)), 10)
	dst = append(dst, ':')
	return append(dst, v...)
}
