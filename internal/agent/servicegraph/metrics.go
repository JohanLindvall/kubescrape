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
	now        func() time.Time

	mu     sync.Mutex
	series map[string]*edgeSeries
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
}

func (h *histAgg) observe(bounds []float64, v float64) {
	if h.buckets == nil {
		h.buckets = make([]uint64, len(bounds)+1)
	}
	h.count++
	h.sum += v
	h.buckets[bucketIndex(bounds, v)]++
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
	return &Registry{
		bounds:     bounds,
		maxCard:    cfg.MaxCardinality,
		staleAfter: cfg.StaleAfter,
		now:        time.Now,
		series:     make(map[string]*edgeSeries),
	}
}

// Record aggregates one completed edge. Called from the pairing store, which
// runs on the ingest goroutines.
func (r *Registry) Record(e Edge) {
	now := r.now()

	// The dimension keys are sorted so the series key and the rendered label
	// order are a function of the SET, not of Go's map iteration order — an
	// unsorted key would mint a new series per permutation.
	//
	// The scratch arrays keep the warm path allocation-free: neither escapes
	// (slices.Sort is generic, so unlike sort.Strings it does not box the slice
	// into an interface), and `map[string(key)]` does not copy the key for a
	// lookup. An edge with more dimensions than the scratch holds simply grows
	// onto the heap.
	var dimScratch [16]string
	dims := dimScratch[:0]
	for k := range e.Dimensions {
		if builtinLabels[k] {
			continue // see builtinLabels
		}
		dims = append(dims, k)
	}
	slices.Sort(dims)

	// truncDimValue (processor.go) is applied again HERE, on the values that go
	// into the key and into the rendered labels alike, even though the processor
	// already truncates what it puts on an Edge. It is a reslice, so the warm
	// path pays nothing, and it removes a class of bug rather than a cost: a
	// promoted virtual-node edge takes its far-side name from a peer attribute
	// after truncation, and truncating the value but not the key is what let
	// spanmetrics hold two series that rendered one byte-identical attribute set
	// (a duplicate series in a single payload — a conflict downstream, not extra
	// detail).
	client := truncDimValue(e.ClientService)
	server := truncDimValue(e.ServerService)

	var keyScratch [256]byte
	key := keyScratch[:0]
	key = appendEdgeKeyPart(key, client)
	key = appendEdgeKeyPart(key, server)
	key = appendEdgeKeyPart(key, string(e.Connection))
	key = appendEdgeKeyPart(key, e.VirtualNode)
	for _, k := range dims {
		key = appendEdgeKeyPart(key, k)
		key = appendEdgeKeyPart(key, truncDimValue(e.Dimensions[k]))
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
		s = &edgeSeries{labels: edgeLabels(client, server, e, dims), start: now}
		r.series[string(key)] = s
	}
	s.requests++
	if e.Failed {
		s.failed++
	}
	// A side that never arrived is not observed — its absence is the point of a
	// virtual-node edge, and a zero would read as a zero-latency call.
	if e.HaveClient {
		s.client.observe(r.bounds, e.ClientSeconds)
	}
	if e.HaveServer {
		s.server.observe(r.bounds, e.ServerSeconds)
	}
	s.lastSeen = now
	s.state = seriesObserved
	r.mu.Unlock()
}

// edgeLabels materializes the attribute set for a newly admitted series (cold
// path). dims is read only, so the caller's stack scratch does not escape.
func edgeLabels(client, server string, e Edge, dims []string) []edgeLabel {
	out := make([]edgeLabel, 0, 4+len(dims))
	out = append(out,
		edgeLabel{labelClient, client},
		edgeLabel{labelServer, server},
		edgeLabel{labelConnectionType, string(e.Connection)})
	if e.VirtualNode != "" {
		out = append(out, edgeLabel{labelVirtualNode, e.VirtualNode})
	}
	for _, k := range dims {
		out = append(out, edgeLabel{k, truncDimValue(e.Dimensions[k])})
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

// afterDelivered records that the rendered values reached the collector — only
// those may later be evicted. A series OBSERVED between render and this call is
// back in seriesObserved and is deliberately not marked: its new values must
// still be exported before eviction may touch them.
func (r *Registry) afterDelivered() {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, s := range r.series {
		if s.state == seriesRendered {
			s.state = seriesDelivered
		}
	}
}

func (r *Registry) render(res pcommon.Resource, now time.Time) pmetric.Metrics {
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

func (r *Registry) renderEdges(sm pmetric.ScopeMetrics, now time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.evictLocked(now)
	if len(r.series) == 0 {
		return
	}

	// Which histograms exist is decided BEFORE any point is appended, so the
	// payload's metric order is fixed rather than a function of which series the
	// map happened to yield first.
	var anyClient, anyServer bool
	for _, s := range r.series {
		anyClient = anyClient || s.client.buckets != nil
		anyServer = anyServer || s.server.buckets != nil
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

	for _, s := range r.series {
		s.state = seriesRendered
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

		if s.server.buckets != nil {
			putHist(server.AppendEmpty(), s.labels, &s.server, r.bounds, start, ts)
		}
		if s.client.buckets != nil {
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

func putHist(p pmetric.HistogramDataPoint, labels []edgeLabel, h *histAgg, bounds []float64, start, ts pcommon.Timestamp) {
	putLabels(p.Attributes(), labels)
	p.SetStartTimestamp(start)
	p.SetTimestamp(ts)
	p.SetCount(h.count)
	p.SetSum(h.sum)
	p.ExplicitBounds().FromRaw(bounds)
	p.BucketCounts().FromRaw(h.buckets)
	// No exemplars: an Edge is a PAIR of spans and carries no trace id by the
	// time it reaches here, and picking one half's id would attach an exemplar
	// that explains only that half of the latency.
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
