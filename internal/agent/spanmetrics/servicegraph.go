package spanmetrics

import (
	"sync"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/JohanLindvall/kubescrape/internal/obs"
)

// serviceGraphPrefix names the edge metrics (Prometheus:
// traces_service_graph_request_*), a distinct namespace from the span metrics.
const serviceGraphPrefix = "traces.service_graph"

// edgeTTL bounds how long a half-edge waits for its counterpart span before it
// is dropped as unpaired (counted in obs.ServiceGraphUnpaired).
const edgeTTL = 2 * time.Minute

// serviceGraph pairs a client span with its child server span (same trace, the
// server span's parent = the client span) into a request edge, and aggregates
// per-edge request/error counts and client/server latency histograms.
//
// A half-edge (whichever span arrives first) is held in `pending` keyed by
// (trace id, client span id); when its counterpart arrives the edge completes.
// Only direct client→server calls pair; an intermediate span between them leaves
// both halves unpaired (they expire). Cumulative like the span metrics.
type serviceGraph struct {
	bounds  []float64
	maxCard int

	mu      sync.Mutex
	pending map[edgeKey]*halfEdge
	edges   map[string]*edgeSeries // key = client\x00server
	start   time.Time
}

type edgeKey struct {
	trace pcommon.TraceID
	span  pcommon.SpanID // the client span's id (= the server span's parent id)
}

type halfEdge struct {
	clientSet bool
	client    string
	clientDur float64
	serverSet bool
	server    string
	serverDur float64
	failed    bool // either half errored
	expires   time.Time
}

type edgeSeries struct {
	client, server string
	total          uint64
	failed         uint64
	serverCount    uint64
	serverSum      float64
	serverBuckets  []uint64
	clientCount    uint64
	clientSum      float64
	clientBuckets  []uint64
}

func newServiceGraph(bounds []float64, maxCard int) *serviceGraph {
	return &serviceGraph{
		bounds:  bounds,
		maxCard: maxCard,
		pending: make(map[edgeKey]*halfEdge),
		edges:   make(map[string]*edgeSeries),
		start:   time.Now(),
	}
}

// consume folds one span into the pending half-edges, completing an edge when
// both halves are present. Safe for concurrent use.
func (sg *serviceGraph) consume(span ptrace.Span, svc string) {
	kind := span.Kind()
	isClient := kind == ptrace.SpanKindClient || kind == ptrace.SpanKindProducer
	isServer := kind == ptrace.SpanKindServer || kind == ptrace.SpanKindConsumer
	if !isClient && !isServer {
		return // internal/unspecified spans are not graph edges
	}
	trace := span.TraceID()
	if trace.IsEmpty() {
		return
	}
	var key edgeKey
	if isClient {
		key = edgeKey{trace: trace, span: span.SpanID()}
	} else {
		key = edgeKey{trace: trace, span: span.ParentSpanID()}
	}
	if key.span.IsEmpty() {
		return // a root server span (no parent) has no client to pair with
	}
	failed := span.Status().Code() == ptrace.StatusCodeError
	dur := durationSeconds(span)

	sg.mu.Lock()
	defer sg.mu.Unlock()
	h := sg.pending[key]
	if h == nil {
		if len(sg.pending) >= sg.maxCard {
			obs.ServiceGraphDropped.Inc()
			return
		}
		h = &halfEdge{expires: time.Now().Add(edgeTTL)}
		sg.pending[key] = h
	}
	if isClient {
		h.clientSet, h.client, h.clientDur = true, svc, dur
	} else {
		h.serverSet, h.server, h.serverDur = true, svc, dur
	}
	if failed {
		h.failed = true
	}
	if h.clientSet && h.serverSet {
		sg.complete(h)
		delete(sg.pending, key)
	}
}

// complete records a finished edge. Caller holds sg.mu.
func (sg *serviceGraph) complete(h *halfEdge) {
	if h.client == "" || h.server == "" {
		return
	}
	ek := h.client + "\x00" + h.server
	e := sg.edges[ek]
	if e == nil {
		if len(sg.edges) >= sg.maxCard {
			obs.ServiceGraphDropped.Inc()
			return
		}
		e = &edgeSeries{
			client: h.client, server: h.server,
			serverBuckets: make([]uint64, len(sg.bounds)+1),
			clientBuckets: make([]uint64, len(sg.bounds)+1),
		}
		sg.edges[ek] = e
	}
	e.total++
	if h.failed {
		e.failed++
	}
	e.serverCount++
	e.serverSum += h.serverDur
	e.serverBuckets[bucketIndex(sg.bounds, h.serverDur)]++
	e.clientCount++
	e.clientSum += h.clientDur
	e.clientBuckets[bucketIndex(sg.bounds, h.clientDur)]++
}

// appendMetrics renders the edge metrics into sm and sweeps expired half-edges.
func (sg *serviceGraph) appendMetrics(sm pmetric.ScopeMetrics, start, ts pcommon.Timestamp, now time.Time) {
	sg.mu.Lock()
	defer sg.mu.Unlock()
	// Drop half-edges whose counterpart never arrived.
	for k, h := range sg.pending {
		if now.After(h.expires) {
			delete(sg.pending, k)
			obs.ServiceGraphUnpaired.Inc()
		}
	}
	if len(sg.edges) == 0 {
		return
	}
	total := sumMetric(sm, serviceGraphPrefix+".request.total", "Requests between two services (client→server edges).", "")
	failed := sumMetric(sm, serviceGraphPrefix+".request.failed", "Failed requests between two services.", "")
	server := histMetric(sm, serviceGraphPrefix+".request.server", "Server-side request duration in seconds.")
	client := histMetric(sm, serviceGraphPrefix+".request.client", "Client-side request duration in seconds.")
	for _, e := range sg.edges {
		tp := total.AppendEmpty()
		edgeAttrs(tp.Attributes(), e)
		tp.SetStartTimestamp(start)
		tp.SetTimestamp(ts)
		tp.SetIntValue(int64(e.total))

		fp := failed.AppendEmpty()
		edgeAttrs(fp.Attributes(), e)
		fp.SetStartTimestamp(start)
		fp.SetTimestamp(ts)
		fp.SetIntValue(int64(e.failed))

		edgeHist(server.AppendEmpty(), e, sg.bounds, start, ts, e.serverCount, e.serverSum, e.serverBuckets)
		edgeHist(client.AppendEmpty(), e, sg.bounds, start, ts, e.clientCount, e.clientSum, e.clientBuckets)
	}
}

func edgeAttrs(a pcommon.Map, e *edgeSeries) {
	a.PutStr("client", e.client)
	a.PutStr("server", e.server)
}

func edgeHist(hp pmetric.HistogramDataPoint, e *edgeSeries, bounds []float64, start, ts pcommon.Timestamp, count uint64, sum float64, buckets []uint64) {
	edgeAttrs(hp.Attributes(), e)
	hp.SetStartTimestamp(start)
	hp.SetTimestamp(ts)
	hp.SetCount(count)
	hp.SetSum(sum)
	hp.ExplicitBounds().FromRaw(bounds)
	hp.BucketCounts().FromRaw(buckets)
}
