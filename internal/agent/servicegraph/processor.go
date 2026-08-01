package servicegraph

import (
	"log/slog"
	"strings"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

// edgeSink receives every edge the store finishes with: completed pairs and
// expired halves promoted against a virtual node. It is the ONLY seam between
// pairing and metrics — metrics.go implements it, and nothing about series,
// buckets or cardinality is visible on this side of it.
type edgeSink interface{ Record(Edge) }

const (
	attrServiceName = "service.name"

	// sweepPerBatch is the incremental expiry one Consume call pays. Expiry has
	// to be driven by SOMETHING on a shard that is only being pushed spans, and
	// paying it here keeps the store from growing to MaxItems while a caller's
	// Sweep ticker is between ticks. It is cheap: the expiry list is ordered,
	// so a batch with nothing due pays one time comparison.
	sweepPerBatch = 32

	// sweepBudget bounds one Sweep call. Bounded rather than draining, because
	// the sweep holds the mutex every concurrent Consume needs: a full drain of
	// a 10k-entry store would stall the shard's ingest for its duration. At a
	// one-second Sweep cadence this retires 1024 half-edges per second — far
	// past what a shard can accumulate in a Wait window without also hitting
	// MaxItems, whose counter is the signal to look at.
	sweepBudget = 1024
)

// databaseAttrs mark a client span as talking to a database. The first two are
// Tempo's (dbNameAttr/db.system); the other two are the current semconv
// spellings after the 1.30 rename — an SDK on today's conventions emits only
// those, and omitting them would classify every modern database client as a
// plain service-to-service call.
var databaseAttrs = []string{"db.system", "db.name", "db.system.name", "db.namespace"}

// Processor pairs the spans of one shard into edges. It mirrors
// spanmetrics.Generator's shape — construct, Consume from the concurrent ingest
// goroutines, injectable clock — but keeps no series of its own: everything it
// derives leaves through the sink.
type Processor struct {
	cfg Config
	log *slog.Logger

	store *edgeStore
	sink  edgeSink

	// dims are the configured dimension keys; clientDims/serverDims are the
	// same keys with the client_/server_ prefix applied ONCE at construction.
	// The prefixed name is what lands on the edge, and concatenating it per
	// span per dimension would allocate on the hot path for a value that never
	// changes.
	dims                   []string
	clientDims, serverDims []string

	// peerAttrs are the virtual-node peer attributes in precedence order;
	// peerIsDB[i] marks the ones that name a database, so a matched db.name
	// classifies the edge without a second attribute scan.
	peerAttrs []string
	peerIsDB  []bool

	// now is injectable for tests; production reads the wall clock once per
	// Consume batch, never per span.
	now func() time.Time
}

// NewProcessor builds a processor from cfg (the zero value is valid and takes
// Tempo's defaults). A sink must be wired with SetSink before the first
// Consume; a processor without one still pairs and counts, it just has nowhere
// to put the edges.
func NewProcessor(cfg Config, log *slog.Logger) *Processor {
	if log == nil {
		log = slog.Default()
	}
	cfg = cfg.withDefaults()
	p := &Processor{cfg: cfg, log: log, now: time.Now}

	// Deduplicate the configured dimensions. A repeat would resolve twice and
	// write the same map key twice — harmless but pure waste on the hot path
	// (spanmetrics learned a sharper version of this lesson: there a duplicate
	// silently blanked a built-in label).
	seen := make(map[string]bool, len(cfg.Dimensions))
	for _, d := range cfg.Dimensions {
		if d == "" || seen[d] {
			continue
		}
		seen[d] = true
		p.dims = append(p.dims, d)
		p.clientDims = append(p.clientDims, "client_"+d)
		p.serverDims = append(p.serverDims, "server_"+d)
	}
	for _, a := range cfg.VirtualNodePeerAttributes {
		if a == "" {
			continue
		}
		p.peerAttrs = append(p.peerAttrs, a)
		p.peerIsDB = append(p.peerIsDB, strings.HasPrefix(a, "db."))
	}
	p.store = newEdgeStore(cfg, p.emit)
	log.Debug("service-graph pairing configured",
		"wait", cfg.Wait, "maxItems", cfg.MaxItems,
		"dimensions", len(p.dims), "peerAttributes", len(p.peerAttrs))
	return p
}

// Wait reports the effective pairing window (Tempo's default when unset). The
// caller driving Sweep sizes its cadence from this rather than re-deriving the
// defaults, so a configured Wait and the sweep that enforces it cannot drift.
func (p *Processor) Wait() time.Duration { return p.cfg.Wait }

// SetSink wires the metric writer. Call it before the first Consume: the sink
// is read on the pairing path under the store's mutex, and swapping it under a
// live ingest stream would be a data race for no gain (there is exactly one
// writer per shard, built at startup).
func (p *Processor) SetSink(s edgeSink) { p.sink = s }

// Stats reports the pairing counters (see Stats). metrics.go publishes them.
func (p *Processor) Stats() Stats { return p.store.stats() }

func (p *Processor) emit(e Edge) {
	if p.sink == nil {
		return
	}
	p.sink.Record(e)
}

// Consume feeds every span in td into the pairing store. It runs on the
// concurrent ingest handler goroutines (the store is mutex-guarded) and never
// mutates td.
func (p *Processor) Consume(td ptrace.Traces) {
	// One clock read per BATCH, not per span, exactly as spanmetrics does it:
	// the clock feeds a Wait-scale (seconds) expiry decision, and a syscall per
	// span would dominate the per-span cost.
	now := p.now()
	// Incremental expiry on the way in — see sweepPerBatch.
	p.store.expire(now, sweepPerBatch)

	rss := td.ResourceSpans()
	for i := 0; i < rss.Len(); i++ {
		rs := rss.At(i)
		resAttrs := rs.Resource().Attributes()
		// Truncated once per resource: it becomes a label value on every edge
		// this resource's spans produce.
		svc := truncDimValue(spanAttrStr(resAttrs, attrServiceName))
		sss := rs.ScopeSpans()
		for j := 0; j < sss.Len(); j++ {
			spans := sss.At(j).Spans()
			for k := 0; k < spans.Len(); k++ {
				p.observe(spans.At(k), resAttrs, svc, now)
			}
		}
	}
}

// Sweep runs one bounded expiry pass. Safe to call periodically (and cheap when
// nothing is due); the caller owns the cadence.
func (p *Processor) Sweep() { p.store.expire(p.now(), sweepBudget) }

func (p *Processor) observe(span ptrace.Span, resAttrs pcommon.Map, svc string, now time.Time) {
	var side edgeSide
	conn := ConnectionUnknown
	switch span.Kind() {
	case ptrace.SpanKindClient:
		side = sideClient
	case ptrace.SpanKindProducer:
		// A producer is the client half of an asynchronous hop, and the pair
		// being a messaging one is known from the KIND alone — the consumer
		// says the same thing, so whichever arrives first classifies the edge.
		side, conn = sideClient, ConnectionMessagingSystem
	case ptrace.SpanKindServer:
		side = sideServer
	case ptrace.SpanKindConsumer:
		side, conn = sideServer, ConnectionMessagingSystem
	default:
		// INTERNAL and UNSPECIFIED spans are not a call between two services;
		// pairing them would invent edges inside a single process.
		return
	}

	tid := span.TraceID()
	if tid.IsEmpty() {
		// Unkeyable: with a zero trace id every such span in the shard shares
		// one key space and would cross-pair unrelated requests into invented
		// edges. Counted, because a whole SDK emitting these would otherwise
		// look like a quiet graph rather than a broken one.
		p.store.countUnkeyable()
		return
	}
	// The client stores under its OWN span id; the server looks up its PARENT's
	// — that is the one id both halves of a request agree on. A server span
	// with no parent (a root, e.g. an ingress hop from outside the mesh) keys
	// under the zero span id: no client half can ever arrive, so it expires and
	// promotes against a peer attribute if it has one. Two such roots in one
	// trace share that key and merge; Tempo has the same behaviour, and the
	// alternative — dropping them — deletes the graph's whole entry edge.
	sid := span.SpanID()
	if side == sideServer {
		sid = span.ParentSpanID()
	}

	spanAttrs := span.Attributes()
	h := halfSpan{
		service:    svc,
		seconds:    spanSeconds(span),
		failed:     span.Status().Code() == ptrace.StatusCodeError,
		connection: conn,
	}

	// Peer attributes, in the configured precedence order: the FIRST one
	// present names the far side. Both sides are scanned, not just the client
	// (Tempo scans only the client): a server span carrying peer.service is how
	// an uninstrumented CALLER — a browser, an external client, an ingress —
	// gets a name, which is the whole virtual_node "client" case.
	//
	// The db.* attributes are the exception, and they are skipped on a server
	// half: they describe the span's own CALLEE, while the side missing from a
	// server half is the CALLER, so honouring them there would name the far
	// side backwards ("postgresql called us") and classify an ordinary
	// application edge as a database one.
	for i, a := range p.peerAttrs {
		if p.peerIsDB[i] && side == sideServer {
			continue
		}
		v := spanAttrStr(spanAttrs, a)
		if v == "" {
			continue
		}
		h.peer = truncDimValue(v)
		if p.peerIsDB[i] {
			h.connection = ConnectionDatabase
		}
		break
	}
	// A client span naming a database is a database edge even when its peer
	// attribute was peer.service (or absent): the db.* attributes are the
	// authoritative statement that the callee is a datastore. Only the client
	// side is asked — a database does not emit the server half, so a server
	// span carrying db.* is a service that happens to talk to one, not the
	// database itself.
	if h.connection == ConnectionUnknown && side == sideClient && namesDatabase(spanAttrs) {
		h.connection = ConnectionDatabase
	}

	// dims is the borrowed scratch handed to upsert; nil when nothing is
	// configured, which is the default.
	var dims []dimKV
	if len(p.dims) > 0 {
		// Stack scratch: upsert COPIES the dimensions and never retains the
		// slice, so for the usual handful of configured dimensions this never
		// reaches the heap (BenchmarkConsumePairWithDimensions is the alarm —
		// the only allocations it may show are the completed edge's map).
		var scratch [8]dimKV
		var ds []dimKV
		if len(p.dims) <= len(scratch) {
			ds = scratch[:0]
		} else {
			ds = make([]dimKV, 0, len(p.dims))
		}
		names := p.clientDims
		if side == sideServer {
			names = p.serverDims
		}
		for i, d := range p.dims {
			v := spanAttrStr(spanAttrs, d)
			if v == "" {
				v = spanAttrStr(resAttrs, d) // fall back to the resource
			}
			if v == "" {
				// Absent dimensions are simply not carried; the metric layer
				// renders a missing key as the empty label value, so recording
				// "" here would only cost a map entry per edge.
				continue
			}
			ds = append(ds, dimKV{name: names[i], value: truncDimValue(v)})
		}
		dims = ds
	}

	p.store.upsert(now, makeEdgeKey(tid, sid), side, h, dims)
}

// namesDatabase reports whether the span carries any of the attributes that
// identify a database callee.
func namesDatabase(attrs pcommon.Map) bool {
	for _, a := range databaseAttrs {
		if _, ok := attrs.Get(a); ok {
			return true
		}
	}
	return false
}

// --- helpers ---
//
// spanAttrStr, spanSeconds and truncDimValue duplicate spanmetrics' attrStr,
// durationSeconds and truncDim: those are unexported there, and exporting them
// to share three lines would couple two packages that otherwise share nothing.
// The names differ from spanmetrics' deliberately — this package's other files
// (metrics.go, ring.go, forward.go) are written independently, and short
// generic names are how two of them collide.

func spanAttrStr(m pcommon.Map, key string) string {
	if v, ok := m.Get(key); ok {
		return v.AsString()
	}
	return ""
}

// spanSeconds is the span's own measured duration. An unset or clock-skewed end
// yields 0 rather than a negative duration, which no histogram can hold.
func spanSeconds(span ptrace.Span) float64 {
	end, start := span.EndTimestamp(), span.StartTimestamp()
	if end <= start {
		return 0
	}
	return float64(end-start) / float64(time.Second)
}

// truncDimValue bounds one label value. These come from an unauthenticated
// listener (the shard receives whatever the agents forward, which is whatever a
// sender pushed): an untruncated service or peer name would be retained for a
// whole Wait per in-flight request, and then per SERIES for as long as the
// series lives. Truncating a Go string is a reslice, so this is free.
func truncDimValue(v string) string {
	if len(v) <= maxDimensionValueBytes {
		return v
	}
	return v[:maxDimensionValueBytes]
}
