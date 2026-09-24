package servicegraph

import (
	"context"
	"log/slog"
	"math"
	"slices"
	"strings"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/JohanLindvall/kubescrape/internal/agent/cumagg"
	"github.com/JohanLindvall/kubescrape/internal/clip"
	"github.com/JohanLindvall/kubescrape/internal/logdedupe"
	"github.com/JohanLindvall/kubescrape/internal/obs"
)

// EdgeSink receives every edge the store finishes with: completed pairs and
// expired halves promoted against a virtual node. It is the ONLY seam between
// pairing and metrics — metrics.go implements it, and nothing about series,
// buckets or cardinality is visible on this side of it.
//
// It is handed the pairing pass's clock with the edge (see Registry.RecordAt):
// the sink runs under the pairing mutex, and the pass has already read the time.
type EdgeSink interface{ RecordAt(Edge, time.Time) }

const (
	attrServiceName = "service.name"

	// Expiry has to be driven by SOMETHING on a shard that is only being pushed
	// spans, so Consume pays an incremental pass on the way in — which is what
	// keeps the store from growing to MaxItems between a caller's Sweep ticks.
	//
	// Its budget SCALES WITH THE BATCH, and that is the whole point: a fixed
	// per-call budget is a per-BATCH rate, and the batch size is the SENDER's
	// choice, not this shard's. The same 1000 unpairable half-edges per second
	// drained comfortably at 20-span pushes and fell far behind at 200-span
	// ones, filling the store and dropping spans — a memory bound that moved
	// when someone tuned an SDK's batch processor.
	//
	// A span creates AT MOST one half-edge, so retiring up to one entry per span
	// can always keep pace with insertion however the spans are batched;
	// sweepFloorPerBatch is the extra headroom that lets a backlog DRAIN rather
	// than merely hold, and covers a small batch arriving after a quiet spell.
	// It stays cheap because the expiry list is expiry-ordered: a batch with
	// nothing due pays one time comparison whatever the budget says.
	sweepFloorPerBatch = 32
	// maxSweepBudgetPerBatch bounds the TOTAL a single Consume may spend. Without
	// any cap a pathological batch could sweep for as long as its span count;
	// with the per-hold ceiling as the only bound the backlog never drains. This
	// is the compromise: many short holds, bounded in aggregate; the ticker's
	// sweep picks up any remainder.
	maxSweepBudgetPerBatch = 16384

	// expirePerHold is the PER-LOCK-HOLD ceiling of every expiry pass: Consume's
	// incremental one, and the sweeps driven from outside it — the ticker's
	// (cmd/kubescrape-agent's sweepServiceGraph, cadence wait/2 clamped to
	// [1s, 30s], i.e. 5s at the default 10s wait) and the shutdown one. A pass
	// holds the mutex every concurrent Consume needs, so neither one huge push
	// nor a deep backlog may stall the shard's ingest for its whole length;
	// expireInPasses spends a larger budget in passes of this size with the
	// mutex released between them. It is a ceiling per HOLD and never a total:
	// the total is the caller's (the batch's span count for Consume, everything
	// due for a sweep).
	expirePerHold = 1024
)

// databaseAttrs mark a client span as talking to a database. The first two are
// Tempo's (dbNameAttr/db.system); the other two are the current semconv
// spellings after the renames — db.name -> db.namespace landed in v1.26.0 and
// db.system -> db.system.name in v1.30.0 (v1.31.0 for database METRICS) — an
// SDK on today's conventions emits only those, and omitting them would
// classify every modern database client as a plain service-to-service call.
//
// DELIBERATELY wider than DefaultPeerAttributes (servicegraph.go), which stays
// Tempo's verbatim {peer.service, db.name, db.system}: that list is a wire
// contract (it names the virtual node, i.e. the `server` label dashboards
// select on), while this one only classifies connection_type. So a 1.30-only
// database client gets `connection_type="database"` but NO virtual node for
// its unpaired spans until the operator adds db.system.name/db.namespace to
// `serviceGraph.virtualNodePeerAttributes` — the config escape hatch. Widening
// the default there is a product decision, not something to "fix" here.
var databaseAttrs = []string{"db.system", "db.name", "db.system.name", "db.namespace"}

// dbAttrPrefix is the prefix every databaseAttrs entry shares, and namesDatabase's
// gate.
const dbAttrPrefix = "db."

// Processor pairs the spans of one shard into edges. It mirrors
// spanmetrics.Generator's shape — construct, a forward-first Tap, Consume from
// the concurrent ingest goroutines, injectable clock — but keeps no series of
// its own: everything it derives leaves through the sink.
type Processor struct {
	// wait is the RESOLVED pairing window: Config.Wait is a string (it has to
	// be, to decode from YAML — see Config.Wait), parsed once here.
	wait time.Duration
	log  *slog.Logger
	// unnamedWarn throttles the no-service.name warning: an unattributable
	// sender pushes continuously, and the useful information is one line.
	unnamedWarn logdedupe.Throttle

	store *edgeStore
	sink  EdgeSink

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
// Tempo's defaults) writing its edges to sink. The sink is fixed at
// construction because it is read on the pairing path under the store's mutex,
// so there is no ordering contract to keep; a nil sink is legal — the processor
// still pairs and counts, it just has nowhere to put the edges.
func NewProcessor(cfg Config, sink EdgeSink, log *slog.Logger) *Processor {
	if log == nil {
		log = slog.Default()
	}
	cfg = cfg.withDefaults()
	// An invalid wait falls back to the default rather than refusing to pair;
	// Config.Validate is what reports it, and -check-config runs that on every
	// start (cumagg.ResolveStaleAfter makes the same trade for staleAfter).
	// "Invalid", not "unparseable": wait() refuses a zero and a negative too,
	// both of which parse.
	wait, err := cfg.wait()
	if err != nil {
		log.Warn("serviceGraph.wait is invalid; using the default pairing window", "error", err, "wait", wait)
	}
	p := &Processor{wait: wait, log: log, now: time.Now, sink: sink}

	// Drop an empty or repeated configured dimension. A repeat would resolve
	// twice and write the same map key twice — harmless but pure waste on the
	// hot path (spanmetrics learned a sharper version of this lesson: there a
	// duplicate silently blanked a built-in label) — and an empty one would mint
	// a client_/server_ label pair with no name. The rule is cumagg's, shared
	// with spanmetrics, which is where the empty-name half had drifted: this
	// loop refused it while spanmetrics rendered an empty-KEY attribute.
	//
	// Dropping is right; doing it SILENTLY is not — the operator asked for a
	// label and got none. The Warn is configWarnings' (cmd/kubescrape-agent),
	// from Config.DimensionWarnings, so -check-config says it too; Configure
	// traces it at Debug only, or every start would print it twice.
	for _, d := range noBuiltins.Configure(dimensionsField, cfg.Dimensions, log) {
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
	p.store = newEdgeStore(cfg.MaxItems, wait, p.emit, log)
	log.Debug("service-graph pairing configured",
		"wait", wait, "maxItems", cfg.MaxItems,
		"dimensions", len(p.dims), "peerAttributes", len(p.peerAttrs))
	return p
}

// Wait reports the effective pairing window (Tempo's default when unset). The
// caller driving Sweep sizes its cadence from this rather than re-deriving the
// defaults, so a configured Wait and the sweep that enforces it cannot drift.
func (p *Processor) Wait() time.Duration { return p.wait }

// Stats reports the pairing counters (see Stats, which says which of them
// cmd/kubescrape-agent publishes through obs.RegisterServiceGraphStats).
func (p *Processor) Stats() Stats { return p.store.stats() }

func (p *Processor) emit(e Edge, now time.Time) {
	if p.sink == nil {
		return
	}
	p.sink.RecordAt(e, now)
}

// Consume feeds every span in td into the pairing store. It runs on the
// concurrent ingest handler goroutines (the store is mutex-guarded) and never
// mutates td.
func (p *Processor) Consume(td ptrace.Traces) {
	// One clock read per BATCH, not per span, exactly as spanmetrics does it:
	// the clock feeds a Wait-scale (seconds) expiry decision, and a syscall per
	// span would dominate the per-span cost.
	now := p.now()
	// Incremental expiry on the way in, budgeted by THIS batch's span count —
	// see sweepFloorPerBatch. SpanCount walks the scopes, not the spans, so it
	// costs nothing next to the per-span work below.
	//
	// The whole budget is spent, in expirePerHold-sized passes. Spending only
	// one pass's worth of it made the batch scaling a lie — the budget IS the
	// batch's span count, and a batch carrying more than 1024 unpairable
	// half-edges then expired fewer than it added, so occupancy climbed past the
	// honest rate x wait working set, pinned at MaxItems, and the store started
	// refusing arriving spans.
	p.store.expireInPasses(now, min(td.SpanCount()+sweepFloorPerBatch, maxSweepBudgetPerBatch))

	rss := td.ResourceSpans()
	for i := 0; i < rss.Len(); i++ {
		rs := rss.At(i)
		resAttrs := rs.Resource().Attributes()
		// Truncated once per resource: it becomes a label value on every edge
		// this resource's spans produce.
		svc := cumagg.Retain(cumagg.AttrStr(resAttrs, attrServiceName))
		sss := rs.ScopeSpans()
		if svc == "" {
			// No service.name (or an empty one) is nothing to name a graph
			// node with: pairing these spans anyway minted anonymous
			// ""-labeled nodes that every unattributable sender in the
			// cluster shared — one bogus vertex accreting edges to real
			// services — where Tempo skips them. Skipped at the RESOURCE and
			// for the GRAPH only: the tap calling Consume forwards first, so
			// the spans still reach the collector, and only the graph goes
			// without them. Counted, never silent — the shipped tier enriches
			// at entry and derives service.name for every attributable
			// sender, so a sustained rate here means senders the tier cannot
			// attribute, whose requests are on no edge at all.
			// Only the spans PAIRING would have used. The named arm below
			// calls observe, which returns without counting anything for an
			// INTERNAL or UNSPECIFIED span — those are not a call between two
			// services — so counting every span here measured a different
			// thing on each side of the same question: an ordinary batch is
			// mostly internal spans, and the incompleteness ratio an operator
			// computes against kubescrape_service_graph_completed_total came
			// out inflated several-fold by a fraction that was never going to
			// be an edge. ONE classifier for both arms, so they cannot drift.
			if n := edgeCapableSpans(sss); n > 0 {
				obs.ServiceGraphUnnamed.Add(float64(n))
				p.reportUnnamed(rs.Resource().Attributes(), n)
			}
			continue
		}
		for j := 0; j < sss.Len(); j++ {
			spans := sss.At(j).Spans()
			for k := 0; k < spans.Len(); k++ {
				p.observe(spans.At(k), resAttrs, svc, now)
			}
		}
	}
}

// Tap returns a TracesExporter that forwards each batch to inner FIRST and feeds
// it to Consume only once that succeeded — spanmetrics.Generator.Tap's shape,
// and for the same reason: a failed export surfaces to the sender as retryable,
// and the re-pushed batch would otherwise pair twice, so every outage or
// back-pressure window would permanently inflate the cumulative edge counters.
// (A retry after a lost ack still double-counts; that is the unavoidable
// at-least-once residue.) It is the top of the trace tier's owner chain
// (cmd/kubescrape-agent's buildOwnerChain), and Consume runs on the concurrent
// receiver goroutines, which the pairing store's mutex is there for.
func (p *Processor) Tap(inner TracesExporter) TracesExporter {
	return &pairTap{proc: p, inner: inner}
}

type pairTap struct {
	proc  *Processor
	inner TracesExporter
}

func (t *pairTap) ExportTraces(ctx context.Context, td ptrace.Traces) error {
	if err := t.inner.ExportTraces(ctx, td); err != nil {
		return err
	}
	t.proc.Consume(td)
	return nil
}

// edgeCapableSpans counts the spans pairing would have used — the kinds spanSide
// accepts — which is the unit kubescrape_service_graph_unnamed_spans_total is
// in (see Consume's unnamed-resource arm).
func edgeCapableSpans(sss ptrace.ScopeSpansSlice) int {
	n := 0
	for j := 0; j < sss.Len(); j++ {
		spans := sss.At(j).Spans()
		for k := 0; k < spans.Len(); k++ {
			if _, _, ok := spanSide(spans.At(k).Kind()); ok {
				n++
			}
		}
	}
	return n
}

// unnamedHints are the attributes a throttled unnamed-resource warning quotes
// to identify the sender. They are IDENTITY attributes only — never the whole
// map, which is sender-controlled and may carry anything.
var unnamedHints = []string{"k8s.namespace.name", "k8s.pod.name", "host.name", "telemetry.sdk.language"}

// unnamedWarnEvery re-warns while unnameable senders keep pushing.
const unnamedWarnEvery = 5 * time.Minute

// maxLoggedValueBytes bounds one SENDER-supplied value on a log line. A
// resource attribute on this tier arrives from an application pushing to the
// unauthenticated :4317/:4318 ports and is bounded only by the message size —
// megabytes — so a megabyte of it in a log record is a second flood wearing the
// shape of one line.
const maxLoggedValueBytes = 96

// clipForLog renders a sender-supplied value for a log attribute: bounded, and
// cut on a rune boundary so a clipped UTF-8 sequence does not become a
// replacement character in whatever reads the line.
//
// promscrape and otlpingest bound their target- and sender-supplied values the
// same way; the bound is each package's own and the cut is internal/clip's.
func clipForLog(v string) string { return clip.Ellipsis(v, maxLoggedValueBytes) }

// reportUnnamed is the context half of kubescrape_service_graph_unnamed_spans_total.
// The counter says requests are missing from the graph; only a line can say
// WHOSE, and a resource with no service.name is exactly the one an operator
// cannot select by. Throttled to the condition — an unattributable sender pushes
// continuously — and quoting a fixed set of identity keys rather than the map.
func (p *Processor) reportUnnamed(attrs pcommon.Map, spans int) {
	if !p.unnamedWarn.Allow(unnamedWarnEvery) {
		return
	}
	args := []any{"spans", spans}
	for _, k := range unnamedHints {
		if v, ok := attrs.Get(k); ok {
			// The KEYS are ours (unnamedHints is a fixed list); the VALUES are
			// the sender's, and the sender reached an UNAUTHENTICATED port —
			// so they are clipped. The throttle above bounds how OFTEN this
			// line is written, never how LARGE it is, and the throttle KEY is
			// keyless (logdedupe.Throttle, one condition per Processor), so no
			// sender byte can silence anyone else's warning either.
			args = append(args, k, clipForLog(v.AsString()))
		}
	}
	p.log.Warn("spans arrived on a resource with no service.name, so they can name no graph node and their requests are on no edge at all; they still reach the collector",
		args...)
}

// Sweep retires every half-edge whose Wait has ELAPSED, in bounded passes. Safe
// to call periodically (and cheap when nothing is due); the caller owns the
// cadence.
//
// Everything due, not one budget's worth of it. The cadence its caller picks is
// a PROMISE about promotion delay — sweepInterval takes wait/2 so a promotable
// half-edge reaches the graph within 1.5x wait — and one bounded pass per tick
// keeps that promise only while fewer than expirePerHold halves are pending. Past
// that the sweep is a RATE (expirePerHold per tick), so the delay grows with the
// backlog instead of being bounded by it. The arithmetic at the defaults:
// MaxItems 10,000 over expirePerHold 1,024 is ten ticks, and at DefaultWait 10s
// the cadence is 5s, so the LAST due half-edge waited 45s past its deadline
// against the 15s the cadence promises — and an operator raising MaxItems made
// that worse without touching anything named like a deadline. The quiet shard is
// the ONLY case this entry point exists for, so nothing else corrects it.
//
// These sweeps are NOT what keeps up with ingest; Consume's per-batch pass is.
// They exist for the shard that has gone QUIET, where nothing is arriving to
// drive expiry and a client half that could still become a virtual-node edge
// would otherwise sit until the next busy batch — or, on a tier that quiesces
// overnight, until morning.
//
// What stays bounded is the LOCK HOLD: sweepDue spends the backlog in
// expirePerHold-sized passes, releasing the pairing mutex between them, because
// that mutex is the one every concurrent Consume needs and the one the sink
// runs under (see metrics.go's render strategy).
func (p *Processor) Sweep() { p.sweepDue(p.now()) }

// SweepAll is Sweep under the name the SHUTDOWN path calls it by
// (cmd/kubescrape-agent's stop sequence, after the receivers have joined). The
// two were different functions while the ticker's pass was a single bounded one;
// they are one function now, and the names are kept because the two call sites
// mean different things by it: the ticker is keeping a promotion-delay promise,
// the shutdown is taking the last chance to promote half-edges whose wait
// elapsed — a bounded pass there silently discarded the remainder on a busy
// tier, which the shutdown path claims to emit.
func (p *Processor) SweepAll() { p.sweepDue(p.now()) }

// sweepDue retires everything due at now: expireInPasses with no total budget.
//
// That cannot spin: a short pass means nothing more is due at THIS now, and
// nothing inserted while it runs can become due at that same now (an entry is
// stamped its inserter's clock + wait, and wait is always positive —
// Config.wait parses it under config.Positive and falls back to DefaultWait).
func (p *Processor) sweepDue(now time.Time) { p.store.expireInPasses(now, math.MaxInt) }

// spanSide classifies a span kind into the half of an edge it can be, and
// whether the kind ALONE already settles the connection type. ok is false for
// the kinds that are not a call between two services at all.
//
// It is a function rather than a switch inside observe because the unnamed-
// resource arm of Consume has to answer the same question — "would pairing have
// used this span?" — and answering it differently there made the counter and
// the graph measure different populations.
func spanSide(k ptrace.SpanKind) (side edgeSide, conn ConnectionType, ok bool) {
	switch k {
	case ptrace.SpanKindClient:
		return sideClient, ConnectionUnknown, true
	case ptrace.SpanKindProducer:
		// A producer is the client half of an asynchronous hop, and the pair
		// being a messaging one is known from the KIND alone — the consumer
		// says the same thing, so either half classifies the edge. (A db.*
		// attribute on the producer overrides it below, and pendingEdge.merge
		// lets that database win whichever half arrives first.)
		return sideClient, ConnectionMessagingSystem, true
	case ptrace.SpanKindServer:
		return sideServer, ConnectionUnknown, true
	case ptrace.SpanKindConsumer:
		return sideServer, ConnectionMessagingSystem, true
	}
	// INTERNAL and UNSPECIFIED spans are not a call between two services;
	// pairing them would invent edges inside a single process.
	return 0, ConnectionUnknown, false
}

func (p *Processor) observe(span ptrace.Span, resAttrs pcommon.Map, svc string, now time.Time) {
	side, conn, ok := spanSide(span.Kind())
	if !ok {
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
	// A CLIENT half with no span id of its own is refused, and that is what
	// keeps the paragraph above safe. Its key would be (trace, zero) — the very
	// key every ROOT SERVER span of that trace uses — so a malformed SDK's
	// zero-id client span would pair with an unrelated ingress hop and invent an
	// edge between two services that never called each other. (A zero-id CLIENT
	// span is unusable anyway: its span id is what a server half looks itself up
	// by, so nothing could ever legitimately pair with it.) Counted, not
	// silent — a whole SDK emitting these would otherwise look like a quiet
	// graph rather than a broken one.
	if side == sideClient && sid.IsEmpty() {
		p.store.countUnkeyable()
		return
	}

	spanAttrs := span.Attributes()
	h := halfSpan{
		service:    svc,
		seconds:    cumagg.SpanSeconds(span),
		failed:     span.Status().Code() == ptrace.StatusCodeError,
		connection: conn,
		traceID:    tid,
		// This half's OWN span id — NOT sid, which for a server half is its
		// parent's. The exemplar on a side's latency histogram must point at
		// the span that measured that latency.
		spanID: span.SpanID(),
	}

	// Peer attributes, in the configured precedence order: the FIRST one
	// present names the far side. Both sides are scanned, not just the client
	// (Tempo scans only the client): a server span carrying peer.service is how
	// an uninstrumented CALLER — a browser, an external client, an ingress —
	// gets a name, which is the whole virtual_node "client" case.
	//
	// The db.* attributes are skipped on a server half — for NAMING here and
	// for classification below alike, since both would read the same attribute
	// the same wrong way round: db.* describes the span's own CALLEE, while
	// the side missing from a server half is its CALLER, so honouring it there
	// would name the far side backwards ("postgresql called us") and label
	// every application edge INTO a database-using service as a database call.
	for i, a := range p.peerAttrs {
		if p.peerIsDB[i] && side == sideServer {
			continue
		}
		v := cumagg.AttrStr(spanAttrs, a)
		if v == "" {
			continue
		}
		// Not cut here: the pairing store cuts it WITH A COPY only if it keeps
		// this half (edgeStore.insert) — the peer names a virtual node only
		// once a half expires, so the half that completes a pair never needs it.
		h.peer = v
		if p.peerIsDB[i] {
			h.connection = ConnectionDatabase
		}
		break
	}
	// Connection-type precedence, settled here for the whole CLIENT SIDE of
	// the call (side == sideClient is exactly the CLIENT and PRODUCER kinds —
	// the switch above maps no other kind to it): a databaseAttrs attribute is
	// AUTHORITATIVE over the kind-derived classification. The kind supplies a
	// DEFAULT inferred from the call's shape (producer/consumer =>
	// messaging_system); a db.* attribute is the instrumentation's explicit
	// statement about what the callee IS, and the explicit statement wins —
	// Tempo's rule too, whose database attributes overwrite to database on the
	// client-side kinds. Gating this on ConnectionUnknown instead made the
	// winner a function of WHICH spelling the SDK used: a db-flagged peer
	// attribute (db.name alone) already overwrote messaging in the scan above,
	// while the same statement spelled peer.service + db.*, or spelled only in
	// the post-1.30 db.system.name, left messaging standing — three spellings
	// of "my callee is a database", three different connection_types on a
	// producer span. Database itself is never overwritten back: the != guard
	// also skips the four-attribute scan once the peer scan settled it, and
	// that scan's own assignment still matters, being the only classifier for
	// an operator-configured db.* peer attribute outside databaseAttrs. The
	// server side is excluded for the scan's reason above: a database emits no
	// spans at all, so a server-side half carrying db.* is a service that
	// TALKS to one, never the database itself.
	if side == sideClient && h.connection != ConnectionDatabase && namesDatabase(spanAttrs) {
		h.connection = ConnectionDatabase
	}

	// dims is the borrowed scratch handed to upsert; nil when nothing is
	// configured, which is the default.
	var dims []EdgeDimension
	if len(p.dims) > 0 {
		// Stack scratch: upsert COPIES the dimensions and never retains the
		// slice, so for the usual handful of configured dimensions this never
		// reaches the heap (BenchmarkConsumePairWithDimensions is the alarm —
		// it must show zero allocations, the store's join buffer and free list
		// having absorbed the per-edge cost too).
		var scratch [8]EdgeDimension
		var ds []EdgeDimension
		if len(p.dims) <= len(scratch) {
			ds = scratch[:0]
		} else {
			ds = make([]EdgeDimension, 0, len(p.dims))
		}
		names := p.clientDims
		if side == sideServer {
			names = p.serverDims
		}
		// Walked in the CONFIGURED order, which is what makes the edge's
		// dimension sequence canonical without anyone sorting it: see joinDims.
		for i, d := range p.dims {
			// Span first, the resource as the fallback: cumagg.DimValue, the
			// precedence spanmetrics resolves its dimensions by too.
			v := cumagg.DimStr(spanAttrs, resAttrs, d)
			if v == "" {
				// Absent dimensions are simply not carried; the metric layer
				// renders a missing key as the empty label value, so recording
				// "" here would only cost a map entry per edge.
				continue
			}
			// Uncut, for the reason the peer is: the store cuts what it KEEPS
			// (pendingEdge.setDims), and the sink cuts what reaches a series,
			// so a clone here allocated per span for the completing half,
			// whose values are dropped the moment the edge is emitted.
			ds = append(ds, EdgeDimension{Name: names[i], Value: v})
		}
		dims = ds
	}

	p.store.upsert(now, makeEdgeKey(tid, sid), side, h, dims)
}

// namesDatabase reports whether the span carries any of the attributes that
// identify a database callee.
//
// ONE walk of the attributes, gated on the "db." prefix, rather than four
// Get calls. A pcommon.Map is a SLICE and Get is a linear scan of it, so the
// four-Get shape walked a realistically instrumented span — otelhttp emits
// fourteen attributes — four times over, and did it for every CLIENT and
// PRODUCER span the tier receives. The prefix check is a three-byte compare
// against a key that, on a span that names no database, never matches, so the
// full comparison against databaseAttrs is reached for essentially no key.
//
// The gate must stay a strict SUPERSET of databaseAttrs, exactly as
// agent/logscrub's prefilters must stay supersets of their regexes: a member
// spelled without the prefix would be silently unreachable, and the failure —
// a database edge classified as a plain service call — looks like a graph that
// is merely uninformative. The gate is spelled with dbAttrPrefix itself, not
// with its bytes unrolled, so TestDatabaseAttrsAllCarryThePrefix pins the gate
// that runs; TestEveryDatabaseAttrClassifiesAlone pins it end to end.
func namesDatabase(attrs pcommon.Map) bool {
	for k := range attrs.All() {
		if strings.HasPrefix(k, dbAttrPrefix) && slices.Contains(databaseAttrs, k) {
			return true
		}
	}
	return false
}

// --- helpers ---
//
// The three that used to live here are gone: spanAttrStr, spanSeconds and
// retainDimValue are cumagg.AttrStr, cumagg.SpanSeconds and cumagg.Retain,
// called by their shared names above. Each existed twice — here and in
// agent/spanmetrics, under a different name — behind a comment arguing that
// sharing three lines would couple two packages that otherwise shared nothing.
// They share a state machine now, and the truncation one had by then had the
// same retention bug (a reslice pinning the sender's whole string) found and
// fixed in each copy separately.
