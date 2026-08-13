package servicegraph

import (
	"sync"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"

	"github.com/JohanLindvall/kubescrape/internal/obs"
)

// The pairing store holds HALF-EDGES: one side of a request that has arrived
// and is waiting for the other. The model is Tempo's verbatim in shape — key =
// trace id + the span id both sides agree on, one Wait per half, complete on
// the second arrival, expire-or-promote after that — with three deliberate
// differences, each explained at the code that makes it:
//
//   - the key is a fixed [24]byte array rather than Tempo's hex string, so the
//     hot path never allocates a key;
//   - the expiry order is an intrusive FIFO rather than a container/list, so an
//     entry costs one allocation instead of two;
//   - retired entries go on a free list, so a steady stream of paired requests
//     allocates nothing at all.
//
// Every method takes the store's own mutex, so the store is safe for the
// concurrent ingest goroutines. The sink is called WITH THAT MUTEX HELD (Tempo
// does the same): a sink must therefore never call back into the processor.

// edgeKey identifies one request's two halves: the trace id plus the span id
// the two sides AGREE on — the client's own span id, which the server span
// carries as its parent.
//
// A fixed-size array, not Tempo's hex string key (its buildKey concatenates two
// hex.EncodeToString results): an array is comparable, so it is a map key
// directly, and neither the lookup nor the insert allocates. Tempo pays two
// string allocations plus the hex encoding per span, on the hottest path in the
// package.
type edgeKey [24]byte

func makeEdgeKey(tid pcommon.TraceID, sid pcommon.SpanID) edgeKey {
	var k edgeKey
	copy(k[:len(tid)], tid[:])
	copy(k[len(tid):], sid[:])
	return k
}

// edgeSide names which half of a request a span is. A PRODUCER counts as a
// client and a CONSUMER as a server: a messaging hop is still one caller and
// one callee, only asynchronous — Tempo folds them the same way and marks the
// connection messaging_system.
type edgeSide uint8

const (
	sideClient edgeSide = iota
	sideServer
)

// Virtual-node marker values. Tempo exposes these as the `virtual_node`
// dimension, and they name the side that was SYNTHESIZED (from a peer
// attribute), not the side that was observed.
const (
	virtualNodeServer = "server"
	virtualNodeClient = "client"
)

// halfSpan is one side of a request as extracted from a span. It is passed BY
// VALUE and nothing in it is retained.
//
// The configured dimensions are NOT a field here: they travel as a separate
// argument to upsert. The strings in this struct legitimately escape (they are
// stored on the heap entry), and escape analysis decides that for a parameter
// as a WHOLE — a dimension slice inside it would inherit the verdict and put
// the caller's stack scratch on the heap, one allocation per span. As its own
// parameter it is only ever read, so it stays on the caller's stack.
type halfSpan struct {
	service string
	seconds float64
	failed  bool
	// connection is the classification THIS side implies: messaging_system for
	// a producer/consumer span, database when the span names a database. The
	// stored half keeps the first non-empty one it is given.
	connection ConnectionType
	// peer names the far side of the call, resolved from the configured peer
	// attributes in precedence order. It matters only if this half expires
	// unpaired, when it becomes the synthesized virtual node.
	peer string
	// traceID and spanID are what an exemplar needs, and nothing else is
	// carried for one: the trace id is the same on both halves by construction
	// (see Edge.TraceID) and the span id is this half's own, so each side's
	// histogram can point at the span that measured it.
	traceID pcommon.TraceID
	spanID  pcommon.SpanID
}

// pendingEdge is a half-edge awaiting its partner. It accumulates both sides
// because either may arrive first — the client half is the common case, but a
// server span reaching this shard before the client's does is ordinary
// network reordering, not an error.
type pendingEdge struct {
	key                          edgeKey
	clientService, serverService string
	clientSeconds, serverSeconds float64
	haveClient, haveServer       bool
	failed                       bool
	connection                   ConnectionType
	peer                         string
	traceID                      pcommon.TraceID
	clientSpanID, serverSpanID   pcommon.SpanID
	// dims holds the dimensions of the ONE side recorded so far. An entry never
	// holds both: the arrival that completes the pair emits and retires the
	// entry on the spot, so the second side's dimensions are joined straight
	// onto the outgoing Edge (see joinDims) and never stored.
	dims []EdgeDimension
	// expiresAt is stamped at INSERT and never refreshed by the second half:
	// every entry gets the same Wait, so insertion order is expiry order and
	// the expiry pass needs no ordering work. (Refreshing it on the second
	// arrival would break that and turn expiry into a scan.)
	//
	// "Is" up to one skew, precisely: Consume reads the clock ONCE PER BATCH
	// and outside this mutex (a syscall per span is what that buys), so two
	// concurrent pushes can enter the FIFO in the opposite order to their
	// stamps — the loser of the clock read winning the lock. The skew is the
	// clock-read-to-lock-acquire gap, microseconds against a Wait measured in
	// seconds, and its only consequence is that sweep's break-at-the-first-
	// not-due leaves an already-due entry for the next pass. It is written down
	// rather than removed: the fix would be stamping under the lock, i.e. the
	// per-span clock read this design deliberately does not pay.
	expiresAt time.Time
	// prev/next thread the expiry FIFO (oldest first) while the entry is live,
	// and the free list once it is retired.
	prev, next *pendingEdge
}

// Stats are the pairing store's counters, snapshotted under its mutex.
//
// They are plain values rather than internal/obs metrics because the metric
// surface of this package belongs to metrics.go, which publishes them the way
// obs.RegisterBufferStats publishes the disk buffer's — every bound in Config
// has a counter that moves when it binds, so a too-small Wait or MaxItems is
// visible rather than silent.
type Stats struct {
	// Items is the number of half-edges currently awaiting a partner.
	Items int
	// Completed counts edges whose two halves both arrived within Wait.
	Completed uint64
	// VirtualNode counts half-edges that expired unpaired but carried a peer
	// attribute naming the far side, so they still reached the graph.
	VirtualNode uint64
	// Unpaired counts half-edges that expired with nothing to promote them:
	// the partner never arrived (dropped span, uninstrumented peer with no
	// peer attribute, a Wait shorter than the request) and nothing named it.
	Unpaired uint64
	// Dropped counts spans refused because the store was at MaxItems.
	Dropped uint64
	// Unkeyable counts spans that could not be keyed at all (no trace id).
	Unkeyable uint64
}

type edgeStore struct {
	wait     time.Duration
	maxItems int
	onEdge   func(Edge)

	mu         sync.Mutex
	items      map[edgeKey]*pendingEdge
	head, tail *pendingEdge // expiry FIFO, oldest first
	free       *pendingEdge // retired entries, reused by newEntry
	counts     Stats
	// dimBuf is the scratch a completing pair's two halves are joined into (see
	// joinDims). One per store rather than one per entry: only the edge being
	// handed to the sink right now needs it, and the mutex that makes the sink
	// call safe is the same one that makes this buffer safe.
	dimBuf []EdgeDimension
}

// newEdgeStore builds the store from an already-defaulted config plus the
// RESOLVED pairing window (Config.Wait is a string — see Config.wait — so the
// caller parses it once at construction rather than per store method).
func newEdgeStore(cfg Config, wait time.Duration, onEdge func(Edge)) *edgeStore {
	return &edgeStore{
		wait:     wait,
		maxItems: cfg.MaxItems,
		onEdge:   onEdge,
		items:    make(map[edgeKey]*pendingEdge),
	}
}

// upsert records one half of a request under k. When it completes the edge, the
// edge is handed to onEdge and the entry retires.
//
// The counters THIS store bumps (ServiceGraphStoreFull here, ServiceGraphExpired
// in expire) are bumped OUTSIDE the pairing mutex: a Registry counter takes the
// metric store's own lock, and holding the pairing mutex across it stalls every
// concurrent Consume for that long. The Stats fields, which callers read as a
// consistent snapshot, are maintained inside.
//
// This is NOT a claim that the pairing path holds one lock at a time — it holds
// the pairing mutex across sink.Record by design (see metrics.go's render
// strategy), so the cardinality-cap refusal is counted inside
// cumagg.AdmitLocked, under the cumagg store's lock, with this one held.
// cumagg.Lock owns that rule ("nothing here ever takes another lock while
// holding this one except the Counter it was given"); the same is true of
// spanmetrics, whose drop counter moved INTO AdmitLocked with the extraction.
func (s *edgeStore) upsert(now time.Time, k edgeKey, side edgeSide, h halfSpan, dims []EdgeDimension) {
	if s.insert(now, k, side, h, dims) {
		obs.ServiceGraphStoreFull.Inc()
	}
}

// insert is upsert under the mutex; it reports whether the span was refused
// because the store is full.
func (s *edgeStore) insert(now time.Time, k edgeKey, side edgeSide, h halfSpan, dims []EdgeDimension) (refused bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	e, ok := s.items[k]
	if !ok {
		// At the cap we refuse the INSERT and count it. Never evict a live
		// half to make room: the entry we would evict is a request whose
		// partner may be one span away, so making room that way silently
		// destroys an edge that was about to complete — trading a counted loss
		// for an uncounted one. (Tempo's store leaves a `todo: try to evict
		// expired items` here; the incremental expiry in Consume is that, done
		// on the way in rather than as a scan on the way out.)
		if len(s.items) >= s.maxItems {
			s.counts.Dropped++
			return true
		}
		e = s.newEntry()
		e.key = k
		e.expiresAt = now.Add(s.wait)
		s.items[k] = e
		s.pushBack(e)
	}
	e.merge(side, h)
	if e.haveClient && e.haveServer {
		// The completing half's dimensions are joined onto the held half's and
		// never stored — the entry is about to go.
		edge := e.edge(s.joinDims(side, e.dims, dims))
		s.counts.Completed++
		// The sink runs BEFORE retirement, not after: the Edge's Dimensions
		// alias buffers that retirement clears (and hands to the next request),
		// so reading them afterwards would read the next half-edge's. Nothing
		// else on the Edge aliases the entry — the strings are values — but the
		// slice does, which is the whole reason a sink must not retain an Edge.
		s.onEdge(edge)
		s.dimBuf = clearDims(s.dimBuf)
		s.retire(e)
	} else {
		// Not complete: this side is the one the entry holds until its partner
		// arrives.
		e.setDims(dims)
	}
	return false
}

// expire retires half-edges whose Wait has elapsed, at most budget of them per
// call, and returns how many it retired. A promoted one (a peer attribute named
// the far side) reaches the sink as a virtual-node edge; the rest are counted
// unpaired.
//
// The budget is the whole point: this runs on the ingest path (see Consume) and
// under the same mutex every concurrent Consume needs, so an unbounded drain of
// a MaxItems-deep store would stall the shard's ingest for as long as it took.
// The list is expiry-ordered, so the not-due case costs ONE comparison — an
// O(store) scan never happens, here or anywhere else.
func (s *edgeStore) expire(now time.Time, budget int) int {
	n := s.sweep(now, budget)
	if n > 0 {
		// Counted whether or not the half was promoted: both are "the partner
		// never arrived", which is what the counter's rate answers. The Stats
		// split (Unpaired vs VirtualNode) says which way each one went.
		obs.ServiceGraphExpired.Add(float64(n))
	}
	return n
}

// sweep is expire under the mutex (see upsert on why the counter bump is not).
func (s *edgeStore) sweep(now time.Time, budget int) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	n := 0
	for n < budget {
		e := s.head
		if e == nil || now.Before(e.expiresAt) {
			break
		}
		edge, promoted := e.promote()
		if promoted {
			s.counts.VirtualNode++
			// Before retirement, for the same reason as in insert: the Edge's
			// Dimensions alias the entry's buffer.
			s.onEdge(edge)
		} else {
			s.counts.Unpaired++
		}
		s.retire(e)
		n++
	}
	return n
}

// countUnkeyable records a span the processor could not key at all.
func (s *edgeStore) countUnkeyable() {
	s.mu.Lock()
	s.counts.Unkeyable++
	s.mu.Unlock()
}

func (s *edgeStore) stats() Stats {
	s.mu.Lock()
	defer s.mu.Unlock()
	st := s.counts
	st.Items = len(s.items)
	return st
}

// merge folds one arriving half into the entry. The half's dimensions are NOT
// merged here — see setDims and joinDims, which decide between storing and
// emitting them.
func (e *pendingEdge) merge(side edgeSide, h halfSpan) {
	// The first non-empty classification sticks. A later half can only ever
	// supply the same one (both spans of a messaging hop are producer/consumer)
	// or none, and letting a plain SERVER span clear the database/messaging
	// classification its client established would erase the only signal
	// Grafana colours the edge by.
	if e.connection == ConnectionUnknown {
		e.connection = h.connection
	}
	e.failed = e.failed || h.failed
	if e.peer == "" {
		e.peer = h.peer
	}
	// The first half to arrive sets the trace id; the second carries the same
	// one (it is half the key both were looked up under), so there is nothing
	// to reconcile — and a promoted half-edge has only the one anyway.
	if e.traceID.IsEmpty() {
		e.traceID = h.traceID
	}
	switch side {
	case sideClient:
		e.clientService, e.clientSeconds, e.haveClient = h.service, h.seconds, true
		e.clientSpanID = h.spanID
	default:
		e.serverService, e.serverSeconds, e.haveServer = h.service, h.seconds, true
		e.serverSpanID = h.spanID
	}
}

// setDims records the dimensions of the one side this entry holds.
//
// It REPLACES rather than appends. An entry only ever holds one side (the other
// completes the edge and retires it), so the only way here twice is the same
// half arriving twice — a duplicate push, which this repo's at-least-once
// export paths make ordinary. Appending would then carry every dimension twice,
// and since the metric layer keys a series on the dimension SEQUENCE that would
// mint a second series rendering a byte-identical label set: a duplicate series
// in one payload, which is a conflict downstream rather than extra detail. The
// rest of merge is last-wins for the same arrival; this matches it.
func (e *pendingEdge) setDims(in []EdgeDimension) {
	if len(in) == 0 && len(e.dims) == 0 {
		return
	}
	was := len(e.dims)
	// Copied, never aliased: `in` is the caller's stack scratch (see halfSpan on
	// why the dimensions travel as their own parameter), and retaining it would
	// both dangle and force that scratch onto the heap once per span.
	e.dims = append(e.dims[:0], in...)
	if was > len(e.dims) {
		// A shorter replacement leaves the tail's strings reachable from the
		// entry, which may now sit here for a whole Wait pinning an OTLP batch.
		clear(e.dims[len(e.dims):was])
	}
}

// joinDims lays a completing request's two halves' dimensions out in ONE
// deterministically ordered slice: the client half's (in the processor's
// configured order) followed by the server half's.
//
// The order is the point. The metric layer keys a series on this sequence, so
// it has to be a function of the dimension SET and not of which half happened
// to arrive first — an arrival-ordered sequence would give the same edge two
// keys, and the two would render byte-identical label sets into one payload.
// Both halves' own order is already fixed (the processor walks its configured
// dimensions in order and prefixes them at construction), so client-then-server
// is canonical without a sort: that is the sort the metric layer used to pay
// per completed request, deleted rather than moved.
//
// The result is the store's scratch, valid until the sink returns (insert
// clears it immediately after). `arriving` is COPIED rather than returned
// as-is even when it would do: it is the caller's stack scratch, and letting it
// reach the sink through the Edge would make escape analysis heap-allocate it
// once per span — the cost this whole path is shaped to avoid.
func (s *edgeStore) joinDims(side edgeSide, held, arriving []EdgeDimension) []EdgeDimension {
	if len(held) == 0 && len(arriving) == 0 {
		return nil
	}
	out := s.dimBuf[:0]
	if side == sideClient { // the arriving half is the client's
		out = append(out, arriving...)
		out = append(out, held...)
	} else {
		out = append(out, held...)
		out = append(out, arriving...)
	}
	s.dimBuf = out
	return out
}

// clearDims drops a dimension scratch's string references — they point into an
// OTLP payload that is otherwise free to be collected — and empties it for
// reuse. Truncating alone would keep the strings reachable from the backing
// array.
func clearDims(d []EdgeDimension) []EdgeDimension {
	clear(d)
	return d[:0]
}

// edge materializes the Edge handed across the sink seam, over the dimension
// slice the caller decided on (the joined pair, or the single held half's for a
// promotion). The Edge BORROWS that slice; see Edge.Dimensions.
func (e *pendingEdge) edge(dims []EdgeDimension) Edge {
	out := Edge{
		ClientService: e.clientService,
		ServerService: e.serverService,
		Connection:    e.connection,
		ClientSeconds: e.clientSeconds,
		ServerSeconds: e.serverSeconds,
		HaveClient:    e.haveClient,
		HaveServer:    e.haveServer,
		Failed:        e.failed,
		Dimensions:    dims,
		TraceID:       e.traceID,
		ClientSpanID:  e.clientSpanID,
		ServerSpanID:  e.serverSpanID,
	}
	return out
}

// promote turns an expiring half-edge into a virtual-node edge when a peer
// attribute named the side that never arrived. Uninstrumented dependencies —
// managed databases, third-party APIs, browsers — are the majority of the
// interesting far sides on a real graph, so an unpromoted expiry is a hole in
// it, not merely a missed sample.
func (e *pendingEdge) promote() (Edge, bool) {
	// A half with BOTH sides would have completed on arrival, and one with
	// neither cannot exist (an entry is only created by an arriving half); the
	// guard keeps promote total anyway, since it decides what the sink sees.
	if e.peer == "" || e.haveClient == e.haveServer {
		return Edge{}, false
	}
	// One side only, so the held half's dimensions are already in their
	// configured order and need no joining.
	out := e.edge(e.dims)
	if e.haveClient {
		out.ServerService = e.peer
		out.VirtualNode = virtualNodeServer
	} else {
		out.ClientService = e.peer
		out.VirtualNode = virtualNodeClient
	}
	// DEVIATION from Tempo, which overwrites the connection type with
	// virtual_node unconditionally: a classification the spans actually
	// carried (database, messaging_system) survives promotion. Every database
	// edge is virtual by nature — a managed database emits no spans — so
	// overwriting would delete the database classification from the graph
	// entirely, and the fact that the far side was synthesized is already
	// carried by the virtual_node dimension. An UNCLASSIFIED edge still
	// becomes virtual_node, which is the case the contract names.
	if out.Connection == ConnectionUnknown {
		out.Connection = ConnectionVirtualNode
	}
	return out, true
}

// --- entry lifecycle: intrusive FIFO + free list ---

// newEntry takes a retired entry when one is available. Half-edges are created
// and destroyed once per request — at a few thousand requests per second per
// shard, allocating each one would make this package the agent's largest
// garbage producer for no reason. The free list is bounded by the peak live
// count, itself bounded by MaxItems.
func (s *edgeStore) newEntry() *pendingEdge {
	e := s.free
	if e == nil {
		return &pendingEdge{}
	}
	s.free = e.next
	e.next = nil
	return e
}

func (s *edgeStore) pushBack(e *pendingEdge) {
	e.prev, e.next = s.tail, nil
	if s.tail != nil {
		s.tail.next = e
	} else {
		s.head = e
	}
	s.tail = e
}

// retire unlinks the entry, drops it from the index and recycles it.
func (s *edgeStore) retire(e *pendingEdge) {
	if e.prev != nil {
		e.prev.next = e.next
	} else {
		s.head = e.next
	}
	if e.next != nil {
		e.next.prev = e.prev
	} else {
		s.tail = e.prev
	}
	delete(s.items, e.key)

	// Clear the dimension slice ELEMENT BY ELEMENT before reusing its backing
	// array (clearDims): the values are strings pointing into the sender's span
	// payload, and keeping them reachable from the free list would pin whole
	// OTLP batches long after they were forwarded. Truncating alone does not
	// drop the references.
	*e = pendingEdge{dims: clearDims(e.dims)}
	e.next = s.free
	s.free = e
}
