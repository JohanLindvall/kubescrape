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
}

type dimKV struct{ name, value string }

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
	dims                         []dimKV
	// expiresAt is stamped at INSERT and never refreshed by the second half:
	// every entry gets the same Wait, so insertion order IS expiry order and
	// the expiry pass needs no ordering work. (Refreshing it on the second
	// arrival would break that and turn expiry into a scan.)
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
}

// newEdgeStore builds the store from an already-defaulted config.
func newEdgeStore(cfg Config, onEdge func(Edge)) *edgeStore {
	return &edgeStore{
		wait:     cfg.Wait,
		maxItems: cfg.MaxItems,
		onEdge:   onEdge,
		items:    make(map[edgeKey]*pendingEdge),
	}
}

// upsert records one half of a request under k. When it completes the edge, the
// edge is handed to onEdge and the entry retires.
//
// The obs counters are bumped OUTSIDE the store's mutex: a Registry counter
// takes the metric store's own lock, and a shard's pairing path should not hold
// two locks to do its bookkeeping (spanmetrics unlocks before its drop counter
// for the same reason). The Stats fields, which callers read as a consistent
// snapshot, are maintained inside.
func (s *edgeStore) upsert(now time.Time, k edgeKey, side edgeSide, h halfSpan, dims []dimKV) {
	if s.insert(now, k, side, h, dims) {
		obs.ServiceGraphStoreFull.Inc()
	}
}

// insert is upsert under the mutex; it reports whether the span was refused
// because the store is full.
func (s *edgeStore) insert(now time.Time, k edgeKey, side edgeSide, h halfSpan, dims []dimKV) (refused bool) {
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
	e.merge(side, h, dims)
	if e.haveClient && e.haveServer {
		// Build the Edge BEFORE retiring: retirement clears the entry and puts
		// it on the free list, so reading it afterwards would read the next
		// request's half.
		edge := e.edge()
		s.counts.Completed++
		s.retire(e)
		s.onEdge(edge)
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
		} else {
			s.counts.Unpaired++
		}
		s.retire(e)
		if promoted {
			s.onEdge(edge)
		}
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

// merge folds one arriving half into the entry.
func (e *pendingEdge) merge(side edgeSide, h halfSpan, dims []dimKV) {
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
	switch side {
	case sideClient:
		e.clientService, e.clientSeconds, e.haveClient = h.service, h.seconds, true
	default:
		e.serverService, e.serverSeconds, e.haveServer = h.service, h.seconds, true
	}
	// Both sides' dimensions accumulate: the names are already client_/server_
	// prefixed, so a dimension carried by whichever side had it lands on the
	// edge exactly once (Tempo's rule).
	e.dims = append(e.dims, dims...)
}

// edge materializes the Edge handed across the sink seam. The Dimensions map is
// built here and only here — once per completed request rather than per span —
// so the map allocation is a cold-path cost, and the Edge never aliases the
// entry, which is about to be recycled.
func (e *pendingEdge) edge() Edge {
	out := Edge{
		ClientService: e.clientService,
		ServerService: e.serverService,
		Connection:    e.connection,
		ClientSeconds: e.clientSeconds,
		ServerSeconds: e.serverSeconds,
		HaveClient:    e.haveClient,
		HaveServer:    e.haveServer,
		Failed:        e.failed,
	}
	if len(e.dims) > 0 {
		out.Dimensions = make(map[string]string, len(e.dims))
		for _, d := range e.dims {
			out.Dimensions[d.name] = d.value
		}
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
	out := e.edge()
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
	// array: the values are strings pointing into the sender's span payload,
	// and keeping them reachable from the free list would pin whole OTLP
	// batches long after they were forwarded. Truncating alone does not drop
	// the references.
	dims := e.dims
	for i := range dims {
		dims[i] = dimKV{}
	}
	*e = pendingEdge{dims: dims[:0]}
	e.next = s.free
	s.free = e
}
