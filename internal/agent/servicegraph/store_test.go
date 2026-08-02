package servicegraph

import (
	"maps"
	"slices"
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"

	"github.com/JohanLindvall/kubescrape/internal/obs"
)

// Fixed test clock: every expiry decision is driven from it, never from sleeps
// (the store takes `now` as an argument precisely so tests can do this).
var t0 = time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)

func traceID(n byte) pcommon.TraceID {
	var id pcommon.TraceID
	id[15] = n
	return id
}

func spanID(n byte) pcommon.SpanID {
	var id pcommon.SpanID
	id[7] = n
	return id
}

// newTestStore returns a store with cfg's defaults filled in and a slice
// collecting everything that crosses the sink seam.
func newTestStore(t *testing.T, cfg Config) (*edgeStore, *[]Edge) {
	t.Helper()
	var got []Edge
	st := newEdgeStore(cfg.withDefaults(), func(e Edge) { got = append(got, cloneEdge(e)) })
	return st, &got
}

// cloneEdge copies the borrowed dimension slice out of an Edge so a test sink
// may RETAIN it — which no production sink does (see Edge.Dimensions: the store
// recycles the buffer the moment Record returns). Without this every retained
// edge would read back with its dimensions zeroed, which is exactly the bug the
// contract exists to make impossible to hit by accident.
func cloneEdge(e Edge) Edge {
	e.Dimensions = append([]EdgeDimension(nil), e.Dimensions...)
	return e
}

// dimsOf is the assertion view: order is pinned separately (see
// TestStoreDimensionOrderIsArrivalIndependent), and every other test cares only
// about which pairs are present.
func dimsOf(e Edge) map[string]string {
	if len(e.Dimensions) == 0 {
		return nil
	}
	m := make(map[string]string, len(e.Dimensions))
	for _, d := range e.Dimensions {
		m[d.Name] = d.Value
	}
	return m
}

func TestStoreKeyIsTraceAndSpan(t *testing.T) {
	k := makeEdgeKey(traceID(7), spanID(9))
	if k[15] != 7 || k[23] != 9 {
		t.Fatalf("key = %v, want trace id in [0:16) and span id in [16:24)", k)
	}
	if k == makeEdgeKey(traceID(7), spanID(8)) {
		t.Fatal("keys differing in the span id collided")
	}
	if k == makeEdgeKey(traceID(6), spanID(9)) {
		t.Fatal("keys differing in the trace id collided")
	}
}

// The two halves may arrive in either order — a server span reaching the shard
// first is ordinary reordering, not an error — and the completed edge must be
// identical either way.
func TestStorePairsInEitherOrder(t *testing.T) {
	client := halfSpan{service: "checkout", seconds: 0.30}
	server := halfSpan{service: "orders", seconds: 0.25}

	for _, tc := range []struct {
		name  string
		first edgeSide
	}{
		{"client first", sideClient},
		{"server first", sideServer},
	} {
		t.Run(tc.name, func(t *testing.T) {
			st, got := newTestStore(t, Config{})
			k := makeEdgeKey(traceID(1), spanID(1))
			if tc.first == sideClient {
				st.upsert(t0, k, sideClient, client, nil)
				st.upsert(t0, k, sideServer, server, nil)
			} else {
				st.upsert(t0, k, sideServer, server, nil)
				st.upsert(t0, k, sideClient, client, nil)
			}
			if len(*got) != 1 {
				t.Fatalf("edges = %d, want 1", len(*got))
			}
			e := (*got)[0]
			if e.ClientService != "checkout" || e.ServerService != "orders" {
				t.Fatalf("services = %q -> %q", e.ClientService, e.ServerService)
			}
			if !e.HaveClient || !e.HaveServer {
				t.Fatalf("have flags = %v/%v, want both", e.HaveClient, e.HaveServer)
			}
			if e.ClientSeconds != 0.30 || e.ServerSeconds != 0.25 {
				t.Fatalf("durations = %v/%v, want each side's own", e.ClientSeconds, e.ServerSeconds)
			}
			if e.Connection != ConnectionUnknown || e.VirtualNode != "" {
				t.Fatalf("connection = %q, virtual node = %q, want a plain edge", e.Connection, e.VirtualNode)
			}
			// The entry must be gone: a completed edge that stays in the store
			// would expire again later and emit a second, half-empty edge.
			if st.stats().Items != 0 {
				t.Fatalf("items = %d after completion, want 0", st.stats().Items)
			}
			if st.stats().Completed != 1 {
				t.Fatalf("completed = %d, want 1", st.stats().Completed)
			}
		})
	}
}

// Independent requests must not interfere: the key is per (trace, span), so two
// calls in one trace are two edges.
func TestStorePairsIndependentKeys(t *testing.T) {
	st, got := newTestStore(t, Config{})
	k1 := makeEdgeKey(traceID(1), spanID(1))
	k2 := makeEdgeKey(traceID(1), spanID(2))
	st.upsert(t0, k1, sideClient, halfSpan{service: "a"}, nil)
	st.upsert(t0, k2, sideClient, halfSpan{service: "b"}, nil)
	if st.stats().Items != 2 {
		t.Fatalf("items = %d, want 2", st.stats().Items)
	}
	st.upsert(t0, k2, sideServer, halfSpan{service: "b2"}, nil)
	if len(*got) != 1 || (*got)[0].ClientService != "b" || (*got)[0].ServerService != "b2" {
		t.Fatalf("edges = %+v, want only k2 completed", *got)
	}
	if st.stats().Items != 1 {
		t.Fatalf("items = %d, want the k1 half still pending", st.stats().Items)
	}
}

func TestStoreFailedFromEitherSide(t *testing.T) {
	for _, tc := range []struct {
		name                 string
		clientBad, serverBad bool
		want                 bool
	}{
		{"neither", false, false, false},
		{"client errored", true, false, true},
		{"server errored", false, true, true},
		{"both errored", true, true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			st, got := newTestStore(t, Config{})
			k := makeEdgeKey(traceID(1), spanID(1))
			st.upsert(t0, k, sideClient, halfSpan{service: "a", failed: tc.clientBad}, nil)
			st.upsert(t0, k, sideServer, halfSpan{service: "b", failed: tc.serverBad}, nil)
			if len(*got) != 1 {
				t.Fatalf("edges = %d, want 1", len(*got))
			}
			if (*got)[0].Failed != tc.want {
				t.Fatalf("Failed = %v, want %v", (*got)[0].Failed, tc.want)
			}
		})
	}
}

// The first non-empty classification sticks: a plain server half must not clear
// the messaging/database type its partner established.
func TestStoreConnectionTypeSticks(t *testing.T) {
	for _, tc := range []struct {
		name           string
		client, server ConnectionType
		want           ConnectionType
	}{
		{"neither classifies", ConnectionUnknown, ConnectionUnknown, ConnectionUnknown},
		{"client classifies", ConnectionMessagingSystem, ConnectionUnknown, ConnectionMessagingSystem},
		{"server classifies", ConnectionUnknown, ConnectionMessagingSystem, ConnectionMessagingSystem},
		{"both classify", ConnectionDatabase, ConnectionDatabase, ConnectionDatabase},
	} {
		t.Run(tc.name, func(t *testing.T) {
			st, got := newTestStore(t, Config{})
			k := makeEdgeKey(traceID(1), spanID(1))
			st.upsert(t0, k, sideClient, halfSpan{service: "a", connection: tc.client}, nil)
			st.upsert(t0, k, sideServer, halfSpan{service: "b", connection: tc.server}, nil)
			if (*got)[0].Connection != tc.want {
				t.Fatalf("connection = %q, want %q", (*got)[0].Connection, tc.want)
			}
		})
	}
}

// Dimensions accumulate from whichever side carried them; the names arrive
// already prefixed, so both sides' copies of one key coexist.
func TestStoreDimensionsMergeFromBothSides(t *testing.T) {
	st, got := newTestStore(t, Config{})
	k := makeEdgeKey(traceID(1), spanID(1))
	st.upsert(t0, k, sideClient, halfSpan{service: "a"}, []EdgeDimension{{"client_http.method", "GET"}})
	st.upsert(t0, k, sideServer, halfSpan{service: "b"}, []EdgeDimension{{"server_http.route", "/orders"}})
	e := (*got)[0]
	want := map[string]string{"client_http.method": "GET", "server_http.route": "/orders"}
	if d := dimsOf(e); !maps.Equal(d, want) {
		t.Fatalf("dimensions = %v, want %v", d, want)
	}
}

// The dimension SEQUENCE handed to the sink must depend on the dimension set
// alone, never on which half arrived first: the metric layer keys a series on
// it, so an arrival-ordered sequence gives one edge two keys — two series
// rendering byte-identical label sets into one payload.
func TestStoreDimensionOrderIsArrivalIndependent(t *testing.T) {
	client := []EdgeDimension{{"client_http.method", "GET"}, {"client_http.route", "/orders"}}
	server := []EdgeDimension{{"server_http.method", "GET"}}

	var orders [][]EdgeDimension
	for _, serverFirst := range []bool{false, true} {
		st, got := newTestStore(t, Config{})
		k := makeEdgeKey(traceID(1), spanID(1))
		if serverFirst {
			st.upsert(t0, k, sideServer, halfSpan{service: "b"}, server)
			st.upsert(t0, k, sideClient, halfSpan{service: "a"}, client)
		} else {
			st.upsert(t0, k, sideClient, halfSpan{service: "a"}, client)
			st.upsert(t0, k, sideServer, halfSpan{service: "b"}, server)
		}
		if len(*got) != 1 {
			t.Fatalf("edges = %d, want 1", len(*got))
		}
		orders = append(orders, (*got)[0].Dimensions)
	}
	if !slices.Equal(orders[0], orders[1]) {
		t.Fatalf("client-first order %v != server-first order %v", orders[0], orders[1])
	}
	// And the canonical order is the documented one: the client half's
	// dimensions in their configured order, then the server half's.
	want := []EdgeDimension{{"client_http.method", "GET"}, {"client_http.route", "/orders"}, {"server_http.method", "GET"}}
	if !slices.Equal(orders[0], want) {
		t.Fatalf("order = %v, want %v", orders[0], want)
	}
}

// The same half arriving twice REPLACES its dimensions. Appending would carry
// each one twice and — the sequence being the series key — mint a second series
// rendering the very same labels.
func TestStoreDuplicateHalfReplacesDimensions(t *testing.T) {
	st, got := newTestStore(t, Config{})
	k := makeEdgeKey(traceID(1), spanID(1))
	dims := []EdgeDimension{{"client_http.method", "GET"}, {"client_http.route", "/orders"}}
	st.upsert(t0, k, sideClient, halfSpan{service: "a"}, dims)
	st.upsert(t0, k, sideClient, halfSpan{service: "a"}, dims) // a re-pushed batch
	st.upsert(t0, k, sideServer, halfSpan{service: "b"}, nil)

	e := (*got)[0]
	if !slices.Equal(e.Dimensions, dims) {
		t.Fatalf("dimensions = %v, want %v (the repeat replaced, not appended)", e.Dimensions, dims)
	}
	// A SHORTER replacement must not leave the dropped pairs' strings reachable
	// from the entry: they point into an OTLP payload the entry could otherwise
	// pin for a whole Wait.
	st2, _ := newTestStore(t, Config{})
	st2.upsert(t0, k, sideClient, halfSpan{service: "a"}, dims)
	st2.upsert(t0, k, sideClient, halfSpan{service: "a"}, dims[:1])
	held := st2.items[k].dims
	if tail := held[:cap(held)][1]; tail != (EdgeDimension{}) {
		t.Fatalf("dropped dimension still referenced: %v", tail)
	}
}

func TestStoreDimensionlessEdgeCarriesNoSlice(t *testing.T) {
	st, got := newTestStore(t, Config{})
	k := makeEdgeKey(traceID(1), spanID(1))
	st.upsert(t0, k, sideClient, halfSpan{service: "a"}, nil)
	st.upsert(t0, k, sideServer, halfSpan{service: "b"}, nil)
	// Not merely cosmetic: the default config configures no dimensions, so the
	// join must not even touch its scratch on the path every edge takes.
	if (*got)[0].Dimensions != nil {
		t.Fatalf("Dimensions = %v, want nil when no dimensions were carried", (*got)[0].Dimensions)
	}
}

// The Edge's dimensions BORROW the store's buffers, so the sink must see them
// intact — the retirement that clears them runs after Record returns, not
// before. (The test sinks clone for that reason; this one deliberately does
// not.)
func TestStoreDimensionsAreLiveDuringRecord(t *testing.T) {
	var seen []EdgeDimension
	st := newEdgeStore(Config{Wait: time.Second}.withDefaults(), func(e Edge) {
		seen = append(seen, e.Dimensions...) // copies during the call, as a sink must
	})
	k := makeEdgeKey(traceID(1), spanID(1))
	st.upsert(t0, k, sideClient, halfSpan{service: "a"}, []EdgeDimension{{"client_http.method", "GET"}})
	st.upsert(t0, k, sideServer, halfSpan{service: "b"}, []EdgeDimension{{"server_http.route", "/orders"}})

	// And the promotion path, whose Edge borrows the ENTRY's own buffer.
	k2 := makeEdgeKey(traceID(2), spanID(2))
	st.upsert(t0, k2, sideClient, halfSpan{service: "a", peer: "db"}, []EdgeDimension{{"client_db.system", "postgresql"}})
	st.expire(t0.Add(time.Second), 10)

	want := []EdgeDimension{{"client_http.method", "GET"}, {"server_http.route", "/orders"}, {"client_db.system", "postgresql"}}
	if !slices.Equal(seen, want) {
		t.Fatalf("sink saw %v, want %v", seen, want)
	}
}

// The scratch the completing pair is joined into must not keep the payload's
// strings alive after the sink has copied what it wants.
func TestStoreJoinScratchIsCleared(t *testing.T) {
	st, _ := newTestStore(t, Config{})
	k := makeEdgeKey(traceID(1), spanID(1))
	st.upsert(t0, k, sideClient, halfSpan{service: "a"}, []EdgeDimension{{"client_http.method", "GET"}})
	st.upsert(t0, k, sideServer, halfSpan{service: "b"}, []EdgeDimension{{"server_http.route", "/orders"}})
	buf := st.dimBuf
	if len(buf) != 0 {
		t.Fatalf("join scratch len = %d, want it emptied after the sink returned", len(buf))
	}
	for i, d := range buf[:cap(buf)] {
		if d != (EdgeDimension{}) {
			t.Fatalf("join scratch still references %v at %d", d, i)
		}
	}
}

func TestStoreExpiryUnpaired(t *testing.T) {
	st, got := newTestStore(t, Config{Wait: 10 * time.Second})
	k := makeEdgeKey(traceID(1), spanID(1))
	st.upsert(t0, k, sideClient, halfSpan{service: "checkout"}, nil)

	// Not yet due.
	if n := st.expire(t0.Add(9*time.Second), 10); n != 0 {
		t.Fatalf("expired %d before Wait elapsed", n)
	}
	if st.stats().Items != 1 {
		t.Fatalf("items = %d, want the half still pending", st.stats().Items)
	}
	// Due at exactly Wait.
	if n := st.expire(t0.Add(10*time.Second), 10); n != 1 {
		t.Fatalf("expired %d at Wait, want 1", n)
	}
	if len(*got) != 0 {
		t.Fatalf("edges = %+v, want none: nothing named the far side", *got)
	}
	s := st.stats()
	if s.Unpaired != 1 || s.VirtualNode != 0 || s.Items != 0 {
		t.Fatalf("stats = %+v, want one unpaired half retired", s)
	}
}

func TestStoreExpiryPromotesVirtualNode(t *testing.T) {
	for _, tc := range []struct {
		name       string
		side       edgeSide
		conn       ConnectionType
		wantClient string
		wantServer string
		wantVirt   string
		wantConn   ConnectionType
	}{
		{
			name: "client half names the callee", side: sideClient, conn: ConnectionUnknown,
			wantClient: "checkout", wantServer: "payments-api", wantVirt: virtualNodeServer,
			wantConn: ConnectionVirtualNode,
		},
		{
			// The observed half is the SERVER (payments-api); the peer names
			// the caller that never arrived.
			name: "server half names the caller", side: sideServer, conn: ConnectionUnknown,
			wantClient: "checkout", wantServer: "payments-api", wantVirt: virtualNodeClient,
			wantConn: ConnectionVirtualNode,
		},
		{
			// A database edge is virtual BY NATURE (the datastore emits no
			// span), so promotion must not overwrite the classification the
			// span carried — that is the deviation from Tempo documented in
			// promote().
			name: "database classification survives promotion", side: sideClient, conn: ConnectionDatabase,
			wantClient: "checkout", wantServer: "payments-api", wantVirt: virtualNodeServer,
			wantConn: ConnectionDatabase,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			st, got := newTestStore(t, Config{Wait: time.Second})
			k := makeEdgeKey(traceID(1), spanID(1))
			svc := "checkout"
			if tc.side == sideServer {
				svc = "payments-api"
			}
			peer := "payments-api" // the callee the client half never heard from
			if tc.side == sideServer {
				peer = "checkout" // the caller the server half never heard from
			}
			st.upsert(t0, k, tc.side, halfSpan{service: svc, seconds: 0.5, peer: peer, connection: tc.conn}, nil)
			st.expire(t0.Add(time.Second), 10)

			if len(*got) != 1 {
				t.Fatalf("edges = %d, want the promoted one", len(*got))
			}
			e := (*got)[0]
			if e.ClientService != tc.wantClient || e.ServerService != tc.wantServer {
				t.Fatalf("services = %q -> %q, want %q -> %q", e.ClientService, e.ServerService, tc.wantClient, tc.wantServer)
			}
			if e.VirtualNode != tc.wantVirt {
				t.Fatalf("VirtualNode = %q, want %q", e.VirtualNode, tc.wantVirt)
			}
			if e.Connection != tc.wantConn {
				t.Fatalf("Connection = %q, want %q", e.Connection, tc.wantConn)
			}
			// Only the observed side has a duration: the synthesized node
			// measured nothing, and a zero observation would drag its
			// histogram down.
			if tc.side == sideClient && (!e.HaveClient || e.HaveServer) {
				t.Fatalf("have flags = %v/%v, want client only", e.HaveClient, e.HaveServer)
			}
			if tc.side == sideServer && (e.HaveClient || !e.HaveServer) {
				t.Fatalf("have flags = %v/%v, want server only", e.HaveClient, e.HaveServer)
			}
			if s := st.stats(); s.VirtualNode != 1 || s.Unpaired != 0 {
				t.Fatalf("stats = %+v, want one promotion", s)
			}
		})
	}
}

// Expiry order is insertion order (one Wait for all), so an entry inserted
// later must not retire before an earlier one.
func TestStoreExpiresInInsertionOrder(t *testing.T) {
	st, got := newTestStore(t, Config{Wait: 10 * time.Second})
	for i := byte(1); i <= 3; i++ {
		st.upsert(t0.Add(time.Duration(i)*time.Second), makeEdgeKey(traceID(1), spanID(i)),
			sideClient, halfSpan{service: "a", peer: string(rune('a' + i - 1))}, nil)
	}
	// At t0+11s only the first (inserted at t0+1s) is due.
	if n := st.expire(t0.Add(11*time.Second), 10); n != 1 {
		t.Fatalf("expired %d, want 1", n)
	}
	if len(*got) != 1 || (*got)[0].ServerService != "a" {
		t.Fatalf("edges = %+v, want the oldest half first", *got)
	}
	if n := st.expire(t0.Add(13*time.Second), 10); n != 2 {
		t.Fatalf("expired %d, want the remaining 2", n)
	}
}

// The sweep must be bounded: it holds the mutex every concurrent Consume needs.
func TestStoreExpireIsBounded(t *testing.T) {
	st, got := newTestStore(t, Config{Wait: time.Second, MaxItems: 1000})
	for i := 0; i < 100; i++ {
		st.upsert(t0, makeEdgeKey(traceID(byte(i/10)), spanID(byte(i%10))), sideClient, halfSpan{service: "a"}, nil)
	}
	if st.stats().Items != 100 {
		t.Fatalf("items = %d, want 100", st.stats().Items)
	}
	now := t0.Add(time.Second)
	if n := st.expire(now, 30); n != 30 {
		t.Fatalf("expired %d with a budget of 30", n)
	}
	if got := st.stats().Items; got != 70 {
		t.Fatalf("items = %d after a bounded sweep, want 70", got)
	}
	if n := st.expire(now, 1000); n != 70 {
		t.Fatalf("expired %d, want the remaining 70", n)
	}
	if len(*got) != 0 {
		t.Fatalf("edges = %d, want none (no peer attributes)", len(*got))
	}
}

// At MaxItems a new key is refused and counted; an existing half must still be
// able to COMPLETE, or the cap would convert a full store into permanent loss
// rather than back-pressure.
func TestStoreMaxItemsRefusesNewKeysOnly(t *testing.T) {
	st, got := newTestStore(t, Config{MaxItems: 2})
	k1 := makeEdgeKey(traceID(1), spanID(1))
	k2 := makeEdgeKey(traceID(1), spanID(2))
	k3 := makeEdgeKey(traceID(1), spanID(3))
	st.upsert(t0, k1, sideClient, halfSpan{service: "a"}, nil)
	st.upsert(t0, k2, sideClient, halfSpan{service: "b"}, nil)

	st.upsert(t0, k3, sideClient, halfSpan{service: "c"}, nil)
	if s := st.stats(); s.Dropped != 1 || s.Items != 2 {
		t.Fatalf("stats = %+v, want the third refused and counted", s)
	}
	// The refusal must not have evicted a live half.
	if _, ok := st.items[k1]; !ok {
		t.Fatal("k1 was evicted to make room; a half about to complete must never be sacrificed")
	}
	st.upsert(t0, k1, sideServer, halfSpan{service: "a2"}, nil)
	if len(*got) != 1 || (*got)[0].ServerService != "a2" {
		t.Fatalf("edges = %+v, want k1 to complete while at the cap", *got)
	}
	// The freed slot is usable again.
	st.upsert(t0, k3, sideClient, halfSpan{service: "c"}, nil)
	if s := st.stats(); s.Dropped != 1 || s.Items != 2 {
		t.Fatalf("stats = %+v, want k3 admitted after k1 retired", s)
	}
}

// Retired entries are recycled, and the recycled entry must be CLEAN — a
// leaked field would attribute one request's service or dimensions to the next.
func TestStoreRecyclesEntries(t *testing.T) {
	st, got := newTestStore(t, Config{})
	k1 := makeEdgeKey(traceID(1), spanID(1))
	k2 := makeEdgeKey(traceID(1), spanID(2))

	st.upsert(t0, k1, sideClient, halfSpan{service: "a", failed: true, peer: "p",
		connection: ConnectionDatabase}, []EdgeDimension{{"client_x", "1"}})
	first := st.items[k1]
	st.upsert(t0, k1, sideServer, halfSpan{service: "b"}, nil)
	if st.free != first {
		t.Fatal("completed entry was not recycled")
	}
	// The recycled entry must not pin the strings it carried: they point into
	// an OTLP payload that is otherwise free to be collected.
	if cap(first.dims) > 0 && first.dims[:1][0] != (EdgeDimension{}) {
		t.Fatalf("recycled entry still references %v", first.dims[:1][0])
	}

	st.upsert(t0, k2, sideClient, halfSpan{service: "c"}, nil)
	if st.items[k2] != first {
		t.Fatal("free entry was not reused for the next insert")
	}
	st.upsert(t0, k2, sideServer, halfSpan{service: "d"}, nil)
	e := (*got)[1]
	if e.ClientService != "c" || e.ServerService != "d" || e.Failed || e.Connection != ConnectionUnknown || e.Dimensions != nil {
		t.Fatalf("recycled entry leaked state into %+v", e)
	}
}

// The two operator-facing counters must actually move: docs/METRICS.md
// documents them as the signal for "the graph is incomplete, and which bound
// caused it", and a counter that is registered but never bumped reads as a
// healthy shard.
func TestStoreCountersReachObs(t *testing.T) {
	fullBefore := obs.ServiceGraphStoreFull.Value()
	expiredBefore := obs.ServiceGraphExpired.Value()

	st, _ := newTestStore(t, Config{Wait: time.Second, MaxItems: 1})
	st.upsert(t0, makeEdgeKey(traceID(1), spanID(1)), sideClient, halfSpan{service: "a"}, nil)
	st.upsert(t0, makeEdgeKey(traceID(1), spanID(2)), sideClient, halfSpan{service: "b"}, nil)
	if got := obs.ServiceGraphStoreFull.Value() - fullBefore; got != 1 {
		t.Fatalf("store-full counter moved by %v, want 1", got)
	}

	// Both shapes of expiry count: promoted (virtual node) and not.
	st.expire(t0.Add(time.Second), 10)
	st.upsert(t0.Add(time.Second), makeEdgeKey(traceID(1), spanID(3)), sideClient,
		halfSpan{service: "c", peer: "stripe-api"}, nil)
	st.expire(t0.Add(2*time.Second), 10)
	if got := obs.ServiceGraphExpired.Value() - expiredBefore; got != 2 {
		t.Fatalf("expired counter moved by %v, want 2", got)
	}
	if s := st.stats(); s.Unpaired != 1 || s.VirtualNode != 1 {
		t.Fatalf("stats = %+v, want one of each shape", s)
	}
}

func TestStoreCountsUnkeyable(t *testing.T) {
	st, _ := newTestStore(t, Config{})
	st.countUnkeyable()
	if st.stats().Unkeyable != 1 {
		t.Fatalf("stats = %+v, want one unkeyable span", st.stats())
	}
}
