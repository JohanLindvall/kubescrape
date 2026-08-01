package servicegraph

import (
	"strings"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

// sgSpan is one span in a test batch.
type sgSpan struct {
	name     string
	kind     ptrace.SpanKind
	status   ptrace.StatusCode
	dur      float64
	traceID  pcommon.TraceID
	spanID   pcommon.SpanID
	parentID pcommon.SpanID
	attrs    map[string]string
}

// sgTraces builds a single-resource trace batch under service.
func sgTraces(service string, spans ...sgSpan) ptrace.Traces {
	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	if service != "" {
		rs.Resource().Attributes().PutStr(attrServiceName, service)
	}
	ss := rs.ScopeSpans().AppendEmpty()
	base := pcommon.Timestamp(1_700_000_000 * 1e9)
	for _, sp := range spans {
		s := ss.Spans().AppendEmpty()
		s.SetName(sp.name)
		s.SetKind(sp.kind)
		s.Status().SetCode(sp.status)
		s.SetStartTimestamp(base)
		s.SetEndTimestamp(base + pcommon.Timestamp(sp.dur*float64(time.Second)))
		s.SetTraceID(sp.traceID)
		s.SetSpanID(sp.spanID)
		s.SetParentSpanID(sp.parentID)
		for k, v := range sp.attrs {
			s.Attributes().PutStr(k, v)
		}
	}
	return td
}

// pairSink collects the edges a processor produces.
type pairSink struct{ edges []Edge }

func (s *pairSink) Record(e Edge) { s.edges = append(s.edges, e) }

func (s *pairSink) only(t *testing.T) Edge {
	t.Helper()
	if len(s.edges) != 1 {
		t.Fatalf("edges = %d (%+v), want 1", len(s.edges), s.edges)
	}
	return s.edges[0]
}

// newTestProcessor returns a processor whose clock the test drives.
func newTestProcessor(t *testing.T, cfg Config) (*Processor, *pairSink, *time.Time) {
	t.Helper()
	p := NewProcessor(cfg, nil)
	sink := &pairSink{}
	p.SetSink(sink)
	clock := t0
	p.now = func() time.Time { return clock }
	return p, sink, &clock
}

// clientSpan/serverSpan are one request's two halves: the server's parent is
// the client's own span id, which is the only id the two agree on.
func clientSpan(dur float64, attrs map[string]string) sgSpan {
	return sgSpan{name: "GET /orders", kind: ptrace.SpanKindClient, dur: dur,
		traceID: traceID(1), spanID: spanID(1), attrs: attrs}
}

func serverSpan(dur float64, attrs map[string]string) sgSpan {
	return sgSpan{name: "GET /orders", kind: ptrace.SpanKindServer, dur: dur,
		traceID: traceID(1), spanID: spanID(2), parentID: spanID(1), attrs: attrs}
}

func TestConsumePairsAcrossBatches(t *testing.T) {
	for _, tc := range []struct {
		name        string
		serverFirst bool
	}{
		{"client first", false},
		{"server first", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p, sink, _ := newTestProcessor(t, Config{})
			client := sgTraces("checkout", clientSpan(0.30, nil))
			server := sgTraces("orders", serverSpan(0.25, nil))
			if tc.serverFirst {
				client, server = server, client
			}
			p.Consume(client)
			p.Consume(server)

			e := sink.only(t)
			if e.ClientService != "checkout" || e.ServerService != "orders" {
				t.Fatalf("services = %q -> %q", e.ClientService, e.ServerService)
			}
			if !e.HaveClient || !e.HaveServer {
				t.Fatalf("have flags = %v/%v", e.HaveClient, e.HaveServer)
			}
			if e.ClientSeconds != 0.30 || e.ServerSeconds != 0.25 {
				t.Fatalf("durations = %v/%v, want each side's own in seconds", e.ClientSeconds, e.ServerSeconds)
			}
			if e.Connection != ConnectionUnknown {
				t.Fatalf("connection = %q, want unset for a plain call", e.Connection)
			}
		})
	}
}

// Both halves in ONE batch is the common shape when a shard owns a whole trace.
func TestConsumePairsWithinOneBatch(t *testing.T) {
	p, sink, _ := newTestProcessor(t, Config{})
	td := sgTraces("checkout", clientSpan(0.1, nil), serverSpan(0.05, nil))
	p.Consume(td)
	// Both spans came from ONE resource here, so the edge is checkout->checkout;
	// what matters is that the pairing happened without a second Consume.
	if e := sink.only(t); !e.HaveClient || !e.HaveServer {
		t.Fatalf("edge = %+v, want both halves", e)
	}
}

func TestConsumeProducerConsumerIsMessaging(t *testing.T) {
	p, sink, _ := newTestProcessor(t, Config{})
	p.Consume(sgTraces("checkout", sgSpan{kind: ptrace.SpanKindProducer, dur: 0.01,
		traceID: traceID(1), spanID: spanID(1)}))
	p.Consume(sgTraces("shipping", sgSpan{kind: ptrace.SpanKindConsumer, dur: 0.02,
		traceID: traceID(1), spanID: spanID(2), parentID: spanID(1)}))

	e := sink.only(t)
	if e.Connection != ConnectionMessagingSystem {
		t.Fatalf("connection = %q, want %q", e.Connection, ConnectionMessagingSystem)
	}
	if e.ClientService != "checkout" || e.ServerService != "shipping" {
		t.Fatalf("services = %q -> %q, want producer as the client half", e.ClientService, e.ServerService)
	}
}

// A consumer whose producer never arrives still pairs with a virtual node, and
// keeps the messaging classification (the deviation documented in promote).
func TestConsumeConsumerOnlyKeepsMessaging(t *testing.T) {
	p, sink, clock := newTestProcessor(t, Config{Wait: time.Second})
	p.Consume(sgTraces("shipping", sgSpan{kind: ptrace.SpanKindConsumer, dur: 0.02,
		traceID: traceID(1), spanID: spanID(2), parentID: spanID(1),
		attrs: map[string]string{"peer.service": "orders-queue"}}))
	*clock = t0.Add(time.Second)
	p.Sweep()

	e := sink.only(t)
	if e.Connection != ConnectionMessagingSystem || e.VirtualNode != virtualNodeClient {
		t.Fatalf("edge = %+v, want a messaging edge with a virtual client", e)
	}
	if e.ClientService != "orders-queue" || e.ServerService != "shipping" {
		t.Fatalf("services = %q -> %q", e.ClientService, e.ServerService)
	}
}

func TestConsumeDatabaseClassification(t *testing.T) {
	for _, tc := range []struct {
		name  string
		kind  ptrace.SpanKind
		attrs map[string]string
		want  ConnectionType
	}{
		{"db.system on the client", ptrace.SpanKindClient, map[string]string{"db.system": "postgresql"}, ConnectionDatabase},
		{"db.name on the client", ptrace.SpanKindClient, map[string]string{"db.name": "orders"}, ConnectionDatabase},
		{"current semconv db.system.name", ptrace.SpanKindClient, map[string]string{"db.system.name": "postgresql"}, ConnectionDatabase},
		{
			// peer.service wins the NAMING (it is first in precedence) but the
			// db.* attributes still classify the edge.
			name: "peer.service plus db.system", kind: ptrace.SpanKindClient,
			attrs: map[string]string{"peer.service": "orders-db", "db.system": "postgresql"}, want: ConnectionDatabase,
		},
		{
			// A SERVER span carrying db.* is a service that talks to a
			// database, not the database itself: classifying it would label
			// every application edge into it as a database call.
			name: "db.system on the server", kind: ptrace.SpanKindServer,
			attrs: map[string]string{"db.system": "postgresql"}, want: ConnectionUnknown,
		},
		{"no database attributes", ptrace.SpanKindClient, nil, ConnectionUnknown},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p, sink, _ := newTestProcessor(t, Config{})
			if tc.kind == ptrace.SpanKindClient {
				p.Consume(sgTraces("checkout", clientSpan(0.1, tc.attrs)))
				p.Consume(sgTraces("orders", serverSpan(0.05, nil)))
			} else {
				p.Consume(sgTraces("checkout", clientSpan(0.1, nil)))
				p.Consume(sgTraces("orders", serverSpan(0.05, tc.attrs)))
			}
			if e := sink.only(t); e.Connection != tc.want {
				t.Fatalf("connection = %q, want %q", e.Connection, tc.want)
			}
		})
	}
}

// The peer attributes are consulted in configured order, and the first present
// one names the far side.
func TestConsumeVirtualNodePeerPrecedence(t *testing.T) {
	for _, tc := range []struct {
		name     string
		attrs    map[string]string
		wantPeer string
		wantConn ConnectionType
	}{
		{"peer.service", map[string]string{"peer.service": "stripe-api"}, "stripe-api", ConnectionVirtualNode},
		{"db.name", map[string]string{"db.name": "orders"}, "orders", ConnectionDatabase},
		{"db.system", map[string]string{"db.system": "postgresql"}, "postgresql", ConnectionDatabase},
		{
			name:  "peer.service beats db.name and db.system",
			attrs: map[string]string{"db.system": "postgresql", "db.name": "orders", "peer.service": "orders-db"},
			// peer.service named it, but the db.* attributes still say what
			// kind of thing it is.
			wantPeer: "orders-db", wantConn: ConnectionDatabase,
		},
		{
			name:     "db.name beats db.system",
			attrs:    map[string]string{"db.system": "postgresql", "db.name": "orders"},
			wantPeer: "orders", wantConn: ConnectionDatabase,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p, sink, clock := newTestProcessor(t, Config{Wait: time.Second})
			p.Consume(sgTraces("checkout", clientSpan(0.1, tc.attrs)))
			*clock = t0.Add(time.Second)
			p.Sweep()

			e := sink.only(t)
			if e.ServerService != tc.wantPeer || e.VirtualNode != virtualNodeServer {
				t.Fatalf("edge = %+v, want server %q synthesized", e, tc.wantPeer)
			}
			if e.ClientService != "checkout" || !e.HaveClient || e.HaveServer {
				t.Fatalf("edge = %+v, want the client half only", e)
			}
			if e.Connection != tc.wantConn {
				t.Fatalf("connection = %q, want %q", e.Connection, tc.wantConn)
			}
			if s := p.Stats(); s.VirtualNode != 1 || s.Unpaired != 0 || s.Items != 0 {
				t.Fatalf("stats = %+v", s)
			}
		})
	}
}

func TestConsumeVirtualNodesDisabled(t *testing.T) {
	// An empty (non-nil) list disables promotion entirely, per the Config
	// contract: nil takes the default, []string{} means "no virtual nodes".
	p, sink, clock := newTestProcessor(t, Config{Wait: time.Second, VirtualNodePeerAttributes: []string{}})
	p.Consume(sgTraces("checkout", clientSpan(0.1, map[string]string{"peer.service": "stripe-api"})))
	*clock = t0.Add(time.Second)
	p.Sweep()
	if len(sink.edges) != 0 {
		t.Fatalf("edges = %+v, want none with virtual nodes disabled", sink.edges)
	}
	if s := p.Stats(); s.Unpaired != 1 {
		t.Fatalf("stats = %+v, want the half counted unpaired", s)
	}
}

func TestConsumeExpiryUnpaired(t *testing.T) {
	p, sink, clock := newTestProcessor(t, Config{Wait: 10 * time.Second})
	p.Consume(sgTraces("checkout", clientSpan(0.1, nil)))

	*clock = t0.Add(9 * time.Second)
	p.Sweep()
	if s := p.Stats(); s.Items != 1 {
		t.Fatalf("stats = %+v, want the half still waiting before Wait", s)
	}
	*clock = t0.Add(10 * time.Second)
	p.Sweep()
	if len(sink.edges) != 0 {
		t.Fatalf("edges = %+v, want none: nothing named the callee", sink.edges)
	}
	if s := p.Stats(); s.Unpaired != 1 || s.Items != 0 {
		t.Fatalf("stats = %+v", s)
	}
}

// Consume itself expires incrementally, so a shard that is only being pushed
// spans never grows to MaxItems between a caller's Sweep ticks.
func TestConsumeExpiresIncrementally(t *testing.T) {
	p, _, clock := newTestProcessor(t, Config{Wait: time.Second})
	p.Consume(sgTraces("checkout", clientSpan(0.1, nil)))
	*clock = t0.Add(time.Second)
	p.Consume(sgTraces("other", sgSpan{kind: ptrace.SpanKindClient, dur: 0.1,
		traceID: traceID(2), spanID: spanID(1)}))
	s := p.Stats()
	if s.Unpaired != 1 || s.Items != 1 {
		t.Fatalf("stats = %+v, want the first half expired by the second Consume", s)
	}
}

func TestConsumeFailedFromEitherSide(t *testing.T) {
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
			p, sink, _ := newTestProcessor(t, Config{})
			c := clientSpan(0.1, nil)
			s := serverSpan(0.05, nil)
			if tc.clientBad {
				c.status = ptrace.StatusCodeError
			}
			if tc.serverBad {
				s.status = ptrace.StatusCodeError
			}
			p.Consume(sgTraces("checkout", c))
			p.Consume(sgTraces("orders", s))
			if e := sink.only(t); e.Failed != tc.want {
				t.Fatalf("Failed = %v, want %v", e.Failed, tc.want)
			}
		})
	}
}

func TestConsumeDimensions(t *testing.T) {
	p, sink, _ := newTestProcessor(t, Config{Dimensions: []string{"http.method", "k8s.cluster.name", "absent"}})
	// http.method on the span, k8s.cluster.name only on the RESOURCE (the
	// documented fallback).
	td := sgTraces("checkout", clientSpan(0.1, map[string]string{"http.method": "GET"}))
	td.ResourceSpans().At(0).Resource().Attributes().PutStr("k8s.cluster.name", "prod")
	p.Consume(td)
	p.Consume(sgTraces("orders", serverSpan(0.05, map[string]string{"http.method": "GET"})))

	e := sink.only(t)
	want := map[string]string{
		"client_http.method":      "GET",
		"client_k8s.cluster.name": "prod",
		"server_http.method":      "GET",
	}
	if len(e.Dimensions) != len(want) {
		t.Fatalf("dimensions = %v, want %v", e.Dimensions, want)
	}
	for k, v := range want {
		if e.Dimensions[k] != v {
			t.Fatalf("dimensions[%q] = %q, want %q (all: %v)", k, e.Dimensions[k], v, e.Dimensions)
		}
	}
}

func TestConsumeTruncatesLabelValues(t *testing.T) {
	long := strings.Repeat("x", maxDimensionValueBytes+10)
	p, sink, clock := newTestProcessor(t, Config{Wait: time.Second, Dimensions: []string{"http.route"}})
	p.Consume(sgTraces(long, clientSpan(0.1, map[string]string{
		"peer.service": long,
		"http.route":   long,
	})))
	*clock = t0.Add(time.Second)
	p.Sweep()

	e := sink.only(t)
	if len(e.ClientService) != maxDimensionValueBytes {
		t.Fatalf("client service len = %d, want %d", len(e.ClientService), maxDimensionValueBytes)
	}
	if len(e.ServerService) != maxDimensionValueBytes {
		t.Fatalf("peer len = %d, want %d", len(e.ServerService), maxDimensionValueBytes)
	}
	if len(e.Dimensions["client_http.route"]) != maxDimensionValueBytes {
		t.Fatalf("dimension len = %d, want %d", len(e.Dimensions["client_http.route"]), maxDimensionValueBytes)
	}
}

// INTERNAL/UNSPECIFIED spans are not a call between two services, and a span
// with no trace id cannot be keyed at all.
func TestConsumeIgnoresUnpairableSpans(t *testing.T) {
	for _, tc := range []struct {
		name          string
		span          sgSpan
		wantUnkeyable uint64
	}{
		{"internal", sgSpan{kind: ptrace.SpanKindInternal, traceID: traceID(1), spanID: spanID(1)}, 0},
		{"unspecified", sgSpan{kind: ptrace.SpanKindUnspecified, traceID: traceID(1), spanID: spanID(1)}, 0},
		{"no trace id", sgSpan{kind: ptrace.SpanKindClient, spanID: spanID(1)}, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p, _, _ := newTestProcessor(t, Config{})
			p.Consume(sgTraces("checkout", tc.span))
			s := p.Stats()
			if s.Items != 0 {
				t.Fatalf("items = %d, want the span ignored", s.Items)
			}
			if s.Unkeyable != tc.wantUnkeyable {
				t.Fatalf("unkeyable = %d, want %d", s.Unkeyable, tc.wantUnkeyable)
			}
		})
	}
}

// A root server span (no parent) keys under the zero span id: no client half
// can ever arrive, so it must promote against its peer attribute — that is the
// graph's entry edge from outside the mesh.
func TestConsumeRootServerSpanBecomesEntryEdge(t *testing.T) {
	p, sink, clock := newTestProcessor(t, Config{Wait: time.Second})
	p.Consume(sgTraces("frontend", sgSpan{kind: ptrace.SpanKindServer, dur: 0.2,
		traceID: traceID(1), spanID: spanID(1),
		attrs: map[string]string{"peer.service": "user"}}))
	*clock = t0.Add(time.Second)
	p.Sweep()

	e := sink.only(t)
	if e.ClientService != "user" || e.ServerService != "frontend" || e.VirtualNode != virtualNodeClient {
		t.Fatalf("edge = %+v, want user -> frontend with a virtual client", e)
	}
}

func TestConsumeMaxItemsDrops(t *testing.T) {
	p, _, _ := newTestProcessor(t, Config{MaxItems: 2})
	spans := make([]sgSpan, 5)
	for i := range spans {
		spans[i] = sgSpan{kind: ptrace.SpanKindClient, dur: 0.1, traceID: traceID(1), spanID: spanID(byte(i + 1))}
	}
	p.Consume(sgTraces("checkout", spans...))
	if s := p.Stats(); s.Items != 2 || s.Dropped != 3 {
		t.Fatalf("stats = %+v, want 2 held and 3 refused", s)
	}
}

// Sweep is bounded per call: it holds the mutex every concurrent Consume needs,
// so it may never drain an arbitrarily deep store in one go.
func TestSweepIsBounded(t *testing.T) {
	p, _, clock := newTestProcessor(t, Config{Wait: time.Second, MaxItems: sweepBudget * 2})
	total := sweepBudget + 100
	spans := make([]sgSpan, total)
	for i := range spans {
		// Trace ids start at 1: a zero trace id is unkeyable by design and
		// would be skipped rather than stored.
		spans[i] = sgSpan{kind: ptrace.SpanKindClient, dur: 0.1,
			traceID: traceID(byte(i/200) + 1), spanID: spanID(byte(i % 200))}
	}
	// Distinct keys: 200 span ids x ceil(total/200) trace ids.
	p.Consume(sgTraces("checkout", spans...))
	if s := p.Stats(); s.Items != total {
		t.Fatalf("items = %d, want %d distinct keys", s.Items, total)
	}
	*clock = t0.Add(time.Second)
	p.Sweep()
	if s := p.Stats(); s.Items != 100 {
		t.Fatalf("items = %d after one Sweep, want exactly %d retired", s.Items, sweepBudget)
	}
	p.Sweep()
	if s := p.Stats(); s.Items != 0 {
		t.Fatalf("items = %d after the second Sweep, want 0", s.Items)
	}
}

// A processor with no sink still pairs and counts — metrics.go is wired after
// construction, and a span arriving in that window must not panic the ingest
// goroutine.
func TestConsumeWithoutSink(t *testing.T) {
	p := NewProcessor(Config{}, nil)
	p.now = func() time.Time { return t0 }
	p.Consume(sgTraces("checkout", clientSpan(0.1, nil)))
	p.Consume(sgTraces("orders", serverSpan(0.05, nil)))
	if s := p.Stats(); s.Completed != 1 || s.Items != 0 {
		t.Fatalf("stats = %+v", s)
	}
}

// Consume runs on the concurrent ingest handler goroutines, so the store's
// mutex is the only thing between two RPCs pairing at once. Run under -race.
func TestConsumeConcurrent(t *testing.T) {
	p := NewProcessor(Config{MaxItems: 1 << 16}, nil)
	sink := &lockedSink{}
	p.SetSink(sink)

	const goroutines, requests = 8, 200
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < requests; i++ {
				tid := traceID(byte(g + 1))
				sid := spanID(byte(i%200 + 1))
				// Halves in separate batches, so the two goroutines' inserts
				// and completions interleave.
				p.Consume(sgTraces("checkout", sgSpan{kind: ptrace.SpanKindClient, dur: 0.1,
					traceID: tid, spanID: sid}))
				p.Consume(sgTraces("orders", sgSpan{kind: ptrace.SpanKindServer, dur: 0.05,
					traceID: tid, spanID: spanID(255), parentID: sid}))
				p.Sweep()
			}
		}(g)
	}
	wg.Wait()

	if got := p.Stats().Completed; got != goroutines*requests {
		t.Fatalf("completed = %d, want %d", got, goroutines*requests)
	}
	if got := sink.n(); got != goroutines*requests {
		t.Fatalf("sink saw %d edges, want %d", got, goroutines*requests)
	}
	if items := p.Stats().Items; items != 0 {
		t.Fatalf("items = %d, want every half paired", items)
	}
}

// lockedSink counts edges from the concurrent Consume goroutines. The sink is
// called with the store's mutex held, but that is an implementation detail a
// sink must not rely on.
type lockedSink struct {
	mu    sync.Mutex
	count int
}

func (s *lockedSink) Record(Edge) {
	s.mu.Lock()
	s.count++
	s.mu.Unlock()
}

func (s *lockedSink) n() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.count
}

// countSink is the benchmark's sink: pairSink RETAINS every edge, so its
// growing slice would be charged to Consume and hide the pairing path's own
// (zero) allocation cost.
type countSink struct{ n int }

func (c *countSink) Record(Edge) { c.n++ }

// BenchmarkConsumePair is the warm path: one request's two halves, i.e. one
// insert and one completion per iteration. It must stay allocation-free — the
// free list and the array key exist for exactly this.
func BenchmarkConsumePair(b *testing.B) {
	p := NewProcessor(Config{}, nil)
	p.SetSink(&countSink{})
	td := sgTraces("checkout", clientSpan(0.30, nil), serverSpan(0.25, nil))
	p.Consume(td) // warm the map and the free list
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p.Consume(td)
	}
}

// With dimensions configured, the per-span extraction uses a stack scratch and
// the completed edge pays one map: the cost must be per EDGE, not per span.
func BenchmarkConsumePairWithDimensions(b *testing.B) {
	p := NewProcessor(Config{Dimensions: []string{"http.method", "http.route"}}, nil)
	p.SetSink(&countSink{})
	attrs := map[string]string{"http.method": "GET", "http.route": "/api/v1/orders"}
	td := sgTraces("checkout", clientSpan(0.30, attrs), serverSpan(0.25, attrs))
	p.Consume(td)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p.Consume(td)
	}
}

// A 100-span batch of client halves whose partners never arrive: the store
// holds them all, so every span pays a lookup against a populated map. This is
// the shape a shard sees when it owns one side of many requests, and the
// per-batch clock read is amortized across 100 spans rather than 2.
func BenchmarkConsumeUnpaired(b *testing.B) {
	p := NewProcessor(Config{MaxItems: 1 << 20}, nil)
	p.SetSink(&countSink{})
	spans := make([]sgSpan, 100)
	for i := range spans {
		spans[i] = sgSpan{kind: ptrace.SpanKindClient, dur: 0.1,
			traceID: traceID(byte(i/10) + 1), spanID: spanID(byte(i % 10))}
	}
	td := sgTraces("checkout", spans...)
	p.Consume(td)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p.Consume(td)
	}
}
