package servicegraph

import (
	"encoding/binary"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/JohanLindvall/kubescrape/internal/obs"
	"github.com/JohanLindvall/kubescrape/internal/testrace"
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

// The sink RETAINS every edge, so it must clone: the Edge's dimension slice
// belongs to the pairing store, which recycles it as soon as Record returns.
func (s *pairSink) Record(e Edge) { s.edges = append(s.edges, cloneEdge(e)) }

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

// producerSpan/consumerSpan are a messaging hop's two halves, keyed exactly
// like clientSpan/serverSpan (a producer is the client side, a consumer the
// server side).
func producerSpan(dur float64, attrs map[string]string) sgSpan {
	return sgSpan{name: "publish orders", kind: ptrace.SpanKindProducer, dur: dur,
		traceID: traceID(1), spanID: spanID(1), attrs: attrs}
}

func consumerSpan(dur float64, attrs map[string]string) sgSpan {
	return sgSpan{name: "consume orders", kind: ptrace.SpanKindConsumer, dur: dur,
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
	p, sink, clock := newTestProcessor(t, Config{Wait: "1s"})
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

		// PRODUCER spans arrive kind-classified messaging_system, and any
		// databaseAttrs attribute overrides that default UNIFORMLY. These three
		// are the same statement — "my callee is a database" — in three
		// spellings, and they used to classify three different ways (db.name
		// alone → database, the other two → messaging survived): which
		// connection_type an edge got depended on which attribute the SDK
		// happened to spell it with.
		{"db.name alone on a producer", ptrace.SpanKindProducer, map[string]string{"db.name": "orders"}, ConnectionDatabase},
		{
			name: "peer.service plus db.system on a producer", kind: ptrace.SpanKindProducer,
			attrs: map[string]string{"peer.service": "orders-db", "db.system": "postgresql"}, want: ConnectionDatabase,
		},
		{"db.system.name alone on a producer", ptrace.SpanKindProducer, map[string]string{"db.system.name": "postgresql"}, ConnectionDatabase},
		// The kind's default stands where nothing overrides it.
		{"no database attributes on a producer", ptrace.SpanKindProducer, nil, ConnectionMessagingSystem},
		{
			// The server-side exclusion read through the messaging kinds: a
			// CONSUMER carrying db.* keeps its kind's classification — the
			// side it is missing is its CALLER, and no database ever calls in.
			name: "db.system on the consumer", kind: ptrace.SpanKindConsumer,
			attrs: map[string]string{"db.system": "postgresql"}, want: ConnectionMessagingSystem,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p, sink, _ := newTestProcessor(t, Config{})
			// The attribute-carrying half under test pairs with a bare
			// partner, ORDERED so the edge's connection_type is decided by
			// that half alone: merge keeps the first non-empty
			// classification, so on the messaging kinds — where the partner
			// carries one too — the half under test must arrive first (the
			// consumer case would otherwise assert only that the producer's
			// messaging stuck, not that the consumer's db.* was excluded).
			// The client/server partners classify Unknown, so their arrival
			// order cannot mask anything.
			switch tc.kind {
			case ptrace.SpanKindClient:
				p.Consume(sgTraces("checkout", clientSpan(0.1, tc.attrs)))
				p.Consume(sgTraces("orders", serverSpan(0.05, nil)))
			case ptrace.SpanKindServer:
				p.Consume(sgTraces("checkout", clientSpan(0.1, nil)))
				p.Consume(sgTraces("orders", serverSpan(0.05, tc.attrs)))
			case ptrace.SpanKindProducer:
				p.Consume(sgTraces("checkout", producerSpan(0.1, tc.attrs)))
				p.Consume(sgTraces("shipping", consumerSpan(0.05, nil)))
			case ptrace.SpanKindConsumer:
				p.Consume(sgTraces("shipping", consumerSpan(0.05, tc.attrs)))
				p.Consume(sgTraces("checkout", producerSpan(0.1, nil)))
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
			p, sink, clock := newTestProcessor(t, Config{Wait: "1s"})
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
	p, sink, clock := newTestProcessor(t, Config{Wait: "1s", VirtualNodePeerAttributes: []string{}})
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
	p, sink, clock := newTestProcessor(t, Config{Wait: "10s"})
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
	p, _, clock := newTestProcessor(t, Config{Wait: "1s"})
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
	// The ORDER is the contract, not just the set: the client half's
	// dimensions in the configured order, then the server half's (see
	// joinDims — the metric layer keys a series on this sequence).
	want := []EdgeDimension{
		{"client_http.method", "GET"},
		{"client_k8s.cluster.name", "prod"},
		{"server_http.method", "GET"},
	}
	if !slices.Equal(e.Dimensions, want) {
		t.Fatalf("dimensions = %v, want %v", e.Dimensions, want)
	}
}

// The ids an exemplar needs reach the Edge: the trace id both halves share
// (they cannot pair without it) and each half's OWN span id. The server half's
// is emphatically NOT the id it paired on — that is its PARENT's, the client's
// span — or the exemplar on the server-latency histogram would point at the
// span that measured the other side.
func TestConsumeCarriesExemplarIDs(t *testing.T) {
	p, sink, _ := newTestProcessor(t, Config{})
	p.Consume(sgTraces("checkout", clientSpan(0.30, nil)))
	p.Consume(sgTraces("orders", serverSpan(0.25, nil)))

	e := sink.only(t)
	if e.TraceID != traceID(1) {
		t.Errorf("TraceID = %v, want the trace both halves carry", e.TraceID)
	}
	if e.ClientSpanID != spanID(1) {
		t.Errorf("ClientSpanID = %v, want the client span's own id", e.ClientSpanID)
	}
	if e.ServerSpanID != spanID(2) {
		t.Errorf("ServerSpanID = %v, want the server span's OWN id, not its parent's (%v)", e.ServerSpanID, spanID(1))
	}
}

// A promoted virtual-node edge keeps the surviving half's ids — the one side
// that was measured is the one an exemplar can point at, and the missing side
// contributes no histogram observation to attach one to.
func TestConsumeVirtualNodeCarriesTheObservedHalfsIDs(t *testing.T) {
	p, sink, clock := newTestProcessor(t, Config{Wait: "1s"})
	p.Consume(sgTraces("checkout", clientSpan(0.30, map[string]string{"peer.service": "orders-db"})))
	*clock = t0.Add(time.Second)
	p.Sweep()

	e := sink.only(t)
	if e.TraceID != traceID(1) || e.ClientSpanID != spanID(1) {
		t.Errorf("ids = %v/%v, want the client half's", e.TraceID, e.ClientSpanID)
	}
	if (e.ServerSpanID != pcommon.SpanID{}) {
		t.Errorf("ServerSpanID = %v, want zero: no server span was ever seen", e.ServerSpanID)
	}
}

func TestConsumeTruncatesLabelValues(t *testing.T) {
	long := strings.Repeat("x", maxDimensionValueBytes+10)
	p, sink, clock := newTestProcessor(t, Config{Wait: "1s", Dimensions: []string{"http.route"}})
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
	if v := dimsOf(e)["client_http.route"]; len(v) != maxDimensionValueBytes {
		t.Fatalf("dimension len = %d, want %d", len(v), maxDimensionValueBytes)
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

// A resource with no service.name — or an empty one — has nothing to name a
// graph node with: its spans are skipped for the graph at the RESOURCE level,
// counted into kubescrape_service_graph_unnamed_spans_total, and never reach
// the pairing store (a nameless half-edge could only ever mint a ""-labeled
// vertex shared by every unattributable sender). Consume does not consume the
// payload — forwarding is the tap's, and happens regardless — so the batch is
// untouched.
func TestConsumeSkipsResourcesWithoutServiceName(t *testing.T) {
	p, sink, _ := newTestProcessor(t, Config{})

	// One NAMED resource carrying a complete pair, one resource with NO
	// service.name (two spans), one with an EMPTY service.name (one span).
	td := sgTraces("checkout", clientSpan(0.1, nil), serverSpan(0.05, nil))
	anon := td.ResourceSpans().AppendEmpty().ScopeSpans().AppendEmpty().Spans()
	for i := byte(0); i < 2; i++ {
		s := anon.AppendEmpty()
		s.SetName("orphan")
		s.SetKind(ptrace.SpanKindClient)
		s.SetTraceID(traceID(7))
		s.SetSpanID(spanID(i + 1))
	}
	empty := td.ResourceSpans().AppendEmpty()
	empty.Resource().Attributes().PutStr(attrServiceName, "")
	s := empty.ScopeSpans().AppendEmpty().Spans().AppendEmpty()
	s.SetName("orphan")
	s.SetKind(ptrace.SpanKindClient)
	s.SetTraceID(traceID(8))
	s.SetSpanID(spanID(1))

	before := obs.ServiceGraphUnnamed.Value()
	p.Consume(td)

	if got := obs.ServiceGraphUnnamed.Value() - before; got != 3 {
		t.Errorf("unnamed counter delta = %v, want 3 (every span of both nameless resources)", got)
	}
	// The named resource still paired into its edge.
	if e := sink.only(t); e.ClientService != "checkout" || !e.HaveClient || !e.HaveServer {
		t.Fatalf("edge = %+v, want the named resource's pair", e)
	}
	// Nothing of the nameless resources reached the store: their client halves
	// would otherwise sit pending (and later expire into anonymous edges), and
	// none of them is unkeyable — the ids are fine, the resource is nameless,
	// and the two counters must not blur.
	if st := p.Stats(); st.Items != 0 || st.Unkeyable != 0 {
		t.Fatalf("stats = %+v, want no half-edge and no unkeyable count from the skipped resources", st)
	}
	// The payload is neither consumed nor trimmed — only the graph goes
	// without these spans.
	if n := td.SpanCount(); n != 5 {
		t.Fatalf("SpanCount = %d after Consume, want 5 (Consume must not mutate the batch)", n)
	}
}

// A root server span (no parent) keys under the zero span id: no client half
// can ever arrive, so it must promote against its peer attribute — that is the
// graph's entry edge from outside the mesh.
func TestConsumeRootServerSpanBecomesEntryEdge(t *testing.T) {
	p, sink, clock := newTestProcessor(t, Config{Wait: "1s"})
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

// One ticker Sweep must retire everything DUE, not sweepBudget of it.
//
// The cadence its caller picks is a promise: sweepInterval takes wait/2 so a
// promotable half-edge reaches the graph within 1.5x wait. One bounded pass per
// tick keeps that promise only while fewer than sweepBudget halves are pending —
// past that the sweep is a RATE, and the delay grows with the backlog: a full
// default store (MaxItems 10,000, sweepBudget 1,024) is ten ticks, so at the
// default 5s cadence its last due half-edge waits 45s against a promised 15s.
// The quiet shard this entry point exists for is where nothing else corrects it.
func TestSweepDrainsEverythingDue(t *testing.T) {
	p, sink, clock := newTestProcessor(t, Config{Wait: "1s", MaxItems: sweepBudget * 4})
	total := sweepBudget*2 + 100
	spans := make([]sgSpan, total)
	for i := range spans {
		// Both ids start at 1: a zero trace id is unkeyable by design, and so is
		// a zero SPAN id on a client half (it would key where every root server
		// span of its trace keys). Either would be skipped rather than stored.
		// peer.service makes each half PROMOTABLE, so the drain is observable as
		// virtual-node edges reaching the sink and not merely as an empty store.
		spans[i] = sgSpan{kind: ptrace.SpanKindClient, dur: 0.1,
			traceID: traceID(byte(i/200) + 1), spanID: spanID(byte(i%200) + 1),
			attrs: map[string]string{"peer.service": "orders"}}
	}
	// Distinct keys: 200 span ids x ceil(total/200) trace ids.
	p.Consume(sgTraces("checkout", spans...))
	if s := p.Stats(); s.Items != total {
		t.Fatalf("items = %d, want %d distinct keys", s.Items, total)
	}

	*clock = t0.Add(time.Second)
	p.Sweep()
	if s := p.Stats(); s.Items != 0 {
		t.Fatalf("items = %d after one Sweep, want 0: %d of %d due half-edges wait for a later tick, "+
			"so the promotion delay is a rate rather than the 1.5x wait the cadence promises",
			s.Items, s.Items, total)
	}
	if len(sink.edges) != total {
		t.Fatalf("sink got %d edges, want %d: the drained halves must reach the graph as virtual nodes", len(sink.edges), total)
	}
}

// …and the drain it does that with is spent in bounded LOCK HOLDS. The mutex
// sweepDue takes is the one every concurrent Consume needs and the one the sink
// runs under, so the drain above may not become one long hold: an operator who
// raises MaxItems would otherwise be lengthening a stall on the ingest path.
//
// It is pinned by OBSERVING THE RELEASE, not by calling expire with a budget and
// checking that expire honours it — that is expire's own contract and
// TestStoreExpireIsBounded already owns it, and a test spelled that way passes
// unchanged when sweepDue stops passing a budget at all (which is exactly how
// this test's predecessor failed to police the thing it was named after).
//
// The observable is kubescrape_service_graph_expired_total, and it is exact
// rather than timing-dependent: expire bumps it with the pass's count AFTER
// sweep has returned, i.e. strictly between two holds and never inside one. So
// the value the sink reads while an edge is being emitted names the hold that
// edge belongs to, and the edge index at which it first moves IS the size of the
// first hold. A drain that never released the mutex would emit every edge under
// one unchanged reading.
func TestSweepReleasesTheMutexEveryBudget(t *testing.T) {
	total := sweepBudget*2 + 100
	p := NewProcessor(Config{Wait: "1s", MaxItems: total * 2}, discardLog())
	clock := t0
	p.now = func() time.Time { return clock }
	sink := &passWatchSink{}
	p.SetSink(sink)

	p.Consume(unpairableClientSpans(0, total))
	if s := p.Stats(); s.Items != total {
		t.Fatalf("items = %d, want %d distinct keys", s.Items, total)
	}

	sink.watch = true
	base := obs.ServiceGraphExpired.Value()
	clock = t0.Add(time.Second)
	p.Sweep()

	if len(sink.seen) != total {
		t.Fatalf("sink saw %d edges, want %d", len(sink.seen), total)
	}
	// seen[i] is the counter as edge i+1 was emitted, so the first index whose
	// reading has moved off `base` is the number of edges the first hold retired.
	first := len(sink.seen)
	for i, v := range sink.seen {
		if v != base {
			first = i
			break
		}
	}
	if first == len(sink.seen) {
		t.Fatalf("all %d retirements happened inside ONE lock hold: a deep store stalls every "+
			"concurrent Consume, and the sink, for the whole drain", total)
	}
	if first != sweepBudget {
		t.Fatalf("the first lock release came after %d retirements, want sweepBudget = %d: "+
			"the per-hold ceiling is not the constant that documents it", first, sweepBudget)
	}
}

// passWatchSink records, for every edge the store emits, the expiry counter as
// it stood inside that hold. Called only from the sweeping goroutine, under the
// pairing mutex.
type passWatchSink struct {
	watch bool
	seen  []float64
}

func (s *passWatchSink) Record(Edge) {
	if s.watch {
		s.seen = append(s.seen, obs.ServiceGraphExpired.Value())
	}
}

// A sweep retires only what is DUE. The FIFO is expiry-ordered, so the pass
// stops at the first entry whose wait has not elapsed — draining past it would
// destroy half-edges whose partner is still legitimately in flight.
func TestSweepLeavesUndueHalvesAlone(t *testing.T) {
	p, sink, clock := newTestProcessor(t, Config{Wait: "10s", MaxItems: 100})
	p.Consume(unpairableClientSpans(0, 10))
	*clock = t0.Add(5 * time.Second) // half a wait later
	p.Consume(unpairableClientSpans(10, 10))

	*clock = t0.Add(11 * time.Second) // the first ten are due, the second ten are not
	p.Sweep()
	if s := p.Stats(); s.Items != 10 {
		t.Fatalf("items = %d after Sweep, want the 10 not-yet-due halves left", s.Items)
	}
	if len(sink.edges) != 10 {
		t.Fatalf("sink got %d edges, want 10", len(sink.edges))
	}
}

// TestExpiryKeepsUpAtRealisticRates: incremental expiry must keep pace with the
// SPAN rate, not with the BATCH rate.
//
// A fixed per-Consume budget is a per-BATCH bound, and the batch size is the
// SENDER's choice, not this shard's: at 1000 unpairable half-edges/s a fixed 32
// drained comfortably when senders pushed 20 spans at a time and fell far
// behind at 200, filling the store to maxItems and dropping spans — a memory
// bound that moved when someone tuned an SDK's batch processor. Since a span
// creates at most one half-edge, a budget that scales with the batch's span
// count can always keep pace whatever the batching.
//
// Sweep is deliberately never called here: the ticker covers a QUIET shard, and
// what is under test is that a BUSY one needs no help.
func TestExpiryKeepsUpAtRealisticRates(t *testing.T) {
	const (
		spansPerBatch = 200                    // a large but ordinary OTLP push
		batchPeriod   = 200 * time.Millisecond // => 1000 spans/s
		batches       = 300                    // 60 simulated seconds, 60k spans
		wait          = 5 * time.Second
		maxItems      = 10000 // 2x the steady-state window (wait x rate = 5000)
	)
	p, _, clock := newTestProcessor(t, Config{Wait: "5s", MaxItems: maxItems})

	n := uint64(0)
	for b := 0; b < batches; b++ {
		spans := make([]sgSpan, spansPerBatch)
		for i := range spans {
			// Every span its own trace: unpairable, so every one becomes a
			// half-edge that only expiry can retire — the worst case, and the
			// ordinary one for a service calling anything uninstrumented.
			spans[i] = sgSpan{kind: ptrace.SpanKindClient, dur: 0.01,
				traceID: fwdTraceID(n), spanID: spanID(1)}
			n++
		}
		p.Consume(sgTraces("checkout", spans...))
		*clock = clock.Add(batchPeriod)
	}

	st := p.Stats()
	if st.Dropped != 0 {
		t.Fatalf("dropped %d spans at %d spans/s in %d-span batches: expiry is bounded by the batch COUNT, not the span rate (stats %+v)",
			st.Dropped, int(time.Second/batchPeriod)*spansPerBatch, spansPerBatch, st)
	}
	// The live set must sit near the pairing window (wait x rate = 5000), not
	// creep toward the cap.
	if want := int(wait/batchPeriod) * spansPerBatch; st.Items > want*3/2 {
		t.Fatalf("items = %d after %d spans, want about %d (one wait window): the store is growing",
			st.Items, batches*spansPerBatch, want)
	}
	if st.Unpaired == 0 {
		t.Fatalf("nothing expired at all: stats = %+v", st)
	}
}

// TestZeroClientSpanIDCannotCrossPair: a CLIENT half with no span id of its own
// would key under (trace, zero) — exactly where every ROOT SERVER span of that
// trace keys, since a root has no parent. One malformed SDK would then pair its
// zero-id client spans with unrelated ingress hops and INVENT edges between
// services that never called each other, which is the worst failure this
// package has: a wrong graph is worse than a missing one, and nothing about the
// resulting edge looks wrong.
func TestZeroClientSpanIDCannotCrossPair(t *testing.T) {
	for _, kind := range []ptrace.SpanKind{ptrace.SpanKindClient, ptrace.SpanKindProducer} {
		t.Run(kind.String(), func(t *testing.T) {
			p, sink, clock := newTestProcessor(t, Config{Wait: "1s"})
			// Malformed: a real trace id, a ZERO span id.
			p.Consume(sgTraces("checkout", sgSpan{kind: kind, dur: 0.1,
				traceID: traceID(1), spanID: spanID(0)}))
			// A legitimate ROOT server span in the SAME trace — an ingress hop,
			// which is the graph's entry edge and must keep working.
			p.Consume(sgTraces("orders", sgSpan{kind: ptrace.SpanKindServer, dur: 0.2,
				traceID: traceID(1), spanID: spanID(9),
				attrs: map[string]string{"peer.service": "browser"}}))

			if len(sink.edges) != 0 {
				t.Fatalf("cross-paired into an invented edge: %+v", sink.edges)
			}
			if s := p.Stats(); s.Unkeyable != 1 {
				t.Errorf("stats = %+v, want the zero-id half counted unkeyable", s)
			}
			// The root server span is untouched: it still expires into the
			// entry edge its peer attribute names.
			*clock = t0.Add(time.Second)
			p.Sweep()
			e := sink.only(t)
			if e.ClientService != "browser" || e.ServerService != "orders" || e.VirtualNode != virtualNodeClient {
				t.Errorf("entry edge = %+v, want browser -> orders as a virtual client node", e)
			}
		})
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
	for b.Loop() {
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
	for b.Loop() {
		p.Consume(td)
	}
}

// The benchmark REPORTS the budget; this fails when it is broken. A completed
// request used to pay a map for its dimensions (two allocations per edge, on
// the shard's per-request path); an ordered slice built in the store's own
// recycled buffers pays none. A closure, a sort or a per-edge slice in this
// path would silently put it back.
func TestConsumeWithDimensionsIsAllocationFree(t *testing.T) {
	if testrace.Enabled {
		t.Skip("-race perturbs allocation counts")
	}
	p := NewProcessor(Config{Dimensions: []string{"http.method", "http.route"}}, nil)
	p.SetSink(&countSink{})
	attrs := map[string]string{"http.method": "GET", "http.route": "/api/v1/orders"}
	td := sgTraces("checkout", clientSpan(0.30, attrs), serverSpan(0.25, attrs))
	p.Consume(td) // warm the map, the free list and the join scratch
	if n := testing.AllocsPerRun(200, func() { p.Consume(td) }); n != 0 {
		t.Errorf("Consume allocates %v times per paired request, want 0", n)
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
	for b.Loop() {
		p.Consume(td)
	}
}

// TestConsumeWithDimensionsIsAllocationFree covers the completed edge; this
// covers the OTHER half of the shard's steady state — a span whose partner has
// not arrived, which is what every span costs until it pairs. A per-span map,
// a fmt call or a slice built per lookup would each land here.
func TestConsumeUnpairedIsAllocationFree(t *testing.T) {
	if testrace.Enabled {
		t.Skip("-race perturbs allocation counts")
	}
	p := NewProcessor(Config{MaxItems: 1 << 20}, nil)
	p.SetSink(&countSink{})
	spans := make([]sgSpan, 100)
	for i := range spans {
		spans[i] = sgSpan{kind: ptrace.SpanKindClient, dur: 0.1,
			traceID: traceID(byte(i/10) + 1), spanID: spanID(byte(i % 10))}
	}
	td := sgTraces("checkout", spans...)
	p.Consume(td) // admit the half-edges: only the warm path is budgeted
	if n := testing.AllocsPerRun(200, func() { p.Consume(td) }); n != 0 {
		t.Errorf("Consume allocates %v times per 100-span unpaired batch, want 0", n)
	}
}

// maxSweepPerBatch is a PER-LOCK-HOLD ceiling, not a total budget. Spending
// only that much of a larger batch-scaled budget made the scaling a lie: a
// batch carrying more unpairable half-edges than the ceiling expired fewer than
// it added, so occupancy climbed past the honest rate x wait working set and
// pinned at MaxItems — after which the store refuses arriving spans.
func TestConsumeSweepsItsWholeBatchBudget(t *testing.T) {
	const n = maxSweepPerBatch * 3
	p := NewProcessor(Config{Wait: "1s", MaxItems: n * 4}, discardLog())
	base := t0
	p.now = func() time.Time { return base }

	// n unpairable half-edges (client spans whose server half never arrives).
	p.Consume(unpairableClientSpans(0, n))
	if got := p.Stats().Items; got != n {
		t.Fatalf("setup: store holds %d, want %d", got, n)
	}

	// Everything is now due. One more batch of the same size must be able to
	// expire all of it: its budget is its own span count.
	p.now = func() time.Time { return base.Add(time.Hour) }
	p.Consume(unpairableClientSpans(n, n)) // a DISJOINT set of traces

	// The n new half-edges remain (they are not due yet at this clock); the n
	// old ones must all be gone. A per-hold-capped sweep leaves ~n-1024.
	if got := p.Stats().Items; got != n {
		t.Errorf("store holds %d after a batch whose budget covers the backlog, want %d — "+
			"the batch-scaled budget is being truncated to one lock-hold ceiling", got, n)
	}
}

// The SHUTDOWN sweep is the last chance to promote half-edges whose wait
// elapsed, and the shutdown path claims to emit them. A bounded pass silently
// discarded the remainder on a busy tier.
func TestSweepAllDrainsEverythingDue(t *testing.T) {
	const n = sweepBudget * 3
	p := NewProcessor(Config{Wait: "1s", MaxItems: n * 2}, discardLog())
	base := t0
	p.now = func() time.Time { return base }
	p.Consume(unpairableClientSpans(0, n))

	p.now = func() time.Time { return base.Add(time.Hour) }
	p.SweepAll()
	if got := p.Stats().Items; got != 0 {
		t.Errorf("SweepAll left %d due half-edges behind; the shutdown path claims to emit them", got)
	}
}

// unpairableClientSpans builds n client spans on DISTINCT traces starting at
// `from`, so none of them can ever pair and each becomes one expiring
// half-edge. The offset matters: reusing ids UPSERTS the existing half-edges
// instead of adding new ones, which silently makes an occupancy test vacuous.
func unpairableClientSpans(from, n int) ptrace.Traces {
	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	rs.Resource().Attributes().PutStr("service.name", "checkout")
	spans := rs.ScopeSpans().AppendEmpty().Spans()
	for i := range n {
		var tid pcommon.TraceID
		var sid pcommon.SpanID
		binary.BigEndian.PutUint64(tid[8:], uint64(from+i+1))
		binary.BigEndian.PutUint64(sid[:], uint64(from+i+1))
		sp := spans.AppendEmpty()
		sp.SetName("GET /orders")
		sp.SetKind(ptrace.SpanKindClient)
		sp.SetTraceID(tid)
		sp.SetSpanID(sid)
		sp.SetStartTimestamp(0)
		sp.SetEndTimestamp(1)
		sp.Attributes().PutStr("peer.service", "orders")
	}
	return td
}

// namesDatabase gates its comparison on the "db." prefix so a realistic span's
// attributes are walked once instead of four times. That gate is only correct
// while every member of databaseAttrs actually carries the prefix: one that did
// not would be unreachable, and the symptom — a database edge rendered as an
// ordinary service-to-service call — is invisible rather than loud. Same
// contract as agent/logscrub's prefilters, and the same reason it is a test.
func TestDatabaseAttrsAllCarryThePrefix(t *testing.T) {
	for _, a := range databaseAttrs {
		if len(a) <= len(dbAttrPrefix) || a[:len(dbAttrPrefix)] != dbAttrPrefix {
			t.Errorf("databaseAttrs member %q does not start with %q, so namesDatabase's prefix gate can never reach it", a, dbAttrPrefix)
		}
	}
}
