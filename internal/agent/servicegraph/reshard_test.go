package servicegraph

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/JohanLindvall/kubescrape/internal/agent/otlpexport"
)

// otlpexportConfigForTest is a flag-built base config as main would hand it to
// NewResharder: the COLLECTOR's endpoint and credentials.
func otlpexportConfigForTest() otlpexport.Config {
	return otlpexport.Config{
		Endpoint:        "otel-collector.observability.svc:4317",
		Protocol:        "grpc",
		Insecure:        true,
		BearerTokenFile: "/var/run/secrets/collector/token",
		Headers:         map[string]string{"X-Scope-OrgID": "tenant-a"},
		Compression:     "gzip",
		Timeout:         12 * time.Second,
		MaxSendBytes:    3 << 20,
	}
}

// --- fixtures ---

// captureExporter records every payload it is handed, and can be made to fail.
type captureExporter struct {
	mu   sync.Mutex
	got  []ptrace.Traces
	err  error
	call int
}

func (c *captureExporter) ExportTraces(_ context.Context, td ptrace.Traces) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.call++
	if c.err != nil {
		return c.err
	}
	c.got = append(c.got, td)
	return nil
}

func (c *captureExporter) spans() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := 0
	for _, td := range c.got {
		n += td.SpanCount()
	}
	return n
}

func (c *captureExporter) calls() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.call
}

// fwdTraceID builds a distinct, non-zero trace id per n. The +1 matters: an
// all-zero id is "no trace id", which is deliberately kept local, so a fixture
// starting at 0 would silently test one trace fewer than it claims.
func fwdTraceID(n uint64) pcommon.TraceID {
	n++
	var id pcommon.TraceID
	for i := 0; i < 8; i++ {
		id[i] = byte(n >> (8 * i))
		id[8+i] = byte((n * 0x9e3779b97f4a7c15) >> (8 * i))
	}
	return id
}

func fwdSpanID(n uint64) pcommon.SpanID {
	var id pcommon.SpanID
	for i := 0; i < 8; i++ {
		id[i] = byte((n + 1) >> (8 * i))
	}
	return id
}

// realisticResource is an ENRICHED resource as the tier's entry path produces
// it: the service identity plus the k8s attributes plus the operator's static
// ones.
func realisticResource(res pcommon.Resource, svc string) {
	a := res.Attributes()
	a.PutStr("service.name", svc)
	a.PutStr("service.namespace", "shop")
	a.PutStr("service.instance.id", "shop/"+svc+"-7d9f8c6b5d-x2k9p")
	a.PutStr("service.version", "2.14.3")
	a.PutStr("k8s.namespace.name", "shop")
	a.PutStr("k8s.pod.name", svc+"-7d9f8c6b5d-x2k9p")
	a.PutStr("k8s.pod.uid", "6f1a2b3c-4d5e-6f70-8192-a3b4c5d6e7f8")
	a.PutStr("k8s.container.name", svc)
	a.PutStr("k8s.node.name", "ip-10-42-3-17.eu-north-1.compute.internal")
	a.PutStr("k8s.deployment.name", svc)
	a.PutStr("k8s.pod.ip", "10.42.3.91")
	a.PutStr("cloud.region", "eu-north-1")
	a.PutStr("telemetry.sdk.language", "go")
	a.PutStr("telemetry.sdk.name", "opentelemetry")
	a.PutStr("telemetry.sdk.version", "1.32.0")
}

// realisticSpan is a typical instrumented HTTP span: 15 attributes, 2 events, a
// status message, a name.
func realisticSpan(s ptrace.Span, kind ptrace.SpanKind, tid pcommon.TraceID, sid, parent pcommon.SpanID) {
	s.SetTraceID(tid)
	s.SetSpanID(sid)
	s.SetParentSpanID(parent)
	s.SetKind(kind)
	s.SetName("GET /api/v2/orders/{orderId}/shipments")
	s.SetStartTimestamp(pcommon.Timestamp(1_750_000_000_000_000_000))
	s.SetEndTimestamp(pcommon.Timestamp(1_750_000_000_042_000_000))
	s.Status().SetCode(ptrace.StatusCodeError)
	s.Status().SetMessage("upstream returned 503 Service Unavailable after 3 attempts")
	a := s.Attributes()
	a.PutStr("http.request.method", "GET")
	a.PutStr("url.full", "https://orders.shop.svc.cluster.local/api/v2/orders/8f3a/shipments?expand=carrier")
	a.PutStr("url.path", "/api/v2/orders/8f3a/shipments")
	a.PutStr("url.scheme", "https")
	a.PutStr("server.address", "orders.shop.svc.cluster.local")
	a.PutInt("server.port", 8443)
	a.PutInt("http.response.status_code", 503)
	a.PutStr("user_agent.original", "checkout/2.14.3 (linux; go1.24.2) otelhttp/0.58.0")
	a.PutStr("network.protocol.version", "1.1")
	a.PutStr("peer.service", "orders")
	a.PutStr("enduser.id", "usr_01HQ8ZK3M4N5P6Q7R8S9T0V1W2")
	a.PutStr("shop.tenant", "acme-nordics")
	a.PutBool("http.request.resend", true)
	a.PutDouble("http.request.body.size", 0)
	e := s.Events().AppendEmpty()
	e.SetName("exception")
	e.Attributes().PutStr("exception.type", "net/http: request canceled (Client.Timeout exceeded)")
	e.Attributes().PutStr("exception.stacktrace", "goroutine 421 [running]:\nnet/http.(*Client).do(...)\n\t/usr/local/go/src/net/http/client.go:724")
	e2 := s.Events().AppendEmpty()
	e2.SetName("retry")
	e2.Attributes().PutInt("retry.attempt", 3)
}

// realisticBatch builds one resource with edge and INTERNAL spans, as an
// instrumented service actually pushes: internal spans dominate.
func realisticBatch(traces, internalPerTrace int) ptrace.Traces {
	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	realisticResource(rs.Resource(), "checkout")
	ss := rs.ScopeSpans().AppendEmpty()
	ss.Scope().SetName("go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp")
	ss.Scope().SetVersion("0.58.0")
	for i := 0; i < traces; i++ {
		tid := fwdTraceID(uint64(i))
		realisticSpan(ss.Spans().AppendEmpty(), ptrace.SpanKindServer, tid, fwdSpanID(uint64(i)*10), pcommon.SpanID{})
		realisticSpan(ss.Spans().AppendEmpty(), ptrace.SpanKindClient, tid, fwdSpanID(uint64(i)*10+1), fwdSpanID(uint64(i)*10))
		for j := 0; j < internalPerTrace; j++ {
			realisticSpan(ss.Spans().AppendEmpty(), ptrace.SpanKindInternal, tid, fwdSpanID(uint64(i)*10+2+uint64(j)), fwdSpanID(uint64(i)*10+1))
		}
	}
	return td
}

// fakeShards builds n PEER shards (shard-1 .. shard-n) plus the name "shard-0"
// for the process under test, which never gets a client of its own.
func fakeShards(t testing.TB, n int) (map[string]TracesExporter, []*captureExporter) {
	t.Helper()
	m := make(map[string]TracesExporter, n)
	caps := make([]*captureExporter, n)
	for i := 0; i < n; i++ {
		c := &captureExporter{}
		caps[i] = c
		m[fmt.Sprintf("shard-%d", i+1)] = c
	}
	return m, caps
}

const selfShard = "shard-0"

func testResharder(t testing.TB, clients map[string]TracesExporter, tokensPerShard int) *Resharder {
	t.Helper()
	return NewResharderWithClients(selfShard, clients, tokensPerShard, discardLog())
}

func discardLog() *slog.Logger { return slog.New(slog.DiscardHandler) }

func marshal(t testing.TB, td ptrace.Traces) []byte {
	t.Helper()
	b, err := (&ptrace.ProtoMarshaler{}).MarshalTraces(td)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return b
}

// spanIDsOf collects every (trace, span) pair in a payload.
func spanIDsOf(td ptrace.Traces) map[[2]string]int {
	out := map[[2]string]int{}
	rss := td.ResourceSpans()
	for i := 0; i < rss.Len(); i++ {
		sss := rss.At(i).ScopeSpans()
		for j := 0; j < sss.Len(); j++ {
			spans := sss.At(j).Spans()
			for k := 0; k < spans.Len(); k++ {
				s := spans.At(k)
				out[[2]string{s.TraceID().String(), s.SpanID().String()}]++
			}
		}
	}
	return out
}

// --- routing ---

// TestEverySpanIsDeliveredExactlyOnce is the invariant the whole hop exists to
// hold: the entry shard is the only copy of a pushed span, so the union of what
// it forwards and what it keeps must be the input, with nothing duplicated and
// nothing dropped.
func TestEverySpanIsDeliveredExactlyOnce(t *testing.T) {
	clients, caps := fakeShards(t, 7)
	r := testResharder(t, clients, 0)
	in := realisticBatch(500, 2)
	want := spanIDsOf(in)

	local, err := r.Reshard(context.Background(), in)
	if err != nil {
		t.Fatalf("Reshard: %v", err)
	}

	got := spanIDsOf(local)
	for _, c := range caps {
		for _, td := range c.got {
			for k, n := range spanIDsOf(td) {
				got[k] += n
			}
		}
	}
	if len(got) != len(want) {
		t.Fatalf("delivered %d distinct spans, want %d", len(got), len(want))
	}
	for k, n := range got {
		if n != 1 {
			t.Fatalf("span %v delivered %d times", k, n)
		}
		if want[k] == 0 {
			t.Fatalf("span %v was invented", k)
		}
	}
	st := r.Stats()
	if int(st.SpansForwarded+st.SpansLocal) != in.SpanCount() {
		t.Errorf("stats = %+v, want SpansForwarded+SpansLocal = %d", st, in.SpanCount())
	}
	if st.SendsFailed != 0 {
		t.Errorf("stats = %+v, want no failed sends", st)
	}
}

// TestBothHalvesOfATraceLandOnOneShard: two entry shards see the two halves of
// the same request (the tier's Service round-robins), and both must route them
// to the SAME owner — otherwise no edge can ever form, which is the failure the
// ring exists to prevent.
func TestBothHalvesOfATraceLandOnOneShard(t *testing.T) {
	const peers, traces = 7, 2000
	clientsA, capsA := fakeShards(t, peers)
	clientsB, capsB := fakeShards(t, peers)
	entryA := testResharder(t, clientsA, 0)
	entryB := testResharder(t, clientsB, 0)

	mk := func(kind ptrace.SpanKind, svc string) ptrace.Traces {
		td := ptrace.NewTraces()
		rs := td.ResourceSpans().AppendEmpty()
		realisticResource(rs.Resource(), svc)
		ss := rs.ScopeSpans().AppendEmpty()
		for i := 0; i < traces; i++ {
			realisticSpan(ss.Spans().AppendEmpty(), kind, fwdTraceID(uint64(i)), fwdSpanID(uint64(i)), pcommon.SpanID{})
		}
		return td
	}
	localA, err := entryA.Reshard(context.Background(), mk(ptrace.SpanKindClient, "checkout"))
	if err != nil {
		t.Fatalf("Reshard A: %v", err)
	}
	localB, err := entryB.Reshard(context.Background(), mk(ptrace.SpanKindServer, "orders"))
	if err != nil {
		t.Fatalf("Reshard B: %v", err)
	}

	// Per OWNER (the local share counts as owner selfShard), the set of trace ids.
	collect := func(local ptrace.Traces, caps []*captureExporter) map[string]map[pcommon.TraceID]bool {
		out := map[string]map[pcommon.TraceID]bool{}
		add := func(owner string, td ptrace.Traces) {
			m := out[owner]
			if m == nil {
				m = map[pcommon.TraceID]bool{}
				out[owner] = m
			}
			rss := td.ResourceSpans()
			for r := 0; r < rss.Len(); r++ {
				sss := rss.At(r).ScopeSpans()
				for s := 0; s < sss.Len(); s++ {
					spans := sss.At(s).Spans()
					for k := 0; k < spans.Len(); k++ {
						m[spans.At(k).TraceID()] = true
					}
				}
			}
		}
		add(selfShard, local)
		for i, c := range caps {
			for _, td := range c.got {
				add(fmt.Sprintf("shard-%d", i+1), td)
			}
		}
		return out
	}
	byOwnerA, byOwnerB := collect(localA, capsA), collect(localB, capsB)

	if len(byOwnerA) != peers+1 {
		t.Errorf("only %d of %d shards received traces", len(byOwnerA), peers+1)
	}
	seen := map[pcommon.TraceID]int{}
	for owner, ids := range byOwnerA {
		if len(ids) != len(byOwnerB[owner]) {
			t.Errorf("%s got %d client halves but %d server halves", owner, len(ids), len(byOwnerB[owner]))
		}
		for tid := range ids {
			if !byOwnerB[owner][tid] {
				t.Fatalf("trace %x: the client half went to %s, the server half did not", tid, owner)
			}
			seen[tid]++
		}
	}
	if len(seen) != traces {
		t.Errorf("%d distinct traces routed, want %d", len(seen), traces)
	}
	for tid, n := range seen {
		if n != 1 {
			t.Errorf("trace %x was routed to %d owners", tid, n)
		}
	}
}

// TestSpansThisShardOwnsTakeNoHop: the share this shard already owns must stay
// in-process. It is 1/N of the tier's traffic, and sending it over the network
// to ourselves would double the internal bandwidth for nothing.
func TestSpansThisShardOwnsTakeNoHop(t *testing.T) {
	clients, _ := fakeShards(t, 3)
	r := testResharder(t, clients, 0)
	local, err := r.Reshard(context.Background(), realisticBatch(400, 0))
	if err != nil {
		t.Fatalf("Reshard: %v", err)
	}
	if local.SpanCount() == 0 {
		t.Fatal("this shard kept nothing; 1/4 of the ring is its own")
	}
	if st := r.Stats(); st.SpansLocal != uint64(local.SpanCount()) {
		t.Errorf("SpansLocal = %d, want %d", st.SpansLocal, local.SpanCount())
	}
	if _, ok := local.ResourceSpans().At(0).Resource().Attributes().Get(ForwardedMarker); ok {
		t.Error("the local share carries the forward marker; it never crossed a wire")
	}
}

// TestSingleShardTierIsPassthrough: with one shard there is nothing to hash, and
// the payload must not be copied at all.
func TestSingleShardTierIsPassthrough(t *testing.T) {
	r := testResharder(t, nil, 0)
	in := realisticBatch(3, 1)
	local, err := r.Reshard(context.Background(), in)
	if err != nil {
		t.Fatalf("Reshard: %v", err)
	}
	if local != in {
		t.Error("a single-shard tier copied the payload")
	}
	// And a nil resharder (the disabled case) behaves identically.
	var nilR *Resharder
	if out, err := nilR.Reshard(context.Background(), in); err != nil || out != in {
		t.Errorf("nil Reshard = %v, %v; want the input back", out, err)
	}
}

// TestSpansWithNoTraceIDStayLocal: they cannot be hashed, and every zero id
// hashes to the same token — routing them would pile the cluster's malformed
// spans onto one shard. They must still be delivered, though: they are real
// spans and the collector is expecting them.
func TestSpansWithNoTraceIDStayLocal(t *testing.T) {
	clients, caps := fakeShards(t, 3)
	r := testResharder(t, clients, 0)
	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	realisticResource(rs.Resource(), "checkout")
	ss := rs.ScopeSpans().AppendEmpty()
	for i := 0; i < 10; i++ {
		realisticSpan(ss.Spans().AppendEmpty(), ptrace.SpanKindClient, pcommon.TraceID{}, fwdSpanID(uint64(i)), pcommon.SpanID{})
	}
	local, err := r.Reshard(context.Background(), td)
	if err != nil {
		t.Fatalf("Reshard: %v", err)
	}
	if local.SpanCount() != 10 {
		t.Errorf("kept %d trace-id-less spans locally, want 10", local.SpanCount())
	}
	for i, c := range caps {
		if c.calls() != 0 {
			t.Errorf("shard %d received a trace-id-less span", i+1)
		}
	}
	if st := r.Stats(); st.SpansUnkeyed != 10 {
		t.Errorf("SpansUnkeyed = %d, want 10", st.SpansUnkeyed)
	}
}

// TestInternalSpansAreForwarded: the hop carries the WHOLE trace now, because
// the owner is what exports it. Dropping INTERNAL spans here — which the
// pairing store ignores — would silently delete most of every trace from the
// collector.
func TestInternalSpansAreForwarded(t *testing.T) {
	clients, caps := fakeShards(t, 3)
	r := testResharder(t, clients, 0)
	in := realisticBatch(200, 3)
	local, err := r.Reshard(context.Background(), in)
	if err != nil {
		t.Fatalf("Reshard: %v", err)
	}
	total := local.SpanCount()
	internal := 0
	count := func(td ptrace.Traces) {
		rss := td.ResourceSpans()
		for i := 0; i < rss.Len(); i++ {
			sss := rss.At(i).ScopeSpans()
			for j := 0; j < sss.Len(); j++ {
				spans := sss.At(j).Spans()
				for k := 0; k < spans.Len(); k++ {
					if spans.At(k).Kind() == ptrace.SpanKindInternal {
						internal++
					}
				}
			}
		}
	}
	count(local)
	for _, c := range caps {
		total += c.spans()
		for _, td := range c.got {
			count(td)
		}
	}
	if total != in.SpanCount() {
		t.Errorf("delivered %d spans, want %d", total, in.SpanCount())
	}
	if internal != 600 {
		t.Errorf("delivered %d INTERNAL spans, want 600 (they belong to the trace the collector receives)", internal)
	}
}

// TestForwardedPayloadKeepsFullFidelity: the owner exports what it receives, so
// span names, events, status messages, attributes, scope identity and schema
// urls all have to survive the hop.
func TestForwardedPayloadKeepsFullFidelity(t *testing.T) {
	clients, caps := fakeShards(t, 1)
	r := testResharder(t, clients, 0)
	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	realisticResource(rs.Resource(), "checkout")
	rs.SetSchemaUrl("https://opentelemetry.io/schemas/1.30.0")
	ss := rs.ScopeSpans().AppendEmpty()
	ss.Scope().SetName("go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp")
	ss.Scope().SetVersion("0.58.0")
	ss.SetSchemaUrl("https://opentelemetry.io/schemas/1.30.0")
	// Two traces, so the payload actually splits rather than taking the
	// single-owner fast path; keep pushing until one lands on the peer.
	for i := 0; i < 64; i++ {
		realisticSpan(ss.Spans().AppendEmpty(), ptrace.SpanKindClient, fwdTraceID(uint64(i)), fwdSpanID(uint64(i)), pcommon.SpanID{})
	}
	if _, err := r.Reshard(context.Background(), td); err != nil {
		t.Fatalf("Reshard: %v", err)
	}
	if len(caps[0].got) == 0 {
		t.Fatal("the peer shard received nothing")
	}
	out := caps[0].got[0]
	ors := out.ResourceSpans().At(0)
	if ors.SchemaUrl() != "https://opentelemetry.io/schemas/1.30.0" {
		t.Errorf("resource schema url = %q", ors.SchemaUrl())
	}
	if got := ors.Resource().Attributes().Len(); got < 15 {
		t.Errorf("resource carries %d attributes, want the sender's whole set (+ the marker)", got)
	}
	oss := ors.ScopeSpans().At(0)
	if oss.Scope().Name() != "go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp" || oss.Scope().Version() != "0.58.0" {
		t.Errorf("scope identity lost: %q %q", oss.Scope().Name(), oss.Scope().Version())
	}
	if oss.SchemaUrl() != "https://opentelemetry.io/schemas/1.30.0" {
		t.Errorf("scope schema url = %q", oss.SchemaUrl())
	}
	sp := oss.Spans().At(0)
	if sp.Name() == "" || sp.Events().Len() != 2 || sp.Status().Message() == "" || sp.Attributes().Len() < 14 {
		t.Errorf("span fidelity lost: name=%q events=%d status=%q attrs=%d",
			sp.Name(), sp.Events().Len(), sp.Status().Message(), sp.Attributes().Len())
	}
}

// --- failure ---

// TestFailedHopFailsThePush: a dropped hop is a lost SPAN, not a lost edge, so
// the entry shard must refuse the application's push and let the sender retry.
// (This is the difference from a best-effort side channel, and getting it wrong
// silently deletes 1/N of the cluster's traces.)
func TestFailedHopFailsThePush(t *testing.T) {
	clients, caps := fakeShards(t, 3)
	boom := errors.New("connection refused")
	caps[1].err = boom
	r := testResharder(t, clients, 0)

	_, err := r.Reshard(context.Background(), realisticBatch(200, 0))
	if err == nil {
		t.Fatal("Reshard succeeded with a shard that refused its share")
	}
	if !errors.Is(err, boom) {
		t.Errorf("error = %v, want it to wrap the shard's own", err)
	}
	if st := r.Stats(); st.SendsFailed == 0 {
		t.Errorf("stats = %+v, want a failed send", st)
	}
}

// TestForwardedMarkerIsStampedAndStripped: the marker rides the wire so the
// ingest path can refuse a hop addressed to the wrong port, and the owner takes
// it off so application telemetry never reaches the collector carrying
// kubescrape's internal plumbing.
func TestForwardedMarkerIsStampedAndStripped(t *testing.T) {
	clients, caps := fakeShards(t, 3)
	r := testResharder(t, clients, 0)
	if _, err := r.Reshard(context.Background(), realisticBatch(200, 0)); err != nil {
		t.Fatalf("Reshard: %v", err)
	}
	marked := 0
	for _, c := range caps {
		for _, td := range c.got {
			if !IsForwarded(td) {
				t.Error("a forwarded payload carries no marker; a hop addressed to the ingest port could not be refused")
			}
			marked++
			StripForwarded(td)
			if IsForwarded(td) {
				t.Error("StripForwarded left the marker on")
			}
		}
	}
	if marked == 0 {
		t.Fatal("nothing was forwarded")
	}
}

// TestIsForwardedFindsAMarkerOnAnyResource: a payload is refused if ANY of its
// resources is marked, not just the first — the split groups by resource, so a
// looped payload can carry a mix.
func TestIsForwardedFindsAMarkerOnAnyResource(t *testing.T) {
	td := ptrace.NewTraces()
	realisticResource(td.ResourceSpans().AppendEmpty().Resource(), "a")
	rs := td.ResourceSpans().AppendEmpty()
	realisticResource(rs.Resource(), "b")
	if IsForwarded(td) {
		t.Fatal("an unmarked payload reported as forwarded")
	}
	rs.Resource().Attributes().PutBool(ForwardedMarker, true)
	if !IsForwarded(td) {
		t.Fatal("a marker on the second resource was missed")
	}
}

// TestCountLoopBlocked: the counter belongs to the resharder even though the
// refusal happens on the receive path, and it tolerates a nil receiver (the
// single-shard tier has none).
func TestCountLoopBlocked(t *testing.T) {
	r := testResharder(t, nil, 0)
	r.CountLoopBlocked(12)
	if st := r.Stats(); st.LoopsBlocked != 12 {
		t.Errorf("LoopsBlocked = %d, want 12", st.LoopsBlocked)
	}
	var nilR *Resharder
	nilR.CountLoopBlocked(3) // must not panic
	if st := nilR.Stats(); st != (ReshardStats{}) {
		t.Errorf("nil Stats = %+v, want the zero value", st)
	}
}

// TestReshardDoesNotMutateTheLocalShare: the payload handed back for local
// processing must still be exactly what the sender pushed for the traces it
// covers — the split copies, it does not edit.
func TestReshardDoesNotMutateTheLocalShare(t *testing.T) {
	r := testResharder(t, nil, 0) // single shard: the whole payload is local
	in := realisticBatch(5, 2)
	before := marshal(t, in)
	local, err := r.Reshard(context.Background(), in)
	if err != nil {
		t.Fatalf("Reshard: %v", err)
	}
	if string(marshal(t, local)) != string(before) {
		t.Error("the local share differs from the pushed payload")
	}
}

// --- config ---

func TestShardTargetsFromTemplate(t *testing.T) {
	cfg := ReshardConfig{StatefulSet: "kubescrape-servicegraph", Replicas: 3, Namespace: "monitoring"}
	got, err := cfg.shardTargets()
	if err != nil {
		t.Fatalf("shardTargets: %v", err)
	}
	// 4319, not 4317/4318: the default MUST be the INTERNAL receiver's port. The
	// application-facing ingest ports on the same pods would loop.
	if DefaultShardPort != 4319 {
		t.Fatalf("DefaultShardPort = %d, want 4319 (the internal receiver's own default)", DefaultShardPort)
	}
	want := []shardTarget{
		{name: "kubescrape-servicegraph-0", endpoint: "kubescrape-servicegraph-0.kubescrape-servicegraph.monitoring.svc:4319"},
		{name: "kubescrape-servicegraph-1", endpoint: "kubescrape-servicegraph-1.kubescrape-servicegraph.monitoring.svc:4319"},
		{name: "kubescrape-servicegraph-2", endpoint: "kubescrape-servicegraph-2.kubescrape-servicegraph.monitoring.svc:4319"},
	}
	if len(got) != len(want) {
		t.Fatalf("got %d targets, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("target %d = %+v, want %+v", i, got[i], want[i])
		}
	}

	// The HTTP protocol needs a scheme, and every way of asking for TLS must
	// produce https — otlpexport ignores Insecure for HTTP, so here the scheme IS
	// the TLS decision, and a request for TLS that fell through to plaintext
	// would put the shared bearer token on the wire in the clear.
	base := ReshardConfig{StatefulSet: "kubescrape-servicegraph", Replicas: 1,
		Namespace: "monitoring", Protocol: "http", Port: 4318}
	const plain = "http://kubescrape-servicegraph-0.kubescrape-servicegraph.monitoring.svc:4318"
	const secure = "https://kubescrape-servicegraph-0.kubescrape-servicegraph.monitoring.svc:4318"
	yes, no := true, false
	for _, tc := range []struct {
		name string
		mut  func(*ReshardConfig)
		want string
	}{
		{"nothing configured", func(*ReshardConfig) {}, plain},
		{"caFile", func(c *ReshardConfig) { c.CAFile = "/etc/ca.pem" }, secure},
		{"insecureSkipVerify", func(c *ReshardConfig) { c.InsecureSkipVerify = &yes }, secure},
		{"insecure: false", func(c *ReshardConfig) { c.Insecure = &no }, secure},
		// An explicit insecure wins, so caFile beside it is a contradiction —
		// which otlpexport.New then refuses loudly rather than quietly
		// handshaking or quietly not.
		{"insecure: true beats a caFile", func(c *ReshardConfig) { c.Insecure, c.CAFile = &yes, "/etc/ca.pem" }, plain},
	} {
		t.Run("http/"+tc.name, func(t *testing.T) {
			c := base
			tc.mut(&c)
			got, err := c.shardTargets()
			if err != nil {
				t.Fatalf("shardTargets: %v", err)
			}
			if got[0].endpoint != tc.want {
				t.Errorf("endpoint = %q, want %q", got[0].endpoint, tc.want)
			}
			// And the exporter config agrees with the scheme it just derived.
			cc := c.clientConfig(got[0], otlpexportConfigForTest())
			if wantInsecure := tc.want == plain; cc.Insecure != wantInsecure {
				t.Errorf("clientConfig Insecure = %v, want %v (it must match the scheme)", cc.Insecure, wantInsecure)
			}
		})
	}

	// Explicit endpoints win, and the name IS the endpoint.
	cfg.Endpoints = []string{"sg-a:4319", " sg-b:4319 "}
	got, _ = cfg.shardTargets()
	if len(got) != 2 || got[0].name != "sg-a:4319" || got[1].endpoint != "sg-b:4319" {
		t.Errorf("explicit endpoints = %+v", got)
	}
}

func TestReshardConfigValidate(t *testing.T) {
	for _, tc := range []struct {
		name    string
		cfg     ReshardConfig
		wantErr bool
		enabled bool
	}{
		{name: "empty"},
		{name: "template", cfg: ReshardConfig{StatefulSet: "sg", Replicas: 2}, enabled: true},
		{name: "endpoints", cfg: ReshardConfig{Endpoints: []string{"a:4319"}}, enabled: true},
		{name: "half-filled template", cfg: ReshardConfig{StatefulSet: "sg"}, wantErr: true},
		{name: "replicas without name", cfg: ReshardConfig{Replicas: 3}, wantErr: true},
		{name: "bad protocol", cfg: ReshardConfig{StatefulSet: "sg", Replicas: 1, Protocol: "htttp"}, wantErr: true, enabled: true},
		{name: "bad port", cfg: ReshardConfig{StatefulSet: "sg", Replicas: 1, Port: 70000}, wantErr: true, enabled: true},
		{name: "empty endpoint", cfg: ReshardConfig{Endpoints: []string{" "}}, wantErr: true, enabled: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.cfg.Enabled(); got != tc.enabled {
				t.Errorf("Enabled = %v, want %v", got, tc.enabled)
			}
			if err := tc.cfg.Validate(); (err != nil) != tc.wantErr {
				t.Errorf("Validate = %v, wantErr %v", err, tc.wantErr)
			}
		})
	}
}

// TestClientConfigDoesNotInheritCredentials: the base config points at the
// COLLECTOR. Inheriting its bearer token would present that credential to a
// sibling shard because a field was left empty.
func TestClientConfigDoesNotInheritCredentials(t *testing.T) {
	base := otlpexportConfigForTest()
	cfg := ReshardConfig{StatefulSet: "sg", Replicas: 1, Namespace: "monitoring", BearerTokenFile: "/var/run/sg/token"}
	got := cfg.clientConfig(shardTarget{name: "sg-0", endpoint: "sg-0:4319"}, base)

	if got.BearerTokenFile != "/var/run/sg/token" {
		t.Errorf("bearer token file = %q", got.BearerTokenFile)
	}
	if got.Headers != nil {
		t.Errorf("base headers leaked to the shard: %v", got.Headers)
	}
	if got.CAFile != "" || got.ClientCertFile != "" {
		t.Errorf("base TLS material leaked to the shard: %q %q", got.CAFile, got.ClientCertFile)
	}
	if got.Endpoint != "sg-0:4319" {
		t.Errorf("endpoint = %q", got.Endpoint)
	}
	// Transport tuning IS inherited.
	if got.Timeout != base.Timeout || got.Compression != base.Compression || got.MaxSendBytes != base.MaxSendBytes {
		t.Errorf("transport tuning not inherited: %+v", got)
	}
	if got.RetryAttempts != 1 {
		t.Errorf("retry attempts = %d, want 1 (the application owns the retry)", got.RetryAttempts)
	}
	if !got.Insecure {
		t.Errorf("default should be plaintext in-cluster when no CA is configured")
	}
}

// TestNewResharderDisabled: wiring it unconditionally must be free, and a
// single-shard tier is legitimately "disabled".
func TestNewResharderDisabled(t *testing.T) {
	r, err := NewResharder(ReshardConfig{}, otlpexportConfigForTest(), discardLog())
	if err != nil || r != nil {
		t.Fatalf("NewResharder(disabled) = %v, %v; want nil, nil", r, err)
	}
	if _, err := r.Reshard(context.Background(), realisticBatch(1, 0)); err != nil {
		t.Fatalf("nil Reshard: %v", err)
	}
	if err := r.Close(); err != nil {
		t.Fatalf("nil Close: %v", err)
	}
}

// TestNewResharderBuildsRingAndSkipsSelf: the ring's names come from the
// template and include this pod, but no client is opened to ourselves.
func TestNewResharderBuildsRingAndSkipsSelf(t *testing.T) {
	r, err := NewResharder(ReshardConfig{StatefulSet: "sg", Replicas: 3, Namespace: "monitoring", Self: "sg-1"},
		otlpexportConfigForTest(), discardLog())
	if err != nil {
		t.Fatalf("NewResharder: %v", err)
	}
	defer func() { _ = r.Close() }()
	want := []string{"sg-0", "sg-1", "sg-2"}
	got := r.Ring().Shards()
	if len(got) != len(want) {
		t.Fatalf("ring shards = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("ring shards = %v, want %v", got, want)
		}
	}
	if _, ok := r.clients["sg-1"]; ok {
		t.Error("a client was opened to this shard itself")
	}
	if len(r.clients) != 2 {
		t.Errorf("%d shard clients, want 2 (three shards minus self)", len(r.clients))
	}
}

// --- benchmark ---

// BenchmarkReshard measures what the entry shard pays per pushed batch: the
// owner walk plus, when the batch spans several owners, the per-span copy. The
// SEND is a no-op here; the cost being measured is the routing itself.
func BenchmarkReshard(b *testing.B) {
	clients := map[string]TracesExporter{}
	for i := 1; i < 8; i++ {
		clients[fmt.Sprintf("shard-%d", i)] = nopExporter{}
	}
	r := testResharder(b, clients, 0)
	td := realisticBatch(20, 3)
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := r.Reshard(ctx, td); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/float64(td.SpanCount()), "ns/span")
}

// BenchmarkReshardSingleOwner is the fast path: a one-shard tier (and any batch
// whose traces hash to one owner) must not copy anything.
func BenchmarkReshardSingleOwner(b *testing.B) {
	r := testResharder(b, nil, 0)
	td := realisticBatch(20, 3)
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := r.Reshard(ctx, td); err != nil {
			b.Fatal(err)
		}
	}
}

type nopExporter struct{}

func (nopExporter) ExportTraces(context.Context, ptrace.Traces) error { return nil }
