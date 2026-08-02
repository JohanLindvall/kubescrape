package servicegraph

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/JohanLindvall/kubescrape/internal/agent/otlpexport"
)

// otlpexportConfigForTest is a flag-built base config as main would hand it to
// NewForwarder: the COLLECTOR's endpoint and credentials.
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
// all-zero id is "no trace id" and is deliberately dropped by the trim, so a
// fixture starting at 0 would silently test one trace fewer than it claims.
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

// realisticResource is an ENRICHED resource as the ingest path produces it:
// the service identity plus the agent's k8s attributes plus the operator's
// static ones. The trim keeps 8 of these keys at most.
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

// realisticSpan is a typical instrumented HTTP span: 15 attributes, 2 events,
// a status message, a name.
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
	a.PutStr("db.system", "")
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

func fakeShards(t testing.TB, n int) (map[string]TracesExporter, []*captureExporter) {
	t.Helper()
	m := make(map[string]TracesExporter, n)
	caps := make([]*captureExporter, n)
	for i := 0; i < n; i++ {
		c := &captureExporter{}
		caps[i] = c
		m[fmt.Sprintf("shard-%d", i)] = c
	}
	return m, caps
}

// testForwarder builds a forwarder over fake shard clients and closes it when
// the test ends: the fan-out owns one goroutine per shard (see Forward), and a
// test that leaked them would leak one per case.
func testForwarder(t testing.TB, clients map[string]TracesExporter, tokensPerShard int, dims, peers []string) *Forwarder {
	t.Helper()
	f := NewForwarderWithClients(clients, tokensPerShard, dims, peers, discardLog())
	t.Cleanup(func() { _ = f.Close() })
	return f
}

// waitForwarded blocks until every queued payload has been delivered (or has
// failed). The shard fan-out is ASYNCHRONOUS by design — Forward hands each
// shard's share to a bounded queue and returns, so the ingest handler is never
// parked behind a shard — which means a test reading counters or the shards'
// captures straight after Forward would be racing the workers.
func waitForwarded(t testing.TB, f *Forwarder) {
	t.Helper()
	if !f.awaitIdle(10 * time.Second) {
		t.Fatalf("the forwarder did not drain within 10s (%d payloads still queued)", f.queued.Load())
	}
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

// --- tap semantics ---

// TestTapForwardsOriginalUntouched: the payload the collector receives must be
// the caller's, byte for byte — the graph hop is a side effect, never a filter.
// (If Trim ever ran in place, the collector would start receiving spans with
// no names and no attributes, which is the most expensive possible bug here.)
func TestTapForwardsOriginalUntouched(t *testing.T) {
	td := realisticBatch(3, 2)
	before := marshal(t, td)

	clients, caps := fakeShards(t, 4)
	f := testForwarder(t, clients, 0, nil, nil)
	inner := &captureExporter{}
	if err := f.Tap(inner).ExportTraces(context.Background(), td); err != nil {
		t.Fatalf("ExportTraces: %v", err)
	}
	waitForwarded(t, f)

	if got := len(inner.got); got != 1 {
		t.Fatalf("inner got %d payloads, want 1", got)
	}
	if got := marshal(t, inner.got[0]); string(got) != string(before) {
		t.Errorf("inner received a different payload than the caller's (%d vs %d bytes)", len(got), len(before))
	}
	if after := marshal(t, td); string(after) != string(before) {
		t.Errorf("the caller's payload was mutated (%d vs %d bytes)", len(after), len(before))
	}
	// The inner payload still has everything; the shards got the trimmed copy.
	if n := inner.got[0].SpanCount(); n != td.SpanCount() {
		t.Errorf("inner span count %d, want %d", n, td.SpanCount())
	}
	forwarded := 0
	for _, c := range caps {
		forwarded += c.spans()
	}
	if forwarded != 6 { // 3 traces x (client + server); the internals are dropped
		t.Errorf("forwarded %d spans, want 6", forwarded)
	}
}

// TestTapDoesNotForwardOnFailure: a failed inner export means the sender will
// retry, and a graph hop that already happened would be repeated — the shard's
// cumulative edge counters dedupe nothing.
func TestTapDoesNotForwardOnFailure(t *testing.T) {
	clients, caps := fakeShards(t, 4)
	f := testForwarder(t, clients, 0, nil, nil)
	boom := errors.New("collector unavailable")
	inner := &captureExporter{err: boom}

	err := f.Tap(inner).ExportTraces(context.Background(), realisticBatch(5, 1))
	if !errors.Is(err, boom) {
		t.Fatalf("ExportTraces error = %v, want %v", err, boom)
	}
	waitForwarded(t, f)
	for i, c := range caps {
		if c.calls() != 0 {
			t.Errorf("shard %d was sent %d payloads after a failed inner export", i, c.calls())
		}
	}
	if got := f.Stats().SpansForwarded; got != 0 {
		t.Errorf("SpansForwarded = %d after a failed export, want 0", got)
	}
}

// TestNilForwarderTapIsPassthrough: the feature off must cost nothing, not a
// wrapper that checks a flag per batch.
func TestNilForwarderTapIsPassthrough(t *testing.T) {
	var f *Forwarder
	inner := &captureExporter{}
	if got := f.Tap(inner); got != TracesExporter(inner) {
		t.Fatalf("nil Forwarder Tap wrapped the inner exporter")
	}
}

// TestFailingShardDoesNotFailExport: losing an edge must never cost a span.
func TestFailingShardDoesNotFailExport(t *testing.T) {
	clients, caps := fakeShards(t, 4)
	// Break every shard, so the result cannot depend on which one owns what.
	for _, c := range caps {
		c.err = errors.New("shard down")
	}
	f := testForwarder(t, clients, 0, nil, nil)
	inner := &captureExporter{}
	td := realisticBatch(20, 1)

	if err := f.Tap(inner).ExportTraces(context.Background(), td); err != nil {
		t.Fatalf("a failing shard failed the caller's export: %v", err)
	}
	waitForwarded(t, f)
	if len(inner.got) != 1 {
		t.Fatalf("inner got %d payloads, want 1", len(inner.got))
	}
	st := f.Stats()
	if st.SendsFailed == 0 || st.SpansLost != 40 {
		t.Errorf("stats = %+v, want SendsFailed > 0 and SpansLost = 40", st)
	}
}

// TestInternalSpansNotForwarded: INTERNAL/UNSPECIFIED spans can never be half
// of an edge, and dropping them is most of the volume saving.
func TestInternalSpansNotForwarded(t *testing.T) {
	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	realisticResource(rs.Resource(), "checkout")
	ss := rs.ScopeSpans().AppendEmpty()
	kinds := []ptrace.SpanKind{
		ptrace.SpanKindInternal, ptrace.SpanKindUnspecified,
		ptrace.SpanKindClient, ptrace.SpanKindServer,
		ptrace.SpanKindProducer, ptrace.SpanKindConsumer,
	}
	for i, k := range kinds {
		realisticSpan(ss.Spans().AppendEmpty(), k, fwdTraceID(uint64(i)), fwdSpanID(uint64(i)), pcommon.SpanID{})
	}

	out := Trim(td)
	got := map[ptrace.SpanKind]bool{}
	rss := out.ResourceSpans()
	for i := 0; i < rss.Len(); i++ {
		sss := rss.At(i).ScopeSpans()
		for j := 0; j < sss.Len(); j++ {
			spans := sss.At(j).Spans()
			for k := 0; k < spans.Len(); k++ {
				got[spans.At(k).Kind()] = true
			}
		}
	}
	for _, k := range []ptrace.SpanKind{ptrace.SpanKindClient, ptrace.SpanKindServer, ptrace.SpanKindProducer, ptrace.SpanKindConsumer} {
		if !got[k] {
			t.Errorf("%s spans were not forwarded", k)
		}
	}
	for _, k := range []ptrace.SpanKind{ptrace.SpanKindInternal, ptrace.SpanKindUnspecified} {
		if got[k] {
			t.Errorf("%s spans were forwarded", k)
		}
	}
	if n := out.SpanCount(); n != 4 {
		t.Errorf("trimmed span count = %d, want 4", n)
	}
}

// TestSpansWithNoTraceIDAreDropped: unpairable by construction, and every zero
// id hashes to the same token — forwarding them would pile a cluster's
// malformed spans onto one shard.
func TestSpansWithNoTraceIDAreDropped(t *testing.T) {
	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	realisticResource(rs.Resource(), "checkout")
	ss := rs.ScopeSpans().AppendEmpty()
	realisticSpan(ss.Spans().AppendEmpty(), ptrace.SpanKindClient, pcommon.TraceID{}, fwdSpanID(1), pcommon.SpanID{})
	if n := Trim(td).SpanCount(); n != 0 {
		t.Errorf("trimmed span count = %d, want 0", n)
	}
}

// --- the fan-out must never block the ingest handler ---

// blockingExporter parks every send until release is closed — a shard that
// accepts the connection and answers nothing, which is the EXPECTED failure
// here: the chart's headless Service publishes not-ready addresses (the ring
// needs stable names before a pod is ready), so DNS resolves to shards that are
// still starting.
type blockingExporter struct {
	release chan struct{}
	calls   atomic.Int64
}

func (b *blockingExporter) ExportTraces(_ context.Context, _ ptrace.Traces) error {
	b.calls.Add(1)
	<-b.release
	return nil
}

// TestForwardNeverBlocksTheCaller is the whole reason the fan-out is
// asynchronous. Forward runs inside the OTLP ingest handler, which holds one of
// only -ingest-max-in-flight slots (32); a synchronous fan-out parks that slot
// for as long as a shard takes to answer, so a black-holing shard sheds the
// node's UNRELATED pushed logs and metrics with 429. Losing an edge must never
// cost a span — that is this package's stated contract, and it is exactly what
// a blocking send breaks.
func TestForwardNeverBlocksTheCaller(t *testing.T) {
	release := make(chan struct{})
	blocked := &blockingExporter{release: release}
	healthy := &captureExporter{}
	f := testForwarder(t, map[string]TracesExporter{"shard-0": blocked, "shard-1": healthy}, 0, nil, nil)
	// Registered AFTER the forwarder's own cleanup, so it runs BEFORE it (LIFO):
	// Close would otherwise spend its whole drain budget waiting on this shard.
	t.Cleanup(func() { close(release) })

	inner := &captureExporter{}
	tap := f.Tap(inner)
	td := realisticBatch(200, 0) // 400 edge spans, spread over both shards

	done := make(chan error, 1)
	go func() { done <- tap.ExportTraces(context.Background(), td) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("ExportTraces: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the shard fan-out blocked the caller: an ingest slot is parked behind a shard that is not answering")
	}
	if len(inner.got) != 1 {
		t.Fatalf("inner got %d payloads, want 1", len(inner.got))
	}

	// And the healthy shard is unaffected — a stuck shard costs its own arc of
	// the ring, not the whole graph.
	deadline := time.Now().Add(5 * time.Second)
	for healthy.spans() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("the healthy shard received nothing while another shard was stuck")
		}
		time.Sleep(time.Millisecond)
	}
	if blocked.calls.Load() == 0 {
		t.Error("the stuck shard was never even attempted")
	}
}

// TestFullForwardQueueShedsAndCounts: the queue is a shock absorber for a
// briefly slow shard, not a buffer for a dead one. Over its bounds it drops and
// COUNTS — silence here would look exactly like a graph with no traffic.
func TestFullForwardQueueShedsAndCounts(t *testing.T) {
	release := make(chan struct{})
	blocked := &blockingExporter{release: release}
	f := testForwarder(t, map[string]TracesExporter{"shard-0": blocked}, 0, nil, nil)
	t.Cleanup(func() { close(release) })

	const batches, perBatch = 400, 50 // 100 edge spans each
	for i := 0; i < batches; i++ {
		f.Forward(context.Background(), realisticBatch(perBatch, 0))
	}

	st := f.Stats()
	if st.SpansQueueFull == 0 {
		t.Fatalf("stats = %+v: nothing was counted as shed, so the queue grew without bound", st)
	}
	// Bounded memory, which is the other half of the claim: neither the item cap
	// nor the span budget may be exceeded.
	q := f.queues["shard-0"]
	if got := len(q.ch); got > forwardQueueItems {
		t.Errorf("queue holds %d items, cap is %d", got, forwardQueueItems)
	}
	// One payload may be in flight past the budget (reserve admits a batch into
	// an EMPTY queue whatever its size, so a single oversized push can never
	// wedge a shard); everything else is inside it.
	if got := q.spans.Load(); got > forwardQueueSpans+perBatch*2 {
		t.Errorf("queue holds %d spans, budget is %d", got, forwardQueueSpans)
	}
	// Every span is accounted for: forwarded, shed, or still queued.
	if total := st.SpansForwarded + st.SpansQueueFull; total > batches*perBatch*2 {
		t.Errorf("accounted for %d spans, only %d were offered", total, batches*perBatch*2)
	}
}

// TestCloseDrainsThenAbandons pins both halves of the shutdown contract: what is
// already queued is given a chance to land, and a shard that is not answering
// cannot hold the process — the agent's other final exports run after this and
// would be lost to SIGKILL.
func TestCloseDrainsThenAbandons(t *testing.T) {
	// Drains what is queued.
	slow := &captureExporter{}
	f := NewForwarderWithClients(map[string]TracesExporter{"shard-0": slow}, 0, nil, nil, discardLog())
	f.Forward(context.Background(), realisticBatch(10, 0))
	if err := f.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if got := slow.spans(); got != 20 {
		t.Errorf("Close delivered %d spans, want 20: queued forwards were abandoned rather than drained", got)
	}
	// Close is idempotent — main defers it beside an explicit path.
	if err := f.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}

	// And is bounded when the shard never answers.
	defer func(d time.Duration) { forwardDrainTimeout = d }(forwardDrainTimeout)
	forwardDrainTimeout = 50 * time.Millisecond
	release := make(chan struct{})
	defer close(release)
	blocked := &blockingExporter{release: release}
	f2 := NewForwarderWithClients(map[string]TracesExporter{"shard-0": blocked}, 0, nil, nil, discardLog())
	f2.Forward(context.Background(), realisticBatch(10, 0))
	done := make(chan struct{})
	go func() { defer close(done); _ = f2.Close() }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not return while a shard send was stuck: the shutdown budget is unbounded")
	}
}

// --- trimming ---

// TestTrimKeepsExactlyWhatPairingNeeds pins the wire contract in both
// directions: every field the shard reads survives, and everything else is
// gone. A trim that quietly kept span names or events would still pair
// correctly and would double the feature's network cost.
func TestTrimKeepsExactlyWhatPairingNeeds(t *testing.T) {
	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	realisticResource(rs.Resource(), "checkout")
	ss := rs.ScopeSpans().AppendEmpty()
	ss.Scope().SetName("otelhttp")
	ss.Scope().SetVersion("0.58.0")
	src := ss.Spans().AppendEmpty()
	tid, sid, parent := fwdTraceID(42), fwdSpanID(7), fwdSpanID(6)
	realisticSpan(src, ptrace.SpanKindClient, tid, sid, parent)
	src.Attributes().PutStr("messaging.system", "kafka")
	src.Links().AppendEmpty().SetTraceID(fwdTraceID(99))

	out := Trim(td)
	if out.ResourceSpans().Len() != 1 {
		t.Fatalf("resource spans = %d, want 1", out.ResourceSpans().Len())
	}
	ors := out.ResourceSpans().At(0)
	if ors.ScopeSpans().Len() != 1 {
		t.Fatalf("scope spans = %d, want 1", ors.ScopeSpans().Len())
	}
	oss := ors.ScopeSpans().At(0)
	if got := oss.Scope().Name(); got != "" {
		t.Errorf("scope name %q survived the trim", got)
	}
	if oss.Spans().Len() != 1 {
		t.Fatalf("spans = %d, want 1", oss.Spans().Len())
	}
	got := oss.Spans().At(0)

	// Kept: the pairing fields.
	if got.TraceID() != tid || got.SpanID() != sid || got.ParentSpanID() != parent {
		t.Errorf("ids not preserved: trace %x span %x parent %x", got.TraceID(), got.SpanID(), got.ParentSpanID())
	}
	if got.Kind() != ptrace.SpanKindClient {
		t.Errorf("kind = %v, want CLIENT", got.Kind())
	}
	if got.StartTimestamp() != src.StartTimestamp() || got.EndTimestamp() != src.EndTimestamp() {
		t.Errorf("timestamps not preserved: %d..%d", got.StartTimestamp(), got.EndTimestamp())
	}
	if got.Status().Code() != ptrace.StatusCodeError {
		t.Errorf("status code = %v, want Error", got.Status().Code())
	}

	// Kept: the resource identity.
	ra := ors.Resource().Attributes()
	for _, k := range []string{"service.name", "service.namespace", "service.instance.id",
		"k8s.namespace.name", "k8s.pod.name", "k8s.pod.uid", "k8s.container.name", "k8s.node.name"} {
		if _, ok := ra.Get(k); !ok {
			t.Errorf("resource attribute %s was dropped", k)
		}
	}
	// Kept: the connection-type / peer attributes.
	for _, k := range []string{"peer.service", "db.system", "messaging.system"} {
		if _, ok := got.Attributes().Get(k); !ok {
			t.Errorf("span attribute %s was dropped", k)
		}
	}

	// Dropped: everything else.
	if got.Name() != "" {
		t.Errorf("span name %q survived", got.Name())
	}
	if got.Status().Message() != "" {
		t.Errorf("status message %q survived", got.Status().Message())
	}
	if got.Events().Len() != 0 || got.Links().Len() != 0 {
		t.Errorf("events/links survived: %d/%d", got.Events().Len(), got.Links().Len())
	}
	for _, k := range []string{"url.full", "url.path", "user_agent.original", "enduser.id", "shop.tenant", "http.response.status_code"} {
		if _, ok := got.Attributes().Get(k); ok {
			t.Errorf("span attribute %s survived the trim", k)
		}
	}
	for _, k := range []string{"service.version", "k8s.deployment.name", "k8s.pod.ip", "cloud.region", "telemetry.sdk.name"} {
		if _, ok := ra.Get(k); ok {
			t.Errorf("resource attribute %s survived the trim", k)
		}
	}
	// The loop marker rides along.
	if v, ok := ra.Get(ForwardedMarker); !ok || !v.Bool() {
		t.Errorf("%s not stamped on the trimmed resource", ForwardedMarker)
	}
}

// TestTrimKeepsConfiguredDimensions: a dimension resolves span-then-resource,
// so it must survive on BOTH sides or the shard renders it empty.
func TestTrimKeepsConfiguredDimensions(t *testing.T) {
	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	realisticResource(rs.Resource(), "checkout")
	rs.Resource().Attributes().PutStr("deployment.environment", "prod")
	ss := rs.ScopeSpans().AppendEmpty()
	s := ss.Spans().AppendEmpty()
	realisticSpan(s, ptrace.SpanKindServer, fwdTraceID(1), fwdSpanID(1), pcommon.SpanID{})

	tr := newTrimmer([]string{"deployment.environment", "http.request.method"}, []string{"peer.service"})
	out := tr.split(td, nil, nil)[""]
	ors := out.ResourceSpans().At(0)
	if _, ok := ors.Resource().Attributes().Get("deployment.environment"); !ok {
		t.Errorf("configured dimension was trimmed off the resource")
	}
	got := ors.ScopeSpans().At(0).Spans().At(0)
	if _, ok := got.Attributes().Get("http.request.method"); !ok {
		t.Errorf("configured dimension was trimmed off the span")
	}
	// A non-default peer attribute list still keeps the fixed classifiers.
	if _, ok := got.Attributes().Get("db.system"); !ok {
		t.Errorf("connection-type attribute db.system was trimmed")
	}
}

// TestTrimKeepsSemconvDatabaseAttributes: the shard's namesDatabase reads the
// post-semconv-1.30 spellings (db.system.name / db.namespace) beside Tempo's
// older db.system / db.name, and an SDK on today's conventions emits ONLY the
// new pair. The trim's fixed floor has to carry them, or that support is dead
// code in the only production topology there is — the agent trims, the shard
// reads — and every modern database client renders as a plain
// service-to-service call. mismatchWatch cannot catch this either: it compares
// the two sides' CONFIGURED lists, and the floor is in neither.
func TestTrimKeepsSemconvDatabaseAttributes(t *testing.T) {
	// A realistic 1.30 client span: the far side named by peer.service, the
	// callee identified only by the new db keys.
	td := sgTraces("checkout", sgSpan{
		kind: ptrace.SpanKindClient, dur: 0.02,
		traceID: traceID(1), spanID: spanID(1),
		attrs: map[string]string{
			"db.system.name":  "postgresql",
			"db.namespace":    "orders",
			"db.query.text":   "SELECT 1", // not a classifier: must still be trimmed
			"peer.service":    "orders-db",
			"server.address":  "orders-db.shop.svc.cluster.local",
			"db.operation.na": "SELECT",
		},
	})

	// The DEFAULT trim — an agent with no serviceGraphShards.peerAttributes,
	// which is exactly what the chart renders.
	out := Trim(td)
	got := out.ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0).Attributes()
	for _, k := range []string{"db.system.name", "db.namespace", "peer.service"} {
		if _, ok := got.Get(k); !ok {
			t.Errorf("span attribute %s was trimmed away, so the shard can never see it", k)
		}
	}
	if _, ok := got.Get("db.query.text"); ok {
		t.Error("db.query.text survived the trim: the floor is the classifiers, not the whole db namespace")
	}

	// End to end: the shard classifies the edge as a database call and names the
	// uninstrumented far side from peer.service.
	p, sink, clock := newTestProcessor(t, Config{Wait: "1s"})
	p.Consume(out)
	*clock = t0.Add(time.Second)
	p.Sweep()
	e := sink.only(t)
	if e.Connection != ConnectionDatabase {
		t.Errorf("connection = %q, want %q", e.Connection, ConnectionDatabase)
	}
	if e.ServerService != "orders-db" || e.VirtualNode != virtualNodeServer {
		t.Errorf("edge = %+v, want a virtual server node named orders-db", e)
	}
}

// TestTrimByteCost reports the wire cost of the feature. Not an assertion
// about a particular number — a printed measurement, plus a floor under the
// saving so a future "just keep the attributes, it is easier" change has to
// argue with a failing test.
func TestTrimByteCost(t *testing.T) {
	const traces, internal = 50, 3
	td := realisticBatch(traces, internal)
	trimmed := Trim(td)

	var m ptrace.ProtoMarshaler
	rawBytes := m.TracesSize(td)
	trimBytes := m.TracesSize(trimmed)
	rawSpans, trimSpans := td.SpanCount(), trimmed.SpanCount()

	perRaw := float64(rawBytes) / float64(rawSpans)
	perTrim := float64(trimBytes) / float64(trimSpans)
	t.Logf("untrimmed: %d bytes / %d spans = %.0f B/span", rawBytes, rawSpans, perRaw)
	t.Logf("trimmed:   %d bytes / %d spans = %.0f B/span (edge spans only)", trimBytes, trimSpans, perTrim)
	t.Logf("per-span saving %.0f%%; whole-batch saving %.0f%% (%d -> %d bytes, incl. dropping %d INTERNAL spans)",
		(1-perTrim/perRaw)*100, (1-float64(trimBytes)/float64(rawBytes))*100, rawBytes, trimBytes, rawSpans-trimSpans)

	if perTrim > perRaw/4 {
		t.Errorf("trim saves less than 75%% per span: %.0f -> %.0f B/span", perRaw, perTrim)
	}
	if trimBytes*10 > rawBytes {
		t.Errorf("trimmed batch is more than a tenth of the original: %d vs %d bytes", trimBytes, rawBytes)
	}
}

// --- sharding ---

// TestBothHalvesOfATraceLandOnOneShard is the entire point of the ring: the
// client span and the server span of one request are produced by two pods on
// two nodes, reach two different agents, and must still meet. Asserted
// directly, with the two halves going through two SEPARATE forwarders (two
// agents) in separate batches.
func TestBothHalvesOfATraceLandOnOneShard(t *testing.T) {
	const shards, traces = 8, 2000
	clientsA, capsA := fakeShards(t, shards)
	clientsB, capsB := fakeShards(t, shards)
	agentA := testForwarder(t, clientsA, 0, nil, nil)
	agentB := testForwarder(t, clientsB, 0, nil, nil)

	// Agent A ships the client halves, agent B the server halves.
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
	agentA.Forward(context.Background(), mk(ptrace.SpanKindClient, "checkout"))
	agentB.Forward(context.Background(), mk(ptrace.SpanKindServer, "orders"))
	waitForwarded(t, agentA)
	waitForwarded(t, agentB)

	// Collect, per shard index, the set of trace ids each side delivered.
	collect := func(caps []*captureExporter) []map[pcommon.TraceID]bool {
		out := make([]map[pcommon.TraceID]bool, len(caps))
		for i, c := range caps {
			out[i] = map[pcommon.TraceID]bool{}
			for _, td := range c.got {
				rss := td.ResourceSpans()
				for r := 0; r < rss.Len(); r++ {
					sss := rss.At(r).ScopeSpans()
					for s := 0; s < sss.Len(); s++ {
						spans := sss.At(s).Spans()
						for k := 0; k < spans.Len(); k++ {
							out[i][spans.At(k).TraceID()] = true
						}
					}
				}
			}
		}
		return out
	}
	byShardA, byShardB := collect(capsA), collect(capsB)

	used := 0
	for i := 0; i < shards; i++ {
		if len(byShardA[i]) > 0 {
			used++
		}
		if len(byShardA[i]) != len(byShardB[i]) {
			t.Errorf("shard %d got %d client halves but %d server halves", i, len(byShardA[i]), len(byShardB[i]))
		}
		for tid := range byShardA[i] {
			if !byShardB[i][tid] {
				t.Fatalf("trace %x: the client half went to shard %d, the server half did not", tid, i)
			}
		}
	}
	if used != shards {
		t.Errorf("only %d of %d shards received traces", used, shards)
	}
	// And every trace was delivered exactly once, to exactly one shard.
	seen := map[pcommon.TraceID]int{}
	for i := 0; i < shards; i++ {
		for tid := range byShardA[i] {
			seen[tid]++
		}
	}
	if len(seen) != traces {
		t.Errorf("%d distinct traces delivered, want %d", len(seen), traces)
	}
	for tid, n := range seen {
		if n != 1 {
			t.Errorf("trace %x was delivered to %d shards", tid, n)
		}
	}
}

// TestForwardGroupsPerShardResource: each shard gets its own payload, and a
// source resource's attributes are copied once per shard rather than per span.
func TestForwardGroupsPerShardResource(t *testing.T) {
	clients, caps := fakeShards(t, 4)
	f := testForwarder(t, clients, 0, nil, nil)
	f.Forward(context.Background(), realisticBatch(200, 0))
	waitForwarded(t, f)

	total := 0
	for _, c := range caps {
		for _, td := range c.got {
			total += td.SpanCount()
			rss := td.ResourceSpans()
			if rss.Len() != 1 {
				t.Errorf("a shard payload carries %d resources, want 1", rss.Len())
			}
			for r := 0; r < rss.Len(); r++ {
				if got := rss.At(r).ScopeSpans().Len(); got != 1 {
					t.Errorf("a shard resource carries %d scopes, want 1", got)
				}
			}
		}
	}
	if total != 400 {
		t.Errorf("forwarded %d spans, want 400", total)
	}
	if st := f.Stats(); st.SpansForwarded != 400 || st.SpansLost != 0 {
		t.Errorf("stats = %+v", st)
	}
}

// TestLoopMarkerBlocksReforwarding: a shard tier misconfigured to point at
// itself must stop at one hop, not amplify.
func TestLoopMarkerBlocksReforwarding(t *testing.T) {
	clients, caps := fakeShards(t, 4)
	f := testForwarder(t, clients, 0, nil, nil)
	// A payload as it arrives at a shard: already trimmed and marked.
	already := Trim(realisticBatch(10, 0))
	f.Forward(context.Background(), already)
	waitForwarded(t, f)

	for i, c := range caps {
		if c.calls() != 0 {
			t.Errorf("shard %d received a re-forwarded payload", i)
		}
	}
	st := f.Stats()
	if st.LoopsBlocked != 1 || st.SpansSkipped != 20 {
		t.Errorf("stats = %+v, want LoopsBlocked = 1 and SpansSkipped = 20", st)
	}
}

// --- config ---

func TestShardTargetsFromTemplate(t *testing.T) {
	cfg := ForwardConfig{StatefulSet: "kubescrape-servicegraph", Replicas: 3, Namespace: "monitoring"}
	got, err := cfg.shardTargets()
	if err != nil {
		t.Fatalf("shardTargets: %v", err)
	}
	// 4319, not 4317: the default MUST be the port the shard receiver listens on
	// by default (-service-graph-listen). 4317 is the AGENTS' own ingest port,
	// which the shard pods do not serve at all, so a config relying on the
	// default forwarded into a black hole.
	if DefaultShardPort != 4319 {
		t.Fatalf("DefaultShardPort = %d, want 4319 (the shard receiver's own default)", DefaultShardPort)
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
	// produce https — otlpexport ignores Insecure for HTTP, so here the scheme
	// IS the TLS decision, and a request for TLS that fell through to plaintext
	// would put the shard bearer token on the wire in the clear.
	base := ForwardConfig{StatefulSet: "kubescrape-servicegraph", Replicas: 1,
		Namespace: "monitoring", Protocol: "http", Port: 4318}
	const plain = "http://kubescrape-servicegraph-0.kubescrape-servicegraph.monitoring.svc:4318"
	const secure = "https://kubescrape-servicegraph-0.kubescrape-servicegraph.monitoring.svc:4318"
	yes, no := true, false
	for _, tc := range []struct {
		name string
		mut  func(*ForwardConfig)
		want string
	}{
		{"nothing configured", func(*ForwardConfig) {}, plain},
		{"caFile", func(c *ForwardConfig) { c.CAFile = "/etc/ca.pem" }, secure},
		{"insecureSkipVerify", func(c *ForwardConfig) { c.InsecureSkipVerify = &yes }, secure},
		{"insecure: false", func(c *ForwardConfig) { c.Insecure = &no }, secure},
		// An explicit insecure wins, so caFile beside it is a contradiction —
		// which otlpexport.New then refuses loudly rather than quietly
		// handshaking or quietly not.
		{"insecure: true beats a caFile", func(c *ForwardConfig) { c.Insecure, c.CAFile = &yes, "/etc/ca.pem" }, plain},
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
	cfg.Protocol, cfg.Port = "http", 4318

	// Explicit endpoints win, and the name IS the endpoint.
	cfg.Endpoints = []string{"sg-a:4317", " sg-b:4317 "}
	got, _ = cfg.shardTargets()
	if len(got) != 2 || got[0].name != "sg-a:4317" || got[1].endpoint != "sg-b:4317" {
		t.Errorf("explicit endpoints = %+v", got)
	}
}

func TestForwardConfigValidate(t *testing.T) {
	for _, tc := range []struct {
		name    string
		cfg     ForwardConfig
		wantErr bool
		enabled bool
	}{
		{name: "empty"},
		{name: "template", cfg: ForwardConfig{StatefulSet: "sg", Replicas: 2}, enabled: true},
		{name: "endpoints", cfg: ForwardConfig{Endpoints: []string{"a:4317"}}, enabled: true},
		{name: "half-filled template", cfg: ForwardConfig{StatefulSet: "sg"}, wantErr: true},
		{name: "replicas without name", cfg: ForwardConfig{Replicas: 3}, wantErr: true},
		{name: "bad protocol", cfg: ForwardConfig{StatefulSet: "sg", Replicas: 1, Protocol: "htttp"}, wantErr: true, enabled: true},
		{name: "bad port", cfg: ForwardConfig{StatefulSet: "sg", Replicas: 1, Port: 70000}, wantErr: true, enabled: true},
		{name: "empty endpoint", cfg: ForwardConfig{Endpoints: []string{" "}}, wantErr: true, enabled: true},
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
// COLLECTOR. Inheriting its bearer token would present that credential to the
// shard tier because a field was left empty.
func TestClientConfigDoesNotInheritCredentials(t *testing.T) {
	base := otlpexportConfigForTest()
	cfg := ForwardConfig{StatefulSet: "sg", Replicas: 1, Namespace: "monitoring", BearerTokenFile: "/var/run/sg/token"}
	got := cfg.clientConfig(shardTarget{name: "sg-0", endpoint: "sg-0:4317"}, base)

	if got.BearerTokenFile != "/var/run/sg/token" {
		t.Errorf("bearer token file = %q", got.BearerTokenFile)
	}
	if got.Headers != nil {
		t.Errorf("base headers leaked to the shard: %v", got.Headers)
	}
	if got.CAFile != "" || got.ClientCertFile != "" {
		t.Errorf("base TLS material leaked to the shard: %q %q", got.CAFile, got.ClientCertFile)
	}
	if got.Endpoint != "sg-0:4317" {
		t.Errorf("endpoint = %q", got.Endpoint)
	}
	// Transport tuning IS inherited.
	if got.Timeout != base.Timeout || got.Compression != base.Compression || got.MaxSendBytes != base.MaxSendBytes {
		t.Errorf("transport tuning not inherited: %+v", got)
	}
	if got.RetryAttempts != 1 {
		t.Errorf("retry attempts = %d, want 1 (a retried graph hop double-counts an edge)", got.RetryAttempts)
	}
	if !got.Insecure {
		t.Errorf("default should be plaintext in-cluster when no CA is configured")
	}
}

// TestNewForwarderDisabled: wiring it unconditionally must be free.
func TestNewForwarderDisabled(t *testing.T) {
	f, err := NewForwarder(ForwardConfig{}, otlpexportConfigForTest(), discardLog())
	if err != nil || f != nil {
		t.Fatalf("NewForwarder(disabled) = %v, %v; want nil, nil", f, err)
	}
	// A disabled forwarder is a nil pointer, and every method must tolerate it.
	f.Forward(context.Background(), realisticBatch(1, 0))
}

// TestNewForwarderBuildsRing: the ring's shard names come from the template.
func TestNewForwarderBuildsRing(t *testing.T) {
	f, err := NewForwarder(ForwardConfig{StatefulSet: "sg", Replicas: 3, Namespace: "monitoring"}, otlpexportConfigForTest(), discardLog())
	if err != nil {
		t.Fatalf("NewForwarder: %v", err)
	}
	defer func() { _ = f.Close() }()
	want := []string{"sg-0", "sg-1", "sg-2"}
	got := f.Ring().Shards()
	if len(got) != len(want) {
		t.Fatalf("ring shards = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("ring shards = %v, want %v", got, want)
		}
	}
}

// --- benchmark ---

// BenchmarkTap measures what the CALLER pays on a realistic batch: a no-op
// inner export plus trim, hash, per-shard grouping and the queue hand-off, for
// 20 traces (40 edge spans) with 3 INTERNAL spans each. The shard SEND is not
// in this number by design — it happens on the fan-out workers, off the ingest
// handler's goroutine (see Forward).
func BenchmarkTap(b *testing.B) {
	clients := map[string]TracesExporter{}
	for i := 0; i < 8; i++ {
		clients[fmt.Sprintf("shard-%d", i)] = nopExporter{}
	}
	f := testForwarder(b, clients, 0, nil, nil)
	tap := f.Tap(nopExporter{})
	td := realisticBatch(20, 3)
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := tap.ExportTraces(ctx, td); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	// Per-span figures, which is how the cost is actually paid: the batch is
	// 100 spans, of which 40 are forwarded.
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/float64(td.SpanCount()), "ns/span")
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/float64(Trim(td).SpanCount()), "ns/fwdspan")
}

func BenchmarkTrim(b *testing.B) {
	td := realisticBatch(20, 3)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = Trim(td)
	}
}

type nopExporter struct{}

func (nopExporter) ExportTraces(context.Context, ptrace.Traces) error { return nil }
