package otlpingest

// The decoded-structure ESTIMATE (decodedsize.go) and the two doors that charge
// it BEFORE the decode: servePush on HTTP, the codec + decodedClaims on gRPC.
//
// Two properties, and the tests below are split along them. The estimate must
// COVER what a decode retains, for every repeated sub-message pdata
// materialises — not only the resources, scopes and items it used to count,
// which left a KeyValue (2 wire bytes, a 40-byte struct), an exemplar, a span
// event or link (~40x their wire bytes) entirely uncharged. And the charge must
// be taken on the near side of the decode, or it bounds how long a payload is
// retained and nothing about its peak.

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"reflect"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/plog/plogotlp"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/pmetric/pmetricotlp"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/mem"
	"google.golang.org/grpc/status"

	"github.com/JohanLindvall/kubescrape/internal/agent/otlpexport"
	"github.com/JohanLindvall/kubescrape/internal/obs"
	"github.com/JohanLindvall/kubescrape/internal/testrace"
	"github.com/JohanLindvall/kubescrape/pkg/otlpsplit"
)

// The three OTLP Export methods, for pushing hand-built wire bytes (rawProto).
const (
	logsExportMethod    = "/opentelemetry.proto.collector.logs.v1.LogsService/Export"
	metricsExportMethod = "/opentelemetry.proto.collector.metrics.v1.MetricsService/Export"
	tracesExportMethod  = "/opentelemetry.proto.collector.trace.v1.TraceService/Export"
)

// wf is one length-delimited field; wrep is n copies of it. The amplifier shapes
// below are built from raw wire bytes because the shape that amplifies most —
// an EMPTY KeyValue, two bytes on the wire — is one pdata's own Map API cannot
// produce (it dedupes keys).
func wf(num int, payload []byte) []byte { return protoField(nil, num, payload) }

func wrep(num int, payload []byte, n int) []byte {
	one := wf(num, payload)
	out := make([]byte, 0, len(one)*n)
	for range n {
		out = append(out, one...)
	}
	return out
}

// amplifierShape is one wire payload of a single repeated sub-message, with the
// signal's estimator, decoder and gRPC method.
type amplifierShape struct {
	name   string
	signal string
	body   []byte
}

func (a amplifierShape) size() int64 {
	switch a.signal {
	case "metrics":
		return decodedMetricsSize(a.body)
	case "traces":
		return decodedTracesSize(a.body)
	}
	return decodedLogsSize(a.body)
}

// decode unmarshals the body the way the receiver does and returns the result,
// which the caller keeps alive for the heap measurement.
func (a amplifierShape) decode() (any, error) {
	switch a.signal {
	case "metrics":
		r := pmetricotlp.NewExportRequest()
		return r, r.UnmarshalProto(a.body)
	case "traces":
		r := ptraceotlp.NewExportRequest()
		return r, r.UnmarshalProto(a.body)
	}
	r := plogotlp.NewExportRequest()
	return r, r.UnmarshalProto(a.body)
}

func (a amplifierShape) method() string {
	switch a.signal {
	case "metrics":
		return metricsExportMethod
	case "traces":
		return tracesExportMethod
	}
	return logsExportMethod
}

func (a amplifierShape) path() string { return "/v1/" + a.signal }

// oldModelSize is what the estimate charged before it read the wire: resources,
// scopes and items (records / metric shells + points / spans) and nothing
// below them. Every amplifier shape below is chosen so that this stays tiny
// while the real decode does not — which is the gap being pinned.
func (a amplifierShape) oldModelSize(t *testing.T) int64 {
	t.Helper()
	v, err := a.decode()
	if err != nil {
		t.Fatalf("%s: fixture does not decode: %v", a.name, err)
	}
	var res, scopes, items int
	switch r := v.(type) {
	case plogotlp.ExportRequest:
		ld := r.Logs()
		res = ld.ResourceLogs().Len()
		for i := 0; i < res; i++ {
			sls := ld.ResourceLogs().At(i).ScopeLogs()
			scopes += sls.Len()
			for j := 0; j < sls.Len(); j++ {
				items += sls.At(j).LogRecords().Len()
			}
		}
	case pmetricotlp.ExportRequest:
		md := r.Metrics()
		res = md.ResourceMetrics().Len()
		for i := 0; i < res; i++ {
			sms := md.ResourceMetrics().At(i).ScopeMetrics()
			scopes += sms.Len()
			for j := 0; j < sms.Len(); j++ {
				ms := sms.At(j).Metrics()
				items += ms.Len()
				for k := 0; k < ms.Len(); k++ {
					items += otlpsplit.DataPointCount(ms.At(k))
				}
			}
		}
	case ptraceotlp.ExportRequest:
		td := r.Traces()
		res = td.ResourceSpans().Len()
		for i := 0; i < res; i++ {
			sss := td.ResourceSpans().At(i).ScopeSpans()
			scopes += sss.Len()
			for j := 0; j < sss.Len(); j++ {
				items += sss.At(j).Spans().Len()
			}
		}
	}
	return int64(res*decodedResourceBytes + scopes*decodedScopeBytes + items*decodedItemBytes)
}

// amplifierShapes is every repeated sub-message the decode materialises, at n
// elements each. The first group is the one the old model already counted
// (resources, scopes, items); the rest are the ones it did not.
func amplifierShapes(n int) (counted, uncounted []amplifierShape) {
	logs := func(name string, body []byte) amplifierShape { return amplifierShape{name, "logs", body} }
	metrics := func(name string, body []byte) amplifierShape { return amplifierShape{name, "metrics", body} }
	traces := func(name string, body []byte) amplifierShape { return amplifierShape{name, "traces", body} }

	// request -> resource_logs(1) -> scope_logs(2) -> log_records(2)
	record := func(rec []byte) []byte { return wf(1, wf(2, wf(2, rec))) }
	// request -> resource_metrics(1) -> scope_metrics(2) -> metrics(2)
	metric := func(m []byte) []byte { return wf(1, wf(2, wf(2, m))) }
	// request -> resource_spans(1) -> scope_spans(2) -> spans(2)
	span := func(s []byte) []byte { return wf(1, wf(2, wf(2, s))) }

	counted = []amplifierShape{
		logs("resources", wrep(1, wf(2, wf(2, nil)), n)),
		logs("records", wf(1, wf(2, wrep(2, nil, n)))),
		logs("empty scopes", wf(1, wrep(2, nil, n))),
		metrics("empty metrics", wf(1, wf(2, wrep(2, nil, n)))),
		metrics("gauge points", metric(wf(5, wrep(1, nil, n)))),
		traces("spans", wf(1, wf(2, wrep(2, nil, n)))),
	}
	uncounted = []amplifierShape{
		logs("record attributes", record(wrep(6, nil, n))),
		logs("resource attributes", wf(1, wf(1, wrep(1, nil, n)))),
		logs("scope attributes", wf(1, wf(2, wf(1, wrep(3, nil, n))))),
		logs("resource entity refs", wf(1, wf(1, wrep(3, nil, n)))),
		logs("array body elements", record(wf(5, wf(5, wrep(1, nil, n))))),
		logs("kvlist body entries", record(wf(5, wf(6, wrep(1, nil, n))))),
		logs("string-valued attributes", record(wrep(6, wf(2, wf(1, nil)), n))),
		metrics("point attributes", metric(wf(5, wf(1, wrep(7, nil, n))))),
		metrics("exemplars", metric(wf(5, wf(1, wrep(5, nil, n))))),
		metrics("exponential buckets", metric(wf(10, wf(1, wf(8, wf(2, make([]byte, n))))))),
		metrics("summary quantiles", metric(wf(11, wf(1, wrep(6, nil, n))))),
		traces("span attributes", span(wrep(9, nil, n))),
		traces("span events", span(wrep(11, nil, n))),
		traces("span links", span(wrep(13, nil, n))),
	}
	return counted, uncounted
}

// liveHeapOf is the heap a decode RETAINS: the live-heap delta across it, with
// the result kept alive past the second measurement.
func liveHeapOf(t *testing.T, decode func() (any, error)) int64 {
	t.Helper()
	var before, after runtime.MemStats
	runtime.GC()
	runtime.GC()
	runtime.ReadMemStats(&before)
	keep, err := decode()
	if err != nil {
		t.Fatalf("fixture does not decode: %v", err)
	}
	runtime.GC()
	runtime.GC()
	runtime.ReadMemStats(&after)
	runtime.KeepAlive(keep)
	return int64(after.HeapAlloc) - int64(before.HeapAlloc)
}

// The estimate must COVER what the decode retains, shape by shape. This is what
// makes the coefficients a bound rather than a guess, and what fails when a
// pdata upgrade grows a struct: the coefficients are generous — measured
// est/live runs 1.2-2.5x on these shapes — so a real gap shows up as a ratio
// below the floor, not as noise around it.
//
// The shapes are STRUCTURE only (empty elements), because that is all the
// estimate charges: content — the strings a decode copies — is the raw
// budget's, and a content-heavy payload legitimately retains more than this
// estimate (admit.go says why that is bounded elsewhere).
func TestWireEstimateCoversTheDecodedHeap(t *testing.T) {
	if testrace.Enabled {
		t.Skip("the race detector's bookkeeping shifts heap measurements")
	}
	const n = 1 << 15
	// Measured est/live bottoms out around 1.2 on these shapes; a list whose
	// length sits just past a doubling retains up to 2x its elements, which is
	// the one way a sender can push the ratio down, and decodedAttrBytes'
	// comment prices that at ~1.2x on its one term.
	const floor = 0.85
	counted, uncounted := amplifierShapes(n)
	for _, sh := range append(counted, uncounted...) {
		t.Run(sh.name, func(t *testing.T) {
			est := sh.size()
			live := liveHeapOf(t, sh.decode)
			if float64(est) < floor*float64(live) {
				t.Errorf("%d wire bytes decode to %d live bytes but are estimated at %d (%.2fx): "+
					"the decoded budget under-charges this shape", len(sh.body), live, est, float64(est)/float64(live))
			}
		})
	}
}

// And the other direction, which is what keeps the bound honest rather than
// merely safe: a FULL-SIZE push of ordinary shape must fit the production
// budget on its own, or every such sender is answered 429 forever. Ten labels
// per point is a label-rich Prometheus series; four attributes and a 200-byte
// body is an ordinary structured log line.
func TestDecodedEstimateAdmitsAnHonestFullSizePush(t *testing.T) {
	limit := NewServer(ServerConfig{}).decoded.limit

	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	for _, k := range []string{"service.name", "k8s.pod.name", "k8s.namespace.name", "host.name"} {
		rm.Resource().Attributes().PutStr(k, "checkout-7f9c")
	}
	dps := rm.ScopeMetrics().AppendEmpty().Metrics().AppendEmpty().SetEmptyGauge().DataPoints()
	for i := 0; ; i++ {
		dp := dps.AppendEmpty()
		dp.SetIntValue(int64(i))
		for j := range 10 {
			dp.Attributes().PutStr(fmt.Sprintf("label_%d", j), fmt.Sprintf("value-%d", i%97))
		}
		if i%1024 == 0 && (&pmetric.ProtoMarshaler{}).MetricsSize(md) > maxIngestBody-(64<<10) {
			break
		}
	}
	mb, err := pmetricotlp.NewExportRequestFromMetrics(md).MarshalProto()
	if err != nil {
		t.Fatal(err)
	}
	if got := decodedMetricsSize(mb); got > limit {
		t.Errorf("a %d-byte push of %d ten-label points estimates %d bytes, past the whole %d-byte "+
			"decoded budget: an honest full-size sender would never be admitted", len(mb), dps.Len(), got, limit)
	}

	ld := plog.NewLogs()
	lrs := ld.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords()
	body := string(bytes.Repeat([]byte("a"), 200))
	for i := 0; ; i++ {
		lr := lrs.AppendEmpty()
		lr.Body().SetStr(body)
		lr.Attributes().PutStr("http.method", "GET")
		lr.Attributes().PutStr("http.route", "/api/v1/things")
		lr.Attributes().PutInt("http.status_code", 200)
		lr.Attributes().PutStr("user_agent.original", "curl/8.0")
		if i%1024 == 0 && (&plog.ProtoMarshaler{}).LogsSize(ld) > maxIngestBody-(64<<10) {
			break
		}
	}
	lb, err := plogotlp.NewExportRequestFromLogs(ld).MarshalProto()
	if err != nil {
		t.Fatal(err)
	}
	if got := decodedLogsSize(lb); got > limit {
		t.Errorf("a %d-byte push of %d ordinary records estimates %d bytes, past the whole %d-byte "+
			"decoded budget", len(lb), lrs.Len(), got, limit)
	}
}

// ampServer is a Server for all three signals whose decoded budget is lowered
// to bind on test-sized payloads; forwards counts what got past the door.
func ampServer(t *testing.T, limit int64) (*Server, *atomic.Int64) {
	t.Helper()
	var forwards atomic.Int64
	count := func() error { forwards.Add(1); return nil }
	p := &countingSink{count: count}
	s := NewServer(ServerConfig{Enricher: newEnricher(newMeta(), MetricsAuto), Exporter: p, Traces: p})
	s.decoded.limit = limit
	return s, &forwards
}

type countingSink struct{ count func() error }

func (c *countingSink) ExportLogs(context.Context, plog.Logs) error          { return c.count() }
func (c *countingSink) ExportMetrics(context.Context, pmetric.Metrics) error { return c.count() }
func (c *countingSink) ExportTraces(context.Context, ptrace.Traces) error    { return c.count() }

// serveHTTP pushes a raw body through the signal's real handler.
func serveHTTP(s *Server, sh amplifierShape, body []byte, gzipped bool) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, sh.path(), bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/x-protobuf")
	if gzipped {
		req.Header.Set("Content-Encoding", "gzip")
	}
	rec := httptest.NewRecorder()
	switch sh.signal {
	case "metrics":
		s.handleHTTPMetrics(rec, req)
	case "traces":
		s.handleHTTPTraces(rec, req)
	default:
		s.handleHTTPLogs(rec, req)
	}
	return rec
}

// Every repeated sub-message the old estimate did not count is now REFUSED,
// retryably, on both doors — at a budget the old model's charge for the same
// payload fits a thousand times over.
func TestDecodedBudgetRefusesEveryAmplifierShape(t *testing.T) {
	const n = 1 << 16
	const limit = 512 << 10
	_, uncounted := amplifierShapes(n)
	for _, sh := range uncounted {
		t.Run(sh.name, func(t *testing.T) {
			if old := sh.oldModelSize(t); old > limit/64 {
				t.Fatalf("fixture: the old model already charged %d bytes, so this shape proves nothing", old)
			}
			if est := sh.size(); est <= limit {
				t.Fatalf("fixture: estimate %d does not exceed the %d budget", est, limit)
			}

			t.Run("http", func(t *testing.T) {
				s, forwards := ampServer(t, limit)
				before := obs.IngestRejected.WithLabelValues(shedDecoded).Value()
				rec := serveHTTP(s, sh, sh.body, false)
				if rec.Code != http.StatusTooManyRequests {
					t.Fatalf("status = %d, want 429 (body %q)", rec.Code, rec.Body.String())
				}
				if rec.Header().Get("Retry-After") == "" {
					t.Error("the refusal carries no Retry-After, so the sender cannot tell it from a rejection")
				}
				if got := obs.IngestRejected.WithLabelValues(shedDecoded).Value() - before; got != 1 {
					t.Errorf("kubescrape_ingest_rejected_total{reason=decoded_bytes} moved %v, want 1", got)
				}
				if forwards.Load() != 0 {
					t.Error("a refused push was forwarded")
				}
				if used := s.decoded.used.Load(); used != 0 {
					t.Errorf("a refused push left %d bytes charged", used)
				}
			})

			t.Run("grpc", func(t *testing.T) {
				s, conn := grpcBudgetServer(t, &countingSink{count: func() error { return nil }},
					&countingSink{count: func() error { return nil }})
				s.decoded.limit = limit
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				err := conn.Invoke(ctx, sh.method(), &rawProto{b: sh.body}, &rawProto{})
				st, _ := status.FromError(err)
				if st.Code() != codes.ResourceExhausted || !otlpexport.RetryableStatus(st) {
					t.Fatalf("got %v, want a retryable ResourceExhausted", err)
				}
				if used := s.decoded.used.Load(); used != 0 {
					t.Errorf("a refused push left %d bytes charged", used)
				}
			})
		})
	}
}

// The finding's own shape, at the PRODUCTION budget: a full 16 MiB body of
// empty attributes — about 16 KB once gzipped — which decoded to 385 MiB of live
// heap (2 GB allocated) and was charged 512 bytes. It must be refused, and
// refused WITHOUT being decoded: the allocation across the whole push is
// bounded by the body itself.
func TestFullSizeEmptyAttributePushIsRefusedBeforeItIsDecoded(t *testing.T) {
	attrs := (maxIngestBody - 64) / 2
	sh := amplifierShape{"record attributes", "logs", wf(1, wf(2, wf(2, wrep(6, nil, attrs))))}
	var gz bytes.Buffer
	zw := gzip.NewWriter(&gz)
	if _, err := zw.Write(sh.body); err != nil {
		t.Fatal(err)
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}

	s, forwards := ampServer(t, 0)
	s.decoded.limit = NewServer(ServerConfig{}).decoded.limit // the production budget

	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	rec := serveHTTP(s, sh, gz.Bytes(), true)
	runtime.ReadMemStats(&after)

	if rec.Code != http.StatusTooManyRequests || rec.Header().Get("Retry-After") == "" {
		t.Fatalf("a %d-byte body (%d gzipped) of %d empty attributes got %d, want 429 + Retry-After",
			len(sh.body), gz.Len(), attrs, rec.Code)
	}
	if forwards.Load() != 0 {
		t.Error("the push was forwarded")
	}
	// The body is read (and gunzipped) into memory — that is the raw budget's
	// charge — and nothing else of size may be allocated. The decode alone
	// allocates ~240 bytes per attribute.
	if alloc := after.TotalAlloc - before.TotalAlloc; alloc > 4*maxIngestBody {
		t.Errorf("the refused push allocated %d bytes; a push refused before its decode allocates "+
			"little more than its %d-byte body", alloc, len(sh.body))
	}
}

// The same property on the gRPC door, where the refusal cannot be an error
// from the codec (grpc-go rewrites those to a PERMANENT Internal): the message
// is left UNDECODED and the verdict is collected by the interceptor.
func TestGRPCRefusedPushIsNeverDecoded(t *testing.T) {
	const n = 1 << 16
	s, _ := ampServer(t, 1<<10)
	c := newDepthGuardCodec(nil, s)
	body := wf(1, wf(2, wf(2, wrep(6, nil, n))))

	msg := reflect.New(grpcLogsRequest.Elem()).Interface()
	if err := c.Unmarshal(mem.BufferSlice{mem.SliceBuffer(body)}, msg); err != nil {
		t.Fatalf("a refused message must not be a codec error (grpc-go answers those Internal): %v", err)
	}
	if got := msg.(otelProtoMessage).SizeProto(); got != 0 {
		t.Errorf("the refused message was decoded anyway (%d bytes of it): the budget bounded "+
			"retention, not the peak", got)
	}
	if got := s.claims.take(msg); got != claimRefused {
		t.Errorf("claim = %d, want claimRefused for the interceptor to answer", got)
	}
	if used := s.decoded.used.Load(); used != 0 {
		t.Errorf("a refused message holds %d bytes of the budget", used)
	}
}

// grpcLogsRequest and its siblings are how the codec tells the signals apart
// without naming pdata's internal types. They must be EXACTLY what grpc-go's
// generated handlers decode into, or every gRPC push is estimated through the
// fallback arm — and a pdata upgrade that moved them would do that silently.
func TestGRPCRequestTypesAreResolved(t *testing.T) {
	want := map[string]reflect.Type{
		logsExportMethod:    grpcLogsRequest,
		metricsExportMethod: grpcMetricsRequest,
		tracesExportMethod:  grpcTracesRequest,
	}
	seen := map[reflect.Type]bool{}
	for m, typ := range want {
		if typ == nil {
			t.Fatalf("%s: the request type did not resolve from its public wrapper", m)
		}
		if seen[typ] {
			t.Fatalf("%s: two signals resolved to the same type %v", m, typ)
		}
		seen[typ] = true
	}

	got := map[string]reflect.Type{}
	s := NewServer(ServerConfig{
		Enricher: newEnricher(newMeta(), MetricsAuto),
		Exporter: &countingSink{count: func() error { return nil }},
		Traces:   &countingSink{count: func() error { return nil }},
	})
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	record := make(chan [2]any, 3)
	srv := grpc.NewServer(grpc.UnaryInterceptor(func(ctx context.Context, req any, info *grpc.UnaryServerInfo, h grpc.UnaryHandler) (any, error) {
		record <- [2]any{info.FullMethod, reflect.TypeOf(req)}
		return h(ctx, req)
	}))
	plogotlp.RegisterGRPCServer(srv, &logsGRPC{s: s})
	pmetricotlp.RegisterGRPCServer(srv, &metricsGRPC{s: s})
	ptraceotlp.RegisterGRPCServer(srv, &tracesGRPC{s: s})
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := plogotlp.NewGRPCClient(conn).Export(ctx, oneLog()); err != nil {
		t.Fatal(err)
	}
	if _, err := pmetricotlp.NewGRPCClient(conn).Export(ctx, pmetricotlp.NewExportRequestFromMetrics(manyResourceMetrics(1))); err != nil {
		t.Fatal(err)
	}
	if _, err := ptraceotlp.NewGRPCClient(conn).Export(ctx, ptraceotlp.NewExportRequestFromTraces(manyResourceSpans(1))); err != nil {
		t.Fatal(err)
	}
	for range 3 {
		r := <-record
		got[r[0].(string)] = r[1].(reflect.Type)
	}
	for m, typ := range want {
		if got[m] != typ {
			t.Errorf("%s: grpc-go decodes into %v, the codec expects %v", m, got[m], typ)
		}
	}
}

// Every claim the codec files is collected, whatever becomes of the push —
// admitted, refused by the budget, refused by the in-flight count after the
// budget admitted it, failed in the forward, or malformed (the decode fails and
// grpc-go never reaches the interceptor). A claim that outlived its push would
// hold decoded budget for the process' life and grow the table without bound.
func TestGRPCDecodedClaimsNeverOutliveTheirPush(t *testing.T) {
	var failing atomic.Bool
	sink := &countingSink{count: func() error {
		if failing.Load() {
			return fmt.Errorf("collector down")
		}
		return nil
	}}
	s, conn := grpcBudgetServer(t, sink, sink)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	push := func(body []byte) error {
		return conn.Invoke(ctx, logsExportMethod, &rawProto{b: body}, &rawProto{})
	}
	ordinary, err := plogotlp.NewExportRequestFromLogs(manyResources(4)).MarshalProto()
	if err != nil {
		t.Fatal(err)
	}
	big := wf(1, wf(2, wf(2, wrep(6, nil, 1<<14))))

	check := func(outcome string, err error, wantCode codes.Code) {
		t.Helper()
		if got := status.Code(err); got != wantCode {
			t.Errorf("%s: code = %v, want %v (%v)", outcome, got, wantCode, err)
		}
		s.claims.mu.Lock()
		left := len(s.claims.m)
		s.claims.mu.Unlock()
		if left != 0 {
			t.Errorf("%s: %d claims outlived their push", outcome, left)
		}
		if used := s.decoded.used.Load(); used != 0 {
			t.Errorf("%s: the decoded budget still holds %d bytes", outcome, used)
		}
	}

	check("admitted", push(ordinary), codes.OK)
	check("empty push", push(nil), codes.OK)

	failing.Store(true)
	check("forward failed", push(ordinary), codes.Unavailable)
	failing.Store(false)

	s.decoded.limit = 1 << 10
	check("refused by the decoded budget", push(big), codes.ResourceExhausted)
	s.decoded.limit = 1 << 30

	for range cap(s.inFlight) {
		s.inFlight <- struct{}{}
	}
	check("shed by the in-flight count", push(ordinary), codes.ResourceExhausted)
	for range cap(s.inFlight) {
		<-s.inFlight
	}

	// Estimated (one resource, so a claim IS filed) and then undecodable: the
	// resource's own payload claims five bytes it does not carry.
	malformed := wf(1, []byte{1<<3 | 2, 5})
	if decodedLogsSize(malformed) == 0 {
		t.Fatal("fixture: the malformed push must still file a claim")
	}
	check("malformed", push(malformed), codes.Internal)

	check("admitted after all of the above", push(ordinary), codes.OK)
}

// A unary RPC can end without ever reaching the interceptor that collects its
// claim. grpc-go's RecvMsg receives the message and then a cardinality probe
// expecting end-of-stream; when the peer cancels (or is reaped by the decode
// window) instead of half-closing, or sends a second message into the same
// request, the probe fails and the generated handler returns before the
// interceptor. conn.Invoke always half-closes, so only a client STREAM on the
// unary method reaches these paths. Each used to leak the claim, its
// decoded-budget charge and the decoded request for the process' life — a few
// unauthenticated streams shed BOTH transports until restart — and a second
// message overwrote the first message's charge, which nothing could then return.
func TestGRPCDecodedClaimsDoNotOutliveAnAbortedUnaryStream(t *testing.T) {
	ordinary, err := plogotlp.NewExportRequestFromLogs(manyResources(4)).MarshalProto()
	if err != nil {
		t.Fatal(err)
	}
	big := wf(1, wf(2, wf(2, wrep(6, nil, 1<<14))))
	// Refuses big, admits ordinary — twice over, for the second-message cases.
	const refusingLimit = 256 << 10
	if o, b := decodedLogsSize(ordinary), decodedLogsSize(big); 2*o > refusingLimit || b <= refusingLimit {
		t.Fatalf("fixture: ordinary estimates %d and big %d bytes against a %d limit", o, b, refusingLimit)
	}

	claims := func(s *Server) int {
		s.claims.mu.Lock()
		defer s.claims.mu.Unlock()
		return len(s.claims.m)
	}
	waitFor := func(t *testing.T, what string, cond func() bool) {
		t.Helper()
		for deadline := time.Now().Add(5 * time.Second); !cond(); {
			if time.Now().After(deadline) {
				t.Fatalf("never %s", what)
			}
			time.Sleep(2 * time.Millisecond)
		}
	}

	for _, tc := range []struct {
		name  string
		limit int64  // the decoded budget; 0 keeps the default
		first []byte // the message whose claim must be filed before the abort
		abort func(t *testing.T, cs grpc.ClientStream, cancel context.CancelFunc)
		setup func(s *Server)
	}{
		{name: "cancelled after its message", first: ordinary,
			abort: func(_ *testing.T, _ grpc.ClientStream, cancel context.CancelFunc) { cancel() }},
		{name: "cancelled after a refused message", limit: refusingLimit, first: big,
			abort: func(_ *testing.T, _ grpc.ClientStream, cancel context.CancelFunc) { cancel() }},
		{name: "held open until the decode window reaps it", first: ordinary,
			setup: func(s *Server) { s.reserveWindow = 300 * time.Millisecond },
			abort: func(*testing.T, grpc.ClientStream, context.CancelFunc) {}},
		{name: "a second message", first: ordinary, abort: secondMessage(ordinary)},
		{name: "a second message after a refused one", limit: refusingLimit, first: big, abort: secondMessage(ordinary)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, conn := grpcBudgetServer(t, &countingSink{count: func() error { return nil }}, nil)
			if tc.limit > 0 {
				s.decoded.limit = tc.limit
			}
			if tc.setup != nil {
				tc.setup(s)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			cs, err := conn.NewStream(ctx, &grpc.StreamDesc{ClientStreams: true}, logsExportMethod)
			if err != nil {
				t.Fatal(err)
			}
			if err := cs.SendMsg(&rawProto{b: tc.first}); err != nil {
				t.Fatal(err)
			}
			waitFor(t, "filed a claim for the first message", func() bool { return claims(s) == 1 })
			tc.abort(t, cs, cancel)
			waitFor(t, "returned the aborted stream's claim and charge", func() bool {
				return claims(s) == 0 && s.decoded.used.Load() == 0
			})

			// And the receiver is not left denying the next honest push.
			ctx2, cancel2 := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel2()
			if err := conn.Invoke(ctx2, logsExportMethod, &rawProto{b: ordinary}, &rawProto{}); err != nil {
				t.Fatalf("an honest push after the aborted stream: %v", err)
			}
			if n, used := claims(s), s.decoded.used.Load(); n != 0 || used != 0 {
				t.Errorf("after an honest push: %d claims, %d decoded bytes held", n, used)
			}
		})
	}
}

// secondMessage sends msg as a SECOND message on a unary stream and asserts
// grpc-go answers the cardinality violation — the path on which the codec
// decodes into a request whose claim is already filed.
func secondMessage(msg []byte) func(t *testing.T, cs grpc.ClientStream, _ context.CancelFunc) {
	return func(t *testing.T, cs grpc.ClientStream, _ context.CancelFunc) {
		t.Helper()
		if err := cs.SendMsg(&rawProto{b: msg}); err != nil {
			t.Fatal(err)
		}
		if err := cs.CloseSend(); err != nil {
			t.Fatal(err)
		}
		if err := cs.RecvMsg(&rawProto{}); status.Code(err) != codes.Internal {
			t.Fatalf("two messages on a unary method: %v, want grpc-go's Internal cardinality violation", err)
		}
	}
}
