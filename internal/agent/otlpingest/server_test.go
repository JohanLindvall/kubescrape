package otlpingest

import (
	"bytes"
	"context"
	mathrand "math/rand"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/JohanLindvall/kubescrape/internal/agent/otlpexport"
	"github.com/JohanLindvall/kubescrape/pkg/spool"
	"github.com/klauspost/compress/gzip"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/plog/plogotlp"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/pmetric/pmetricotlp"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

type captureExporter struct {
	logs    []plog.Logs
	metrics []pmetric.Metrics
	fail    bool
}

func (c *captureExporter) ExportLogs(_ context.Context, ld plog.Logs) error {
	if c.fail {
		return context.DeadlineExceeded
	}
	c.logs = append(c.logs, ld)
	return nil
}

func (c *captureExporter) ExportMetrics(_ context.Context, md pmetric.Metrics) error {
	if c.fail {
		return context.DeadlineExceeded
	}
	c.metrics = append(c.metrics, md)
	return nil
}

// httpTestServer wires the ingest HTTP handlers to a captured exporter.
func httpTestServer(t *testing.T, exp Exporter) *httptest.Server {
	t.Helper()
	s := NewServer(ServerConfig{Enricher: newEnricher(newMeta(), MetricsAuto), Exporter: exp})
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/logs", s.handleHTTPLogs)
	mux.HandleFunc("POST /v1/metrics", s.handleHTTPMetrics)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

func TestHTTPLogsRoundTrip(t *testing.T) {
	exp := &captureExporter{}
	srv := httpTestServer(t, exp)

	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	rl.Resource().Attributes().PutStr("container.id", "cafe01")
	rl.ScopeLogs().AppendEmpty().LogRecords().AppendEmpty().Body().SetStr("hi")
	body, err := plogotlp.NewExportRequestFromLogs(ld).MarshalProto()
	if err != nil {
		t.Fatal(err)
	}

	resp, err := http.Post(srv.URL+"/v1/logs", "application/x-protobuf", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d", resp.StatusCode)
	}
	if len(exp.logs) != 1 {
		t.Fatalf("exported %d log batches", len(exp.logs))
	}
	a := exp.logs[0].ResourceLogs().At(0).Resource().Attributes()
	if v, _ := a.Get("k8s.pod.name"); v.Str() != "web-1" {
		t.Errorf("enrichment missing from exported logs: %q", v.Str())
	}
}

func TestHTTPRejectsBadContentType(t *testing.T) {
	srv := httpTestServer(t, &captureExporter{})
	resp, err := http.Post(srv.URL+"/v1/logs", "application/json", bytes.NewReader([]byte("{}")))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusUnsupportedMediaType {
		t.Errorf("status = %d, want 415", resp.StatusCode)
	}
}

// A parameterized media type is still the right media type.
func TestHTTPParameterizedContentType(t *testing.T) {
	exp := &captureExporter{}
	srv := httpTestServer(t, exp)
	ld := plog.NewLogs()
	ld.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords().AppendEmpty().Body().SetStr("hi")
	body, err := plogotlp.NewExportRequestFromLogs(ld).MarshalProto()
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.Post(srv.URL+"/v1/logs", "application/x-protobuf; charset=utf-8", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want 200", resp.StatusCode)
	}
	if len(exp.logs) != 1 {
		t.Errorf("exported %d log batches", len(exp.logs))
	}
}

func TestHTTPMalformedPayload(t *testing.T) {
	srv := httpTestServer(t, &captureExporter{})
	resp, err := http.Post(srv.URL+"/v1/metrics", "application/x-protobuf", bytes.NewReader([]byte("not-proto")))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", resp.StatusCode)
	}
}

func TestHTTPExportFailure(t *testing.T) {
	srv := httpTestServer(t, &captureExporter{fail: true})
	ld := plog.NewLogs()
	ld.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()
	body, _ := plogotlp.NewExportRequestFromLogs(ld).MarshalProto()
	resp, err := http.Post(srv.URL+"/v1/logs", "application/x-protobuf", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want 503", resp.StatusCode)
	}
}

// errExporter fails every export with a fixed error.
type errExporter struct{ err error }

func (e *errExporter) ExportLogs(context.Context, plog.Logs) error          { return e.err }
func (e *errExporter) ExportMetrics(context.Context, pmetric.Metrics) error { return e.err }

// A permanent upstream rejection must reach the HTTP sender as 400 (do not
// retry), mirroring the gRPC path — not as a blanket retryable 503.
func TestHTTPExportPermanentRejection(t *testing.T) {
	srv := httpTestServer(t, &errExporter{err: &otlpexport.HTTPStatusError{Code: 400, Body: "bad"}})
	ld := plog.NewLogs()
	ld.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()
	body, _ := plogotlp.NewExportRequestFromLogs(ld).MarshalProto()
	resp, err := http.Post(srv.URL+"/v1/logs", "application/x-protobuf", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("status = %d, want 400 (permanent rejection must not be retried)", resp.StatusCode)
	}

	// A retryable upstream condition (spool back-pressure) stays 503.
	srv2 := httpTestServer(t, &errExporter{err: spool.ErrFull})
	resp2, err := http.Post(srv2.URL+"/v1/logs", "application/x-protobuf", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp2.Body.Close() }()
	if resp2.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want 503 (back-pressure is retryable)", resp2.StatusCode)
	}
}

func pushedMetrics(containerID string) pmetric.Metrics {
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr("container.id", containerID)
	m := rm.ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
	m.SetName("pushed_total")
	m.SetEmptyGauge().DataPoints().AppendEmpty().SetDoubleValue(1)
	return md
}

func TestHTTPMetricsRoundTrip(t *testing.T) {
	exp := &captureExporter{}
	srv := httpTestServer(t, exp)
	body, err := pmetricotlp.NewExportRequestFromMetrics(pushedMetrics("cafe01")).MarshalProto()
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.Post(srv.URL+"/v1/metrics", "application/x-protobuf", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d", resp.StatusCode)
	}
	if len(exp.metrics) != 1 {
		t.Fatalf("exported %d metric batches", len(exp.metrics))
	}
	a := exp.metrics[0].ResourceMetrics().At(0).Resource().Attributes()
	if v, _ := a.Get("k8s.pod.name"); v.Str() != "web-1" {
		t.Errorf("enrichment missing: %q", v.Str())
	}
}

// OTel SDKs commonly gzip OTLP/HTTP bodies; the receiver must decompress.
func TestHTTPGzipBody(t *testing.T) {
	exp := &captureExporter{}
	srv := httpTestServer(t, exp)

	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	rl.Resource().Attributes().PutStr("container.id", "cafe01")
	rl.ScopeLogs().AppendEmpty().LogRecords().AppendEmpty().Body().SetStr("hi")
	plain, err := plogotlp.NewExportRequestFromLogs(ld).MarshalProto()
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	_, _ = zw.Write(plain)
	_ = zw.Close()

	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/v1/logs", bytes.NewReader(buf.Bytes()))
	req.Header.Set("Content-Type", "application/x-protobuf")
	req.Header.Set("Content-Encoding", "gzip")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("gzip status %d", resp.StatusCode)
	}
	if len(exp.logs) != 1 {
		t.Fatalf("exported %d log batches", len(exp.logs))
	}
	a := exp.logs[0].ResourceLogs().At(0).Resource().Attributes()
	if v, _ := a.Get("k8s.pod.name"); v.Str() != "web-1" {
		t.Errorf("enrichment missing after gzip decode: %q", v.Str())
	}

	// Garbage behind a gzip header and unsupported encodings are 400s.
	for _, tc := range []struct{ enc, body string }{
		{"gzip", "not gzip"},
		{"zstd", "whatever"},
	} {
		req, _ := http.NewRequest(http.MethodPost, srv.URL+"/v1/logs", bytes.NewReader([]byte(tc.body)))
		req.Header.Set("Content-Type", "application/x-protobuf")
		req.Header.Set("Content-Encoding", tc.enc)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusBadRequest {
			t.Errorf("encoding %q status = %d, want 400", tc.enc, resp.StatusCode)
		}
	}
}

// TestServerGRPC runs the real gRPC listener and pushes both signals.
func TestServerGRPC(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := lis.Addr().String()
	_ = lis.Close()

	exp := &captureExporter{}
	srv := NewServer(ServerConfig{
		GRPCAddr: addr,
		Enricher: newEnricher(newMeta(), MetricsAuto),
		Exporter: exp,
	})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { _ = srv.Run(ctx); close(done) }()
	defer func() { cancel(); <-done }()

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()

	// Retry until the listener is up.
	logs := plogotlp.NewGRPCClient(conn)
	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	rl.Resource().Attributes().PutStr("container.id", "cafe01")
	rl.ScopeLogs().AppendEmpty().LogRecords().AppendEmpty().Body().SetStr("hi")
	var lastErr error
	for i := 0; i < 100; i++ {
		if _, lastErr = logs.Export(context.Background(), plogotlp.NewExportRequestFromLogs(ld)); lastErr == nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if lastErr != nil {
		t.Fatal(lastErr)
	}
	if len(exp.logs) != 1 {
		t.Fatalf("exported %d log batches", len(exp.logs))
	}
	a := exp.logs[0].ResourceLogs().At(0).Resource().Attributes()
	if v, _ := a.Get("k8s.pod.name"); v.Str() != "web-1" {
		t.Errorf("gRPC logs not enriched: %q", v.Str())
	}

	metrics := pmetricotlp.NewGRPCClient(conn)
	if _, err := metrics.Export(context.Background(), pmetricotlp.NewExportRequestFromMetrics(pushedMetrics("cafe01"))); err != nil {
		t.Fatal(err)
	}
	if len(exp.metrics) != 1 {
		t.Fatalf("exported %d metric batches", len(exp.metrics))
	}
	ma := exp.metrics[0].ResourceMetrics().At(0).Resource().Attributes()
	if v, _ := ma.Get("k8s.pod.name"); v.Str() != "web-1" {
		t.Errorf("gRPC metrics not enriched: %q", v.Str())
	}

	// A failing exporter surfaces as a gRPC error to the sender.
	exp.fail = true
	if _, err := logs.Export(context.Background(), plogotlp.NewExportRequestFromLogs(ld)); err == nil {
		t.Error("export failure must propagate to the gRPC sender")
	}
}

// A gaugeMetrics helper lives in enrich_test.go; these tests reuse it.

// grpcForwardStatus must return retryable codes for transient forwarding
// failures and permanent codes only for definitive upstream rejections.
func TestGRPCForwardStatus(t *testing.T) {
	cases := []struct {
		err  error
		want codes.Code
	}{
		{spool.ErrFull, codes.Unavailable},
		{&otlpexport.HTTPStatusError{Code: 503, Body: "overloaded"}, codes.Unavailable},
		{&otlpexport.HTTPStatusError{Code: 429, Body: "slow down"}, codes.Unavailable},
		{&otlpexport.HTTPStatusError{Code: 400, Body: "bad"}, codes.InvalidArgument},
		// 401 is transient per otlpexport.IsPermanent (a rotating bearer token
		// produces it for windows retrying survives) — the sender must retry.
		{&otlpexport.HTTPStatusError{Code: 401, Body: "unauthorized"}, codes.Unavailable},
		{&otlpexport.HTTPStatusError{Code: 404, Body: "not here"}, codes.Unavailable},
		{context.DeadlineExceeded, codes.Unavailable},
		{status.Error(codes.ResourceExhausted, "too large"), codes.ResourceExhausted}, // upstream status passes through
	}
	for _, c := range cases {
		got := status.Code(grpcForwardStatus(c.err))
		if got != c.want {
			t.Errorf("grpcForwardStatus(%v) = %v, want %v", c.err, got, c.want)
		}
	}
}

// A runtime listener failure (the HTTP port already bound) must surface as a
// non-nil error from Run so main can crash the agent instead of leaving it
// looking healthy while apps push into a void; a ctx-cancelled shutdown still
// returns nil.
func TestRunReturnsListenerError(t *testing.T) {
	occupied, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = occupied.Close() }()

	srv := NewServer(ServerConfig{
		HTTPAddr: occupied.Addr().String(),
		Enricher: newEnricher(newMeta(), MetricsAuto),
		Exporter: &captureExporter{},
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Run(ctx); err == nil {
		t.Fatal("Run = nil with the HTTP port already bound, want error")
	}

	// A clean ctx-cancelled shutdown returns nil.
	free, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := free.Addr().String()
	_ = free.Close()
	srv2 := NewServer(ServerConfig{
		HTTPAddr: addr,
		Enricher: newEnricher(newMeta(), MetricsAuto),
		Exporter: &captureExporter{},
	})
	ctx2, cancel2 := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- srv2.Run(ctx2) }()
	time.Sleep(50 * time.Millisecond)
	cancel2()
	if err := <-errCh; err != nil {
		t.Fatalf("Run after ctx cancel = %v, want nil", err)
	}
}

// An oversized body must be rejected with 413, not silently truncated (a
// truncated protobuf could unmarshal and be ACKed with its tail dropped).
func TestHTTPBodyTooLarge(t *testing.T) {
	srv := httpTestServer(t, &captureExporter{})
	big := bytes.Repeat([]byte{0x0a}, maxIngestBody+2)
	resp, err := http.Post(srv.URL+"/v1/logs", "application/x-protobuf", bytes.NewReader(big))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want 413", resp.StatusCode)
	}
}

// An oversized COMPRESSED body must also surface as 413 — its truncation at
// the cap would otherwise read as a gzip parse error and blame the sender
// with a 400. The decompressed cap (zip bomb) is a 413 too.
func TestHTTPGzipBodyTooLarge(t *testing.T) {
	srv := httpTestServer(t, &captureExporter{})

	post := func(body []byte) int {
		t.Helper()
		req, _ := http.NewRequest(http.MethodPost, srv.URL+"/v1/logs", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/x-protobuf")
		req.Header.Set("Content-Encoding", "gzip")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = resp.Body.Close() }()
		return resp.StatusCode
	}

	// Incompressible data: the compressed body itself exceeds the cap.
	raw := make([]byte, maxIngestBody+(1<<20))
	rnd := mathrand.New(mathrand.NewSource(1))
	_, _ = rnd.Read(raw)
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	_, _ = zw.Write(raw)
	_ = zw.Close()
	if buf.Len() <= maxIngestBody {
		t.Fatalf("test payload compressed to %d bytes, below the cap", buf.Len())
	}
	if code := post(buf.Bytes()); code != http.StatusRequestEntityTooLarge {
		t.Errorf("oversized compressed body status = %d, want 413", code)
	}

	// Zip bomb: tiny compressed, decompresses beyond the cap.
	var bomb bytes.Buffer
	zw = gzip.NewWriter(&bomb)
	_, _ = zw.Write(make([]byte, maxIngestBody+2))
	_ = zw.Close()
	if code := post(bomb.Bytes()); code != http.StatusRequestEntityTooLarge {
		t.Errorf("zip-bomb status = %d, want 413", code)
	}
}

// Auto mode with a mixed batch: the resource that carried its ID at the
// resource level keeps its enrichment when the batch falls back to splitting.
func TestAutoSplitKeepsResourceLevelID(t *testing.T) {
	md := pmetric.NewMetrics()
	// RM-A: id on the resource, none on the points.
	rmA := md.ResourceMetrics().AppendEmpty()
	rmA.SetSchemaUrl("https://example.com/schema/1")
	rmA.Resource().Attributes().PutStr("container.id", "cafe01")
	mA := rmA.ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
	mA.SetName("a_metric")
	mA.Metadata().PutStr("prometheus.type", "gauge")
	mA.SetEmptyGauge().DataPoints().AppendEmpty().SetDoubleValue(1)
	// RM-B: ids on the points.
	rmB := md.ResourceMetrics().AppendEmpty()
	mB := rmB.ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
	mB.SetName("b_metric")
	dpB := mB.SetEmptyGauge().DataPoints().AppendEmpty()
	dpB.SetDoubleValue(1)
	dpB.Attributes().PutStr("k8s.pod.uid", "pod-uid-2")

	out := newEnricher(newMeta(), MetricsAuto).EnrichMetrics(context.Background(), md)
	got := collectPodNames(out)
	if got["web-1"] != 1 || got["web-2"] != 1 {
		t.Fatalf("mixed auto-split points = %+v (resource-level ID lost)", got)
	}
	// SchemaUrl and metric metadata survive the rebuild.
	rms := out.ResourceMetrics()
	for i := 0; i < rms.Len(); i++ {
		rm := rms.At(i)
		if name, _ := rm.Resource().Attributes().Get("k8s.pod.name"); name.Str() == "web-1" {
			if rm.SchemaUrl() != "https://example.com/schema/1" {
				t.Errorf("schema url = %q", rm.SchemaUrl())
			}
			m := rm.ScopeMetrics().At(0).Metrics().At(0)
			if v, ok := m.Metadata().Get("prometheus.type"); !ok || v.Str() != "gauge" {
				t.Errorf("metric metadata lost: %v", m.Metadata().AsRaw())
			}
		}
	}
}

func tracesWith(resAttrs map[string]string) ptrace.Traces {
	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	for k, v := range resAttrs {
		rs.Resource().Attributes().PutStr(k, v)
	}
	span := rs.ScopeSpans().AppendEmpty().Spans().AppendEmpty()
	span.SetName("op")
	return td
}

type captureTraces struct{ traces []ptrace.Traces }

func (c *captureTraces) ExportTraces(_ context.Context, td ptrace.Traces) error {
	c.traces = append(c.traces, td)
	return nil
}

// Traces round-trip over HTTP: enriched by container ID and forwarded.
func TestHTTPTracesRoundTrip(t *testing.T) {
	texp := &captureTraces{}
	s := NewServer(ServerConfig{Enricher: newEnricher(newMeta(), MetricsAuto), Exporter: &captureExporter{}, Traces: texp})
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/traces", s.handleHTTPTraces)
	srv := httptest.NewServer(mux)
	defer srv.Close()

	body, err := ptraceotlp.NewExportRequestFromTraces(tracesWith(map[string]string{"container.id": "cafe01"})).MarshalProto()
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.Post(srv.URL+"/v1/traces", "application/x-protobuf", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	if len(texp.traces) != 1 {
		t.Fatalf("forwarded traces = %d", len(texp.traces))
	}
	got := texp.traces[0].ResourceSpans().At(0).Resource().Attributes()
	if v, ok := got.Get("k8s.pod.name"); !ok || v.Str() != "web-1" {
		t.Fatalf("trace resource not enriched: %v", got.AsRaw())
	}
}

// Peer-IP fallback: a resource with no ID resolves via the connection's peer
// IP when enabled, and stays untouched when disabled.
func TestPeerIPFallback(t *testing.T) {
	enr := NewEnricher(Config{Meta: newMeta(), PeerIPFallback: true})
	ld := plog.NewLogs()
	lr := ld.ResourceLogs().AppendEmpty()
	lr.ScopeLogs().AppendEmpty().LogRecords().AppendEmpty().Body().SetStr("hi")

	ctx := withPeerIP(context.Background(), "10.1.2.3:41234")
	enr.EnrichLogs(ctx, ld)
	a := ld.ResourceLogs().At(0).Resource().Attributes()
	if v, ok := a.Get("k8s.pod.name"); !ok || v.Str() != "web-3" {
		t.Fatalf("peer-IP fallback did not enrich: %v", a.AsRaw())
	}

	// Disabled: untouched.
	enr2 := NewEnricher(Config{Meta: newMeta(), PeerIPFallback: false})
	ld2 := plog.NewLogs()
	ld2.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords().AppendEmpty().Body().SetStr("hi")
	enr2.EnrichLogs(ctx, ld2)
	if n := ld2.ResourceLogs().At(0).Resource().Attributes().Len(); n != 0 {
		t.Fatalf("fallback disabled but resource enriched: %d attrs", n)
	}

	// Unknown peer IP: untouched (and no error).
	ld3 := plog.NewLogs()
	ld3.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords().AppendEmpty().Body().SetStr("hi")
	enr.EnrichLogs(withPeerIP(context.Background(), "172.99.0.1:1"), ld3)
	if n := ld3.ResourceLogs().At(0).Resource().Attributes().Len(); n != 0 {
		t.Fatalf("unknown peer enriched: %d attrs", n)
	}
}

// The datapoint splitter's ID-less group also uses the peer-IP fallback.
func TestPeerIPFallbackSplitMode(t *testing.T) {
	enr := NewEnricher(Config{Meta: newMeta(), MetricsMode: MetricsDatapoint, PeerIPFallback: true})
	md := gaugeMetrics(nil, map[string]any{"path": "/x"}) // no IDs anywhere
	out := enr.EnrichMetrics(withPeerIP(context.Background(), "10.1.2.3:5"), md)
	got := collectPodNames(out)
	if got["web-3"] != 1 {
		t.Fatalf("split-mode peer fallback points = %+v", got)
	}
}

// The gRPC traces service enriches and forwards, wired through Server.Run.
func TestServerGRPCTraces(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := lis.Addr().String()
	_ = lis.Close()

	texp := &captureTraces{}
	srv := NewServer(ServerConfig{
		GRPCAddr: addr,
		Enricher: newEnricher(newMeta(), MetricsAuto),
		Exporter: &captureExporter{},
		Traces:   texp,
	})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { _ = srv.Run(ctx); close(done) }()
	defer func() { cancel(); <-done }()

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()

	traces := ptraceotlp.NewGRPCClient(conn)
	td := tracesWith(map[string]string{"container.id": "cafe01"})
	var lastErr error
	for i := 0; i < 100; i++ {
		if _, lastErr = traces.Export(context.Background(), ptraceotlp.NewExportRequestFromTraces(td)); lastErr == nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if lastErr != nil {
		t.Fatal(lastErr)
	}
	if len(texp.traces) != 1 {
		t.Fatalf("forwarded traces = %d", len(texp.traces))
	}
	a := texp.traces[0].ResourceSpans().At(0).Resource().Attributes()
	if v, _ := a.Get("k8s.pod.name"); v.Str() != "web-1" {
		t.Fatalf("trace resource not enriched over gRPC: %v", a.AsRaw())
	}
}
