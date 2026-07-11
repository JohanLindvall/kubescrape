package otlpingest

import (
	"bytes"
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/JohanLindvall/kubescrape/internal/agent/otlpexport"
	"github.com/JohanLindvall/kubescrape/internal/agent/spool"
	"github.com/klauspost/compress/gzip"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/plog/plogotlp"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/pmetric/pmetricotlp"
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
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", resp.StatusCode)
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
