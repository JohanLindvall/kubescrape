package otlpingest

import (
	"bytes"
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/klauspost/compress/gzip"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/plog/plogotlp"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/pmetric/pmetricotlp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
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
