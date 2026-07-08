package otlpingest

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/plog/plogotlp"
	"go.opentelemetry.io/collector/pdata/pmetric"
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
	defer resp.Body.Close()
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
	defer resp.Body.Close()
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
	defer resp.Body.Close()
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
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want 503", resp.StatusCode)
	}
}
