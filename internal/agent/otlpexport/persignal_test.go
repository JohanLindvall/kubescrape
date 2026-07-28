// Tests for the per-signal destination mux (persignal.go).
package otlpexport

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/plog"
)

// testLogsPayload is one log record, enough to exercise a send.
func testLogsPayload() plog.Logs {
	ld := plog.NewLogs()
	ld.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords().AppendEmpty().Body().SetStr("x")
	return ld
}

// countingCollector records hits per path plus the last tenancy header.
func countingCollector(hits *atomic.Int32, tenant *atomic.Value) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		tenant.Store(r.Header.Get("X-Scope-OrgID"))
		w.Header().Set("Content-Type", "application/x-protobuf")
		w.WriteHeader(http.StatusOK)
	}))
}

// A logs override routes ONLY logs to its endpoint; metrics stay on the
// default — and headers merge (section base + override, override winning).
func TestPerSignalRoutesAndMergesHeaders(t *testing.T) {
	var defHits, logHits atomic.Int32
	var defTenant, logTenant atomic.Value
	defSrv := countingCollector(&defHits, &defTenant)
	defer defSrv.Close()
	logSrv := countingCollector(&logHits, &logTenant)
	defer logSrv.Close()

	ps, err := BuildExporter(Config{
		Endpoint: defSrv.URL, Protocol: "http", Compression: "none", Timeout: 5 * time.Second, RetryAttempts: 1,
	}, &ExportConfig{
		Headers: map[string]string{"X-Scope-OrgID": "base-tenant"},
		Logs:    &ExportOverride{Endpoint: logSrv.URL, Headers: map[string]string{"X-Scope-OrgID": "logs-tenant"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ps.Close() }()

	if err := ps.ExportLogs(context.Background(), testLogsPayload()); err != nil {
		t.Fatal(err)
	}
	if err := ps.ExportMetrics(context.Background(), testMetrics()); err != nil {
		t.Fatal(err)
	}
	if logHits.Load() != 1 || defHits.Load() != 1 {
		t.Fatalf("hits: logs=%d default=%d, want 1/1", logHits.Load(), defHits.Load())
	}
	if logTenant.Load() != "logs-tenant" {
		t.Fatalf("logs tenant = %v, want the override", logTenant.Load())
	}
	if defTenant.Load() != "base-tenant" {
		t.Fatalf("default tenant = %v, want the section base header on the DEFAULT chain", defTenant.Load())
	}
}

// Shape validation is dry (no files touched) and rejects half a client cert
// and unknown protocols.
func TestExportConfigValidate(t *testing.T) {
	if err := (&ExportConfig{ClientCertFile: "c.pem"}).Validate(); err == nil {
		t.Fatal("half a client cert pair must be rejected")
	}
	if err := (&ExportConfig{Logs: &ExportOverride{Protocol: "carrier-pigeon"}}).Validate(); err == nil {
		t.Fatal("unknown protocol must be rejected")
	}
	var nilCfg *ExportConfig
	if err := nilCfg.Validate(); err != nil {
		t.Fatalf("nil section: %v", err)
	}
}

// A client certificate on a plaintext destination must refuse startup, not
// silently skip the handshake — for the flag-built base and per-signal
// overrides alike.
func TestClientCertOnPlaintextRefused(t *testing.T) {
	dir := t.TempDir()
	// New checks plaintext-ness BEFORE loading the pair, so paths suffice.
	cert, key := dir+"/c.crt", dir+"/c.key"

	if _, err := New(Config{Endpoint: "h:4317", Protocol: "grpc", Insecure: true,
		ClientCertFile: cert, ClientKeyFile: key}); err == nil {
		t.Fatal("client cert on plaintext gRPC must be refused")
	}
	if _, err := New(Config{Endpoint: "http://h:4318", Protocol: "http",
		ClientCertFile: cert, ClientKeyFile: key}); err == nil {
		t.Fatal("client cert on plain http must be refused")
	}
	// The per-signal path surfaces the same refusal.
	if _, err := BuildExporter(Config{Endpoint: "h:4317", Protocol: "grpc", Insecure: true},
		&ExportConfig{ClientCertFile: cert, ClientKeyFile: key}); err == nil {
		t.Fatal("BuildExporter must refuse a base client cert on a plaintext base")
	}
	// Validate (shape-only) catches a scheme-less http override endpoint.
	if err := (&ExportConfig{Logs: &ExportOverride{Protocol: "http", Endpoint: "loki:3100"}}).Validate(); err == nil {
		t.Fatal("http override endpoint without a scheme must fail Validate")
	}
}
