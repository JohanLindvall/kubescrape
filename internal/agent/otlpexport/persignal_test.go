// Tests for the per-signal destination mux (persignal.go).
package otlpexport

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
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

// The collectorless shape — a client certificate plus an override for EVERY
// signal — must start under the default flags (-otlp-protocol=grpc,
// -otlp-insecure=true). The base is then unreachable, so building it (and
// refusing it for carrying a cert on a plaintext connection) would reject the
// documented config for a destination nothing can ever send to.
func TestAllSignalsOverriddenSkipsUnreachableDefault(t *testing.T) {
	cert, key := writeKeyPair(t)
	base := Config{Endpoint: "otel-collector.monitoring:4317", Protocol: "grpc", Insecure: true}
	no := false
	ps, err := BuildExporter(base, &ExportConfig{
		ClientCertFile: cert, ClientKeyFile: key,
		Logs:    &ExportOverride{Endpoint: "https://loki.example.com/otlp", Protocol: "http"},
		Metrics: &ExportOverride{Endpoint: "https://mimir.example.com/otlp", Protocol: "http"},
		Traces:  &ExportOverride{Endpoint: "tempo.example.com:4317", Insecure: &no},
	})
	if err != nil {
		t.Fatalf("fully-overridden config must start: %v", err)
	}
	defer func() { _ = ps.Close() }()
	if ps.Default != nil {
		t.Fatal("no signal can reach the default; it must not be built")
	}
	// Every signal still resolves to a non-nil client.
	if ps.logsClient() == nil || ps.metricsClient() == nil || ps.tracesClient() == nil {
		t.Fatal("a signal resolved to a nil client")
	}

	// But leave ONE signal on the base and the cert genuinely would be unused
	// there — that must still be refused rather than silently ignored.
	if _, err := BuildExporter(base, &ExportConfig{
		ClientCertFile: cert, ClientKeyFile: key,
		Logs:    &ExportOverride{Endpoint: "https://loki.example.com/otlp", Protocol: "http"},
		Metrics: &ExportOverride{Endpoint: "https://mimir.example.com/otlp", Protocol: "http"},
	}); err == nil {
		t.Fatal("a reachable plaintext default carrying a client cert must be refused")
	}
}

// writeKeyPair writes a throwaway self-signed certificate and key, for the
// TLS-carrying destinations that actually load the pair.
func writeKeyPair(t *testing.T) (certFile, keyFile string) {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "kubescrape-test"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	certFile, keyFile = dir+"/c.crt", dir+"/c.key"
	writePEM(t, certFile, &pem.Block{Type: "CERTIFICATE", Bytes: der})
	writePEM(t, keyFile, &pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
	return certFile, keyFile
}

func writePEM(t *testing.T, path string, b *pem.Block) {
	t.Helper()
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := pem.Encode(f, b); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
}

// Validate walks the three signals in a FIXED order, so a section with two
// mistakes is refused for the same one on every run (the walk used to range a
// map, and a test of the wording could only pin whichever came first).
func TestExportConfigValidateRefusesSignalsInOrder(t *testing.T) {
	cfg := &ExportConfig{
		Logs:   &ExportOverride{Protocol: "carrier-pigeon"},
		Traces: &ExportOverride{Compression: "brotli"},
	}
	for range 20 {
		err := cfg.Validate()
		if err == nil || !strings.Contains(err.Error(), "export.logs.protocol") {
			t.Fatalf("Validate = %v, want the logs override refused first", err)
		}
	}
}
