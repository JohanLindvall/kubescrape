package otlpexport

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/pmetric/pmetricotlp"
)

func testMetrics() pmetric.Metrics {
	md := pmetric.NewMetrics()
	m := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
	m.SetName("x")
	m.SetEmptyGauge().DataPoints().AppendEmpty().SetDoubleValue(1)
	return md
}

func TestHTTPExportWithBearer(t *testing.T) {
	tokenFile := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(tokenFile, []byte("tok42\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	var gotAuth, gotCT, gotPath string
	var gotPoints int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		gotCT = r.Header.Get("Content-Type")
		gotPath = r.URL.Path
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		req := pmetricotlp.NewExportRequest()
		if err := req.UnmarshalProto(body); err == nil {
			gotPoints = req.Metrics().DataPointCount()
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	c, err := New(Config{Endpoint: srv.URL, Protocol: "http", Timeout: 5 * time.Second, BearerTokenFile: tokenFile})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if err := c.ExportMetrics(context.Background(), testMetrics()); err != nil {
		t.Fatal(err)
	}
	if gotAuth != "Bearer tok42" || gotCT != "application/x-protobuf" || gotPath != "/v1/metrics" {
		t.Fatalf("auth=%q ct=%q path=%q", gotAuth, gotCT, gotPath)
	}
	if gotPoints != 1 {
		t.Fatalf("decoded points = %d", gotPoints)
	}
}

func TestHTTPExportRetries(t *testing.T) {
	var calls atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if calls.Add(1) < 3 {
			http.Error(w, "try again", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	c, err := New(Config{
		Endpoint: srv.URL, Protocol: "http", Timeout: 5 * time.Second,
		RetryAttempts: 3, RetryBackoff: 5 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.ExportMetrics(context.Background(), testMetrics()); err != nil {
		t.Fatalf("export after retries: %v (calls=%d)", err, calls.Load())
	}
	if calls.Load() != 3 {
		t.Fatalf("calls = %d, want 3", calls.Load())
	}

	// Exhausted retries surface the error.
	calls.Store(-100)
	if err := c.ExportMetrics(context.Background(), testMetrics()); err == nil {
		t.Fatal("expected error after exhausted retries")
	}
}

func TestConfigValidation(t *testing.T) {
	if _, err := New(Config{Endpoint: "host:4317", Protocol: "http"}); err == nil {
		t.Fatal("http endpoint without scheme must error")
	}
	if _, err := New(Config{Endpoint: "x", Protocol: "carrier-pigeon"}); err == nil {
		t.Fatal("unknown protocol must error")
	}
	if _, err := New(Config{Endpoint: "x:1", Protocol: "grpc", Insecure: true, CAFile: "/nonexistent"}); err == nil {
		t.Fatal("missing CA file must error")
	}
}
