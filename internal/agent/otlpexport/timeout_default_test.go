package otlpexport

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/pmetric"
)

// A zero Config.Timeout must be defaulted, never handed to
// context.WithTimeout: that yields an already-expired context, so every send
// fails instantly with DeadlineExceeded. Because that error is transient AND
// not a collector response, the disk buffer's poison budget can never arm — the
// spool retries forever, fills to its cap and back-pressures every producer,
// producing a total permanent telemetry outage with nothing counted as dropped.
func TestZeroTimeoutIsDefaulted(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	c, err := New(Config{Endpoint: srv.URL, Protocol: "http", Compression: "none", RetryAttempts: 1})
	if err != nil {
		t.Fatal(err)
	}
	if c.cfg.Timeout <= 0 {
		t.Fatalf("Timeout left at %v; New must default it like every other field", c.cfg.Timeout)
	}

	md := pmetric.NewMetrics()
	g := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
	g.SetName("x")
	g.SetEmptyGauge().DataPoints().AppendEmpty().SetIntValue(1)

	if err := c.ExportMetrics(context.Background(), md); err != nil {
		t.Fatalf("export against a healthy 200 collector failed with an unset Timeout: %v", err)
	}
}

// An explicitly configured timeout must be honoured, not overwritten.
func TestExplicitTimeoutPreserved(t *testing.T) {
	c, err := New(Config{Endpoint: "http://127.0.0.1:1", Protocol: "http", Compression: "none", Timeout: 3 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if c.cfg.Timeout != 3*time.Second {
		t.Fatalf("Timeout = %v, want 3s", c.cfg.Timeout)
	}
}
