package otlpexport

// The OTLP/HTTP partial-success flow lives ONCE (Client.httpExport) and is
// shared by all six HTTP sends — the three pdata sends and rawsend.go's raw
// (spooled-bytes) siblings. These tests pin the shared behavior per signal and
// per path: a 2xx whose body carries partial_success counts the rejected
// records into obs.ExportRejected WITHOUT failing the export, and an
// undecodable 2xx body is advisory noise, never a failure.

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/plog/plogotlp"
	"go.opentelemetry.io/collector/pdata/pmetric/pmetricotlp"
	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"

	"github.com/JohanLindvall/kubescrape/internal/obs"
)

// partialSuccessBody renders the signal's ExportResponse claiming n rejected
// records.
func partialSuccessBody(t *testing.T, path string, n int64) []byte {
	t.Helper()
	var (
		body []byte
		err  error
	)
	switch path {
	case "/v1/logs":
		resp := plogotlp.NewExportResponse()
		resp.PartialSuccess().SetRejectedLogRecords(n)
		resp.PartialSuccess().SetErrorMessage("rejected")
		body, err = resp.MarshalProto()
	case "/v1/metrics":
		resp := pmetricotlp.NewExportResponse()
		resp.PartialSuccess().SetRejectedDataPoints(n)
		resp.PartialSuccess().SetErrorMessage("rejected")
		body, err = resp.MarshalProto()
	case "/v1/traces":
		resp := ptraceotlp.NewExportResponse()
		resp.PartialSuccess().SetRejectedSpans(n)
		resp.PartialSuccess().SetErrorMessage("rejected")
		body, err = resp.MarshalProto()
	default:
		t.Fatalf("unexpected path %s", path)
	}
	if err != nil {
		t.Fatal(err)
	}
	return body
}

func TestHTTPPartialSuccessCountsRejectedRecords(t *testing.T) {
	// Built before the server starts: the handler must not call t.Fatal.
	bodies := map[string][]byte{
		"/v1/logs":    partialSuccessBody(t, "/v1/logs", 3),
		"/v1/metrics": partialSuccessBody(t, "/v1/metrics", 5),
		"/v1/traces":  partialSuccessBody(t, "/v1/traces", 7),
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(bodies[r.URL.Path])
	}))
	defer srv.Close()

	c, err := New(Config{Endpoint: srv.URL, Protocol: "http", Timeout: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()

	ctx := context.Background()
	rejected := func(signal string) float64 {
		return obs.ExportRejected.WithLabelValues(signal).Value()
	}

	// The pdata sends: partial_success counts, the export still succeeds.
	before := rejected("logs")
	if err := c.ExportLogs(ctx, testLogsPayload()); err != nil {
		t.Fatalf("logs: %v", err)
	}
	if got := rejected("logs") - before; got != 3 {
		t.Fatalf("ExportRejected{logs} delta = %v, want 3", got)
	}
	before = rejected("metrics")
	if err := c.ExportMetrics(ctx, testMetrics()); err != nil {
		t.Fatalf("metrics: %v", err)
	}
	if got := rejected("metrics") - before; got != 5 {
		t.Fatalf("ExportRejected{metrics} delta = %v, want 5", got)
	}
	before = rejected("traces")
	if err := c.ExportTraces(ctx, testTraces()); err != nil {
		t.Fatalf("traces: %v", err)
	}
	if got := rejected("traces") - before; got != 7 {
		t.Fatalf("ExportRejected{traces} delta = %v, want 7", got)
	}

	// The raw (spooled-bytes) sends share the same flow — a divergence here is
	// exactly what folding the six copies into httpExport forecloses.
	lf, mf, tf, _ := c.rawSingleAttemptSends()
	for _, tc := range []struct {
		signal string
		wire   func() ([]byte, error)
		send   func(context.Context, []byte) error
		want   float64
	}{
		{"logs", plogotlp.NewExportRequestFromLogs(testLogsPayload()).MarshalProto, lf, 3},
		{"metrics", pmetricotlp.NewExportRequestFromMetrics(testMetrics()).MarshalProto, mf, 5},
		{"traces", ptraceotlp.NewExportRequestFromTraces(testTraces()).MarshalProto, tf, 7},
	} {
		wire, err := tc.wire()
		if err != nil {
			t.Fatal(err)
		}
		before := rejected(tc.signal)
		if err := tc.send(ctx, wire); err != nil {
			t.Fatalf("raw %s: %v", tc.signal, err)
		}
		if got := rejected(tc.signal) - before; got != tc.want {
			t.Fatalf("ExportRejected{%s} delta = %v, want %v (raw path)", tc.signal, got, tc.want)
		}
	}
}

// An undecodable 2xx body reads as NO partial_success and never as a failure:
// the request was accepted, and the response is only advisory.
func TestHTTPUndecodablePartialSuccessBodyIsIgnored(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte{0xff}) // wire type 7: never a valid ExportResponse
	}))
	defer srv.Close()

	c, err := New(Config{Endpoint: srv.URL, Protocol: "http", Timeout: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()

	ctx := context.Background()
	for _, tc := range []struct {
		signal string
		send   func() error
	}{
		{"logs", func() error { return c.ExportLogs(ctx, testLogsPayload()) }},
		{"metrics", func() error { return c.ExportMetrics(ctx, testMetrics()) }},
		{"traces", func() error { return c.ExportTraces(ctx, testTraces()) }},
	} {
		before := obs.ExportRejected.WithLabelValues(tc.signal).Value()
		if err := tc.send(); err != nil {
			t.Fatalf("%s: %v", tc.signal, err)
		}
		if got := obs.ExportRejected.WithLabelValues(tc.signal).Value() - before; got != 0 {
			t.Fatalf("ExportRejected{%s} delta = %v, want 0 for an undecodable body", tc.signal, got)
		}
	}
}
