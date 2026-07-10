package otlpexport

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/klauspost/compress/gzip"

	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/plog/plogotlp"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/pmetric/pmetricotlp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
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

	var gotAuth, gotCT, gotEnc, gotPath string
	var gotPoints int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		gotCT = r.Header.Get("Content-Type")
		gotEnc = r.Header.Get("Content-Encoding")
		gotPath = r.URL.Path
		reader := io.Reader(r.Body)
		if gotEnc == "gzip" {
			zr, err := gzip.NewReader(r.Body)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			reader = zr
		}
		body, err := io.ReadAll(reader)
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

	// Default compression is gzip; "none" sends the plain body.
	for _, compression := range []string{"", "none"} {
		gotPoints = 0
		c, err := New(Config{Endpoint: srv.URL, Protocol: "http", Timeout: 5 * time.Second,
			BearerTokenFile: tokenFile, Compression: compression})
		if err != nil {
			t.Fatal(err)
		}
		if err := c.ExportMetrics(context.Background(), testMetrics()); err != nil {
			t.Fatal(err)
		}
		_ = c.Close()
		if gotAuth != "Bearer tok42" || gotCT != "application/x-protobuf" || gotPath != "/v1/metrics" {
			t.Fatalf("auth=%q ct=%q path=%q", gotAuth, gotCT, gotPath)
		}
		wantEnc := "gzip"
		if compression == "none" {
			wantEnc = ""
		}
		if gotEnc != wantEnc {
			t.Fatalf("Content-Encoding = %q, want %q (compression %q)", gotEnc, wantEnc, compression)
		}
		if gotPoints != 1 {
			t.Fatalf("decoded points = %d (compression %q)", gotPoints, compression)
		}
	}

	if _, err := New(Config{Endpoint: srv.URL, Protocol: "http", Compression: "snappy"}); err == nil {
		t.Fatal("invalid compression must error")
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

type grpcState struct {
	mu      sync.Mutex
	logs    int
	metrics int
	auth    string
}

func (s *grpcState) captureAuth(ctx context.Context) {
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		if v := md.Get("authorization"); len(v) > 0 {
			s.auth = v[0]
		}
	}
}

type logSink struct {
	plogotlp.UnimplementedGRPCServer
	st *grpcState
}

func (s *logSink) Export(ctx context.Context, req plogotlp.ExportRequest) (plogotlp.ExportResponse, error) {
	s.st.mu.Lock()
	defer s.st.mu.Unlock()
	s.st.logs += req.Logs().LogRecordCount()
	s.st.captureAuth(ctx)
	return plogotlp.NewExportResponse(), nil
}

type metricSink struct {
	pmetricotlp.UnimplementedGRPCServer
	st *grpcState
}

func (s *metricSink) Export(ctx context.Context, req pmetricotlp.ExportRequest) (pmetricotlp.ExportResponse, error) {
	s.st.mu.Lock()
	defer s.st.mu.Unlock()
	s.st.metrics += req.Metrics().DataPointCount()
	s.st.captureAuth(ctx)
	return pmetricotlp.NewExportResponse(), nil
}

func TestGRPCExport(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	sink := &grpcState{}
	srv := grpc.NewServer()
	plogotlp.RegisterGRPCServer(srv, &logSink{st: sink})
	pmetricotlp.RegisterGRPCServer(srv, &metricSink{st: sink})
	go func() { _ = srv.Serve(lis) }()
	defer srv.Stop()

	tokenFile := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(tokenFile, []byte("grpc-tok"), 0o600); err != nil {
		t.Fatal(err)
	}
	c, err := New(Config{
		Endpoint: lis.Addr().String(), Protocol: "grpc", Insecure: true,
		Timeout: 5 * time.Second, BearerTokenFile: tokenFile,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()

	ld := plog.NewLogs()
	ld.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords().AppendEmpty().Body().SetStr("hi")
	if err := c.ExportLogs(context.Background(), ld); err != nil {
		t.Fatal(err)
	}
	if err := c.ExportMetrics(context.Background(), testMetrics()); err != nil {
		t.Fatal(err)
	}

	sink.mu.Lock()
	defer sink.mu.Unlock()
	if sink.logs != 1 || sink.metrics != 1 {
		t.Fatalf("received logs=%d metrics=%d", sink.logs, sink.metrics)
	}
	if sink.auth != "Bearer grpc-tok" {
		t.Fatalf("authorization = %q", sink.auth)
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
