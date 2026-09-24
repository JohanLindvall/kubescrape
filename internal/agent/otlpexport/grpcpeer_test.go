package otlpexport

// What the gRPC client makes of peers that are not a healthy gRPC collector,
// driven through REAL listeners rather than hand-built errors: the
// classification and the hint both key on text grpc-go writes, so a grpc-go
// upgrade that rewords it must fail here rather than silently change which
// payloads are dropped.

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/plog/plogotlp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// grpcClientTo is a plaintext gRPC client for addr with a captured reporter.
func grpcClientTo(t *testing.T, addr string) (*Client, func() string) {
	t.Helper()
	log, dump := capturedLogger()
	c, err := New(Config{Endpoint: addr, Protocol: "grpc", Insecure: true, Compression: "none", Timeout: 5 * time.Second, RetryAttempts: 1},
		WithReport(log, "the OTLP collector"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c, dump
}

// A proxy in front of the collector that is not gRPC-aware — the
// ingress-nginx default backend, a host or path that does not match yet
// during a Helm upgrade — answers a gRPC request with a plain HTTP 404, and
// grpc-go spells that codes.Unimplemented (HTTPStatusConvTab). Read as the
// collector's own Unimplemented it was PERMANENT: the buffered drain dropped
// the whole backlog with no backoff, the tailer took its permanent-drop path,
// and ingest told senders to discard what they pushed — for a 404, which the
// HTTP arm has always kept transient because routes reprogram during a
// rollout.
func TestGRPC404FromANonGRPCProxyIsTransient(t *testing.T) {
	var p http.Protocols
	p.SetHTTP1(true)
	p.SetUnencryptedHTTP2(true) // gRPC speaks h2c with prior knowledge
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		w.WriteHeader(http.StatusNotFound)
		_, _ = io.WriteString(w, "<html><body><h1>404 Not Found</h1></body></html>")
	}))
	srv.Config.Protocols = &p
	srv.Start()
	defer srv.Close()

	c, dump := grpcClientTo(t, srv.Listener.Addr().String())
	err := c.ExportLogs(context.Background(), oneRecord())
	if err == nil {
		t.Fatal("a 404 from a non-gRPC peer must fail the export")
	}
	st, _ := status.FromError(err)
	if st.Code() != codes.Unimplemented || !strings.HasPrefix(st.Message(), grpcHTTP404Prefix) {
		t.Fatalf("grpc-go no longer spells a non-gRPC 404 as Unimplemented %q...: got %v — re-derive grpcHTTP404", grpcHTTP404Prefix, err)
	}
	if IsPermanent(err) {
		t.Errorf("IsPermanent(%v) = true: an ingress route miss must stay transient like the HTTP arm's 404", err)
	}
	if got := Class(err); got != "transient" {
		t.Errorf("Class = %q, want transient", got)
	}
	if got := Diagnose(err); got != note404 {
		t.Errorf("Diagnose = %q, want the 404 hint (no collector was reached, so 'does not serve this signal' is wrong)", got)
	}
	if out := dump(); strings.Contains(out, "rejected this telemetry outright") {
		t.Errorf("the reporter narrated a transient 404 as an outright rejection:\n%s", out)
	}

	// A collector's OWN Unimplemented — no pipeline for the signal — is still
	// its verdict and still permanent: the 501 parity the HTTP arm keeps.
	own := status.Error(codes.Unimplemented, "unknown service opentelemetry.proto.collector.logs.v1.LogsService")
	if !IsPermanent(own) {
		t.Error("a collector's own Unimplemented must stay permanent")
	}
}

// A cancelled export is not a destination failure. On gRPC — the default —
// grpc-go reports it as status codes.Canceled, which errors.Is(err,
// context.Canceled) does not match, so every shutdown with an export in flight
// (and every unbuffered ingest forward whose sender gave up mid-send) logged
// "are failing" and then a recovery, for a healthy collector.
func TestCancelledGRPCExportIsNotReportedAsAFailure(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	entered := make(chan struct{}, 1)
	srv := grpc.NewServer()
	plogotlp.RegisterGRPCServer(srv, &blockingLogSink{entered: entered})
	go func() { _ = srv.Serve(lis) }()
	defer srv.Stop()

	c, dump := grpcClientTo(t, lis.Addr().String())
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		<-entered
		cancel()
	}()
	err = c.ExportLogs(ctx, oneRecord())
	if status.Code(err) != codes.Canceled {
		t.Fatalf("export error = %v, want grpc-go's codes.Canceled (the spelling this test exists for)", err)
	}
	if out := dump(); strings.Contains(out, "are failing") || strings.Contains(out, "accepted again") {
		t.Errorf("a cancelled gRPC export was narrated as a destination failure:\n%s", out)
	}
}

// blockingLogSink holds every Export until the caller goes away.
type blockingLogSink struct {
	plogotlp.UnimplementedGRPCServer
	entered chan<- struct{}
}

func (s *blockingLogSink) Export(ctx context.Context, _ plogotlp.ExportRequest) (plogotlp.ExportResponse, error) {
	select {
	case s.entered <- struct{}{}:
	default:
	}
	<-ctx.Done()
	return plogotlp.NewExportResponse(), ctx.Err()
}

// A PLAINTEXT gRPC client whose peer does not answer the HTTP/2 preface. The
// two shapes need opposite hints, so each is pinned against a real listener:
// an HTTP/1.1 server (typically an OTLP/HTTP port, 4318) and a TLS server (an
// own-endpoint route or export override naming a TLS backend inherits
// -otlp-insecure, which defaults to true).
func TestPlaintextGRPCToTheWrongListenerIsDiagnosed(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	for _, tc := range []struct {
		name  string
		start func(http.Handler) *httptest.Server
		want  string
	}{
		{"http/1.1 port", httptest.NewServer, "speaks HTTP/1.1, not gRPC"},
		{"tls port", httptest.NewTLSServer, "insecure: false"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv := tc.start(handler)
			defer srv.Close()
			c, dump := grpcClientTo(t, srv.Listener.Addr().String())
			err := c.ExportLogs(context.Background(), oneRecord())
			if err == nil {
				t.Fatal("a plaintext gRPC export to a non-gRPC listener must fail")
			}
			if !strings.Contains(err.Error(), "error reading server preface") {
				t.Fatalf("grpc-go no longer reports %q for this peer: %v — re-derive Diagnose's preface arms", "error reading server preface", err)
			}
			if got := Diagnose(err); !strings.Contains(got, tc.want) {
				t.Errorf("Diagnose(%v) = %q, want it to say %q", err, got, tc.want)
			}
			if out := dump(); !strings.Contains(out, tc.want) {
				t.Errorf("the failure line does not carry the hint:\n%s", out)
			}
		})
	}
}
