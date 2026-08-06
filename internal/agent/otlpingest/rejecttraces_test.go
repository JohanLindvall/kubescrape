package otlpingest

// The receive-path refusal seam (ServerConfig.RejectTraces).
//
// What is under test is an ORDERING, not a verdict: the guard's answer is the
// same either way, and the whole value of taking it above EnrichTraces is that
// a refused payload costs no metadata lookup and moves no ingest counter. So
// every test here watches the MetadataSource — a lookup on a refused push is
// the regression, and the accepted push that follows is what makes the zero
// mean something.

import (
	"bytes"
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// n reads countingMeta's lookup count (enrich_test.go) under its lock: here it
// is written by a handler goroutine and read by the test's.
func (c *countingMeta) n() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.calls
}

// refuseAttr marks the payload this test's guard turns away. The real one keys
// on servicegraph.ForwardedMarker; this package must not know that.
const refuseAttr = "test.refuse"

// refuseMarked is a guard shaped like the trace tier's: it refuses PERMANENTLY,
// which is what keeps the sender from re-pushing a payload nothing will accept.
func refuseMarked(_ context.Context, td ptrace.Traces) error {
	rss := td.ResourceSpans()
	for i := 0; i < rss.Len(); i++ {
		if _, ok := rss.At(i).Resource().Attributes().Get(refuseAttr); ok {
			return status.Error(codes.InvalidArgument, "refused on the receive path")
		}
	}
	return nil
}

// guardedServer is a traces-only receiver with the guard wired and one in-flight
// slot: a slot the refusal failed to release would shed the push that follows.
func guardedServer(cfg ServerConfig) (*Server, *countingMeta, *captureTraces) {
	meta := &countingMeta{fakeMeta: newMeta()}
	texp := &captureTraces{}
	cfg.Enricher = NewEnricher(Config{Meta: meta})
	cfg.Traces = texp
	cfg.RejectTraces = refuseMarked
	cfg.MaxInFlight = 1
	return NewServer(cfg), meta, texp
}

// drained waits for the admission accounting to settle. A handler's releases run
// in defers, i.e. after the response the client is already reading.
func drained(t *testing.T, s *Server) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if s.buffer.used.Load() == 0 && len(s.inFlight) == 0 {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("a refused push left the admission accounting held: %d budget bytes, %d in-flight slots",
		s.buffer.used.Load(), len(s.inFlight))
}

// The HTTP arm: refused with 400 — which otlpexport.IsPermanent reads as
// do-not-retry — and refused without the enricher being consulted at all.
func TestRejectTracesRefusesBeforeEnrichmentHTTP(t *testing.T) {
	s, meta, texp := guardedServer(ServerConfig{})
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/traces", s.handleHTTPTraces)
	srv := httptest.NewServer(mux)
	defer srv.Close()

	post := func(t *testing.T, td ptrace.Traces) int {
		t.Helper()
		body, err := ptraceotlp.NewExportRequestFromTraces(td).MarshalProto()
		if err != nil {
			t.Fatal(err)
		}
		resp, err := http.Post(srv.URL+"/v1/traces", "application/x-protobuf", bytes.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = resp.Body.Close() }()
		return resp.StatusCode
	}

	refused := tracesWith(map[string]string{"container.id": "cafe01", refuseAttr: "yes"})
	if code := post(t, refused); code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400: a retryable refusal has the sender re-push a payload nothing will accept", code)
	}
	if n := meta.n(); n != 0 {
		t.Errorf("a refused push cost %d metadata lookups; the guard runs above enrichment precisely so it costs none", n)
	}
	if len(texp.traces) != 0 {
		t.Error("a refused payload reached the traces exporter")
	}
	drained(t, s)

	// The same payload without the marker: enriched and forwarded as before, so
	// the zero above is an ordering result and not a broken enricher.
	if code := post(t, tracesWith(map[string]string{"container.id": "cafe01"})); code != http.StatusOK {
		t.Fatalf("an ordinary push answered %d", code)
	}
	if meta.n() == 0 {
		t.Error("an accepted push consulted no metadata source; the refusal assertion above proves nothing")
	}
	if len(texp.traces) != 1 {
		t.Fatalf("forwarded traces = %d, want 1", len(texp.traces))
	}
	if v, ok := texp.traces[0].ResourceSpans().At(0).Resource().Attributes().Get("k8s.pod.name"); !ok || v.Str() != "web-1" {
		t.Error("an accepted push was not enriched")
	}
}

// The gRPC arm, wired through Server.Run: the same refusal, the same permanence
// (InvalidArgument), the same untouched enricher.
func TestRejectTracesRefusesBeforeEnrichmentGRPC(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := lis.Addr().String()
	_ = lis.Close()

	s, meta, texp := guardedServer(ServerConfig{GRPCAddr: addr})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { _ = s.Run(ctx); close(done) }()
	defer func() { cancel(); <-done }()

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	client := ptraceotlp.NewGRPCClient(conn)

	send := func(t *testing.T, td ptrace.Traces) error {
		t.Helper()
		var lastErr error
		for i := 0; i < 100; i++ {
			_, lastErr = client.Export(context.Background(), ptraceotlp.NewExportRequestFromTraces(td))
			if status.Code(lastErr) != codes.Unavailable {
				return lastErr // the listener is up; this is the server's answer
			}
			time.Sleep(20 * time.Millisecond)
		}
		return lastErr
	}

	err = send(t, tracesWith(map[string]string{"container.id": "cafe01", refuseAttr: "yes"}))
	if got := status.Code(err); got != codes.InvalidArgument {
		t.Errorf("refusal code = %v (%v), want InvalidArgument: anything retryable has the sender re-push it forever", got, err)
	}
	if n := meta.n(); n != 0 {
		t.Errorf("a refused push cost %d metadata lookups", n)
	}
	if len(texp.traces) != 0 {
		t.Error("a refused payload reached the traces exporter")
	}
	drained(t, s)

	if err := send(t, tracesWith(map[string]string{"container.id": "cafe01"})); err != nil {
		t.Fatalf("an ordinary push failed: %v", err)
	}
	if meta.n() == 0 {
		t.Error("an accepted push consulted no metadata source; the refusal assertion above proves nothing")
	}
	if len(texp.traces) != 1 {
		t.Fatalf("forwarded traces = %d, want 1", len(texp.traces))
	}
}

// The refusal a guard returns is ALREADY a gRPC status, and already the code
// this receiver's classifier would choose. Re-wrapping it rendered the sender
// `code = InvalidArgument desc = rpc error: code = InvalidArgument desc = …`,
// which buries the reason it needs one nesting deep inside the field it reads
// first — and every test here asserted only the code, so nothing held it.
func TestPermanentStatusRefusalIsRelayedVerbatim(t *testing.T) {
	const msg = "refused on the receive path"
	err := grpcForwardStatus(status.Error(codes.InvalidArgument, msg))

	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.InvalidArgument {
		t.Fatalf("status = %v, want InvalidArgument", err)
	}
	if st.Message() != msg {
		t.Errorf("status message = %q, want %q", st.Message(), msg)
	}
	if strings.Count(err.Error(), "rpc error:") != 1 {
		t.Errorf("rendered error = %q: a status must not be wrapped in a status", err)
	}
}

// An unset guard is the DaemonSet's -ingest server, and it must change nothing:
// the field is opt-in, and a nil hook is not a refusal.
func TestNoRejectTracesHookAcceptsEverything(t *testing.T) {
	meta := &countingMeta{fakeMeta: newMeta()}
	texp := &captureTraces{}
	s := NewServer(ServerConfig{Enricher: NewEnricher(Config{Meta: meta}), Traces: texp})
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/traces", s.handleHTTPTraces)
	srv := httptest.NewServer(mux)
	defer srv.Close()

	body, err := ptraceotlp.NewExportRequestFromTraces(
		tracesWith(map[string]string{"container.id": "cafe01", refuseAttr: "yes"})).MarshalProto()
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.Post(srv.URL+"/v1/traces", "application/x-protobuf", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d with no guard configured", resp.StatusCode)
	}
	if len(texp.traces) != 1 || meta.n() == 0 {
		t.Errorf("with no guard the payload must be enriched and forwarded: traces=%d lookups=%d",
			len(texp.traces), meta.n())
	}
}
