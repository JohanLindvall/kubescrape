package otlpingest

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/plog/plogotlp"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// budgetTestServer wires the HTTP handlers with a small buffer budget.
func budgetTestServer(t *testing.T, limit int64) (*Server, *httptest.Server) {
	t.Helper()
	s := NewServer(ServerConfig{
		Enricher: newEnricher(newMeta(), MetricsAuto),
		Exporter: exporterFunc(func(plog.Logs) error { return nil }),
	})
	s.buffer.limit = limit
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/logs", s.handleHTTPLogs)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return s, srv
}

// The HTTP arm reads the whole body BEFORE taking an in-flight slot — it must,
// or a trickled upload sheds every other sender for a ReadTimeout — so the
// count bound does not bound the memory those reads accumulate. Senders that no
// longer fit the byte budget must be refused RETRYABLY while the bytes are
// still theirs, and the receiver's resident bytes must stay near the budget
// rather than near (senders x body cap).
func TestHTTPBufferBudgetBoundsResidentBytes(t *testing.T) {
	const (
		limit   = 8 << 20
		body    = 4 << 20
		senders = 16
	)
	_, srv := budgetTestServer(t, limit)
	addr := srv.Listener.Addr().String()

	var (
		wg      sync.WaitGroup
		refused atomic.Int64
		holding atomic.Int64
	)
	release := make(chan struct{})
	for range senders {
		wg.Add(1)
		go func() {
			defer wg.Done()
			conn, err := net.Dial("tcp", addr)
			if err != nil {
				return
			}
			defer func() { _ = conn.Close() }()
			_, _ = fmt.Fprintf(conn, "POST /v1/logs HTTP/1.1\r\nHost: x\r\n"+
				"Content-Type: application/x-protobuf\r\nContent-Length: %d\r\n\r\n", body)
			// All but the last byte: an admitted handler is now parked inside
			// its read, holding the payload it has buffered.
			if _, err := conn.Write(make([]byte, body-1)); err != nil {
				// The 429 arrived while we were still writing and the server
				// closed on an undrained 4 MiB body, so the socket is reset
				// before the response can be read back. That the sender was
				// refused is what matters here; the refusal's SHAPE is pinned
				// by TestHTTPBufferBudgetRefusalIsRetryable, which uses a body
				// small enough to answer cleanly.
				refused.Add(1)
				return
			}
			_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
			br := bufio.NewReader(conn)
			resp, err := http.ReadResponse(br, nil)
			if err != nil {
				holding.Add(1) // still parked in the read: nothing answered yet
				<-release
				return
			}
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusTooManyRequests {
				refused.Add(1)
			}
		}()
	}

	// Give every sender time to either be parked mid-body or be refused.
	time.Sleep(1500 * time.Millisecond)
	runtime.GC()
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	resident := int64(ms.HeapAlloc)
	close(release)
	wg.Wait()

	t.Logf("heap in use with %d senders x %d MiB mid-upload: %.1f MiB (budget %d MiB); %d admitted-and-parked, %d refused",
		senders, body>>20, float64(resident)/(1<<20), limit>>20, holding.Load(), refused.Load())

	if refused.Load() == 0 {
		t.Fatal("no sender was refused: the budget never bound, so this proves nothing")
	}
	// Every sender buffered would be senders*body = 64 MiB. Allow generous
	// slack for the test binary's own heap; the failure mode this pins is the
	// whole wave being resident at once.
	if max := int64(4 * limit); resident > max {
		t.Errorf("heap in use %d bytes exceeds %d: the read path buffered past the budget", resident, max)
	}
}

// A budget refusal must leave the payload with the sender, which means it has
// to be retryable on the wire: 429 plus Retry-After, exactly like the in-flight
// shed. (Anything else — a 400, a 503 without the hint — turns back-pressure
// into data loss on an unauthenticated receiver.)
func TestHTTPBufferBudgetRefusalIsRetryable(t *testing.T) {
	_, srv := budgetTestServer(t, 1) // no room for any push

	ld := plog.NewLogs()
	ld.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords().AppendEmpty().Body().SetStr("x")
	body, err := plogotlp.NewExportRequestFromLogs(ld).MarshalProto()
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.Post(srv.URL+"/v1/logs", "application/x-protobuf", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("status = %d, want 429", resp.StatusCode)
	}
	if resp.Header.Get("Retry-After") == "" {
		t.Error("no Retry-After: a shed that a sender cannot schedule is a shed that loses data")
	}
}

// A budget refusal on the gRPC arm must reach the sender as a RETRYABLE
// ResourceExhausted. It is issued from the tap (the only hook that runs before
// grpc-go decodes the message, which is the point: the semaphore never sees a
// push until its bytes are already resident), and a tap status is written by
// http2Server.writeEarlyAbort — which forwards status DETAILS, so the RetryInfo
// that keeps the code retryable survives the early abort.
func TestGRPCBufferBudgetRefusalCarriesRetryInfo(t *testing.T) {
	s := NewServer(ServerConfig{
		Enricher: newEnricher(newMeta(), MetricsAuto),
		Exporter: exporterFunc(func(plog.Logs) error { return nil }),
		Traces:   nil,
	})
	// No room for even one push: every RPC is refused at the tap.
	s.buffer.limit = 1

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer(
		grpc.InTapHandle(s.tapAdmit),
		grpc.UnaryInterceptor(s.limitUnary),
	)
	plogotlp.RegisterGRPCServer(srv, &logsGRPC{s: s})
	go func() { _ = srv.Serve(lis) }()
	defer srv.Stop()

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ld := plog.NewLogs()
	ld.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords().AppendEmpty().Body().SetStr("x")
	_, err = plogotlp.NewGRPCClient(conn).Export(ctx, plogotlp.NewExportRequestFromLogs(ld))
	if err == nil {
		t.Fatal("a push was accepted with an exhausted buffer budget")
	}
	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.ResourceExhausted {
		t.Fatalf("status = %v, want ResourceExhausted", err)
	}
	found := false
	for _, d := range st.Details() {
		if _, ok := d.(*errdetails.RetryInfo); ok {
			found = true
		}
	}
	if !found {
		t.Fatal("no RetryInfo detail: ResourceExhausted alone reads as permanent, and conformant senders drop the batch")
	}
}

// The gRPC reservation is taken before the message is read and handed over to
// the count bound once it is decoded. If it could leak, the listener would shed
// everything for the process' life — so it must be back at zero after ordinary
// RPCs, after an application error, and after a stream that is opened and
// abandoned without ever reaching the interceptor.
func TestGRPCReservationIsAlwaysReleased(t *testing.T) {
	var heldDuringForward atomic.Int64
	var s *Server
	s = NewServer(ServerConfig{
		Enricher: newEnricher(newMeta(), MetricsAuto),
		Exporter: exporterFunc(func(plog.Logs) error {
			// By the time the payload is being forwarded the message is
			// decoded and the count bound has taken over, so the worst-case
			// reservation must already be back: holding it across a slow
			// collector's ack would bound gRPC pushes at budget/MaxRecvMsgSize
			// instead of at -ingest-max-in-flight.
			heldDuringForward.Store(s.buffer.used.Load())
			return context.DeadlineExceeded
		}),
	})
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer(
		grpc.InTapHandle(s.tapAdmit),
		grpc.UnaryInterceptor(s.limitUnary),
	)
	plogotlp.RegisterGRPCServer(srv, &logsGRPC{s: s})
	go func() { _ = srv.Serve(lis) }()
	defer srv.Stop()

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	client := plogotlp.NewGRPCClient(conn)
	ld := plog.NewLogs()
	ld.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords().AppendEmpty().Body().SetStr("x")

	for range 20 {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_, _ = client.Export(ctx, plogotlp.NewExportRequestFromLogs(ld)) // the exporter fails: an error path
		cancel()
	}
	if got := heldDuringForward.Load(); got != 0 {
		t.Errorf("%d budget bytes still reserved while forwarding: the decode-window reservation outlived the decode", got)
	}
	// A stream cancelled before the interceptor ever runs: only the context
	// backstop can return this reservation.
	for range 20 {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_, _ = client.Export(ctx, plogotlp.NewExportRequestFromLogs(ld))
	}

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if s.buffer.used.Load() == 0 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("buffer budget still holds %d bytes after every RPC finished", s.buffer.used.Load())
}
