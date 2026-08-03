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
	"testing/iotest"
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

// Bounding the pre-size splits the destination's growth into two regimes — a
// small head, then a jump to the declared length — so the bytes have to be
// checked, not just counted: a wrong copy in the grow branch would truncate or
// corrupt a payload rather than fail it.
func TestReadAllCappedIsFaithfulAcrossPresizeBoundary(t *testing.T) {
	const bodyMax = 1 << 20
	sizes := []int{0, 1, 511, 512, maxPresizeBytes - 1, maxPresizeBytes, maxPresizeBytes + 1, 3 * maxPresizeBytes, bodyMax}
	for _, size := range sizes {
		want := make([]byte, size)
		for i := range want {
			want[i] = byte(i * 31)
		}
		// Every hint a sender can offer: none, exact, a lie in each direction,
		// and one over the receiver's own cap.
		for _, hint := range []int64{0, int64(size), int64(size) / 2, int64(size) * 4, bodyMax * 8} {
			// Small reads, so the growth branch is crossed mid-payload rather
			// than only at a buffer boundary.
			got, err := readAllCapped(iotest.OneByteReader(bytes.NewReader(want)), hint, bodyMax)
			if err != nil {
				t.Fatalf("size %d hint %d: %v", size, hint, err)
			}
			if !bytes.Equal(got, want) {
				t.Fatalf("size %d hint %d: read %d bytes, want %d (identical=%v)",
					size, hint, len(got), len(want), bytes.Equal(got, want))
			}
			// And the DESTINATION stays proportional to what ARRIVED. Jumping
			// to the declaration once the pre-sized head fills would let a peer
			// buy the whole allocation with maxPresizeBytes of real bytes — the
			// same trade that made crediting Content-Length a denial of service.
			if limit := 4 * max(int64(size), maxPresizeBytes); int64(cap(got)) > limit {
				t.Fatalf("size %d hint %d: buffer capacity %d for %d bytes received (want <= %d)",
					size, hint, cap(got), size, limit)
			}
		}
	}
}

// The pre-size bound must not cost a large HONEST body its correctness: it is
// read through the same two-regime growth, and it must still land intact,
// charged, and released.
func TestLargeIdentityBodyRoundTrips(t *testing.T) {
	var exported atomic.Int64
	s := NewServer(ServerConfig{
		Enricher: newEnricher(newMeta(), MetricsAuto),
		Exporter: exporterFunc(func(ld plog.Logs) error {
			exported.Add(int64(ld.LogRecordCount()))
			return nil
		}),
	})
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/logs", s.handleHTTPLogs)
	srv := httptest.NewServer(mux)
	defer srv.Close()

	body := bigLogsPayload(t, 4<<20)
	if len(body) <= maxPresizeBytes {
		t.Fatalf("payload of %d bytes does not cross the pre-size boundary", len(body))
	}
	resp, err := http.Post(srv.URL+"/v1/logs", "application/x-protobuf", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d, want 200", resp.StatusCode)
	}
	if exported.Load() == 0 {
		t.Error("a 4 MiB body was ACKed without being exported")
	}
	if got := s.buffer.used.Load(); got != 0 {
		t.Errorf("%d budget bytes still charged after the handler returned", got)
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

// Content-Length is the sender's claim, and crediting it against the budget
// made the budget spendable by peers who send NOTHING: four sockets announcing
// 16 MiB each took the whole 64 MiB and 64 MiB of heap with it, three were
// enough to lock out honest full-size pushes, and none of them had to transmit
// a byte or hold a credential. That is a cheaper denial of service than the
// memory exhaustion the budget was added to prevent. Nothing may be charged, and
// nothing may be allocated, on the strength of a declaration alone.
func TestHTTPDeclaredLengthIsNotCredited(t *testing.T) {
	const (
		limit   = 4 * maxIngestBody
		sockets = 8
	)
	s, srv := budgetTestServer(t, limit)
	addr := srv.Listener.Addr().String()

	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)

	for range sockets {
		conn, err := net.Dial("tcp", addr)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = conn.Close() }()
		// A full-size declaration, and then silence. Twice the budget declared
		// between them.
		if _, err := fmt.Fprintf(conn, "POST /v1/logs HTTP/1.1\r\nHost: x\r\n"+
			"Content-Type: application/x-protobuf\r\nContent-Length: %d\r\n\r\n", maxIngestBody); err != nil {
			t.Fatal(err)
		}
	}
	// Long enough for every handler to be parked inside its read.
	time.Sleep(750 * time.Millisecond)

	runtime.GC()
	var after runtime.MemStats
	runtime.ReadMemStats(&after)
	grown := int64(after.HeapAlloc) - int64(before.HeapAlloc)
	charged := s.buffer.used.Load()
	t.Logf("%d sockets declaring %d MiB each and sending nothing: budget %d of %d, heap %+.1f MiB",
		sockets, maxIngestBody>>20, charged, limit, float64(grown)/(1<<20))

	if charged != 0 {
		t.Errorf("budget charged %d bytes for %d bytes actually received: an unverified declaration is not a payload", charged, 0)
	}
	// Pre-sizing from the declaration would be sockets*16 MiB here. A modest
	// per-socket head plus net/http's own buffers is what should remain.
	if max := int64(sockets * 4 * maxPresizeBytes); grown > max {
		t.Errorf("heap grew %d bytes, want <= %d: the destination is still pre-allocated from an unverified Content-Length", grown, max)
	}

	// And the receiver is still open for business, which is the whole point.
	body, err := oneLog().MarshalProto()
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.Post(srv.URL+"/v1/logs", "application/x-protobuf", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("honest push while the liars idle: status %d, want 200", resp.StatusCode)
	}
}

// Dropping the up-front credit must not drop the ACCOUNTING: a sender that
// actually transmits is charged for what it has transmitted, so the budget still
// bounds resident bytes and still refuses the sender that no longer fits.
func TestHTTPChargesBytesActuallyRead(t *testing.T) {
	const limit = 4 << 20
	s, srv := budgetTestServer(t, limit)
	addr := srv.Listener.Addr().String()

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	// Declare a full-size body and then trickle a real megabyte of it.
	if _, err := fmt.Fprintf(conn, "POST /v1/logs HTTP/1.1\r\nHost: x\r\n"+
		"Content-Type: application/x-protobuf\r\nContent-Length: %d\r\n\r\n", maxIngestBody); err != nil {
		t.Fatal(err)
	}
	const sent = 1 << 20
	if _, err := conn.Write(make([]byte, sent)); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if s.buffer.used.Load() >= sent {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("budget holds %d bytes after %d were received: charge-as-you-read stopped charging", s.buffer.used.Load(), sent)
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

// grpcTestServer wires the tap and the interceptor onto a real gRPC listener
// with a small buffer budget and a short reservation window.
func grpcTestServer(t *testing.T, limit int64, window time.Duration) (*Server, plogotlp.GRPCClient, *grpc.ClientConn) {
	t.Helper()
	s := NewServer(ServerConfig{
		Enricher: newEnricher(newMeta(), MetricsAuto),
		Exporter: exporterFunc(func(plog.Logs) error { return nil }),
	})
	s.buffer.limit = limit
	s.reserveWindow = window

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
	t.Cleanup(srv.Stop)

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return s, plogotlp.NewGRPCClient(conn), conn
}

// oneLog is the smallest valid push.
func oneLog() plogotlp.ExportRequest {
	ld := plog.NewLogs()
	ld.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords().AppendEmpty().Body().SetStr("x")
	return plogotlp.NewExportRequestFromLogs(ld)
}

// export runs one push with a bounded deadline.
func export(t *testing.T, c plogotlp.GRPCClient) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := c.Export(ctx, oneLog())
	return err
}

// The reservation is taken on the HEADERS frame, because that is the only hook
// grpc-go offers before it reads the message — which means a peer can take one
// by opening a stream and then doing NOTHING AT ALL. Neither release path fires
// for that peer: the interceptor needs a decoded message, and the stream context
// is not cancelled until the stream ends, which is the same peer's choice
// (MaxConnectionIdle does not fire either — a connection carrying an open stream
// is not idle). Four such streams here, sixteen in production, and the whole
// budget is pinned for the process' life with gRPC and HTTP ingest shed behind
// it. So the reservation must expire on a clock the SENDER does not hold.
func TestGRPCHeadersOnlyStreamsDoNotPinTheBudget(t *testing.T) {
	const window = 300 * time.Millisecond
	s, client, conn := grpcTestServer(t, 4*grpcReserveBytes, window)

	if err := export(t, client); err != nil {
		t.Fatalf("warm-up push: %v", err) // also establishes the connection
	}

	// Exactly enough headers-only streams to exhaust the budget. They are never
	// written to and never closed.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	streams := make([]grpc.ClientStream, 0, 4)
	for i := range 4 {
		st, err := conn.NewStream(ctx, &grpc.StreamDesc{ServerStreams: true, ClientStreams: true},
			"/opentelemetry.proto.collector.logs.v1.LogsService/Export")
		if err != nil {
			t.Fatalf("open stream %d: %v", i, err)
		}
		streams = append(streams, st)
	}

	// The pin is real: while the window is open, honest senders are shed.
	pinned := false
	for range 50 {
		if s.buffer.used.Load() == s.buffer.limit {
			pinned = true
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !pinned {
		t.Fatalf("the headers-only streams never took the budget (used=%d limit=%d): this test proves nothing",
			s.buffer.used.Load(), s.buffer.limit)
	}

	// And it is bounded: the window elapses, the bytes come back, and the
	// listener serves again — without the abandoned streams having moved.
	deadline := time.Now().Add(10 * window)
	for s.buffer.used.Load() != 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if got := s.buffer.used.Load(); got != 0 {
		t.Fatalf("%d budget bytes still pinned %v after the reservation window: streams that send nothing hold the receiver hostage",
			got, 10*window)
	}
	if err := export(t, client); err != nil {
		t.Fatalf("honest push after the reclaim: %v", err)
	}

	// The reaped streams are refused, not silently forgotten: a reservation
	// released while grpc-go still waits for the message would leave the decode
	// unaccounted for. Their RPCs must be over.
	for i, st := range streams {
		done := make(chan error, 1)
		go func() { done <- st.RecvMsg(new(plogotlp.ExportResponse)) }()
		select {
		case err := <-done:
			if err == nil {
				t.Errorf("stream %d: a push that never sent a message succeeded", i)
			}
		case <-time.After(5 * time.Second):
			t.Errorf("stream %d is still open after its reservation was reclaimed: the decode window outlives the budget that accounts for it", i)
		}
	}
}

// The reclaim races the handover, so it must be exclusive with it: a stream
// whose message lands just as the window elapses may be refused (retryably) or
// served, but the budget must not go negative, double-release, or drift. Run
// with -race, this is also where a torn timer/cancel would surface.
func TestGRPCReserveWindowRacesTheHandoverSafely(t *testing.T) {
	var served, refused atomic.Int64
	// A sweep from "always reclaims" to "never reclaims", straddling the
	// round-trip time in between, so both orderings certainly occur.
	for _, window := range []time.Duration{time.Nanosecond, 50 * time.Microsecond, 500 * time.Microsecond, 5 * time.Millisecond, 100 * time.Millisecond} {
		s, client, _ := grpcTestServer(t, 64*grpcReserveBytes, window)
		var wg sync.WaitGroup
		for range 8 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for range 25 {
					if err := export(t, client); err != nil {
						refused.Add(1) // a reclaimed reservation: legitimate here
					} else {
						served.Add(1)
					}
				}
			}()
		}
		wg.Wait()

		settled := false
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			got := s.buffer.used.Load()
			if got < 0 {
				t.Fatalf("window %v: budget went negative (%d): a reservation was released twice", window, got)
			}
			if got == 0 {
				settled = true
				break
			}
			time.Sleep(20 * time.Millisecond)
		}
		if !settled {
			t.Fatalf("window %v: budget did not settle at zero: %d", window, s.buffer.used.Load())
		}
	}
	t.Logf("%d served, %d reclaimed mid-flight across the sweep", served.Load(), refused.Load())
	// Vacuous if only one side of the race ever happened.
	if served.Load() == 0 {
		t.Error("no push was ever served: the window is reclaiming everything, so nothing raced the handover")
	}
	if refused.Load() == 0 {
		t.Error("no push was ever reclaimed mid-flight: the window never fired, so nothing raced the handover")
	}
}
