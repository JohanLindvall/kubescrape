package otlpingest

// The decoded-pdata budget (admit.go): the bound on what admitted BYTES inflate
// into, which neither the byte budget (raw bytes) nor the in-flight count (a
// count) can express.

import (
	"bytes"
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/plog/plogotlp"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/pmetric/pmetricotlp"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/JohanLindvall/kubescrape/internal/obs"
)

// manyResources is the amplification shape: minimal ResourceLogs, one one-byte
// record each — ~30 wire bytes per resource, entirely legal OTLP, and ~490 B of
// live heap each once decoded.
func manyResources(n int) plog.Logs {
	ld := plog.NewLogs()
	for i := 0; i < n; i++ {
		ld.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().
			LogRecords().AppendEmpty().Body().SetStr("x")
	}
	return ld
}

// The same amplification shape for the other two signals: the budget is charged
// per signal, so each needs its own end-to-end proof.
func manyResourceMetrics(n int) pmetric.Metrics {
	md := pmetric.NewMetrics()
	for i := 0; i < n; i++ {
		md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics().AppendEmpty().
			SetEmptyGauge().DataPoints().AppendEmpty().SetIntValue(1)
	}
	return md
}

func manyResourceSpans(n int) ptrace.Traces {
	td := ptrace.NewTraces()
	for i := 0; i < n; i++ {
		td.ResourceSpans().AppendEmpty().ScopeSpans().AppendEmpty().Spans().AppendEmpty().SetName("x")
	}
	return td
}

// budgetHTTPServer returns the Server behind an httptest listener so a test can
// lower its budgets, which is how every other admission test here works.
func budgetHTTPServer(t *testing.T, exp Exporter) (*Server, *httptest.Server) {
	t.Helper()
	s := NewServer(ServerConfig{Enricher: newEnricher(newMeta(), MetricsAuto), Exporter: exp})
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/logs", s.handleHTTPLogs)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return s, srv
}

func postLogs(t *testing.T, url string, ld plog.Logs) *http.Response {
	t.Helper()
	raw, err := plogotlp.NewExportRequestFromLogs(ld).MarshalProto()
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.Post(url+"/v1/logs", "application/x-protobuf", bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
	return resp
}

// A payload the RAW budget waves through — a few hundred KiB of wire — but
// whose decoded structure is what the receiver has to hold. Before this bound
// existed, four such bodies at the byte budget's own limit were ~1 GiB of live
// heap on a pod limited to 512Mi.
func TestDecodedStructureIsChargedNotOnlyRawBytes(t *testing.T) {
	exp := &captureExporter{}
	s, srv := budgetHTTPServer(t, exp)
	// A budget small enough to bind on a test-sized payload; the production
	// arithmetic is pinned by TestDecodedBudgetScalesWithTheRawBudget.
	s.decoded.limit = 1 << 20

	ld := manyResources(4000) // ~4 MB of estimated structure, ~120 KB of wire
	if got := decodedLogsSize(ld); got <= s.decoded.limit {
		t.Fatalf("fixture estimates %d bytes, want more than the %d budget", got, s.decoded.limit)
	}
	before := obs.IngestRejected.Value()

	resp := postLogs(t, srv.URL, ld)
	if resp.StatusCode != http.StatusTooManyRequests {
		t.Errorf("status = %d, want 429: the refusal must be RETRYABLE — the sender still holds the payload",
			resp.StatusCode)
	}
	if resp.Header.Get("Retry-After") != "1" {
		t.Errorf("Retry-After = %q, want 1", resp.Header.Get("Retry-After"))
	}
	if len(exp.logs) != 0 {
		t.Errorf("a refused push was forwarded (%d exports)", len(exp.logs))
	}
	if got := obs.IngestRejected.Value() - before; got != 1 {
		t.Errorf("kubescrape_ingest_rejected_total moved %v, want 1", got)
	}
	// And the charge does not leak: the next push, which fits, is admitted.
	if used := s.decoded.used.Load(); used != 0 {
		t.Fatalf("decoded budget still holds %d bytes after the handler returned", used)
	}
	if resp := postLogs(t, srv.URL, manyResources(4)); resp.StatusCode != http.StatusOK {
		t.Fatalf("a small push after a refused one: status %d", resp.StatusCode)
	}
	if used := s.decoded.used.Load(); used != 0 {
		t.Fatalf("decoded budget still holds %d bytes after a successful push", used)
	}
}

// The charge is HELD for the whole handler, not just across the unmarshal: the
// decoded payload lives until the forward is acked, which on a slow collector
// is the entire -otlp-timeout.
func TestDecodedChargeIsHeldAcrossTheForward(t *testing.T) {
	seen := make(chan int64, 1)
	release := make(chan struct{})
	var s *Server
	exp := exporterFunc(func(plog.Logs) error {
		seen <- s.decoded.used.Load()
		<-release
		return nil
	})
	var srv *httptest.Server
	s, srv = budgetHTTPServer(t, exp)

	done := make(chan int)
	go func() { done <- postLogs(t, srv.URL, manyResources(10)).StatusCode }()

	held := <-seen
	if want := decodedLogsSize(manyResources(10)); held != want {
		t.Errorf("decoded budget holds %d bytes while the exporter runs, want %d", held, want)
	}
	close(release)
	if code := <-done; code != http.StatusOK {
		t.Fatalf("status = %d", code)
	}
}

// grpcBudgetServer wires a Server exactly as Run wires it — tap, unary
// interceptor, nesting guard — onto a real listener serving all three signals.
//
// Driving a REAL server is the whole point of this helper, and the reason the
// tests below were rewritten. Their predecessors called s.limitUnary directly
// with a hand-built plogotlp.ExportRequest and passed against a budget that
// charged NOTHING in production: grpc-go hands an interceptor the message its
// generated handler decoded into, which for pdata is the unexported
// *pdata/internal.ExportLogsServiceRequest, so the type switch that used to
// live there matched only in the tests that fabricated its input.
func grpcBudgetServer(t *testing.T, exp Exporter, traces TracesExporter) (*Server, *grpc.ClientConn) {
	t.Helper()
	s := NewServer(ServerConfig{
		Enricher: newEnricher(newMeta(), MetricsAuto),
		Exporter: exp,
		Traces:   traces,
	})
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer(
		grpc.InTapHandle(s.tapAdmit),
		grpc.UnaryInterceptor(s.limitUnary),
		NestingGuardOption(s.noteTooDeep),
	)
	plogotlp.RegisterGRPCServer(srv, &logsGRPC{s: s})
	pmetricotlp.RegisterGRPCServer(srv, &metricsGRPC{s: s})
	ptraceotlp.RegisterGRPCServer(srv, &tracesGRPC{s: s})
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return s, conn
}

// budgetProbe is an exporter for all three signals that can PARK inside the
// forward, so a test can read the decoded budget while a push is in flight —
// the observation that distinguishes "charged" from "charged nothing", which no
// before-and-after assertion can make.
type budgetProbe struct {
	s        *Server
	forwards atomic.Int64
	entered  chan int64    // decoded.used as seen from inside the forward
	release  chan struct{} // closed to let the forward return
}

func (p *budgetProbe) observe() error {
	p.forwards.Add(1)
	if p.entered != nil {
		p.entered <- p.s.decoded.used.Load()
		<-p.release
	}
	return nil
}

func (p *budgetProbe) ExportLogs(context.Context, plog.Logs) error          { return p.observe() }
func (p *budgetProbe) ExportMetrics(context.Context, pmetric.Metrics) error { return p.observe() }
func (p *budgetProbe) ExportTraces(context.Context, ptrace.Traces) error    { return p.observe() }

// grpcSignal is one signal's end of the gRPC arm: how to push a payload of n
// amplification resources, and what that payload is estimated to cost.
type grpcSignal struct {
	name string
	push func(ctx context.Context, conn *grpc.ClientConn, n int) error
	size func(n int) int64
}

func grpcSignals() []grpcSignal {
	return []grpcSignal{
		{"logs", func(ctx context.Context, conn *grpc.ClientConn, n int) error {
			_, err := plogotlp.NewGRPCClient(conn).Export(ctx, plogotlp.NewExportRequestFromLogs(manyResources(n)))
			return err
		}, func(n int) int64 { return decodedLogsSize(manyResources(n)) }},
		{"metrics", func(ctx context.Context, conn *grpc.ClientConn, n int) error {
			_, err := pmetricotlp.NewGRPCClient(conn).Export(ctx, pmetricotlp.NewExportRequestFromMetrics(manyResourceMetrics(n)))
			return err
		}, func(n int) int64 { return decodedMetricsSize(manyResourceMetrics(n)) }},
		{"traces", func(ctx context.Context, conn *grpc.ClientConn, n int) error {
			_, err := ptraceotlp.NewGRPCClient(conn).Export(ctx, ptraceotlp.NewExportRequestFromTraces(manyResourceSpans(n)))
			return err
		}, func(n int) int64 { return decodedTracesSize(manyResourceSpans(n)) }},
	}
}

// The refusal, through a real client on a real listener, for every signal: over
// the decoded budget a push is shed with a RETRYABLE ResourceExhausted — bare
// ResourceExhausted reads as permanent and conformant senders DROP the batch —
// nothing is forwarded, the operator's counter moves, and the charge does not
// leak.
func TestGRPCDecodedBudgetRefusalIsRetryable(t *testing.T) {
	for _, sig := range grpcSignals() {
		t.Run(sig.name, func(t *testing.T) {
			p := &budgetProbe{}
			s, conn := grpcBudgetServer(t, p, p)
			p.s = s
			// Small enough to bind on a test-sized payload; the production
			// arithmetic is TestDecodedBudgetScalesWithTheRawBudget's.
			s.decoded.limit = 1 << 20

			const n = 4000 // ~150 KB of wire, megabytes of structure
			if got := sig.size(n); got <= s.decoded.limit {
				t.Fatalf("fixture estimates %d bytes, want more than the %d budget", got, s.decoded.limit)
			}
			before := obs.IngestRejected.Value()

			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			err := sig.push(ctx, conn, n)
			if err == nil {
				t.Fatal("a push past the decoded budget was admitted")
			}
			st, _ := status.FromError(err)
			if st.Code() != codes.ResourceExhausted {
				t.Fatalf("code = %v, want ResourceExhausted", st.Code())
			}
			if !retryableStatus(st) {
				t.Error("the refusal carries no RetryInfo, so a conformant sender drops the batch it still holds")
			}
			var hasRetry bool
			for _, d := range st.Details() {
				if _, ok := d.(*errdetails.RetryInfo); ok {
					hasRetry = true
				}
			}
			if !hasRetry {
				t.Error("no errdetails.RetryInfo on the refusal")
			}
			if got := p.forwards.Load(); got != 0 {
				t.Errorf("a refused push was forwarded (%d exports)", got)
			}
			if got := obs.IngestRejected.Value() - before; got != 1 {
				t.Errorf("kubescrape_ingest_rejected_total moved %v, want 1", got)
			}
			if used := s.decoded.used.Load(); used != 0 {
				t.Errorf("a refused push left %d bytes charged", used)
			}

			// Nothing else leaked either: the refusal happens inside the
			// handler, i.e. with the in-flight slot held and the pre-decode
			// reservation already handed over, so a leak of either would shed
			// this listener for the process' life rather than for one push.
			s.decoded.limit = 1 << 20
			if err := sig.push(ctx, conn, 1); err != nil {
				t.Fatalf("an in-budget push after a refused one: %v", err)
			}
			if used := s.buffer.used.Load(); used != 0 {
				t.Errorf("the raw budget still holds %d bytes: the refused push leaked its reservation", used)
			}
			if p.forwards.Load() != 1 {
				t.Errorf("forwards = %d, want exactly the one admitted push", p.forwards.Load())
			}
		})
	}
}

// THE assertion that would have caught the defect this file's gRPC tests missed
// for three review rounds: an ADMITTED push must actually MOVE the budget while
// it is in flight. Everything else here — the refusal, the release, the counter
// — is satisfied by a bound that charges zero, because zero always fits.
//
// The budget is read from inside the forward, which is where the payload is at
// its most expensive: decoded, enriched, and waiting on the collector's ack.
func TestGRPCAdmittedPushHoldsTheDecodedBudgetWhileInFlight(t *testing.T) {
	for _, sig := range grpcSignals() {
		t.Run(sig.name, func(t *testing.T) {
			p := &budgetProbe{entered: make(chan int64, 1), release: make(chan struct{})}
			s, conn := grpcBudgetServer(t, p, p)
			p.s = s

			const n = 10
			done := make(chan error, 1)
			go func() {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				done <- sig.push(ctx, conn, n)
			}()

			var held int64
			select {
			case held = <-p.entered:
			case <-time.After(10 * time.Second):
				t.Fatal("the push never reached the forward")
			}
			if want := sig.size(n); held != want {
				t.Errorf("decoded budget holds %d bytes while the forward runs, want %d "+
					"(0 means the gRPC arm charges nothing, and the decoded payload is bounded "+
					"by the in-flight COUNT alone)", held, want)
			}
			close(p.release)
			if err := <-done; err != nil {
				t.Fatalf("an in-budget push was refused: %v", err)
			}
			if used := s.decoded.used.Load(); used != 0 {
				t.Errorf("decoded budget still holds %d bytes after the push returned", used)
			}
		})
	}
}

// The ordering fix, which lives in limitUnary and has no end-to-end proxy: the
// pre-decode reservation and the in-flight slot are two accountings of one
// message, and the handover between them must never leave a gap. This one stays
// a direct call BECAUSE the interceptor is the unit under test — what a direct
// call may NOT stand in for is a bound whose production input is a type the test
// cannot construct, which is exactly what went wrong above.
//
// The message is ALREADY decoded when the interceptor runs, so the pre-decode
// reservation is the only thing accounting for it: handing that back before a
// slot is taken leaves a window where the payload is charged to nothing at all,
// and lets a refused push give up an accounting it should still be holding.
func TestGRPCSlotIsTakenBeforeTheReservationIsHandedOver(t *testing.T) {
	s := NewServer(ServerConfig{
		Enricher:    newEnricher(newMeta(), MetricsAuto),
		Exporter:    exporterFunc(func(plog.Logs) error { return nil }),
		MaxInFlight: 1,
	})
	s.inFlight <- struct{}{} // every slot taken: the next push is shed
	defer func() { <-s.inFlight }()

	const held = 4 << 20
	if !s.buffer.reserve(held) {
		t.Fatal("reserve")
	}
	r := &reservation{b: s.buffer, cancel: func() {}}
	r.held.Store(held)
	ctx := context.WithValue(context.Background(), reservationKey{}, r)

	handler := func(context.Context, any) (any, error) { return nil, nil }
	if _, err := s.limitUnary(ctx, oneLog(), &grpc.UnaryServerInfo{}, handler); err == nil {
		t.Fatal("the push was admitted with every slot taken")
	}
	if r.held.Load() != held {
		t.Error("the shed push released its pre-decode reservation: between that release and the slot " +
			"it was refused, its decoded message is accounted for by nothing")
	}
	r.release()

	// And on the ACCEPTED path the handover still happens, or the reservation
	// would pin the byte budget for as long as the collector takes to ack.
	if !s.buffer.reserve(held) {
		t.Fatal("reserve")
	}
	r2 := &reservation{b: s.buffer, cancel: func() {}}
	r2.held.Store(held)
	<-s.inFlight // free the slot
	ctx = context.WithValue(context.Background(), reservationKey{}, r2)
	inHandler := int64(-1)
	_, err := s.limitUnary(ctx, oneLog(), &grpc.UnaryServerInfo{},
		func(context.Context, any) (any, error) {
			inHandler = r2.held.Load()
			return nil, nil
		})
	s.inFlight <- struct{}{} // restore for the deferred drain
	if err != nil {
		t.Fatal(err)
	}
	if inHandler != 0 {
		t.Errorf("the reservation was still held inside the handler (%d bytes): it must be handed over "+
			"once the slot is taken, or a slow collector pins the byte budget", inHandler)
	}
}

// The budget is derived from the raw one, so a receiver told to accept bigger
// messages holds proportionally more of everything.
func TestDecodedBudgetScalesWithTheRawBudget(t *testing.T) {
	s := NewServer(ServerConfig{})
	if want := int64(decodedBudgetFactor) * s.buffer.limit; s.decoded.limit != want {
		t.Errorf("decoded budget = %d, want %d", s.decoded.limit, want)
	}
	big := NewServer(ServerConfig{MaxRecvBytes: 64 << 20})
	if want := int64(decodedBudgetFactor) * big.buffer.limit; big.decoded.limit != want {
		t.Errorf("raised MaxRecvBytes: decoded budget = %d, want %d", big.decoded.limit, want)
	}
}

// An empty scope costs heap and carries no item, so counting items alone would
// charge a payload of half a million of them exactly nothing.
func TestEmptyScopesAreCharged(t *testing.T) {
	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	for i := 0; i < 1000; i++ {
		rl.ScopeLogs().AppendEmpty()
	}
	if ld.LogRecordCount() != 0 {
		t.Fatal("fixture must carry no records")
	}
	if got, want := decodedLogsSize(ld), int64(decodedResourceBytes+1000*decodedScopeBytes); got != want {
		t.Errorf("decodedLogsSize = %d, want %d", got, want)
	}
}
