package otlpingest

import (
	"context"
	"errors"
	"fmt"

	"log/slog"
	"net"
	"net/http"
	"time"

	"github.com/JohanLindvall/kubescrape/internal/obs"

	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/plog/plogotlp"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/pmetric/pmetricotlp"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/JohanLindvall/kubescrape/internal/agent/otlpexport"
)

// Exporter forwards enriched telemetry; implemented by otlpexport.Client.
type Exporter interface {
	ExportLogs(ctx context.Context, ld plog.Logs) error
	ExportMetrics(ctx context.Context, md pmetric.Metrics) error
}

// TracesExporter forwards traces; implemented by otlpexport.Client (and
// Buffered, which passes a forwarded trace through unbuffered — the pushing
// sender owns the retry; only a tail-sampling decision is spooled, and nothing
// on this path marks one).
type TracesExporter interface {
	ExportTraces(ctx context.Context, td ptrace.Traces) error
}

// ServerConfig configures the ingest listeners. An empty address disables
// that transport; disabling both makes Run a no-op.
type ServerConfig struct {
	GRPCAddr string // default ":4317" when enabled
	HTTPAddr string // default ":4318" when enabled
	Enricher *Enricher
	// Exporter accepts pushed logs and metrics on /v1/logs, /v1/metrics and the
	// matching gRPC services. nil disables BOTH signals — the services are not
	// registered and the routes are not served, so a sender gets Unimplemented /
	// 404 rather than a nil-pointer panic.
	//
	// A receiver for one signal is a real deployment, not a degenerate one: the
	// service-graph tier takes traces and nothing else (logs and metrics belong
	// on the node-local DaemonSet, where the peer address is a pod on the same
	// node and the payload never crosses a network to be enriched).
	Exporter Exporter
	// Traces accepts pushed traces on /v1/traces and the gRPC trace service,
	// enriching resources and passing them through. nil disables traces.
	Traces TracesExporter
	// MaxInFlight bounds concurrently-processed pushes across both transports
	// (0 = defaultMaxInFlight). Over the bound, senders are refused with a
	// RETRYABLE answer rather than accepted into memory the node does not have.
	MaxInFlight int
	// Ready is called once every configured listener is BOUND (not once
	// something has been received). It is what a readiness gate hangs on: a
	// rollout that advanced on a probe answering before the port existed would
	// march a broken listener across the fleet.
	Ready  func()
	Logger *slog.Logger
}

// defaultMaxInFlight bounds concurrently-processed pushes across BOTH
// transports. The listeners are unauthenticated and node-local, and every
// in-flight request holds its body plus the inflated pdata, so an unbounded
// count is an OOM the agent cannot defend against — on the process that also
// tails the node's logs. Tune with -ingest-max-in-flight: the right value
// depends on how long the collector takes to ack (a slow one holds every slot
// for -otlp-timeout) and on how many pods push to this node.
const defaultMaxInFlight = 32

// Server receives pushed OTLP over gRPC and/or HTTP, enriches it, and
// forwards it through the exporter.
type Server struct {
	cfg ServerConfig
	log *slog.Logger
	// inFlight admits a bounded number of concurrently-processed pushes across
	// both transports; maxInFlight is its capacity (kept for
	// grpc.MaxConcurrentStreams).
	inFlight    chan struct{}
	maxInFlight int
	// buffer bounds the RAW payload bytes both transports may hold while
	// reading and decoding, which the count above deliberately does not (see
	// admit.go). Tests lower its limit to exercise the refusal.
	buffer *byteBudget
	// reserveWindow bounds how long ONE gRPC reservation may live, i.e. how long
	// a peer may sit between its HEADERS frame and a decoded message
	// (grpcReserveWindow). Tests shorten it to exercise the reclaim.
	reserveWindow time.Duration
	// body reads one HTTP request body against that budget and this receiver's
	// cap. The same reader serves the trace tier's internal listener with a
	// different cap and no budget (httpbody.go).
	body *BodyReader
}

// NewServer creates an ingest Server.
func NewServer(cfg ServerConfig) *Server {
	log := cfg.Logger
	if log == nil {
		log = slog.Default()
	}
	n := cfg.MaxInFlight
	if n <= 0 {
		n = defaultMaxInFlight
	}
	s := &Server{
		cfg: cfg, log: log,
		inFlight:      make(chan struct{}, n),
		maxInFlight:   n,
		buffer:        &byteBudget{limit: maxBufferBytes},
		reserveWindow: grpcReserveWindow,
	}
	s.body = &BodyReader{max: maxIngestBody, budget: s.buffer}
	return s
}

// acquire takes an in-flight slot without waiting. A sender that is refused
// gets a RETRYABLE answer (429 / ResourceExhausted): the payload is intact and
// the sender owns the retry — far better than accepting it and running the
// node out of memory, or queueing it and turning back-pressure into latency
// the sender cannot see.
func (s *Server) acquire() bool {
	select {
	case s.inFlight <- struct{}{}:
		return true
	default:
		obs.IngestRejected.Inc()
		return false
	}
}

func (s *Server) release() { <-s.inFlight }

// limitUnary applies the same bound to gRPC pushes.
func (s *Server) limitUnary(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	// The message is decoded by the time we get here, so the buffer
	// reservation tapAdmit took for the read has done its job: release it and
	// let the count bound account for the inflated pdata from here on.
	releaseReservation(ctx)
	if !s.acquire() {
		return nil, exhaustedStatus("too many concurrent pushes; retry")
	}
	defer s.release()
	return handler(ctx, req)
}

// exhaustedStatus builds the gRPC refusal. ResourceExhausted ALONE reads as
// PERMANENT to conformant senders — the OTLP spec makes it retryable only with
// RetryInfo attached, and both the OTel SDK and the Collector drop the batch
// without it. A shed that loses the data is worse than the OOM it prevents, so
// the hint rides along, mirroring the HTTP arm's Retry-After: 1.
func exhaustedStatus(msg string) error {
	st, err := status.New(codes.ResourceExhausted, msg).
		WithDetails(&errdetails.RetryInfo{RetryDelay: durationpb.New(time.Second)})
	if err != nil {
		return status.Error(codes.ResourceExhausted, msg)
	}
	return st.Err()
}

// Run serves until ctx is cancelled, then shuts both listeners down.
func (s *Server) Run(ctx context.Context) error {
	var grpcSrv *grpc.Server
	var httpSrv *http.Server
	errc := make(chan error, 2)
	started := 0

	if s.cfg.GRPCAddr != "" {
		lis, err := net.Listen("tcp", s.cfg.GRPCAddr)
		if err != nil {
			return fmt.Errorf("ingest gRPC listen %s: %w", s.cfg.GRPCAddr, err)
		}
		// Mirror the HTTP server's IdleTimeout: reap connections apps opened
		// and abandoned (default gRPC keeps them forever). MaxConnectionIdle
		// alone does NOT cover the abandoned-STREAM case — a connection carrying
		// an open stream is not idle, which is why the reservation carries its
		// own window (grpcReserveWindow). The age bounds are the coarse
		// complement: whatever a peer accumulates on one socket (stream ids,
		// HPACK state, half-open RPCs), it gets a GOAWAY after the age and loses
		// the socket after the grace. Conformant senders reconnect
		// transparently, and the grace is far longer than any RPC here — those
		// are bounded by the pre-decode window plus the exporter's own timeout.
		grpcSrv = grpc.NewServer(
			grpc.KeepaliveParams(keepalive.ServerParameters{
				MaxConnectionIdle:     120 * time.Second,
				MaxConnectionAge:      30 * time.Minute,
				MaxConnectionAgeGrace: 30 * time.Second,
			}),
			// What each of these actually bounds, since they are easy to
			// over-credit:
			//
			//   - MaxRecvMsgSize caps ONE decoded message (the gRPC default,
			//     stated explicitly because it is the only hard cap on how
			//     much a single push can allocate).
			//   - MaxConcurrentStreams caps concurrent RPCs PER CONNECTION.
			//     Connections themselves are not capped, so this is not a
			//     server-wide bound: N connections may decode N x this many
			//     messages at once.
			//   - The semaphore (limitUnary) caps concurrent PROCESSING —
			//     enrichment, the inflated pdata and the forward — across both
			//     transports. It runs in the unary interceptor, i.e. AFTER
			//     grpc-go has decoded the message, so it does not bound the
			//     decode itself.
			//   - The tap (tapAdmit, admit.go) is what bounds the decode: it
			//     runs on the HEADERS frame, before grpc-go reads the message,
			//     and reserves MaxRecvMsgSize from the server-wide byte budget
			//     for at most grpcReserveWindow — a peer that sends no message
			//     loses the reservation and the stream with it.
			//     That closes the gap the previous three left — unbounded
			//     concurrent BUFFERING — and it can carry the RetryInfo a shed
			//     needs, because writeEarlyAbort forwards a status' details.
			grpc.MaxRecvMsgSize(maxIngestGRPCMessage),
			grpc.MaxConcurrentStreams(uint32(s.maxInFlight)),
			grpc.InTapHandle(s.tapAdmit),
			grpc.UnaryInterceptor(s.limitUnary),
		)
		if s.cfg.Exporter != nil {
			plogotlp.RegisterGRPCServer(grpcSrv, &logsGRPC{s: s})
			pmetricotlp.RegisterGRPCServer(grpcSrv, &metricsGRPC{s: s})
		}
		if s.cfg.Traces != nil {
			ptraceotlp.RegisterGRPCServer(grpcSrv, &tracesGRPC{s: s})
		}
		started++
		go func() { errc <- grpcSrv.Serve(lis) }()
		s.log.Info("otlp ingest gRPC listening", "addr", s.cfg.GRPCAddr)
	}

	if s.cfg.HTTPAddr != "" {
		mux := http.NewServeMux()
		if s.cfg.Exporter != nil {
			mux.HandleFunc("POST /v1/logs", s.handleHTTPLogs)
			mux.HandleFunc("POST /v1/metrics", s.handleHTTPMetrics)
		}
		if s.cfg.Traces != nil {
			mux.HandleFunc("POST /v1/traces", s.handleHTTPTraces)
		}
		// ReadHeaderTimeout kills Slowloris header trickling; ReadTimeout
		// bounds a trickled request body (the handlers read up to 16 MiB and
		// senders are node-local, so 60s is generous — it also caps handler
		// runtime via the whole-request read deadline, fine because forwarding
		// is bounded by the exporter's own much shorter timeout and a cut-off
		// surfaces as a retryable 503); IdleTimeout reaps parked keep-alives.
		// WriteTimeout is deliberately omitted: responses are tiny and its
		// clock would race a slow-but-legal body upload plus the forward.
		httpSrv = &http.Server{
			Addr:              s.cfg.HTTPAddr,
			Handler:           mux,
			ReadHeaderTimeout: 10 * time.Second,
			ReadTimeout:       60 * time.Second,
			IdleTimeout:       120 * time.Second,
		}
		// Listened explicitly rather than through ListenAndServe so the BIND
		// failure is known before Ready fires below: a readiness probe that goes
		// green on a port nobody bound is worse than no probe.
		lis, err := net.Listen("tcp", s.cfg.HTTPAddr)
		if err != nil {
			if grpcSrv != nil {
				grpcSrv.Stop()
			}
			return fmt.Errorf("ingest HTTP listen %s: %w", s.cfg.HTTPAddr, err)
		}
		started++
		go func() {
			if err := httpSrv.Serve(lis); err != nil && !errors.Is(err, http.ErrServerClosed) {
				errc <- err
				return
			}
			errc <- nil
		}()
		s.log.Info("otlp ingest HTTP listening", "addr", s.cfg.HTTPAddr)
	}

	if started == 0 {
		return nil
	}
	if s.cfg.Ready != nil {
		s.cfg.Ready()
	}

	// A runtime listener failure must propagate to the caller (main treats it
	// as fatal and exits non-zero); a ctx-cancelled shutdown returns nil.
	var runErr error
	select {
	case <-ctx.Done():
	case err := <-errc:
		if err != nil {
			s.log.Error("otlp ingest listener failed", "error", err)
			runErr = fmt.Errorf("ingest listener: %w", err)
		}
	}
	if grpcSrv != nil {
		grpcSrv.GracefulStop()
	}
	if httpSrv != nil {
		sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = httpSrv.Shutdown(sctx)
	}
	return runErr
}

// --- gRPC ---

type logsGRPC struct {
	plogotlp.UnimplementedGRPCServer
	s *Server
}

func (g *logsGRPC) Export(ctx context.Context, req plogotlp.ExportRequest) (plogotlp.ExportResponse, error) {
	ctx = grpcPeerCtx(ctx)
	ld := req.Logs()
	g.s.cfg.Enricher.EnrichLogs(ctx, ld)
	if err := g.s.cfg.Exporter.ExportLogs(ctx, ld); err != nil {
		return plogotlp.ExportResponse{}, grpcForwardStatus(err)
	}
	return plogotlp.NewExportResponse(), nil
}

type metricsGRPC struct {
	pmetricotlp.UnimplementedGRPCServer
	s *Server
}

func (g *metricsGRPC) Export(ctx context.Context, req pmetricotlp.ExportRequest) (pmetricotlp.ExportResponse, error) {
	ctx = grpcPeerCtx(ctx)
	md := g.s.cfg.Enricher.EnrichMetrics(ctx, req.Metrics())
	if err := g.s.cfg.Exporter.ExportMetrics(ctx, md); err != nil {
		return pmetricotlp.ExportResponse{}, grpcForwardStatus(err)
	}
	return pmetricotlp.NewExportResponse(), nil
}

// grpcForwardStatus maps a forwarding failure onto a gRPC status the sender's
// SDK retries correctly. A bare error would surface as codes.Unknown —
// NON-retryable per the OTLP spec — making senders permanently drop batches on
// transient conditions (a full disk buffer, an upstream 5xx). A status error
// from a gRPC upstream passes through unchanged.
func grpcForwardStatus(err error) error {
	if _, ok := status.FromError(err); ok {
		return err
	}
	// Permanence is classified by otlpexport.IsPermanent (the single source of
	// truth): only definitive upstream rejections become InvalidArgument (do
	// not retry). Everything else — diskqueue.ErrFull back-pressure, upstream 5xx,
	// 401/403/404 windows, timeouts, unclassified failures — is Unavailable:
	// the receiver is a proxy, and the sender retrying is the safe default.
	if otlpexport.IsPermanent(err) {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	return status.Error(codes.Unavailable, err.Error())
}

type tracesGRPC struct {
	ptraceotlp.UnimplementedGRPCServer
	s *Server
}

func (g *tracesGRPC) Export(ctx context.Context, req ptraceotlp.ExportRequest) (ptraceotlp.ExportResponse, error) {
	ctx = grpcPeerCtx(ctx)
	td := req.Traces()
	g.s.cfg.Enricher.EnrichTraces(ctx, td)
	if err := g.s.cfg.Traces.ExportTraces(ctx, td); err != nil {
		return ptraceotlp.ExportResponse{}, grpcForwardStatus(err)
	}
	return ptraceotlp.NewExportResponse(), nil
}

// --- HTTP (OTLP/HTTP protobuf) ---

func (s *Server) handleHTTPLogs(w http.ResponseWriter, r *http.Request) {
	body, charged, err := s.body.Read(r)
	if err != nil {
		WriteBodyError(w, err)
		return
	}
	// The body stays alive for the whole handler, so its budget charge does
	// too: releasing it at the end of the read would leave the bytes resident
	// and unaccounted through enrichment and the forward.
	defer s.body.Release(charged)
	// Acquired AFTER the read: holding a slot across the upload let 32
	// trickled 16 MiB bodies shed every other sender on the node for a
	// ReadTimeout (60s) — no credentials required, on an unauthenticated
	// listener, which is the threat the bound exists for. What bounds the read
	// itself is the byte budget the body was charged against (admit.go), not
	// this slot. The gRPC arm is naturally on this side of the decode.
	if !s.acquire() {
		// Retryable by design: the sender still holds the payload.
		w.Header().Set("Retry-After", "1")
		http.Error(w, "too many concurrent pushes", http.StatusTooManyRequests)
		return
	}
	defer s.release()
	req := plogotlp.NewExportRequest()
	if err := req.UnmarshalProto(body); err != nil {
		http.Error(w, "malformed OTLP logs payload", http.StatusBadRequest)
		return
	}
	ctx := withPeerIP(r.Context(), r.RemoteAddr)
	ld := req.Logs()
	s.cfg.Enricher.EnrichLogs(ctx, ld)
	if err := s.cfg.Exporter.ExportLogs(ctx, ld); err != nil {
		http.Error(w, err.Error(), HTTPForwardStatus(err))
		return
	}
	WriteProto(w, plogotlp.NewExportResponse())
}

func (s *Server) handleHTTPMetrics(w http.ResponseWriter, r *http.Request) {
	body, charged, err := s.body.Read(r)
	if err != nil {
		WriteBodyError(w, err)
		return
	}
	defer s.body.Release(charged)
	// Acquired AFTER the read (see handleHTTPLogs).
	if !s.acquire() {
		// Retryable by design: the sender still holds the payload.
		w.Header().Set("Retry-After", "1")
		http.Error(w, "too many concurrent pushes", http.StatusTooManyRequests)
		return
	}
	defer s.release()
	req := pmetricotlp.NewExportRequest()
	if err := req.UnmarshalProto(body); err != nil {
		http.Error(w, "malformed OTLP metrics payload", http.StatusBadRequest)
		return
	}
	ctx := withPeerIP(r.Context(), r.RemoteAddr)
	md := s.cfg.Enricher.EnrichMetrics(ctx, req.Metrics())
	if err := s.cfg.Exporter.ExportMetrics(ctx, md); err != nil {
		http.Error(w, err.Error(), HTTPForwardStatus(err))
		return
	}
	WriteProto(w, pmetricotlp.NewExportResponse())
}

func (s *Server) handleHTTPTraces(w http.ResponseWriter, r *http.Request) {
	body, charged, err := s.body.Read(r)
	if err != nil {
		WriteBodyError(w, err)
		return
	}
	defer s.body.Release(charged)
	// Acquired AFTER the read (see handleHTTPLogs).
	if !s.acquire() {
		// Retryable by design: the sender still holds the payload.
		w.Header().Set("Retry-After", "1")
		http.Error(w, "too many concurrent pushes", http.StatusTooManyRequests)
		return
	}
	defer s.release()
	req := ptraceotlp.NewExportRequest()
	if err := req.UnmarshalProto(body); err != nil {
		http.Error(w, "malformed OTLP traces payload", http.StatusBadRequest)
		return
	}
	ctx := withPeerIP(r.Context(), r.RemoteAddr)
	td := req.Traces()
	s.cfg.Enricher.EnrichTraces(ctx, td)
	if err := s.cfg.Traces.ExportTraces(ctx, td); err != nil {
		http.Error(w, err.Error(), HTTPForwardStatus(err))
		return
	}
	WriteProto(w, ptraceotlp.NewExportResponse())
}

const maxIngestBody = 16 << 20 // 16 MiB per request

// maxIngestGRPCMessage caps ONE decoded gRPC message (grpc-go's own default,
// stated here because the tap reserves exactly this much per push).
const maxIngestGRPCMessage = 4 << 20
