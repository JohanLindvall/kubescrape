package otlpingest

import (
	"context"

	"log/slog"
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
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/JohanLindvall/kubescrape/internal/agent/otlpexport"
	"github.com/JohanLindvall/kubescrape/internal/agent/transform"
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
	// RejectTraces refuses a trace payload on the RECEIVE path — before
	// enrichment, on both transports. nil accepts everything, which is what a
	// node-local ingest server wants: it has nothing to refuse.
	//
	// A refusal has to be reached before anything is spent on the payload.
	// Enrichment costs one metadata lookup per resource and moves
	// kubescrape_ingest_resources_total per resource, so a verdict taken below
	// it charges the metadata service — and the operator's enriched/unresolved
	// signal — for traffic that is then thrown away. The trace tier's loop guard
	// is the case this exists for: a payload carrying the tier's own re-shard
	// marker on the APPLICATION port was never sent by an application, and
	// attributing it by the connection's peer address would name a sibling shard.
	//
	// The error travels back through the same mapping as an export failure
	// (grpcForwardStatus / HTTPForwardStatus), so a guard that must refuse
	// PERMANENTLY returns something otlpexport.IsPermanent classifies as such
	// (codes.InvalidArgument) and the sender sees InvalidArgument / 400.
	//
	// It runs AFTER admission (the byte budget and the in-flight slot), so a
	// refused push releases everything it took, exactly as an accepted one does.
	RejectTraces func(ctx context.Context, td ptrace.Traces) error
	// MaxInFlight bounds concurrently-processed pushes across both transports
	// (0 = defaultMaxInFlight). Over the bound, senders are refused with a
	// RETRYABLE answer rather than accepted into memory the node does not have.
	MaxInFlight int
	// MaxRecvBytes caps ONE decoded gRPC message (0 = maxIngestGRPCMessage,
	// grpc-go's own 4 MiB default). It exists for senders that batch large —
	// a collector or Alloy config raising max_recv_msg_size has no other
	// counterpart here — and it is a real memory grant on an unauthenticated
	// listener: the tap reserves exactly this much per push, and the byte
	// budget scales with it (NewServer) so a single legal push always fits.
	// The HTTP body cap is unaffected (maxIngestBody, already 16 MiB).
	MaxRecvBytes int
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
	// grpcMaxRecv is the resolved MaxRecvBytes: the per-message gRPC cap and
	// the tap's per-push reservation.
	grpcMaxRecv int
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
	recv := cfg.MaxRecvBytes
	if recv <= 0 {
		recv = maxIngestGRPCMessage
	}
	// The budget must scale with the reservation size: at the fixed
	// 4x-maxIngestBody it holds sixteen default-size gRPC reservations, but a
	// raised MaxRecvBytes past maxIngestBody would leave room for fewer than
	// four — and past 4x it would admit no push at all.
	budget := int64(maxBufferBytes)
	if b := 4 * int64(recv); b > budget {
		budget = b
	}
	s := &Server{
		cfg: cfg, log: log,
		inFlight:      make(chan struct{}, n),
		maxInFlight:   n,
		grpcMaxRecv:   recv,
		buffer:        &byteBudget{limit: budget},
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

// Run serves until ctx is cancelled, then shuts both listeners down. The
// run/shutdown skeleton is Listeners (listeners.go); this builds the two
// servers it runs.
func (s *Server) Run(ctx context.Context) error {
	l := Listeners{Name: "otlp ingest", Logger: s.log, Ready: s.cfg.Ready}

	if s.cfg.GRPCAddr != "" {
		l.GRPCAddr = s.cfg.GRPCAddr
		l.GRPC = grpc.NewServer(
			KeepaliveOption(),
			// What each of these actually bounds, since they are easy to
			// over-credit:
			//
			//   - MaxRecvMsgSize caps ONE decoded message (the gRPC default
			//     unless ServerConfig.MaxRecvBytes raises it — the only hard
			//     cap on how much a single push can allocate).
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
			grpc.MaxRecvMsgSize(s.grpcMaxRecv),
			grpc.MaxConcurrentStreams(uint32(s.maxInFlight)),
			grpc.InTapHandle(s.tapAdmit),
			grpc.UnaryInterceptor(s.limitUnary),
		)
		if s.cfg.Exporter != nil {
			plogotlp.RegisterGRPCServer(l.GRPC, &logsGRPC{s: s})
			pmetricotlp.RegisterGRPCServer(l.GRPC, &metricsGRPC{s: s})
		}
		if s.cfg.Traces != nil {
			ptraceotlp.RegisterGRPCServer(l.GRPC, &tracesGRPC{s: s})
		}
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
		l.HTTP = NewPushHTTPServer(s.cfg.HTTPAddr, mux)
	}

	return l.Run(ctx)
}

// --- gRPC ---

// grpcExport is the shared shape of the three gRPC Export wrappers: stamp the
// connection's peer address into ctx (the enricher's peer-IP fallback reads
// it), run the signal's enrich-and-forward step, and map a failure onto a
// status the sender's SDK retries correctly (grpcForwardStatus).
func grpcExport(ctx context.Context, forward func(ctx context.Context) error) error {
	if err := forward(grpcPeerCtx(ctx)); err != nil {
		return grpcForwardStatus(err)
	}
	return nil
}

type logsGRPC struct {
	plogotlp.UnimplementedGRPCServer
	s *Server
}

func (g *logsGRPC) Export(ctx context.Context, req plogotlp.ExportRequest) (plogotlp.ExportResponse, error) {
	err := grpcExport(ctx, func(ctx context.Context) error {
		ld := req.Logs()
		g.s.cfg.Enricher.EnrichLogs(ctx, ld)
		// Handoff (logs and metrics only, never traces — the tier's tap reads
		// a forwarded trace AFTER the export): on failure the decoded ld dies
		// with this RPC and the sender's retry re-decodes retransmitted bytes,
		// so the transform seam may run in place instead of deep-copying.
		return g.s.cfg.Exporter.ExportLogs(transform.Handoff(ctx), ld)
	})
	if err != nil {
		return plogotlp.ExportResponse{}, err
	}
	return plogotlp.NewExportResponse(), nil
}

type metricsGRPC struct {
	pmetricotlp.UnimplementedGRPCServer
	s *Server
}

func (g *metricsGRPC) Export(ctx context.Context, req pmetricotlp.ExportRequest) (pmetricotlp.ExportResponse, error) {
	err := grpcExport(ctx, func(ctx context.Context) error {
		md := g.s.cfg.Enricher.EnrichMetrics(ctx, req.Metrics())
		// Handoff: same reasoning as the logs arm — a failed forward drops the
		// decoded object and the sender retransmits bytes.
		return g.s.cfg.Exporter.ExportMetrics(transform.Handoff(ctx), md)
	})
	if err != nil {
		return pmetricotlp.ExportResponse{}, err
	}
	return pmetricotlp.NewExportResponse(), nil
}

// retryableStatus reports whether a sender's SDK will retry a batch that came
// back with this status, per the OTLP specification's list.
func retryableStatus(st *status.Status) bool {
	switch st.Code() {
	case codes.Canceled, codes.DeadlineExceeded, codes.Aborted,
		codes.OutOfRange, codes.Unavailable, codes.DataLoss:
		return true
	case codes.ResourceExhausted:
		// Retryable ONLY when it carries RetryInfo — the same rule this
		// receiver honours when IT sheds a push (see exhaustedStatus). Without
		// the detail both the OTel SDK and the Collector drop the batch, so a
		// bare ResourceExhausted relayed verbatim is a silent data loss; it
		// gets rewritten to Unavailable below instead.
		for _, d := range st.Details() {
			if _, ok := d.(*errdetails.RetryInfo); ok {
				return true
			}
		}
	}
	return false
}

// grpcForwardStatus maps a forwarding failure onto a gRPC status the sender's
// SDK retries correctly. A bare error would surface as codes.Unknown —
// NON-retryable per the OTLP spec — making senders permanently drop batches on
// transient conditions (a full disk buffer, an upstream 5xx).
//
// Permanence is classified by otlpexport.IsPermanent, the single source of
// truth, and that classification has to come FIRST. Passing an upstream status
// through on sight — which is what this used to do — made IsPermanent dead code
// for the DEFAULT gRPC upstream, because virtually every failure from it is a
// status error. So an upstream Unauthenticated/PermissionDenied (a collector
// token mid-rotation) or Internal reached the pushing application, which reads
// all three as NON-retryable and drops the batch — while IsPermanent
// deliberately classifies auth failures and 404 as TRANSIENT, and the HTTP arm
// of this same receiver answered 503 for the identical error. One receiver, two
// answers, and the wrong one lost data.
//
// An upstream status is still preserved where it is genuinely more informative
// than Unavailable, but only when the sender will also read it as retryable —
// otherwise the code is rewritten rather than relayed.
func grpcForwardStatus(err error) error {
	// Only definitive upstream rejections become InvalidArgument (do not
	// retry). Everything else — diskqueue.ErrFull back-pressure, upstream 5xx,
	// 401/403/404 windows, timeouts, unclassified failures — is retryable: the
	// receiver is a proxy, and the sender retrying is the safe default.
	if otlpexport.IsPermanent(err) {
		// Already the answer this function would build (the trace tier's receive
		// guard returns exactly this): relay it verbatim. Re-wrapping rendered
		// the sender `code = InvalidArgument desc = rpc error: code =
		// InvalidArgument desc = …`, burying the reason it needs one nesting deep
		// inside the field it reads first.
		if st, ok := status.FromError(err); ok && st.Code() == codes.InvalidArgument {
			return err
		}
		return status.Error(codes.InvalidArgument, err.Error())
	}
	if st, ok := status.FromError(err); ok && retryableStatus(st) {
		return err
	}
	return status.Error(codes.Unavailable, err.Error())
}

type tracesGRPC struct {
	ptraceotlp.UnimplementedGRPCServer
	s *Server
}

// rejectTraces consults the receive-path guard (ServerConfig.RejectTraces). It
// is the FIRST step of both trace handlers and the last one that may run before
// EnrichTraces: a payload this refuses must cost no metadata lookup and move no
// ingest counter. An unset guard accepts everything.
func (s *Server) rejectTraces(ctx context.Context, td ptrace.Traces) error {
	if s.cfg.RejectTraces == nil {
		return nil
	}
	return s.cfg.RejectTraces(ctx, td)
}

func (g *tracesGRPC) Export(ctx context.Context, req ptraceotlp.ExportRequest) (ptraceotlp.ExportResponse, error) {
	err := grpcExport(ctx, func(ctx context.Context) error {
		td := req.Traces()
		if err := g.s.rejectTraces(ctx, td); err != nil {
			return err
		}
		g.s.cfg.Enricher.EnrichTraces(ctx, td)
		return g.s.cfg.Traces.ExportTraces(ctx, td)
	})
	if err != nil {
		return ptraceotlp.ExportResponse{}, err
	}
	return ptraceotlp.NewExportResponse(), nil
}

// --- HTTP (OTLP/HTTP protobuf) ---

// servePush is the ONE body of the three HTTP push handlers; they differ only
// in the codec (unmarshal) and the enrich-and-forward step (handle). The
// sequence is load-bearing and its steps are ordered deliberately:
//
//  1. Read the body (BodyReader: media type, gzip, caps, byte budget) —
//     failures answer through WriteBodyError (413/415/429/400).
//  2. The charge is released when the HANDLER returns, not when the read
//     ends: the body stays alive through enrichment and the forward, so
//     releasing earlier would leave the bytes resident and unaccounted.
//  3. The in-flight slot is acquired AFTER the read: holding a slot across
//     the upload let 32 trickled 16 MiB bodies shed every other sender on
//     the node for a ReadTimeout (60s) — no credentials required, on an
//     unauthenticated listener, which is the threat the bound exists for.
//     What bounds the read itself is the byte budget the body was charged
//     against (admit.go), not this slot. The gRPC arm is naturally on this
//     side of the decode. A refusal is RETRYABLE by design (429 +
//     Retry-After: 1): the sender still holds the payload.
//  4. A payload that does not unmarshal is 400 — permanent; retrying a
//     malformed batch can never succeed.
//  5. A forward failure maps through HTTPForwardStatus (permanent 400 vs
//     retryable 503), and success answers with the OTLP proto response.
func (s *Server) servePush(w http.ResponseWriter, r *http.Request,
	signal string,
	unmarshal func(body []byte) error,
	handle func(ctx context.Context) (ProtoMarshaler, error),
) {
	body, charged, err := s.body.Read(r)
	if err != nil {
		WriteBodyError(w, err)
		return
	}
	defer s.body.Release(charged)
	if !s.acquire() {
		w.Header().Set("Retry-After", "1")
		http.Error(w, "too many concurrent pushes", http.StatusTooManyRequests)
		return
	}
	defer s.release()
	if err := unmarshal(body); err != nil {
		http.Error(w, "malformed OTLP "+signal+" payload", http.StatusBadRequest)
		return
	}
	resp, err := handle(withPeerIP(r.Context(), r.RemoteAddr))
	if err != nil {
		http.Error(w, err.Error(), HTTPForwardStatus(err))
		return
	}
	WriteProto(w, resp)
}

func (s *Server) handleHTTPLogs(w http.ResponseWriter, r *http.Request) {
	req := plogotlp.NewExportRequest()
	s.servePush(w, r, "logs", req.UnmarshalProto, func(ctx context.Context) (ProtoMarshaler, error) {
		ld := req.Logs()
		s.cfg.Enricher.EnrichLogs(ctx, ld)
		// Handoff: as on the gRPC arm — the decoded push dies with a failed
		// request, and the sender's retry re-decodes retransmitted bytes.
		if err := s.cfg.Exporter.ExportLogs(transform.Handoff(ctx), ld); err != nil {
			return nil, err
		}
		return plogotlp.NewExportResponse(), nil
	})
}

func (s *Server) handleHTTPMetrics(w http.ResponseWriter, r *http.Request) {
	req := pmetricotlp.NewExportRequest()
	s.servePush(w, r, "metrics", req.UnmarshalProto, func(ctx context.Context) (ProtoMarshaler, error) {
		md := s.cfg.Enricher.EnrichMetrics(ctx, req.Metrics())
		// Handoff: as on the gRPC arm.
		if err := s.cfg.Exporter.ExportMetrics(transform.Handoff(ctx), md); err != nil {
			return nil, err
		}
		return pmetricotlp.NewExportResponse(), nil
	})
}

func (s *Server) handleHTTPTraces(w http.ResponseWriter, r *http.Request) {
	req := ptraceotlp.NewExportRequest()
	s.servePush(w, r, "traces", req.UnmarshalProto, func(ctx context.Context) (ProtoMarshaler, error) {
		td := req.Traces()
		if err := s.rejectTraces(ctx, td); err != nil {
			return nil, err
		}
		s.cfg.Enricher.EnrichTraces(ctx, td)
		if err := s.cfg.Traces.ExportTraces(ctx, td); err != nil {
			return nil, err
		}
		return ptraceotlp.NewExportResponse(), nil
	})
}

const maxIngestBody = 16 << 20 // 16 MiB per request

// maxIngestGRPCMessage caps ONE decoded gRPC message (grpc-go's own default,
// stated here because the tap reserves exactly this much per push).
const maxIngestGRPCMessage = 4 << 20
