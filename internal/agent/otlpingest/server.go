package otlpingest

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/JohanLindvall/kubescrape/internal/obs"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/plog/plogotlp"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/pmetric/pmetricotlp"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	grpcpeer "google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/JohanLindvall/kubescrape/internal/agent/otlpexport"
	"github.com/JohanLindvall/kubescrape/internal/agent/transform"
	"github.com/JohanLindvall/kubescrape/internal/logdedupe"
	"github.com/JohanLindvall/kubescrape/internal/logline"
	"github.com/JohanLindvall/kubescrape/internal/metrics"
	"github.com/JohanLindvall/kubescrape/pkg/logattrs"
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
	// Admit, when set, is consulted once per pushed RESOURCE (all three
	// signals) before enrichment: false removes the resource from the
	// payload — the transforms file's ingest: hook, the operator's
	// per-sender policy on listeners nothing authenticates. Removals are
	// counted (obs.IngestAdmissionRejected) and the push is still acked.
	Admit func(attrs pcommon.Map) bool
	// ReservedAttrs are kubescrape's own plumbing keys, stripped from every
	// accepted payload before enrichment (see reserved.go — their consumers
	// are presence-only and cannot tell kubescrape's mark from a sender's, so
	// a wire-supplied copy steers routing or masquerades as an operator's
	// drop()). WHICH keys are reserved is the caller's knowledge, like
	// RejectTraces' marker: this package must not know route's or transform's
	// spellings. The zero value strips nothing.
	ReservedAttrs ReservedAttrs
	// Rules is the global logs.rules keep/drop/sample chain, applied to
	// INGESTED log records after enrichment (so __severity__ selects on the
	// enriched severity) — the same chain, same semantics, as the tailer,
	// journald, events and Azure producers. A dropped record is removed from
	// the payload before forwarding and the push is still ACKED: the sender
	// delivered it, the operator chose to drop it (obs.LogRulesDropped). nil
	// forwards everything.
	Rules *logline.LineFilter
	// LogAttrs is the global logAttributes extractor, applied to INGESTED log
	// records in the producers' position — after scrubbing, before enrichment,
	// before log-metrics and before Rules — so a rule or a metric label keyed
	// on a LIFTED name selects identically whether the line was tailed or
	// pushed. nil lifts nothing.
	//
	// The `target: log` half is applied; the resource and scope halves are
	// deliberately NOT. A producer applies those by GROUPING records into a
	// resource that carries them, and an ingested record already lives in the
	// sender's own grouping — writing them onto the shared resource would
	// stamp one record's lifted values onto every other record beside it. They
	// The RESOURCE half still RESOLVES for metric labels and rule keys
	// (Resolver.SetLifted takes exactly that slice — logchain.go), which is
	// where the divergence was actually visible. The SCOPE half is dropped on
	// this path entirely: nothing carries it and nothing resolves it. A tailed
	// line differs there, since the tailer stamps scope lifts onto the
	// ScopeLogs it groups records into, so they reach the wire.
	LogAttrs *logattrs.Extractor
	// LogMetrics observes EVERY ingested log record (before Rules — a metric
	// counting errors must not fall to zero because a rule stopped shipping
	// the lines), with the sender's enriched resource as the metric resource.
	// nil observes nothing.
	//
	// Observations here are once per RECEIVE ATTEMPT rather than once per
	// delivery — the sender's retry of a NACKed push re-runs the chain and
	// observes again. See chainCommit (logchain.go) for why the tally that
	// COULD be staged is, and this one is not.
	LogMetrics *metrics.DynamicMetricSet
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
	// decoded bounds what those bytes inflate INTO — the structural half of
	// the decoded pdata, charged after the unmarshal and released with the
	// handler (admit.go). Neither of the other two bounds covers it: a count
	// cannot bound a size, and 30 wire bytes can mint a ResourceLogs.
	decoded *byteBudget
	// decodedWarns throttles the one line worth saying about it: a single
	// push whose structure alone exceeds the whole budget can never be
	// admitted, and an operator would otherwise read the resulting stream of
	// 429s as ordinary back-pressure.
	decodedWarns logdedupe.Throttle
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
	// reservedWarns throttles the stripped-key log line per key, for BOTH
	// classes (the plumbing markers' Warn and the identity claim's Debug — the
	// two key spaces are disjoint, so one table cannot crowd the other out).
	// The key space is the operator-wired ReservedAttrs lists — never
	// sender-chosen — so the table is sized to exactly that.
	reservedWarns *logdedupe.Table
	// reserveExpiries is the since-start total of pre-decode reservations
	// reclaimed by the decode window, and it exists ONLY to put a magnitude on
	// the throttled log line reserveExpiryWarns paces — the series an operator
	// alerts on is obs.IngestReserveExpired, kept apart from obs.IngestRejected
	// on purpose (reservation.expire says why).
	reserveExpiries    atomic.Uint64
	reserveExpiryWarns logdedupe.Throttle
	// emptyMetricWarns throttles the empty-metric prune's warning
	// (emptymetrics.go). Keyless: the condition is one sender-side
	// instrumentation bug shipped on every push, and the count rides the line.
	emptyMetricWarns logdedupe.Throttle
	// chainSkipWarns throttles the ingested-log-chain skip line, per reason
	// (logchain.go). Keyed: three bounds, three different sender-side fixes.
	chainSkipWarns *logdedupe.Table
	// shedWarns throttles the admission-bound shedding line, per bound
	// (noteShed). Keyed rather than keyless because the three bounds are three
	// different problems and the busiest must not suppress the others.
	shedWarns *logdedupe.Table
	// tooDeepWarns throttles the wire-shape refusal's warning (depth.go).
	// Keyless for the same reason: it is one sender emitting one shape, and a
	// line per refusal is what an attacker would use to fill the node's disk.
	tooDeepWarns logdedupe.Throttle
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
		decoded:       &byteBudget{limit: decodedBudgetFactor * budget},
		reserveWindow: grpcReserveWindow,
		reservedWarns: logdedupe.New(len(cfg.ReservedAttrs.Resource)+len(cfg.ReservedAttrs.Element)+
			len(cfg.ReservedAttrs.Identity), reservedWarnEvery),
		shedWarns:      logdedupe.New(3, shedWarnEvery),      // one key per admission bound
		chainSkipWarns: logdedupe.New(3, chainSkipWarnEvery), // one key per chain-skip reason
	}
	s.body = newBodyReader(maxIngestBody, s.buffer, log)
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

// chargeDecoded reserves a push's estimated decoded structure, reporting
// whether it fits. A refusal is the same event as a full byte budget or a full
// slot table — obs.IngestRejected, answered retryably by both transports — with
// one addition: a push too big for the WHOLE budget is a sender that must batch
// smaller, and no amount of back-pressure will teach it that, so it gets a line.
func (s *Server) chargeDecoded(n int64) bool {
	if n <= 0 || s.decoded.reserve(n) {
		return true
	}
	obs.IngestRejected.Inc()
	if n > s.decoded.limit && s.decodedWarns.Allow(decodedWarnEvery) {
		s.log.Warn("ingest: refused a push whose decoded structure alone exceeds the receiver's whole "+
			"decoded budget; every retry of it will be refused too — the sender must batch smaller",
			"estimatedBytes", n, "budgetBytes", s.decoded.limit)
	}
	return false
}

// decodedWarnEvery paces that line: a sender batching this large batches this
// large on every push.
const decodedWarnEvery = time.Minute

// limitUnary applies the COUNT bound to gRPC pushes and hands the pre-decode
// reservation over to it.
//
// The ORDER is load-bearing and it used to be the other way round. The message
// is already decoded by the time this runs, so the pre-decode reservation is
// the only thing accounting for it; releasing that first and only then asking
// for a slot left a window in which the payload was charged to NOTHING, and a
// refused push released a reservation it had already given up — so a peer could
// hold as many decoded messages resident as it could open streams, bounded by
// the count alone. Take the slot first, then hand the accounting over.
//
// What is deliberately NOT here is the DECODED-STRUCTURE charge, and its
// absence is the correction of a bound that measured zero in production for as
// long as it existed. grpc-go hands an interceptor the message its GENERATED
// handler decoded into; for pdata that is *pdata/internal.ExportLogsServiceRequest
// (and its two siblings), because the public plogotlp.ExportRequest wrapper is
// constructed one layer further in, by rawLogsServer.Export. The type is in
// pdata's internal/, so NO type switch here can name it — the charge that sat
// here matched nothing, reserved nothing, and left a decoded gRPC payload
// bounded by this count alone, which admit.go's header says in as many words a
// count cannot do. It is taken in the three Export methods below, where the
// typed request exists.
func (s *Server) limitUnary(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	if !s.acquire() {
		s.noteShed(shedInFlight, grpcPeerAddr(ctx))
		return nil, exhaustedStatus("too many concurrent pushes; retry")
	}
	defer s.release()
	// The slot is held: the reservation tapAdmit took for the read has done its
	// job and the message buffer itself is already freed by the codec.
	releaseReservation(ctx)
	return handler(ctx, req)
}

// chargeDecodedGRPC charges one gRPC push's estimated decoded structure and
// returns the release, which the caller defers so EVERY exit — refusal from a
// downstream guard, a failed forward, an early ack — gives the budget back.
//
// The refusal is the retryable ResourceExhausted + RetryInfo that the HTTP arm
// spells 429 + Retry-After (exhaustedStatus): one condition, one answer,
// whichever transport a sender used. Bare ResourceExhausted reads as PERMANENT
// to conformant senders, and a shed that loses data is worse than the OOM it
// prevents.
func (s *Server) chargeDecodedGRPC(ctx context.Context, n int64) (func(), error) {
	if !s.chargeDecoded(n) {
		s.noteShed(shedDecoded, grpcPeerAddr(ctx))
		return nil, exhaustedStatus(errDecodedBudget.Error())
	}
	return func() { s.decoded.release(n) }, nil
}

// tooDeepWarnEvery paces the wire-shape refusal warning: a sender emitting a
// body this deep emits it on every push, and one line names the condition.
const tooDeepWarnEvery = time.Minute

// noteTooDeep reports a payload refused for its nesting on the gRPC arm. It
// counts into the same series as the HTTP door's refusals
// (obs.IngestBodyRejected{reason="too_deep"}) because it is the same event —
// an application push refused at a listener nothing authenticates — reached
// through the other transport; the HTTP arm's own counting happens in
// BodyReader.noteRejected.
func (s *Server) noteTooDeep() {
	obs.IngestBodyRejected.WithLabelValues(reasonTooDeep).Inc()
	if s.tooDeepWarns.Allow(tooDeepWarnEvery) {
		s.log.Warn("ingest: refused a gRPC push whose payload nests deeper than the decoder may recurse; "+
			"decoding it costs unbounded goroutine stack, so the shape is refused before the decode",
			"maxDepth", maxNestingDepth)
	}
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
			// And this is what bounds the HEADERS, which none of the four
			// above reach: grpc-go decodes a stream's header block before the
			// tap runs, at a 16 MiB default (MaxHeaderListSizeOption).
			MaxHeaderListSizeOption(),
			grpc.MaxConcurrentStreams(uint32(s.maxInFlight)),
			grpc.InTapHandle(s.tapAdmit),
			grpc.UnaryInterceptor(s.limitUnary),
			// And this is what bounds the DECODE ITSELF (depth.go): the
			// message is unmarshalled before the interceptor runs, so a
			// payload's nesting — which costs unbounded goroutine stack — can
			// only be refused from inside the codec.
			NestingGuardOption(s.noteTooDeep),
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
	// The decoded structure is charged HERE rather than in limitUnary, which
	// cannot see the typed request at all (see there), and it is held for the
	// whole enrich-and-forward step exactly as the HTTP arm holds it for the
	// whole handler: the payload stays resident until the collector acks.
	release, err := g.s.chargeDecodedGRPC(ctx, decodedLogsSize(req.Logs()))
	if err != nil {
		return plogotlp.ExportResponse{}, err
	}
	defer release()
	err = grpcExport(ctx, func(ctx context.Context) error { return g.s.forwardLogs(ctx, req.Logs()) })
	if err != nil {
		return plogotlp.ExportResponse{}, err
	}
	return plogotlp.NewExportResponse(), nil
}

// forwardLogs is the enrich-and-forward step for a decoded logs push, shared
// by the gRPC and HTTP arms (each used to spell it, and the two had drifted by
// a comment already). ctx carries the connection's peer address. In order:
// admission (the ingest: hook, per resource, pre-enrichment), the reserved
// strip (reserved.go), enrichment, then logAttributes + logs.rules +
// logMetrics AFTER enrichment (logchain.go) — a payload filtered to nothing is
// acked without a send, its drops final — and the export.
//
// The export takes transform.Handoff (logs and metrics only, never traces —
// the tier's tap reads a forwarded trace AFTER the export): on failure the
// decoded payload dies with this request and the sender's retry re-decodes
// retransmitted bytes, so the transform seam may run in place instead of
// deep-copying. A failed export is NOT counted: the sender will resend these
// very records.
func (s *Server) forwardLogs(ctx context.Context, ld plog.Logs) error {
	s.admitLogs(ld)
	s.sanitizeLogs(ld)
	s.cfg.Enricher.EnrichLogs(ctx, ld)
	cc, forward := s.applyLogChain(ld)
	if !forward {
		cc.commit()
		return nil
	}
	if err := s.cfg.Exporter.ExportLogs(transform.Handoff(ctx), ld); err != nil {
		return err
	}
	cc.commit()
	return nil
}

// forwardMetrics is forwardLogs' metrics sibling: admission, then the
// point-less metrics die before anything downstream pays to carry them
// (emptymetrics.go) — a push emptied by either is acked without a send — then
// the reserved strip, enrichment and the export under the same Handoff.
func (s *Server) forwardMetrics(ctx context.Context, in pmetric.Metrics) error {
	s.admitMetrics(in)
	s.pruneEmptyMetrics(in)
	if in.ResourceMetrics().Len() == 0 {
		return nil
	}
	s.sanitizeMetrics(in)
	md := s.cfg.Enricher.EnrichMetrics(ctx, in)
	return s.cfg.Exporter.ExportMetrics(transform.Handoff(ctx), md)
}

// forwardTraces is the traces sibling. The loop guard (rejectTraces) runs
// FIRST, before admission and the reserved strip — a refused payload must cost
// no lookup and move no counter — and the export takes NO Handoff: the tier's
// tap reads a forwarded trace after the export.
func (s *Server) forwardTraces(ctx context.Context, td ptrace.Traces) error {
	if err := s.rejectTraces(ctx, td); err != nil {
		return err
	}
	s.admitTraces(td)
	if td.ResourceSpans().Len() == 0 {
		return nil
	}
	s.sanitizeTraces(td)
	s.cfg.Enricher.EnrichTraces(ctx, td)
	return s.cfg.Traces.ExportTraces(ctx, td)
}

type metricsGRPC struct {
	pmetricotlp.UnimplementedGRPCServer
	s *Server
}

func (g *metricsGRPC) Export(ctx context.Context, req pmetricotlp.ExportRequest) (pmetricotlp.ExportResponse, error) {
	// Charged here for the same reason as the logs arm, and estimated BEFORE
	// pruning: the point-less metrics emptymetrics.go removes are resident by
	// the time it runs.
	release, err := g.s.chargeDecodedGRPC(ctx, decodedMetricsSize(req.Metrics()))
	if err != nil {
		return pmetricotlp.ExportResponse{}, err
	}
	defer release()
	err = grpcExport(ctx, func(ctx context.Context) error { return g.s.forwardMetrics(ctx, req.Metrics()) })
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
	// Charged here for the same reason as the logs arm — including for a
	// payload RejectTraces will refuse: the spans are already decoded and
	// resident when the guard runs.
	release, err := g.s.chargeDecodedGRPC(ctx, decodedTracesSize(req.Traces()))
	if err != nil {
		return ptraceotlp.ExportResponse{}, err
	}
	defer release()
	err = grpcExport(ctx, func(ctx context.Context) error { return g.s.forwardTraces(ctx, req.Traces()) })
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
//     failures answer through WriteBodyError: 413 over either cap, 415 on the
//     media type, 429 + Retry-After when the byte budget is full, 503 for an
//     upload that ENDED rather than arrived (a killed pod, a rolled
//     deployment, the server's own ReadTimeout — retryable, and deliberately
//     not 400 or 408; see BodyErrorStatus), 400 for everything else.
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
//  5. The DECODED payload is charged its own budget, held for the rest of the
//     handler exactly as the raw body's charge is: the raw bytes bound what
//     arrives, not what it inflates into, and 30 wire bytes can mint a
//     ResourceLogs (admit.go). Over it, 429 + Retry-After, like the byte
//     budget — the sender still holds an intact payload.
//  6. A forward failure maps through HTTPForwardStatus (permanent 400 vs
//     retryable 503), and success answers with the OTLP proto response.
func (s *Server) servePush(w http.ResponseWriter, r *http.Request,
	signal string,
	// decode unmarshals the body and returns the ESTIMATED decoded structure
	// (decodedLogsSize and its siblings), which only the caller knows the
	// signal of.
	decode func(body []byte) (int64, error),
	handle func(ctx context.Context) (ProtoMarshaler, error),
) {
	body, charged, err := s.body.Read(r)
	if err != nil {
		// The door's own refusals (media type, caps, a truncated upload) are
		// counted AND warned inside the reader, per reason. The byte BUDGET is
		// not one of those — it is this receiver protecting itself rather than
		// the request being wrong — so it is narrated here, on the arm that
		// still holds the request and can name the sender.
		if errors.Is(err, errBufferBudget) {
			s.noteShed(shedBuffer, r.RemoteAddr)
		}
		WriteBodyError(w, err)
		return
	}
	defer s.body.Release(charged)
	if !s.acquire() {
		s.noteShed(shedInFlight, r.RemoteAddr)
		w.Header().Set("Retry-After", "1")
		http.Error(w, "too many concurrent pushes", http.StatusTooManyRequests)
		return
	}
	defer s.release()
	decoded, err := decode(body)
	if err != nil {
		// The same door as the read above, and the same reason label: a body
		// that arrived intact and is not OTLP. This was answered and counted by
		// NOTHING while the seam three lines up owned a reason literally called
		// "malformed" — the likelier of the two ways to be wrong was the
		// invisible one.
		s.body.noteMalformed(r, err)
		http.Error(w, "malformed OTLP "+signal+" payload", http.StatusBadRequest)
		return
	}
	if !s.chargeDecoded(decoded) {
		s.noteShed(shedDecoded, r.RemoteAddr)
		w.Header().Set("Retry-After", "1")
		http.Error(w, errDecodedBudget.Error(), http.StatusTooManyRequests)
		return
	}
	defer s.decoded.release(decoded)
	resp, err := handle(withPeerIP(r.Context(), r.RemoteAddr))
	if err != nil {
		http.Error(w, err.Error(), HTTPForwardStatus(err))
		return
	}
	WriteProto(w, resp)
}

func (s *Server) handleHTTPLogs(w http.ResponseWriter, r *http.Request) {
	req := plogotlp.NewExportRequest()
	decode := func(body []byte) (int64, error) {
		if err := req.UnmarshalProto(body); err != nil {
			return 0, err
		}
		return decodedLogsSize(req.Logs()), nil
	}
	s.servePush(w, r, "logs", decode, func(ctx context.Context) (ProtoMarshaler, error) {
		if err := s.forwardLogs(ctx, req.Logs()); err != nil {
			return nil, err
		}
		return plogotlp.NewExportResponse(), nil
	})
}

func (s *Server) handleHTTPMetrics(w http.ResponseWriter, r *http.Request) {
	req := pmetricotlp.NewExportRequest()
	decode := func(body []byte) (int64, error) {
		if err := req.UnmarshalProto(body); err != nil {
			return 0, err
		}
		return decodedMetricsSize(req.Metrics()), nil
	}
	s.servePush(w, r, "metrics", decode, func(ctx context.Context) (ProtoMarshaler, error) {
		if err := s.forwardMetrics(ctx, req.Metrics()); err != nil {
			return nil, err
		}
		return pmetricotlp.NewExportResponse(), nil
	})
}

func (s *Server) handleHTTPTraces(w http.ResponseWriter, r *http.Request) {
	req := ptraceotlp.NewExportRequest()
	decode := func(body []byte) (int64, error) {
		if err := req.UnmarshalProto(body); err != nil {
			return 0, err
		}
		return decodedTracesSize(req.Traces()), nil
	}
	s.servePush(w, r, "traces", decode, func(ctx context.Context) (ProtoMarshaler, error) {
		if err := s.forwardTraces(ctx, req.Traces()); err != nil {
			return nil, err
		}
		return ptraceotlp.NewExportResponse(), nil
	})
}

const maxIngestBody = 16 << 20 // 16 MiB per request

// maxIngestGRPCMessage caps ONE decoded gRPC message (grpc-go's own default,
// stated here because the tap reserves exactly this much per push).
const maxIngestGRPCMessage = 4 << 20

// --- shedding: the CONTEXT half of obs.IngestRejected ---

// The three admission bounds, as throttle keys and as the `reason` on the line.
// They are the counter's three causes, spelled the same way, because "the
// receiver is shedding" has three different fixes: too many senders at once,
// too many raw bytes resident, or too much structure inflated out of them.
const (
	shedInFlight = "in_flight"
	shedBuffer   = "buffer_bytes"
	shedDecoded  = "decoded_bytes"
)

// shedWarnEvery paces the shedding line. A receiver at a bound stays at it for
// as long as the load lasts, and every refused push would otherwise produce a
// line — on an UNAUTHENTICATED listener, i.e. a log volume a stranger chooses.
const shedWarnEvery = time.Minute

// noteShed narrates a push refused by an admission bound. The COUNT is taken at
// each bound (obs.IngestRejected, whose per-bound comments argue for it); this
// is the half a counter cannot carry — which bound bound, what its limit is,
// which flag moves it, and who was pushing.
//
// It matters because the refusal is INVISIBLE to everything except the sender:
// the answer is retryable by design (429 + Retry-After / ResourceExhausted +
// RetryInfo), so a well-behaved SDK simply retries and the operator sees
// telemetry arriving late, or not at all, with nothing in this process's log
// saying it refused anything. That was true of all three bounds.
//
// peer is empty where the transport cannot supply one. The gRPC pre-decode tap
// is the real case: grpc-go runs it before the peer reaches the stream context
// (server.go's peer.NewContext happens per RPC, after the tap) and tap.Info
// carries no address, so the one refusal taken before any decode is also the
// one that cannot name its sender. An empty key is omitted rather than logged
// blank.
func (s *Server) noteShed(reason, peer string) {
	if allow, _ := s.shedWarns.Allow(reason); !allow {
		return
	}
	args := []any{"reason", reason}
	var limit int64
	switch reason {
	case shedInFlight:
		limit = int64(s.maxInFlight)
		args = append(args, "limit", limit, "flag", "-ingest-max-in-flight")
	case shedBuffer:
		limit = s.buffer.limit
		args = append(args, "limitBytes", limit)
		args = append(args, s.budgetSourceArgs()...)
	case shedDecoded:
		limit = s.decoded.limit
		args = append(args, "limitBytes", limit)
		args = append(args, s.budgetSourceArgs()...)
	}
	if peer != "" {
		args = append(args, "peer", peer)
	}
	s.log.Warn("ingest: shedding pushes at an admission bound; senders are answered retryably and keep their "+
		"payloads, so telemetry arrives late or not at all while this lasts", args...)
}

// budgetSourceArgs says where the byte budget that just bound came FROM, which
// is not the same question as which flag exists. Both budgets derive from
// max(maxBufferBytes, 4 x MaxRecvBytes) (NewServer), so at the default 4 MiB
// receive cap the built-in floor is what binds and
// -ingest-grpc-max-recv-bytes moves NOTHING until it is set above
// maxBufferBytes/4. Naming the flag unconditionally — which this line used to
// do — sends an operator to raise a value that cannot change the limit they
// are reading in the same record, and the only evidence that it did nothing is
// the shedding continuing.
func (s *Server) budgetSourceArgs() []any {
	if 4*int64(s.grpcMaxRecv) > int64(maxBufferBytes) {
		return []any{"flag", "-ingest-grpc-max-recv-bytes", "recvBytes", s.grpcMaxRecv}
	}
	return []any{
		"recvBytes", s.grpcMaxRecv,
		"floorBytes", int64(maxBufferBytes),
		"note", "the budget is at its built-in floor; -ingest-grpc-max-recv-bytes raises it only once set above " +
			strconv.Itoa(maxBufferBytes/4) + " bytes, so shrink the senders' batches instead",
	}
}

// grpcPeerAddr is the sender's address for a REFUSAL line. It is called only on
// a refusal: p.Addr.String() allocates, and the admitted path must not pay for
// a string nothing reads.
func grpcPeerAddr(ctx context.Context) string {
	if p, ok := grpcpeer.FromContext(ctx); ok && p.Addr != nil {
		return p.Addr.String()
	}
	return ""
}
