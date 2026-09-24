package otlpingest

import (
	"context"
	"log/slog"
	"math"
	"net/http"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/plog/plogotlp"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/pmetric/pmetricotlp"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"
	"google.golang.org/grpc"

	"github.com/JohanLindvall/kubescrape/internal/agent/logscrub"
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
	// The error travels back through the same CLASSIFICATION as an export
	// failure (grpcForwardStatus / HTTPForwardStatus), so a guard that must
	// refuse PERMANENTLY returns something otlpexport.IsPermanent classifies as
	// such (codes.InvalidArgument) and the sender sees InvalidArgument / 400.
	//
	// Its TEXT, unlike a forward failure's, reaches the sender verbatim
	// (receiveRefusal): a forward failure's words are the collector's — its
	// endpoint, its resolved address, its response body — and are redacted on a
	// listener with no credentials, while this refusal is kubescrape's own
	// sentence about the payload in front of it and is usually the only thing
	// that tells a misconfigured hop which marker refused it. The corollary is a
	// contract on the guard: whatever it writes here is read by an
	// unauthenticated sender, so it must name the payload and never a
	// destination.
	//
	// It runs AFTER admission (the byte budget and the in-flight slot), so a
	// refused push releases everything it took, exactly as an accepted one does.
	RejectTraces func(ctx context.Context, td ptrace.Traces) error
	// Admit, when set, is consulted once per pushed RESOURCE (all three
	// signals) AFTER the reserved strip and BEFORE enrichment: false removes
	// the resource from the payload — the transforms file's ingest: hook, the
	// operator's per-sender policy on listeners nothing authenticates. Removals
	// are counted (obs.IngestAdmissionRejected) and the push is still acked.
	//
	// WHAT THE HOOK SEES, because a policy is only as good as its inputs. The
	// strip has already run, so the sender's own Kubernetes identity claim
	// (k8s.namespace.name, the k8s.pod.*/k8s.node.name/container.* siblings) is
	// GONE — a policy cannot be steered by a value any pod may write. Enrichment
	// has NOT run, so the resolved identity is not there either: on this path a
	// resource carries the sender's lookup key (container.id / k8s.pod.uid),
	// its service triple, and whatever descriptive attributes it chose, and a
	// hook keyed on a namespace matches nothing at all. That is deliberate on
	// both ends — the pre-enrichment position is what keeps a rejected sender
	// from spending a metadata lookup per resource on its way out, and the
	// post-strip position is what keeps the hook from deciding on a forgery.
	// Write per-sender policy against the lookup key or the service triple.
	Admit func(attrs pcommon.Map) bool
	// ReservedAttrs are kubescrape's own plumbing keys, stripped from every
	// accepted payload before enrichment (see reserved.go — their consumers
	// are presence-only and cannot tell kubescrape's mark from a sender's, so
	// a wire-supplied copy steers routing or masquerades as an operator's
	// drop()). WHICH keys are reserved is the caller's knowledge, like
	// RejectTraces' marker: this package must not know route's or transform's
	// spellings. The zero value strips nothing.
	ReservedAttrs ReservedAttrs
	// Scrub redacts sensitive values from every pushed log body — every string
	// leaf of a structured one (scrub.go) — as the FIRST per-record step of the
	// log chain, before the lift, enrichment, log-metrics and the rules read
	// it, and whatever the body's size: enrichment copies body slices
	// (exception attributes) that must not carry secrets, and the metric/rule
	// chain matches against the same scrubbed view. nil scrubs nothing. The
	// producers' chain takes the same scrubber (logchain.Config.Scrub).
	Scrub *logscrub.Scrubber
	// EnrichLines parses each pushed log record's body for a timestamp,
	// severity, trace/span IDs and structured fields (-enrich, the same flag
	// the tailer's line enrichment reads), filling only fields the sender left
	// unset — over the chain's one bounded rendering of the body (logchain.go).
	EnrichLines bool
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
	// stamp one record's lifted values onto every other record beside it.
	//
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
	// The HTTP body cap is unaffected (maxIngestBody, already 16 MiB). Values
	// past what gRPC can frame (4 GiB - 1) are clamped to it.
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
	// the decoded pdata, estimated from the wire bytes and charged BEFORE the
	// unmarshal, released with the handler (admit.go, decodedsize.go). Neither
	// of the other two bounds covers it: a count cannot bound a size, and 30
	// wire bytes can mint a ResourceLogs.
	decoded *byteBudget
	// claims hands a gRPC push's decoded charge from the codec, which takes it,
	// to the interceptor, which releases it or answers its refusal.
	claims decodedClaims
	// decodedWarns throttles the one line worth saying about it: a single
	// push whose structure alone exceeds the whole budget can never be
	// admitted, and an operator would otherwise read the resulting stream of
	// 429s as ordinary back-pressure.
	decodedWarns logdedupe.Throttle
	// reserveWindow bounds how long ONE gRPC reservation may live, i.e. how long
	// a peer may sit between its HEADERS frame and a decoded message — the whole
	// upload, not just the decode. It is reserveWindowFor(grpcMaxRecv), so a
	// raised -ingest-grpc-max-recv-bytes buys the time to deliver the bigger
	// message it just authorised. Tests shorten it to exercise the reclaim.
	reserveWindow time.Duration
	// grpcMaxRecv is the resolved MaxRecvBytes: the per-message gRPC cap and
	// the tap's per-push reservation.
	grpcMaxRecv int
	// stampPeer records, once, whether the enricher reads the connection's peer
	// address (its opt-in peer-IP fallback); both transports stamp the address
	// onto the request context only then (peer.go says what the stamp costs).
	stampPeer bool
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
	// (logchain.go). Keyed: four bounds, four different sender-side fixes, and
	// sized from chainSkipReasons so no reason can be crowded out.
	chainSkipWarns *logdedupe.Table
	// shedWarns throttles the admission-bound shedding line, per bound
	// (noteShed). Keyed rather than keyless because the three bounds are three
	// different problems and the busiest must not suppress the others.
	shedWarns *logdedupe.Table
	// tooDeepWarns throttles the wire-shape refusal's warning (depth.go).
	// Keyless for the same reason: it is one sender emitting one shape, and a
	// line per refusal is what an attacker would use to fill the node's disk.
	tooDeepWarns logdedupe.Throttle
	// forwardWarns throttles the forward-failure narration, keyed by SIGNAL
	// (noteForwardFailure). Keyed rather than keyless because routing can send
	// the three signals to three different destinations, and a dead logs
	// endpoint must not suppress the line about a dead metrics one.
	forwardWarns *logdedupe.Table
}

// NewServer creates an ingest Server.
func NewServer(cfg ServerConfig) *Server {
	log := cfg.Logger
	if log == nil {
		log = slog.Default()
	}
	n := EffectiveMaxInFlight(cfg.MaxInFlight)
	// EffectiveMaxRecvBytes: the default when unset, then clamped to the
	// largest message gRPC can FRAME (a uint32 length prefix), before
	// anything is derived from it. A larger value authorises nothing a
	// sender could deliver, and it is what kept the arithmetic below honest: at
	// math.MaxInt the 4x budget wrapped negative and was ignored (the buffer
	// stayed at its 64 MiB floor while each tap reserved MaxInt, so a
	// reservation with ANY bytes already held wrapped `used` negative and was
	// admitted — the budget off), and past MaxInt/8 the decoded limit went
	// negative and refused every push. reserveWindowFor guards its own
	// multiply; these did not.
	recv := EffectiveMaxRecvBytes(cfg.MaxRecvBytes)
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
		stampPeer:     cfg.Enricher.usesPeerIP(),
		buffer:        &byteBudget{limit: budget},
		decoded:       &byteBudget{limit: decodedBudgetFactor * budget},
		reserveWindow: reserveWindowFor(recv),
		reservedWarns: logdedupe.New(len(cfg.ReservedAttrs.Resource)+len(cfg.ReservedAttrs.Element)+
			len(cfg.ReservedAttrs.Identity), reservedWarnEvery),
		shedWarns:      logdedupe.New(3, shedWarnEvery), // one key per admission bound
		chainSkipWarns: logdedupe.New(len(chainSkipReasons), chainSkipWarnEvery),
		forwardWarns:   logdedupe.New(3, forwardWarnEvery), // one key per signal
	}
	s.body = newIngestBodyReader(maxIngestBody, s.buffer, log)
	return s
}

// Run serves until ctx is cancelled, then shuts both listeners down. The
// run/shutdown skeleton is Listeners (listeners.go); this builds the two
// servers it runs.
func (s *Server) Run(ctx context.Context) error {
	l := Listeners{Name: "otlp ingest", Logger: s.log, Ready: s.cfg.Ready}

	if s.cfg.GRPCAddr != "" {
		l.GRPCAddr = s.cfg.GRPCAddr
		l.GRPC = grpc.NewServer(append([]grpc.ServerOption{
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
			//   - The tap (tapAdmit, grpcadmit.go) is what bounds the decode: it
			//     runs on the HEADERS frame, before grpc-go reads the message,
			//     and reserves MaxRecvMsgSize from the server-wide byte budget
			//     for at most the decode window (reserveWindowFor) — a peer
			//     that does not finish its message inside it loses the
			//     reservation and the stream with it.
			//     That closes the gap the previous three left — unbounded
			//     concurrent BUFFERING — and it can carry the RetryInfo a shed
			//     needs, because writeEarlyAbort forwards a status' details.
			grpc.MaxRecvMsgSize(s.grpcMaxRecv),
			// And this is what bounds the HEADERS, which none of the four
			// above reach: grpc-go decodes a stream's header block before the
			// tap runs, at a 16 MiB default (MaxHeaderListSizeOption).
			MaxHeaderListSizeOption(),
			grpc.MaxConcurrentStreams(uint32(s.maxInFlight)),
			// And admissionOptions is the tap, the interceptor, the codec —
			// what bounds the DECODE ITSELF (depth.go): the message is
			// unmarshalled before the interceptor runs, so a payload's nesting,
			// which costs unbounded goroutine stack, can only be refused from
			// inside the codec, and so can what it inflates into (the
			// decoded-structure charge, decodedClaims) — and the stats handler
			// that returns a charge whose RPC never reached the interceptor.
		}, s.admissionOptions()...)...)
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

// forwardLogs is the enrich-and-forward step for a decoded logs push, shared
// by the gRPC and HTTP arms (each used to spell it, and the two had drifted by
// a comment already). ctx carries the connection's peer address. In order: the
// reserved strip (reserved.go), admission (the ingest: hook, per resource),
// enrichment, then logAttributes + logs.rules + logMetrics AFTER enrichment
// (logchain.go) — a payload filtered to nothing is acked without a send, its
// drops final — and the export.
//
// THE STRIP RUNS FIRST, and that order is load-bearing rather than incidental.
// The hook is the operator's per-sender policy on a listener that authenticates
// nothing, and it used to be handed the raw attribute map — so a policy written
// as `resource["k8s.namespace.name"] in ("a","b")` decided on a value the
// receiver itself refuses to trust one line later, admitting any pod in the
// cluster that simply declared the namespace. Sanitizing first gives the hook
// the same view of a resource as everything downstream: the sender's claim gone,
// nothing forged left to key on. It costs no metadata lookup — both strips are
// pure map walks — so the reason admission sits above ENRICHMENT (a rejected
// sender must not spend a lookup per resource on its way out) is untouched.
//
// What the hook still cannot see is the RESOLVED identity, which enrichment
// writes afterwards: on this path a resource has no k8s.namespace.name at all
// unless the sender forged one. ServerConfig.Admit says so.
//
// The export takes transform.Handoff (logs and metrics only, never traces —
// the tier's tap reads a forwarded trace AFTER the export): on failure the
// decoded payload dies with this request and the sender's retry re-decodes
// retransmitted bytes, so the transform seam may run in place instead of
// deep-copying. A failed export is NOT counted: the sender will resend these
// very records.
func (s *Server) forwardLogs(ctx context.Context, ld plog.Logs) error {
	s.sanitizeLogs(ld)
	s.admitLogs(ld)
	s.dedupeLogResources(ld)
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

// forwardMetrics is forwardLogs' metrics sibling, in the same order and for the
// same reason: the reserved strip, admission, then the point-less metrics die
// before anything downstream pays to carry them (emptymetrics.go) — a push
// emptied by either is acked without a send — then enrichment and the export
// under the same Handoff.
func (s *Server) forwardMetrics(ctx context.Context, in pmetric.Metrics) error {
	s.sanitizeMetrics(in)
	s.admitMetrics(in)
	s.pruneEmptyMetrics(in)
	if in.ResourceMetrics().Len() == 0 {
		return nil
	}
	md := s.cfg.Enricher.EnrichMetrics(ctx, in)
	return s.cfg.Exporter.ExportMetrics(transform.Handoff(ctx), md)
}

// forwardTraces is the traces sibling. The loop guard (rejectTraces) runs
// FIRST, before the reserved strip and admission — a refused payload must cost
// no lookup and move no counter, and it is refused on a marker the strip would
// otherwise have removed — and the export takes NO Handoff: the tier's tap reads
// a forwarded trace after the export.
func (s *Server) forwardTraces(ctx context.Context, td ptrace.Traces) error {
	if err := s.rejectTraces(ctx, td); err != nil {
		return err
	}
	s.sanitizeTraces(td)
	s.admitTraces(td)
	if td.ResourceSpans().Len() == 0 {
		return nil
	}
	s.cfg.Enricher.EnrichTraces(ctx, td)
	return s.cfg.Traces.ExportTraces(ctx, td)
}

// rejectTraces consults the receive-path guard (ServerConfig.RejectTraces). It
// is the FIRST step of both trace handlers and the last one that may run before
// EnrichTraces: a payload this refuses must cost no metadata lookup and move no
// ingest counter. An unset guard accepts everything.
func (s *Server) rejectTraces(ctx context.Context, td ptrace.Traces) error {
	if s.cfg.RejectTraces == nil {
		return nil
	}
	if err := s.cfg.RejectTraces(ctx, td); err != nil {
		return receiveRefusal{err}
	}
	return nil
}

const maxIngestBody = 16 << 20 // 16 MiB per request

// maxIngestGRPCMessage caps ONE decoded gRPC message (grpc-go's own default,
// stated here because the tap reserves exactly this much per push).
const maxIngestGRPCMessage = 4 << 20

// maxGRPCFrameBytes is the largest message gRPC's framing can carry: its length
// prefix is a uint32. NewServer clamps ServerConfig.MaxRecvBytes to it.
const maxGRPCFrameBytes = math.MaxUint32
