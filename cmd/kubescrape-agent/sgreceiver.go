package main

// The trace tier's INTERNAL receiver (sgReceiver): the bearer-authenticated
// port sibling shards forward their share of a push to. The application-facing
// ports are an otlpingest.Server, built in servicegraph.go.

import (
	"context"
	"errors"
	"log/slog"
	"math"
	"net/http"
	"time"

	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/tap"

	"github.com/JohanLindvall/kubescrape/internal/agent/otlpingest"
	"github.com/JohanLindvall/kubescrape/internal/bearer"
	"github.com/JohanLindvall/kubescrape/internal/logdedupe"
	"github.com/JohanLindvall/kubescrape/pkg/otlpsplit"
)

// sgReceiver is the tier's internal intake: a TRACES-ONLY OTLP receiver, gRPC on
// -service-graph-listen plus optional OTLP/HTTP protobuf on
// -service-graph-http-listen, both behind the shared bearer token.
//
// It is the port that says "this payload is final". What arrives here has
// already been enriched and routed by the shard that received it from an
// application, so this path never enriches (the peer is a sibling shard, not the
// sender) and never re-shards (we are the owner). Both are structural: there is
// no Enricher and no Resharder in this path at all.
//
// # Why not internal/agent/otlpingest
//
// That receiver is what the APPLICATION listeners use, and this one is its
// opposite on the two axes that matter. It is unauthenticated, because its
// senders are every instrumented pod in the cluster; this one must
// authenticate, because what it accepts skips enrichment and routing and a
// forged payload would be exported unattributed. And it enriches by peer
// address, which is exactly the thing that is meaningless here. Reusing it
// would have meant bolting an auth mode and a skip-enrichment mode onto the
// ingest path so neither could be changed without re-reasoning about the other.
//
// The SERVER is separate; the HTTP request seam is NOT. Reading a body, mapping
// a read failure to a status and mapping a forward failure to one are the same
// decisions on both ports and are shared (otlpingest.BodyReader,
// WriteBodyError, GRPCForwardStatus, HTTPForwardStatus) — this file's copies of
// them had already drifted: an over-cap gzip answered 400 "malformed" here and
// 413 there, and 400 tells a kubescrape sender to DROP the batch.
//
// No in-flight shed here, for the reason the application listener needs one:
// that one holds a slot for as long as the whole owner chain takes (a
// re-shard hop plus the collector's ack, up to -otlp-timeout), from senders it
// cannot identify. This one is authenticated and shorter — it runs the owner
// chain, whose one blocking step is the collector export — and the gRPC message
// cap below bounds what a single request can allocate.
type sgReceiver struct {
	grpcAddr string
	httpAddr string
	// tokens is the accepted set, re-read and rotation-aware
	// (bearer.Rotating.Tokens). A due refresh claims one token-file read, which
	// runs on its own goroutine, and the caller waits for it at most a bounded
	// tenth of a second (bearer's refreshWait). Even that is too long to hold
	// the transport's mutex for, so only the per-RPC interceptor and the
	// per-request HTTP handler call it; the tap reads cached.
	tokens func() []string
	// cached is the same set WITHOUT the refresh (bearer.Rotating.Cached, kept
	// current by Rotating.Run): what the gRPC auth tap reads, because the tap
	// runs on the connection's frame-read goroutine with the transport's mutex
	// held, and a refresh claimed there on a wedged token mount froze every
	// stream on that sibling's connection.
	cached func() []string
	// consume runs the owner chain (strip the marker, pair, RED metrics, sample,
	// export). Safe to call from the concurrent handler goroutines. It RETURNS AN
	// ERROR, and that error becomes the sending shard's — which becomes the
	// application's, whose retry is the only thing standing between a failed
	// export and a lost span.
	consume func(context.Context, ptrace.Traces) error
	ready   func()
	log     *slog.Logger

	// body reads one OTLP/HTTP body under sgMaxRecvBytes. Lazily built by Run
	// so a zero-value sgReceiver (tests construct one directly) still works.
	body *otlpingest.BodyReader

	// warnGate throttles the rejected-push log; see warnUnauthorized.
	warnGate logdedupe.Throttle
}

// sgAuthRealm is sent on 401s so a client can tell "wrong credentials" from
// "wrong URL".
const sgAuthRealm = `Bearer realm="kubescrape service-graph"`

// sgUnauthorizedMsg is the internal receiver's refusal, spelled once for the
// gRPC tap, the gRPC interceptor and the HTTP handler alike: three sites
// re-typing it is how one drifts into naming a different flag.
const sgUnauthorizedMsg = "missing or invalid bearer token (-service-graph-token-file)"

// msgShardNoListener is the "shard would receive nothing" refusal, spelled
// ONCE: validateConfig raises it so -check-config catches the config before
// the StatefulSet CrashLoops, and sgReceiver.Run raises it again at the real
// start. One const is what keeps the two paths' wording from drifting.
const msgShardNoListener = "-service-graph is set but -service-graph-listen (and -service-graph-http-listen) are empty: the shard would receive nothing"

// sgMaxRecvFloor is the split-cap term's lower bound when the sender's split
// size is unknown or small: the historical 4 MiB, matching a collector's own
// default. (The application-push term in sgMaxRecvBytes is larger today, so
// this binds only if that one ever shrinks below it.) Derived
// with the sender's default split cap as a lower bound so a future raise of
// otlpsplit.DefaultMaxBytes can never silently out-size the internal hop's
// receive floor — today DefaultMaxBytes is 3.75 MiB, so the value is
// unchanged at 4 MiB.
const sgMaxRecvFloor = max(4<<20, otlpsplit.DefaultMaxBytes)

// sgEnrichHeadroom is what the internal hop allows ON TOP of the largest
// application push for the entry shard's enrichment: the k8s resource
// attributes it adds to the one resource an unsplittable part carries (a single
// span over the split cap ships alone, and an abandoned split ships one
// resource's remainder — otlpsplit), which is bounded by one pod's metadata
// rendering rather than by anything the sender controls.
const sgEnrichHeadroom = 1 << 20

// sgMaxRecvBytes caps one decoded payload on the internal hop.
//
// It must be at least what the SENDING shard will produce, and the sender is
// another kubescrape splitting at -otlp-max-send-bytes. That flag is the
// operator's, tuned for the COLLECTOR's receive limit — so pinning this at a
// constant meant raising it for a collector that accepts 8 MiB silently made
// every over-4-MiB shard-to-shard payload fail, breaking the ring for exactly
// the large traces the raise was for. Derived from the same flag instead, with
// the floor as a lower bound so a small or unset value cannot shrink it.
//
// And a split cap is NOT an upper bound on a part: otlpsplit ships a single
// leaf over the cap alone, so one span larger than -otlp-max-send-bytes leaves
// the entry shard as one oversized part. Its size is bounded instead by what
// the APPLICATION ports admitted (16 MiB OTLP/HTTP, -ingest-grpc-max-recv-bytes
// on gRPC — otlpingest.MaxPushBytes) plus the enrichment the entry shard added.
// Sized from the split cap alone, the hop refused that span when a SIBLING
// owned its trace while the same span owned locally reached a collector whose
// limit had been raised — delivery depending on the trace id. So the hop
// accepts whatever entered the ring. The memory this grants is bounded
// elsewhere: the port is authenticated, and a sibling forwards only what its
// own byte-budgeted entry listener admitted.
//
// A NEGATIVE flag disables splitting outright, so "what the sender will
// produce" stops being a split cap and becomes the whole enriched payload —
// whose enrichment grows it by its resource count, so no constant this side
// can name bounds it. The receive cap is therefore disabled WITH the splitting:
// they are one decision, and the flag spells it. Reading the negative form as
// "use the floor" instead sent unsplit shares into a sibling capped at 4 MiB —
// every large trace deterministically rejected on the ring, a rejection
// otlpingest.GRPCForwardStatus hands the application as retryable, so its SDK
// re-pushed an undeliverable payload until the retry budget dropped the spans,
// with only SendsFailed moving.
func sgMaxRecvBytes() int {
	ring := otlpingest.MaxPushBytes(*ingestGRPCMaxRecv) + sgEnrichHeadroom
	n := *otlpMaxSendBytes
	if n < 0 {
		// grpc-go's own ceiling; the internal hop's BodyReader shares it (Run).
		// max() only for an application gRPC cap raised past it.
		return max(math.MaxInt32, ring)
	}
	return max(sgMaxRecvFloor, n, ring)
}

// sgWarnEvery throttles the rejected-push warning: a fleet pointed at the
// wrong token would otherwise write one line per forwarded batch — thousands a
// second — burying the diagnosis in its own symptom.
const sgWarnEvery = 30 * time.Second

// Run serves until ctx is cancelled. A runtime listener failure propagates to
// the caller (fatal there); a cancelled shutdown returns nil.
//
// The bind-then-ready-then-serve-then-drain skeleton is otlpingest.Listeners —
// the SERVERS stay this receiver's own (the auth tap and interceptor on gRPC,
// the bearer check in the HTTP handler, no in-flight shed — see the type doc),
// only the run shape is shared. This file's hand-rolled copy of that shape is
// how the keepalive policy below drifted in the first place.
func (r *sgReceiver) Run(ctx context.Context) error {
	if r.body == nil {
		r.body = otlpingest.NewBodyReader(int64(sgMaxRecvBytes()))
	}
	if r.grpcAddr == "" && r.httpAddr == "" {
		// A shard with no listener pairs nothing, and would report ready and
		// idle forever. Refuse instead — indistinguishable-from-working is the
		// failure mode this whole feature's counters exist to avoid. (Refused
		// HERE, because Listeners.Run treats nothing-configured as a no-op.)
		return errors.New(msgShardNoListener)
	}

	l := otlpingest.Listeners{Name: "service-graph internal", Logger: r.log, Ready: r.ready}
	if r.grpcAddr != "" {
		l.GRPCAddr = r.grpcAddr
		l.GRPC = grpc.NewServer(
			// Reap connections a peer opened and abandoned, and bound a
			// socket's AGE (otlpingest.KeepaliveOption, the policy every
			// kubescrape OTLP receiver shares). The MaxConnectionAge/AgeGrace
			// half is a deliberate behavior change with this adoption: this
			// port used to set only MaxConnectionIdle — a documented drift, an
			// authenticated peer's abandoned stream had no age bound, since a
			// connection carrying an open stream is never idle.
			otlpingest.KeepaliveOption(),
			grpc.MaxRecvMsgSize(sgMaxRecvBytes()),
			// The header-block bound (otlpingest.MaxHeaderListSizeOption).
			// This hop authenticates, but the credential arrives IN the header
			// block, so grpc-go has already decoded 16 MiB of it — at the
			// default — by the time the tap can read the token.
			otlpingest.MaxHeaderListSizeOption(),
			// The wire-SHAPE guard, which MaxRecvMsgSize cannot give: pdata's
			// generated unmarshaller recurses per nesting level, so a small
			// message of deeply nested groups costs unbounded goroutine stack
			// and only the codec — which runs before the decode — can refuse
			// it. Every OTLP gRPC listener in this repo carries it; this one
			// assembles its own grpc.Server, so it takes the option form.
			//
			// nil onRefused, deliberately: this hop is authenticated
			// kubescrape-to-kubescrape, and the refusal series means "an
			// APPLICATION push was refused at a listener nothing
			// authenticates" — the same reason NewBodyReader counts nothing
			// here.
			otlpingest.NestingGuardOption(nil),
			// Authenticate on the HEADERS frame, BEFORE grpc-go reads the
			// message. A UnaryInterceptor runs only after recvAndDecompress has
			// pulled the whole thing into memory, so an unauthenticated peer
			// could make this process allocate sgMaxRecvBytes per stream and be
			// refused afterwards — the credential bought nothing it was there
			// to buy. tap.Info carries the request headers (grpc/tap/tap.go),
			// which is exactly what the check needs, and the ingest server
			// already uses a tap for its byte budget for the same reason.
			grpc.InTapHandle(r.authTap),
			// A cap on concurrent streams PER CONNECTION. grpc-go's default is
			// math.MaxUint32, so without it one authenticated connection could
			// hold unbounded concurrent decodes; this receiver has no in-flight
			// semaphore of its own (its senders are sibling shards, not
			// arbitrary applications).
			grpc.MaxConcurrentStreams(sgMaxConcurrentStreams),
			grpc.UnaryInterceptor(r.authUnary),
		)
		ptraceotlp.RegisterGRPCServer(l.GRPC, &sgTraces{r: r})
	}
	if r.httpAddr != "" {
		mux := http.NewServeMux()
		mux.HandleFunc("POST /v1/traces", r.handleHTTPTraces)
		// The shared push-server shape: Slowloris header bound, trickled-body
		// bound, keep-alive reaping, and deliberately no WriteTimeout (its
		// clock would race a slow but legal upload).
		l.HTTP = otlpingest.NewPushHTTPServer(r.httpAddr, mux)
	}
	// Graceful on shutdown: in-flight forwards are already-paid-for spans, and
	// pairing them costs microseconds.
	return l.Run(ctx)
}

// sgMaxConcurrentStreams bounds concurrent RPCs per connection on the internal
// hop. The senders are sibling shards issuing one synchronous forward per push,
// so this is far above the working set; it exists so a single connection cannot
// pin an unbounded number of in-flight sgMaxRecvBytes decodes.
const sgMaxConcurrentStreams = 64

// authorized reports whether the metadata carries an accepted bearer token.
//
// gRPC lower-cases metadata keys and otlpexport sends `authorization: Bearer
// <token>` (otlpexport.grpcAuth), which is the same header its HTTP arm sets —
// one credential, two transports.
func authorized(md metadata.MD, tokens []string) bool {
	for _, v := range md.Get("authorization") {
		if bearer.Authorized(v, tokens) {
			return true
		}
	}
	return false
}

// authTap rejects an unauthenticated push on the HEADERS frame, before grpc-go
// reads (and allocates) the message. It runs in the transport's I/O goroutine
// with its mutex held, so it must not block — and it reads the accept set
// through cached, never tokens: Rotating.Tokens re-reads the file on its caller
// once the refresh interval has lapsed, and on a wedged token mount that read
// never returns, which froze every stream on the sibling's connection until the
// mount recovered or MaxConnectionAge reaped it. The set is kept current by
// Rotating.Run (startServiceGraph), at the refresh cadence, so a sibling that
// rotated first is refused here for at most about a second.
// The returned context BECOMES the stream's context (http2Server.operateHeaders
// assigns it), so the success path must hand back the one it was given —
// returning nil leaves the stream with no context at all.
func (r *sgReceiver) authTap(ctx context.Context, info *tap.Info) (context.Context, error) {
	if info != nil && authorized(info.Header, r.cached()) {
		return ctx, nil
	}
	r.warnUnauthorized("grpc")
	return nil, status.Error(codes.Unauthenticated, sgUnauthorizedMsg)
}

// authUnary re-checks the token after decode. The tap above is what actually
// keeps an unauthenticated peer from spending memory; this stays as the second
// line, so a future grpc-go that stopped running taps (they are marked
// experimental) could not silently open the listener.
func (r *sgReceiver) authUnary(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if authorized(md, r.tokens()) {
		return handler(ctx, req)
	}
	r.warnUnauthorized("grpc")
	return nil, status.Error(codes.Unauthenticated, sgUnauthorizedMsg)
}

// warnUnauthorized logs a rejected push at most once per sgWarnEvery. Silence
// would be worse than noise here: a token mismatch after a botched rotation
// produces no other symptom on either side — the agents' forwards "fail" into
// a counter, and the graph is simply empty.
func (r *sgReceiver) warnUnauthorized(transport string) {
	if !r.warnGate.Allow(sgWarnEvery) {
		return
	}
	r.log.Warn("rejected a service-graph push with a missing or invalid bearer token; the agents' -service-graph-token-file must match this shard's",
		"transport", transport)
}

// sgTraces is the gRPC trace service. Only traces are registered: logs and
// metrics on this port would be an unhandled method, which is the honest
// answer — this listener exists to pair spans.
type sgTraces struct {
	ptraceotlp.UnimplementedGRPCServer
	r *sgReceiver
}

func (g *sgTraces) Export(ctx context.Context, req ptraceotlp.ExportRequest) (ptraceotlp.ExportResponse, error) {
	// The ack is honest only if the whole owner chain succeeded: the sending
	// shard holds no copy after we answer, and its own sender is the only thing
	// that can produce these spans again. An error travels back to the
	// application, whose retry re-pushes the identical batch — which is safe
	// because the taps count only after a successful export.
	//
	// The error is mapped by the ingest receiver's own classification
	// (otlpingest.GRPCForwardStatus), not a copy: a bare error surfaces as
	// codes.Unknown, which otlpexport reads as NON-permanent — fine — but a
	// genuinely permanent upstream rejection has to stay permanent, or the
	// sending shard's application retries a payload nothing will ever accept.
	// The two receivers must answer the same way, or the same collector failure
	// reads as retryable on one port and permanent on the other.
	if err := g.r.consume(ctx, req.Traces()); err != nil {
		return ptraceotlp.ExportResponse{}, otlpingest.GRPCForwardStatus(err)
	}
	return ptraceotlp.NewExportResponse(), nil
}

func (r *sgReceiver) handleHTTPTraces(w http.ResponseWriter, req *http.Request) {
	if !bearer.Authorized(req.Header.Get("Authorization"), r.tokens()) {
		r.warnUnauthorized("http")
		w.Header().Set("WWW-Authenticate", sgAuthRealm)
		w.Header().Set("Cache-Control", "no-store")
		http.Error(w, sgUnauthorizedMsg, http.StatusUnauthorized)
		return
	}
	// otlpingest owns the body reader for BOTH receivers. This one used to
	// have its own copy, and the fix that makes an over-cap GZIP report 413
	// instead of 400 "malformed" landed only in the other — on the one hop
	// whose sender is another kubescrape, whose exporter reads 400 as PERMANENT
	// and drops the batch. The CAP is the parameter (sgMaxRecvBytes here — at
	// least the largest application push plus enrichment headroom — 16 MiB for
	// application pushes); the byte budget is deliberately absent, as is the
	// in-flight semaphore — see the type doc. So is the door COUNTER: this
	// process serves the unauthenticated application ports too, and
	// kubescrape_ingest_body_rejected_total means "an application push was
	// refused at a listener nothing authenticates" (otlpingest.NewBodyReader).
	body, charged, err := r.body.Read(req)
	if err != nil {
		otlpingest.WriteBodyError(w, err)
		return
	}
	defer r.body.Release(charged)
	er := ptraceotlp.NewExportRequest()
	if err := er.UnmarshalProto(body); err != nil {
		http.Error(w, "malformed OTLP traces payload", http.StatusBadRequest)
		return
	}
	if err := r.consume(req.Context(), er.Traces()); err != nil {
		// The HTTP counterpart of the gRPC arm's GRPCForwardStatus: a permanent
		// upstream rejection is 400 (do not retry this batch), everything else
		// 503 (retryable). The sending shard's exporter reads both correctly.
		http.Error(w, err.Error(), otlpingest.HTTPForwardStatus(err))
		return
	}
	otlpingest.WriteProto(w, ptraceotlp.NewExportResponse())
}
