package otlpingest

// Admission control for the unauthenticated ingest listeners.
//
// There are THREE resources to bound, and no one bound covers another:
//
//   - PROCESSING (Server.inFlight, -ingest-max-in-flight): enrichment, the
//     forward, and the wall-clock a slot is held for — as long as the
//     collector takes to ack.
//   - BUFFERING (byteBudget below): the RAW payload bytes a request holds
//     while it is being read off the wire and decoded, before any of that
//     processing starts.
//   - the DECODED pdata (decodedBudget below): what the bytes inflate INTO,
//     which is neither of the other two and is the one that decides whether
//     the container lives.
//
// The third was missing, and this file used to claim the count covered it
// ("the far more expensive resource (inflated pdata)"). It does not: a COUNT
// cannot bound a size. Measured through the real HTTP handler, a legal
// 15.99 MiB logs body of 578 000 minimal ResourceLogs (40 KiB gzipped)
// inflates 16x, to ~256 MiB of live heap; the byte budget deliberately admits
// FOUR full-size bodies, so the shape it is designed to allow is ~1 GiB of
// heap on a pod the chart limits to 512Mi — an OOM-kill of the node's whole
// agent, bought with ~160 KiB of unauthenticated traffic, repeatable into
// CrashLoopBackOff. The inflation is a property of SHAPE, not of size: the
// same 16 MiB carrying a realistic 60 000-record batch inflates 1.12x.
//
// The count bound alone does not bound buffering. The HTTP handlers read the
// whole body (up to maxIngestBody) BEFORE taking a slot — deliberately, because
// holding one of 32 slots across a trickled 16 MiB upload let a handful of
// senders shed everyone else on the node with 429 for a whole ReadTimeout,
// which is the exact denial-of-service the bound exists to prevent. So the read
// must stay outside the slot, and the memory it accumulates needs its own,
// wider bound. The gRPC arm has the same shape one layer down: grpc-go decodes
// the message before the unary interceptor runs, so the semaphore never sees a
// push until its bytes are already resident, and nothing caps how many
// connections do that at once (MaxConcurrentStreams is PER CONNECTION).
//
// The budgets are global across both transports because the memory is: one
// process, one heap. A refusal is always RETRYABLE (429 + Retry-After, or
// ResourceExhausted carrying RetryInfo) — the sender still holds the payload,
// which is what makes shedding better than accepting it and running the node
// out of memory. Bounding by connection count instead (a limit listener) was
// rejected for the same reason the slot was moved off the read path: it sheds
// on the wrong axis, punishing a hundred idle keep-alives and letting four busy
// ones allocate without limit.
//
// WHAT THE DECODED BUDGET CHARGES, and what it deliberately leaves to the other
// two, since the split is the whole design (decodedLogsSize and its two
// siblings, one per signal, in decodedsize.go):
//
//   - STRUCTURE — every object the decode allocates that is not a copy of wire
//     bytes: a resource, a scope, a record/point/span, and below them every
//     KeyValue, array element, value wrapper, exemplar, span event and link,
//     quantile, entity ref and varint bucket count — is charged here, because
//     it is exactly what the raw budget cannot see: 30 wire bytes can mint a
//     ResourceLogs, TWO can mint a 40-byte KeyValue, and the ratio between the
//     two is unbounded from the receiver's side. A KeyValue is structure, not
//     content. This comment used to file everything below the item under
//     CONTENT, and the estimate followed it: one 16 MiB push of empty
//     attributes (~16 KB gzipped) decoded to 385 MiB of live heap charged
//     512 B, and exemplars, span events and links amplify ~40x.
//   - CONTENT (the strings and byte slices a decode copies out of the body) is
//     NOT charged here, because it is bounded transitively and charging it
//     twice would shed honest senders: on HTTP the raw body is charged to the
//     byte budget for the SAME lifetime (the release is deferred to the
//     handler's return), and a copied string costs at most ~2x the wire bytes
//     it came from (a two-byte string is four wire bytes and one 8-byte
//     allocation; the runtime interns one-byte strings), ~1.8x in practice, so
//     64 MiB of admitted body is ~115 MiB of strings. On gRPC the message
//     buffer is freed by the codec at decode, and what remains is bounded by
//     the in-flight count times the per-message cap (32 x 4 MiB, ~230 MiB of
//     strings in the worst case) — the residual this pair of bounds does not
//     tighten, and the reason limitUnary takes the slot BEFORE handing the
//     reservation over rather than after.
//
// WHEN AND WHERE THE CHARGE IS TAKEN: from the WIRE bytes, BEFORE the decode.
// It used to be taken after, from a walk over the decoded pdata, which bounded
// how long a payload stayed resident and nothing about its peak — four pushes
// the budget refused were still four decoded payloads on the heap together.
// On HTTP the charge is servePush's, between the read and the unmarshal. On
// gRPC it is the codec's (depthGuardCodec.Unmarshal), the only code grpc-go
// runs between receiving a message and decoding it. The codec cannot answer a
// refusal itself — grpc-go rewrites every codec error to codes.Internal, which
// a conformant sender reads as PERMANENT — so an over-budget message is left
// UNDECODED and the verdict travels to the unary interceptor (limitUnary)
// through decodedClaims, which answers the retryable ResourceExhausted +
// RetryInfo and otherwise holds the admitted charge until the handler returns.
// The claim is keyed by the message's IDENTITY because its TYPE cannot be
// named: grpc-go decodes into pdata's unexported
// *internal.ExportLogsServiceRequest (the public wrapper is built one layer
// further in, by rawLogsServer.Export), which is why a type switch in the
// interceptor once charged nothing at all.

import (
	"errors"
	"io"
	"net/http"
	"strconv"
	"sync/atomic"
	"time"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/JohanLindvall/kubescrape/internal/obs"
	"github.com/JohanLindvall/kubescrape/internal/peerip"
)

// maxBufferBytes bounds the raw payload bytes both transports may hold at once,
// at four full-size requests. It is a floor, not a flag: it must be at least
// maxIngestBody or a single legal request could never be admitted, and
// NewServer raises it in step with a raised ServerConfig.MaxRecvBytes for the
// same reason. It bounds RAW bytes and nothing else — what those bytes inflate
// into is the decoded budget's (decodedLogsSize and its siblings), and the
// count an operator tunes with -ingest-max-in-flight bounds neither.
//
// What is charged is the PAYLOAD as it is read. The buffer it lands in grows by
// doubling past a small pre-sized head (readAllCapped), so its peak can briefly
// reach ~2x its charge; the four-body headroom is sized with that in mind. It is
// deliberately NOT pre-sized from Content-Length — see maxPresizeBytes.
const maxBufferBytes = 4 * maxIngestBody

// budgetGranule is the top-up size for an in-progress read: a trickled body
// touches the shared counter once per 64 KiB rather than once per Read.
const budgetGranule = 64 << 10

// decodedBudgetFactor sizes the decoded-structure budget from the raw one:
// TWICE the bytes this receiver will hold. The number is a memory decision made
// against the shipped limit (charts/kubescrape/values.yaml and deploy/agent.yaml
// both set 512Mi): 64 MiB of raw bodies + ~115 MiB of the strings they decode
// into + 128 MiB of charged structure leaves the tailer, the scrapers and Go's
// own slack the rest. Raising ServerConfig.MaxRecvBytes scales it, as it scales
// the raw budget, because a receiver told to accept bigger messages was told to
// hold more of everything.
//
// The cost of the bound, stated plainly: a SINGLE push whose structure alone
// estimates past the whole budget can never be admitted, and is answered the
// same retryable refusal (there is no permanent answer that is safe here — a
// shed that loses data is worse than the OOM it prevents, and the estimate is a
// model, not a measurement). At decodedsize.go's coefficients that takes
// ~130 000 minimal resources, ~500 000 bare records or ~1.6 million attributes
// in ONE push, an order of magnitude past what a batching SDK emits; the sender
// must split. It is warned about, once a minute,
// because "every push from this sender is 429" is otherwise indistinguishable
// from ordinary back-pressure.
const decodedBudgetFactor = 2

// errBufferBudget is the refusal: retryable, and mapped to 429 + Retry-After by
// BodyErrorStatus / WriteBodyError.
var errBufferBudget = errors.New("receiver is holding its maximum buffered payload bytes; retry")

// errDecodedBudget is its sibling one layer in: the bytes were admitted, and
// what they decode to does not fit. Same shape of answer for the same reason —
// the sender still holds the payload.
var errDecodedBudget = errors.New("receiver is holding its maximum decoded payload; retry")

// errInFlight is the COUNT bound's refusal (-ingest-max-in-flight), spelled once
// so both transports describe it identically: one condition, one description,
// whichever transport a sender used (exhaustedStatus on gRPC, writeShed on
// HTTP). They had drifted by a word.
var errInFlight = errors.New("too many concurrent pushes; retry")

// byteBudget is a non-blocking counting semaphore over bytes. Reserving is
// add-then-check-then-undo, so concurrent reservers can transiently overshoot
// the limit by at most (concurrent reservers x their sizes) before backing out —
// bounded, and cheaper than a mutex on a path that runs per read.
type byteBudget struct {
	used  atomic.Int64
	limit int64
}

func (b *byteBudget) reserve(n int64) bool {
	if b.used.Add(n) > b.limit {
		b.used.Add(-n)
		return false
	}
	return true
}

func (b *byteBudget) release(n int64) {
	if n > 0 {
		b.used.Add(-n)
	}
}

// budgetReader charges the budget for the bytes it reads, so a request is
// refused when the memory it is accumulating no longer fits — mid-upload if
// necessary. Charging as-you-read (rather than reserving the 16 MiB cap up
// front) is what keeps a hundred small senders from being refused on behalf of
// bytes they were never going to send.
//
// A NIL budget passes bytes through uncharged: the trace tier's internal
// receiver bounds its senders by authentication and a 4 MiB message cap and has
// no budget at all (see httpbody.go), and a receiver without one must read, not
// panic.
type budgetReader struct {
	r    io.Reader
	b    *byteBudget
	held int64 // reserved from the budget so far (the caller releases it)
	used int64 // bytes actually read
}

func (br *budgetReader) Read(p []byte) (int, error) {
	n, err := br.r.Read(p)
	if n > 0 && br.b != nil {
		br.used += int64(n)
		if br.used > br.held {
			want := br.used - br.held
			if rem := want % budgetGranule; rem != 0 {
				want += budgetGranule - rem
			}
			if !br.b.reserve(want) {
				// The bytes already read are still charged (held) and the
				// caller releases them; the payload itself is dropped.
				return n, errBufferBudget
			}
			br.held += want
		}
	}
	return n, err
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
		obs.IngestRejected.WithLabelValues(shedInFlight).Inc()
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
	obs.IngestRejected.WithLabelValues(shedDecoded).Inc()
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

// writeShed answers an HTTP push refused by an admission bound: 429 with
// Retry-After, the retryable answer the sender keeps its payload for — the HTTP
// half of exhaustedStatus, and the one spelling of it for all three bounds
// (servePush's two, and WriteBodyError's byte-budget arm).
func writeShed(w http.ResponseWriter, err error) {
	w.Header().Set("Retry-After", "1")
	http.Error(w, err.Error(), http.StatusTooManyRequests)
}

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
		args = append(args, "peer", peerip.ForLog(peer))
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
//
// It reads the RESOLVED budget rather than re-deriving NewServer's formula: the
// budget is above the floor exactly when the receive cap raised it, and a
// second copy of the factor would name the wrong source the day one of the two
// changed.
func (s *Server) budgetSourceArgs() []any {
	if s.buffer.limit > int64(maxBufferBytes) {
		return []any{"flag", "-ingest-grpc-max-recv-bytes", "recvBytes", s.grpcMaxRecv}
	}
	return []any{
		"recvBytes", s.grpcMaxRecv,
		"floorBytes", int64(maxBufferBytes),
		"note", "the budget is at its built-in floor; -ingest-grpc-max-recv-bytes raises it only once set above " +
			strconv.Itoa(maxBufferBytes/4) + " bytes, so shrink the senders' batches instead",
	}
}
