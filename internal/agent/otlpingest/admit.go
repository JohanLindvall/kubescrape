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
// siblings, one per signal — there is deliberately no `decodedSize(any)`
// dispatcher: see the note on WHERE the charge is taken, below):
//
//   - STRUCTURE — one heap object per resource, scope, record/point/span — is
//     charged here, because it is exactly what the raw budget cannot see: 30
//     wire bytes can mint a ResourceLogs, and the ratio between the two is
//     unbounded from the receiver's side.
//   - CONTENT (the strings a decode copies out of the body) is NOT charged
//     here, because it is bounded transitively and charging it twice would
//     shed honest senders: on HTTP the raw body is charged to the byte budget
//     for the SAME lifetime (the release is deferred to the handler's return),
//     and content measures ~1.8x the wire bytes it came from, so 64 MiB of
//     admitted body is ~115 MiB of strings. On gRPC the message buffer is
//     freed by the codec at decode, and what remains is bounded by the
//     in-flight count times the per-message cap (32 x 4 MiB, ~230 MiB of
//     strings in the worst case) — the residual this pair of bounds does not
//     tighten, and the reason limitUnary now takes the slot BEFORE handing the
//     reservation over rather than after.
//
// WHERE THE CHARGE IS TAKEN, and why it is not the obvious place. On HTTP it is
// servePush, which has the typed request because it did the unmarshal itself. On
// gRPC it is the three Export methods (server.go), NOT the unary interceptor:
// grpc-go hands an interceptor the message its GENERATED handler decoded into,
// which for pdata is *pdata/internal.ExportLogsServiceRequest — the public
// plogotlp.ExportRequest wrapper is built one layer further in, by
// rawLogsServer.Export. That type lives in pdata's internal/, so a type switch
// in the interceptor can never name it: the charge that used to live there
// matched nothing, reserved nothing, and left a decoded gRPC payload bounded by
// the in-flight COUNT alone — the exact thing the top of this file says a count
// structurally cannot bound. Charge where the typed request exists.

import (
	"context"
	"errors"
	"io"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"google.golang.org/grpc/tap"

	"github.com/JohanLindvall/kubescrape/internal/obs"
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
// model, not a measurement). At the coefficients below that takes ~130 000
// resources or ~500 000 records in ONE push, an order of magnitude past what a
// batching SDK emits; the sender must split. It is warned about, once a minute,
// because "every push from this sender is 429" is otherwise indistinguishable
// from ordinary back-pressure.
const decodedBudgetFactor = 2

// What one decoded object costs on the heap, measured (Go 1.26, pdata v1.65) as
// the live-heap delta of UnmarshalProto with the result kept alive:
//
//	logs    200k resources x 1 tiny record   8.0 MB wire -> 97.9 MB  (489 B/resource-chain)
//	logs    1 resource x 400k tiny records   4.4 MB wire -> 61.3 MB  (153 B/record)
//	logs    60k records, 200 B body, 4 attrs 17.6 MB wire -> 38.9 MB (648 B/record, mostly content)
//	metrics 200k resources x 1 point        11.0 MB wire -> 131.5 MB (657 B/resource-chain)
//	metrics 1 resource x 400k points         8.8 MB wire -> 74.1 MB  (185 B/point)
//	traces  200k resources x 1 span         17.8 MB wire -> 139.5 MB (697 B/resource-chain)
//	traces  1 resource x 200k spans         12.0 MB wire -> 72.3 MB  (361 B/span)
//
// The coefficients round those UP, and deliberately so on the resource term:
// resources are the multiplier a hostile sender reaches for (30 wire bytes
// each), records are what an honest one has a lot of, and an estimate that is
// generous where the attack lives and tight where the traffic lives sheds the
// right one. They are a MODEL of a shape that varies by an order of magnitude,
// not a measurement of any particular payload — which is why the refusal they
// drive stays retryable.
const (
	decodedResourceBytes = 512
	decodedScopeBytes    = 256
	decodedItemBytes     = 256
)

// decodedLogsSize and its two siblings estimate the STRUCTURAL heap of a
// decoded payload: one charge per resource, per scope, and per record / data
// point / span. Content is charged by nobody here, on purpose — see the file
// comment. There is one per SIGNAL and no `any`-taking dispatcher over them,
// because the only caller that would have needed one — the gRPC unary
// interceptor — is handed a type it can never name (see limitUnary).
//
// It walks resources and scopes (never the items themselves), which is the same
// walk plog.Logs.LogRecordCount already does and costs 3.5 ms for a hostile
// 600 000-node payload against the ~170 ms that payload's decode took. Counting
// SCOPES rather than trusting a per-resource constant is not tidiness: one
// resource holding 500 000 empty ScopeLogs is a legal payload whose item count
// is zero.
func decodedLogsSize(ld plog.Logs) int64 {
	rls := ld.ResourceLogs()
	n := int64(rls.Len()) * decodedResourceBytes
	for i := 0; i < rls.Len(); i++ {
		sls := rls.At(i).ScopeLogs()
		n += int64(sls.Len()) * decodedScopeBytes
		for j := 0; j < sls.Len(); j++ {
			n += int64(sls.At(j).LogRecords().Len()) * decodedItemBytes
		}
	}
	return n
}

func decodedMetricsSize(md pmetric.Metrics) int64 {
	rms := md.ResourceMetrics()
	n := int64(rms.Len()) * decodedResourceBytes
	for i := 0; i < rms.Len(); i++ {
		sms := rms.At(i).ScopeMetrics()
		n += int64(sms.Len()) * decodedScopeBytes
		for j := 0; j < sms.Len(); j++ {
			ms := sms.At(j).Metrics()
			// A metric SHELL is charged like an item: a payload of a million
			// point-less metrics is legal (emptymetrics.go prunes them, but
			// only after they are resident).
			n += int64(ms.Len()) * decodedItemBytes
			for k := 0; k < ms.Len(); k++ {
				n += int64(metricPointCount(ms.At(k))) * decodedItemBytes
			}
		}
	}
	return n
}

func decodedTracesSize(td ptrace.Traces) int64 {
	rss := td.ResourceSpans()
	n := int64(rss.Len()) * decodedResourceBytes
	for i := 0; i < rss.Len(); i++ {
		sss := rss.At(i).ScopeSpans()
		n += int64(sss.Len()) * decodedScopeBytes
		for j := 0; j < sss.Len(); j++ {
			n += int64(sss.At(j).Spans().Len()) * decodedItemBytes
		}
	}
	return n
}

// One gRPC push reserves Server.grpcMaxRecv before grpc-go reads it. The size
// of the message is not knowable at that point — the tap runs on the HEADERS
// frame — so the reservation is the worst case the transport will accept
// (MaxRecvMsgSize). It is released again as soon as the message is decoded and
// the interceptor takes over, so this bounds concurrent DECODES, not
// concurrent pushes: the reservation is held for microseconds of unmarshal,
// not for the seconds a slow collector holds a processing slot.

// grpcReserveWindow bounds how long ONE reservation may live, and it is the
// difference between a bound and a gift.
//
// The reservation is taken on the HEADERS frame and handed over in the
// interceptor once the message is decoded. A peer that opens a stream and then
// sends NOTHING reaches neither: the interceptor never runs, and the stream
// context that backstops the reservation is not cancelled until the stream ENDS
// — which is the same peer's choice. MaxConnectionIdle does not help, because a
// connection carrying an open stream is not idle. So sixteen headers-only
// streams, from one unauthenticated socket, at zero cost in bytes, pinned the
// whole budget for the process' life and shed gRPC AND HTTP ingest with it.
//
// The window is therefore a DEADLINE ON THE PRE-DECODE READ: armed at HEADERS,
// disarmed the moment the interceptor takes over, so it never runs against the
// far longer time a handler spends waiting for the collector to ack. Expiry
// releases the reservation AND cancels the stream (reservation.expire); the
// cancel is what makes the reclaim honest, since releasing alone would leave
// grpc-go free to decode a message the budget no longer accounts for, and would
// still let the peer re-arm the pin by simply opening another stream. The
// sender sees codes.Canceled, which the OTLP spec lists as retryable, so an
// honest-but-slow sender re-pushes rather than losing data.
//
// 10s to deliver at most MaxRecvMsgSize is 3.4 Mbit/s from a pod on this node
// (or, on the trace tier, from a pod in this cluster) — two orders of magnitude
// of slack — and it is the same clock, on the same question, as the HTTP arm's
// ReadHeaderTimeout: the peer has connected and shown no intent.
//
// It does not make the budget unspendable by a hostile peer: nothing can, on a
// listener with no credentials. It removes the asymmetry, which is the part
// that mattered — spending it now costs a stream open per 4 MiB per 10s, and
// the budget recovers on its own.
const grpcReserveWindow = 10 * time.Second

// errBufferBudget is the refusal: retryable, and mapped to 429 + Retry-After by
// bodyErrorStatus / writeBodyError.
var errBufferBudget = errors.New("receiver is holding its maximum buffered payload bytes; retry")

// errDecodedBudget is its sibling one layer in: the bytes were admitted, and
// what they decode to does not fit. Same shape of answer for the same reason —
// the sender still holds the payload.
var errDecodedBudget = errors.New("receiver is holding its maximum decoded payload; retry")

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

// maxPresizeBytes bounds what an UNVERIFIED size hint may allocate before the
// sender has produced a single byte. Content-Length is the sender's claim, not
// a fact: sizing the destination from it let four idle sockets declaring 16 MiB
// each add 64 MiB of heap while sending nothing. Below this, one allocation
// still covers the overwhelming majority of real OTLP pushes; above it, the
// buffer grows only as the sender proves it is good for the bytes.
const maxPresizeBytes = 64 << 10

// readAllCapped is io.ReadAll with a bounded pre-sized destination. A body that
// fits the pre-sized head lands in ONE allocation instead of the log2(n)
// doublings io.ReadAll performs; past it the buffer doubles, and only the LAST
// step is trimmed to the declared length so a full-size body finishes on an
// exact fit rather than an overshoot. Growth therefore stays proportional to the
// bytes the sender has actually produced — the reason it may not simply jump to
// the declaration is that a peer would then buy the whole allocation with one
// pre-sized head's worth of real bytes, which is the same trade that made
// crediting Content-Length a denial of service. Sizes are grown by one byte
// because the loop needs a final short read to see EOF, and an exactly-sized
// buffer would double for it.
//
// limit is the reader's own cap (16 MiB for application pushes, 4 MiB on the
// trace tier's internal hop) rather than a constant: the two receivers offer
// different limits deliberately.
func readAllCapped(r io.Reader, hint, limit int64) ([]byte, error) {
	if hint > limit {
		// An over-cap declaration is rejected once the read confirms it, but
		// the read still happens: never size past what the LimitReader will
		// hand over.
		hint = limit
	}
	start := hint
	if start > maxPresizeBytes {
		start = maxPresizeBytes
	}
	if start <= 0 {
		start = 511 // unknown length: start where io.ReadAll does
	}
	buf := make([]byte, 0, start+1)
	for {
		if len(buf) == cap(buf) {
			// Double, except for the step that would overshoot a declared
			// length still ahead of us — that one lands exactly on it.
			next := int64(cap(buf)) * 2
			if hint > int64(cap(buf)) && hint < next {
				next = hint
			}
			grown := make([]byte, len(buf), next+1)
			copy(grown, buf)
			buf = grown
		}
		n, err := r.Read(buf[len(buf):cap(buf)])
		buf = buf[:len(buf)+n]
		if err != nil {
			if errors.Is(err, io.EOF) {
				err = nil
			}
			return buf, err
		}
	}
}

// reservation is one gRPC push's budget claim. It is returned by whichever of
// three paths comes first: the interceptor (the fast path — the message is
// decoded and the count bound takes over), the stream context's cancellation
// (every abort path that the peer or the transport drives), and grpcReserveWindow
// elapsing (the peer that drives NEITHER). A leaked reservation sheds the whole
// listener for the process' life, so the last of those is not optional.
//
// held is the interlock: every path claims through the same Swap, so exactly one
// of them ever sees a non-zero value. That is what makes the window safe to add
// — no double release, no negative budget, and no cancel fired at a handler that
// has already been handed over.
type reservation struct {
	b      *byteBudget
	held   atomic.Int64
	cancel context.CancelFunc
	// expired is called by expire, and ONLY by expire, so the window elapsing
	// stays distinguishable from an admission refusal (see there). nil is
	// tolerated: the package's own tests build bare reservations.
	expired func()
	// timer is written after it is armed and read by release, which can run on
	// another goroutine; a nil load simply skips the Stop and leaves a timer
	// whose expire finds held already at zero.
	timer atomic.Pointer[time.Timer]
}

// release returns the reservation without touching the stream: the handover to
// the count bound, or an RPC that ended on its own.
func (r *reservation) release() {
	if n := r.held.Swap(0); n > 0 {
		r.b.release(n)
		if t := r.timer.Load(); t != nil {
			t.Stop()
		}
	}
}

// expire is grpcReserveWindow elapsing with no message decoded. It reclaims the
// bytes and REAPS the stream, so the peer cannot hold a decode window open
// without paying for one, and so nothing is decoded outside the accounting. The
// sender sees codes.Canceled — retryable per the OTLP spec — which is the
// degradation an honest-but-slow sender gets.
//
// It deliberately does NOT count obs.IngestRejected. That counter means one
// thing — a push refused because an admission bound was REACHED, answered
// 429/ResourceExhausted with the payload still in the sender's hands — and it
// is read as "this node cannot keep up with what is being pushed at it". An
// expiry is the opposite shape: the budget had room, and a peer that opened a
// stream and then delivered nothing inside the decode window was reaped. One
// headers-only prober, at zero cost in bytes, could therefore drive the rate
// an operator scales on.
//
// The two causes need different responses, so they are two series — NOT one
// cause made invisible. Separating them by dropping the count would be the
// worse of the two errors this seam can make: the reclaim cancels a peer's
// stream and hands its bytes back, and a listener nothing authenticates is
// exactly where that has to be visible to Prometheus. expired is the seam
// (Server.noteReserveExpired → obs.IngestReserveExpired).
func (r *reservation) expire() {
	if n := r.held.Swap(0); n > 0 {
		r.b.release(n)
		if r.expired != nil {
			r.expired()
		}
		r.cancel()
	}
}

type reservationKey struct{}

// tapAdmit reserves buffer budget for a gRPC push BEFORE grpc-go reads its
// message. tap.ServerInHandle is the only pre-decode hook the server exposes;
// it is marked experimental, and the one property this use depends on —
// that a status returned here reaches the client intact — is real:
// http2Server.writeEarlyAbort emits grpc-status-details-bin whenever the status
// carries details, so the RetryInfo that keeps a ResourceExhausted retryable
// survives (TestGRPCBufferBudgetRefusalCarriesRetryInfo pins it).
//
// It runs with the transport's own mutex held, so it must not block: what
// follows is two atomics, a context and an armed timer.
func (s *Server) tapAdmit(ctx context.Context, _ *tap.Info) (context.Context, error) {
	reserve := int64(s.grpcMaxRecv)
	if !s.buffer.reserve(reserve) {
		obs.IngestRejected.Inc()
		// No peer: see noteShed. This is the one refusal taken before grpc-go
		// has put the address anywhere this code can reach.
		s.noteShed(shedBuffer, "")
		// The same refusal text as the HTTP arm's (errBufferBudget, which
		// WriteBodyError answers 429 with): one condition, one description,
		// whichever transport a sender used.
		return nil, exhaustedStatus(errBufferBudget.Error())
	}
	// grpc-go makes the context returned here the STREAM's context and reads the
	// message through it (http2Server.operateHeaders wires s.ctxDone into the
	// recvBufferReader), so cancelling it aborts a read that is waiting for DATA
	// frames that never arrive. That is what gives expire something to reap.
	ctx, cancel := context.WithCancel(ctx)
	r := &reservation{b: s.buffer, cancel: cancel, expired: s.noteReserveExpired}
	r.held.Store(reserve)
	ctx = context.WithValue(ctx, reservationKey{}, r)
	// The stream context is cancelled on every RPC outcome, so this is the
	// backstop for the paths that never reach the interceptor but do end.
	context.AfterFunc(ctx, r.release)
	// And this is the bound for the peer that ends nothing (see
	// grpcReserveWindow). Armed last: expire is a no-op until held is non-zero,
	// and release tolerates a not-yet-stored timer.
	r.timer.Store(time.AfterFunc(s.reserveWindow, r.expire))
	return ctx, nil
}

// reserveExpiryWarnEvery is the re-warn cadence for reaped decode windows. The
// condition is a peer's behaviour, not an event worth a line each: one slow or
// probing sender can produce one per stream open.
const reserveExpiryWarnEvery = time.Minute

// noteReserveExpired reports a reaped pre-decode reservation: it counts the
// metric that is the condition's ONLY standing signal — see reservation.expire
// for why it may not fold into obs.IngestRejected — and warns, throttled,
// because the condition is a peer's behaviour rather than an event worth a line
// each (one probing sender produces one per stream open). The local total is
// kept only to put a magnitude on that one line; the series is obs's.
func (s *Server) noteReserveExpired() {
	obs.IngestReserveExpired.Inc()
	total := s.reserveExpiries.Add(1)
	if s.reserveExpiryWarns.Allow(reserveExpiryWarnEvery) {
		s.log.Warn("reclaimed a gRPC pre-decode buffer reservation and cancelled the stream: "+
			"a peer opened a stream and delivered no message inside the decode window",
			"window", s.reserveWindow, "reservedBytes", s.grpcMaxRecv, "total", total)
	}
}

// releaseReservation hands the accounting over from the decode window to the
// processing bound.
func releaseReservation(ctx context.Context) {
	if r, ok := ctx.Value(reservationKey{}).(*reservation); ok {
		r.release()
	}
}
