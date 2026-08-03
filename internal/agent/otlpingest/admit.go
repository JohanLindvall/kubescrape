package otlpingest

// Admission control for the unauthenticated ingest listeners.
//
// There are TWO resources to bound, and one semaphore cannot bound both:
//
//   - PROCESSING (Server.inFlight, -ingest-max-in-flight): enrichment, the
//     inflated pdata and the forward. A slot is held for as long as the
//     collector takes to ack.
//   - BUFFERING (byteBudget below): the RAW payload bytes a request holds
//     while it is being read off the wire and decoded, before any of that
//     processing starts.
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
// The budget is global across both transports because the memory is: one
// process, one heap. A refusal is always RETRYABLE (429 + Retry-After, or
// ResourceExhausted carrying RetryInfo) — the sender still holds the payload,
// which is what makes shedding better than accepting it and running the node
// out of memory. Bounding by connection count instead (a limit listener) was
// rejected for the same reason the slot was moved off the read path: it sheds
// on the wrong axis, punishing a hundred idle keep-alives and letting four busy
// ones allocate without limit.

import (
	"context"
	"errors"
	"io"
	"sync/atomic"
	"time"

	"google.golang.org/grpc/tap"

	"github.com/JohanLindvall/kubescrape/internal/obs"
)

// maxBufferBytes bounds the raw payload bytes both transports may hold at once,
// at four full-size requests. It is a hard ceiling rather than a flag: it must
// be at least maxIngestBody or a single legal request could never be admitted,
// and the knob an operator actually tunes is -ingest-max-in-flight, which bounds
// the far more expensive resource (inflated pdata held across a collector's ack
// latency).
//
// What is charged is the PAYLOAD as it is read. The buffer it lands in grows by
// doubling past a small pre-sized head (readAllCapped), so its peak can briefly
// reach ~2x its charge; the four-body headroom is sized with that in mind. It is
// deliberately NOT pre-sized from Content-Length — see maxPresizeBytes.
const maxBufferBytes = 4 * maxIngestBody

// budgetGranule is the top-up size for an in-progress read: a trickled body
// touches the shared counter once per 64 KiB rather than once per Read.
const budgetGranule = 64 << 10

// grpcReserveBytes is what one gRPC push reserves before grpc-go reads it. The
// size of the message is not knowable at that point — the tap runs on the
// HEADERS frame — so the reservation is the worst case the transport will
// accept (MaxRecvMsgSize). It is released again as soon as the message is
// decoded and the interceptor takes over, so this bounds concurrent DECODES,
// not concurrent pushes: the reservation is held for microseconds of unmarshal,
// not for the seconds a slow collector holds a processing slot.
const grpcReserveBytes = maxIngestGRPCMessage

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
// max is the reader's own cap (16 MiB for application pushes, 4 MiB on the
// trace tier's internal hop) rather than a constant: the two receivers offer
// different limits deliberately.
func readAllCapped(r io.Reader, hint, max int64) ([]byte, error) {
	if hint > max {
		// An over-cap declaration is rejected once the read confirms it, but
		// the read still happens: never size past what the LimitReader will
		// hand over.
		hint = max
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
func (r *reservation) expire() {
	if n := r.held.Swap(0); n > 0 {
		r.b.release(n)
		obs.IngestRejected.Inc()
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
	if !s.buffer.reserve(grpcReserveBytes) {
		obs.IngestRejected.Inc()
		return nil, exhaustedStatus("receiver is holding its maximum buffered payload bytes; retry")
	}
	// grpc-go makes the context returned here the STREAM's context and reads the
	// message through it (http2Server.operateHeaders wires s.ctxDone into the
	// recvBufferReader), so cancelling it aborts a read that is waiting for DATA
	// frames that never arrive. That is what gives expire something to reap.
	ctx, cancel := context.WithCancel(ctx)
	r := &reservation{b: s.buffer, cancel: cancel}
	r.held.Store(grpcReserveBytes)
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

// releaseReservation hands the accounting over from the decode window to the
// processing bound.
func releaseReservation(ctx context.Context) {
	if r, ok := ctx.Value(reservationKey{}).(*reservation); ok {
		r.release()
	}
}
