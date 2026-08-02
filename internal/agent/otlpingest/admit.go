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
// What is charged is the PAYLOAD, which for a declared-length body is also what
// is allocated (readAllCapped pre-sizes it). A body whose size is not declared —
// gzip, chunked — is read into a buffer that grows by doubling, so its peak can
// briefly reach ~2x its charge; the four-body headroom is sized with that in
// mind.
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

// readAllCapped is io.ReadAll with a pre-sized destination: with a trustworthy
// size hint the body lands in ONE allocation instead of the log2(n) doublings
// io.ReadAll performs, halving the peak heap a charged payload occupies. The
// hint is grown by one byte because the loop needs a final short read to see
// EOF, and an exactly-sized buffer would double for it.
//
// max is the reader's own cap (16 MiB for application pushes, 4 MiB on the
// trace tier's internal hop) rather than a constant: the two receivers offer
// different limits deliberately.
func readAllCapped(r io.Reader, hint, max int64) ([]byte, error) {
	switch {
	case hint <= 0:
		hint = 511 // unknown length: start where io.ReadAll does
	case hint > max:
		// An over-cap declaration is rejected once the read confirms it, but
		// the read still happens: cap the buffer at what the LimitReader will
		// hand over so the rejection costs one allocation rather than a
		// doubling sequence up to it.
		hint = max
	}
	buf := make([]byte, 0, hint+1)
	for {
		if len(buf) == cap(buf) {
			buf = append(buf, 0)[:len(buf)] // let append pick the growth
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

// reservation is one gRPC push's budget claim. It is released by whichever of
// the interceptor (the fast path: the message is decoded, the count bound takes
// over) and the stream context's cancellation (every abort path, including a
// stream that is opened and never sends a message) happens first — a leaked
// reservation would shed the whole listener for the process' life.
type reservation struct {
	b    *byteBudget
	held atomic.Int64
}

func (r *reservation) release() {
	if n := r.held.Swap(0); n > 0 {
		r.b.release(n)
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
func (s *Server) tapAdmit(ctx context.Context, _ *tap.Info) (context.Context, error) {
	if !s.buffer.reserve(grpcReserveBytes) {
		obs.IngestRejected.Inc()
		return nil, exhaustedStatus("receiver is holding its maximum buffered payload bytes; retry")
	}
	r := &reservation{b: s.buffer}
	r.held.Store(grpcReserveBytes)
	ctx = context.WithValue(ctx, reservationKey{}, r)
	// The stream context is cancelled on every RPC outcome, so this is the
	// backstop for the paths that never reach the interceptor.
	context.AfterFunc(ctx, r.release)
	return ctx, nil
}

// releaseReservation hands the accounting over from the decode window to the
// processing bound.
func releaseReservation(ctx context.Context) {
	if r, ok := ctx.Value(reservationKey{}).(*reservation); ok {
		r.release()
	}
}
