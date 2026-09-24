package otlpingest

// gRPC admission (admit.go argues the three bounds): the pre-decode byte
// reservation the tap takes on the HEADERS frame and the decode window that
// bounds it, the decoded-structure claims the codec files for the interceptor,
// the unary interceptor that settles both and applies the count bound, and the
// stats handler that returns a claim whose RPC ended before the interceptor.

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/stats"
	"google.golang.org/grpc/tap"

	"github.com/JohanLindvall/kubescrape/internal/obs"
)

// One gRPC push reserves Server.grpcMaxRecv before grpc-go reads it. The size
// of the message is not knowable at that point — the tap runs on the HEADERS
// frame — so the reservation is the worst case the transport will accept
// (MaxRecvMsgSize). It is released as soon as the message is decoded and the
// interceptor takes over, so this bounds concurrent RECEIVES rather than
// concurrent pushes: it does NOT span the seconds a slow collector holds a
// processing slot, which is the count bound's job.
//
// What it DOES span is the upload, and that correction matters: this used to
// say "microseconds of unmarshal", which is true only of the decode at the end
// of it. grpc-go runs the unary interceptor once the whole message has been
// received, so the reservation covers every DATA frame — which is why the
// window that bounds it has to scale with the message cap (reserveWindowFor).

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
// 10s to deliver at most maxIngestGRPCMessage is 3.4 Mbit/s from a pod on this
// node (or, on the trace tier, from a pod in this cluster) — two orders of
// magnitude of slack — and it is the same clock, on the same question, as the
// HTTP arm's ReadHeaderTimeout: the peer has connected and shown no intent.
//
// It does not make the budget unspendable by a hostile peer: nothing can, on a
// listener with no credentials. It removes the asymmetry, which is the part
// that mattered — spending it now costs a stream open per 4 MiB per 10s, and
// the budget recovers on its own.
//
// It is the window for the DEFAULT message cap. reserveWindowFor scales it,
// because the sentence above is a BIT RATE and the numerator is a flag.
const grpcReserveWindow = 10 * time.Second

// maxReserveWindow caps what reserveWindowFor will scale to. The window's whole
// job is to reclaim a pin a peer would otherwise hold for the process' life, so
// it has to stay finite however large the configured message is — a reservation
// is grpcMaxRecv bytes of a budget only four of them fit in, and the peer that
// takes them needs no credentials.
//
// Five minutes is where the scaling stops being a rate and starts being a gift:
// at the default's 3.4 Mbit/s it is a ~120 MiB message, an order of magnitude
// past anything a batching SDK emits and well past what -ingest-grpc-max-recv-bytes
// is documented for. Above that the flag buys bytes, not time.
const maxReserveWindow = 5 * time.Minute

// reserveWindowFor sizes the pre-decode window against the message it has to
// carry, which is what grpcReserveWindow's own justification assumes and what a
// constant cannot do.
//
// The window is armed on the HEADERS frame and disarmed in the unary
// interceptor, which grpc-go runs only once the whole message has been received
// and decoded — so it spans the ENTIRE upload, not the "microseconds of
// unmarshal" the paragraph above reservation once claimed. The reservation SIZE
// already scales with -ingest-grpc-max-recv-bytes (tapAdmit reserves
// grpcMaxRecv, and NewServer grows the budget with it); leaving the window fixed
// turned that flag into a silent per-byte deadline. A tier told to accept 64 MiB
// messages gave a sender 10s to deliver one — 54 Mbit/s per stream — and reaped
// every push that could not, under a counter and a Warn that both said the peer
// "delivered no message", which is the opposite of what happened.
//
// So the rate is held constant instead of the time: the window is
// grpcReserveWindow scaled by MaxRecvBytes/maxIngestGRPCMessage, never shorter
// than grpcReserveWindow (a receiver configured for SMALLER messages keeps the
// full grace — the flag exists to raise the cap, and shrinking the window would
// make a lowered cap reap honest senders for a bound they never asked to
// tighten) and never longer than maxReserveWindow.
//
// The rate is derived by dividing FIRST and the ceiling is applied BEFORE the
// multiply, so no int an operator may pass — math.MaxInt included, whatever
// NewServer's own clamp does first — can overflow the arithmetic into a short
// window, which would be the failure this function exists to remove wearing a
// different hat. Truncating the per-byte rate costs
// well under a millisecond of a ten-second base.
func reserveWindowFor(recv int) time.Duration {
	if recv <= maxIngestGRPCMessage {
		return grpcReserveWindow
	}
	const perByte = int64(grpcReserveWindow) / int64(maxIngestGRPCMessage) // ns per byte
	if int64(recv) > int64(maxReserveWindow)/perByte {
		return maxReserveWindow
	}
	return time.Duration(perByte * int64(recv))
}

// reservation is one gRPC push's budget claim. It is returned by whichever of
// three paths comes first: the interceptor (the fast path — the message is
// decoded and the count bound takes over), the stream context's cancellation
// (every abort path that the peer or the transport drives), and the decode
// window (Server.reserveWindow, which reserveWindowFor sizes) elapsing (the peer
// that drives NEITHER). A leaked reservation sheds the whole
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
	// decodedKey is the request message this stream's codec filed its
	// decoded-structure claim under (decodedClaims), bound when grpc-go reports
	// the message received and returned when the RPC ends (claimReaper). Both
	// happen on the RPC's own handler goroutine — RecvMsg and processRPC's
	// deferred End — so, unlike the fields above, it needs no synchronisation.
	decodedKey any
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

// expire is the decode window (Server.reserveWindow, sized by reserveWindowFor)
// elapsing before the message was received and decoded. It reclaims the
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
		obs.IngestRejected.WithLabelValues(shedBuffer).Inc()
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
	// grpcReserveWindow for why, reserveWindowFor for how long). Armed last: expire is a no-op until held is non-zero,
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
			"a peer opened a stream and did not finish delivering its message inside the decode window "+
			"(a headers-only probe, or a sender slower than reservedBytes per window)",
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

// limitUnary applies the COUNT bound to gRPC pushes, hands the pre-decode
// reservation over to it, and settles the push's DECODED-STRUCTURE claim.
//
// The claim comes first. The codec took it from the wire bytes before the
// decode (decodedClaims): a refused push arrives here UNDECODED and is answered
// the retryable refusal the codec could not give; an admitted one is charged
// until the handler returns — for the whole enrich-and-forward step, exactly as
// the HTTP arm holds it for the whole handler, since the payload stays resident
// until the collector acks.
//
// Then the ORDER is load-bearing and it used to be the other way round. The
// message is already decoded by the time this runs, so the pre-decode
// reservation is the only RAW accounting for it; releasing that first and only
// then asking for a slot left a window in which the payload was charged to
// NOTHING, and a refused push released a reservation it had already given up —
// so a peer could hold as many decoded messages resident as it could open
// streams, bounded by the count alone. Take the slot first, then hand the
// accounting over.
func (s *Server) limitUnary(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	switch n := s.claims.take(req); {
	case n == claimRefused:
		// Nothing was decoded and the message buffer is already freed, so the
		// raw reservation has nothing left to account for.
		releaseReservation(ctx)
		s.noteShed(shedDecoded, grpcPeerAddr(ctx))
		return nil, exhaustedStatus(errDecodedBudget.Error())
	case n > 0:
		defer s.decoded.release(n)
	}
	if !s.acquire() {
		s.noteShed(shedInFlight, grpcPeerAddr(ctx))
		return nil, exhaustedStatus(errInFlight.Error())
	}
	defer s.release()
	// The slot is held: the reservation tapAdmit took for the read has done its
	// job and the message buffer itself is already freed by the codec.
	releaseReservation(ctx)
	return handler(ctx, req)
}

// decodedClaims carries each gRPC push's decoded-structure charge from the
// codec, which takes it from the wire bytes BEFORE the decode, to the unary
// interceptor, which releases it when the handler returns — or, for a push the
// budget refused, answers the refusal.
//
// It is a side table because nothing else connects the two: the codec is handed
// the bytes and the message it decodes into but no context, and grpc-go turns
// any error it returns into codes.Internal, which a conformant sender reads as
// PERMANENT and answers by dropping the batch. So the codec never refuses. An
// over-budget message is left UNDECODED — no allocation, which is the point —
// and the verdict is filed here under the message's identity; the interceptor,
// which receives that same message, collects it and answers the retryable
// ResourceExhausted + RetryInfo.
//
// The interceptor is NOT guaranteed to run, and that is why the claim has a
// second collector. A generated handler's decode is stream.RecvMsg, which for a
// unary method receives TWICE — the message, then a cardinality probe that
// expects end-of-stream — and returns before the interceptor if the probe fails:
// a peer that cancels or resets after its message instead of half-closing, one
// held open until the decode window reaps it, or one that sends a second
// message (decoded into the SAME request, then answered as a cardinality
// violation). Each such RPC used to leak its claim, its decoded-budget charge
// and the decoded request the map key pins, for the process' life — a few
// unauthenticated streams that each sent one wide message and cancelled shed
// both transports until restart. claimReaper returns whatever the interceptor
// did not collect when the RPC ends. A decode that FAILS returns its claim
// itself (abandonDecode) before grpc-go answers the malformed payload.
// TestGRPCDecodedClaimsNeverOutliveTheirPush and
// TestGRPCDecodedClaimsDoNotOutliveAnAbortedUnaryStream drive every outcome and
// assert the table and the budget both drain.
type decodedClaims struct {
	mu sync.Mutex
	m  map[any]int64
}

// claimRefused is the claim filed for a push the budget refused.
const claimRefused = -1

// put files n (bytes charged, or claimRefused) under v. A second claim for the
// same v — the cardinality probe decoding a second message into the same
// request — ADDS to a charge already filed rather than replacing it:
// overwriting forgot bytes the budget still held, which nothing could then
// release. A refusal never displaces a charge, and a charge displaces a
// refusal (the refusal only mattered to an interceptor that will not now run).
func (c *decodedClaims) put(v any, n int64) {
	c.mu.Lock()
	if c.m == nil {
		c.m = make(map[any]int64)
	}
	if old := c.m[v]; old > 0 {
		if n > 0 {
			n += old
		} else {
			n = old
		}
	}
	c.m[v] = n
	c.mu.Unlock()
}

// take collects (and forgets) v's claim: the bytes charged for it, claimRefused,
// or 0 when the codec filed nothing (an empty push, or a receiver with no
// budget).
func (c *decodedClaims) take(v any) int64 {
	c.mu.Lock()
	n, ok := c.m[v]
	if ok {
		delete(c.m, v)
	}
	c.mu.Unlock()
	return n
}

// claimDecode is the codec's half of the decoded charge (decodeAdmission): it
// estimates what b decodes into, charges it, and files the result for the
// interceptor. It reports whether the message may be decoded at all.
func (s *Server) claimDecode(v any, b []byte) bool {
	n := decodedSizeOf(v, b)
	if !s.chargeDecoded(n) {
		s.claims.put(v, claimRefused)
		return false
	}
	if n > 0 {
		s.claims.put(v, n)
	}
	return true
}

// abandonDecode returns a claim the interceptor will never collect: a decode
// that failed (grpc-go answers the malformed payload itself), or an RPC that
// ended without reaching the interceptor (claimReaper). A claim the interceptor
// already took is gone from the table, so this is then a no-op.
func (s *Server) abandonDecode(v any) {
	if n := s.claims.take(v); n > 0 {
		s.decoded.release(n)
	}
}

// claimReaper is the stats.Handler that closes decodedClaims' one gap: an RPC
// that ends before the unary interceptor collects its claim. It is the only
// hook that can: the codec sees the message but no context, the interceptor
// may never run, and grpc-go runs no stream interceptor for a unary method
// (processRPC calls the handler directly) — while pdata's RegisterGRPCServer
// takes a concrete *grpc.Server, so the method handler cannot be wrapped.
//
// grpc-go reports InPayload with the stream context and the very message the
// codec decoded into, right after the first receive succeeds — for a refused
// claim too, since the codec answers that one with a nil error — and reports
// End on the same goroutine once the handler has returned. So InPayload binds
// the message to the stream's reservation (tapAdmit's per-RPC state, found in
// the context) and End returns whatever is still filed under it. Ending on End
// rather than on the context's cancellation is deliberate: End cannot run until
// the handler has returned, so it can never release a claim out from under an
// interceptor still about to collect it.
//
// The price is grpc-go's own per-RPC stats bookkeeping (a handful of small
// event allocations, and the metadata copy InHeader carries), which is noise
// beside decoding a push. It rides the reservation, so it acts only where the
// tap is wired — which is why admissionOptions wires the four together.
type claimReaper struct{ s *Server }

func (claimReaper) TagRPC(ctx context.Context, _ *stats.RPCTagInfo) context.Context   { return ctx }
func (claimReaper) TagConn(ctx context.Context, _ *stats.ConnTagInfo) context.Context { return ctx }
func (claimReaper) HandleConn(context.Context, stats.ConnStats)                       {}

func (h claimReaper) HandleRPC(ctx context.Context, rs stats.RPCStats) {
	switch rs := rs.(type) {
	case *stats.InPayload:
		if r, ok := ctx.Value(reservationKey{}).(*reservation); ok {
			r.decodedKey = rs.Payload
		}
	case *stats.End:
		if r, ok := ctx.Value(reservationKey{}).(*reservation); ok && r.decodedKey != nil {
			h.s.abandonDecode(r.decodedKey)
			r.decodedKey = nil
		}
	}
}

// admissionOptions is the gRPC half of this receiver's admission, as ONE unit:
// the pre-decode tap (the raw reservation and its window), the codec (the
// nesting guard and the decoded-structure claim), the unary interceptor (the
// count bound, and the collector of both) and the claim reaper (the claim an
// aborted RPC left behind). They share per-RPC state — the codec files what
// the interceptor or the reaper collects, and the reaper finds the RPC through
// the tap's reservation — so wiring a subset of them is how a charge leaks.
func (s *Server) admissionOptions() []grpc.ServerOption {
	return []grpc.ServerOption{
		grpc.InTapHandle(s.tapAdmit),
		grpc.UnaryInterceptor(s.limitUnary),
		s.codecOption(),
		grpc.StatsHandler(claimReaper{s: s}),
	}
}
