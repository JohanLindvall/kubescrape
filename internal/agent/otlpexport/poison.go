package otlpexport

// The drain's poison-payload machinery: how a batch the collector keeps
// refusing — while it demonstrably accepts OTHER batches — is told apart from
// an outage, and dropped only on that evidence (stuckTooLong).

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/JohanLindvall/haste/xxh3"
	"google.golang.org/grpc/status"

	"github.com/JohanLindvall/kubescrape/internal/agent/backoff"
)

type stuckBatch struct {
	cycles        int
	lastDelivered uint64 // s.delivered at this payload's previous failed cycle
}

// progressed reports whether the collector delivered some OTHER batch since this
// payload's previous failed cycle — the whole poison evidence (stuckTooLong says
// why it is re-established per lap). ONE predicate, read by stuckTooLong after a
// send and by accountableLap before one: were they to drift, trySend would
// shorten laps that spend no budget.
func (b stuckBatch) progressed(delivered uint64) bool { return delivered > b.lastDelivered }

// maxDrainCycles bounds how many drain cycles a batch may fail — while the
// collector is demonstrably accepting OTHER batches — before it is dropped as
// undeliverable. A rejection classified TRANSIENT that is in fact permanent for
// this ONE payload (canonically an over-limit message, which gRPC reports as
// ResourceExhausted) would otherwise circle the queue forever: never delivered,
// never dropped, never counted, holding spool bytes across restarts.
//
// The "accepting other batches" condition is what makes this safe. A collector
// OUTAGE fails every batch too, and dropping there would breach the zero-loss
// guarantee for logs — so a batch that fails while nothing else is getting
// through is retried indefinitely, which is exactly right: the outage is the
// only thing wrong with it. Only a payload the live collector keeps singling
// out is poison.
const maxDrainCycles = 3

// maxStuckTracked bounds the stuck map during a long outage (every queued batch
// fails then, and none is poison).
const maxStuckTracked = 4096

// stuckTooLong records another failed drain cycle for this payload and reports
// whether it should now be given up on: it must have failed maxDrainCycles times
// AFTER the collector proved — while this very batch was stuck — that it was
// alive and still rejecting THIS payload (otherwise this is an outage, not a
// poison payload, and the batch is retried indefinitely rather than lost).
//
// The evidence is s.delivered advancing between two of this payload's own
// failed cycles: some OTHER batch got through while this one was circling the
// queue failing — the collector singling this payload out. A batch that fails
// only during a pure outage (nothing else delivering) never shows it, so it is
// never dropped. Crucially, deliveries banked BEFORE this payload got stuck, or
// during a recovery it merely sat behind in the queue without being attempted,
// do not count: those advance s.delivered without any stuck cycle of ours
// spanning them, so the comparison against stuckBatch.lastDelivered — the
// delivery count at OUR previous failure — stays false and a single later
// failure cannot spend the poison budget. (The earlier cumulative "deliveries
// since first stuck" test dropped good batches on exactly that.)
//
// Keying by content hash (rather than threading a counter through the spool
// format) means the count resets on restart — which is what we want: a fresh
// process should re-offer the batch, since the collector may have been fixed or
// upgraded in the meantime.
//
// The key is 128 bits because a collision here DESTROYS GOOD DATA rather than
// merely mis-measuring: two distinct payloads sharing a slot pool their
// accountable-failure counts, so a healthy batch can inherit a poison batch's
// budget and be dropped after fewer laps than maxDrainCycles allows. The map is
// keyed by hash alone — there is no stored payload to re-compare against on a
// hit, which is what would otherwise turn a collision back into a miss — so the
// width IS the whole defence.
func (s *sink[T]) stuckTooLong(data []byte) bool {
	h := xxh3.Sum128(data)
	if s.stuck == nil {
		s.stuck = make(map[xxh3.Uint128]stuckBatch)
	}
	st, seen := s.stuck[h]
	if !seen && len(s.stuck) >= maxStuckTracked {
		// Evict the entry with the OLDEST evidence rather than refuse to
		// track: an entry leaks permanently when its payload leaves the queue
		// by a path other than success/permanent/max-cycles (an ErrCorrupt
		// skip-past, a recovery truncation), and a full map that refuses new
		// entries disarms poison detection for every later payload — the
		// exact rebuild-forever wedge stuckTooLong exists to break. The
		// victim's budget resets, which at worst delays a real poison batch
		// by a few laps; the O(n) scan runs only at the cap, on the failure
		// path.
		var evict xxh3.Uint128
		var evictAt uint64
		first := true
		for k, v := range s.stuck {
			if first || v.lastDelivered < evictAt {
				evict, evictAt, first = k, v.lastDelivered, false
			}
		}
		delete(s.stuck, evict)
	}
	// Did the collector deliver some OTHER batch since THIS lap's predecessor?
	// That is the whole evidence, and it must be re-established every lap.
	// Latching it once — "it delivered something at some point, so every later
	// responded lap counts" — spent the poison budget during a pure
	// back-pressure event, destroying good data in exactly the outage the disk
	// buffer exists to survive: one delivery early on armed the latch, and a
	// collector that then started refusing everything (ResourceExhausted, which
	// respondedError treats as a response) burned a cycle per lap until the
	// head was dropped.
	//
	// Deliveries banked before this batch got stuck, or while it merely sat
	// behind in the queue during a recovery, advance s.delivered without any
	// failed lap of ours spanning them — the comparison is against the delivery
	// count at our own previous failure, so they cannot count either.
	//
	// accountableLap asks the same question BEFORE the send to decide whether
	// the lap may end after one attempt, through the same stuckBatch.progressed.
	progressed := seen && st.progressed(s.delivered)
	// A lap counts only when the collector is alive (progressed) AND actually
	// RESPONDED to this lap's send (a rejection, not a transport failure): only
	// a live collector repeatedly refusing THIS payload while accepting others
	// is poison evidence.
	if progressed && s.stuckResponded {
		st.cycles++
	}
	st.lastDelivered = s.delivered
	s.stuck[h] = st
	if st.cycles < maxDrainCycles {
		return false
	}
	delete(s.stuck, h)
	return true
}

// respondedError reports whether err carries a REJECTION from the collector (an
// HTTP status or a gRPC status), as opposed to a transport-level failure where
// the collector may simply be down, or a status OTLP defines as retryable
// back-pressure. The retryable statuses are treated as not-a-rejection even when
// server-sent — the conservative direction: an outage or a throttle must never
// count toward the poison budget.
//
// Known asymmetry, deliberate for now: gRPC Internal counts (a collector bug
// that rejects THIS payload is poison evidence) while its HTTP counterpart 500
// does not (5xx is OTLP/HTTP's retryable server-error class). Changing it means
// changing both arms together.
func respondedError(err error) bool {
	if he, ok := errors.AsType[*HTTPStatusError](err); ok {
		// Throttling and server-side outages are the HTTP counterparts of the
		// gRPC statuses RetryableStatus lists, and OTLP defines them as
		// RETRYABLE: a collector answering 429/503 while it accepts other
		// batches is back-pressuring, not rejecting this payload. Counting them
		// as poison evidence dropped perfectly good data during exactly the
		// outage the disk buffer exists to survive.
		switch {
		case he.Code == http.StatusRequestTimeout, // 408
			he.Code == http.StatusTooManyRequests, // 429
			he.Code >= 500:                        // 502/503/504 and friends
			return false
		}
		return true
	}
	if st, ok := status.FromError(err); ok {
		// The spec's retryable list, shared with otlpingest's relay decision.
		// A BARE ResourceExhausted (grpc-go's over-limit refusal of THIS
		// message) is not on it, and is exactly the poison this budget exists
		// for.
		return !RetryableStatus(st)
	}
	return false
}

// forget drops a payload's stuck-tracking once it is committed, so a batch that
// eventually succeeds does not leak an entry to the maxStuckTracked cap.
//
// It runs on EVERY committed batch, so it takes accountableLap's short circuit
// on EMPTINESS, not on nil: stuckTooLong allocates the map on the first stuck
// lap and nothing sets it back, so a nil test left every delivery for the rest
// of the process hashing its whole payload to delete from an empty map.
func (s *sink[T]) forget(data []byte) {
	if len(s.stuck) == 0 {
		return
	}
	delete(s.stuck, xxh3.Sum128(data))
}

type sendResult int

const (
	sendOK sendResult = iota
	sendCancelled
	sendRejected
	sendStuck
)

// stuckAfterAttempts is how many transient failures trySend tolerates before
// reporting the batch stuck (drain then rotates it to the back of the queue).
const stuckAfterAttempts = 5

// accountableLap reports whether a failure of THIS payload, right now, would
// count toward its poison budget: it has already failed a whole lap (so it is
// in the stuck map) and the collector has delivered some OTHER batch since that
// lap. It is deliberately the SAME predicate stuckTooLong weighs after the send,
// read off the same two fields, evaluated before it — so a lap trySend cuts
// short is always a lap the drop rule is already spending. It is therefore
// never true on a payload's first sighting, which is what leaves every batch
// its full stuckAfterAttempts before anything about it is held against it.
//
// The evidence has to be about the PAYLOAD. A sink-wide "has the collector
// delivered anything since the last failure here" is a property of the
// COLLECTOR, and it is true on attempt 1 for every batch that meets a responded
// transient error while the sink is warm — and respondedError is true for
// exactly the classes IsPermanent deliberately treats as transient: the 401/403
// of a rotating bearer token, the 404 of a collector rolling out behind an
// ingress. Gating on it cut the retry depth of good data to one attempt per lap
// during precisely the two events the depth exists for, and rotated it into
// drop evidence five times faster.
//
// It is self-limiting: stuckTooLong stamps st.lastDelivered on every failed lap,
// so a second short lap must be earned by a fresh delivery of something else. A
// queue with nothing else left to drain falls back to the full backed-off cycle
// instead of spinning on its only record at wire speed.
//
// The empty-map short circuit keeps the hash off the drain's steady state: the
// map holds entries only while something is failing.
func (s *sink[T]) accountableLap(data []byte) bool {
	if len(s.stuck) == 0 {
		return false
	}
	st, seen := s.stuck[xxh3.Sum128(data)]
	return seen && st.progressed(s.delivered)
}

// trySend retries with backoff until the exporter accepts the batch, the
// error is a permanent rejection, the attempt budget is spent, or ctx is
// cancelled. send is the one attempt — the drain chooses between the raw-bytes
// and the pdata send, and everything else about the policy is identical.
//
// accountable (accountableLap) shortens the cycle to ONE attempt for a payload
// that is ALREADY spending its poison budget — see the early return below. The
// queue head is a node-shared resource, and the whole point is to stop holding
// it for a question this payload has already had answered.
func (s *sink[T]) trySend(ctx context.Context, send func(context.Context) error, accountable bool) sendResult {
	// The backoff persists across trySend cycles (s.cur) so a long outage
	// actually reaches the 30s cap instead of restarting at s.backoff every
	// stuckAfterAttempts sends; success resets it. The floor is clamped to the
	// cap like backoff.New's initial: an -otlp-retry-backoff above 30s would
	// otherwise wait that long, "double" down to the cap, and climb back to the
	// flag at the next cycle's floor.
	if floor := min(s.backoff, backoff.Cap); s.cur < floor {
		s.cur = floor
	}
	for attempt := 1; ; attempt++ {
		err := send(ctx)
		if err == nil {
			s.cur = 0
			return sendOK
		}
		if IsPermanent(err) {
			s.cur = 0 // the connection demonstrably works; don't penalize the next batch
			s.log.Warn("buffered export permanently rejected", "signal", s.kind, "error", err)
			return sendRejected
		}
		responded := respondedError(err)
		// End the cycle after ONE attempt when this lap is one the drop rule is
		// ALREADY counting: the payload failed a whole cycle before, the
		// collector has delivered some OTHER batch since that failure, and it has
		// now ANSWERED about this one rather than gone quiet. Under all three the
		// remaining attempts only re-ask, at the QUEUE HEAD, a question answered
		// this lap and the last — and the head is a resource the whole node
		// shares, since commits are a cursor and nothing behind it drains while it
		// is held. With the backoff persisting across cycles (s.cur, capped at
		// 30s) such a head rotated at most once every ~2 minutes, and the drop
		// rule's evidence accrues at most once per rotation: one sender pushing
		// records no collector will accept (a single over-4-MiB log record is
		// enough) stalled every other producer's logs on that node for minutes at
		// a time, repeatedly.
		//
		// What it may NOT do is shorten the depth of a payload that is merely
		// meeting a transient collector condition, which is why the gate is
		// accountableLap and not "the collector is up": respondedError is true for
		// the classes IsPermanent deliberately treats as transient (a rotating
		// token's 401, a rollout's 404), so a sink-warm gate fired on attempt 1
		// for good data during exactly the two events the retry depth exists for.
		//
		// The arithmetic, first sighting to drop: stuckAfterAttempts attempts on
		// the first lap — nothing is accountable yet, so a condition lasting four
		// attempts is still delivered on the fifth — and one attempt on each of
		// the maxDrainCycles accountable laps after it. Eight wire attempts,
		// against the twenty a full cycle per lap costs, and no lap that the drop
		// rule is not already spending is ever shortened, warm sink or cold.
		if attempt >= stuckAfterAttempts || (accountable && responded) {
			s.stuckResponded = responded
			// Throttled: a collector outage produces one of these per signal
			// per cycle on every node, and the condition is one line. The
			// destination itself is narrated once by the client's health report
			// (report.go), which is where the endpoint and the remedy are; this
			// line's own news is about the SPOOL — the head is going back and
			// the queue is rotating around it.
			if s.allowWarn(drainWarnRequeue) {
				s.log.Warn("a buffered batch has failed a whole drain cycle and is going back to the queue; "+
					"the spool keeps growing until the collector accepts it",
					"signal", s.kind, "error", err, "attempts", attempt)
			}
			return sendStuck
		}
		// Debug, not Warn: this is one ATTEMPT inside a cycle, and a cycle
		// against a dead collector makes stuckAfterAttempts of them — per
		// signal, per node, forever. The state is reported once above and once
		// by the client's health report; the per-attempt detail belongs to an
		// incident, not to the steady stream. (Both arguments are field reads,
		// so no Enabled guard is needed.)
		s.log.Debug("buffered export failed, retrying", "signal", s.kind, "error", err, "backoff", s.cur)
		// NewTimer+Stop, backoff.B.Wait's style (a style choice, not a leak
		// fix — see there). Not a backoff.B: this delay persists across
		// trySend cycles and resets to zero on success, so only the doubling
		// rule is shared.
		t := time.NewTimer(s.cur)
		select {
		case <-ctx.Done():
			t.Stop()
			return sendCancelled
		case <-t.C:
		}
		s.cur = backoff.Double(s.cur)
	}
}
