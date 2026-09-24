package otlpexport

// One signal's spool DRAIN: enqueue, the write-side refusal warnings, and the
// drain loop that reserves, sends and commits. The poison-payload machinery the
// drain consults lives in poison.go.

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/JohanLindvall/diskqueue"
	"github.com/JohanLindvall/haste/xxh3"

	"github.com/JohanLindvall/kubescrape/internal/logdedupe"
	"github.com/JohanLindvall/kubescrape/internal/obs"
)

// sink is one signal's buffer: a queue plus the (un)marshal and send functions
// for its pdata type.
type sink[T any] struct {
	buf       *Buffer
	marshal   func(T) ([]byte, error)
	unmarshal func([]byte) (T, error)
	// count reports how many records a decoded payload carries (log records,
	// metric data points, spans). A drop here is data loss, and a batch is
	// 1..1024 records — without this the magnitude of the loss on the durable
	// configuration, which is the one an operator is told to alert on, was
	// simply not knowable.
	count func(T) int
	send  func(context.Context, T) error
	// sendRaw hands the RESERVED BYTES straight to the wire, skipping the
	// decode-and-re-encode round trip that produced exactly those bytes again
	// (see rawsend.go). nil when the inner exporter cannot promise the
	// equivalence, in which case the drain decodes as it always did.
	// maxSendBytes is the client's send cap: a payload over it needs otlpsplit
	// and therefore pdata, so that one decodes (<= 0 = splitting disabled).
	sendRaw      func(context.Context, []byte) error
	maxSendBytes int
	backoff      time.Duration
	cur          time.Duration // current backoff, persisted across trySend cycles of a failing head
	log          *slog.Logger
	kind         string
	// delivered counts batches this sink has successfully exported; stuck
	// tracks, per stuck payload (keyed by content hash), its accountable failed
	// cycles and the value of delivered at its own previous failed cycle — the
	// two together are the poison evidence (see stuckTooLong).
	delivered uint64
	stuck     map[xxh3.Uint128]stuckBatch
	// stuckResponded records whether the latest sendStuck's final error carried
	// a RESPONSE from the collector (vs a transport failure); set by trySend,
	// read by stuckTooLong (both on the drain goroutine).
	stuckResponded bool
	// enqueueWarns throttles this spool's repeating conditions, keyed: the two
	// WRITE-side refusals, which had counters
	// and no line at all: a full spool and a spool that cannot be written to.
	// Both are STATES an operator must act on and both repeat at the producers'
	// full rate (every tailer flush, every scrape), so they are exactly what
	// logdedupe exists for. Keyed, because "the disk is full" and "the queue is
	// at its cap" are different problems with different fixes and one must not
	// suppress the other. Producer goroutines share it; Table is mutexed.
	enqueueWarns *logdedupe.Table
}

// enqueue durably appends one payload to the spool; a refusal is counted,
// warned about (throttled) and returned to the producer.
func (s *sink[T]) enqueue(v T) error {
	data, err := s.marshal(v)
	if err != nil {
		// pdata proto marshal is effectively infallible, but a payload that
		// cannot SERIALIZE can never enqueue: returned bare (and hence
		// transient) the tailer would rewind and rebuild the same batch
		// forever with every buffer metric flat. Count it with the other
		// non-capacity refusals and classify it permanent.
		obs.BufferEnqueueErrors.WithLabelValues(s.kind).Inc()
		return fmt.Errorf("%w: %w", errUnmarshalable, err)
	}
	err = s.buf.add(data)
	switch {
	case err == nil:
	case errors.Is(err, diskqueue.ErrFull) || errors.Is(err, diskqueue.ErrRecordTooLarge):
		// Back-pressure for logs (the tailer rewinds and re-reads), but a lost
		// batch for the scraper and the self-metrics registry (their next
		// payload carries current values again). The log-metric exporter
		// re-offers ErrFull's samples (IsPermanent is false for it) and drops
		// only an ErrRecordTooLarge batch. Count it: an undelivered metric
		// batch must never disappear silently.
		// ErrRecordTooLarge is one batch larger than the whole cap — a config
		// problem, but the producer-facing handling is the same refusal.
		obs.BufferFull.WithLabelValues(s.kind).Inc()
		s.warnEnqueue(enqueueWarnFull, err)
	default:
		// EVERY other refusal is counted too, and separately: a latched I/O
		// error, a closed queue, or the raw *os.PathError diskqueue returns when
		// segment preallocation hits ENOSPC. These were returned bare, so a full
		// disk made a node go dark with every buffer metric flat — _full_total
		// is the wrong class, _dropped_total is drain-side, and obs.Exports sits
		// BELOW the buffer and is never reached.
		obs.BufferEnqueueErrors.WithLabelValues(s.kind).Inc()
		s.warnEnqueue(enqueueWarnBroken, err)
	}
	return err
}

// The two enqueue-refusal classes, as throttle keys and as the `reason` an
// operator greps for. They are deliberately not folded into one: a FULL spool
// is the collector being down for longer than the cap covers (fix the
// collector, or raise -buffer-max-bytes), while a BROKEN one is the node's disk
// refusing the write (out of space, read-only mount, a latched fsync failure) —
// and only the second makes the buffer useless while the collector is fine.
const (
	enqueueWarnFull   = "full"
	enqueueWarnBroken = "write_failed"
	// drainWarnRequeue shares the table: it is the same spool's condition seen
	// from the DRAIN side, and it is throttled for the same reason.
	drainWarnRequeue = "requeue"
)

// bufferWarnEvery paces both. The condition persists for as long as the
// collector is down or the disk is full, and every producer on the node meets
// it on every flush.
const bufferWarnEvery = time.Minute

// allowWarn is the throttle gate for this spool's repeating conditions. A sink
// built without a table (only this package's own tests can make one) warns
// every time rather than falling silent: losing a diagnostic is the worse of
// the two failures.
func (s *sink[T]) allowWarn(reason string) bool {
	if s.enqueueWarns == nil {
		return true
	}
	allow, _ := s.enqueueWarns.Allow(reason)
	return allow
}

// warnEnqueue puts the CONTEXT on a refusal the counters could only ever give a
// rate for: which spool, how full it is against its cap, and — the whole
// diagnosis in the broken case — the error the filesystem returned, which is
// where the *os.PathError naming the directory lives.
func (s *sink[T]) warnEnqueue(reason string, err error) {
	if !s.allowWarn(reason) {
		return
	}
	st := s.buf.stats()
	args := []any{"signal", s.kind, "reason", reason, "dir", s.buf.dir,
		"bytes", st.BacklogBytes, "maxBytes", st.MaxBytes, "error", err}
	if reason == enqueueWarnFull {
		s.log.Warn("the disk buffer is refusing new batches: it is at its cap and the collector is not draining it. "+
			"Producers that can rewind (the tailer) back-pressure; every other producer's batch is lost", args...)
		return
	}
	s.log.Error("the disk buffer cannot be written to, so this signal has no durability and batches are being lost. "+
		"Check the buffer directory's free space, its mount and its permissions", args...)
}

// records is count(v), defensively tolerating a sink built without one (only
// the package's own tests can construct such a thing).
func (s *sink[T]) records(v T) int {
	if s.count == nil {
		return 0
	}
	return s.count(v)
}

// countDropped counts one dropped batch and the n records it took with it.
// n comes from the caller because the drain decodes the payload ON DEMAND —
// a drop is the one place the record count is needed, and the one place the
// pdata round trip is worth paying for.
func (s *sink[T]) countDropped(n int) {
	obs.BufferDroppedBatches.WithLabelValues(s.kind).Inc()
	if n > 0 {
		obs.BufferDroppedRecords.WithLabelValues(s.kind).Add(float64(n))
	}
}

// bufOf returns the sink's buffer, nil-safe (a signal can be disabled).
func (s *sink[T]) bufOf() *Buffer {
	if s == nil {
		return nil
	}
	return s.buf
}

func (s *sink[T]) drain(ctx context.Context) { s.drainLoop(ctx, false) }

// drainUntilEmpty is drain that returns when the spool runs dry instead of
// waiting for more (the shutdown pass — see Buffered.FinalDrain).
func (s *sink[T]) drainUntilEmpty(ctx context.Context) { s.drainLoop(ctx, true) }

func (s *sink[T]) drainLoop(ctx context.Context, untilEmpty bool) {
	if s == nil {
		return
	}
	for {
		q, rd := s.buf.handles()
		var (
			data []byte
			ok   bool
			off  int64
			err  error
		)
		if untilEmpty {
			data, ok, off, err = rd.TryReserve()
		} else {
			// Reserve blocks until a record arrives (or ctx ends) — the
			// queue's own wakeup replaces the old signal-channel machinery.
			data, ok, off, err = rd.Reserve(ctx)
		}
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			// ErrCorrupt is REPORTED loss: the queue already advanced past the
			// damage (its Stats carry the magnitude), so the drain just counts
			// and keeps going. Anything else leaves the queue where it was; a
			// dead handle (latched I/O failure, or a close under us) is
			// replaced per diskqueue's close-and-reopen contract.
			lost := errors.Is(err, diskqueue.ErrCorrupt)
			// A read on a handle this process RETIRED — the recover swap, or a
			// deliberate Close — is an expected transition of our own making,
			// and counting it makes a self-inflicted swap indistinguishable
			// from a disk that is failing. Everything else is a read failure,
			// including a reopen that did not take (the handle is then still
			// ours and still dead).
			if errors.Is(err, diskqueue.ErrClosed) && s.buf.retired(q) {
				s.log.Debug("disk buffer handle retired under the drain", "signal", s.kind, "error", err)
			} else {
				obs.BufferReadErrors.WithLabelValues(s.kind, strconv.FormatBool(lost)).Inc()
				s.log.Error("disk buffer read failed", "signal", s.kind, "dataLost", lost, "error", err)
			}
			if queueDead(err) {
				s.buf.recover(q)
			}
			if !lost {
				if untilEmpty {
					return // a dead disk must not spin the bounded shutdown pass
				}
				// NewTimer+Stop, the package's style for a ctx-cancellable
				// wait (Retry and trySend do the same). Not a leak fix: since
				// Go 1.23 an abandoned time.After timer is garbage-collected
				// rather than left live until it fires.
				t := time.NewTimer(time.Second)
				select {
				case <-ctx.Done():
					t.Stop()
					return
				case <-t.C:
				}
			}
			continue
		}
		if !ok {
			if untilEmpty {
				return // queue dry: the shutdown pass is done
			}
			continue // blocking Reserve yields !ok only on ctx/close; re-check both
		}
		// The spooled bytes ARE the wire body (see rawsend.go), so the common
		// path sends them as they are: decoding here only for the client to
		// re-encode the identical bytes cost ~520µs and 10,274 allocations per
		// 215 KB batch, and a node shipping 10 MB/min through the spool pays
		// ~50 of those a minute. pdata is materialized on the COLD branches
		// only: a payload over the send cap (otlpsplit needs the structure) and
		// a DROP (whose counter reports records). A payload that does not decode
		// is then no longer caught before the send — the collector rejects it
		// instead, which is the same outcome by a different judge, and
		// diskqueue's per-record checksums mean this path was already all but
		// unreachable.
		var (
			v       T
			decoded bool
		)
		decode := func() bool {
			if decoded {
				return true
			}
			dv, derr := s.unmarshal(data)
			if derr != nil {
				s.log.Warn("buffered batch does not decode", "signal", s.kind, "error", derr)
				return false
			}
			v, decoded = dv, true
			return true
		}
		// records is the drop counters' magnitude, decoded on demand: a batch
		// that cannot decode contributes 0 records, exactly as before.
		records := func() int {
			if !decode() {
				return 0
			}
			return s.records(v)
		}
		sendable := s.sendRaw != nil && (s.maxSendBytes <= 0 || len(data) <= s.maxSendBytes)
		if !sendable && !decode() {
			// Undecodable payload with no raw path: the data is gone either way,
			// but the drop must be counted like every other one. The RECORD count
			// is the one thing that cannot be counted here — it lives in the
			// bytes that failed to decode — so this is the sole drop path that
			// moves the batch counter alone.
			obs.BufferDroppedBatches.WithLabelValues(s.kind).Inc()
			s.log.Warn("dropping corrupt buffered batch", "signal", s.kind)
			s.commit(q, rd, off)
			continue
		}
		send := func(c context.Context) error { return s.send(c, v) }
		if sendable {
			// One copy, per BATCH. The reserved payload aliases the reader's
			// buffer and the next queue operation reuses it — Requeue clobbers it
			// outright — while a transport can still be referencing the body it
			// was given after a FAILED attempt returns (net/http's writeLoop and
			// grpc's loopyWriter both finish asynchronously once Do/Invoke has
			// errored). The pdata path was implicitly safe because each attempt
			// marshaled private memory. So: one allocation and a memcpy, against
			// the ~11,300 allocations and 3.7ms the decode-and-re-encode round
			// trip cost for the same 183 KB batch.
			payload := append([]byte(nil), data...)
			send = func(c context.Context) error { return s.sendRaw(c, payload) }
		}
		// data still aliases the reader's buffer — no queue operation has run
		// since Reserve, and trySend performs none — so it is still the right
		// bytes to key the stuck map on. Every later use of it (stuckTooLong,
		// forget) carries the same note for the same reason.
		switch s.trySend(ctx, send, s.accountableLap(data)) {
		case sendOK:
			s.forget(data) // a previously-stuck payload that recovered; hash before the next queue op
			s.commit(q, rd, off)
			s.delivered++ // proof the collector is alive: see stuckTooLong
		case sendCancelled:
			// ctx cancelled mid-send: nack the reservation so the batch is
			// queued for the next run (or the final drain).
			s.buf.rewindQ(q)
			return
		case sendRejected:
			// A definitive rejection (bad payload, auth, unimplemented):
			// retrying cannot fix it and keeping it would block the queue.
			n := records()
			s.log.Error("dropping buffered batch permanently rejected by the collector",
				"signal", s.kind, "records", n)
			s.countDropped(n)
			s.forget(data) // a batch that got stuck then turned permanent must not leak its entry
			s.commit(q, rd, off)
		case sendStuck:
			// Repeated transient failures. Commits are a cursor — nothing
			// behind the head can retire first — so the batch goes back to the
			// head (Rewind) and, when anything is queued behind it, rotates to
			// the back (Requeue, exempt from the caps since diskqueue v0.0.4:
			// a full queue must not pin a poison head, or nothing ever drains,
			// no other batch can ever prove the collector alive, and the
			// signal wedges across restarts).
			// Rotating an only batch is pointless churn; it retries at the
			// head. NOTE the hash in stuckTooLong happens before Requeue —
			// Requeue clobbers the reader buffer data aliases.
			if s.stuckTooLong(data) {
				n := records()
				s.log.Error("dropping buffered batch the collector never accepted",
					"signal", s.kind, "cycles", maxDrainCycles, "bytes", len(data), "records", n)
				s.countDropped(n)
				s.commit(q, rd, off)
				continue
			}
			s.buf.rewindQ(q)
			if s.buf.stats().Backlog > 1 {
				if requeued, rerr := rd.Requeue(); rerr == nil && requeued {
					obs.BufferRequeued.WithLabelValues(s.kind).Inc()
				} else if rerr != nil && queueDead(rerr) {
					s.buf.recover(q)
				}
			}
		}
	}
}

// commit retires one delivered (or dropped) record; a dead queue handle is
// replaced, and the record simply redelivers after the reopen — at-least-once
// duplicates, never loss. It takes the queue handle the CALLER observed:
// recover's guard compares against it to decide whether someone else already
// replaced the handle, so re-reading the current one here defeated that guard
// and let a commit failure racing another recover reopen the queue twice.
func (s *sink[T]) commit(q *diskqueue.Queue[[]byte], rd *diskqueue.Reader[[]byte], off int64) {
	if err := rd.Commit(off); err != nil {
		s.log.Warn("disk buffer commit failed; the batch will redeliver", "signal", s.kind, "error", err)
		if queueDead(err) {
			s.buf.recover(q)
		}
	}
}
