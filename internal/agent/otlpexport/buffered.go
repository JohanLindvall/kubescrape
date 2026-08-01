package otlpexport

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/cespare/xxhash/v2"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/JohanLindvall/diskqueue"

	"github.com/JohanLindvall/kubescrape/internal/obs"
)

// Exporter exports logs and metrics; implemented by *Client and *Buffered, so
// the agent can route every consumer through one value whether or not
// buffering is enabled.
type Exporter interface {
	ExportLogs(ctx context.Context, ld plog.Logs) error
	ExportMetrics(ctx context.Context, md pmetric.Metrics) error
}

// TracesExporter exports traces. *Client implements it natively; *Buffered
// passes traces through to the inner exporter unbuffered (traces are a
// passthrough signal — the pushing sender owns retry).
type TracesExporter interface {
	ExportTraces(ctx context.Context, td ptrace.Traces) error
}

// Buffered is a disk-backed write-ahead buffer in front of an exporter, for
// both logs and metrics. Export{Logs,Metrics} serialize the batch and append it
// to a durable on-disk queue (github.com/JohanLindvall/diskqueue, one per
// signal), returning as soon as it is persisted — so producers can commit
// their progress and source logs may rotate away while their data waits on the
// node. Run drains each queue to the real exporter with retries; a batch is
// removed only after the collector acknowledges it (at-least-once, surviving
// restarts). A full queue makes Export return diskqueue.ErrFull, which the
// tailer treats as a failure and rewinds — bounding disk use and
// back-pressuring to the source.
type Buffered struct {
	inner   Exporter // direct path for a signal with no buffer
	logs    *sink[plog.Logs]
	metrics *sink[pmetric.Metrics]
}

// Buffer is one signal's durable queue plus what recovering from a latched
// I/O failure needs. diskqueue latches a failed fsync (every later Add,
// commit and Sync returns ErrIO — a retried fsync can report success over
// pages the kernel already dropped) and its documented recovery is
// close-and-reopen; recover does exactly that, guarded so the enqueue side
// (producer goroutines) and the drain side never race the swap.
type Buffer struct {
	mu   sync.RWMutex
	q    *diskqueue.Queue[[]byte]
	rd   *diskqueue.Reader[[]byte]
	dir  string
	opts diskqueue.Options
}

// marshalBytes/unmarshalBytes are the identity codec: the sink owns the pdata
// (un)marshaling, the queue stores opaque bytes. unmarshalBytes ALIASES the
// reader's buffer — valid until that reader's next read — which the single
// drain goroutine respects: every use of a reserved payload (pdata unmarshal,
// stuck-tracking hash) happens before it touches the queue again.
func marshalBytes(dst []byte, v []byte) ([]byte, error) { return append(dst, v...), nil }
func unmarshalBytes(data []byte) ([]byte, error)        { return data, nil }

// OpenBuffer opens (or creates) one signal's queue directory. maxBytes caps
// the undelivered backlog (0 = uncapped); the segment-count cap is disabled —
// MaxBytes is the one knob operators size, and a count cap would bind behind
// its back at 32 x 8 MiB — with open descriptors bounded instead.
func OpenBuffer(dir string, maxBytes int64) (*Buffer, error) {
	opts := diskqueue.Options{
		MaxBytes:     maxBytes,
		MaxSegments:  -1, // unbounded: MaxBytes is the budget
		MaxOpenFiles: 8,  // a deep backlog must not hold an fd per segment
	}
	q, err := diskqueue.New[[]byte](dir, marshalBytes, unmarshalBytes, opts)
	if err != nil {
		return nil, err
	}
	return &Buffer{q: q, rd: q.NewReader(), dir: dir, opts: opts}, nil
}

// handles returns the current queue/reader pair; recover swaps them.
func (b *Buffer) handles() (*diskqueue.Queue[[]byte], *diskqueue.Reader[[]byte]) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.q, b.rd
}

// add durably enqueues one payload.
func (b *Buffer) add(data []byte) error {
	q, _ := b.handles()
	err := q.Add(data)
	if errors.Is(err, diskqueue.ErrIO) {
		b.recover(q)
	}
	return err
}

// Bytes is the undelivered backlog in bytes (in-flight reservations
// included — they are uncommitted until the collector acks).
func (b *Buffer) Bytes() int64 { return b.stats().BacklogBytes }

// stats snapshots the queue's occupancy.
func (b *Buffer) stats() diskqueue.Stats {
	q, _ := b.handles()
	return q.Stats()
}

// rewindQ returns in-flight (reserved, uncommitted) records to the queue — the
// bulk nack used when a send is cancelled or a stuck head goes back for
// rotation.
func (b *Buffer) rewindQ() {
	q, _ := b.handles()
	if _, err := q.Rewind(); err != nil && queueDead(err) {
		b.recover(q)
	}
}

// Close flushes and closes the queue; the error is worth logging — a latched
// I/O failure surfaces here.
func (b *Buffer) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.q.Close()
}

// recover replaces a queue whose durability failed (latched ErrIO) or that was
// closed under the caller, per diskqueue's close-and-reopen contract. prev
// guards the swap: whichever caller loses the race sees b.q already replaced
// and does nothing. On a reopen failure the closed queue stays in place — its
// ErrClosed sends the next caller back here, so the reopen retries at the
// callers' own cadence rather than hot-looping.
func (b *Buffer) recover(prev *diskqueue.Queue[[]byte]) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.q != prev {
		return
	}
	_ = b.q.Close()
	q, err := diskqueue.New[[]byte](b.dir, marshalBytes, unmarshalBytes, b.opts)
	if err != nil {
		return
	}
	b.q = q
	b.rd = q.NewReader()
}

// queueDead reports an error that poisons the queue handle (recover applies).
func queueDead(err error) bool {
	return errors.Is(err, diskqueue.ErrIO) || errors.Is(err, diskqueue.ErrClosed)
}

// NewBuffered wraps inner. logBuf and metricBuf back the two signals; either
// may be nil to leave that signal unbuffered (exported directly).
func NewBuffered(inner Exporter, logBuf, metricBuf *Buffer, backoff time.Duration, log *slog.Logger) *Buffered {
	if backoff <= 0 {
		backoff = time.Second
	}
	if log == nil {
		log = slog.Default()
	}
	b := &Buffered{inner: inner}
	// The drain owns retry policy; when the inner exporter is the raw client
	// (or the per-signal mux over raw clients), bypass its own bounded retries
	// so attempts do not multiply (drain x client = up to 15 wire sends per
	// cycle otherwise).
	sendLogs, sendMetrics := inner.ExportLogs, inner.ExportMetrics
	switch c := inner.(type) {
	case *Client:
		sendLogs, sendMetrics = c.exportLogsCounted, c.exportMetricsCounted
	case *PerSignal:
		sendLogs, sendMetrics = c.logsClient().exportLogsCounted, c.metricsClient().exportMetricsCounted
	}
	// Report what corruption cost at open: everything the recovery scan
	// dropped or truncated away is data no drain will ever see.
	for kind, buf := range map[string]*Buffer{"logs": logBuf, "metrics": metricBuf} {
		if buf == nil {
			continue
		}
		st := buf.stats()
		if lost := st.LostBytes + st.ForeignBytes + st.DiscardedBytes; lost > 0 {
			obs.BufferTruncated.WithLabelValues(kind).Add(float64(lost))
			log.Error("disk buffer lost data to damage discovered at open",
				"signal", kind, "bytes_lost", lost)
		}
	}
	if logBuf != nil {
		lm := plog.ProtoMarshaler{}
		lu := plog.ProtoUnmarshaler{}
		b.logs = &sink[plog.Logs]{
			buf: logBuf, backoff: backoff, log: log, kind: "logs",
			marshal:   lm.MarshalLogs,
			unmarshal: lu.UnmarshalLogs,
			send:      sendLogs,
		}
	}
	if metricBuf != nil {
		mm := pmetric.ProtoMarshaler{}
		mu := pmetric.ProtoUnmarshaler{}
		b.metrics = &sink[pmetric.Metrics]{
			buf: metricBuf, backoff: backoff, log: log, kind: "metrics",
			marshal:   mm.MarshalMetrics,
			unmarshal: mu.UnmarshalMetrics,
			send:      sendMetrics,
		}
	}
	return b
}

// ExportTraces passes traces to the inner exporter unbuffered.
func (b *Buffered) ExportTraces(ctx context.Context, td ptrace.Traces) error {
	if te, ok := b.inner.(TracesExporter); ok {
		return te.ExportTraces(ctx, td)
	}
	return errors.New("inner exporter does not support traces")
}

// ExportLogs durably enqueues a log batch (Run sends it); with no log spool it
// exports directly.
func (b *Buffered) ExportLogs(ctx context.Context, ld plog.Logs) error {
	if b.logs == nil {
		return b.inner.ExportLogs(ctx, ld)
	}
	return b.logs.enqueue(ld)
}

// ExportMetrics durably enqueues a metric batch (Run sends it); with no metric
// spool it exports directly.
func (b *Buffered) ExportMetrics(ctx context.Context, md pmetric.Metrics) error {
	if b.metrics == nil {
		return b.inner.ExportMetrics(ctx, md)
	}
	return b.metrics.enqueue(md)
}

// Run drains both spools until ctx is done.
func (b *Buffered) Run(ctx context.Context) {
	var wg sync.WaitGroup
	for _, run := range []func(context.Context){b.logs.drain, b.metrics.drain} {
		wg.Add(1)
		go func(r func(context.Context)) { defer wg.Done(); r(ctx) }(run)
	}
	wg.Wait()
}

// Stats reports each signal's disk-buffer occupancy, for the gauges that make
// a filling buffer visible BEFORE it starts refusing writes.
func (b *Buffered) Stats() map[string]obs.BufferStat {
	out := map[string]obs.BufferStat{}
	for kind, buf := range map[string]*Buffer{"logs": b.logs.bufOf(), "metrics": b.metrics.bufOf()} {
		if buf == nil {
			continue
		}
		st := buf.stats()
		out[kind] = obs.BufferStat{Backlog: st.BacklogBytes, Cap: st.MaxBytes, Segments: st.Segments}
	}
	return out
}

// FinalDrain empties both spools, returning as soon as they run dry (or when
// ctx expires). Run stops the instant its context is cancelled, so everything
// exported after SIGTERM — the tailer's and journald's last flushes, the
// ingest server's in-flight forwards, and the final log-metrics and
// self-metrics windows, which are exported only after those goroutines have
// joined — reaches the spool and nothing carries it further. Without this pass that data waits for
// the next start of this pod ON THIS NODE, and is lost outright when the pod
// never returns (node drained or scaled away, release uninstalled) or the
// buffer dir is not a persistent mount. Call it with a bounded fresh context
// once the producers have stopped, before closing the exporter and spools.
//
// "Once the producers have stopped" is best-effort at the caller: the agent
// joins them on a shutdown BUDGET, so a producer stuck against a dead
// collector can still enqueue while this runs, and whatever it enqueues after
// the drain runs dry stays spooled. That is durability-preserving — the spool
// is committed, so the next start of this pod on this node redelivers it —
// but it is not the same as "nothing is left behind", and the drain does not
// wait for a straggler it cannot bound.
func (b *Buffered) FinalDrain(ctx context.Context) {
	var wg sync.WaitGroup
	for _, run := range []func(context.Context){b.logs.drainUntilEmpty, b.metrics.drainUntilEmpty} {
		wg.Add(1)
		go func(r func(context.Context)) { defer wg.Done(); r(ctx) }(run)
	}
	wg.Wait()
}

// sink is one signal's buffer: a queue plus the (un)marshal and send functions
// for its pdata type.
type sink[T any] struct {
	buf       *Buffer
	marshal   func(T) ([]byte, error)
	unmarshal func([]byte) (T, error)
	send      func(context.Context, T) error
	backoff   time.Duration
	cur       time.Duration // current backoff, persisted across trySend cycles of a failing head
	log       *slog.Logger
	kind      string
	// delivered counts batches this sink has successfully exported; stuck
	// tracks, per stuck payload (keyed by content hash), its accountable failed
	// cycles and the value of delivered at its own previous failed cycle — the
	// two together are the poison evidence (see stuckTooLong).
	delivered uint64
	stuck     map[uint64]stuckBatch
	// stuckResponded records whether the latest sendStuck's final error carried
	// a RESPONSE from the collector (vs a transport failure); set by trySend,
	// read by stuckTooLong (both on the drain goroutine).
	stuckResponded bool
}

type stuckBatch struct {
	cycles        int
	lastDelivered uint64 // s.delivered at this payload's previous failed cycle
}

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

func (s *sink[T]) enqueue(v T) error {
	data, err := s.marshal(v)
	if err != nil {
		return err
	}
	err = s.buf.add(data)
	switch {
	case err == nil:
	case errors.Is(err, diskqueue.ErrFull) || errors.Is(err, diskqueue.ErrRecordTooLarge):
		// Back-pressure for logs (the tailer rewinds and re-reads), but a lost
		// batch for every producer that cannot rewind — the scraper, the
		// self-metrics registry, the log-metric exporter. Count it: an
		// undelivered metric batch must never disappear silently.
		// ErrRecordTooLarge is one batch larger than the whole cap — a config
		// problem, but the producer-facing handling is the same refusal.
		obs.BufferFull.WithLabelValues(s.kind).Inc()
	default:
		// EVERY other refusal is counted too, and separately: a latched I/O
		// error, a closed queue, or the raw *os.PathError diskqueue returns when
		// segment preallocation hits ENOSPC. These were returned bare, so a full
		// disk made a node go dark with every buffer metric flat — _full_total
		// is the wrong class, _dropped_total is drain-side, and obs.Exports sits
		// BELOW the buffer and is never reached.
		obs.BufferEnqueueErrors.WithLabelValues(s.kind).Inc()
	}
	return err
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
			obs.BufferReadErrors.WithLabelValues(s.kind, boolLabel(lost)).Inc()
			s.log.Error("disk buffer read failed", "signal", s.kind, "data_lost", lost, "error", err)
			if queueDead(err) {
				s.buf.recover(q)
			}
			if !lost {
				if untilEmpty {
					return // a dead disk must not spin the bounded shutdown pass
				}
				select {
				case <-ctx.Done():
					return
				case <-time.After(time.Second):
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
		v, err := s.unmarshal(data)
		if err != nil {
			// Undecodable payload: the data is gone either way, but the drop
			// must be counted like every other one.
			obs.BufferDropped.WithLabelValues(s.kind).Inc()
			s.log.Warn("dropping corrupt buffered batch", "signal", s.kind, "error", err)
			s.commit(rd, off)
			continue
		}
		switch s.trySend(ctx, v) {
		case sendOK:
			s.forget(data) // a previously-stuck payload that recovered; hash before the next queue op
			s.commit(rd, off)
			s.delivered++ // proof the collector is alive: see stuckTooLong
		case sendCancelled:
			// ctx cancelled mid-send: nack the reservation so the batch is
			// queued for the next run (or the final drain).
			s.buf.rewindQ()
			return
		case sendRejected:
			// A definitive rejection (bad payload, auth, unimplemented):
			// retrying cannot fix it and keeping it would block the queue.
			s.log.Error("dropping buffered batch permanently rejected by the collector", "signal", s.kind)
			obs.BufferDropped.WithLabelValues(s.kind).Inc()
			s.forget(data) // a batch that got stuck then turned permanent must not leak its entry
			s.commit(rd, off)
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
				s.log.Error("dropping buffered batch the collector never accepted",
					"signal", s.kind, "cycles", maxDrainCycles, "bytes", len(data))
				obs.BufferDropped.WithLabelValues(s.kind).Inc()
				s.commit(rd, off)
				continue
			}
			s.buf.rewindQ()
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
// duplicates, never loss.
func (s *sink[T]) commit(rd *diskqueue.Reader[[]byte], off int64) {
	if err := rd.Commit(off); err != nil {
		s.log.Warn("disk buffer commit failed; the batch will redeliver", "signal", s.kind, "error", err)
		if queueDead(err) {
			q, _ := s.buf.handles()
			s.buf.recover(q)
		}
	}
}

func boolLabel(b bool) string {
	if b {
		return "true"
	}
	return "false"
}

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
func (s *sink[T]) stuckTooLong(data []byte) bool {
	h := xxhash.Sum64(data)
	if s.stuck == nil {
		s.stuck = make(map[uint64]stuckBatch)
	}
	st, seen := s.stuck[h]
	if !seen && len(s.stuck) >= maxStuckTracked {
		return false
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
	progressed := seen && s.delivered > st.lastDelivered
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

// respondedError reports whether err carries a response FROM the collector (an
// HTTP status or a gRPC status), as opposed to a transport-level failure where
// the collector may simply be down. Unavailable/DeadlineExceeded/Canceled are
// treated as transport even when server-sent — the conservative direction: an
// outage must never count toward the poison budget.
func respondedError(err error) bool {
	var he *HTTPStatusError
	if errors.As(err, &he) {
		// Throttling and server-side outages are the HTTP counterparts of the
		// gRPC codes excluded below, and OTLP defines them as RETRYABLE: a
		// collector answering 429/503 while it accepts other batches is
		// back-pressuring, not rejecting this payload. Counting them as poison
		// evidence dropped perfectly good data during exactly the outage the
		// disk buffer exists to survive.
		switch {
		case he.Code == http.StatusRequestTimeout, // 408
			he.Code == http.StatusTooManyRequests, // 429
			he.Code >= 500:                        // 502/503/504 and friends
			return false
		}
		return true
	}
	if st, ok := status.FromError(err); ok {
		switch st.Code() {
		case codes.Unavailable, codes.DeadlineExceeded, codes.Canceled:
			return false
		}
		return true
	}
	return false
}

// forget drops a payload's stuck-tracking once it is committed, so a batch that
// eventually succeeds does not leak an entry to the maxStuckTracked cap.
func (s *sink[T]) forget(data []byte) {
	if s.stuck != nil {
		delete(s.stuck, xxhash.Sum64(data))
	}
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

// trySend retries with backoff until the exporter accepts the batch, the
// error is a permanent rejection, the attempt budget is spent, or ctx is
// cancelled.
func (s *sink[T]) trySend(ctx context.Context, v T) sendResult {
	// The backoff persists across trySend cycles (s.cur) so a long outage
	// actually reaches the 30s cap instead of restarting at s.backoff every
	// stuckAfterAttempts sends; success resets it.
	if s.cur < s.backoff {
		s.cur = s.backoff
	}
	for attempt := 1; ; attempt++ {
		err := s.send(ctx, v)
		if err == nil {
			s.cur = 0
			return sendOK
		}
		if IsPermanent(err) {
			s.cur = 0 // the connection demonstrably works; don't penalize the next batch
			s.log.Warn("buffered export permanently rejected", "signal", s.kind, "error", err)
			return sendRejected
		}
		if attempt >= stuckAfterAttempts {
			s.stuckResponded = respondedError(err)
			s.log.Warn("buffered export still failing, requeueing", "signal", s.kind, "error", err, "attempts", attempt)
			return sendStuck
		}
		s.log.Warn("buffered export failed, retrying", "signal", s.kind, "error", err, "backoff", s.cur)
		select {
		case <-ctx.Done():
			return sendCancelled
		case <-time.After(s.cur):
		}
		if s.cur *= 2; s.cur > 30*time.Second {
			s.cur = 30 * time.Second
		}
	}
}

// IsPermanent reports whether err is a definitive collector rejection that
// retrying cannot fix (bad payload, unimplemented signal). Everything
// ambiguous is transient. Deliberately transient despite the OTLP spec
// listing them non-retryable: auth failures (401/403, Unauthenticated,
// PermissionDenied) and 404 — a rotating bearer token, a collector rolling
// out behind an ingress, or a route being reprogrammed produces them for
// windows the disk buffer exists to survive; the bounded requeue path caps
// their cost, whereas classifying them permanent drains the whole backlog
// into drops. OutOfRange is retryable per the OTLP failure table.
//
// ErrRecordTooLarge is the one NON-collector error classified permanent:
// with -buffer-dir the producer's Export returns the enqueue error rather
// than any collector verdict, and a batch larger than the whole buffer cap
// can never fit however often it is retried. Left transient, the tailer
// rewound and rebuilt the identical batch every sweep - wedging its single
// sweep goroutine, and with it log shipping for every file on the node -
// while the counter added for exactly this class stayed at 0. ErrFull is
// deliberately NOT here: that one drains.
func IsPermanent(err error) bool {
	if errors.Is(err, diskqueue.ErrRecordTooLarge) {
		return true
	}
	var he *HTTPStatusError
	if errors.As(err, &he) {
		switch he.Code {
		case 400, 405, 413, 414, 415, 422, 431:
			return true
		}
		return false
	}
	if st, ok := status.FromError(err); ok {
		switch st.Code() {
		case codes.InvalidArgument, codes.Unimplemented, codes.FailedPrecondition:
			return true
		}
	}
	return false
}
