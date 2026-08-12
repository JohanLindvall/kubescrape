package otlpexport

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/zeebo/xxh3"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/JohanLindvall/diskqueue"

	"github.com/JohanLindvall/kubescrape/internal/obs"
)

// singleAttempt is the seam through which the disk-buffer drain reaches an
// exporter's SINGLE-ATTEMPT sends, bypassing the client's own bounded retry
// loop: the drain owns retry policy (a backoff persisting across queue
// cycles, requeue rotation, the poison budget), and the Exporter interface
// alone cannot say that ExportMetrics retries internally while ExportLogs
// does not. It replaces a concrete-type switch over *Client / *PerSignal
// whose default arm silently fell back to RETRIED sends for any other
// Exporter — the enumeration is now each implementation's own business, and
// the fallback is loud (NewBuffered warns). Unexported deliberately:
// single-attempt sends are this package's internals, and a foreign wrapper
// stacking its own retries is exactly what the warning exists to name.
type singleAttempt interface {
	singleAttemptSends() (logs func(context.Context, plog.Logs) error, metrics func(context.Context, pmetric.Metrics) error)
}

// Exporter exports logs and metrics; implemented by *Client and *Buffered, so
// the agent can route every consumer through one value whether or not
// buffering is enabled.
type Exporter interface {
	ExportLogs(ctx context.Context, ld plog.Logs) error
	ExportMetrics(ctx context.Context, md pmetric.Metrics) error
}

// TracesExporter exports traces. *Client implements it natively; *Buffered
// passes traces through to the inner exporter unbuffered UNLESS the payload is
// marked Own(ctx) — see owned.go: a trace is normally a forwarded push whose
// sender owns the retry, and the one exception is a tail-sampling decision,
// which acked its senders seconds earlier.
type TracesExporter interface {
	ExportTraces(ctx context.Context, td ptrace.Traces) error
}

// Buffered is a disk-backed write-ahead buffer in front of an exporter, for
// logs, metrics and OWNED traces. Export{Logs,Metrics} serialize the batch and
// append it to a durable on-disk queue (github.com/JohanLindvall/diskqueue, one
// per signal), returning as soon as it is persisted — so producers can commit
// their progress and source logs may rotate away while their data waits on the
// node. Run drains each queue to the real exporter with retries; a batch is
// removed only after the collector acknowledges it (at-least-once, surviving
// restarts). A full queue makes Export return diskqueue.ErrFull, which the
// tailer treats as a failure and rewinds — bounding disk use and
// back-pressuring to the source.
//
// ExportTraces is the exception, and it is a PER-PAYLOAD one: a plain forwarded
// trace passes straight through (the pushing application still holds it and its
// SDK retries — spooling would ack a sender for data that has not shipped),
// while a payload marked Own(ctx) is spooled like the other two signals. The
// only marker in this repo is agent/tailbuffer, whose senders were acked when
// their spans were BUFFERED and hold nothing by the time a decision ships. See
// owned.go for the whole argument. With no traces spool open, an owned payload
// passes through as before — the marker asks for durability, it does not
// require it.
type Buffered struct {
	inner   Exporter // direct path for a signal with no buffer
	logs    *sink[plog.Logs]
	metrics *sink[pmetric.Metrics]
	traces  *sink[ptrace.Traces] // nil unless a traces spool was opened
	log     *slog.Logger
	// drainGate holds ONE token, taken for the whole of Run and for the whole
	// of FinalDrain. Both drive the same diskqueue Readers and the same
	// per-sink bookkeeping (cur backoff, delivered, the stuck map), none of
	// which is synchronised — it never needed to be while a single drain
	// goroutine per signal owned it.
	//
	// FinalDrain is documented to run "once the producers have stopped", but
	// the agent joins them on a BOUNDED budget and Run is one of them: a Run
	// still inside a send cycle when the budget expires overlaps the FinalDrain
	// that follows, so the two share that state with no happens-before between
	// them. The token restores the one-drain-at-a-time invariant.
	//
	// A channel rather than a Mutex because the acquire must be CANCELLABLE:
	// FinalDrain has to honour its own deadline instead of blocking behind a
	// drain wedged against a dead collector, and giving up there is safe —
	// nothing is committed until the collector acks, so whatever is still
	// spooled redelivers on the next start of this pod on this node.
	drainGate chan struct{}
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
	// closed latches a DELIBERATE Close, which is otherwise indistinguishable
	// from a dead handle — both answer ErrClosed. Without the latch a straggler
	// enqueue after shutdown sends recover down the reopen path: a fresh
	// diskqueue with its own flock and its own preallocated segment, which
	// nothing will ever close, while the enqueue that triggered it still fails.
	// It is reachable because the agent joins its producers on a BUDGET, so one
	// wedged against a dead collector can outlive the join and reach the
	// deferred Close.
	closed bool
	// kind and log let recover() report what ITS reopen cost. Set by
	// NewBuffered, which is where the signal names live.
	kind string
	log  *slog.Logger
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

// retired reports that the handle the caller was using is no longer this
// buffer's — recover replaced it, or Close retired it for good. Taking the read
// lock waits out a recover that is mid-swap, so the answer is never "not yet".
func (b *Buffer) retired(q *diskqueue.Queue[[]byte]) bool {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.closed || b.q != q
}

// add durably enqueues one payload.
func (b *Buffer) add(data []byte) error {
	q, _ := b.handles()
	err := q.Add(data)
	// queueDead, not ErrIO alone: a handle CLOSED under a concurrent enqueue
	// (the recover path swaps it) is just as dead, and refusing forever without
	// attempting the reopen was a buffer that silently stopped accepting.
	if queueDead(err) {
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
//
// It takes the queue handle the CALLER observed, for the same reason commit
// does: nacking is a statement about the reservation the caller holds, and
// re-reading the current handle would apply it to whatever queue happens to be
// installed now — a handle on which this drain holds nothing. (Today one drain
// goroutine per sink means the replacement queue has no reservation to roll
// back, so the re-read was harmless; the two functions disagreeing about a
// rule both comments treat as load-bearing is the thing worth not leaving in
// place.) A retired handle answers ErrClosed, which recover's own guard then
// resolves to a no-op.
func (b *Buffer) rewindQ(q *diskqueue.Queue[[]byte]) {
	if _, err := q.Rewind(); err != nil && queueDead(err) {
		b.recover(q)
	}
}

// Close flushes and closes the queue; the error is worth logging — a latched
// I/O failure surfaces here.
func (b *Buffer) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.closed = true // a close on purpose is not a handle to recover (see Buffer.closed)
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
	if b.closed || b.q != prev {
		return
	}
	_ = b.q.Close()
	q, err := diskqueue.New[[]byte](b.dir, marshalBytes, unmarshalBytes, b.opts)
	if err != nil {
		return
	}
	b.q = q
	b.rd = q.NewReader()
	// A reopen runs diskqueue's recovery scan again, and whatever IT drops or
	// truncates is data no drain will ever see — exactly the loss counted at
	// startup. It was sampled only in NewBuffered, so corruption discovered by
	// a latched-I/O recovery (the very situation most likely to have damaged
	// the queue) passed with the dedicated loss counter flat.
	if b.kind != "" {
		st := q.Stats()
		if lost := st.LostBytes + st.ForeignBytes + st.DiscardedBytes; lost > 0 {
			obs.BufferTruncated.WithLabelValues(b.kind).Add(float64(lost))
			if b.log != nil {
				b.log.Error("disk buffer lost data to damage discovered while recovering from an I/O failure",
					"signal", b.kind, "bytes_lost", lost)
			}
		}
	}
}

// queueDead reports an error that poisons the queue handle (recover applies).
func queueDead(err error) bool {
	return errors.Is(err, diskqueue.ErrIO) || errors.Is(err, diskqueue.ErrClosed)
}

// NewBuffered wraps inner. logBuf, metricBuf and traceBuf back the three
// signals; any of them may be nil to leave that signal unbuffered (exported
// directly). traceBuf is only ever consulted for a payload marked Own(ctx) —
// pass nil unless something in the chain marks one, or the queue is a directory
// and a segment file that never receive a record.
func NewBuffered(inner Exporter, logBuf, metricBuf, traceBuf *Buffer, backoff time.Duration, log *slog.Logger) *Buffered {
	if backoff <= 0 {
		backoff = time.Second
	}
	if log == nil {
		log = slog.Default()
	}
	b := &Buffered{inner: inner, log: log, drainGate: make(chan struct{}, 1)}
	b.drainGate <- struct{}{} // the token starts free: FinalDrain without a Run must not wait
	// The drain owns retry policy; reach the inner exporter's single-attempt
	// sends through the singleAttempt seam so attempts do not multiply (drain
	// x client = up to 15 wire sends per cycle otherwise). The fallback for an
	// exporter outside this package is LOUD: stacked retries are a behavior
	// change worth a log line, not a silent property of whoever wired the
	// stack.
	sendLogs, sendMetrics := inner.ExportLogs, inner.ExportMetrics
	if sa, ok := inner.(singleAttempt); ok {
		sendLogs, sendMetrics = sa.singleAttemptSends()
	} else {
		log.Warn("inner exporter exposes no single-attempt sends; the drain's retries will stack on the exporter's own",
			"type", fmt.Sprintf("%T", inner))
	}
	// The raw seam is separate and optional: a spooled payload is already the
	// wire body, so the common drain path sends the reserved bytes verbatim and
	// materializes pdata only where it is genuinely needed (an over-cap payload
	// to split, a drop whose counter reports records). An exporter outside this
	// package cannot promise that equivalence and simply does not get it.
	var rawLogs, rawMetrics, rawTraces func(context.Context, []byte) error
	var rawMaxBytes int
	if ra, ok := inner.(rawSingleAttempt); ok {
		rawLogs, rawMetrics, rawTraces, rawMaxBytes = ra.rawSingleAttemptSends()
	}
	// Report what corruption cost at open: everything the recovery scan
	// dropped or truncated away is data no drain will ever see.
	for kind, buf := range map[string]*Buffer{"logs": logBuf, "metrics": metricBuf, "traces": traceBuf} {
		if buf == nil {
			continue
		}
		buf.kind, buf.log = kind, log
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
			count:     plog.Logs.LogRecordCount,
			send:      sendLogs,
			sendRaw:   rawLogs, maxSendBytes: rawMaxBytes,
		}
	}
	if metricBuf != nil {
		mm := pmetric.ProtoMarshaler{}
		mu := pmetric.ProtoUnmarshaler{}
		b.metrics = &sink[pmetric.Metrics]{
			buf: metricBuf, backoff: backoff, log: log, kind: "metrics",
			marshal:   mm.MarshalMetrics,
			unmarshal: mu.UnmarshalMetrics,
			count:     pmetric.Metrics.DataPointCount,
			send:      sendMetrics,
			sendRaw:   rawMetrics, maxSendBytes: rawMaxBytes,
		}
	}
	if te, ok := inner.(TracesExporter); ok && traceBuf != nil {
		tm := ptrace.ProtoMarshaler{}
		tu := ptrace.ProtoUnmarshaler{}
		b.traces = &sink[ptrace.Traces]{
			buf: traceBuf, backoff: backoff, log: log, kind: "traces",
			marshal:   tm.MarshalTraces,
			unmarshal: tu.UnmarshalTraces,
			count:     ptrace.Traces.SpanCount,
			// No counted-unwrap twin here (as sendLogs/sendMetrics have):
			// Client.ExportTraces is ALREADY a single counted attempt — the
			// pushing sender's retry has always been the trace path's retry —
			// so there is no client-side loop for the drain's own retries to
			// multiply with.
			send:    te.ExportTraces,
			sendRaw: rawTraces, maxSendBytes: rawMaxBytes,
		}
	} else if traceBuf != nil {
		log.Error("a traces disk buffer was opened but the exporter does not support traces; owned trace payloads will be refused")
	}
	return b
}

// ExportTraces spools an OWNED payload (see Own) and passes every other one
// straight to the inner exporter.
//
// The asymmetry with logs and metrics is the point: a forwarded trace is still
// held by the application that pushed it, and its SDK's retry is a better
// durability story than this spool — acking that sender to put its data in a
// queue would REMOVE the only other copy. A tail-sampling decision is the
// opposite case (its senders were acked when the spans were buffered, and are
// long gone), and it says so by marking the context.
func (b *Buffered) ExportTraces(ctx context.Context, td ptrace.Traces) error {
	if b.traces != nil && Owned(ctx) {
		return b.traces.enqueue(td)
	}
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

// acquireDrain takes the single drain token, or reports false when ctx ends
// first. See Buffered.drainGate.
func (b *Buffered) acquireDrain(ctx context.Context) bool {
	select {
	case <-b.drainGate:
		return true
	case <-ctx.Done():
		return false
	}
}

func (b *Buffered) releaseDrain() { b.drainGate <- struct{}{} }

// Run drains both spools until ctx is done.
func (b *Buffered) Run(ctx context.Context) {
	if !b.acquireDrain(ctx) {
		return // already cancelled, or another drain pass owns the queues
	}
	defer b.releaseDrain()
	var wg sync.WaitGroup
	for _, run := range []func(context.Context){b.logs.drain, b.metrics.drain, b.traces.drain} {
		wg.Add(1)
		go func(r func(context.Context)) { defer wg.Done(); r(ctx) }(run)
	}
	wg.Wait()
}

// Stats reports each signal's disk-buffer occupancy, for the gauges that make
// a filling buffer visible BEFORE it starts refusing writes.
func (b *Buffered) Stats() map[string]obs.BufferStat {
	out := map[string]obs.BufferStat{}
	for kind, buf := range map[string]*Buffer{"logs": b.logs.bufOf(), "metrics": b.metrics.bufOf(), "traces": b.traces.bufOf()} {
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
//
// Run is one of the goroutines joined on that budget, and it drives the same
// queue readers and per-sink bookkeeping this pass does, so this first takes
// the drain token (Buffered.drainGate) — waiting for a Run that has not
// finished its cycle, WITHIN ctx. If the token does not come free in time this
// returns having drained nothing rather than racing the running drain: the
// spool is durable and redelivers, which is the better of the two outcomes a
// spent shutdown budget leaves.
func (b *Buffered) FinalDrain(ctx context.Context) {
	if !b.acquireDrain(ctx) {
		b.log.Warn("skipping the final disk-buffer drain: the background drain did not stop within the shutdown budget; " +
			"the spool is durable and redelivers on the next start")
		return
	}
	defer b.releaseDrain()
	var wg sync.WaitGroup
	for _, run := range []func(context.Context){b.logs.drainUntilEmpty, b.metrics.drainUntilEmpty, b.traces.drainUntilEmpty} {
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
		// pdata proto marshal is effectively infallible, but a payload that
		// cannot SERIALIZE can never enqueue: returned bare (and hence
		// transient) the tailer would rewind and rebuild the same batch
		// forever with every buffer metric flat. Count it with the other
		// non-capacity refusals and classify it permanent.
		obs.BufferEnqueueErrors.WithLabelValues(s.kind).Inc()
		return fmt.Errorf("%w: %v", errUnmarshalable, err)
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
				obs.BufferReadErrors.WithLabelValues(s.kind, boolLabel(lost)).Inc()
				s.log.Error("disk buffer read failed", "signal", s.kind, "data_lost", lost, "error", err)
			}
			if queueDead(err) {
				s.buf.recover(q)
			}
			if !lost {
				if untilEmpty {
					return // a dead disk must not spin the bounded shutdown pass
				}
				// NewTimer+Stop, not time.After — the rule Retry and trySend
				// already follow: a cancelled wait must not leave the timer live
				// until it fires, and at SIGTERM every draining sink abandons
				// its wait at once.
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
// duplicates, never loss.
// commit takes the queue handle the CALLER observed. recover's guard compares
// against it to decide whether someone else already replaced the handle, so
// re-reading the current one here defeated that guard and let a commit failure
// racing another recover reopen the queue twice.
func (s *sink[T]) commit(q *diskqueue.Queue[[]byte], rd *diskqueue.Reader[[]byte], off int64) {
	if err := rd.Commit(off); err != nil {
		s.log.Warn("disk buffer commit failed; the batch will redeliver", "signal", s.kind, "error", err)
		if queueDead(err) {
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
//
// The key is 128 bits because a collision here DESTROYS GOOD DATA rather than
// merely mis-measuring: two distinct payloads sharing a slot pool their
// accountable-failure counts, so a healthy batch can inherit a poison batch's
// budget and be dropped after fewer laps than maxDrainCycles allows. The map is
// keyed by hash alone — there is no stored payload to re-compare against on a
// hit, which is what would otherwise turn a collision back into a miss — so the
// width IS the whole defence.
func (s *sink[T]) stuckTooLong(data []byte) bool {
	h := xxh3.Hash128(data)
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
	// accountableLap reads these same two fields BEFORE the send to decide
	// whether the lap may end after one attempt; keep them the one predicate, or
	// trySend starts shortening laps that spend no budget.
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
		delete(s.stuck, xxh3.Hash128(data))
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
	st, seen := s.stuck[xxh3.Hash128(data)]
	return seen && s.delivered > st.lastDelivered
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
	// stuckAfterAttempts sends; success resets it.
	if s.cur < s.backoff {
		s.cur = s.backoff
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
			s.log.Warn("buffered export still failing, requeueing", "signal", s.kind, "error", err, "attempts", attempt)
			return sendStuck
		}
		s.log.Warn("buffered export failed, retrying", "signal", s.kind, "error", err, "backoff", s.cur)
		// NewTimer+Stop, not time.After (otlpexport.Retry carries the whole
		// argument): a cancelled wait must not leave the timer live until it
		// fires, and at SIGTERM every draining sink abandons its wait at once.
		t := time.NewTimer(s.cur)
		select {
		case <-ctx.Done():
			t.Stop()
			return sendCancelled
		case <-t.C:
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
// errUnmarshalable marks a payload that cannot serialize for the disk buffer:
// permanent by definition — retrying cannot change the payload's own
// encodability.
var errUnmarshalable = errors.New("payload does not marshal")

func IsPermanent(err error) bool {
	if errors.Is(err, diskqueue.ErrRecordTooLarge) || errors.Is(err, errUnmarshalable) {
		return true
	}
	var he *HTTPStatusError
	if errors.As(err, &he) {
		switch he.Code {
		// 501 is the HTTP spelling of gRPC Unimplemented, which is permanent
		// below: without it the same collector-serves-no-such-signal mistake
		// was a counted drop over gRPC and an unbounded rewind-and-rebuild
		// loop over OTLP/HTTP. (404 stays transient DELIBERATELY: a
		// collector's routes reprogram during a rollout, and draining a
		// backlog into drops for a path that returns tomorrow is the wrong
		// trade.)
		case 400, 405, 413, 414, 415, 422, 431, 501:
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
