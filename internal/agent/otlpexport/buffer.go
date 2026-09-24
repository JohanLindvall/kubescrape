package otlpexport

// One signal's durable queue (Buffer) and the recovery of a latched I/O
// failure: the handle lifecycle the Buffered facade and its sinks share.

import (
	"errors"
	"log/slog"
	"sync"

	"github.com/JohanLindvall/diskqueue"

	"github.com/JohanLindvall/kubescrape/internal/logdedupe"
	"github.com/JohanLindvall/kubescrape/internal/obs"
)

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
	// reopenWarns throttles the FAILED-reopen line. recover is reached from
	// every enqueue and every drain iteration while the handle stays dead, so
	// the condition repeats at the producers' rate; the keyless Throttle is
	// enough because a Buffer has exactly one of this condition.
	reopenWarns logdedupe.Throttle
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
	lost, reopenErr := b.swap(prev)
	// EMITTED WITH b.mu RELEASED, and that is the whole reason swap exists as
	// a separate function. b.mu is read by handles(), which every enqueue,
	// every drain iteration and every buffer-stats gauge evaluation goes
	// through, so it is as hot as the node's telemetry rate; a slog call is a
	// handler plus a write to stderr, and on a pod whose log collector has
	// backed up that write is unbounded. Holding the WRITE lock across it
	// would stall every producer on this buffer for as long as the collector
	// takes to read — during a disk failure, which is when the producers most
	// need to find out their batches are being refused. Decide under the lock,
	// render and emit after it.
	if reopenErr != nil {
		// The closed queue stays installed, so every later Add and commit
		// answers ErrClosed and comes back here — the reopen retries at the
		// callers' cadence rather than hot-looping. That is the right recovery
		// and it was completely silent: the enqueue side then counts
		// kubescrape_buffer_enqueue_errors_total forever while the reason the
		// spool cannot come back (the directory is gone, the mount is
		// read-only, the flock is held) lived only in this discarded error.
		if b.log != nil && b.reopenWarns.Allow(bufferWarnEvery) {
			b.log.Error("the disk buffer could not be reopened after an I/O failure; this signal has no durability "+
				"until it can be, and each retry happens on the next enqueue or drain",
				"signal", b.kind, "dir", b.dir, "error", reopenErr)
		}
		return
	}
	if lost > 0 && b.log != nil {
		b.log.Error("disk buffer lost data to damage discovered while recovering from an I/O failure",
			"signal", b.kind, "bytesLost", lost)
	}
}

// swap is recover's critical section: it performs the close-and-reopen under
// b.mu and RETURNS what happened, so the reporting can happen with the lock
// released. kind, dir and log are immutable after NewBuffered, so recover may
// read them outside it.
//
// The counters stay here rather than moving out with the lines: they are
// atomics, they cost nothing under the lock, and counting them exactly once
// per swap is the property worth keeping.
func (b *Buffer) swap(prev *diskqueue.Queue[[]byte]) (lost uint64, reopenErr error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed || b.q != prev {
		return 0, nil
	}
	_ = b.q.Close()
	q, err := diskqueue.New[[]byte](b.dir, marshalBytes, unmarshalBytes, b.opts)
	if err != nil {
		return 0, err
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
		if n := truncatedBytes(st); n > 0 {
			obs.BufferTruncated.WithLabelValues(b.kind).Add(float64(n))
			lost = n
		}
	}
	return lost, nil
}

// truncatedBytes is what a queue's recovery scan dropped or truncated away —
// data no drain will ever see, counted as kubescrape_buffer_truncated_bytes_total
// at open (NewBuffered) and after every latched-I/O reopen (swap).
func truncatedBytes(st diskqueue.Stats) uint64 {
	return st.LostBytes + st.ForeignBytes + st.DiscardedBytes
}

// queueDead reports an error that poisons the queue handle (recover applies).
func queueDead(err error) bool {
	return errors.Is(err, diskqueue.ErrIO) || errors.Is(err, diskqueue.ErrClosed)
}
