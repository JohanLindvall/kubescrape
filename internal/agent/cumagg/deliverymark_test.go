package cumagg

import (
	"context"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

// chunkDwell is how long the mark spends in each chunk after the first. See the
// ResetExemplars hook for why the pass has to be slow to be observable.
const chunkDwell = 20 * time.Millisecond

// markHarness drives one Export and parks the delivery mark inside its FIRST
// chunk, so the test can put a waiter on the series mutex while the mark cannot
// make progress: the test releases it only once the waiter is PARKED on the
// mutex (waitUntilParkedOnMutex), never after a guessed sleep.
//
// What remains is the scheduler, and it is named rather than hidden. The waiter
// is served mid-pass through sync.Mutex's starvation mode: woken by the first
// chunk's Unlock, it finds the lock retaken, notices it has waited over a
// millisecond, and is handed the lock directly at the next Unlock. That needs
// the woken goroutine to RUN during one of the later chunks' dwells (see the
// ResetExemplars hook), and the tests give it seven chances of chunkDwell each.
// Unchunked, no scheduling can serve it before the whole pass ends, so the
// assertion still fails deterministically when the chunking is removed.
type markHarness struct {
	store   *Store[*series]
	calls   atomic.Int64  // ResetExemplars calls, i.e. series marked so far
	parked  chan struct{} // closed from inside the first chunk's lock hold
	release chan struct{} // closed by the test to let that hold end
	once    sync.Once
}

func newMarkHarness(maxCard int) *markHarness {
	h := &markHarness{parked: make(chan struct{}), release: make(chan struct{})}
	now := time.Unix(1_700_000_000, 0)
	h.store = NewStore(Options[*series]{
		Scope:          "test",
		Name:           "test metrics",
		MaxCardinality: maxCard,
		// Eviction off: this is about the pass that runs AFTER the send.
		Now:       func() time.Time { return now },
		NewSeries: func() *series { return &series{} },
		Render: func(sm pmetric.ScopeMetrics, _ time.Time) {
			h.store.Lock()
			defer h.store.Unlock()
			dps := SumMetric(sm, "calls", "", "")
			for _, s := range h.store.series {
				h.store.MarkRenderedLocked(s)
				dps.AppendEmpty().SetIntValue(int64(s.calls))
			}
		},
		ResetExemplars: func(*series) {
			n := h.calls.Add(1)
			if n == 1 {
				// Under the mark's own lock. Holding here until the test
				// releases is what makes the waiter's acquisition below a
				// consequence of the CHUNKING and not of scheduling luck.
				h.once.Do(func() { close(h.parked) })
				<-h.release
				return
			}
			// The first series of every LATER chunk dwells, so the pass takes
			// milliseconds rather than microseconds. Without it the whole
			// remainder runs inside one scheduling quantum: a woken waiter
			// never gets to run, sync.Mutex therefore never enters the
			// starvation mode that hands the lock over, and the test measures
			// the scheduler instead of the chunking. The dwell is under the
			// lock, so it widens no gap — it only gives the waiter time to
			// wake, notice it has waited, and queue for the handoff.
			if (n-1)%snapChunk == 0 {
				time.Sleep(chunkDwell)
			}
		},
	})
	return h
}

func (h *markHarness) ExportMetrics(context.Context, pmetric.Metrics) error { return nil }

// The delivery mark is the pass an export makes after the SEND, over every
// series the render marked, and it takes the mutex Record/observe take per edge
// and per span — the pairing store from inside its own. Held whole it was a stall nothing bounded, linear
// in MaxCardinality, i.e. the one part of an export that kept growing after the
// render's was chunked. So it releases the lock between chunks, and this is what
// says so: a waiter queued while the first chunk is parked is served while
// series are still unmarked. Unchunked, the same waiter is served only once
// every series has been marked.
func TestDeliveryMarkReleasesTheLockBetweenChunks(t *testing.T) {
	const series = 8 * snapChunk
	h := newMarkHarness(series + 1)
	now := time.Unix(1_700_000_000, 0)
	for i := range series {
		h.store.Lock()
		s, _, ok := h.store.AdmitLocked([]byte{byte(i), byte(i >> 8)}, now)
		if !ok {
			t.Fatalf("series %d refused by the cardinality cap", i)
		}
		s.calls++
		h.store.ObservedLocked(s, now)
		h.store.Unlock()
	}

	done := make(chan error, 1)
	go func() { done <- h.store.Export(context.Background(), h, pcommon.NewResource()) }()

	// The mark is now inside its first chunk, holding the mutex and going
	// nowhere.
	<-h.parked

	acquired := make(chan struct{})
	var markedWhenServed atomic.Int64
	go lockWaiter(h.store, func() { markedWhenServed.Store(h.calls.Load()) }, acquired)
	// Nothing can take the mutex from the waiter meanwhile: the only other
	// holder is the parked mark.
	waitUntilParkedOnMutex(t)
	close(h.release)

	<-acquired
	if err := <-done; err != nil {
		t.Fatalf("Export: %v", err)
	}
	if got := h.calls.Load(); got != series {
		t.Fatalf("the mark touched %d series, want %d", got, series)
	}
	if served := markedWhenServed.Load(); served >= series {
		t.Errorf("a queued Lock() was served only after all %d series had been marked: the delivery mark is holding the series mutex for the whole map, which is a receive-path stall linear in MaxCardinality", series)
	} else {
		t.Logf("the waiter was served after %d of %d series were marked", served, series)
	}
}

// The eviction and the pointer collection are ONE walk of the map: it is the
// only whole-map pass a render cannot chunk (a map cannot be iterated across
// lock releases), so doing it twice doubled that hold for nothing.
func TestLivePointersLockedEvictsAndReturnsTheSurvivorsInOneWalk(t *testing.T) {
	h := newHarness(10, time.Minute)
	h.observe("stale")
	h.export(t) // renders and delivers, so "stale" is evictable once it ages

	h.now = h.now.Add(10 * time.Minute)
	h.observe("fresh")

	h.store.Lock()
	live := h.store.LivePointersLocked(nil, h.now)
	h.store.Unlock()

	if len(live) != 1 {
		t.Fatalf("survivors = %d, want 1 (the stale series is evicted by the same walk that collects)", len(live))
	}
	if h.evicted.n != 1 {
		t.Fatalf("evicted counter = %d, want 1", h.evicted.n)
	}
	if got := h.store.Len(); got != 1 {
		t.Fatalf("the store holds %d series, want 1", got)
	}
}

// A zero StaleAfter disables eviction, and the combined walk must not lose that:
// it still has to return every series.
func TestLivePointersLockedWithEvictionDisabledReturnsEverything(t *testing.T) {
	h := newHarness(10, 0)
	h.observe("a")
	h.export(t)
	h.now = h.now.Add(10 * time.Hour)

	h.store.Lock()
	live := h.store.LivePointersLocked(nil, h.now)
	h.store.Unlock()

	if len(live) != 1 || h.evicted.n != 0 {
		t.Fatalf("survivors = %d, evicted = %d, want 1 and 0: staleAfter=0 disables eviction", len(live), h.evicted.n)
	}
}

// lockWaiter is the goroutine the chunking tests queue on the series mutex: it
// takes the lock, records what the pass had done by then, and signals. A named
// function rather than a closure so waitUntilParkedOnMutex can find it in a
// goroutine dump.
func lockWaiter(l sync.Locker, served func(), acquired chan<- struct{}) {
	l.Lock()
	served()
	l.Unlock()
	close(acquired)
}

// waitUntilParkedOnMutex returns once a lockWaiter goroutine is PARKED in
// sync.Mutex.Lock — asleep in the runtime's semaphore queue, not still spinning
// or not yet scheduled — which is the state the starvation handoff starts from.
// This replaced a fixed sleep that merely hoped the waiter had got there.
func waitUntilParkedOnMutex(t *testing.T) {
	t.Helper()
	buf := make([]byte, 1<<20)
	for deadline := time.Now().Add(5 * time.Second); ; {
		n := runtime.Stack(buf, true)
		for g := range strings.SplitSeq(string(buf[:n]), "\n\n") {
			if strings.Contains(g, "[sync.Mutex.Lock") && strings.Contains(g, "cumagg.lockWaiter(") {
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatal("the waiter never parked on the series mutex")
		}
		time.Sleep(time.Millisecond)
	}
}
