package cumagg

import (
	"context"
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
// chunk, so the test can put a waiter on the series mutex with no timing
// assumption at all: the mark cannot make progress until the test says so.
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
			h.store.EachLocked(func(s *series) {
				h.store.MarkRenderedLocked(s)
				dps.AppendEmpty().SetIntValue(int64(s.calls))
			})
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
			if (n-1)%markChunk == 0 {
				time.Sleep(chunkDwell)
			}
		},
	})
	return h
}

func (h *markHarness) ExportMetrics(context.Context, pmetric.Metrics) error { return nil }

// The delivery mark is the whole-map pass an export makes after the SEND, and
// it takes the mutex Record/observe take per edge and per span — the pairing
// store from inside its own. Held whole it was a stall nothing bounded, linear
// in MaxCardinality, i.e. the one part of an export that kept growing after the
// render's was chunked. So it releases the lock between chunks, and this is what
// says so: a waiter queued while the first chunk is parked is served while
// series are still unmarked. Unchunked, the same waiter is served only once
// every series has been marked.
func TestDeliveryMarkReleasesTheLockBetweenChunks(t *testing.T) {
	const series = 3 * markChunk
	h := newMarkHarness(series + 1)
	now := time.Unix(1_700_000_000, 0)
	for i := 0; i < series; i++ {
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

	queued, acquired := make(chan struct{}), make(chan struct{})
	var markedWhenServed atomic.Int64
	go func() {
		close(queued)
		h.store.Lock()
		markedWhenServed.Store(h.calls.Load())
		h.store.Unlock()
		close(acquired)
	}()
	<-queued
	// The waiter is one statement from blocking on the mutex, and nothing can
	// take it from it: the only other holder is parked below.
	time.Sleep(100 * time.Millisecond)
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
