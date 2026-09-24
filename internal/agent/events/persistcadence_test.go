package events

import (
	"context"
	"strconv"
	"sync"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/watch"
)

// countingStore is recordingStore with the concurrency the Run loop needs.
type countingStore struct {
	mu    sync.Mutex
	calls int
	last  Position
}

func (s *countingStore) Load(context.Context) (Position, bool, error) { return Position{}, false, nil }
func (s *countingStore) Save(_ context.Context, pos Position) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	s.last = pos
	return nil
}
func (s *countingStore) count() int { s.mu.Lock(); defer s.mu.Unlock(); return s.calls }

// The position must keep being written under a load that flushes on the COUNT
// trigger — and no more often than PersistInterval. persist ran only in the
// ticker branch, and tryFlush resets that ticker, so a rate tripping the count
// trigger faster than FlushInterval reset it before it could ever fire and the
// position was never written at all, silently removing the PersistInterval
// bound on how far a successor replays after a hard kill.
//
// Driven directly on a stepped clock: handle -> tryFlush -> persist is exactly
// the path that fix added, and with the clock injectable the test pins the
// UPPER bound too (a real Run loop could only assert "at least once").
func TestPositionIsWrittenUnderCountTriggeredFlushes(t *testing.T) {
	store := &countingStore{}
	const interval = 10 * time.Second
	r, _, _ := newReader(t, Config{
		Positions:       store,
		BatchSize:       1, // every event trips the count trigger
		FlushInterval:   time.Hour,
		PersistInterval: interval,
	})
	now := time.Unix(1_700_000_000, 0)
	r.now = func() time.Time { return now }
	ctx := context.Background()

	const n = 20
	for i := range n {
		e := event("e"+strconv.Itoa(i), "R", "m", "Normal", strconv.Itoa(100+i), 1, now)
		if err := r.handle(ctx, watch.Event{Type: watch.Added, Object: e}); err != nil {
			t.Fatal(err)
		}
		now = now.Add(interval / 2)
	}
	// Writes land at events 0, 2, 4, ...: one per elapsed interval.
	if got := store.count(); got != n/2 {
		t.Fatalf("position writes = %d over %d count-triggered flushes spanning %d intervals, want %d: "+
			"0 means persist is starved by the ticker reset, more means PersistInterval is not honoured",
			got, n, n/2, n/2)
	}
}

// A position that has not moved is not rewritten. ConfigMapStore.Save stamps
// Updated and always issues an Update, so on a quiet cluster (core/v1 event
// watches get no bookmarks) the ticker rewrote the same position every
// PersistInterval — a new etcd revision recording nothing, ~8,640 a day. The
// forced write at shutdown, and the first write of a leadership term (so Holder
// names the current leader), are always made.
func TestUnchangedPositionIsNotRewritten(t *testing.T) {
	store := &countingStore{}
	r, _, _ := newReader(t, Config{Positions: store, PersistInterval: 10 * time.Second})
	now := time.Unix(1_700_000_000, 0)
	r.now = func() time.Time { return now }
	ctx := context.Background()
	tick := func() {
		now = now.Add(10 * time.Second)
		r.persist(ctx, false)
	}

	r.committed = Position{ResourceVersion: "100", Watermark: now}
	tick()
	if store.count() != 1 {
		t.Fatalf("writes = %d, want the term's first write", store.count())
	}
	tick()
	tick()
	if store.count() != 1 {
		t.Fatalf("writes = %d after two quiet intervals, want still 1: nothing moved", store.count())
	}
	r.committed.Watermark = r.committed.Watermark.Add(time.Second) // the watermark alone moving is a move
	tick()
	if store.count() != 2 {
		t.Fatalf("writes = %d after the watermark advanced, want 2", store.count())
	}
	r.committed.ResourceVersion = "101"
	tick()
	if store.count() != 3 || store.last.ResourceVersion != "101" {
		t.Fatalf("writes = %d (last %q) after the revision advanced, want 3 writing 101", store.count(), store.last.ResourceVersion)
	}
	r.persist(ctx, true)
	if store.count() != 4 {
		t.Fatalf("writes = %d, want the forced shutdown write made even though nothing moved", store.count())
	}
}
