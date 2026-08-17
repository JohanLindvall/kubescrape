package events

import (
	"context"
	"sync"
	"testing"
	"time"
)

// countingStore is recordingStore with the concurrency the Run loop needs.
type countingStore struct {
	mu    sync.Mutex
	calls int
}

func (s *countingStore) Load(context.Context) (Position, bool, error) { return Position{}, false, nil }
func (s *countingStore) Save(context.Context, Position) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	return nil
}
func (s *countingStore) count() int { s.mu.Lock(); defer s.mu.Unlock(); return s.calls }

// The position must keep being written under a load that flushes on the COUNT
// trigger. persist ran only in the ticker branch, and tryFlush resets that
// ticker — so a rate tripping the count trigger faster than FlushInterval reset
// it before it could ever fire, and the position was never written at all. That
// silently removed the PersistInterval bound on how far a successor replays
// after a hard kill.
func TestPositionIsWrittenUnderCountTriggeredFlushes(t *testing.T) {
	store := &countingStore{}
	r, _, w := newReader(t, Config{
		Positions:       store,
		BatchSize:       1, // every event trips the count trigger
		FlushInterval:   150 * time.Millisecond,
		PersistInterval: 50 * time.Millisecond,
	})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { r.Run(ctx); close(done) }()

	// Feed steadily for well over a second: many count-triggered flushes, and
	// many PersistInterval windows.
	deadline := time.Now().Add(1500 * time.Millisecond)
	for i := 0; time.Now().Before(deadline); i++ {
		w.Add(event("e", "R", "m", "Normal", "42", 1, time.Now()))
		time.Sleep(30 * time.Millisecond)
	}
	got := store.count()
	cancel()
	<-done

	t.Logf("position writes during a count-triggered run: %d", got)
	if got == 0 {
		t.Error("the position was NEVER written while the count trigger was flushing; " +
			"persist is starved by the ticker reset")
	}
}
