package server

// Drain's COUNT is idempotent, not just its close. main logs the number —
// "released blocked container lookups so they can be answered" — as the one
// loss a graceful shutdown causes, so a second call reporting the same parks
// again re-reports a loss that happened once. It could: the readiness-park
// count was read OUTSIDE the sync.Once, so only the store's half (which
// returns 0 on its second call) was idempotent, and the existing coverage
// parked its request in the store where readyParked is 0.

import (
	"testing"
	"time"

	"github.com/JohanLindvall/kubescrape/internal/store"
)

// The counter is set directly rather than by parking a real request: a parked
// request wakes and decrements the instant Drain closes the channel, so a
// second call could read 0 by timing alone and prove nothing.
func TestDrainReportsTheReadinessParksExactlyOnce(t *testing.T) {
	api := newAPI(store.New(time.Minute), time.Second)
	api.readyParked.Store(3)

	if got := api.Drain(); got != 3 {
		t.Fatalf("first Drain reported %d, want the 3 parked requests", got)
	}
	if got := api.Drain(); got != 0 {
		t.Errorf("second Drain reported %d, want 0: those parks were released by the first call, and the caller logs a nonzero count as a loss", got)
	}
	// The parks are still counted as parked (their handlers have not returned
	// yet), which is precisely why the guard has to be the sync.Once and not
	// the counter reaching zero.
	if got := api.readyParked.Load(); got != 3 {
		t.Errorf("readyParked = %d, want it untouched by Drain: the handlers decrement it on their way out", got)
	}
}

// A Server with no store at all (Config.Store nil is legal for the halves of
// this API that do not need it) must still close the channel once and report
// its own parks once.
func TestDrainWithoutAStoreIsStillIdempotent(t *testing.T) {
	api := newAPI(nil, time.Second)
	api.readyParked.Store(1)
	if got := api.Drain(); got != 1 {
		t.Fatalf("first Drain reported %d, want 1", got)
	}
	if got := api.Drain(); got != 0 {
		t.Errorf("second Drain reported %d, want 0", got)
	}
	select {
	case <-api.draining:
	default:
		t.Error("Drain did not close the draining channel")
	}
}
