package main

import (
	"testing"
	"time"

	"github.com/JohanLindvall/kubescrape/internal/agent/servicegraph"
)

// obs.RegisterServiceGraphStats turns one closure into FOUR independent gauge
// registrations, and the metrics Registry evaluates them back to back in a
// single loop. Unmemoised, that took the pairing store's mutex four times per
// export and published four separately-sampled readings of a struct whose whole
// point is that it is one instant — a completed count from before a pairing
// beside a virtual-node count from after it.
func TestServiceGraphStatsAreOneSnapshotPerExport(t *testing.T) {
	calls := 0
	n := uint64(0)
	m := &sgStatsMemo{stats: func() servicegraph.Stats {
		calls++
		n++
		return servicegraph.Stats{Items: int(n), Completed: n, VirtualNode: n, Unkeyable: n}
	}}

	// The four gauges of one export.
	first := m.get()
	for i := 0; i < 3; i++ {
		if got := m.get(); got != first {
			t.Fatalf("gauge %d read %+v, want the same snapshot as the first, %+v", i+2, got, first)
		}
	}
	if calls != 1 {
		t.Fatalf("the pairing store was sampled %d times for one export, want 1", calls)
	}
}

// The memo is a per-export window, not a cache: the next export sees the store
// as it is then.
func TestServiceGraphStatsMemoExpires(t *testing.T) {
	calls := 0
	m := &sgStatsMemo{stats: func() servicegraph.Stats {
		calls++
		return servicegraph.Stats{Completed: uint64(calls)}
	}}
	m.get()
	// Age the memo past its window without sleeping for it.
	m.mu.Lock()
	m.at = m.at.Add(-sgStatsMemoWindow - time.Millisecond)
	m.mu.Unlock()
	if got := m.get(); got.Completed != 2 {
		t.Fatalf("completed = %d after the window, want 2: the memo never refreshes", got.Completed)
	}
}
