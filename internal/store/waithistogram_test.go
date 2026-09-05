package store

import (
	"context"
	"testing"
	"time"

	"github.com/JohanLindvall/kubescrape/internal/obs"
)

// waitObservations is the number of parked lookups observed under one outcome
// of kubescrape_container_lookup_wait_seconds, read through the same Dump the
// Prometheus bridge serves.
func waitObservations(t *testing.T, outcome string) uint64 {
	t.Helper()
	for _, s := range obs.Registry.Dump() {
		if s.Name != "kubescrape_container_lookup_wait_seconds" {
			continue
		}
		for _, p := range s.Points {
			for _, l := range p.Labels {
				if l[0] == "outcome" && l[1] == outcome {
					return p.Count
				}
			}
		}
	}
	return 0
}

// TestParkedLookupsAreObservedByOutcome pins the wait histogram's two rules: a
// lookup that PARKED is observed under the outcome its wait ended in, and a
// lookup answered from the store never is — the series is the informer's lag
// behind the kubelet, and a warm hit has no lag to report.
func TestParkedLookupsAreObservedByOutcome(t *testing.T) {
	s, _ := newTestStore(time.Minute)
	resolvedBefore, timeoutBefore := waitObservations(t, "resolved"), waitObservations(t, "timeout")

	// Parks, then the ID lands: resolved.
	got := make(chan bool, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, ok, _ := s.GetContainer(ctx, "wanted1")
		got <- ok
	}()
	time.Sleep(50 * time.Millisecond)
	s.UpsertPod(makePod("uid1", "pod1", "node1", "1", map[string]string{"app": "wanted1"}))
	if !<-got {
		t.Fatal("the parked lookup was not answered")
	}
	// Parks and expires: timeout.
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if _, ok, _ := s.GetContainer(ctx, "never2"); ok {
		t.Fatal("an unknown id resolved")
	}
	// A warm hit: no park, no observation.
	if _, ok, _ := s.GetContainer(context.Background(), "wanted1"); !ok {
		t.Fatal("the indexed id was not found")
	}

	if got := waitObservations(t, "resolved") - resolvedBefore; got != 1 {
		t.Errorf("resolved observations moved by %d, want 1 (one parked lookup that was answered)", got)
	}
	if got := waitObservations(t, "timeout") - timeoutBefore; got != 1 {
		t.Errorf("timeout observations moved by %d, want 1 (one parked lookup whose budget expired)", got)
	}
}
