package store

// The store's change tokens are what internal/server's node-targets ETag memo
// hangs on (targetsValidity): equal tokens let a revalidation be answered 304
// WITHOUT re-deriving, so they must move for every change a read could see and
// for nothing else. Getting it wrong has two different costs — a missing bump
// serves a stale target list (the one way the memo can be WRONG), a spurious
// one re-derives the whole node on every agent poll (the cost the memo exists
// to remove) — and neither shows up anywhere but here: the sibling sources,
// internal/services and internal/servicemonitors, have had a generation_test
// all along while this one had none, and deleting DeletePod's bump left every
// suite green.

import (
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/types"
)

// generation reads the store-wide change token (the gen field). It is a test
// accessor only: nothing outside the store reads gen directly — callers read
// NodeGeneration, which is minted from it — but it is the token every
// mutation's bumpLocked advances, so it is where "did this count as a change"
// is observable for operations that touch no node (a sweep, a no-op delete).
func (s *Store) generation() uint64 { return s.gen.Load() }

// Every store operation that is not a change must leave the store-wide token
// alone, and every one that is must advance it. Each case starts from a store
// holding one live pod ("a", on node-1) and one tombstone ("gone"), with the
// clock at the tombstone's stamp.
func TestGenerationAdvancesOnlyOnRealChange(t *testing.T) {
	for _, tc := range []struct {
		name    string
		ttl     time.Duration
		op      func(s *Store, clk *fakeClock)
		advance bool
	}{
		{"byte-identical re-delivery (resync)", time.Minute, func(s *Store, _ *fakeClock) {
			s.UpsertPod(makePod("a", "a", "node-1", "1", map[string]string{"app": "c1"}))
		}, false},
		{"delete of an unknown uid", time.Minute, func(s *Store, _ *fakeClock) {
			s.DeletePod(types.UID("never-seen"))
		}, false},
		{"sweep with nothing due", time.Minute, func(s *Store, _ *fakeClock) {
			s.sweep()
		}, false},
		{"upsert with a new resourceVersion", time.Minute, func(s *Store, _ *fakeClock) {
			s.UpsertPod(makePod("a", "a", "node-1", "2", map[string]string{"app": "c1"}))
		}, true},
		{"delete of a known uid (tombstoned)", time.Minute, func(s *Store, _ *fakeClock) {
			s.DeletePod(types.UID("a"))
		}, true},
		{"delete of a known uid (ttl 0, removed)", 0, func(s *Store, _ *fakeClock) {
			s.DeletePod(types.UID("a"))
		}, true},
		{"sweep that removes an expired tombstone", time.Minute, func(s *Store, clk *fakeClock) {
			clk.Advance(2 * time.Minute)
			s.sweep()
		}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, clk := newTestStore(tc.ttl)
			s.UpsertPod(makePod("a", "a", "node-1", "1", map[string]string{"app": "c1"}))
			s.UpsertPod(makePod("gone", "gone", "node-2", "1", map[string]string{"app": "c2"}))
			s.DeletePod(types.UID("gone"))
			before := s.generation()
			tc.op(s, clk)
			if moved := s.generation() != before; moved != tc.advance {
				t.Fatalf("the store-wide token moved=%v, want %v: a token that does not move on a change serves a "+
					"stale memo; one that moves on a non-change rebuilds it for nothing", moved, tc.advance)
			}
		})
	}
}

// The node-targets memo is per NODE and its derivation reads only
// PodsOnNode(node), so its token must be per node too: with the store-wide one,
// a pod event on ANY node — a readiness flip, a restart, a Job pod — lapsed
// every node's memo, and the memo only ever hit on a cluster with no pod churn
// at all.
func TestNodeGenerationMovesOnlyForTheNodeThatChanged(t *testing.T) {
	s, _ := newTestStore(time.Minute)
	s.UpsertPod(makePod("a", "a", "node-1", "1", nil))
	s.UpsertPod(makePod("b", "b", "node-2", "1", nil))
	n1, n2 := s.NodeGeneration("node-1"), s.NodeGeneration("node-2")

	// Churn on node-2: an update, a new pod, a delete.
	s.UpsertPod(makePod("b", "b", "node-2", "2", nil))
	s.UpsertPod(makePod("c", "c", "node-2", "1", nil))
	s.DeletePod(types.UID("c"))
	if got := s.NodeGeneration("node-1"); got != n1 {
		t.Fatalf("node-1's token moved (%d -> %d) for pod churn on node-2, whose pods node-1's "+
			"targets cannot contain", n1, got)
	}
	if got := s.NodeGeneration("node-2"); got == n2 {
		t.Fatal("node-2's token did not move for changes to its own pods")
	}

	// A pod MOVING between nodes changes both sets.
	n1, n2 = s.NodeGeneration("node-1"), s.NodeGeneration("node-2")
	s.UpsertPod(makePod("b", "b", "node-1", "3", nil))
	if s.NodeGeneration("node-1") == n1 || s.NodeGeneration("node-2") == n2 {
		t.Fatal("a pod moving from node-2 to node-1 must advance both nodes' tokens")
	}
}

// A sweep only ever removes TOMBSTONES, and a tombstone left byNode in
// deletePodLocked, before it was one — so no node's pod set changes and no
// node's token may move, or every node memo would lapse on the sweeper's
// ticker (as short as every five seconds).
func TestSweepLeavesNodeGenerationAlone(t *testing.T) {
	s, clk := newTestStore(time.Minute)
	s.UpsertPod(makePod("a", "a", "node-1", "1", nil))
	s.UpsertPod(makePod("gone", "gone", "node-1", "1", nil))
	s.DeletePod(types.UID("gone"))
	before := s.NodeGeneration("node-1")
	clk.Advance(2 * time.Minute)
	s.sweep()
	if got := s.NodeGeneration("node-1"); got != before {
		t.Fatalf("a sweep moved node-1's token (%d -> %d) without changing its pod set", before, got)
	}
}

// A node emptied and refilled must never repeat a token, and neither may the
// EMPTY state repeat across the two transitions. The memo's reader samples the
// token BEFORE reading the pods, so a memo can legitimately be tagged with an
// empty-node token and hold a non-empty list (a pod arrived in between); if the
// node emptying again brought that same token back, the stale list would be
// served for it. So an empty node reads the store-wide token, which the
// emptying mutation advanced.
func TestNodeGenerationNeverRepeatsAcrossEmptyAndRefill(t *testing.T) {
	s, _ := newTestStore(0)
	seen := map[uint64]string{}
	record := func(state string) {
		t.Helper()
		g := s.NodeGeneration("node-1")
		if prev, dup := seen[g]; dup {
			t.Fatalf("token %d for %q repeats the one read for %q: a memo tagged then would be "+
				"served now", g, state, prev)
		}
		seen[g] = state
	}
	record("empty before any pod")
	s.UpsertPod(makePod("a", "a", "node-1", "1", nil))
	record("one pod")
	s.DeletePod(types.UID("a"))
	record("empty again")
	s.UpsertPod(makePod("b", "b", "node-1", "1", nil))
	record("refilled")
	s.DeletePod(types.UID("b"))
	record("empty a third time")

	// And an emptied node keeps no entry: node churn (an autoscaler's joins
	// and leaves) must not grow the token map.
	s.mu.RLock()
	defer s.mu.RUnlock()
	if _, ok := s.nodeGen["node-1"]; ok {
		t.Fatal("an empty node kept its nodeGen entry; the map would grow with every node ever seen")
	}
}
