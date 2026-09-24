package store

import (
	"fmt"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/types"
)

// sweep runs under the EXCLUSIVE write lock, on Run's ticker (ttl/4, capped at
// a minute). Every container lookup, every pod lookup and every node-targets
// request waits behind it, and the informer's own upserts queue up too, so its
// cost has to be proportional to what actually expired — not to the size of
// the store.
//
// It has been wrong at two different scales. First a lapsing tombstone armed a
// SECOND full pass over byContainer, each entry paying a s.pods lookup, though
// the pods loop already holds the record's own container IDs. Then the two
// remaining passes turned out to be the whole cost: they ran whether or not
// anything was due, so a 40k-pod store paid 7.58 ms under the exclusive lock
// every tick to remove nothing.
//
// The assertion is the SHAPE, measured as a ratio between two store sizes with
// the same amount of expiring work in each: an absolute duration would measure
// the machine, and a ratio against an idle sweep in one store measures whatever
// the idle sweep happens to cost.
func TestSweepCostDoesNotScaleWithTheStore(t *testing.T) {
	if testing.Short() {
		t.Skip("scaling measurement")
	}
	const (
		small = 2000
		large = 10 * small
		batch = 100 // tombstones lapsing per sweep, the same in both stores
	)
	cheap := sweepCost(t, small, batch)
	dear := sweepCost(t, large, batch)

	// 10x the store for the same work. The scanning shape measured 15-24x here
	// and the current one 0.65-2.0x; anything under 4 is a sweep whose cost is
	// its removals.
	if dear > 4*cheap {
		t.Errorf("sweeping %d tombstones took %v in a %d-pod store and %v in a %d-pod store: "+
			"the cost is scaling with the store, not with what expired", batch, cheap, small, dear, large)
	}
	t.Logf("sweep of %d lapsed tombstones: %v at %d pods, %v at %d pods", batch, cheap, small, dear, large)
}

// sweepCost builds a store of the given size and returns the FASTEST of nine
// sweeps, each retiring `batch` freshly lapsed tombstones — the least noisy
// estimator of a cost that only ever grows under scheduling interference.
func sweepCost(t *testing.T, pods, batch int) time.Duration {
	t.Helper()
	s, clk := newTestStore(time.Minute)
	for i := range pods {
		uid := fmt.Sprintf("uid-%d", i)
		s.UpsertPod(makePod(uid, fmt.Sprintf("pod-%d", i), "node1", "1",
			map[string]string{"app": fmt.Sprintf("c0ffee%06d", i)}))
	}
	best := time.Duration(1<<63 - 1)
	const rounds = 9
	for round := range rounds {
		for i := round * batch; i < (round+1)*batch; i++ {
			s.DeletePod(types.UID(fmt.Sprintf("uid-%d", i)))
		}
		clk.Advance(2 * time.Minute)
		start := time.Now()
		s.sweep()
		if d := time.Since(start); d < best {
			best = d
		}
	}
	if got, _ := s.Stats(); got != pods-rounds*batch {
		t.Fatalf("the measurement did not retire what it deleted: %d pods left, want %d", got, pods-rounds*batch)
	}
	return best
}

// A lapsed pod must take its NAME index entry with it too. Nothing else ever
// revisits byPodName — sweep is the last event in a record's life — so an entry
// left behind keeps the whole kubemeta.Pod reachable for the process lifetime,
// one per expired tombstone, on a cluster whose pod names never repeat (Jobs,
// CronJobs, any generateName workload).
//
// It is invisible from the API, which is why nothing caught it: the stale
// record's expireAt is in the past, so GetPodByName's expiry check rejects it
// and every lookup still answers correctly. Only the map grows. The existing
// TestPodIPIndexDoesNotLeakAcrossDeletes runs the same create/delete/sweep loop
// and checks byPodIP alone.
func TestSweepDropsTheNameIndexEntry(t *testing.T) {
	s, clk := newTestStore(time.Minute)
	const n = 300
	for i := range n {
		uid := fmt.Sprintf("uid-%d", i)
		s.UpsertPod(makePod(uid, fmt.Sprintf("pod-%d", i), "node1", "1",
			map[string]string{"app": fmt.Sprintf("c0ffee%06d", i)}))
		s.DeletePod(types.UID(uid))
	}
	// One pod that is NOT deleted: the sweep must not take the live index with
	// the dead entries.
	s.UpsertPod(makePod("live", "pod-live", "node1", "1", map[string]string{"app": "abcdef000001"}))

	clk.Advance(2 * time.Minute)
	s.sweep()

	s.mu.RLock()
	names, pods := len(s.byPodName), len(s.pods)
	s.mu.RUnlock()
	if names != 1 {
		t.Fatalf("byPodName holds %d entries after %d tombstones lapsed (pods=%d, want 1 of each): "+
			"each leaked entry pins a whole kubemeta.Pod and nothing ever revisits the index",
			names, n, pods)
	}
	if _, ok := s.GetPodByName("default", "pod-live"); !ok {
		t.Fatal("the live pod's name entry was swept away")
	}
}

// The sweep visits a LIST of stamped tombstones instead of scanning the store,
// so a stamp that never reaches the list is a tombstone nothing ever reclaims —
// and it is invisible, because every lookup checks expiry for itself and keeps
// answering correctly while the map grows.
//
// This is the case that is not reclaimed by its pod: a pod is deleted (which
// stamps the pod AND its container entries), then RESURRECTED by a late update
// — a missed delete, which is exactly what the byPodName guard exists for —
// whose status no longer reports one of the containers. The pod is live again,
// so no pod sweep will ever run for it, and the aged-out entry keeps the stamp
// its deletion gave it.
func TestSweepReclaimsAContainerLeftBehindByAResurrectedPod(t *testing.T) {
	s, clk := newTestStore(time.Minute)
	s.UpsertPod(makePod("uid-1", "pod-1", "node1", "1",
		map[string]string{"app": "aaaa01", "sidecar": "bbbb02"}))
	s.DeletePod("uid-1")

	// The late update: the pod is back, but the kubelet no longer reports the
	// sidecar's container (a second restart aged it out of the status).
	s.UpsertPod(makePod("uid-1", "pod-1", "node1", "2", map[string]string{"app": "aaaa01"}))
	if _, ok := s.GetPodByUID("uid-1"); !ok {
		t.Fatal("the pod was not resurrected")
	}

	clk.Advance(2 * time.Minute)
	s.sweep()

	s.mu.RLock()
	_, stale := s.byContainer["bbbb02"]
	_, live := s.byContainer["aaaa01"]
	n := len(s.byContainer)
	s.mu.RUnlock()
	if stale {
		t.Errorf("the aged-out container entry survived its expiry (byContainer holds %d entries): "+
			"nothing will ever revisit it", n)
	}
	if !live {
		t.Error("the sweep removed the resurrected pod's LIVE container entry")
	}
}

// The same invariant over a churning store rather than one hand-built case:
// after every tombstone has lapsed, a sweep must leave exactly the live pods
// and their containers. Any stamp the sweep cannot reach shows up here as a
// survivor.
func TestSweepLeavesNothingExpiredBehind(t *testing.T) {
	s, clk := newTestStore(time.Minute)
	live := map[string]bool{}
	for i := range 200 {
		uid := fmt.Sprintf("uid-%d", i)
		name := fmt.Sprintf("pod-%d", i)
		s.UpsertPod(makePod(uid, name, "node1", "1",
			map[string]string{"app": fmt.Sprintf("aaaa%06d", i), "side": fmt.Sprintf("bbbb%06d", i)}))
		live[uid] = true
		clk.Advance(time.Second)

		switch i % 5 {
		case 0: // deleted and left dead
			s.DeletePod(types.UID(uid))
			delete(live, uid)
		case 1: // container restarted: the old ID is stamped, the pod lives on
			s.UpsertPod(makePod(uid, name, "node1", "2",
				map[string]string{"app": fmt.Sprintf("cccc%06d", i), "side": fmt.Sprintf("bbbb%06d", i)}))
		case 2: // deleted, then resurrected by a late update with fewer containers
			s.DeletePod(types.UID(uid))
			s.UpsertPod(makePod(uid, name, "node1", "3",
				map[string]string{"app": fmt.Sprintf("aaaa%06d", i)}))
		case 3: // deleted twice (a replayed informer delete re-stamps the pod)
			s.DeletePod(types.UID(uid))
			clk.Advance(time.Second)
			s.DeletePod(types.UID(uid))
			delete(live, uid)
		case 4: // its name is reused by a fresh UID, which tombstones this one
			s.UpsertPod(makePod(uid+"-new", name, "node1", "1",
				map[string]string{"app": fmt.Sprintf("dddd%06d", i)}))
			delete(live, uid)
			live[uid+"-new"] = true
		}
	}

	clk.Advance(2 * time.Minute)
	s.sweep()

	pods, containers := s.Stats()
	if pods != len(live) {
		t.Errorf("the store holds %d pod records after every tombstone lapsed, want the %d live ones", pods, len(live))
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	for uid, rec := range s.pods {
		if !live[string(uid)] {
			t.Errorf("expired pod %q survived the sweep (expireAt %v)", uid, rec.expireAt)
		}
	}
	for id, e := range s.byContainer {
		if !e.expireAt.IsZero() {
			t.Errorf("container %q survived the sweep with a lapsed stamp (%v)", id, e.expireAt)
		}
		if !live[string(e.podUID)] {
			t.Errorf("container %q survived, owned by dead pod %q", id, e.podUID)
		}
	}
	t.Logf("%d live pods, %d container entries, %d stamps still listed", pods, containers, len(s.pending))
}

// The pending list is reused across sweeps, but a burst's peak must not be
// resident for the process lifetime: a rollout stamps three tombstones per pod
// and drains them all within one TTL.
func TestSweepReleasesThePendingListItGrewForABurst(t *testing.T) {
	s, clk := newTestStore(time.Minute)
	const n = 4000
	for i := range n {
		uid := fmt.Sprintf("uid-%d", i)
		s.UpsertPod(makePod(uid, fmt.Sprintf("pod-%d", i), "node1", "1",
			map[string]string{"app": fmt.Sprintf("aaaa%06d", i)}))
		s.DeletePod(types.UID(uid))
	}
	s.mu.RLock()
	peak := cap(s.pending)
	s.mu.RUnlock()
	if peak < n {
		t.Fatalf("the burst did not grow the pending list (cap %d for %d deletes)", peak, n)
	}

	clk.Advance(2 * time.Minute)
	s.sweep()

	s.mu.RLock()
	defer s.mu.RUnlock()
	if len(s.pending) != 0 {
		t.Fatalf("%d stamps still listed after everything lapsed", len(s.pending))
	}
	if cap(s.pending) > maxIdlePendingStamps {
		t.Errorf("the drained pending list still holds the burst's array (cap %d, peak %d): "+
			"a rollout's high-water mark stays resident for the process lifetime", cap(s.pending), peak)
	}
}

// The drained case above is the easy one. Under steady churn the list never
// drains to zero — a fresh, unexpired stamp arrives between every two sweeps —
// and releasing the burst's array only at zero kept the rollout's peak resident
// behind a handful of live entries for as long as the churn lasted. The array
// must be sized by what it HOLDS, and must not then be re-allocated on every
// sweep by the churn that keeps it small.
func TestSweepGivesBackABurstsArrayWhileChurnKeepsItNonEmpty(t *testing.T) {
	s, clk := newTestStore(time.Minute)
	const n = 4000
	for i := range n {
		uid := fmt.Sprintf("burst-%d", i)
		s.UpsertPod(makePod(uid, uid, "node1", "1",
			map[string]string{"app": fmt.Sprintf("bbbb%06d", i)}))
		s.DeletePod(types.UID(uid))
	}
	s.mu.RLock()
	peak := cap(s.pending)
	s.mu.RUnlock()
	if peak < n {
		t.Fatalf("the burst did not grow the pending list (cap %d for %d deletes)", peak, n)
	}

	churn := func(i int) {
		uid := fmt.Sprintf("churn-%d", i)
		s.UpsertPod(makePod(uid, uid, "node1", "1",
			map[string]string{"app": fmt.Sprintf("cccc%06d", i)}))
		s.DeletePod(types.UID(uid))
	}
	// One pod deleted per 30s, a sweep after each: the burst lapses at the
	// third tick, and from then on the list always holds the one or two churn
	// stamps that have not.
	var arrays []*pendingExpiry
	for i := range 10 {
		clk.Advance(30 * time.Second)
		churn(i)
		s.sweep()
		s.mu.RLock()
		l, c := len(s.pending), cap(s.pending)
		if c > 0 {
			arrays = append(arrays, &s.pending[:1][0])
		}
		s.mu.RUnlock()
		if l == 0 {
			t.Fatalf("tick %d: the list drained to zero, so this no longer exercises the churn case", i)
		}
		if i >= 2 && c > maxIdlePendingStamps {
			t.Fatalf("tick %d: %d live stamps in an array of %d (peak %d): the burst's high-water mark "+
				"stays resident for as long as churn keeps the list non-empty", i, l, c, peak)
		}
	}
	// Once shrunk, the churn reuses one array rather than re-allocating per sweep.
	for i := 4; i < len(arrays); i++ {
		if arrays[i] != arrays[3] {
			t.Errorf("sweep %d re-allocated the pending array: a shrink must not thrash under churn", i)
		}
	}
}

// Shrinking is by occupancy, and the new array is at least half full: the
// arithmetic that keeps a shrink from being undone by the very next append.
func TestShrinkPendingIsByOccupancy(t *testing.T) {
	big := maxIdlePendingStamps * 8
	for _, tc := range []struct {
		name         string
		len, cap     int
		wantCap      int // 0 = nil
		wantSameBack bool
	}{
		{"small arrays are always kept", 10, maxIdlePendingStamps, maxIdlePendingStamps, true},
		{"a quarter full is kept", big / 4, big, big, true},
		{"a drained burst is released", 0, big, 0, false},
		{"a sparse burst shrinks to the floor", 10, big, maxIdlePendingStamps, false},
		{"a large remainder keeps twice its length", big/4 - 1, big, 2 * (big/4 - 1), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := make([]pendingExpiry, tc.len, tc.cap)
			for i := range p {
				p[i].id = fmt.Sprint(i)
			}
			got := shrinkPending(p)
			if tc.wantCap == 0 {
				if got != nil {
					t.Fatalf("cap %d, want the array released", cap(got))
				}
				return
			}
			if cap(got) != tc.wantCap || len(got) != tc.len {
				t.Fatalf("len/cap = %d/%d, want %d/%d", len(got), cap(got), tc.len, tc.wantCap)
			}
			if same := cap(got) > 0 && &got[:1][0] == &p[:1][0]; same != tc.wantSameBack {
				t.Fatalf("returned the same array = %v, want %v", same, tc.wantSameBack)
			}
			for i := range got {
				if got[i].id != fmt.Sprint(i) {
					t.Fatalf("entry %d = %q after the shrink: survivors must keep their order", i, got[i].id)
				}
			}
		})
	}
}

// A record keyed by an EMPTY UID is degenerate — the informer never delivers
// one, only a hand-built caller can — but it is representable, and the sweep
// must not decide what a listed tombstone IS from the emptiness of a field the
// caller controls. Reading an empty pod UID as a container id would leave that
// record in the store for the process lifetime.
func TestSweepRetiresAPodWithNoUID(t *testing.T) {
	s, clk := newTestStore(time.Minute)
	s.UpsertPod(makePod("", "pod-nouid", "node1", "1", map[string]string{"app": "aaaa01"}))
	s.DeletePod("")

	clk.Advance(2 * time.Minute)
	s.sweep()

	if pods, containers := s.Stats(); pods != 0 || containers != 0 {
		t.Errorf("the sweep left pods=%d containers=%d behind", pods, containers)
	}
}

// Whatever the sweep's shape, a lapsed pod must take its container entries with
// it: they are looked up by ID with no pod reference, so a survivor would
// resolve into a record that no longer exists.
func TestSweepRemovesTheLapsedPodsContainers(t *testing.T) {
	s, clk := newTestStore(time.Minute)
	s.UpsertPod(makePod("uid-1", "pod-1", "node1", "1", map[string]string{"app": "aaaa01"}))
	s.UpsertPod(makePod("uid-2", "pod-2", "node1", "1", map[string]string{"app": "bbbb02"}))
	s.DeletePod("uid-1")
	clk.Advance(2 * time.Minute)
	s.sweep()

	if _, ok := s.GetPodByUID("uid-1"); ok {
		t.Error("the expired pod is still resolvable")
	}
	s.mu.RLock()
	_, stale := s.byContainer["aaaa01"]
	_, live := s.byContainer["bbbb02"]
	s.mu.RUnlock()
	if stale {
		t.Error("byContainer still holds the expired pod's entry")
	}
	if !live {
		t.Error("the sweep removed a LIVE pod's container entry")
	}
}
