package store

import (
	"fmt"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/types"
)

// Sweep runs under the EXCLUSIVE write lock, on Run's ticker (ttl/4, capped at
// a minute). Every container lookup, every pod lookup and every node-targets
// request waits behind it, so its cost has to be proportional to what actually
// expired — not to the size of the store.
//
// A lapsing tombstone used to arm a second full pass over byContainer, each
// entry paying a s.pods lookup, and in a cluster of any size ordinary churn
// puts a lapsing tombstone in essentially every window. The pods loop already
// holds the record's own container IDs, so the rescan bought nothing.
//
// The assertion is a RATIO against a sweep with nothing to remove, in the same
// process on the same maps: an absolute duration would measure the machine.
func TestSweepDoesNotRescanEveryContainer(t *testing.T) {
	if testing.Short() {
		t.Skip("scaling measurement")
	}
	const pods = 20000
	s, clk := newTestStore(time.Minute)
	for i := range pods {
		uid := fmt.Sprintf("uid-%d", i)
		s.UpsertPod(makePod(uid, fmt.Sprintf("pod-%d", i), "node1", "1",
			map[string]string{"app": fmt.Sprintf("c0ffee%06d", i)}))
	}

	// Nothing expired: the whole cost is the two passes the sweep must make.
	control := minSweep(s, 5, func(int) {})
	// One tombstone lapsing per call — the shape the ticker sees constantly.
	subject := minSweep(s, 5, func(i int) {
		s.DeletePod(types.UID(fmt.Sprintf("uid-%d", i)))
		clk.Advance(2 * time.Minute)
	})

	if subject > 2*control {
		t.Errorf("a sweep removing ONE tombstone took %v against %v for a sweep removing nothing: "+
			"the cost is scaling with the store, not with what expired", subject, control)
	}
	t.Logf("sweep: %v with nothing expired, %v with one tombstone expiring", control, subject)
}

// minSweep times n sweeps, arming each with the given setup (untimed), and
// returns the FASTEST — the least noisy estimator of a cost that only ever
// grows under scheduling interference.
func minSweep(s *Store, n int, arm func(i int)) time.Duration {
	best := time.Duration(1<<63 - 1)
	for i := range n {
		arm(i)
		start := time.Now()
		s.Sweep()
		if d := time.Since(start); d < best {
			best = d
		}
	}
	return best
}

// A lapsed pod must take its NAME index entry with it too. Nothing else ever
// revisits byPodName — Sweep is the last event in a record's life — so an entry
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
	s.Sweep()

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

// Whatever the sweep's shape, a lapsed pod must take its container entries with
// it: they are looked up by ID with no pod reference, so a survivor would
// resolve into a record that no longer exists.
func TestSweepRemovesTheLapsedPodsContainers(t *testing.T) {
	s, clk := newTestStore(time.Minute)
	s.UpsertPod(makePod("uid-1", "pod-1", "node1", "1", map[string]string{"app": "aaaa01"}))
	s.UpsertPod(makePod("uid-2", "pod-2", "node1", "1", map[string]string{"app": "bbbb02"}))
	s.DeletePod("uid-1")
	clk.Advance(2 * time.Minute)
	s.Sweep()

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
