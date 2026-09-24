package store

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/JohanLindvall/kubescrape/internal/testrace"
)

func TestWaitForContainer(t *testing.T) {
	s, _ := newTestStore(time.Minute)

	done := make(chan ContainerResult, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		res, ok, _ := s.GetContainer(ctx, "late999")
		if ok {
			done <- res
		}
		close(done)
	}()

	// Parked, not merely started: a lookup that has not reached the waiter
	// table yet would find the pod on its fast path and pass without ever
	// exercising the wakeup this test is named for.
	waitForCount(t, s, 1)
	s.UpsertPod(makePod("uid1", "pod1", "node1", "1", map[string]string{"app": "late999"}))

	select {
	case res, ok := <-done:
		if !ok {
			t.Fatal("waiter returned not-found")
		}
		if res.Pod.Name != "pod1" {
			t.Fatalf("got pod %q", res.Pod.Name)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("waiter did not wake up")
	}
}

func TestWaitIsPerContainer(t *testing.T) {
	s, _ := newTestStore(time.Minute)

	got := make(chan bool, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, ok, _ := s.GetContainer(ctx, "wanted1")
		got <- ok
	}()
	// The other lookup ends when the TEST says so, not on a short deadline of
	// its own: a 300ms budget could lapse before the first lookup had even
	// parked on a loaded machine, and then the two were never parked together.
	otherCtx, cancelOther := context.WithCancel(context.Background())
	defer cancelOther()
	other := make(chan bool, 1)
	go func() {
		_, ok, _ := s.GetContainer(otherCtx, "other2")
		other <- ok
	}()

	waitForCount(t, s, 2) // both parked: wanted1 and other2
	// Indexing "wanted1" must release only its own waiter.
	s.UpsertPod(makePod("uid1", "pod1", "node1", "1", map[string]string{"app": "wanted1"}))

	select {
	case ok := <-got:
		if !ok {
			t.Fatal("waiter for indexed container did not get a result")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the indexed container's waiter did not wake up")
	}
	select {
	case ok := <-other:
		t.Fatalf("waiter for a different container returned (ok=%v) before its budget ended", ok)
	default:
	}
	cancelOther()
	select {
	case ok := <-other:
		if ok {
			t.Fatal("waiter for a different container was satisfied")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("other waiter never returned after its budget ended")
	}

	// All waiter registrations must be cleaned up.
	s.mu.RLock()
	defer s.mu.RUnlock()
	if len(s.waiters) != 0 {
		t.Fatalf("leaked waiters: %v", s.waiters)
	}
}

func TestManyWaitersSameContainer(t *testing.T) {
	s, _ := newTestStore(time.Minute)

	const n = 20
	results := make(chan bool, n)
	for range n {
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			res, ok, _ := s.GetContainer(ctx, "shared123")
			results <- ok && res.Pod.Name == "pod1"
		}()
	}

	waitForCount(t, s, n) // every one of them parked on the same ID
	s.UpsertPod(makePod("uid1", "pod1", "node1", "1", map[string]string{"app": "shared123"}))

	for i := range n {
		select {
		case ok := <-results:
			if !ok {
				t.Fatalf("waiter %d did not get the pod", i)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("waiter %d never woke up", i)
		}
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if len(s.waiters) != 0 {
		t.Fatalf("leaked waiters: %v", s.waiters)
	}
}

func TestWaitTimesOut(t *testing.T) {
	s, _ := newTestStore(time.Minute)
	start := time.Now()
	mustMiss(t, s, "never")
	if time.Since(start) > time.Second {
		t.Fatal("wait took far longer than its context timeout")
	}
}

// An expired-but-unswept tombstone must return not-found immediately: the
// container is definitively gone, so waiting the full budget for it is
// pointless (and blocks callers for seconds per dead container).
func TestExpiredTombstoneDoesNotWait(t *testing.T) {
	s, clk := newTestStore(time.Minute)
	s.UpsertPod(makePod("uid1", "pod1", "node1", "1", map[string]string{"app": "abc123"}))
	s.DeletePod("uid1")
	clk.Advance(time.Minute + time.Second)
	// No sweep: the tombstone is expired but still present.

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	start := time.Now()
	if _, ok, _ := s.GetContainer(ctx, "abc123"); ok {
		t.Fatal("expired tombstone resolved")
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("lookup took %v; must not wait for an expired tombstone", elapsed)
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if len(s.waiters) != 0 {
		t.Fatalf("expired-tombstone lookup registered waiters: %v", s.waiters)
	}
}

// A replaced container ID whose TTL has lapsed is equally definitive: it can
// never be reported again, so its lookup must not block either.
func TestExpiredReplacedIDDoesNotWait(t *testing.T) {
	s, clk := newTestStore(time.Minute)
	s.UpsertPod(makePod("uid1", "pod1", "node1", "1", map[string]string{"app": "old111"}))
	s.UpsertPod(makePod("uid1", "pod1", "node1", "2", map[string]string{"app": "new222"}))
	clk.Advance(time.Minute + time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	start := time.Now()
	if _, ok, _ := s.GetContainer(ctx, "old111"); ok {
		t.Fatal("expired replaced ID resolved")
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("lookup took %v; must not wait for an expired replaced ID", elapsed)
	}
}

// waitForCount polls BlockedLookups until it reaches want.
func waitForCount(t *testing.T, s *Store, want int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if s.BlockedLookups() == want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("waiter count = %d, want %d", s.BlockedLookups(), want)
}

// The waiter cap sheds additional blocking lookups with ErrTooManyWaiters
// instead of pinning unbounded memory; capped lookups are retryable, and the
// already-registered waiters still wake normally.
func TestWaiterOverflowSheds(t *testing.T) {
	s, _ := newTestStore(time.Minute, WithMaxWaiters(2))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	results := make(chan string, 2)
	for _, id := range []string{"want1", "want2"} {
		go func() {
			if _, ok, err := s.GetContainer(ctx, id); ok && err == nil {
				results <- id
			} else {
				results <- "miss:" + id
			}
		}()
	}
	waitForCount(t, s, 2)

	// Third distinct blocking lookup: shed immediately, not queued.
	start := time.Now()
	_, ok, err := s.GetContainer(ctx, "want3")
	if ok || !errors.Is(err, ErrTooManyWaiters) {
		t.Fatalf("overflow lookup: ok=%v err=%v, want ErrTooManyWaiters", ok, err)
	}
	if d := time.Since(start); d > time.Second {
		t.Fatalf("overflow lookup blocked %v, want immediate shed", d)
	}
	if got := s.BlockedLookups(); got != 2 {
		t.Fatalf("waiter count after shed = %d, want 2", got)
	}

	// A lookup for an ID that is already resolvable is unaffected by the cap.
	s.UpsertPod(makePod("uidx", "podx", "node1", "1", map[string]string{"app": "known1"}))
	if _, ok, err := s.GetContainer(ctx, "known1"); !ok || err != nil {
		t.Fatalf("known lookup at cap: ok=%v err=%v", ok, err)
	}

	// The registered waiters wake when their IDs appear.
	s.UpsertPod(makePod("uid1", "pod1", "node1", "1", map[string]string{"app": "want1"}))
	s.UpsertPod(makePod("uid2", "pod2", "node1", "1", map[string]string{"app": "want2"}))
	got := map[string]bool{}
	for range 2 {
		got[<-results] = true
	}
	if !got["want1"] || !got["want2"] {
		t.Fatalf("waiters did not resolve: %v", got)
	}
	waitForCount(t, s, 0)
}

// Every code path that unblocks a waiter (wake, ctx cancel) must release its
// accounting; otherwise the cap would wedge shut over time.
func TestWaiterAccountingDrains(t *testing.T) {
	s, _ := newTestStore(time.Minute)

	// N waiters cancelled by ctx: count returns to zero, map empties.
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	for i := range 50 {
		wg.Go(func() {
			id := "garbage" + string(rune('a'+i%26)) + string(rune('a'+i/26))
			_, _, _ = s.GetContainer(ctx, id)
		})
	}
	waitFn := func() int { return s.BlockedLookups() }
	deadline := time.Now().Add(5 * time.Second)
	for waitFn() < 50 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if waitFn() != 50 {
		t.Fatalf("waiters registered = %d, want 50", waitFn())
	}
	cancel()
	wg.Wait()
	if got := s.BlockedLookups(); got != 0 {
		t.Fatalf("waiter count after cancel = %d, want 0", got)
	}
	s.mu.RLock()
	entries := len(s.waiters)
	s.mu.RUnlock()
	if entries != 0 {
		t.Fatalf("waiter map entries after cancel = %d, want 0", entries)
	}

	// Waiters woken by an upsert drain the accounting too.
	ctx2, cancel2 := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel2()
	done := make(chan struct{})
	go func() {
		_, _, _ = s.GetContainer(ctx2, "woken1")
		close(done)
	}()
	waitForCount(t, s, 1)
	s.UpsertPod(makePod("uid1", "pod1", "node1", "1", map[string]string{"app": "woken1"}))
	<-done
	waitForCount(t, s, 0)
}

// A grotesquely long ID can never be a real runtime ID; it must not become a
// waiter key (client-chosen bytes pinned in the map) — the lookup degrades to
// an immediate miss.
func TestOversizedIDDoesNotWait(t *testing.T) {
	s, _ := newTestStore(time.Minute)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	huge := strings.Repeat("a", 1<<20) // 1 MiB of "container ID"
	start := time.Now()
	_, ok, err := s.GetContainer(ctx, huge)
	if ok || err != nil {
		t.Fatalf("oversized lookup: ok=%v err=%v, want plain miss", ok, err)
	}
	if d := time.Since(start); d > time.Second {
		t.Fatalf("oversized lookup blocked %v, want immediate miss", d)
	}
	if got := s.BlockedLookups(); got != 0 {
		t.Fatalf("oversized ID registered a waiter (count %d)", got)
	}
}

// A lookup that CANNOT block (?wait=0, or a client that already hung up) must
// not walk the waiter protocol: registering the channel takes the exclusive
// lock and removeWaiter takes it again, so an unauthenticated caller could
// drive write-lock churn that every reader and the informer goroutine contend
// with — for a channel nothing could ever wait on. The allocation count is the
// proxy: the channel plus its map entry are exactly what the registration
// costs.
func TestNonBlockingMissDoesNotRegisterAWaiter(t *testing.T) {
	if testrace.Enabled {
		t.Skip("-race perturbs allocation counts")
	}
	s, _ := newTestStore(time.Minute)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	got := testing.AllocsPerRun(100, func() {
		if _, ok, err := s.GetContainer(ctx, "deadbeef01"); ok || err != nil {
			t.Fatalf("unknown container resolved: ok=%v err=%v", ok, err)
		}
	})
	if got != 0 {
		t.Errorf("a non-blocking miss allocates %v times per call, want 0", got)
	}
	if n := s.BlockedLookups(); n != 0 {
		t.Errorf("%d waiters left registered", n)
	}
}
