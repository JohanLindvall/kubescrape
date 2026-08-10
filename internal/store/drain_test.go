package store

import (
	"context"
	"errors"
	"testing"
	"time"
)

// A parked lookup must be ANSWERED when the process starts shutting down.
// Without Drain it stayed parked while http.Server.Shutdown waited out its
// budget and then returned without closing anything, and the process exit that
// followed cut the connection: no status, no body ("Empty reply from server"),
// and nothing logged. The wake must be a retryable refusal, not a 404 — the
// container may well exist, and the next pod behind the Service can answer.
func TestDrainReleasesParkedLookups(t *testing.T) {
	s, _ := newTestStore(time.Minute)

	// A budget far longer than the test: if the drain does not wake this, the
	// test times out rather than passing on a coincidence.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	type result struct {
		ok  bool
		err error
	}
	done := make(chan result, 1)
	go func() {
		_, ok, err := s.GetContainer(ctx, "parked1")
		done <- result{ok, err}
	}()
	waitForCount(t, s, 1)

	if released := s.Drain(); released != 1 {
		t.Fatalf("Drain released %d, want 1", released)
	}

	select {
	case got := <-done:
		if got.ok || !errors.Is(got.err, ErrShuttingDown) {
			t.Fatalf("parked lookup: ok=%v err=%v, want ErrShuttingDown", got.ok, got.err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("parked lookup was not released by Drain")
	}

	if got := s.DrainedLookups(); got != 1 {
		t.Fatalf("DrainedLookups = %d, want 1", got)
	}
	// The cap counter is the abuse signal and must stay clean: a rolling update
	// firing it would page an operator on every deploy.
	if got := s.ShedLookups(); got != 0 {
		t.Fatalf("ShedLookups = %d, want 0 (a drain is not a shed)", got)
	}
	if got := s.waiterCount(); got != 0 {
		t.Fatalf("waiter count after drain = %d, want 0", got)
	}

	// An upsert racing the drain must not close an already-closed channel:
	// Drain deletes the map entries exactly as the wakeup path does, so this
	// upsert panics ("close of closed channel") if it ever stops doing that.
	s.UpsertPod(makePod("uid1", "pod1", "node1", "1", map[string]string{"app": "parked1"}))
	if _, ok, err := s.GetContainer(ctx, "parked1"); !ok || err != nil {
		t.Fatalf("resolvable lookup after drain: ok=%v err=%v, want it served", ok, err)
	}
}

// After the drain, a lookup that WOULD park is refused immediately instead:
// the informers are stopping, so the ID it would wait for can never arrive,
// and holding the handler only parks it until the process exit cuts it.
func TestDrainRefusesLaterBlockingLookups(t *testing.T) {
	s, _ := newTestStore(time.Minute)
	s.Drain()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	start := time.Now()
	_, ok, err := s.GetContainer(ctx, "later1")
	if ok || !errors.Is(err, ErrShuttingDown) {
		t.Fatalf("lookup after drain: ok=%v err=%v, want ErrShuttingDown", ok, err)
	}
	if d := time.Since(start); d > time.Second {
		t.Fatalf("lookup after drain blocked %v, want an immediate refusal", d)
	}
	if got := s.DrainedLookups(); got != 1 {
		t.Fatalf("DrainedLookups = %d, want 1", got)
	}

	// A hit is still served: shutting down bounds WAITING, not answering.
	s.UpsertPod(makePod("uid1", "pod1", "node1", "1", map[string]string{"app": "known1"}))
	if _, ok, err := s.GetContainer(ctx, "known1"); !ok || err != nil {
		t.Fatalf("known lookup after drain: ok=%v err=%v, want it served", ok, err)
	}
}
