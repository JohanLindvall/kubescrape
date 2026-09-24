package logdedupe

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func atClock(t *Table, tick *time.Time) { t.now = func() time.Time { return *tick } }

func TestOncePerProcessWithNoWindow(t *testing.T) {
	tab := New(10, 0)
	if allow, _ := tab.Allow("k"); !allow {
		t.Fatal("first call must log")
	}
	for range 5 {
		if allow, _ := tab.Allow("k"); allow {
			t.Fatal("a zero window means once per process; the key logged again")
		}
	}
	if allow, _ := tab.Allow("other"); !allow {
		t.Fatal("a different key must log")
	}
}

func TestReWarnsAfterTheWindow(t *testing.T) {
	now := time.Unix(0, 0)
	tab := New(10, time.Minute)
	atClock(tab, &now)

	if allow, _ := tab.Allow("k"); !allow {
		t.Fatal("first call must log")
	}
	now = now.Add(59 * time.Second)
	if allow, _ := tab.Allow("k"); allow {
		t.Fatal("logged inside the window")
	}
	now = now.Add(2 * time.Second)
	if allow, _ := tab.Allow("k"); !allow {
		t.Fatal("did not re-warn after the window elapsed")
	}
}

// THE point of this package. A full table must SUPPRESS new keys, never clear
// itself: clearing means that at cap+1 distinct keys the table re-fills,
// overflows and clears on every cycle, so every key logs every time — worse
// than no cap at all, and silent.
func TestSaturationSuppressesAndNeverClears(t *testing.T) {
	now := time.Unix(0, 0)
	tab := New(4, time.Minute)
	atClock(tab, &now)

	for i := range 4 {
		if allow, sat := tab.Allow(string(rune('a' + i))); !allow || sat {
			t.Fatalf("key %d: allow=%v saturated=%v, want true/false", i, allow, sat)
		}
	}
	// The 5th distinct key is refused, and exactly that call reports saturation.
	allow, sat := tab.Allow("e")
	if allow || !sat {
		t.Fatalf("at cap: allow=%v saturated=%v, want false/true", allow, sat)
	}
	// The notice fires ONCE, however many further keys arrive.
	for _, k := range []string{"f", "g", "h"} {
		if allow, sat := tab.Allow(k); allow || sat {
			t.Fatalf("key %q after saturation: allow=%v saturated=%v, want false/false", k, allow, sat)
		}
	}
	// And the table did not grow past the cap, nor lose what it holds.
	if n := tab.Len(); n != 4 {
		t.Fatalf("table holds %d keys, want 4 (it must neither grow nor clear)", n)
	}

	// The regression this package exists to prevent: cycle through cap+1 keys
	// repeatedly, WITHIN the window. A clearing table would allow on every
	// lap; a suppressing one allows nothing new.
	for lap := range 3 {
		for i := range 5 {
			if allow, _ := tab.Allow(string(rune('a' + i))); allow {
				t.Fatalf("lap %d key %d logged again: the table is re-arming the flood", lap, i)
			}
		}
	}
}

// A key already in the table must keep its schedule even when the table is
// full — saturation refuses NEW keys, it does not freeze existing ones.
func TestSaturationDoesNotBlockExistingKeys(t *testing.T) {
	now := time.Unix(0, 0)
	tab := New(2, time.Minute)
	atClock(tab, &now)
	tab.Allow("a")
	tab.Allow("b")
	if allow, sat := tab.Allow("c"); allow || !sat {
		t.Fatalf("want saturation on the third key, got allow=%v sat=%v", allow, sat)
	}
	now = now.Add(2 * time.Minute)
	if allow, _ := tab.Allow("a"); !allow {
		t.Fatal("an existing key must still re-warn on schedule at saturation")
	}
}

// Expired entries are reclaimed rather than counted against the cap — that is
// honest bookkeeping (an expired entry would have been allowed to log anyway),
// as distinct from clearing live suppression state.
func TestExpiredEntriesAreReclaimed(t *testing.T) {
	now := time.Unix(0, 0)
	tab := New(2, time.Minute)
	atClock(tab, &now)
	tab.Allow("a")
	tab.Allow("b")
	now = now.Add(2 * time.Minute) // both age out
	if allow, sat := tab.Allow("c"); !allow || sat {
		t.Fatalf("a new key must take a reclaimed slot: allow=%v sat=%v", allow, sat)
	}
}

// A FULL table must not walk its whole map for every refused new key: a
// refused key is never inserted, so it would pay the walk again next time, and
// nothing can have expired before the oldest entry's window runs out. The
// sweep runs once per reclaimable instant — and is never late: the new key
// arriving when the oldest entry expires is admitted into its slot.
func TestAReclaimSweepIsSkippedUntilSomethingCanHaveExpired(t *testing.T) {
	t0 := time.Unix(0, 0)
	now := t0
	tab := New(4, time.Minute)
	atClock(tab, &now)
	for i, k := range []string{"a", "b", "c", "d"} {
		now = t0.Add(time.Duration(i) * 10 * time.Second)
		if allow, _ := tab.Allow(k); !allow {
			t.Fatalf("setup: key %q refused", k)
		}
	}

	now = t0.Add(40 * time.Second)
	if allow, sat := tab.Allow("e"); allow || !sat {
		t.Fatalf("at cap: allow=%v saturated=%v, want false/true", allow, sat)
	}
	if tab.sweeps != 1 {
		t.Fatalf("first refusal at a full table swept %d times, want 1 (it cannot know yet)", tab.sweeps)
	}
	// Two more refusals, one at the same instant and one just before the
	// oldest entry ("a", stamped at t0) expires: nothing can have expired, so
	// neither may walk the map.
	tab.Allow("f")
	now = t0.Add(time.Minute - time.Nanosecond)
	tab.Allow("g")
	if tab.sweeps != 1 {
		t.Fatalf("refusals before anything could expire swept %d times, want still 1: "+
			"every refused new key is walking the whole table", tab.sweeps)
	}

	// The instant "a" expires, a sweep runs and the new key takes its slot.
	now = t0.Add(time.Minute)
	if allow, _ := tab.Allow("h"); !allow {
		t.Fatal("the new key arriving as the oldest entry expires was refused: the skipped sweep is LATE")
	}
	if tab.sweeps != 2 {
		t.Fatalf("sweeps = %d, want 2", tab.sweeps)
	}
	// Full again, and the bound moved to the next survivor ("b", at t0+10s).
	if allow, _ := tab.Allow("i"); allow {
		t.Fatal("admitted past the cap")
	}
	if tab.sweeps != 2 {
		t.Fatalf("a refusal before the next survivor expires swept (sweeps = %d, want 2)", tab.sweeps)
	}
	now = t0.Add(time.Minute + 10*time.Second)
	if allow, _ := tab.Allow("i"); !allow {
		t.Fatal("the new key was refused once the next survivor expired")
	}
	if n := tab.Len(); n != 4 {
		t.Fatalf("table holds %d keys, want the cap 4", n)
	}
}

// An existing key's refresh moves its own expiry LATER than the recorded
// bound. That may cost a sweep that frees nothing — but it must never make a
// later reclaim late.
func TestARefreshedKeyDoesNotDelayTheNextReclaim(t *testing.T) {
	t0 := time.Unix(0, 0)
	now := t0
	tab := New(2, time.Minute)
	atClock(tab, &now)
	tab.Allow("a") // t0
	now = t0.Add(30 * time.Second)
	tab.Allow("b") // t0+30s
	tab.Allow("c") // full: sweeps, bound = a's expiry at t0+60s

	now = t0.Add(time.Minute)
	if allow, _ := tab.Allow("a"); !allow { // refresh: a now expires at t0+120s
		t.Fatal("setup: an existing key did not re-warn after its window")
	}
	// At the recorded bound nothing has expired (a was refreshed): the sweep
	// frees nothing, and re-derives the bound from b.
	if allow, _ := tab.Allow("c"); allow {
		t.Fatal("admitted past the cap")
	}
	now = t0.Add(90 * time.Second) // b expires
	if allow, _ := tab.Allow("c"); !allow {
		t.Fatal("the new key was refused once b expired: the refresh made the reclaim late")
	}
}

// Concurrent use must be race-free AND must respect the cap: the earlier
// version of this test used a key space of 26x26 with a cap of 1000, so it
// never reached the cap, reclaim or saturation branches at all and asserted
// nothing. 1000 distinct keys against a cap of 200 exercises all three.
func TestConcurrentUse(t *testing.T) {
	tab := New(200, time.Minute)
	var wg sync.WaitGroup
	for i := range 50 {
		wg.Go(func() {
			for j := range 20 {
				tab.Allow(fmt.Sprintf("%d-%d", i, j))
			}
		})
	}
	wg.Wait()
	if n := tab.Len(); n != 200 {
		t.Errorf("table holds %d keys after 1000 distinct concurrent keys, want exactly the cap 200 "+
			"(over means the bound leaks, under means it cleared itself)", n)
	}
}

// The ordering inside Allow — look the key up BEFORE the cap check — is what
// keeps the one-shot saturation notice attached to a genuinely NEW key. An
// existing key inside its window at a full table must be refused QUIETLY.
func TestExistingKeyAtSaturationIsQuiet(t *testing.T) {
	now := time.Unix(0, 0)
	tab := New(2, time.Minute)
	atClock(tab, &now)
	tab.Allow("a")
	tab.Allow("b")
	if allow, sat := tab.Allow("c"); allow || !sat {
		t.Fatalf("setup: want the saturation notice on the third key, got %v/%v", allow, sat)
	}
	// "a" is present and inside its window: refused, and NOT re-announced as a
	// truncation — the notice belongs to new keys.
	if allow, sat := tab.Allow("a"); allow || sat {
		t.Errorf("existing key at saturation: allow=%v saturated=%v, want false/false", allow, sat)
	}
}

// Throttle: first caller through fires, a caller inside the interval does not,
// and the CAS loser rule keeps concurrent racers to ONE fire per interval.
func TestThrottle(t *testing.T) {
	var th Throttle
	if !th.Allow(time.Hour) {
		t.Fatal("zero-value Throttle must allow the first fire")
	}
	if th.Allow(time.Hour) {
		t.Fatal("second fire inside the interval must be suppressed")
	}
	var again Throttle
	fired := 0
	var mu sync.Mutex
	var wg sync.WaitGroup
	for range 16 {
		wg.Go(func() {
			if again.Allow(time.Hour) {
				mu.Lock()
				fired++
				mu.Unlock()
			}
		})
	}
	wg.Wait()
	if fired != 1 {
		t.Fatalf("concurrent racers fired %d times, want exactly 1", fired)
	}
}

// The re-fire through the REAL Allow and clock, which the claim-level tests
// cannot see: with a zero interval every call has waited long enough, so a
// throttle that fired once per process — a CompareAndSwap from 0 only, say —
// is caught here.
func TestThrottleFiresAgainOnceTheIntervalHasElapsed(t *testing.T) {
	var th Throttle
	for i := range 3 {
		if !th.Allow(0) {
			t.Fatalf("call %d with a zero interval was suppressed: the throttle never re-fires", i+1)
		}
	}
}

// AllowAt follows the caller's clock, whatever that clock's origin: first call
// fires, repeats inside the interval stay silent, the interval elapsing fires
// again. A holder with an injected clock relies on exactly this to test its
// re-warn cadence without sleeping — a fake clock far from the process epoch
// included.
func TestThrottleAllowAtFollowsTheCallersClock(t *testing.T) {
	for _, start := range []time.Time{time.Now(), time.Unix(1_700_000_000, 0), time.Now().Add(1000 * time.Hour)} {
		var th Throttle
		now := start
		if !th.AllowAt(now, time.Minute) {
			t.Fatalf("start %v: the first AllowAt must fire", start)
		}
		now = now.Add(59 * time.Second)
		if th.AllowAt(now, time.Minute) {
			t.Fatalf("start %v: AllowAt fired again inside the interval", start)
		}
		now = now.Add(time.Second)
		if !th.AllowAt(now, time.Minute) {
			t.Fatalf("start %v: AllowAt stayed silent once the interval had elapsed", start)
		}
	}
}

// The loser rule, deterministically: two callers that observed the SAME last
// value race for one slot, and exactly one wins. TestThrottle's goroutine race
// passes most of the time even without the CompareAndSwap (the racers rarely
// interleave), so it cannot tell whether the rule is there; this can.
func TestThrottleLoserOfTheRaceStaysSilent(t *testing.T) {
	var th Throttle
	last, now := th.last.Load(), int64(time.Hour)
	if !th.claim(last, now, time.Minute) {
		t.Fatal("the first claim of a never-fired throttle must win")
	}
	if th.claim(last, now, time.Minute) {
		t.Fatal("a second caller that observed the same last value won too: the loser of the race must stay silent")
	}
	// And the slot re-opens once the interval has elapsed from the winner's
	// claim — the re-fire TestThrottle cannot reach without waiting an hour.
	if th.claim(th.last.Load(), now+int64(time.Minute)-1, time.Minute) {
		t.Fatal("re-fired before the interval elapsed")
	}
	if !th.claim(th.last.Load(), now+int64(time.Minute), time.Minute) {
		t.Fatal("did not re-fire once the interval elapsed")
	}
}

// The clock is an offset from process start, so a never-fired throttle must
// fire even when "now" is smaller than the interval — which is every call in
// the first interval after start. Without the never-fired arm a 5-minute
// throttle stayed silent for the first 5 minutes of the process.
func TestThrottleFiresFirstEvenRightAfterProcessStart(t *testing.T) {
	var th Throttle
	if !th.claim(0, 1, time.Hour) {
		t.Fatal("a never-fired throttle refused its first fire one nanosecond after the epoch")
	}
}
