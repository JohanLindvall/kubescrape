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

// Concurrent use must be race-free AND must respect the cap: the earlier
// version of this test used a key space of 26x26 with a cap of 1000, so it
// never reached the cap, reclaim or saturation branches at all and asserted
// nothing. 1000 distinct keys against a cap of 200 exercises all three.
func TestConcurrentUse(t *testing.T) {
	tab := New(200, time.Minute)
	var wg sync.WaitGroup
	for i := range 50 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := range 20 {
				tab.Allow(fmt.Sprintf("%d-%d", i, j))
			}
		}()
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
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if again.Allow(time.Hour) {
				mu.Lock()
				fired++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if fired != 1 {
		t.Fatalf("concurrent racers fired %d times, want exactly 1", fired)
	}
}
