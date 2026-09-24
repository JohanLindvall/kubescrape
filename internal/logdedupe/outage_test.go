package logdedupe

import (
	"testing"
	"time"
)

// The rule the hand-rolled copies kept losing: a failure that OPENS a run is
// loud even when the previous run's loud line is inside the interval. A copy
// that gated on its Throttle alone opened the second outage below here at
// Debug, directly under "recovered".
func TestOutageFirstFailureOfEveryRunIsLoud(t *testing.T) {
	var o Outage
	t0 := time.Unix(1_700_000_000, 0)
	if first, loud := o.Fail(t0, time.Minute); !first || !loud {
		t.Fatalf("opening failure: first=%v loud=%v, want true/true", first, loud)
	}
	if first, loud := o.Fail(t0.Add(time.Second), time.Minute); first || loud {
		t.Fatalf("second failure of the run, inside the interval: first=%v loud=%v, want false/false — "+
			"the opening failure must claim the throttle slot its own line occupies", first, loud)
	}
	if n, lasted, ok := o.Recover(t0.Add(2 * time.Second)); !ok || n != 2 || lasted != 2*time.Second {
		t.Fatalf("Recover = %d, %v, %v; want 2, 2s, true", n, lasted, ok)
	}
	// A second run, well inside the interval of the first run's loud line.
	if first, loud := o.Fail(t0.Add(3*time.Second), time.Minute); !first || !loud {
		t.Fatalf("a second run opened with first=%v loud=%v, want true/true", first, loud)
	}
	if _, loud := o.Fail(t0.Add(4*time.Second), time.Minute); loud {
		t.Fatal("the second run's repeat was loud inside the interval")
	}
}

// The opening failure claims the throttle slot UNCONDITIONALLY. A run that
// opens inside the previous run's slot is loud (first of run) — and its line
// must still move the slot, or its second failure goes loud again the moment
// the OLD slot lapses, seconds after the opening line said the same thing.
func TestOutageOpeningFailureClaimsTheSlotEvenInsideThePreviousOne(t *testing.T) {
	var o Outage
	t0 := time.Unix(1_700_000_000, 0)
	o.Fail(t0, time.Minute)
	o.Recover(t0.Add(50 * time.Second))
	open := t0.Add(59 * time.Second) // inside run 1's slot, which lapses at +60s
	if first, loud := o.Fail(open, time.Minute); !first || !loud {
		t.Fatalf("run 2 opened with first=%v loud=%v, want true/true", first, loud)
	}
	if _, loud := o.Fail(open.Add(2*time.Second), time.Minute); loud {
		t.Fatal("run 2's second failure, 2s after its loud opening line, was loud again: " +
			"the opening line must claim the slot it occupies")
	}
	if _, loud := o.Fail(open.Add(time.Minute), time.Minute); !loud {
		t.Fatal("run 2 did not restate itself one interval after its opening line")
	}
}

// A persisting run restates itself once per interval, measured on the
// caller's clock, and carries what the run has cost so far.
func TestOutageRepeatsRestateOncePerInterval(t *testing.T) {
	var o Outage
	t0 := time.Unix(1_700_000_000, 0)
	steps := []struct {
		at   time.Duration
		loud bool
	}{
		{0, true},
		{59 * time.Second, false},
		{60 * time.Second, true},
		{61 * time.Second, false},
		{119 * time.Second, false},
		{120 * time.Second, true},
	}
	for i, s := range steps {
		if _, loud := o.Fail(t0.Add(s.at), time.Minute); loud != s.loud {
			t.Fatalf("failure %d at +%v: loud=%v, want %v", i+1, s.at, loud, s.loud)
		}
	}
	if !o.Failing() || o.Failures() != len(steps) {
		t.Fatalf("Failing=%v Failures=%d, want true/%d", o.Failing(), o.Failures(), len(steps))
	}
	if got := o.Lasted(t0.Add(120*time.Second + 400*time.Millisecond)); got != 120*time.Second {
		t.Fatalf("Lasted = %v, want 2m0s (rounded to a second)", got)
	}
}

// The recovery is reported exactly once per run, and a success outside a run
// is not a recovery.
func TestOutageRecoverReportsOncePerRun(t *testing.T) {
	var o Outage
	now := time.Unix(1_700_000_000, 0)
	if _, _, ok := o.Recover(now); ok {
		t.Fatal("a success with no run open reported a recovery")
	}
	o.Fail(now, time.Minute)
	if _, _, ok := o.Recover(now.Add(1500 * time.Millisecond)); !ok {
		t.Fatal("the first success after a failure did not report a recovery")
	}
	if _, _, ok := o.Recover(now.Add(2 * time.Second)); ok {
		t.Fatal("a second success reported the same recovery again")
	}
	if o.Failing() || o.Failures() != 0 || o.Lasted(now.Add(time.Hour)) != 0 {
		t.Fatalf("after recovery: Failing=%v Failures=%d Lasted=%v, want false/0/0",
			o.Failing(), o.Failures(), o.Lasted(now.Add(time.Hour)))
	}
}

// every == 0 is the unthrottled shape (a producer whose back-off already
// spaces its attempts): every failure is loud, and only the first opens the run.
func TestOutageZeroIntervalIsAlwaysLoud(t *testing.T) {
	var o Outage
	now := time.Unix(1_700_000_000, 0)
	for i := range 4 {
		first, loud := o.Fail(now, 0)
		if !loud || first != (i == 0) {
			t.Fatalf("failure %d with every=0: first=%v loud=%v, want %v/true", i+1, first, loud, i == 0)
		}
	}
}
