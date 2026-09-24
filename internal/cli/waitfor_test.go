package cli

import (
	"sync"
	"testing"
	"time"
)

// A shutdown that has spent its shared deadline reaches its final join with a
// budget of 0 (or less), and the join must still ANSWER rather than assume: a
// zero timer is ready before the goroutine that observes the WaitGroup has been
// scheduled, so an already-drained group reported a timeout that had not
// happened — on exactly the shutdown whose log an operator reads to find out
// what went wrong, where "did not stop" then accused goroutines that had.
func TestWaitForAnswersAnAlreadyDrainedGroupAtASpentBudget(t *testing.T) {
	for _, budget := range []time.Duration{0, -time.Millisecond, -1200 * time.Millisecond} {
		var wg sync.WaitGroup // nothing added: already drained
		for i := range 200 {
			if !WaitFor(&wg, budget) {
				t.Fatalf("WaitFor(drained, %v) reported a timeout on attempt %d: a spent budget must not beat a wait that is already satisfied", budget, i)
			}
		}
	}
}

// The floor is a scheduling grace, not a wait: a group that is genuinely still
// running is still reported as such, and within its budget.
func TestWaitForStillTimesOutOnAGroupThatHasNotStopped(t *testing.T) {
	var wg sync.WaitGroup
	wg.Add(1)
	defer wg.Done()
	if WaitFor(&wg, 0) {
		t.Error("WaitFor reported a live group as stopped at a spent budget")
	}
	start := time.Now()
	if WaitFor(&wg, 20*time.Millisecond) {
		t.Error("WaitFor reported a live group as stopped")
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Errorf("WaitFor blocked for %v past a 20ms budget", elapsed)
	}
}

// And a positive budget returns as soon as the group drains, rather than
// waiting the budget out.
func TestWaitForReturnsAsSoonAsTheGroupDrains(t *testing.T) {
	var wg sync.WaitGroup
	wg.Go(func() { time.Sleep(5 * time.Millisecond) })
	start := time.Now()
	if !WaitFor(&wg, time.Minute) {
		t.Fatal("WaitFor timed out on a group that drained")
	}
	if d := time.Since(start); d > 10*time.Second {
		t.Errorf("WaitFor took %v; it waited out the budget instead of the group", d)
	}
}
