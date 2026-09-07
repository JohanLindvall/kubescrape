package main

import (
	"sync"
	"testing"
	"time"
)

// A shutdown that has spent its shared deadline reaches the deferred producer
// join with nothing left, and the join must still ANSWER rather than assume.
// time.After(<=0) is ready before the goroutine that observes the WaitGroup has
// been scheduled at all, so an already-drained group reported a timeout that had
// not happened — and it did so on exactly the shutdown whose log an operator
// reads to find out what went wrong: the truthful "shutdown deadline exceeded"
// line was joined by "producers did not stop", accusing a tailer, journald or
// events producer that had in fact stopped.
func TestWaitForAnswersAnAlreadyDrainedGroupAtASpentBudget(t *testing.T) {
	for _, budget := range []time.Duration{0, -time.Millisecond, -1200 * time.Millisecond} {
		var wg sync.WaitGroup // nothing added: already drained
		for i := 0; i < 200; i++ {
			if !waitFor(&wg, budget) {
				t.Fatalf("waitFor(drained, %v) reported a timeout on attempt %d: a spent budget must not beat a wait that is already satisfied", budget, i)
			}
		}
	}
}

// The floor is a scheduling grace, not a wait: a group that is genuinely still
// running is still reported as such.
func TestWaitForStillTimesOutOnAProducerThatHasNotStopped(t *testing.T) {
	var wg sync.WaitGroup
	wg.Add(1)
	defer wg.Done()
	if waitFor(&wg, 0) {
		t.Error("waitFor reported a live producer as stopped")
	}
	if waitFor(&wg, time.Millisecond) {
		t.Error("waitFor reported a live producer as stopped")
	}
}

// And a positive budget still returns as soon as the group drains, rather than
// waiting the budget out.
func TestWaitForReturnsAsSoonAsTheGroupDrains(t *testing.T) {
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { time.Sleep(5 * time.Millisecond); wg.Done() }()
	start := time.Now()
	if !waitFor(&wg, time.Minute) {
		t.Fatal("waitFor timed out on a group that drained")
	}
	if d := time.Since(start); d > 10*time.Second {
		t.Errorf("waitFor took %v; it waited out the budget instead of the group", d)
	}
}
