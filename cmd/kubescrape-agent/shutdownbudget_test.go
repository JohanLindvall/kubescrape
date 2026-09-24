package main

import (
	"testing"
	"time"
)

// Every wait and step of the shutdown sequence takes its budget through ONE
// clamp: the full limit before the deadline is anchored (an early return from
// run()), what is left of the deadline when that is less, and zero — never a
// negative — once it has been spent.
func TestShutdownBudgetClampsToTheSharedDeadline(t *testing.T) {
	const limit = 10 * time.Second
	if got := shutdownBudget(limit, time.Time{}); got != limit {
		t.Errorf("unanchored deadline: budget = %v, want the whole limit %v", got, limit)
	}
	if got := shutdownBudget(limit, time.Now().Add(time.Hour)); got != limit {
		t.Errorf("distant deadline: budget = %v, want the limit %v", got, limit)
	}
	if got := shutdownBudget(limit, time.Now().Add(2*time.Second)); got <= 0 || got > 2*time.Second {
		t.Errorf("near deadline: budget = %v, want what is left of it (0, 2s]", got)
	}
	if got := shutdownBudget(limit, time.Now().Add(-time.Second)); got != 0 {
		t.Errorf("spent deadline: budget = %v, want 0 — a negative budget is not a wait", got)
	}
}
