package journald

import (
	"testing"
	"time"
)

// TestShutdownFlushBudgetMatchesTailerDefault pins shutdownFlushBudget to the
// value the tailer's defaultShutdownBudget carries
// (internal/agent/tailer/tailer.go): the agent budgets every final export
// against the pod's terminationGracePeriodSeconds, so the two drifting apart
// would let one pipeline's flush eat another's share. Both constants are
// unexported across packages, so the pin is by VALUE with the tailer constant
// named here — if either moves, move both.
func TestShutdownFlushBudgetMatchesTailerDefault(t *testing.T) {
	if shutdownFlushBudget != 10*time.Second {
		t.Fatalf("shutdownFlushBudget = %v, want 10s (the tailer's defaultShutdownBudget; keep them in step)", shutdownFlushBudget)
	}
}
