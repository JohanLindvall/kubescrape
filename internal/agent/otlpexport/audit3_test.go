package otlpexport

// AUDIT round 5 (adversarial review of bbcf4bb): the poison-batch drop.

import (
	"testing"

	"go.opentelemetry.io/collector/pdata/plog"
)

// TestOutageCyclesDoNotCountTowardPoisonBudget: stuckTooLong counts a
// batch's failed cycles from the moment it FIRST fails — including every cycle
// of a collector outage, when by the commit's own reasoning nothing is poison —
// and never resets that count when the collector proves alive. deliveredAt is
// only recorded on the first sighting, and the entry is only deleted on a drop.
//
// So after any outage longer than maxDrainCycles cycles, every batch in the
// spool is already "over budget"; the instant ONE batch gets through
// (s.delivered++), the next single failure of any other batch satisfies both
// clauses (cycles >= 3, delivered != deliveredAt) and it is DROPPED — with no
// evidence whatsoever that it is poison. The batch failed once since the
// collector proved alive, not maxDrainCycles times.
//
// This is the normal shape of a collector recovery: a backlogged spool drains
// into a collector that is still cold and 503s/429s intermittently, or a second
// rollout follows the first. Each such blip discards log batches the buffer
// exists to protect — the zero-loss breach the "only while the collector is
// accepting other batches" condition was written to prevent.
//
// Fix: reset the cycle counter (or re-seed deliveredAt) whenever s.delivered
// advances, so the maxDrainCycles budget is only ever spent on failures that
// happened while the collector was demonstrably accepting other batches.
func TestOutageCyclesDoNotCountTowardPoisonBudget(t *testing.T) {
	s := &sink[plog.Logs]{kind: "logs"}
	data := []byte("a perfectly good log batch")

	// Collector outage: this batch circles the queue, failing every cycle.
	// Nothing may be dropped (and nothing is).
	for i := 0; i < 10; i++ {
		if s.stuckTooLong(data) {
			t.Fatalf("dropped during the outage at cycle %d", i+1)
		}
	}

	// The collector comes back and accepts one other batch from the backlog.
	s.delivered++

	// It then fails this batch ONCE (a 503 while it is still cold, or a second
	// rollout). One failure is not evidence of a poison payload.
	if s.stuckTooLong(data) {
		t.Fatalf("ZERO-LOSS BREACH: batch dropped after a SINGLE failure following the recovery — "+
			"its %d outage cycles were counted toward the poison budget and never reset "+
			"(stuckBatch.cycles keeps accumulating and deliveredAt is only set on first sighting)", 10)
	}
}
