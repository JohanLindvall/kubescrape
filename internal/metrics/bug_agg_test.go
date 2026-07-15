package metrics

// Adversarial interaction review (angle 4): the never-exported guard added to
// snapshot()'s DELETE branch (idle >= 4*60) is gated on `!s.aggregating()`, so
// an AGGREGATING gauge (min/max/avg/first/sum/count/stddev/range/delta) that
// reaches the delete branch before it was ever emitted is dropped unseen —
// exactly the loss the sibling non-aggregating fix just closed.
//
// Reachable under the same "export interval past maxAge+grace" configuration the
// commit itself calls legal and unclamped: with expiration (maxAge) 30s and an
// export interval of 5m, a value observed just after one export has idle == 270
// >= 240 at the very next snapshot, so it hits the delete branch on its FIRST
// export. The aggregating branch (which "keeps emitting while idle") never runs
// because idle already passed the grace threshold — the windowed observation is
// destroyed without ever leaving the process.

import (
	"testing"
	"time"
)

func TestAggregatingGaugeEmittedBeforeDelete(t *testing.T) {
	t0 := int64(1_700_900_000)
	setTimeForTest(time.Unix(t0, 0))
	defer testEpoch.Store(0)

	// action=avg is an aggregating gauge; expiration 30s, export interval 5m.
	s := newSeries(seriesSpec{name: "g", kind: kindGauge, action: actionAvg, expiration: 30 * time.Second})
	s.observe(labels{}.set("k", "v"), 5, resKey{}, emptyResource, nil)

	setTimeForTest(time.Unix(t0+300, 0)) // first export after the observation
	got := s.snapshot()
	if len(got) != 1 {
		t.Fatalf("aggregating gauge produced %d samples at first export, want 1: the windowed "+
			"avg (5) was DELETED by the 4-minute grace sweep before it was ever emitted — the "+
			"delete-branch never-exported guard excludes aggregating gauges (!s.aggregating())", len(got))
	}
	if got[0].value != 5 {
		t.Fatalf("emitted aggregate = %v, want 5 (avg of a single 5)", got[0].value)
	}
}
