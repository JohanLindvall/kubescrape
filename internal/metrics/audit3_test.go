package metrics

// AUDIT round 5 (adversarial review of 7e653f2).

import (
	"testing"
	"time"
)

// TestDeleteEmitsNeverExportedSample: snapshot() grew an emit-before-reset
// guard for the `idle > 0` branch (expiringSample.exported), but the branch
// ABOVE it — `idle >= 4*60`, which DELETES the sample outright — was left alone.
// It is the same loss, and it is reachable with the same "maxAge below the
// export interval" configuration the fix was written for: whenever
//
//	exportInterval > maxAge + 240s
//
// a sample observed just after an export is deleted at the next one, having
// never been emitted. -logs-metrics-interval=5m with `maxAge: 30s` (both legal,
// neither clamped nor validated) loses every observation, permanently and
// silently — the counter reports nothing at all.
//
// The package's own TestEvictThenReadmitAtCap asserts exactly this loss
// (snapshot at t0+300 returns 0 samples for a series observed at t0), so the
// hole is codified rather than caught.
func TestDeleteEmitsNeverExportedSample(t *testing.T) {
	t0 := int64(1_700_900_000)
	setTimeForTest(time.Unix(t0, 0))
	defer testEpoch.Store(0)

	// maxAge 30s, export interval 5m: both legal, and their combination means
	// idle == 300-30 == 270 >= 240 at the very first export.
	s := newSeries(seriesSpec{name: "c", kind: kindCounter, expiration: 30 * time.Second})
	s.observe(labels{}.set("k", "v"), 7, resKey{}, emptyResource, nil)

	setTimeForTest(time.Unix(t0+300, 0)) // the first export after the observation
	var total float64
	for _, samp := range s.snapshot() {
		total += samp.value
	}
	if total != 7 {
		t.Fatalf("exported total = %v, want 7: the sample was DELETED by the 4-minute grace sweep "+
			"without ever being exported — the same never-exported loss the idle-reset branch just fixed", total)
	}
}
