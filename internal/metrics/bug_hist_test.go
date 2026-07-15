package metrics

import (
	"testing"
	"time"
)

// TestHistogramIdleEmitKeepsAllBuckets is the regression test for the partial
// histogram emit: with the export interval longer than maxAge, an observation
// that lands only in the upper buckets used to make the next idle snapshot emit
// ONLY those buckets, dropping the lower buckets' cumulative counts from the
// point (a strict subset of the distribution). Every bucket of a family shares
// its idle deadline, so a complete point must carry all of them.
func TestHistogramIdleEmitKeepsAllBuckets(t *testing.T) {
	t0 := int64(1_700_600_000)
	setTimeForTest(time.Unix(t0, 0))
	defer testEpoch.Store(0)

	// maxAge 30s, export gap 60s — a legal configuration.
	s := newSeries(seriesSpec{name: "h", kind: kindHistogram,
		buckets: []float64{1, 5, 7.5, 10}, expiration: 30 * time.Second})
	lbls := labels{}.set("route", "/x")

	s.observe(lbls, 5, resKey{}, emptyResource, nil) // lands in le>=5
	if len(s.snapshot()) == 0 {                      // full export: marks every bucket exported
		t.Fatal("first snapshot emitted nothing")
	}

	setTimeForTest(time.Unix(t0+15, 0))
	s.observe(lbls, 8, resKey{}, emptyResource, nil) // lands only in le>=10

	setTimeForTest(time.Unix(t0+75, 0)) // past maxAge -> idle reset branch
	byBucket := map[int]uint64{}
	for _, sm := range s.snapshot() {
		byBucket[sm.bucket] = sm.count
	}

	// All five bucket streams (le=1,5,7.5,10,+Inf) must be present, and the
	// cumulative counts must reflect BOTH observations: one <=5, both <=10.
	if len(byBucket) != 5 {
		t.Fatalf("idle snapshot emitted %d/5 bucket streams: %v — lower buckets were dropped", len(byBucket), byBucket)
	}
	if byBucket[1] != 1 { // le=5
		t.Fatalf("le=5 count = %d, want 1 (the value-5 observation): distribution corrupted", byBucket[1])
	}
	if byBucket[3] != 2 { // le=10
		t.Fatalf("le=10 count = %d, want 2", byBucket[3])
	}
	if byBucket[4] != 2 { // +Inf
		t.Fatalf("+Inf count = %d, want 2", byBucket[4])
	}
}
