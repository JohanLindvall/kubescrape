package metrics

import (
	"context"
	"testing"
	"time"
)

// TestHistogramIdleEmitThroughExport is the end-to-end counterpart to
// TestHistogramIdleEmitKeepsAllBuckets: it drives the partial-emit scenario
// through the real Dynamic.Add + Export (OTLP) path and asserts the RENDERED
// histogram point carries the complete, correct distribution across an idle
// reset — not just the buckets the last observation touched.
func TestHistogramIdleEmitThroughExport(t *testing.T) {
	t0 := int64(1_700_800_000)
	setTimeForTest(time.Unix(t0, 0))
	defer testEpoch.Store(0)

	set, err := NewDynamicMetricSet([]Dynamic{{
		Name:    "lat_seconds",
		Type:    HistogramType,
		Value:   "d",
		Buckets: []float64{1, 5, 7.5, 10},
		MaxAge:  "30s", // shorter than the (75s) export gap below — the trigger
	}})
	if err != nil {
		t.Fatal(err)
	}
	add := func(v string) {
		set.Add(valuesFrom(map[string]string{"d": v}), labelsFrom(map[string]string{"d": v}), noRes(), "")
	}

	add("5") // (1,5]
	if err := set.Export(context.Background(), &capExporter{}, 0); err != nil {
		t.Fatal(err) // full export; marks buckets exported
	}
	setTimeForTest(time.Unix(t0+15, 0))
	add("8") // (7.5,10] — touches only the upper buckets
	setTimeForTest(time.Unix(t0+75, 0))
	idle := &capExporter{} // inspect the idle export in isolation
	if err := set.Export(context.Background(), idle, 0); err != nil {
		t.Fatal(err) // idle reset: the point must still carry all buckets
	}

	m, ok := idle.find("lat_seconds")
	if !ok {
		t.Fatal("histogram not exported on the idle reset")
	}
	dp := m.Histogram().DataPoints().At(0)
	if dp.Count() != 2 {
		t.Fatalf("count = %d, want 2 (both observations)", dp.Count())
	}
	if s := dp.Sum(); s < 12.9 || s > 13.1 {
		t.Fatalf("sum = %v, want 13 (5+8)", s)
	}
	// Absolute bucket counts [le1, (1,5], (5,7.5], (7.5,10], +Inf]: the value-5
	// observation must sit in (1,5], not be dropped or misplaced up to (7.5,10].
	got := dp.BucketCounts().AsRaw()
	want := []uint64{0, 1, 0, 1, 0}
	if len(got) != len(want) {
		t.Fatalf("bucket counts = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("bucket counts = %v, want %v (value-5 lost from (1,5])", got, want)
		}
	}
}

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
