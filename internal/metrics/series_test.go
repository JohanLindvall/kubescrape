// Tests for the series store (series.go): hashing/collision guards, expiry
// and cardinality caps, and gauge actions/windowed aggregations.
package metrics

import (
	"context"
	"log/slog"
	"math"
	"testing"
	"time"

	"github.com/cespare/xxhash/v2"
)

// TestHistogramLeFoldExact pins the fold-subtract path in baseAccum/streamHash
// against the ground truth: for every bucket, subtracting the caller's "le"
// pair and adding the precomputed bucketHash/bucketCheck must be EXACTLY the
// accumulators of the merged label set that admit() serializes. With the new
// linear-projection combineHash this must stay exact (wrapping sum fold).
func TestHistogramLeFoldExact(t *testing.T) {
	s := newSeries(seriesSpec{name: "h", kind: kindHistogram, buckets: []float64{0.1, 0.5, 1}})

	for _, lbls := range []labels{
		labels{}.set("handler", "/api"),
		labels{}.set("handler", "/api").set("le", "0.5"), // caller-provided le stripped
		labels{}.set("le", "+Inf"),
		nil,
	} {
		base, check := s.baseAccum(lbls)
		for i := range s.buckets {
			gotHash := s.streamHash(base, i)
			gotCheck := s.streamCheck(check, i)

			full := lbls.without(leLabel).set(leLabel, s.bucketStr[i])
			wantBase, wantCheck := full.accums()
			if gotHash != mixHash(wantBase) {
				t.Errorf("labels %v bucket %d: streamHash %#x != full-set hash %#x", lbls, i, gotHash, mixHash(wantBase))
			}
			if gotCheck != wantCheck {
				t.Errorf("labels %v bucket %d: streamCheck %#x != full-set check %#x", lbls, i, gotCheck, wantCheck)
			}
		}
	}

	// Fold a pair out and back in: byte-identical accumulators.
	lbls := labels{}.set("a", "1").set("le", "0.25")
	h0, c0 := lbls.accums()
	hk, hv := xxhash.Sum64String("le"), xxhash.Sum64String("0.25")
	h1 := h0 - combineHash(hk, hv) + combineHash(hk, hv)
	c1 := c0 - combineCheck(hk, hv) + combineCheck(hk, hv)
	if h0 != h1 || c0 != c1 {
		t.Errorf("fold out+in not identity: (%#x,%#x) vs (%#x,%#x)", h0, c0, h1, c1)
	}
}

// TestCollisionDropObserve verifies the check-hash rejection on both observe
// paths: a primary-hash hit with a differing check is dropped and counted,
// never merged.
func TestCollisionDropObserve(t *testing.T) {
	setTimeForTest(time.Unix(1_700_400_000, 0))
	defer testEpoch.Store(0)

	s := newSeries(seriesSpec{name: "c", kind: kindCounter})
	lbls := labels{}.set("u", "alice")
	s.observe(lbls, 1, resKey{}, emptyResource, nil)

	for _, samp := range s.db {
		samp.check++ // simulate: existing sample belongs to a colliding series
	}
	before := DroppedCollision()
	s.observe(lbls, 5, resKey{}, emptyResource, nil)
	if got := DroppedCollision() - before; got != 1 {
		t.Fatalf("droppedCollision delta = %d, want 1", got)
	}
	for _, samp := range s.db {
		if samp.value != 1 {
			t.Fatalf("collision merged data: value = %v, want 1", samp.value)
		}
	}

	// observePreHashed path (registry counters).
	s2 := newSeries(seriesSpec{name: "c2", kind: kindCounter, expiration: registryExpiration})
	b := newBound(s2, labels{}.set("k", "v"))
	b.observe(1)
	for _, samp := range s2.db {
		samp.check++
	}
	before = DroppedCollision()
	b.observe(1)
	if got := DroppedCollision() - before; got != 1 {
		t.Fatalf("observePreHashed droppedCollision delta = %d, want 1", got)
	}
}

// TestHistogramCollisionAllOrNothing: a check mismatch on ANY bucket stream
// must drop the whole observation — a partial record would export underflowed
// cumulative buckets.
func TestHistogramCollisionAllOrNothing(t *testing.T) {
	setTimeForTest(time.Unix(1_700_400_100, 0))
	defer testEpoch.Store(0)

	s := newSeries(seriesSpec{name: "h", kind: kindHistogram, buckets: []float64{1, 10}})
	lbls := labels{}.set("k", "v")
	s.observe(lbls, 0.5, resKey{}, emptyResource, nil) // all 3 streams admitted, count 1 each

	// Corrupt exactly one stream's check.
	done := false
	for _, samp := range s.db {
		if !done {
			samp.check++
			done = true
		}
	}
	before := DroppedCollision()
	s.observe(lbls, 0.5, resKey{}, emptyResource, nil)
	if got := DroppedCollision() - before; got != 1 {
		t.Fatalf("droppedCollision delta = %d, want 1", got)
	}
	for _, samp := range s.db {
		if samp.count != 1 {
			t.Fatalf("sibling bucket recorded after a collision drop: count = %d, want 1 (labels %s)", samp.count, samp.labels)
		}
	}
}

// TestResourceAccumCollisionCaught: two resources whose PRIMARY accumulators
// collide (simulated) but whose check accumulators differ must not merge — the
// observation is dropped and counted (angle 4).
func TestResourceAccumCollisionCaught(t *testing.T) {
	setTimeForTest(time.Unix(1_700_400_200, 0))
	defer testEpoch.Store(0)

	s := newSeries(seriesSpec{name: "c", kind: kindCounter})
	lbls := labels{}.set("k", "v")
	resA := res(map[string]string{"k8s.pod.name": "pod-a"})
	resB := res(map[string]string{"k8s.pod.name": "pod-b"})

	s.observe(lbls, 1, resKey{accum: 12345, check: 1}, resA, nil)
	before := DroppedCollision()
	s.observe(lbls, 1, resKey{accum: 12345, check: 2}, resB, nil)
	if got := DroppedCollision() - before; got != 1 {
		t.Fatalf("droppedCollision delta = %d, want 1", got)
	}
	if len(s.db) != 1 {
		t.Fatalf("samples = %d, want 1 (collision must not admit)", len(s.db))
	}
	for _, samp := range s.db {
		if samp.value != 1 {
			t.Fatalf("value = %v, want 1 (pod-b's observation must be dropped, not merged)", samp.value)
		}
	}
}

// --- Angle 2: expiry + cardinality caps -------------------------------------

// TestEvictThenReadmitAtCap: at maxSize, a new label set is refused; after the
// old sample expires and is swept, the SAME refused hash must be admitted.
func TestEvictThenReadmitAtCap(t *testing.T) {
	t0 := int64(1_700_500_000)
	setTimeForTest(time.Unix(t0, 0))
	defer testEpoch.Store(0)

	s := newSeries(seriesSpec{name: "c", kind: kindCounter, maxSize: 1, expiration: 60 * time.Second})
	s.observe(labels{}.set("u", "a"), 1, resKey{}, emptyResource, nil)

	beforeCap := DroppedCapped()
	s.observe(labels{}.set("u", "b"), 1, resKey{}, emptyResource, nil)
	if got := DroppedCapped() - beforeCap; got != 1 {
		t.Fatalf("droppedCapped delta = %d, want 1", got)
	}

	// Past expiration + 4 min grace: the sweep deletes u=a. Its single
	// observation (value 1) never reached an export, so the delete sweep must
	// emit it once before dropping it (the never-exported guarantee) — then the
	// sample is gone from the db.
	setTimeForTest(time.Unix(t0+60+240, 0))
	if out := s.snapshot(); len(out) != 1 || out[0].value != 1 {
		t.Fatalf("expired-but-never-exported sample must ship once on delete: %+v", out)
	}
	if len(s.db) != 0 {
		t.Fatalf("expired sample not deleted: %d", len(s.db))
	}

	// The same hash re-arrives: admitted as a fresh series.
	s.observe(labels{}.set("u", "b"), 1, resKey{}, emptyResource, nil)
	if len(s.db) != 1 {
		t.Fatalf("re-admission after evict failed: db = %d", len(s.db))
	}
	out := s.snapshot()
	if len(out) != 1 || out[0].value != 1 || !out[0].initial {
		t.Fatalf("re-admitted sample = %+v, want value 1, initial", out)
	}
}

// TestExpiryEmitsBeforeDiscarding: the snapshot idle-reset used to zero a
// non-aggregating sample WITHOUT emitting it — with maxAge shorter than the
// export interval (legal; nothing clamps it), every observation made since
// the last export was silently destroyed: observed at t, exported never.
// Regression guard for the fix: an observation must appear in at least one
// export before the idle reset may zero it (expiringSample.exported).
func TestExpiryEmitsBeforeDiscarding(t *testing.T) {
	t0 := int64(1_700_510_000)
	setTimeForTest(time.Unix(t0, 0))
	defer testEpoch.Store(0)

	// maxAge 10s, export gap 30s — a legal configuration (maxAge: 10s).
	s := newSeries(seriesSpec{name: "c", kind: kindCounter, expiration: 10 * time.Second})
	s.observe(labels{}.set("k", "v"), 1, resKey{}, emptyResource, nil)

	setTimeForTest(time.Unix(t0+30, 0)) // first export 30s later
	var total float64
	for _, samp := range s.snapshot() {
		total += samp.value
	}
	setTimeForTest(time.Unix(t0+31, 0))
	s.observe(labels{}.set("k", "v"), 1, resKey{}, emptyResource, nil)
	setTimeForTest(time.Unix(t0+32, 0))
	for _, samp := range s.snapshot() {
		total += samp.value
	}
	// Two increments happened; the cumulative counter as seen across exports
	// must reach 2. The idle reset silently ate the first one (total == 1).
	if total != 2 {
		t.Fatalf("counter total across exports = %v, want 2: an observation was zeroed by the idle reset before ever being exported", total)
	}
}

// --- Angle 3: aggregating gauge windows across export cycles ----------------

func TestAggregationsAcrossThreeExports(t *testing.T) {
	const eps = 1e-6
	actions := []struct {
		action gaugeAction
		w1     float64 // aggregate of {10, 5, 20}
		w2     float64 // aggregate of {2, 4} (fresh window after reseal)
	}{
		{actionMin, 5, 2},
		{actionMax, 20, 4},
		{actionAvg, 35.0 / 3.0, 3},
		{actionFirst, 10, 2},
		{actionSum, 35, 6},
		{actionCount, 3, 2},
		{actionStddev, 6.236095644623236, 1}, // population stddev {10,5,20}; {2,4}
		{actionRange, 15, 2},
		{actionDelta, 10, 2}, // 20-10; 4-2
	}
	for _, c := range actions {
		t0 := int64(1_700_600_000)
		setTimeForTest(time.Unix(t0, 0))
		s := newSeries(seriesSpec{name: "g", kind: kindGauge, action: c.action, expiration: time.Hour})
		lbls := labels{}.set("m", "1")
		for _, v := range []float64{10, 5, 20} {
			s.observe(lbls, v, resKey{}, emptyResource, nil)
		}
		// Export 1: the window's aggregate; this seals it.
		out := s.snapshot()
		if len(out) != 1 || math.Abs(out[0].value-c.w1) > eps {
			t.Errorf("action %d export1 = %+v, want %v", c.action, out, c.w1)
		}
		// Export 2 (idle gap, no new values): aggregate KEEPS being emitted.
		setTimeForTest(time.Unix(t0+60, 0))
		out = s.snapshot()
		if len(out) != 1 || math.Abs(out[0].value-c.w1) > eps {
			t.Errorf("action %d idle export2 = %+v, want %v (kept)", c.action, out, c.w1)
		}
		// New values after the seal start a FRESH window (no leakage of the
		// old min/max/Welford state).
		setTimeForTest(time.Unix(t0+120, 0))
		for _, v := range []float64{2, 4} {
			s.observe(lbls, v, resKey{}, emptyResource, nil)
		}
		out = s.snapshot()
		if len(out) != 1 || math.Abs(out[0].value-c.w2) > eps {
			t.Errorf("action %d export3 = %+v, want %v (fresh window)", c.action, out, c.w2)
		}
	}
	testEpoch.Store(0)
}

// TestDeltaSingleValueWindowAndReset: delta of a single-value window is 0, and
// a "counter reset" (smaller value in the next window) yields that window's
// last-first, not leakage from the previous window.
func TestDeltaSingleValueWindowAndReset(t *testing.T) {
	setTimeForTest(time.Unix(1_700_610_000, 0))
	defer testEpoch.Store(0)
	s := newSeries(seriesSpec{name: "g", kind: kindGauge, action: actionDelta, expiration: time.Hour})
	lbls := labels{}.set("m", "1")

	s.observe(lbls, 100, resKey{}, emptyResource, nil)
	s.observe(lbls, 150, resKey{}, emptyResource, nil)
	if out := s.snapshot(); out[0].value != 50 {
		t.Fatalf("delta w1 = %v, want 50", out[0].value)
	}
	s.observe(lbls, 3, resKey{}, emptyResource, nil) // process restarted: counter reset
	if out := s.snapshot(); out[0].value != 0 {
		t.Fatalf("delta single-value w2 = %v, want 0", out[0].value)
	}
	s.observe(lbls, 5, resKey{}, emptyResource, nil)
	s.observe(lbls, 9, resKey{}, emptyResource, nil)
	if out := s.snapshot(); out[0].value != 4 {
		t.Fatalf("delta w3 = %v, want 4 (9-5, no leakage)", out[0].value)
	}
}

// --- Angle 4: resource grouping ----------------------------------------------

// --- Angle 9: hash domain separation between the three label namespaces -------
//
// TestResourceAndDataPointLabelsHaveSeparateHashDomains: the series key used
// to be a plain sum of combineHash(key,value) over the data-point labels, the
// resource attributes and the resource labels — all three namespaces folded
// into ONE accumulator with NO domain tag. A pair contributed as a DATA-POINT
// label was indistinguishable from the same pair contributed as a RESOURCE
// attribute (the primary AND check hashes were identical by construction), so
// the second observation merged into the first sample — recorded under the
// wrong identity.
//
// Reachable whenever two rules share a metric name (a documented feature —
// "rules sharing a name share one series") and one lifts a key onto the resource
// that the other keeps on the data point. Regression guard for the fix: the
// resource contributions fold under a separate hash domain
// (combineResHash/combineResCheck).
func TestResourceAndDataPointLabelsHaveSeparateHashDomains(t *testing.T) {
	setTimeForTest(time.Unix(1_701_100_000, 0))
	defer testEpoch.Store(0)

	set, err := NewDynamicMetricSet([]Dynamic{
		{Name: "shared_total", Type: CounterType, Value: "1", Match: []string{"kind=dp"},
			Labels: []string{"tenant=$tenant"}},
		{Name: "shared_total", Type: CounterType, Value: "1", Match: []string{"kind=res"},
			ResourceLabels: []string{"tenant=$tenant"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	pod := res(map[string]string{"k8s.pod.name": "p"})
	set.Add(nil, labelsFrom(map[string]string{"kind": "dp", "tenant": "acme"}), pod, "")
	set.Add(nil, labelsFrom(map[string]string{"kind": "res", "tenant": "acme"}), pod, "")

	// Ground truth: two DISTINCT identities — {tenant="acme"} on the data point
	// (resource: pod only) and tenant="acme" on the RESOURCE (no data-point
	// label). Two data points, each 1.
	exp := &capExporter{}
	if err := set.Export(context.Background(), exp, 0); err != nil {
		t.Fatal(err)
	}
	dpLabel, resLabel := 0.0, 0.0
	for _, md := range exp.md {
		rms := md.ResourceMetrics()
		for i := 0; i < rms.Len(); i++ {
			rm := rms.At(i)
			_, onRes := rm.Resource().Attributes().Get("tenant")
			sms := rm.ScopeMetrics()
			for j := 0; j < sms.Len(); j++ {
				ms := sms.At(j).Metrics()
				for k := 0; k < ms.Len(); k++ {
					dps := ms.At(k).Sum().DataPoints()
					for d := 0; d < dps.Len(); d++ {
						dp := dps.At(d)
						if dp.DoubleValue() == 0 {
							continue // synthetic baseline zeros
						}
						if _, onDP := dp.Attributes().Get("tenant"); onDP {
							dpLabel += dp.DoubleValue()
						}
						if onRes {
							resLabel += dp.DoubleValue()
						}
					}
				}
			}
		}
	}
	if dpLabel != 1 || resLabel != 1 {
		t.Fatalf("data-point-label total = %v (want 1), resource-label total = %v (want 1): "+
			"the resource-label observation collided with the data-point-label series and merged into it", dpLabel, resLabel)
	}
}

// TestResourceValueSwapDoesNotCollide is the same flaw with a single rule: a
// data-point label whose NAME matches a resource attribute's name. Pod A's
// resource says name=A while its line says B, and vice versa — the two sums are
// the same multiset of (key,value) pairs, so both the hash and the check hash
// collide EXACTLY and the two pods' observations merge into one sample.
func TestResourceValueSwapDoesNotCollide(t *testing.T) {
	setTimeForTest(time.Unix(1_701_200_000, 0))
	defer testEpoch.Store(0)

	set, err := NewDynamicMetricSet([]Dynamic{{
		Name: "peer_total", Type: CounterType, Value: "1",
		Labels: []string{"k8s.pod.name=$peer"}, // a line field named like a resource attr
	}})
	if err != nil {
		t.Fatal(err)
	}
	set.Add(nil, labelsFrom(map[string]string{"peer": "b"}), res(map[string]string{"k8s.pod.name": "a"}), "")
	set.Add(nil, labelsFrom(map[string]string{"peer": "a"}), res(map[string]string{"k8s.pod.name": "b"}), "")

	s := set.rules[0].series
	if len(s.db) != 2 {
		t.Fatalf("samples = %d, want 2: (res a, label b) and (res b, label a) hash identically", len(s.db))
	}
}

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

// gaugeValue reads the single gauge data point's value for the given label.
func gaugeValue(t *testing.T, set *DynamicMetricSet, name, labelKey, labelVal string) (float64, bool) {
	t.Helper()
	m := exportOne(t, set, name)
	dps := m.Gauge().DataPoints()
	for i := 0; i < dps.Len(); i++ {
		dp := dps.At(i)
		if labelKey == "" {
			return dp.DoubleValue(), true
		}
		if v, ok := dp.Attributes().Get(labelKey); ok && v.Str() == labelVal {
			return dp.DoubleValue(), true
		}
	}
	return 0, false
}

func TestGaugeActions(t *testing.T) {
	cases := []struct {
		action string
		value  string // "" means none (inc/dec)
		want   float64
	}{
		{"inc", "", 3},        // three matching lines, +1 each
		{"dec", "", -3},       // -1 each
		{"add", "amount", 60}, // 10+20+30
		{"sub", "amount", -60},
		{"set", "amount", 30}, // last value wins
	}
	for _, c := range cases {
		t.Run(c.action, func(t *testing.T) {
			setTimeForTest(time.Unix(1_700_200_000, 0))
			defer testEpoch.Store(0)

			set, err := NewDynamicMetricSet([]Dynamic{{
				Name:   "g",
				Type:   GaugeType,
				Action: c.action,
				Value:  c.value,
				Match:  []string{"m=1"},
			}})
			if err != nil {
				t.Fatal(err)
			}
			for _, a := range []string{"10", "20", "30"} {
				set.Add(valuesFrom(map[string]string{"amount": a}),
					labelsFrom(map[string]string{"m": "1", "amount": a}), noRes(), "")
			}
			got, ok := gaugeValue(t, set, "g", "", "")
			if !ok || got != c.want {
				t.Fatalf("%s gauge = %v (ok=%v), want %v", c.action, got, ok, c.want)
			}
		})
	}
}

func TestGaugeAggregations(t *testing.T) {
	// Values 10, 5, 20 over one window.
	const eps = 1e-4
	cases := []struct {
		action string
		want   float64
	}{
		{"min", 5},
		{"max", 20},
		{"avg", 35.0 / 3.0},   // (10+5+20)/3
		{"first", 10},         // first value
		{"sum", 35},           // 10+5+20
		{"count", 3},          // three matching lines
		{"stddev", 6.2360956}, // population stddev of {10,5,20}
		{"range", 15},         // 20 − 5
		{"delta", 10},         // 20 (last) − 10 (first)
	}
	for _, c := range cases {
		t.Run(c.action, func(t *testing.T) {
			setTimeForTest(time.Unix(1_700_300_000, 0))
			defer testEpoch.Store(0)
			set, err := NewDynamicMetricSet([]Dynamic{{
				Name: "g", Type: GaugeType, Action: c.action, Value: "v", Match: []string{"m=1"},
			}})
			if err != nil {
				t.Fatal(err)
			}
			for _, v := range []string{"10", "5", "20"} {
				set.Add(valuesFrom(map[string]string{"v": v}), labelsFrom(map[string]string{"m": "1"}), noRes(), "")
			}
			got, ok := gaugeValue(t, set, "g", "", "")
			if !ok || got < c.want-eps || got > c.want+eps {
				t.Fatalf("%s = %v (ok=%v), want %v", c.action, got, ok, c.want)
			}
		})
	}
}

func TestGaugeAggregationWindow(t *testing.T) {
	setTimeForTest(time.Unix(1_700_300_100, 0))
	defer testEpoch.Store(0)
	set, err := NewDynamicMetricSet([]Dynamic{{
		Name: "g", Type: GaugeType, Action: "max", Value: "v", Match: []string{"m=1"},
	}})
	if err != nil {
		t.Fatal(err)
	}
	add := func(v string) {
		set.Add(valuesFrom(map[string]string{"v": v}), labelsFrom(map[string]string{"m": "1"}), noRes(), "")
	}

	add("10")
	add("20")
	if got, _ := gaugeValue(t, set, "g", "", ""); got != 20 { // window max = 20 (this export seals)
		t.Fatalf("first window max = %v, want 20", got)
	}
	// No new values: the aggregate keeps being emitted.
	if got, _ := gaugeValue(t, set, "g", "", ""); got != 20 {
		t.Fatalf("kept aggregate = %v, want 20", got)
	}
	// A new value after an export starts a fresh window — the old 20 is gone.
	add("7")
	if got, _ := gaugeValue(t, set, "g", "", ""); got != 7 {
		t.Fatalf("new window max = %v, want 7 (window reset)", got)
	}
	add("9") // folds into the current (post-export) window
	if got, _ := gaugeValue(t, set, "g", "", ""); got != 9 {
		t.Fatalf("window max after fold = %v, want 9", got)
	}
}

// TestStddevLargeMagnitude pins Welford's numerical stability: the naive
// E[x²]−E[x]² form catastrophically cancelled for large values with small
// spread and reported 0.
func TestStddevLargeMagnitude(t *testing.T) {
	s := newSeries(seriesSpec{name: "m", kind: kindGauge, action: actionStddev, log: slog.Default()})
	for _, v := range []float64{1e9, 1e9 + 1, 1e9 + 2} {
		s.observe(labels{{"k", "v"}}, v, resKey{}, emptyResource, nil)
	}
	samps := s.snapshot()
	if len(samps) != 1 {
		t.Fatalf("samples: %d", len(samps))
	}
	got := samps[0].value     // snapshot already aggregated the window
	want := 0.816496580927726 // population stddev of {0,1,2}
	if math.Abs(got-want) > 1e-9 {
		t.Fatalf("stddev = %v, want %v", got, want)
	}
}
