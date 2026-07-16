package metrics

// Audit tests for the metrics engine (the 2026-07 correctness sweep). Each of
// these pins a bug the sweep found and fixed: the shared resource/label hash
// domain, the idle reset discarding never-exported counts, and valueRegexp
// being ignored by the counting gauge actions.

import (
	"context"
	"math"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"

	"github.com/cespare/xxhash/v2"
	"go.opentelemetry.io/collector/pdata/pcommon"
)

// --- Angle 1: hash folding & collision handling -----------------------------

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

// TestEmptyResourceExport: lines with an EMPTY resource map export under their
// own ResourceMetrics with zero attributes, distinct from a populated resource.
func TestEmptyResourceExport(t *testing.T) {
	setTimeForTest(time.Unix(1_700_700_000, 0))
	defer testEpoch.Store(0)

	set, err := NewDynamicMetricSet([]Dynamic{{
		Name: "lines_total", Type: CounterType, Value: "1", Match: []string{"m=1"},
	}})
	if err != nil {
		t.Fatal(err)
	}
	set.Add(nil, labelsFrom(map[string]string{"m": "1"}), noRes(), "")
	set.Add(nil, labelsFrom(map[string]string{"m": "1"}), res(map[string]string{"k8s.pod.name": "p"}), "")

	exp := &capExporter{}
	if err := set.Export(context.Background(), exp, 0); err != nil {
		t.Fatal(err)
	}
	emptyRMs, podRMs, totalRMs := 0, 0, 0
	for _, md := range exp.md {
		rms := md.ResourceMetrics()
		for i := 0; i < rms.Len(); i++ {
			totalRMs++
			switch rms.At(i).Resource().Attributes().Len() {
			case 0:
				emptyRMs++
			default:
				podRMs++
			}
		}
	}
	if totalRMs != 2 || emptyRMs != 1 || podRMs != 1 {
		t.Fatalf("ResourceMetrics: total %d empty %d pod %d, want 2/1/1", totalRMs, emptyRMs, podRMs)
	}
}

// --- Angle 5: valueRegexp ----------------------------------------------------

func TestValueRegexpWholeMatchAndBadCapture(t *testing.T) {
	setTimeForTest(time.Unix(1_700_800_000, 0))
	defer testEpoch.Store(0)

	// No capture group: the whole match is the value.
	set, err := NewDynamicMetricSet([]Dynamic{
		{Name: "whole_total", Type: CounterType, ValueRegexp: `[0-9]+\.[0-9]+`},
		{Name: "bad_total", Type: CounterType, ValueRegexp: `id=([a-z]+)`},
	})
	if err != nil {
		t.Fatal(err)
	}
	beforeNaN := DroppedNaN()
	set.Add(nil, nil, noRes(), "latency 12.5 seconds")
	set.Add(nil, nil, noRes(), "id=abc done") // matches, capture non-numeric -> skipped

	m := exportOne(t, set, "whole_total")
	var total float64
	dps := m.Sum().DataPoints()
	for i := 0; i < dps.Len(); i++ {
		total += dps.At(i).DoubleValue()
	}
	if total != 12.5 {
		t.Errorf("whole-match total = %v, want 12.5", total)
	}
	exp := &capExporter{}
	if err := set.Export(context.Background(), exp, 0); err != nil {
		t.Fatal(err)
	}
	if _, ok := exp.find("bad_total"); ok {
		t.Error("non-numeric capture produced a series; the line must be skipped")
	}
	if got := DroppedNaN() - beforeNaN; got != 0 {
		t.Errorf("droppedNaN delta = %d, want 0 (skip, not NaN admission)", got)
	}
}

// TestLineKeyMultilineBody: __line__ selectors/labels and valueRegexp against a
// body with embedded newlines; a newline-carrying label value must round-trip
// the serialized label form into the exported attribute intact.
func TestLineKeyMultilineBody(t *testing.T) {
	setTimeForTest(time.Unix(1_700_800_100, 0))
	defer testEpoch.Store(0)

	set, err := NewDynamicMetricSet([]Dynamic{{
		Name:        "panic_lines",
		Type:        GaugeType,
		Action:      "count",
		Match:       nil,
		MatchRegexp: []string{`__line__=(?s)panic:.*goroutine`},
		Labels:      []string{"head=$__line__(______)"},
	}})
	if err != nil {
		t.Fatal(err)
	}
	body := "panic: boom\ngoroutine 1 [running]:\nmain.main()"
	set.Add(nil, nil, noRes(), body)

	m := exportOne(t, set, "panic_lines")
	dps := m.Gauge().DataPoints()
	if dps.Len() != 1 {
		t.Fatalf("data points = %d", dps.Len())
	}
	if v := dps.At(0).DoubleValue(); v != 1 {
		t.Fatalf("count = %v, want 1", v)
	}
	if head, ok := dps.At(0).Attributes().Get("head"); !ok || head.Str() != "panic:" {
		t.Fatalf("head = %q, want %q", head.Str(), "panic:")
	}

	// And a raw newline INSIDE a label value survives serialize/parse/export.
	set2, err := NewDynamicMetricSet([]Dynamic{{
		Name: "nl_total", Type: CounterType, Value: "1", Labels: []string{"tail=$t"},
	}})
	if err != nil {
		t.Fatal(err)
	}
	set2.Add(nil, labelsFrom(map[string]string{"t": "a\nb\"c\\d"}), noRes(), "")
	m2 := exportOne(t, set2, "nl_total")
	found := false
	dps2 := m2.Sum().DataPoints()
	for i := 0; i < dps2.Len(); i++ {
		if v, ok := dps2.At(i).Attributes().Get("tail"); ok {
			found = true
			if v.Str() != "a\nb\"c\\d" {
				t.Fatalf("tail = %q, want %q", v.Str(), "a\nb\"c\\d")
			}
		}
	}
	if !found {
		t.Fatal("tail label missing")
	}
}

// --- Angle 6: unsafe line-field views must not be retained -------------------

// TestNoAliasRetentionAfterLineBufferReuse: linefields hands out strings that
// may alias the log line (lightning's UnescapeString aliases its input when the
// string has no escapes). If any such string were RETAINED by a series, a
// caller reusing its line buffer would corrupt exported label values. Series
// storage must copy (labels.String / resourceString) before the line dies.
func TestNoAliasRetentionAfterLineBufferReuse(t *testing.T) {
	setTimeForTest(time.Unix(1_700_900_000, 0))
	defer testEpoch.Store(0)

	set, err := NewDynamicMetricSet([]Dynamic{{
		Name:           "aliased_total",
		Type:           CounterType,
		Value:          "1",
		Match:          []string{"level=info"},
		Labels:         []string{"user=$user"},
		ResourceLabels: []string{"tenant=$tenant"},
	}})
	if err != nil {
		t.Fatal(err)
	}

	buf := []byte(`{"level":"info","user":"alice","tenant":"acme"}`)
	line := unsafe.String(&buf[0], len(buf)) // simulates a reused read buffer
	set.Add(nil, nil, res(map[string]string{"k8s.pod.name": "p"}), line)

	for i := range buf { // the "buffer" is reused for the next read
		buf[i] = 'X'
	}

	exp := &capExporter{}
	if err := set.Export(context.Background(), exp, 0); err != nil {
		t.Fatal(err)
	}
	m, ok := exp.find("aliased_total")
	if !ok {
		t.Fatal("metric missing")
	}
	dps := m.Sum().DataPoints()
	userOK := false
	for i := 0; i < dps.Len(); i++ {
		if v, ok := dps.At(i).Attributes().Get("user"); ok {
			if v.Str() != "alice" {
				t.Fatalf("user label = %q, want alice: series retained an aliased line view", v.Str())
			}
			userOK = true
		}
	}
	if !userOK {
		t.Fatal("user label missing")
	}
	for _, md := range exp.md {
		rms := md.ResourceMetrics()
		for i := 0; i < rms.Len(); i++ {
			if v, ok := rms.At(i).Resource().Attributes().Get("tenant"); ok && v.Str() != "acme" {
				t.Fatalf("tenant resource label = %q, want acme: aliased retention", v.Str())
			}
		}
	}
}

// --- Angle 7: registry concurrency -------------------------------------------

// TestVecConcurrentFirstUse hammers the wrapper cache's first use of ONE tuple
// from many goroutines plus concurrent Value() reads (run under -race).
func TestVecConcurrentFirstUse(t *testing.T) {
	r := NewRegistry()
	cv := r.CounterVec("audit_conc_total", "d", "k")

	const goroutines, n = 8, 200
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < n; i++ {
				cv.WithLabelValues("same").Inc()
				_ = cv.WithLabelValues("same").Value()
			}
		}()
	}
	wg.Wait()
	if got := cv.WithLabelValues("same").Value(); got != goroutines*n {
		t.Fatalf("Value = %v, want %d", got, goroutines*n)
	}
}

// TestRegistryConcurrentExportAndObserve runs two exporters against the same
// registry while counters/gauges/histograms are hammered (run under -race):
// GaugeFunc evaluation happens inside Export, concurrently with observes.
func TestRegistryConcurrentExportAndObserve(t *testing.T) {
	r := NewRegistry()
	c := r.CounterVec("audit_race_total", "d", "k")
	g := r.Gauge("audit_race_gauge", "d")
	h := r.HistogramVec("audit_race_hist", "d", []float64{1, 5}, "k")
	var n atomic.Int64
	r.GaugeFunc("audit_race_func", "d", func() float64 { return float64(n.Load()) })

	resAttrs := pcommon.NewResource()
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				c.WithLabelValues("a").Inc()
				g.Set(float64(i))
				h.WithLabelValues("a").Observe(2)
				n.Add(1)
			}
		}(i)
	}
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				if err := r.Export(context.Background(), &capExporter{}, resAttrs); err != nil {
					t.Error(err)
				}
			}
		}()
	}
	time.Sleep(50 * time.Millisecond)
	close(stop)
	wg.Wait()
	if c.WithLabelValues("a").Value() == 0 {
		t.Fatal("no counter value")
	}
}

// --- Angle 8: value extraction vs the counting gauge actions ------------------
//
// TestValueRegexpFiltersCountingActions: valueRegexp is documented as
// "a line that does not match is skipped" — it is both an extractor AND a
// filter. needsValue() returns false for gauge inc/dec/count, so observe used
// to skip readValue entirely and the regexp was NEVER evaluated: every line
// passing the selectors was counted, including lines the valueRegexp rejects.
// Regression guard for the fix: valueRe is evaluated as a filter even for
// counting actions.
func TestValueRegexpFiltersCountingActions(t *testing.T) {
	setTimeForTest(time.Unix(1_701_000_000, 0))
	defer testEpoch.Store(0)

	set, err := NewDynamicMetricSet([]Dynamic{
		{Name: "errs_inc", Type: GaugeType, Action: "inc", ValueRegexp: `code=(\d+)`},
		{Name: "errs_count", Type: GaugeType, Action: "count", ValueRegexp: `code=(\d+)`},
	})
	if err != nil {
		t.Fatal(err)
	}
	set.Add(nil, nil, noRes(), "all good, nothing to see")     // no code= — must be skipped
	set.Add(nil, nil, noRes(), "request failed code=500 oops") // matches

	for _, name := range []string{"errs_inc", "errs_count"} {
		m := exportOne(t, set, name)
		dps := m.Gauge().DataPoints()
		if dps.Len() != 1 {
			t.Fatalf("%s: data points = %d, want 1", name, dps.Len())
		}
		if v := dps.At(0).DoubleValue(); v != 1 {
			t.Errorf("%s = %v, want 1: the line not matching valueRegexp must be skipped, but the counting actions never evaluate it", name, v)
		}
	}
}

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

// TestGaugeFuncReentrant: a GaugeFunc that itself drives registry metrics (and
// registers nothing new) must not deadlock Export; its value is exported.
func TestGaugeFuncReentrant(t *testing.T) {
	r := NewRegistry()
	c := r.Counter("audit_reentrant_total", "d")
	r.GaugeFunc("audit_reentrant_gauge", "d", func() float64 {
		c.Inc() // re-enters the series lock of ANOTHER series during Export
		return 42
	})

	resAttrs := pcommon.NewResource()
	exp := &capExporter{}
	done := make(chan error, 1)
	go func() { done <- r.Export(context.Background(), exp, resAttrs) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Export deadlocked with a re-entrant GaugeFunc")
	}
	m, ok := exp.find("audit_reentrant_gauge")
	if !ok || m.Gauge().DataPoints().At(0).DoubleValue() != 42 {
		t.Fatal("re-entrant gauge func value missing/wrong")
	}
	if c.Value() != 1 {
		t.Fatalf("counter bumped from GaugeFunc = %v, want 1", c.Value())
	}
}
