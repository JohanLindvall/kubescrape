// Tests for the series store (series.go): hashing/fold guards, expiry
// and cardinality caps, and gauge actions/windowed aggregations.
package metrics

import (
	"bytes"
	"context"
	"log/slog"
	"math"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/JohanLindvall/haste/xxh3"
	"go.opentelemetry.io/collector/pdata/pcommon"
)

// TestHistogramLeIsRefusedAtCompile pins the config-load half of the "le" rule.
// A histogram keyed on a label set containing "le" would split its distribution
// into one sample per value, each rendering a full bucket set, so the label is
// refused where its name is known — statically, from the spec plus labelPrefix.
func TestHistogramLeIsRefusedAtCompile(t *testing.T) {
	_, err := newTestSet([]Dynamic{{
		Name:    "h",
		Type:    HistogramType,
		Value:   "ms",
		Buckets: []float64{1, 5},
		Labels:  []string{"le=$bucket"},
	}})
	if err == nil {
		t.Fatal("a histogram rule setting le compiled; want a startup error")
	}
	if !strings.Contains(err.Error(), "le") || !strings.Contains(err.Error(), "h") {
		t.Errorf("error must name the label and the metric: %v", err)
	}

	// The prefixed form is the same label and must be refused identically —
	// the name is checked AFTER labelPrefix is applied.
	if _, err := newTestSet([]Dynamic{{
		Name: "h2", Type: HistogramType, Value: "ms", Buckets: []float64{1},
		LabelPrefix: "l", Labels: []string{"e=$bucket"},
	}}); err == nil {
		t.Error("labelPrefix composing to \"le\" compiled; want a startup error")
	}

	// Non-histograms are untouched: there "le" is an ordinary label.
	if _, err := newTestSet([]Dynamic{{
		Name: "c", Type: CounterType, Value: "1", Labels: []string{"le=$bucket"},
	}}); err != nil {
		t.Errorf("le on a counter must stay legal: %v", err)
	}
}

// TestHistogramResourceLabelLeIsRefusedAtCompile pins the OTHER static door.
// resourceLabels folds into the same series identity as labels (observeFold
// XORs its term into the key), so an "le" there splits the distribution
// identically — and, being a resource attribute, it is the one that meets the
// generated bucket label downstream once a collector promotes resource
// attributes onto data points. The guard checked only `labels` and this
// compiled clean.
func TestHistogramResourceLabelLeIsRefusedAtCompile(t *testing.T) {
	_, err := newTestSet([]Dynamic{{
		Name:           "h",
		Type:           HistogramType,
		Value:          "ms",
		Buckets:        []float64{1, 5},
		ResourceLabels: []string{"le=$code"},
	}})
	if err == nil {
		t.Fatal("a histogram rule setting le via resourceLabels compiled; want a startup error")
	}
	// The message must name the list that carried it, or an operator reads a
	// refusal about `labels` while staring at a clean `labels`.
	if !strings.Contains(err.Error(), "resourceLabels") {
		t.Errorf("error must name the resourceLabels list: %v", err)
	}
	if !strings.Contains(err.Error(), "le") || !strings.Contains(err.Error(), "h") {
		t.Errorf("error must name the label and the metric: %v", err)
	}

	// labelPrefix composes into resourceLabels too — same name, same refusal.
	if _, err := newTestSet([]Dynamic{{
		Name: "h2", Type: HistogramType, Value: "ms", Buckets: []float64{1},
		LabelPrefix: "l", ResourceLabels: []string{"e=$code"},
	}}); err == nil {
		t.Error("labelPrefix composing to \"le\" in resourceLabels compiled; want a startup error")
	}

	// And the data-point door still names ITS list, so the two are told apart.
	_, err = newTestSet([]Dynamic{{
		Name: "h3", Type: HistogramType, Value: "ms", Buckets: []float64{1},
		Labels: []string{"le=$code"},
	}})
	if err == nil || !strings.Contains(err.Error(), "labels") || strings.Contains(err.Error(), "resourceLabels") {
		t.Errorf("data-point door must name `labels`: %v", err)
	}

	// Non-histograms are untouched here as well.
	if _, err := newTestSet([]Dynamic{{
		Name: "c", Type: CounterType, Value: "1", ResourceLabels: []string{"le=$code"},
	}}); err != nil {
		t.Errorf("le on a counter's resourceLabels must stay legal: %v", err)
	}
}

// TestHistogramLeIsRefusedByEmitDirect pins the runtime half. A Starlark
// script's label map is the ONE door whose keys are not known at compile time,
// so it carries the only remaining check.
func TestHistogramLeIsRefusedByEmitDirect(t *testing.T) {
	set, err := newTestSet([]Dynamic{
		{Name: "h", Type: HistogramType, Value: "ms", Buckets: []float64{1, 5}},
		{Name: "c", Type: CounterType, Value: "1"},
	})
	if err != nil {
		t.Fatal(err)
	}
	res := pcommon.NewMap()

	err = set.EmitDirect("h", 0.5, map[string]string{"le": "1"}, res)
	if err == nil {
		t.Fatal("emit_metric set le on a histogram; want an error")
	}
	if !strings.Contains(err.Error(), "le") {
		t.Errorf("error must name the label: %v", err)
	}
	// Refused means NOT observed: a rejected emit must not leave a sample.
	for _, r := range set.rules {
		if r.series.name == "h" && len(r.series.db) != 0 {
			t.Errorf("refused emit still admitted %d samples", len(r.series.db))
		}
	}

	// Same label on a non-histogram is fine, and a histogram without it is fine.
	if err := set.EmitDirect("c", 1, map[string]string{"le": "1"}, res); err != nil {
		t.Errorf("le on a counter must stay legal: %v", err)
	}
	if err := set.EmitDirect("h", 0.5, map[string]string{"route": "/x"}, res); err != nil {
		t.Errorf("histogram without le must emit: %v", err)
	}
}

// TestEvictThenReadmitAtCap: at maxSize, a new label set is refused; after the
// old sample expires and is swept, the SAME refused hash must be admitted.
func TestEvictThenReadmitAtCap(t *testing.T) {
	t0 := int64(1_700_500_000)
	setTimeForTest(time.Unix(t0, 0))
	defer testEpoch.Store(0)

	s := newTestSeries(seriesSpec{name: "c", kind: kindCounter, maxSize: 1, expiration: 60 * time.Second})
	s.observe(labels{}.set("u", "a"), 1, xxh3.Uint128{}, emptyResource, nil)

	s.observe(labels{}.set("u", "b"), 1, xxh3.Uint128{}, emptyResource, nil)
	if got := s.cappedDrops.Load(); got != 1 {
		t.Fatalf("cap drops = %d, want 1", got)
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
	s.observe(labels{}.set("u", "b"), 1, xxh3.Uint128{}, emptyResource, nil)
	if len(s.db) != 1 {
		t.Fatalf("re-admission after evict failed: db = %d", len(s.db))
	}
	out := s.snapshot()
	if len(out) != 1 || out[0].value != 1 || !out[0].initial {
		t.Fatalf("re-admitted sample = %+v, want value 1, initial", out)
	}
}

// TestCappedDropsAreReportedPerMetricAndOnlyOnceTheyHappen pins what
// kubescrape_log_metrics_dropped_capped_total{metric} reads: the count per
// metric NAME (two rules sharing a name share one series, so they must be
// counted once, not once per rule), and nothing at all for a metric that has
// dropped nothing — the family is documented as absent until something drops.
// The counts live on the series, so this walks the set rather than a set-wide
// map; the concurrent half is what -race checks for the lock-free refusal path.
func TestCappedDropsAreReportedPerMetricAndOnlyOnceTheyHappen(t *testing.T) {
	setTimeForTest(time.Unix(1_700_520_000, 0))
	defer testEpoch.Store(0)

	set, err := newTestSet([]Dynamic{
		{Name: "a_total", Type: CounterType, Value: "1", MaxCardinality: 1},
		{Name: "a_total", Type: CounterType, Value: "1", MaxCardinality: 1, Match: []string{"source=second"}},
		{Name: "b_total", Type: CounterType, Value: "1", MaxCardinality: 1},
		{Name: "quiet_total", Type: CounterType, Value: "1", MaxCardinality: 1},
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := set.DroppedCappedByMetric(); len(got) != 0 {
		t.Fatalf("before any refusal = %v, want empty (the family is absent until something drops)", got)
	}
	emit := func(name, u string) {
		t.Helper()
		if err := set.EmitDirect(name, 1, map[string]string{"u": u}, noRes()); err != nil {
			t.Fatal(err)
		}
	}
	emit("quiet_total", "0")
	emit("a_total", "0")
	emit("b_total", "0")

	// Concurrent refusals on two series of ONE set: the shape a cardinality
	// blow-up fed from several ingest goroutines takes.
	const workers, each = 4, 50
	var wg sync.WaitGroup
	for w := range workers {
		wg.Go(func() {
			for i := range each {
				name := "a_total"
				if w%2 == 1 {
					name = "b_total"
				}
				_ = set.EmitDirect(name, 1, map[string]string{"u": strconv.Itoa(w*each + i + 1)}, noRes())
			}
		})
	}
	wg.Wait()

	got := set.DroppedCappedByMetric()
	want := map[string]float64{"a_total": workers / 2 * each, "b_total": workers / 2 * each}
	if len(got) != len(want) || got["a_total"] != want["a_total"] || got["b_total"] != want["b_total"] {
		t.Fatalf("DroppedCappedByMetric = %v, want %v (quiet_total absent, a_total counted once despite two rules)", got, want)
	}
}

// TestExpiryEmitsBeforeDiscarding: the snapshot idle branch used to zero a
// non-aggregating sample WITHOUT emitting it — with maxAge shorter than the
// export interval (legal; nothing clamps it), every observation made since
// the last export was silently destroyed: observed at t, exported never.
// Regression guard for the fix: an observation must appear in at least one
// export before the idle branch goes quiet on it (expiringSample.exported) —
// and, the counter being cumulative, the idle branch must not zero it either,
// so a re-observation continues the running total.
func TestExpiryEmitsBeforeDiscarding(t *testing.T) {
	t0 := int64(1_700_510_000)
	setTimeForTest(time.Unix(t0, 0))
	defer testEpoch.Store(0)

	// maxAge 10s, export gap 30s — a legal configuration (maxAge: 10s).
	s := newTestSeries(seriesSpec{name: "c", kind: kindCounter, expiration: 10 * time.Second})
	s.observe(labels{}.set("k", "v"), 1, xxh3.Uint128{}, emptyResource, nil)

	setTimeForTest(time.Unix(t0+30, 0)) // first export 30s later
	first := s.snapshot()
	if len(first) != 1 || first[0].value != 1 {
		t.Fatalf("first export = %+v, want the idle sample of value 1: it was never exported, so the idle branch must emit it", first)
	}
	setTimeForTest(time.Unix(t0+31, 0))
	s.observe(labels{}.set("k", "v"), 1, xxh3.Uint128{}, emptyResource, nil)
	setTimeForTest(time.Unix(t0+32, 0))
	last := s.snapshot()
	// Two increments happened; the cumulative counter's last export must carry
	// both.
	if len(last) != 1 || last[0].value != 2 {
		t.Fatalf("last export = %+v, want the cumulative 2: the idle branch zeroed the counter", last)
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
		{actionSum, 35, 6},
		{actionCount, 3, 2},
	}
	for _, c := range actions {
		t0 := int64(1_700_600_000)
		setTimeForTest(time.Unix(t0, 0))
		s := newTestSeries(seriesSpec{name: "g", kind: kindGauge, action: c.action, expiration: time.Hour})
		lbls := labels{}.set("m", "1")
		for _, v := range []float64{10, 5, 20} {
			s.observe(lbls, v, xxh3.Uint128{}, emptyResource, nil)
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
			s.observe(lbls, v, xxh3.Uint128{}, emptyResource, nil)
		}
		out = s.snapshot()
		if len(out) != 1 || math.Abs(out[0].value-c.w2) > eps {
			t.Errorf("action %d export3 = %+v, want %v (fresh window)", c.action, out, c.w2)
		}
	}
	testEpoch.Store(0)
}

// TestUnknownAggregationActionsRejected: the action set is deliberately
// closed (derivable aggregations belong in backend recording rules); an
// unknown action must fail startup, never silently mean something else.
func TestUnknownAggregationActionsRejected(t *testing.T) {
	for _, action := range []string{"stddev", "range", "p99"} {
		_, err := newTestSet([]Dynamic{{
			Name: "g", Type: GaugeType, Action: action, Value: "v", Match: []string{"m=1"},
		}})
		if err == nil || !strings.Contains(err.Error(), "invalid gauge action") {
			t.Fatalf("action %q: err = %v, want an invalid-action error", action, err)
		}
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

	set, err := newTestSet([]Dynamic{
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

	set, err := newTestSet([]Dynamic{{
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
	s := newTestSeries(seriesSpec{name: "c", kind: kindCounter, expiration: 30 * time.Second})
	s.observe(labels{}.set("k", "v"), 7, xxh3.Uint128{}, emptyResource, nil)

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
	s := newTestSeries(seriesSpec{name: "g", kind: kindGauge, action: actionAvg, expiration: 30 * time.Second})
	s.observe(labels{}.set("k", "v"), 5, xxh3.Uint128{}, emptyResource, nil)

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

// TestAggregatingGaugeNotReEmittedAtDelete: once the aggregating branch has
// snapshotted (and thus exported) a window, the later 4-minute grace-DELETE
// must NOT emit that same window a second time — the DELETE guard is `!exported`
// and the aggregating branch now marks the window exported. Without that mark
// the window is double-counted (emitted at the snapshot AND again at delete).
func TestAggregatingGaugeNotReEmittedAtDelete(t *testing.T) {
	t0 := int64(1_700_900_000)
	setTimeForTest(time.Unix(t0, 0))
	defer testEpoch.Store(0)

	s := newTestSeries(seriesSpec{name: "g", kind: kindGauge, action: actionAvg, expiration: 30 * time.Second})
	s.observe(labels{}.set("k", "v"), 5, xxh3.Uint128{}, emptyResource, nil)

	// First snapshot within the grace window: the aggregating branch emits the
	// window (avg 5) and marks it exported.
	setTimeForTest(time.Unix(t0+60, 0)) // idle = 60-30 = 30 < 240
	if got := s.snapshot(); len(got) != 1 || got[0].value != 5 {
		t.Fatalf("first snapshot = %+v, want one sample value 5", got)
	}

	// Second snapshot past maxAge+grace with no new observation: the DELETE
	// branch fires and must emit nothing — the window was already exported.
	setTimeForTest(time.Unix(t0+400, 0)) // idle = 400-30 = 370 >= 240
	if got := s.snapshot(); len(got) != 0 {
		t.Fatalf("delete-time snapshot = %+v, want 0 samples: an already-exported "+
			"aggregating window was re-emitted at DELETE (double count)", got)
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

	set, err := newTestSet([]Dynamic{{
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
		t.Fatal(err) // idle emit: the point must still carry all buckets
	}

	m, ok := idle.find("lat_seconds")
	if !ok {
		t.Fatal("histogram not exported by the idle branch")
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

// TestHistogramIdleEmitKeepsAllBuckets: with the export interval longer than
// maxAge, the idle snapshot's emit must carry the COMPLETE cumulative
// distribution — an observation that lands only in the upper buckets must not
// narrow what the point reports (with the old one-sample-per-bucket layout
// that was a real bug; with counts on one sample it is structural, and this
// pins the emitted copy). The live distribution is NOT reset by going idle (a
// histogram is cumulative), and the emitted copy must not move with it.
func TestHistogramIdleEmitKeepsAllBuckets(t *testing.T) {
	t0 := int64(1_700_600_000)
	setTimeForTest(time.Unix(t0, 0))
	defer testEpoch.Store(0)

	// maxAge 30s, export gap 60s — a legal configuration.
	s := newTestSeries(seriesSpec{name: "h", kind: kindHistogram,
		buckets: []float64{1, 5, 7.5, 10}, expiration: 30 * time.Second})
	lbls := labels{}.set("route", "/x")

	s.observe(lbls, 5, xxh3.Uint128{}, emptyResource, nil) // lands in le>=5
	if len(s.snapshot()) == 0 {                            // full export: marks the sample exported
		t.Fatal("first snapshot emitted nothing")
	}

	setTimeForTest(time.Unix(t0+15, 0))
	s.observe(lbls, 8, xxh3.Uint128{}, emptyResource, nil) // lands only in le>=10

	setTimeForTest(time.Unix(t0+75, 0)) // past maxAge -> idle branch (no reset: cumulative)
	out := s.snapshot()
	if len(out) != 1 {
		t.Fatalf("idle snapshot emitted %d samples, want 1", len(out))
	}
	sm := out[0]
	// Cumulative counts over bounds {1,5,7.5,10} must reflect BOTH
	// observations: one <=5, both <=10; +Inf (the total) is sm.count.
	want := []uint64{0, 1, 1, 2}
	for i, c := range sm.counts {
		if c != want[i] {
			t.Fatalf("counts = %v, want %v: distribution corrupted", sm.counts, want)
		}
	}
	if sm.count != 2 {
		t.Fatalf("+Inf count = %d, want 2", sm.count)
	}

	// The live distribution kept counting — and an observation after the
	// snapshot must not reach the emitted copy (counts is cloned: export
	// retention and the render both read it after the lock is released).
	s.observe(lbls, 0.5, xxh3.Uint128{}, emptyResource, nil)
	for i, c := range sm.counts {
		if c != want[i] {
			t.Fatalf("emitted counts = %v after a later observation, want %v: the emitted copy aliases the live array", sm.counts, want)
		}
	}
	for samp := range s.all() {
		if samp.count != 3 {
			t.Fatalf("live count = %d, want 3: going idle must not reset a cumulative histogram", samp.count)
		}
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

			set, err := newTestSet([]Dynamic{{
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
		{"avg", 35.0 / 3.0}, // (10+5+20)/3
		{"sum", 35},         // 10+5+20
		{"count", 3},        // three matching lines
	}
	for _, c := range cases {
		t.Run(c.action, func(t *testing.T) {
			setTimeForTest(time.Unix(1_700_300_000, 0))
			defer testEpoch.Store(0)
			set, err := newTestSet([]Dynamic{{
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
	set, err := newTestSet([]Dynamic{{
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

// TestCardinalityWarningNamesTheResourceThatWasRefused pins the cap's warn
// line. maxCardinality bounds SERIES — (resource, label-combination) pairs —
// so one agent-wide set shared by every pod on the node exhausts its budget
// across pods, not across label sets. The line used to name only the label set
// and the cap ("labels={path=\"/a\"} maxsize=10000"), which reads as one label
// set against ten thousand: self-contradictory on its face, and no help at all
// in finding WHICH pod stopped being measured. Naming the resource is what
// makes the sizing surprise falsifiable from the logs.
func TestCardinalityWarningNamesTheResourceThatWasRefused(t *testing.T) {
	t0 := int64(1_700_600_000)
	setTimeForTest(time.Unix(t0, 0))
	defer testEpoch.Store(0)

	var buf bytes.Buffer
	log := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo}))
	s := newTestSeries(seriesSpec{name: "c", kind: kindCounter, maxSize: 1,
		expiration: time.Hour, log: log})

	res := func(pod string) pcommon.Map {
		m := pcommon.NewMap()
		m.PutStr("k8s.namespace.name", "prod")
		m.PutStr("k8s.pod.name", pod)
		return m
	}
	// ONE label combination, two pods. The cap binds on the second pod, which
	// is the whole point: the label set is not what exhausted it.
	lbls := labels{}.set("path", "/a")
	for _, pod := range []string{"web-0", "web-1"} {
		r := res(pod)
		s.observe(lbls, 1, resourceAccum(r), r, nil)
	}
	if len(s.db) != 1 || s.cappedDrops.Load() != 1 {
		t.Fatalf("want one admitted series and one capped drop; db=%d capped=%d", len(s.db), s.cappedDrops.Load())
	}

	line := buf.String()
	if !strings.Contains(line, "web-1") {
		t.Errorf("the cardinality warning must name the refused resource, or the operator cannot tell which pod lost the metric: %q", line)
	}
	if !strings.Contains(line, `labels="{path=\"/a\"}"`) && !strings.Contains(line, "path") {
		t.Errorf("the warning must still name the label set: %q", line)
	}
	if !strings.Contains(line, "maxSeries=1") {
		t.Errorf("the warning must still quote the cap: %q", line)
	}
}
