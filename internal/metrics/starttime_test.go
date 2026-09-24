package metrics

// The start timestamp of a cumulative point, and the instrumentation scope it
// ships under. Both are wire contracts that nothing in this package used to
// assert, and both were wrong in a way no test could see: the values were
// right, so every test that checked values passed.

import (
	"context"
	"testing"
	"time"

	"github.com/JohanLindvall/haste/xxh3"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

// forEachNumberPoint visits every number data point of every exported payload.
func forEachNumberPoint(exp *capExporter, fn func(m pmetric.Metric, dp pmetric.NumberDataPoint)) {
	for _, md := range exp.md {
		rms := md.ResourceMetrics()
		for i := 0; i < rms.Len(); i++ {
			sms := rms.At(i).ScopeMetrics()
			for j := 0; j < sms.Len(); j++ {
				ms := sms.At(j).Metrics()
				for k := 0; k < ms.Len(); k++ {
					m := ms.At(k)
					var dps pmetric.NumberDataPointSlice
					switch m.Type() {
					case pmetric.MetricTypeSum:
						dps = m.Sum().DataPoints()
					case pmetric.MetricTypeGauge:
						dps = m.Gauge().DataPoints()
					default:
						continue
					}
					for p := 0; p < dps.Len(); p++ {
						fn(m, dps.At(p))
					}
				}
			}
		}
	}
}

// TestCumulativeStartIsNotTheExportTime is the load-bearing assertion for the
// whole fix: StartTimeUnixNano == TimeUnixNano is the OTLP encoding for a point
// that RESET at that instant, and snapshot() does not reset counters — the
// value is cumulative since the sample was admitted. Stamping the export time
// on both made every log-derived counter (and every one of the ~92
// kubescrape_* self-metrics, which render through the same code) declare
// itself a reset on every single push.
//
// What that costs downstream: a cumulative-to-delta consumer (Datadog,
// Dynatrace, AWS EMF) reads each point as a fresh series and reports the whole
// running total as that interval's delta — an unbounded over-count that grows
// with uptime — and Google Cloud Monitoring rejects a cumulative point whose
// start is not strictly before its end outright.
func TestCumulativeStartIsNotTheExportTime(t *testing.T) {
	setTimeForTest(time.Unix(1_700_700_000, 0))
	defer testEpoch.Store(0)

	set, err := newTestSet([]Dynamic{{
		Name: "lines_total", Type: CounterType, Value: "1", Match: []string{"m=1"},
	}})
	if err != nil {
		t.Fatal(err)
	}
	set.Add(nil, labelsFrom(map[string]string{"m": "1"}), noRes(), "")

	exp := &capExporter{}
	if err := set.Export(context.Background(), exp, 0); err != nil {
		t.Fatal(err)
	}

	points := 0
	forEachNumberPoint(exp, func(m pmetric.Metric, dp pmetric.NumberDataPoint) {
		points++
		if dp.StartTimestamp() == 0 {
			t.Errorf("%s: no start timestamp at all", m.Name())
			return
		}
		if dp.StartTimestamp() >= dp.Timestamp() {
			t.Errorf("%s: start %d is not before end %d — that is the OTLP encoding for a reset, "+
				"and this counter has not reset", m.Name(), dp.StartTimestamp(), dp.Timestamp())
		}
	})
	if points == 0 {
		t.Fatal("no data points exported")
	}
}

// The stream's start must be STABLE across exports: it identifies one
// accumulation run, so a start that walks forward with each push is the same
// "this is a new series" claim spelled a slower way.
func TestCumulativeStartIsStableAcrossExports(t *testing.T) {
	setTimeForTest(time.Unix(1_700_700_000, 0))
	defer testEpoch.Store(0)

	set, err := newTestSet([]Dynamic{{
		Name: "lines_total", Type: CounterType, Value: "1", Match: []string{"m=1"},
	}})
	if err != nil {
		t.Fatal(err)
	}
	set.Add(nil, labelsFrom(map[string]string{"m": "1"}), noRes(), "")

	starts := map[pcommon.Timestamp]bool{}
	for round := range 3 {
		// A later export at a later wall clock, with another observation in
		// between — nothing here is a reset.
		setTimeForTest(time.Unix(1_700_700_000+int64(round)*60, 0))
		set.Add(nil, labelsFrom(map[string]string{"m": "1"}), noRes(), "")
		exp := &capExporter{}
		if err := set.Export(context.Background(), exp, 0); err != nil {
			t.Fatal(err)
		}
		forEachNumberPoint(exp, func(m pmetric.Metric, dp pmetric.NumberDataPoint) {
			if m.Type() == pmetric.MetricTypeSum {
				starts[dp.StartTimestamp()] = true
			}
		})
	}
	if len(starts) != 1 {
		t.Fatalf("counter reported %d distinct start timestamps across three exports, want 1: %v", len(starts), starts)
	}
}

// An idle CUMULATIVE series is not reset: it stops being exported past maxAge
// and keeps its value, its count and its start through the grace window, so a
// re-appearance inside it continues the SAME stream. It used to be zeroed with
// the start moved — and the zero never sent — so the wire went from the old
// total straight to the new small one: a reset to a start-aware consumer, a
// counter running backwards to the Prometheus-lineage backends that discard
// StartTimeUnixNano, and increase()/rate() undercounted by the whole old
// total. The moved start was also BACKDATED (streamStart's three minutes, which
// exist only for the baseline zeros a reset stream never renders), so it landed
// before the old stream's last point — OTLP's overlap signature.
func TestIdleCumulativeSeriesContinuesItsStreamThroughTheGraceWindow(t *testing.T) {
	base := int64(1_700_700_000)
	setTimeForTest(time.Unix(base, 0))
	defer testEpoch.Store(0)

	set, err := newTestSet([]Dynamic{{
		Name: "lines_total", Type: CounterType, Value: "1", Match: []string{"m=1"},
		MaxAge: "10s",
	}})
	if err != nil {
		t.Fatal(err)
	}
	set.Add(nil, labelsFrom(map[string]string{"m": "1"}), noRes(), "")

	first := &capExporter{}
	if err := set.Export(context.Background(), first, 0); err != nil {
		t.Fatal(err)
	}
	var firstStart pcommon.Timestamp
	forEachNumberPoint(first, func(m pmetric.Metric, dp pmetric.NumberDataPoint) {
		if m.Type() == pmetric.MetricTypeSum && dp.DoubleValue() > 0 {
			firstStart = dp.StartTimestamp()
		}
	})
	if firstStart == 0 {
		t.Fatal("no counter point in the first export")
	}

	// Idle past MaxAge, inside the grace window: nothing to send. Then observe
	// again, which must CONTINUE the stream.
	setTimeForTest(time.Unix(base+60, 0))
	idle := &capExporter{}
	if err := set.Export(context.Background(), idle, 0); err != nil {
		t.Fatal(err)
	}
	forEachNumberPoint(idle, func(m pmetric.Metric, dp pmetric.NumberDataPoint) {
		t.Errorf("an idle, already-exported counter was exported again: %v", dp.DoubleValue())
	})
	set.Add(nil, labelsFrom(map[string]string{"m": "1"}), noRes(), "")

	after := &capExporter{}
	if err := set.Export(context.Background(), after, 0); err != nil {
		t.Fatal(err)
	}
	points := 0
	forEachNumberPoint(after, func(m pmetric.Metric, dp pmetric.NumberDataPoint) {
		if m.Type() != pmetric.MetricTypeSum {
			return
		}
		points++
		if dp.DoubleValue() != 2 {
			t.Errorf("value after the grace window = %v, want the cumulative 2 — the idle branch zeroed the counter", dp.DoubleValue())
		}
		if dp.StartTimestamp() != firstStart {
			t.Errorf("start after the grace window = %d, want the unchanged %d — the stream never reset", dp.StartTimestamp(), firstStart)
		}
	})
	if points != 1 {
		t.Fatalf("points after re-observation = %d, want exactly 1 (no fresh baseline zeros: nothing reset)", points)
	}
}

// A GAUGE is still zeroed when it idles, and that is the one reset left — so
// its new start is NOW, never backdated: streamStart's backdate exists for a
// counter's baseline zeros, which a gauge never renders.
func TestIdleGaugeResetStartsItsNewStreamNow(t *testing.T) {
	t0 := int64(1_700_700_000)
	setTimeForTest(time.Unix(t0, 0))
	defer testEpoch.Store(0)

	s := newTestSeries(seriesSpec{name: "g", kind: kindGauge, action: actionInc, expiration: 10 * time.Second})
	s.observe(labels{}.set("k", "v"), 1, xxh3.Uint128{}, emptyResource, nil)
	if out := s.snapshot(); len(out) != 1 || out[0].value != 1 {
		t.Fatalf("first snapshot = %+v, want one sample of value 1", out)
	}

	setTimeForTest(time.Unix(t0+60, 0)) // idle past maxAge: the reset
	s.snapshot()
	for samp := range s.all() {
		if samp.value != 0 {
			t.Errorf("idle gauge kept value %v, want the reset to 0", samp.value)
		}
		if samp.start != t0+60 {
			t.Errorf("reset gauge's start = %d, want the reset instant %d", samp.start, t0+60)
		}
	}
}

// The two synthetic baseline zeros a counter's first export emits are kept
// (they give rate() a real second sample, which a start timestamp cannot be),
// but they must carry the STREAM's start, not their own timestamp. Stamping
// their own put the reset encoding back on exactly the points that exist to be
// a baseline, so a delta consumer discarded the baseline and Google Cloud
// rejected it.
func TestSyntheticBaselineZerosCarryTheStreamStart(t *testing.T) {
	setTimeForTest(time.Unix(1_700_700_000, 0))
	defer testEpoch.Store(0)

	set, err := newTestSet([]Dynamic{{
		Name: "lines_total", Type: CounterType, Value: "1", Match: []string{"m=1"},
	}})
	if err != nil {
		t.Fatal(err)
	}
	set.Add(nil, labelsFrom(map[string]string{"m": "1"}), noRes(), "")

	exp := &capExporter{}
	if err := set.Export(context.Background(), exp, 0); err != nil {
		t.Fatal(err)
	}

	zeros, starts := 0, map[pcommon.Timestamp]bool{}
	forEachNumberPoint(exp, func(m pmetric.Metric, dp pmetric.NumberDataPoint) {
		if m.Type() != pmetric.MetricTypeSum {
			return
		}
		starts[dp.StartTimestamp()] = true
		if dp.DoubleValue() == 0 {
			zeros++
		}
		if dp.StartTimestamp() >= dp.Timestamp() {
			t.Errorf("baseline point at %d has start %d — not strictly before its own timestamp",
				dp.Timestamp(), dp.StartTimestamp())
		}
	})
	if zeros != 2 {
		t.Fatalf("synthetic baseline zeros = %d, want 2 (they are what gives rate() a second sample)", zeros)
	}
	if len(starts) != 1 {
		t.Fatalf("the baseline zeros and the real point disagree on the stream start: %v", starts)
	}
}

// A counter's backdated start and its synthetic baseline zeros claim the
// counter was zero for minutes before its first observation — which is only
// true of time THIS process was there to see. The series identity survives a
// restart (a self-metric's instance is the node, a log-derived counter is keyed
// by the pod it describes) and a rolling update replaces a pod in well under a
// minute, so a new process's zeros, and the start they carry, used to land
// BEFORE the old process's final samples of the same series: refused by a
// backend with no out-of-order window, a fake reset that double counts
// increase() by one with it. Nothing may be stamped before the series existed.
func TestCounterBaselineNeverPrecedesTheSeriesCreation(t *testing.T) {
	// The series clock starts 30s before the wall clock the export reads: a
	// process that started, observed, and exported half a minute later.
	born := time.Now().Add(-30 * time.Second).Unix()
	setTimeForTest(time.Unix(born, 0))
	defer testEpoch.Store(0)
	floor := pcommon.Timestamp(time.Unix(born, 0).UnixNano())

	check := func(t *testing.T, exp *capExporter) {
		t.Helper()
		zeros, real := 0, 0
		forEachNumberPoint(exp, func(m pmetric.Metric, dp pmetric.NumberDataPoint) {
			if dp.StartTimestamp() < floor || dp.Timestamp() < floor {
				t.Errorf("%s: point at %d with start %d is stamped before the series existed (%d) — "+
					"it lands among the previous process's samples of the same series",
					m.Name(), dp.Timestamp(), dp.StartTimestamp(), floor)
			}
			if dp.StartTimestamp() >= dp.Timestamp() {
				t.Errorf("%s: start %d is not before the point at %d", m.Name(), dp.StartTimestamp(), dp.Timestamp())
			}
			if dp.DoubleValue() == 0 {
				zeros++
			} else {
				real++
			}
		})
		if real != 1 || zeros == 0 {
			t.Fatalf("points: %d real, %d baseline zeros — want the real point AND a baseline zero after the floor", real, zeros)
		}
	}

	t.Run("log-derived", func(t *testing.T) {
		set, err := newTestSet([]Dynamic{{Name: "lines_total", Type: CounterType, Value: "1", Match: []string{"m=1"}}})
		if err != nil {
			t.Fatal(err)
		}
		set.Add(nil, labelsFrom(map[string]string{"m": "1"}), noRes(), "")
		exp := &capExporter{}
		if err := set.Export(context.Background(), exp, 0); err != nil {
			t.Fatal(err)
		}
		check(t, exp)
	})

	t.Run("self-metric", func(t *testing.T) {
		r := NewRegistry()
		c := r.Counter("kubescrape_test_restart_total", "help")
		// A Registry series takes the coarse clock; pin it (and the
		// construction instant it floors against) to the injected one.
		for _, s := range r.series {
			s.now, s.created = testNow, born
		}
		c.Inc()
		exp := &capExporter{}
		if err := r.Export(context.Background(), exp, pcommon.NewResource()); err != nil {
			t.Fatal(err)
		}
		check(t, exp)
	})
}

// The floor binds only in a series' first minutes: a counter first observed
// long after its series was built keeps the full three-minute backdate and
// BOTH baseline zeros, exactly as before.
func TestCounterBaselineKeepsBothZerosAwayFromTheFloor(t *testing.T) {
	now := time.Now()
	setTimeForTest(now.Add(-time.Hour))
	defer testEpoch.Store(0)

	set, err := newTestSet([]Dynamic{{Name: "lines_total", Type: CounterType, Value: "1", Match: []string{"m=1"}}})
	if err != nil {
		t.Fatal(err)
	}
	setTimeForTest(now.Add(-30 * time.Second)) // the first observation, long after construction
	set.Add(nil, labelsFrom(map[string]string{"m": "1"}), noRes(), "")
	exp := &capExporter{}
	if err := set.Export(context.Background(), exp, 0); err != nil {
		t.Fatal(err)
	}
	zeros := 0
	forEachNumberPoint(exp, func(m pmetric.Metric, dp pmetric.NumberDataPoint) {
		if dp.DoubleValue() == 0 {
			zeros++
		}
		if dp.StartTimestamp() >= dp.Timestamp() {
			t.Errorf("start %d is not before the point at %d", dp.StartTimestamp(), dp.Timestamp())
		}
	})
	if zeros != 2 {
		t.Fatalf("baseline zeros = %d, want 2 when the floor does not bind", zeros)
	}
}

// TestExportedScopeIsNamed: log-derived metrics shipped under an EMPTY
// instrumentation scope while every other producer in the repo named its own,
// so they were the only series a consumer could not attribute to the code that
// produced them.
func TestExportedScopeIsNamed(t *testing.T) {
	setTimeForTest(time.Unix(1_700_700_000, 0))
	defer testEpoch.Store(0)
	SetScopeVersion("v9.9.9-test")
	defer SetScopeVersion("")

	set, err := newTestSet([]Dynamic{{
		Name: "lines_total", Type: CounterType, Value: "1", Match: []string{"m=1"},
	}})
	if err != nil {
		t.Fatal(err)
	}
	set.Add(nil, labelsFrom(map[string]string{"m": "1"}), noRes(), "")

	exp := &capExporter{}
	if err := set.Export(context.Background(), exp, 0); err != nil {
		t.Fatal(err)
	}
	scopes := 0
	for _, md := range exp.md {
		rms := md.ResourceMetrics()
		for i := 0; i < rms.Len(); i++ {
			sms := rms.At(i).ScopeMetrics()
			for j := 0; j < sms.Len(); j++ {
				scopes++
				sc := sms.At(j).Scope()
				if sc.Name() != ScopeName {
					t.Errorf("scope name = %q, want %q", sc.Name(), ScopeName)
				}
				if sc.Version() != "v9.9.9-test" {
					t.Errorf("scope version = %q, want the build version", sc.Version())
				}
			}
		}
	}
	if scopes == 0 {
		t.Fatal("no ScopeMetrics exported")
	}
}

// The self-metrics Registry renders through the same code, so it gets the same
// two guarantees: a versioned scope and a start that is not the export time.
func TestRegistryScopeAndStart(t *testing.T) {
	setTimeForTest(time.Unix(1_700_700_000, 0))
	defer testEpoch.Store(0)
	SetScopeVersion("v9.9.9-test")
	defer SetScopeVersion("")

	r := NewRegistry()
	c := r.Counter("kubescrape_test_total", "help")
	c.Inc()

	exp := &capExporter{}
	if err := r.Export(context.Background(), exp, pcommon.NewResource()); err != nil {
		t.Fatal(err)
	}
	if len(exp.md) != 1 {
		t.Fatalf("exports = %d, want 1", len(exp.md))
	}
	sc := exp.md[0].ResourceMetrics().At(0).ScopeMetrics().At(0).Scope()
	if sc.Name() != RegistryScopeName {
		t.Errorf("scope name = %q, want %q", sc.Name(), RegistryScopeName)
	}
	if sc.Version() != "v9.9.9-test" {
		t.Errorf("scope version = %q, want the build version", sc.Version())
	}
	forEachNumberPoint(exp, func(m pmetric.Metric, dp pmetric.NumberDataPoint) {
		if dp.StartTimestamp() >= dp.Timestamp() {
			t.Errorf("%s: self-metric start %d is not before end %d", m.Name(), dp.StartTimestamp(), dp.Timestamp())
		}
	})
}
