package metrics

// The start timestamp of a cumulative point, and the instrumentation scope it
// ships under. Both are wire contracts that nothing in this package used to
// assert, and both were wrong in a way no test could see: the values were
// right, so every test that checked values passed.

import (
	"context"
	"testing"
	"time"

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
	for round := 0; round < 3; round++ {
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

// The idle reset IS a genuine reset (snapshot zeroes value and count), so it is
// the one place the start must move — otherwise the drop to zero reads as a
// counter running backwards under an unchanged start, which is the one thing
// consumers are entitled to treat as corruption rather than a restart.
func TestIdleResetMovesTheStart(t *testing.T) {
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

	// Idle past MaxAge: snapshot zeroes the sample. Then observe again, so the
	// stream has a value to export under its new start.
	setTimeForTest(time.Unix(base+60, 0))
	if err := set.Export(context.Background(), &capExporter{}, 0); err != nil {
		t.Fatal(err)
	}
	set.Add(nil, labelsFrom(map[string]string{"m": "1"}), noRes(), "")

	after := &capExporter{}
	if err := set.Export(context.Background(), after, 0); err != nil {
		t.Fatal(err)
	}
	var afterStart pcommon.Timestamp
	forEachNumberPoint(after, func(m pmetric.Metric, dp pmetric.NumberDataPoint) {
		if m.Type() == pmetric.MetricTypeSum {
			afterStart = dp.StartTimestamp()
		}
	})
	if afterStart <= firstStart {
		t.Fatalf("start after an idle reset = %d, want later than %d — the zeroing IS a reset and must be encoded as one",
			afterStart, firstStart)
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
