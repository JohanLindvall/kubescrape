package metrics

// Binding a label set CREATES its series, at zero.
//
// Every caller that resolves its label values at startup — tailbuffer, one
// counter pair per configured tail-sampling policy plus one per early-decision
// reason; logenrich, one per enrich format — does it to make the absent form
// impossible: a series that appears only when the outcome first occurs cannot
// be told apart from a policy that is not configured or a code path this build
// does not have, and an alert written over the absent form matches nothing at
// all. The binding used to be a pure caching step, so that intent never reached
// the wire.

import (
	"context"
	"testing"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

// pointsFor collects (label value, value) for a metric's number points.
func pointsFor(t *testing.T, exp *capExporter, name, labelKey string) map[string]float64 {
	t.Helper()
	out := map[string]float64{}
	m, ok := exp.find(name)
	if !ok {
		return out
	}
	if m.Type() != pmetric.MetricTypeSum {
		t.Fatalf("%s: type %v, want a sum", name, m.Type())
	}
	dps := m.Sum().DataPoints()
	for i := 0; i < dps.Len(); i++ {
		dp := dps.At(i)
		v, _ := dp.Attributes().Get(labelKey)
		// The synthetic baseline zeros share the real point's labels; the real
		// point is the last one rendered for that label set, so last wins.
		out[v.Str()] = dp.DoubleValue()
	}
	return out
}

// A bound-but-never-incremented label value must export as 0, not be absent.
func TestBoundLabelValuesExportAsZero(t *testing.T) {
	r := NewRegistry()
	c := r.CounterVec("kubescrape_test_decisions_total", "help", "policy")
	// The startup binding pattern: every configured outcome, resolved up front.
	kept := c.WithLabelValues("keep-errors")
	c.WithLabelValues("never-fires")
	kept.Inc()

	exp := &capExporter{}
	if err := r.Export(context.Background(), exp, pcommon.NewResource()); err != nil {
		t.Fatal(err)
	}
	got := pointsFor(t, exp, "kubescrape_test_decisions_total", "policy")
	if _, ok := got["never-fires"]; !ok {
		t.Fatal("a bound policy that never fired has NO series: absent is indistinguishable from unconfigured, " +
			"which is exactly what binding every configured policy at startup exists to prevent")
	}
	if got["never-fires"] != 0 {
		t.Fatalf("bound-but-unfired series = %v, want 0", got["never-fires"])
	}
	if got["keep-errors"] != 1 {
		t.Fatalf("fired series = %v, want 1 — materializing must not be an observation", got["keep-errors"])
	}
}

// Materializing is idempotent and is never an observation: re-binding the same
// label values (a cache miss cannot happen twice, but a second vec over the
// same series can) must not reset or double-count what is already there.
func TestMaterializeIsNotAnObservation(t *testing.T) {
	r := NewRegistry()
	c := r.CounterVec("kubescrape_test_repeat_total", "help", "outcome")
	c.WithLabelValues("ok").Add(3)
	// A second registration of the same name reuses the series (Registry.add),
	// and its own vec cache is empty — so this binds the same label set again.
	same := r.CounterVec("kubescrape_test_repeat_total", "help", "outcome")
	if got := same.WithLabelValues("ok").Value(); got != 3 {
		t.Fatalf("value after re-binding = %v, want 3 (materialize must not zero a live sample)", got)
	}
	same.WithLabelValues("ok").Inc()
	if got := c.WithLabelValues("ok").Value(); got != 4 {
		t.Fatalf("value = %v, want 4", got)
	}
}

// A histogram's bound label set gets a zero-count point for the same reason,
// and a caller-supplied "le" is refused rather than minting a second,
// never-observed series under a hash observations never land on (see
// series.materialize and observePreHashed).
func TestBoundHistogramExportsZeroPoint(t *testing.T) {
	r := NewRegistry()
	h := r.HistogramVec("kubescrape_test_latency_seconds", "help", []float64{1, 10}, "kind")
	h.WithLabelValues("cold")

	exp := &capExporter{}
	if err := r.Export(context.Background(), exp, pcommon.NewResource()); err != nil {
		t.Fatal(err)
	}
	m, ok := exp.find("kubescrape_test_latency_seconds")
	if !ok {
		t.Fatal("a bound histogram label set exported no series at all")
	}
	dps := m.Histogram().DataPoints()
	if dps.Len() != 1 {
		t.Fatalf("histogram points = %d, want 1", dps.Len())
	}
	if dps.At(0).Count() != 0 || dps.At(0).Sum() != 0 {
		t.Fatalf("bound histogram point = count %d sum %v, want an empty distribution", dps.At(0).Count(), dps.At(0).Sum())
	}
}

func TestBoundHistogramWithLeLabelMintsNothing(t *testing.T) {
	r := NewRegistry()
	h := r.HistogramVec("kubescrape_test_le_seconds", "help", []float64{1, 10}, leLabel)
	h.WithLabelValues("5")
	if n := len(h.s.db); n != 0 {
		t.Fatalf("materialized %d samples for an le-labelled bind; observations fold le OUT of the identity, "+
			"so any sample created under the pre-hashed key is one nothing will ever observe into", n)
	}
	h.WithLabelValues("5").Observe(2)
	if n := len(h.s.db); n != 1 {
		t.Fatalf("samples after observing = %d, want 1", n)
	}
}
