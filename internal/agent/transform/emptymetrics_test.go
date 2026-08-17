package transform

import (
	"context"
	"testing"

	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/JohanLindvall/kubescrape/internal/pdatacheck"
)

// pruneDataPoints answers "is this metric left empty?" per metric type, and an
// UNTYPED metric took the default arm and answered `false` — no data points to
// drop was read as nothing to report, so the metric survived the prune and
// shipped a name, a description and a unit carrying no measurement. Legal
// OTLP, rejected by nothing, invisible forever.
//
// It runs alongside a metric the script empties and one it leaves alone, so
// the three verdicts are distinguished rather than merely counted.
func TestAnUntypedMetricIsPrunedLikeAnyOtherEmptyOne(t *testing.T) {
	prog, err := Compile([]byte(`
metrics: |
  def transform(batch):
      for m in batch:
          if m.name == "emptied":
              for dp in m.datapoints:
                  dp.drop()
`))
	if err != nil {
		t.Fatal(err)
	}
	next := &capExp{}
	w := Wrap(next, next, prog)

	md := pmetric.NewMetrics()
	sm := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty()
	// Untyped: no points to drop and none to keep.
	sm.Metrics().AppendEmpty().SetName("untyped")
	// Typed, and the script drops its only point.
	emptied := sm.Metrics().AppendEmpty()
	emptied.SetName("emptied")
	emptied.SetEmptyGauge().DataPoints().AppendEmpty().SetIntValue(1)
	// Typed, untouched.
	kept := sm.Metrics().AppendEmpty()
	kept.SetName("kept")
	kept.SetEmptyGauge().DataPoints().AppendEmpty().SetIntValue(2)

	if err := w.ExportMetrics(context.Background(), md); err != nil {
		t.Fatal(err)
	}
	if len(next.metrics) != 1 {
		t.Fatalf("exports = %d, want 1", len(next.metrics))
	}
	out := next.metrics[0]
	if bad := pdatacheck.EmptyMetrics(out); len(bad) > 0 {
		t.Errorf("empty metrics survived the transform: %v", bad)
	}

	var names []string
	rms := out.ResourceMetrics()
	for i := 0; i < rms.Len(); i++ {
		sms := rms.At(i).ScopeMetrics()
		for j := 0; j < sms.Len(); j++ {
			ms := sms.At(j).Metrics()
			for k := 0; k < ms.Len(); k++ {
				names = append(names, ms.At(k).Name())
			}
		}
	}
	if len(names) != 1 || names[0] != "kept" {
		t.Errorf("forwarded metrics = %v, want exactly [kept]", names)
	}
}

// A payload holding nothing but an untyped metric prunes to nothing, and a
// payload pruned to nothing is ACKED without a send — the scope and resource
// go with it rather than shipping as bare framing.
func TestAPayloadOfOnlyUntypedMetricsIsAckedWithoutASend(t *testing.T) {
	prog, err := Compile([]byte("metrics: |\n  def transform(batch):\n      pass\n"))
	if err != nil {
		t.Fatal(err)
	}
	next := &capExp{}
	w := Wrap(next, next, prog)

	md := pmetric.NewMetrics()
	md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics().AppendEmpty().SetName("untyped")

	if err := w.ExportMetrics(context.Background(), md); err != nil {
		t.Fatalf("must be acked, not failed: %v", err)
	}
	if len(next.metrics) != 0 {
		t.Errorf("exports = %d, want 0", len(next.metrics))
	}
}
