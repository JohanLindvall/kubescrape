package transform

import (
	"testing"

	"go.opentelemetry.io/collector/pdata/pmetric"
)

// Data points are where a metric's cost lives: without reaching them a script
// could only rename or drop whole metrics, so "strip this one noisy label" —
// the usual reason to write a metrics transform — was inexpressible.
func TestMetricDataPointsAccess(t *testing.T) {
	src := `
def transform(batch):
    for m in batch:
        if m.type == "sum":
            for p in m.datapoints:
                p.attributes["pod_name"] = None
                if p.attributes["code"] == "500":
                    p.drop()
                else:
                    p.value = p.value * 2
        elif m.type == "histogram":
            for p in m.datapoints:
                p.attributes["kept"] = "yes"
`
	prog, err := compileStarlark("metrics", src)
	if err != nil {
		t.Fatal(err)
	}

	md := pmetric.NewMetrics()
	sm := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty()
	m := sm.Metrics().AppendEmpty()
	m.SetName("requests")
	sum := m.SetEmptySum()
	for _, code := range []string{"200", "500"} {
		p := sum.DataPoints().AppendEmpty()
		p.Attributes().PutStr("code", code)
		p.Attributes().PutStr("pod_name", "api-abc")
		p.SetDoubleValue(3)
	}
	h := sm.Metrics().AppendEmpty()
	h.SetName("latency")
	h.SetEmptyHistogram().DataPoints().AppendEmpty()

	dropped, err := prog.runMetrics(md, nil)
	if err != nil {
		t.Fatal(err)
	}
	prog.countDropped(dropped)

	pts := md.ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().At(0).Sum().DataPoints()
	if pts.Len() != 1 {
		t.Fatalf("data points = %d, want 1 (the 500 point must be dropped)", pts.Len())
	}
	p := pts.At(0)
	if _, ok := p.Attributes().Get("pod_name"); ok {
		t.Error("pod_name survived deletion")
	}
	if v, _ := p.Attributes().Get(dropMarker); v.Str() != "" || p.Attributes().Len() != 1 {
		t.Errorf("survivor attributes = %v, want just code", p.Attributes().AsRaw())
	}
	if p.DoubleValue() != 6 {
		t.Errorf("value = %v, want 6", p.DoubleValue())
	}
	hp := md.ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().At(1).Histogram().DataPoints().At(0)
	if v, ok := hp.Attributes().Get("kept"); !ok || v.Str() != "yes" {
		t.Error("histogram data point attributes not reachable")
	}
}

// A metric all of whose points are dropped carries no data and must not ship
// as bare descriptor overhead.
func TestMetricEmptiedByDroppedPointsIsPruned(t *testing.T) {
	prog, err := compileStarlark("metrics", "def transform(batch):\n    for m in batch:\n        for p in m.datapoints:\n            p.drop()\n")
	if err != nil {
		t.Fatal(err)
	}
	md := pmetric.NewMetrics()
	m := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
	m.SetName("g")
	m.SetEmptyGauge().DataPoints().AppendEmpty().SetIntValue(1)
	dropped, err := prog.runMetrics(md, nil)
	if err != nil {
		t.Fatal(err)
	}
	prog.countDropped(dropped)
	if md.ResourceMetrics().Len() != 0 {
		t.Fatalf("resource metrics = %d, want 0", md.ResourceMetrics().Len())
	}
}

// name/unit/description round-trip, and a bucketed point reports no scalar.
func TestMetricDescriptorFieldsAndBucketedValue(t *testing.T) {
	prog, err := compileStarlark("metrics", `
def transform(batch):
    for m in batch:
        m.unit = "s"
        m.description = "d"
        for p in m.datapoints:
            if p.value != None:
                fail("bucketed point reported a scalar value")
`)
	if err != nil {
		t.Fatal(err)
	}
	md := pmetric.NewMetrics()
	m := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
	m.SetName("latency")
	m.SetEmptyHistogram().DataPoints().AppendEmpty()
	dropped, err := prog.runMetrics(md, nil)
	if err != nil {
		t.Fatal(err)
	}
	prog.countDropped(dropped)
	got := md.ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().At(0)
	if got.Unit() != "s" || got.Description() != "d" {
		t.Fatalf("unit=%q description=%q", got.Unit(), got.Description())
	}
}
