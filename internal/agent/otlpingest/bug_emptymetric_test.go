package otlpingest

import (
	"context"
	"testing"

	"go.opentelemetry.io/collector/pdata/pmetric"
)

// TestSplitPreservesPointlessMetric is the regression test for datapoint/split
// mode dropping a metric that carries no data points: the per-type routing loops
// create the output metric shell only when a point routes, so a zero-point (or
// MetricTypeEmpty) metric produced no shell and its descriptor vanished, whereas
// resource mode keeps it. No sample data is lost either way, but the descriptor
// must survive.
func TestSplitPreservesPointlessMetric(t *testing.T) {
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	scope := rm.ScopeMetrics().AppendEmpty().Metrics()

	// A normal metric with a routable point.
	g := scope.AppendEmpty()
	g.SetName("queue.depth")
	dp := g.SetEmptyGauge().DataPoints().AppendEmpty()
	dp.SetIntValue(1)
	dp.Attributes().PutStr("container.id", "cafe01")

	// A metric with ZERO data points (e.g. a family scraped/pushed empty this
	// cycle) plus a bare MetricTypeEmpty one.
	empty := scope.AppendEmpty()
	empty.SetName("build.info")
	empty.SetEmptySum() // no data points

	bare := scope.AppendEmpty()
	bare.SetName("bare.metric") // MetricTypeEmpty

	out := newEnricher(newMeta(), MetricsDatapoint).EnrichMetrics(context.Background(), md)

	names := map[string]bool{}
	for i := 0; i < out.ResourceMetrics().Len(); i++ {
		r := out.ResourceMetrics().At(i)
		for j := 0; j < r.ScopeMetrics().Len(); j++ {
			ms := r.ScopeMetrics().At(j).Metrics()
			for k := 0; k < ms.Len(); k++ {
				names[ms.At(k).Name()] = true
			}
		}
	}
	for _, want := range []string{"queue.depth", "build.info", "bare.metric"} {
		if !names[want] {
			t.Fatalf("metric %q dropped by the split; present descriptors = %v", want, names)
		}
	}
}
