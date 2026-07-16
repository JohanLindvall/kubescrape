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

// A point-level ID that fails to resolve must NOT leave the copied sender
// identity on the foreign group's resource (misattribution): the group keeps
// only the described object's raw ID, re-attributable downstream.
func TestSplitUnresolvedForeignIDDropsSenderIdentity(t *testing.T) {
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	ra := rm.Resource().Attributes()
	ra.PutStr("service.name", "my-exporter") // the SENDER's own identity
	ra.PutStr("k8s.pod.name", "my-exporter-abc")
	g := rm.ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
	g.SetName("kube_pod_status_ready")
	dp := g.SetEmptyGauge().DataPoints().AppendEmpty()
	dp.SetIntValue(1)
	dp.Attributes().PutStr("k8s.pod.uid", "no-such-uid") // resolves to nothing

	out := newEnricher(newMeta(), MetricsDatapoint).EnrichMetrics(context.Background(), md)

	var found bool
	for i := 0; i < out.ResourceMetrics().Len(); i++ {
		a := out.ResourceMetrics().At(i).Resource().Attributes()
		uid, ok := a.Get("k8s.pod.uid")
		if !ok || uid.Str() != "no-such-uid" {
			continue
		}
		found = true
		if v, ok := a.Get("service.name"); ok {
			t.Errorf("unresolved foreign group kept the sender's service.name %q", v.Str())
		}
		if v, ok := a.Get("k8s.pod.name"); ok {
			t.Errorf("unresolved foreign group kept the sender's k8s.pod.name %q", v.Str())
		}
	}
	if !found {
		t.Fatal("unresolved foreign group lost its raw ID (must stay re-attributable)")
	}
}
