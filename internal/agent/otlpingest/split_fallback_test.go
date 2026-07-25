package otlpingest

// Regression tests: the datapoint-split path must resolve ids with the same
// container-id-then-pod-uid fallback the resource path uses. Without it an
// identical payload was attributed differently depending on the metrics mode,
// and the split path additionally reduced the resource to the bare unresolved
// id — discarding every attribute the sender had set.

import (
	"context"
	"testing"

	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/JohanLindvall/kubescrape/internal/obs"
)

// staleIDMetrics builds a payload whose points carry an unresolvable container
// id alongside a resolvable pod uid — an SDK k8s resource detector emits both,
// then the container restarts (or its tombstone expires).
func staleIDMetrics(onDataPoint bool) pmetric.Metrics {
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr("service.name", "my-app")
	rm.Resource().Attributes().PutStr("deployment.environment", "prod")
	g := rm.ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
	g.SetName("q")
	dp := g.SetEmptyGauge().DataPoints().AppendEmpty()
	dp.SetIntValue(1)
	target := rm.Resource().Attributes()
	if onDataPoint {
		target = dp.Attributes()
	}
	target.PutStr("container.id", "gone-restarted")
	target.PutStr("k8s.pod.uid", "pod-uid-2")
	return md
}

func podNames(md pmetric.Metrics) []string {
	var out []string
	rms := md.ResourceMetrics()
	for i := 0; i < rms.Len(); i++ {
		v, ok := rms.At(i).Resource().Attributes().Get("k8s.pod.name")
		if !ok {
			out = append(out, "<none>")
			continue
		}
		out = append(out, v.Str())
	}
	return out
}

// Both metrics modes must reach the same attribution for the same payload.
func TestSplitFallsBackFromStaleContainerIDToPodUID(t *testing.T) {
	for _, tc := range []struct {
		name        string
		mode        MetricsMode
		onDataPoint bool
	}{
		{"resource mode", MetricsResource, false},
		{"split mode, resource-level id", MetricsDatapoint, false},
		{"split mode, datapoint-level id", MetricsDatapoint, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			out := newEnricher(newMeta(), tc.mode).
				EnrichMetrics(context.Background(), staleIDMetrics(tc.onDataPoint))
			got := podNames(out)
			if len(got) != 1 || got[0] != "web-2" {
				t.Errorf("pod names = %v, want [web-2]: the pod-uid fallback was lost", got)
			}
		})
	}
}

// A resource carrying an unresolvable container id AND an unresolvable pod uid
// is ONE unattributed resource. kubescrape_ingest_resources_total counts
// resources, so probing two ids must not tally two.
func TestUnresolvedCountedOncePerResource(t *testing.T) {
	before := obs.Ingested.WithLabelValues("unresolved").Value()

	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr("container.id", "nope")
	rm.Resource().Attributes().PutStr("k8s.pod.uid", "also-nope")
	g := rm.ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
	g.SetName("q")
	g.SetEmptyGauge().DataPoints().AppendEmpty().SetIntValue(1)

	newEnricher(newMeta(), MetricsResource).EnrichMetrics(context.Background(), md)

	if got := obs.Ingested.WithLabelValues("unresolved").Value() - before; got != 1 {
		t.Fatalf("unresolved delta = %v, want 1 for one resource", got)
	}
}
