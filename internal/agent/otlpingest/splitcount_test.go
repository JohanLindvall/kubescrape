package otlpingest

// F3 regression: the DRY refactor's shared Enricher.sameObject resolves both of
// its candidate tokens through attrsFor, which writes them into the per-request
// enrichCache under the token key. builtAttrs then decided WHETHER to tally the
// enriched/unresolved outcome by asking "was this token already cached?", so on
// the split path — where resource() calls sameObject BEFORE builtAttrs for the
// same id — every described object was silently omitted from
// kubescrape_ingest_resources_total{enriched|unresolved}, the ingest-attribution
// health signal, in exactly the datapoint / auto-demoted-to-split mode the
// counter matters most. The fix gates the tally on a dedicated counted-marker
// instead, so it fires once per object regardless of what cached the token.

import (
	"context"
	"testing"

	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/JohanLindvall/kubescrape/internal/obs"
	"github.com/JohanLindvall/kubescrape/pkg/kubemeta"
)

// TestSplitCountsForeignDescribedObject is the F3 reproduction: a datapoint-mode
// push whose resource names container A (pod A, via container.id) and whose
// point names a DIFFERENT, resolvable pod B (via k8s.pod.uid). The point's
// object is foreign, so resource() calls sameObject (which resolves and caches
// both tokens) and then builtAttrs — which, pre-fix, saw the id already cached
// and tallied 0. Post-fix it tallies enriched exactly once for pod B.
func TestSplitCountsForeignDescribedObject(t *testing.T) {
	enriched := obs.Ingested.WithLabelValues("enriched").Value()
	unresolved := obs.Ingested.WithLabelValues("unresolved").Value()

	// newMeta: cafe01 -> web-1/pod-uid-1, pod-uid-2 -> web-2. Two distinct pods.
	md := gaugeWith(
		map[string]string{"container.id": "cafe01"},   // resource = container A / pod A
		map[string]string{"k8s.pod.uid": "pod-uid-2"}, // point = foreign pod B
	)
	out := newEnricher(newMeta(), MetricsDatapoint).EnrichMetrics(context.Background(), md)

	// The foreign object was split out and enriched (sanity: the fix is about the
	// COUNTER, but a wrong split would make the count meaningless).
	if got := podNames(out); len(got) != 1 || got[0] != "web-2" {
		t.Fatalf("pod names = %v, want [web-2]", got)
	}
	if got := obs.Ingested.WithLabelValues("enriched").Value() - enriched; got != 1 {
		t.Fatalf("enriched delta = %v, want 1 (pre-fix it was 0: the described object was omitted from the counter)", got)
	}
	if got := obs.Ingested.WithLabelValues("unresolved").Value() - unresolved; got != 0 {
		t.Fatalf("unresolved delta = %v, want 0", got)
	}
}

// mergePodMeta resolves container id cafe01 and pod uid pod-uid-1 to the SAME
// pod, so a point naming the pod by uid describes the resource's own object at
// pod grain (sameObject == true, the merge branch of resource()).
func mergePodMeta() *fakeMeta {
	pod := kubemeta.Pod{Name: "web-1", Namespace: "default", UID: "pod-uid-1", NodeName: "node1"}
	return &fakeMeta{
		containers: map[string]*kubemeta.ContainerMetadata{
			"cafe01": {Container: kubemeta.Container{Name: "app", ID: "containerd://cafe01"}, Pod: pod},
		},
		pods: map[string]*kubemeta.Pod{"pod-uid-1": &pod},
	}
}

// TestSplitCountsSameObjectMergeOnce: when the point names the SAME object as
// the resource (pod-grain merge branch), sameObject is still called first and
// caches the id — yet the object must count exactly once, never zero (pre-fix)
// and never twice (the double-count the single accounting site rules out).
func TestSplitCountsSameObjectMergeOnce(t *testing.T) {
	enriched := obs.Ingested.WithLabelValues("enriched").Value()

	md := gaugeWith(
		map[string]string{"container.id": "cafe01"},   // resource = pod-uid-1 via container A
		map[string]string{"k8s.pod.uid": "pod-uid-1"}, // point = the same pod, by uid
	)
	out := newEnricher(mergePodMeta(), MetricsDatapoint).EnrichMetrics(context.Background(), md)

	if n := out.ResourceMetrics().Len(); n != 1 {
		t.Fatalf("resources = %d, want 1 (same object, merged)", n)
	}
	if got := obs.Ingested.WithLabelValues("enriched").Value() - enriched; got != 1 {
		t.Fatalf("enriched delta = %v, want exactly 1 (0 = under-count, 2 = double-count)", got)
	}
}

// TestSplitDescribedObjectCountedOncePerRequest: two input resources both
// describing the SAME foreign object must count that object once — the
// counted-marker lives in the batch-wide enrichCache, so a second grouper
// sharing it does not re-tally. Guards against the marker regressing to a
// per-grouper flag.
func TestSplitDescribedObjectCountedOncePerRequest(t *testing.T) {
	enriched := obs.Ingested.WithLabelValues("enriched").Value()

	md := pmetric.NewMetrics()
	for r := 0; r < 2; r++ {
		rm := md.ResourceMetrics().AppendEmpty()
		rm.Resource().Attributes().PutStr("container.id", "cafe01") // resource = pod A
		dp := rm.ScopeMetrics().AppendEmpty().Metrics().AppendEmpty().
			SetEmptyGauge().DataPoints().AppendEmpty()
		dp.SetIntValue(1)
		dp.Attributes().PutStr("k8s.pod.uid", "pod-uid-2") // both describe foreign pod B
	}
	newEnricher(newMeta(), MetricsDatapoint).EnrichMetrics(context.Background(), md)

	if got := obs.Ingested.WithLabelValues("enriched").Value() - enriched; got != 1 {
		t.Fatalf("enriched delta = %v, want 1 (one distinct described object across the request)", got)
	}
}

// TestResourceModeStillCountsOnce pins that the fix did not disturb the
// resource-mode accounting (which counts per RESOURCE, inline in applyMetadata,
// not through builtAttrs).
func TestResourceModeStillCountsOnce(t *testing.T) {
	enriched := obs.Ingested.WithLabelValues("enriched").Value()

	md := gaugeWith(map[string]string{"container.id": "cafe01"}, nil)
	newEnricher(newMeta(), MetricsResource).EnrichMetrics(context.Background(), md)

	if got := obs.Ingested.WithLabelValues("enriched").Value() - enriched; got != 1 {
		t.Fatalf("enriched delta = %v, want 1 for one enriched resource", got)
	}
}
