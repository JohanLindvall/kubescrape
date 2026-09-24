package otlpingest

// The datapoint/split path (split.go): regrouping every data point into one
// resource per described object — attribution, identity, the per-push group
// and copy budgets, their overflow, and the outcome accounting.

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/JohanLindvall/kubescrape/internal/obs"
	"github.com/JohanLindvall/kubescrape/pkg/kubemeta"
)

// Datapoint mode with points naming DIFFERENT pods inside ONE metric: each
// point must land in its own resource, keeping the metric's identity. Clean.
func TestDatapointModeSplitsDifferentPodsInOneMetric(t *testing.T) {
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	g := rm.ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
	g.SetName("queue.depth")
	dps := g.SetEmptyGauge().DataPoints()
	d1 := dps.AppendEmpty()
	d1.SetIntValue(1)
	d1.Attributes().PutStr("k8s.pod.uid", "pod-uid-2")
	d2 := dps.AppendEmpty()
	d2.SetIntValue(2)
	d2.Attributes().PutStr("container.id", "cafe01")

	out := newEnricher(newMeta(), MetricsDatapoint).EnrichMetrics(context.Background(), md)
	if out.ResourceMetrics().Len() != 2 {
		t.Fatalf("resources = %d; want one per distinct object", out.ResourceMetrics().Len())
	}
	got := map[string]int64{}
	for i := 0; i < out.ResourceMetrics().Len(); i++ {
		r := out.ResourceMetrics().At(i)
		v, _ := r.Resource().Attributes().Get("k8s.pod.name")
		m := r.ScopeMetrics().At(0).Metrics().At(0)
		if m.Name() != "queue.depth" {
			t.Fatalf("metric identity lost: %q", m.Name())
		}
		got[v.Str()] = m.Gauge().DataPoints().At(0).IntValue()
	}
	if got["web-2"] != 1 || got["web-1"] != 2 {
		t.Fatalf("points mis-routed: %v", got)
	}
}

// Regression guard: in datapoint/split mode the output resource for EACH
// described object starts as a copy of the SENDER's resource, which carries
// the sender's own identity attrs (k8s.pod.name, service.name, ... — typical
// of any SDK with a k8s resource detector). A merge that refused to overwrite
// them ("the sender is authoritative") attributed every split point to the
// pushing pod. The fix: on split resources the resolved identity OVERWRITES
// the copied sender attributes (overwriteAttrs) — the sender is authoritative
// about itself, not about the other objects it describes.
func TestSplitResourceUsesDescribedObjectIdentity(t *testing.T) {
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	ra := rm.Resource().Attributes()
	ra.PutStr("service.name", "my-exporter")     // the SENDER's own identity,
	ra.PutStr("k8s.pod.name", "my-exporter-abc") // e.g. from the downward API
	ra.PutStr("k8s.pod.uid", "exporter-uid")
	g := rm.ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
	g.SetName("kube_pod_status_ready")
	dp := g.SetEmptyGauge().DataPoints().AppendEmpty()
	dp.SetIntValue(1)
	dp.Attributes().PutStr("k8s.pod.uid", "pod-uid-2") // describes a DIFFERENT pod

	out := newEnricher(newMeta(), MetricsDatapoint).EnrichMetrics(context.Background(), md)

	var found bool
	for i := 0; i < out.ResourceMetrics().Len(); i++ {
		a := out.ResourceMetrics().At(i).Resource().Attributes()
		uid, _ := a.Get("k8s.pod.uid")
		if uid.Str() != "pod-uid-2" {
			continue
		}
		found = true
		if v, _ := a.Get("k8s.pod.name"); v.Str() != "web-2" {
			t.Errorf("k8s.pod.name = %q; want web-2 (the described pod), got the sender's own pod", v.Str())
		}
		if v, _ := a.Get("service.name"); v.Str() == "my-exporter" {
			t.Errorf("service.name = %q; the described object's series are attributed to the exporter", v.Str())
		}
	}
	if !found {
		t.Fatal("no resource for the described pod")
	}
}

// --- container id, then pod uid: the same token fallback as the resource path ---

// Regression tests: the datapoint-split path must resolve ids with the same
// container-id-then-pod-uid fallback the resource path uses. Without it an
// identical payload was attributed differently depending on the metrics mode,
// and the split path additionally reduced the resource to the bare unresolved
// id — discarding every attribute the sender had set.

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

// --- the enriched/unresolved tally ---

// F3 regression: the DRY refactor's shared Enricher.sameObject resolves both of
// its candidate tokens through attrsFor, which writes them into the
// per-request cache (reqCache) under the token key. builtAttrs then decided
// WHETHER to tally the enriched/unresolved outcome by asking "was this token
// already cached?", so on
// the split path — where resource() calls sameObject BEFORE builtAttrs for the
// same id — every described object was silently omitted from
// kubescrape_ingest_resources_total{enriched|unresolved}, the ingest-attribution
// health signal, in exactly the datapoint / auto-demoted-to-split mode the
// counter matters most. The fix gates the tally on a dedicated counted-marker
// instead, so it fires once per object regardless of what cached the token.

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
// counted-marker lives in the push-wide reqCache, so a second grouper
// sharing it does not re-tally. Guards against the marker regressing to a
// per-grouper flag.
func TestSplitDescribedObjectCountedOncePerRequest(t *testing.T) {
	enriched := obs.Ingested.WithLabelValues("enriched").Value()

	md := pmetric.NewMetrics()
	for range 2 {
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
// resource-mode accounting (which counts per RESOURCE, inline in enrichAttrs,
// not through builtAttrs).
func TestResourceModeStillCountsOnce(t *testing.T) {
	enriched := obs.Ingested.WithLabelValues("enriched").Value()

	md := gaugeWith(map[string]string{"container.id": "cafe01"}, nil)
	newEnricher(newMeta(), MetricsResource).EnrichMetrics(context.Background(), md)

	if got := obs.Ingested.WithLabelValues("enriched").Value() - enriched; got != 1 {
		t.Fatalf("enriched delta = %v, want 1 for one enriched resource", got)
	}
}

// senderSelfLabelledPush is a sender naming ITSELF two ways: its resource
// carries its container.id, one data point its pod's k8s.pod.uid, another
// nothing at all.
func senderSelfLabelledPush(resAttrs map[string]string, first map[string]string) pmetric.Metrics {
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	for k, v := range resAttrs {
		rm.Resource().Attributes().PutStr(k, v)
	}
	dps := rm.ScopeMetrics().AppendEmpty().Metrics().AppendEmpty().SetEmptyGauge().DataPoints()
	dp := dps.AppendEmpty()
	dp.SetIntValue(1)
	for k, v := range first {
		dp.Attributes().PutStr(k, v)
	}
	dps.AppendEmpty().SetIntValue(2) // names nothing: the resource's id applies
	return md
}

// Datapoint mode used to split ONE object across two resources when a sender
// named itself two ways: the id-less point went to the container-grain group,
// the pod-uid point to a second, pod-grain group with a different
// service.instance.id and no container name — while auto mode attributed the
// same payload to one resource. A point naming the sender's own object at the
// same or a coarser grain now joins the sender's group, in both modes, and the
// object is counted enriched once.
func TestSplitFoldsTheSendersOwnPointsIntoOneResource(t *testing.T) {
	for _, mode := range []MetricsMode{MetricsAuto, MetricsDatapoint} {
		t.Run(string(mode), func(t *testing.T) {
			enriched := obs.Ingested.WithLabelValues("enriched").Value()
			md := senderSelfLabelledPush(
				map[string]string{"container.id": "cafe01", "k8s.container.name": "app"},
				map[string]string{"k8s.pod.uid": "pod-uid-1"},
			)
			out := newEnricher(mergePodMeta(), mode).EnrichMetrics(context.Background(), md)
			if n := out.ResourceMetrics().Len(); n != 1 {
				for i := range n {
					t.Logf("resource %d: %v", i, out.ResourceMetrics().At(i).Resource().Attributes().AsRaw())
				}
				t.Fatalf("resources = %d, want 1: one object, named two ways, split across resources", n)
			}
			if got := out.DataPointCount(); got != 2 {
				t.Errorf("data points = %d, want 2", got)
			}
			a := out.ResourceMetrics().At(0).Resource().Attributes()
			if v, _ := a.Get("k8s.container.name"); v.Str() != "app" {
				t.Errorf("k8s.container.name = %q, want the sender's own container", v.Str())
			}
			if got := obs.Ingested.WithLabelValues("enriched").Value() - enriched; got != 1 {
				t.Errorf("enriched delta = %v, want 1 (one object)", got)
			}
		})
	}
}

// The fold is by grain, and only one way: a point naming one CONTAINER of the
// pod a pod-grain resource describes is a FINER object, and keeps the group
// datapoint mode exists to give it.
func TestSplitKeepsTheContainerGrainOfAPointUnderAPodResource(t *testing.T) {
	md := senderSelfLabelledPush(
		map[string]string{"k8s.pod.uid": "pod-uid-1"},
		map[string]string{"container.id": "cafe01"},
	)
	out := newEnricher(mergePodMeta(), MetricsDatapoint).EnrichMetrics(context.Background(), md)
	if n := out.ResourceMetrics().Len(); n != 2 {
		t.Fatalf("resources = %d, want 2: the container's point and the pod's", n)
	}
	containers := map[string]bool{}
	for i := 0; i < out.ResourceMetrics().Len(); i++ {
		v, _ := out.ResourceMetrics().At(i).Resource().Attributes().Get("k8s.container.name")
		containers[v.Str()] = true
	}
	if !containers["app"] || !containers[""] {
		t.Errorf("container names = %v, want one container-grain and one pod-grain resource", containers)
	}
}

// --- a metric with no data points ---

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

// --- the per-push budgets: group count and copied bytes ---

// A push naming more than maxSplitGroups distinct objects with NO id-less
// point (so the "" fallback group is never created before the cap binds) must
// not recurse forever: resource("") missing the map while the cap is exceeded
// used to call itself unboundedly — a stack overflow crash on the
// unauthenticated ingest listener, triggerable by any pod.
func TestSplitCapWithoutIdlessPointDoesNotRecurse(t *testing.T) {
	md := pmetric.NewMetrics()
	sm := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty()
	const n = maxSplitGroups + 5
	for i := range n {
		m := sm.Metrics().AppendEmpty()
		m.SetName("m")
		dp := m.SetEmptyGauge().DataPoints().AppendEmpty()
		dp.SetIntValue(1)
		dp.Attributes().PutStr("k8s.pod.uid", fmt.Sprintf("uid-%d", i))
	}
	out := NewEnricher(Config{Meta: newMeta(), MetricsMode: MetricsDatapoint}).
		EnrichMetrics(context.Background(), md)

	// Every point is still forwarded: the capped remainder folds into the one
	// "" fallback resource rather than crashing.
	got := 0
	rms := out.ResourceMetrics()
	for i := 0; i < rms.Len(); i++ {
		sms := rms.At(i).ScopeMetrics()
		for j := 0; j < sms.Len(); j++ {
			ms := sms.At(j).Metrics()
			for k := 0; k < ms.Len(); k++ {
				got += ms.At(k).Gauge().DataPoints().Len()
			}
		}
	}
	if got != n {
		t.Fatalf("forwarded %d points, want all %d (capped remainder folded, none dropped)", got, n)
	}
	// The fallback is a single bucket, so the group count is bounded at the cap.
	if rms.Len() > maxSplitGroups+1 {
		t.Fatalf("group count %d exceeds the cap+fallback bound", rms.Len())
	}
}

// Regression tests for the split path's OUTPUT bounds. The ingest byte budget
// bounds what a push READS and maxSplitGroups bounds how many groups it may
// mint — but the groups are full copies of the sender's resource (and every
// routed shell a copy of its metric's descriptor), so the group cap had to be
// per PAYLOAD and the minted bytes needed a budget of their own
// (maxSplitCopyBytes). Both degrade into the "" fallback: forwarded,
// unenriched, counted split_capped — never refused, never an OOM.

// The split-group cap bounds one PUSH, not one input ResourceMetrics: with the
// cap checked against a per-grouper map, 3 input resources of ~780 distinct
// pod uids each minted ~2350 group resources — the payload's own structure
// chose the bound, and a 16 MiB body fits enough minimal ResourceMetrics for
// ~390k groups.
func TestSplitGroupCapIsPerPayload(t *testing.T) {
	capped := obs.Ingested.WithLabelValues("split_capped").Value()

	const rmCount = 3
	perRM := maxSplitGroups/rmCount + 100 // over the cap in aggregate, under it per input resource
	md := pmetric.NewMetrics()
	for r := range rmCount {
		dps := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().
			Metrics().AppendEmpty().SetEmptyGauge().DataPoints()
		for i := range perRM {
			dp := dps.AppendEmpty()
			dp.SetIntValue(1)
			dp.Attributes().PutStr("k8s.pod.uid", fmt.Sprintf("u-%d-%d", r, i))
		}
	}

	out := NewEnricher(Config{Meta: &fakeMeta{}, MetricsMode: MetricsDatapoint}).
		EnrichMetrics(context.Background(), md)

	if got := out.DataPointCount(); got != rmCount*perRM {
		t.Fatalf("forwarded %d points, want all %d (capped remainder folded, none dropped)", got, rmCount*perRM)
	}
	// The documented bound: the cap plus one "" fallback per input resource.
	if got := out.ResourceMetrics().Len(); got > maxSplitGroups+rmCount {
		t.Fatalf("output resources = %d, want <= %d: the group cap is per payload", got, maxSplitGroups+rmCount)
	}
	if got, want := obs.Ingested.WithLabelValues("split_capped").Value()-capped, float64(rmCount*perRM-maxSplitGroups); got != want {
		t.Errorf("split_capped delta = %v, want %v (one per refused id)", got, want)
	}
}

// The splitter's output BYTES are bounded. Every admitted group is a full copy
// of the sender's resource, so a 282 KB push — 200 KiB of resource attributes
// plus 2048 resolvable pod uids — serialized to 421 MB (1493x): one ~421 MB
// marshal per push at the disk buffer's enqueue, ~112 otlpsplit parts without
// one, from the unauthenticated listener. Past maxSplitCopyBytes the remaining
// objects fold into the stripped overflow group (overflowID), exactly as the
// group cap's refusals do.
func TestSplitOutputBytesAreBounded(t *testing.T) {
	capped := obs.Ingested.WithLabelValues("split_capped").Value()

	const uids = 2048
	meta := &fakeMeta{pods: map[string]*kubemeta.Pod{}}
	for i := range uids {
		uid := fmt.Sprintf("uid-%d", i)
		meta.pods[uid] = &kubemeta.Pod{Name: "pod-" + uid, Namespace: "apps", UID: uid, NodeName: "node1"}
	}

	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	pad := strings.Repeat("x", 4<<10)
	for i := range 50 { // ~200 KiB of sender resource attributes
		rm.Resource().Attributes().PutStr(fmt.Sprintf("bulk.attr.%02d", i), pad)
	}
	g := rm.ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
	g.SetName("kube_pod_info")
	dps := g.SetEmptyGauge().DataPoints()
	for i := range uids {
		dp := dps.AppendEmpty()
		dp.SetIntValue(1)
		dp.Attributes().PutStr("k8s.pod.uid", fmt.Sprintf("uid-%d", i))
	}

	var m pmetric.ProtoMarshaler
	in := m.MetricsSize(md)

	out := NewEnricher(Config{Meta: meta, MetricsMode: MetricsDatapoint}).
		EnrichMetrics(context.Background(), md)

	if got := out.DataPointCount(); got != uids {
		t.Fatalf("forwarded %d points, want all %d", got, uids)
	}
	// Copy budget plus estimate slack (the estimate charges string payloads,
	// not exact proto framing, and the last admitted group may overshoot by one
	// resource copy).
	bound := maxSplitCopyBytes + maxSplitCopyBytes/2
	if got := m.MetricsSize(out); got > bound {
		t.Fatalf("output serializes to %d bytes for a %d-byte input, want <= %d: the split output is unbounded", got, in, bound)
	}
	if obs.Ingested.WithLabelValues("split_capped").Value() == capped {
		t.Fatal("split_capped did not move: the byte-budget degradation must be counted")
	}
	// Degraded, not disabled: groups admitted before the budget bound are
	// still split out and enriched.
	enriched := 0
	rms := out.ResourceMetrics()
	for i := 0; i < rms.Len(); i++ {
		if _, ok := rms.At(i).Resource().Attributes().Get("k8s.pod.name"); ok {
			enriched++
		}
	}
	if enriched == 0 {
		t.Fatal("no group was enriched: the budget must degrade, not disable")
	}
}

// Descriptor copies are charged too: every point routed to a new shell repeats
// its metric's description, so 2048 cheaply-admitted groups times a 256 KiB
// description was the resource amplification through a different copy — and a
// group admitted under the byte budget must stop minting shells once it binds.
func TestSplitDescriptorCopiesAreBounded(t *testing.T) {
	md := pmetric.NewMetrics()
	sm := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty()
	desc := strings.Repeat("d", 256<<10)
	const metrics = 4
	for mi := range metrics {
		g := sm.Metrics().AppendEmpty()
		g.SetName(fmt.Sprintf("m%d", mi))
		g.SetDescription(desc)
		dps := g.SetEmptyGauge().DataPoints()
		for i := range maxSplitGroups {
			dp := dps.AppendEmpty()
			dp.SetIntValue(1)
			dp.Attributes().PutStr("k8s.pod.uid", fmt.Sprintf("uid-%d", i))
		}
	}

	out := NewEnricher(Config{Meta: &fakeMeta{}, MetricsMode: MetricsDatapoint}).
		EnrichMetrics(context.Background(), md)

	if got, want := out.DataPointCount(), metrics*maxSplitGroups; got != want {
		t.Fatalf("forwarded %d points, want all %d", got, want)
	}
	var m pmetric.ProtoMarshaler
	bound := maxSplitCopyBytes + maxSplitCopyBytes/2
	if got := m.MetricsSize(out); got > bound {
		t.Fatalf("output serializes to %d bytes, want <= %d: descriptor copies are not budgeted", got, bound)
	}
}

// --- what a refused object's points fold into ---

// Regression tests for what happens to a described object the split budgets
// REFUSE. maxSplitCopyBytes/maxSplitGroups is 8 KiB of resource estimate, so an
// attribute-rich exporter reaches the byte budget long before the group cap —
// the overflow path is the ordinary case for exactly the KSM-shaped senders
// split mode exists for, not a corner.

// ksmPush builds a describing-exporter payload: one fat sender resource naming
// ITSELF, and pods data points naming distinct OTHER pods, all resolvable.
func ksmPush(t *testing.T, pods, padAttrs int) (pmetric.Metrics, *fakeMeta) {
	t.Helper()
	meta := &fakeMeta{pods: map[string]*kubemeta.Pod{
		"ksm-uid": {Name: "ksm-0", Namespace: "monitoring", UID: "ksm-uid", NodeName: "node1"},
	}}
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	a := rm.Resource().Attributes()
	a.PutStr("k8s.pod.uid", "ksm-uid")
	a.PutStr("k8s.pod.name", "ksm-0")
	a.PutStr("service.name", "kube-state-metrics")
	pad := strings.Repeat("x", 2<<10)
	for i := range padAttrs { // the sender's own resource, over the 8 KiB seam
		a.PutStr(fmt.Sprintf("sender.attr.%02d", i), pad)
	}
	g := rm.ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
	g.SetName("kube_pod_info")
	dps := g.SetEmptyGauge().DataPoints()
	for i := range pods {
		uid := fmt.Sprintf("uid-%d", i)
		meta.pods[uid] = &kubemeta.Pod{Name: "app-" + uid, Namespace: "apps", UID: uid, NodeName: "node1"}
		dp := dps.AppendEmpty()
		dp.SetIntValue(1)
		dp.Attributes().PutStr("k8s.pod.uid", uid)
	}
	return md, meta
}

// A byte-refused object's points must not export under the SENDER's identity.
// The refusal used to fold them into the id-less "" chain, which skips the
// strip-and-overwrite the admitted groups get — so the exporter's
// k8s.pod.name/service.name labelled hundreds of other pods' series, which in
// Prometheus terms is those pods' series under the exporter's job and instance.
func TestByteRefusedGroupDoesNotCarrySenderIdentity(t *testing.T) {
	capped := obs.Ingested.WithLabelValues("split_capped").Value()
	const pods = 400
	md, meta := ksmPush(t, pods, 42) // ~84 KiB of sender resource attributes

	out := NewEnricher(Config{Meta: meta, MetricsMode: MetricsDatapoint}).
		EnrichMetrics(context.Background(), md)

	if got := out.DataPointCount(); got != pods {
		t.Fatalf("forwarded %d points, want all %d", got, pods)
	}
	if obs.Ingested.WithLabelValues("split_capped").Value() == capped {
		t.Fatal("nothing was refused: the payload no longer exercises the byte budget")
	}
	mislabelled := 0
	rms := out.ResourceMetrics()
	for i := 0; i < rms.Len(); i++ {
		rm := rms.At(i)
		v, ok := rm.Resource().Attributes().Get("service.name")
		if !ok || v.Str() != "kube-state-metrics" {
			continue
		}
		sms := rm.ScopeMetrics()
		for j := 0; j < sms.Len(); j++ {
			ms := sms.At(j).Metrics()
			for k := 0; k < ms.Len(); k++ {
				mislabelled += ms.At(k).Gauge().DataPoints().Len()
			}
		}
	}
	if mislabelled != 0 {
		t.Errorf("%d described pods' points carry the exporter's service.name; the sender's identity must be stripped from a refused group", mislabelled)
	}
	// The id survives on the points, so a downstream consumer can still resolve
	// what this push could not afford to.
	last := rms.At(rms.Len() - 1)
	dp := last.ScopeMetrics().At(0).Metrics().At(0).Gauge().DataPoints().At(0)
	if _, ok := dp.Attributes().Get("k8s.pod.uid"); !ok {
		t.Error("a refused point lost its own id attribute")
	}
}

// The other half of the same rule: a refused id that is the SENDER's own keeps
// the sender's resource. Those points describe the exporter, so its identity is
// theirs, and folding them into the stripped overflow group would lose what the
// push got right.
func TestByteRefusedSenderIDKeepsItsOwnIdentity(t *testing.T) {
	md, meta := ksmPush(t, 400, 42)
	// A second metric, routed after the budget has bound, whose points name the
	// SENDER — the id its own resource carries.
	sm := md.ResourceMetrics().At(0).ScopeMetrics().At(0)
	own := sm.Metrics().AppendEmpty()
	own.SetName("kube_state_metrics_build_info")
	for range 4 {
		dp := own.SetEmptyGauge().DataPoints().AppendEmpty()
		dp.SetIntValue(1)
		dp.Attributes().PutStr("k8s.pod.uid", "ksm-uid")
	}

	out := NewEnricher(Config{Meta: meta, MetricsMode: MetricsDatapoint}).
		EnrichMetrics(context.Background(), md)

	found := false
	rms := out.ResourceMetrics()
	for i := 0; i < rms.Len(); i++ {
		rm := rms.At(i)
		ms := rm.ScopeMetrics().At(0).Metrics()
		for j := 0; j < ms.Len(); j++ {
			if ms.At(j).Name() != "kube_state_metrics_build_info" {
				continue
			}
			found = true
			if v, ok := rm.Resource().Attributes().Get("service.name"); !ok || v.Str() != "kube-state-metrics" {
				t.Errorf("the sender's own points lost its service.name (got %q, present=%v)", v.Str(), ok)
			}
		}
	}
	if !found {
		t.Fatal("the sender's own metric was not forwarded")
	}
}

// The peer-IP outcome accounting belongs to a push that named NOTHING. A
// budget-refused id folding into the id-less chain made a fully-attributed
// exporter tally unresolved once per input ResourceMetrics — noise on the one
// counter that says whether ingest attribution works at all.
func TestByteRefusedGroupDoesNotCountUnresolved(t *testing.T) {
	unresolved := obs.Ingested.WithLabelValues("unresolved").Value()
	md, meta := ksmPush(t, 400, 42)

	NewEnricher(Config{Meta: meta, MetricsMode: MetricsDatapoint}).
		EnrichMetrics(context.Background(), md)

	if got := obs.Ingested.WithLabelValues("unresolved").Value() - unresolved; got != 0 {
		t.Fatalf("unresolved delta = %v, want 0: every id in this push resolved", got)
	}
}

// Once the byte budget binds, admit also refuses further scope/shell copies
// for ids that ALREADY have their own enriched group — and those refusals ARE
// counted, once per object. The refused points do not stay on the group: they
// fold into the stripped overflow resource, so the object's series fork across
// its enriched resource and an unenriched one — a real degradation of an
// object the push named, which is what split_capped reports. (An earlier
// version left the grouped case uncounted on the theory that the object "was
// in fact enriched"; its points' actual placement made that claim false and
// the mid-push bind invisible.)
func TestByteRefusedShellOfAnEnrichedGroupIsCounted(t *testing.T) {
	capped := obs.Ingested.WithLabelValues("split_capped").Value()

	const objects = 8
	meta := &fakeMeta{pods: map[string]*kubemeta.Pod{}}
	md := pmetric.NewMetrics()
	sm := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty()
	for _, desc := range []string{"", strings.Repeat("d", 4<<20)} {
		// Metric 1 admits every object cheaply; metric 2's fat descriptor drives
		// splitCopied past the budget partway through the SAME ids.
		g := sm.Metrics().AppendEmpty()
		g.SetName(fmt.Sprintf("m%d", len(desc)))
		g.SetDescription(desc)
		dps := g.SetEmptyGauge().DataPoints()
		for i := range objects {
			uid := fmt.Sprintf("uid-%d", i)
			meta.pods[uid] = &kubemeta.Pod{Name: "app-" + uid, Namespace: "apps", UID: uid, NodeName: "node1"}
			dp := dps.AppendEmpty()
			dp.SetIntValue(1)
			dp.Attributes().PutStr("k8s.pod.uid", uid)
		}
	}

	out := NewEnricher(Config{Meta: meta, MetricsMode: MetricsDatapoint}).
		EnrichMetrics(context.Background(), md)

	if got := out.DataPointCount(); got != 2*objects {
		t.Fatalf("forwarded %d points, want all %d", got, 2*objects)
	}
	// Every object got its own group from the first metric; the fat metric's
	// points split between shells admitted before the budget bound (on their
	// enriched groups) and the stripped overflow fallback after it.
	enriched := 0
	forked := map[string]struct{}{} // uids whose fat-metric points landed on overflow
	rms := out.ResourceMetrics()
	for i := 0; i < rms.Len(); i++ {
		rm := rms.At(i)
		_, isEnriched := rm.Resource().Attributes().Get("k8s.pod.name")
		if isEnriched {
			enriched++
		}
		sms := rm.ScopeMetrics()
		for j := 0; j < sms.Len(); j++ {
			ms := sms.At(j).Metrics()
			for k := 0; k < ms.Len(); k++ {
				m := ms.At(k)
				if isEnriched || len(m.Description()) == 0 {
					continue
				}
				dps := m.Gauge().DataPoints()
				for l := 0; l < dps.Len(); l++ {
					if v, ok := dps.At(l).Attributes().Get("k8s.pod.uid"); ok {
						forked[v.Str()] = struct{}{}
					}
				}
			}
		}
	}
	if enriched != objects {
		t.Fatalf("enriched groups = %d, want %d: the payload no longer admits every object first", enriched, objects)
	}
	if len(forked) == 0 {
		t.Fatal("no fat-metric point landed on the overflow fallback: the payload no longer exercises the mid-push byte bind")
	}
	if got := obs.Ingested.WithLabelValues("split_capped").Value() - capped; got != float64(len(forked)) {
		t.Errorf("split_capped delta = %v, want %d: each grouped object whose refused shell folded to overflow must count exactly once", got, len(forked))
	}
}

// --- a mid-push bind on an already-grouped id ---

// Mid-push byte-budget exhaustion for an id that ALREADY has a group: the
// budget refuses the new (scope, metric) shell a later metric's point needs,
// so that point folds into the stripped overflow resource while the group's
// existing shells keep taking points. admit() used to leave that refusal
// uncounted, behind a comment claiming the points "still land on their own
// enriched resource" — the degradation was real and invisible.

func TestMidPushByteBudgetOnGroupedIDCountsAndFoldsToOverflow(t *testing.T) {
	capped := obs.Ingested.WithLabelValues("split_capped").Value()
	meta := &fakeMeta{pods: map[string]*kubemeta.Pod{
		"ksm-uid": {Name: "ksm-0", Namespace: "monitoring", UID: "ksm-uid", NodeName: "node1"},
		"uid-1":   {Name: "app-1", Namespace: "apps", UID: "uid-1", NodeName: "node1"},
	}}
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	a := rm.Resource().Attributes()
	a.PutStr("k8s.pod.uid", "ksm-uid")
	a.PutStr("service.name", "kube-state-metrics")
	// One resource copy exceeds the whole byte budget on its own, so the budget
	// binds immediately after the FIRST group is admitted and charged. (pcommon
	// CopyTo shares string bytes, so the big value costs headers, not copies.)
	a.PutStr("pad", strings.Repeat("x", maxSplitCopyBytes+1))
	sm := rm.ScopeMetrics().AppendEmpty()
	first := sm.Metrics().AppendEmpty()
	first.SetName("kube_pod_info")
	dps := first.SetEmptyGauge().DataPoints()
	for range 2 {
		dp := dps.AppendEmpty()
		dp.SetIntValue(1)
		dp.Attributes().PutStr("k8s.pod.uid", "uid-1")
	}
	// A SECOND metric for the SAME described object, routed after the budget
	// has bound: its point needs a new metric shell in uid-1's group, which the
	// byte budget refuses.
	second := sm.Metrics().AppendEmpty()
	second.SetName("kube_pod_status_ready")
	dp := second.SetEmptyGauge().DataPoints().AppendEmpty()
	dp.SetIntValue(1)
	dp.Attributes().PutStr("k8s.pod.uid", "uid-1")

	out := NewEnricher(Config{Meta: meta, MetricsMode: MetricsDatapoint}).
		EnrichMetrics(context.Background(), md)

	if got := out.DataPointCount(); got != 3 {
		t.Fatalf("forwarded %d points, want all 3 (a refused shell folds, it must not drop)", got)
	}

	var groupPoints, overflowPoints int
	rms := out.ResourceMetrics()
	for i := 0; i < rms.Len(); i++ {
		orm := rms.At(i)
		ra := orm.Resource().Attributes()
		enriched := false
		if v, ok := ra.Get("k8s.pod.name"); ok && v.Str() == "app-1" {
			enriched = true
		}
		sms := orm.ScopeMetrics()
		for j := 0; j < sms.Len(); j++ {
			ms := sms.At(j).Metrics()
			for k := 0; k < ms.Len(); k++ {
				m := ms.At(k)
				n := m.Gauge().DataPoints().Len()
				switch m.Name() {
				case "kube_pod_info":
					// The group's EXISTING shell keeps taking points past the
					// bind: both land on the enriched resource.
					if !enriched {
						t.Errorf("kube_pod_info points landed on an unenriched resource; the existing group must keep taking them")
					}
					groupPoints += n
				case "kube_pod_status_ready":
					// The refused shell's point folds into the stripped
					// overflow fallback — unenriched, sender identity gone, its
					// own id kept for downstream re-resolution.
					if enriched {
						t.Errorf("kube_pod_status_ready landed on the described object's group; the byte budget should have refused its shell")
					}
					if v, ok := ra.Get("service.name"); ok {
						t.Errorf("the overflow resource carries the sender's service.name=%q; it must be stripped", v.Str())
					}
					if _, ok := m.Gauge().DataPoints().At(0).Attributes().Get("k8s.pod.uid"); !ok {
						t.Error("the folded point lost its own id attribute")
					}
					overflowPoints += n
				}
			}
		}
	}
	if groupPoints != 2 || overflowPoints != 1 {
		t.Fatalf("placement: %d points on the enriched group (want 2), %d on the overflow fallback (want 1)", groupPoints, overflowPoints)
	}
	// The degradation is COUNTED: uid-1 tallies split_capped exactly once even
	// though it has its own group — its series forked onto the overflow
	// resource, which is what the counter's outcome reports.
	if got := obs.Ingested.WithLabelValues("split_capped").Value() - capped; got != 1 {
		t.Fatalf("split_capped delta = %v, want 1 (the grouped id's refused shell must be counted once)", got)
	}
}
