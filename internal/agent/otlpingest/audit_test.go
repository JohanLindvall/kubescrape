package otlpingest

import (
	"context"
	"testing"

	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

// audit_test.go: targeted tests from the 2026-07 audit.

// A resource carrying BOTH a container ID and a pod UID resolves via the
// container ID (documented in Config: container keys are checked first — a
// container ID names the exact incarnation, a pod UID does not).
func TestBothContainerIDAndPodUIDPrefersContainer(t *testing.T) {
	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	rl.Resource().Attributes().PutStr("k8s.pod.uid", "pod-uid-2") // would resolve web-2
	rl.Resource().Attributes().PutStr("container.id", "cafe01")   // resolves web-1 + container
	rl.ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()

	newEnricher(newMeta(), MetricsAuto).EnrichLogs(context.Background(), ld)

	a := rl.Resource().Attributes()
	if v, _ := a.Get("k8s.pod.name"); v.Str() != "web-1" {
		t.Errorf("k8s.pod.name = %q; container ID must take precedence over pod UID", v.Str())
	}
	if v, ok := a.Get("k8s.container.name"); !ok || v.Str() != "app" {
		t.Errorf("k8s.container.name = %q ok=%v; container-level enrichment lost", v.Str(), ok)
	}
}

// When the container ID is present but unresolvable (stale/garbage), the
// enricher falls back to a resolvable pod UID the sender also provided:
// container is still preferred when it resolves (see the precedence test
// above), but a dead container ID must not veto pod-level enrichment.
func TestUnresolvableContainerIDFallsBackToUID(t *testing.T) {
	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	rl.Resource().Attributes().PutStr("container.id", "unknown")
	rl.Resource().Attributes().PutStr("k8s.pod.uid", "pod-uid-2")
	rl.ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()

	newEnricher(newMeta(), MetricsAuto).EnrichLogs(context.Background(), ld)
	a := rl.Resource().Attributes()
	if v, ok := a.Get("k8s.pod.name"); !ok || v.Str() != "web-2" {
		t.Errorf("k8s.pod.name = %q ok=%v; expected UID fallback to web-2 after a container-ID miss", v.Str(), ok)
	}
	// A stale container ID must not leave a container name behind.
	if _, ok := a.Get("k8s.container.name"); ok {
		t.Error("gained a container name from an unresolvable container ID")
	}
}

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

// An ordinary sender that labels its own data points with its own container id
// must stay on the resource path in auto mode: the split path overwrites the
// sender's resource attributes with the derived ones, so demoting it changed
// service.name — the Prometheus job — for every app that adds such a label.
func TestAutoModeKeepsSelfLabelledSenderOnResourcePath(t *testing.T) {
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	ra := rm.Resource().Attributes()
	ra.PutStr("service.name", "checkout")
	ra.PutStr("container.id", "cafe01")
	g := rm.ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
	g.SetName("http_requests")
	dp := g.SetEmptyGauge().DataPoints().AppendEmpty()
	dp.SetIntValue(1)
	dp.Attributes().PutStr("container.id", "cafe01") // its OWN id, not a foreign object's

	out := newEnricher(newMeta(), MetricsAuto).EnrichMetrics(context.Background(), md)

	if n := out.ResourceMetrics().Len(); n != 1 {
		t.Fatalf("ResourceMetrics = %d; want 1 — the sender was regrouped by the splitter", n)
	}
	a := out.ResourceMetrics().At(0).Resource().Attributes()
	if v, _ := a.Get("service.name"); v.Str() != "checkout" {
		t.Errorf("service.name = %q; want checkout — the sender is authoritative about itself", v.Str())
	}
	if v, _ := a.Get("k8s.pod.name"); v.Str() != "web-1" {
		t.Errorf("k8s.pod.name = %q; want web-1 — the resource path still enriches", v.Str())
	}
}
