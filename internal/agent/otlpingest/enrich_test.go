package otlpingest

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/JohanLindvall/kubescrape/internal/kubemeta"
)

// fakeMeta resolves a fixed set of container IDs and pod UIDs.
// fakeMeta resolves a fixed set of IDs. It is read-only after construction so
// it is safe to share across concurrent enrichers (see TestEnricherConcurrent).
type fakeMeta struct {
	containers map[string]*kubemeta.ContainerMetadata
	pods       map[string]*kubemeta.Pod
}

func (f *fakeMeta) Container(_ context.Context, id string, _ time.Duration) (*kubemeta.ContainerMetadata, error) {
	if md, ok := f.containers[id]; ok {
		return md, nil
	}
	return nil, fmt.Errorf("container %s not found", id)
}

func (f *fakeMeta) PodByUID(_ context.Context, uid string) (*kubemeta.Pod, error) {
	if p, ok := f.pods[uid]; ok {
		return p, nil
	}
	return nil, fmt.Errorf("pod uid %s not found", uid)
}

func newMeta() *fakeMeta {
	return &fakeMeta{
		containers: map[string]*kubemeta.ContainerMetadata{
			"cafe01": {Container: kubemeta.Container{Name: "app", ID: "containerd://cafe01"},
				Pod: kubemeta.Pod{Name: "web-1", Namespace: "default", UID: "pod-uid-1", NodeName: "node1"}},
		},
		pods: map[string]*kubemeta.Pod{
			"pod-uid-2": {Name: "web-2", Namespace: "default", UID: "pod-uid-2", NodeName: "node1"},
		},
	}
}

func newEnricher(m MetadataSource, mode MetricsMode) *Enricher {
	return NewEnricher(Config{Meta: m, MetricsMode: mode})
}

func TestEnrichLogsByContainerID(t *testing.T) {
	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	rl.Resource().Attributes().PutStr("container.id", "cafe01")
	rl.ScopeLogs().AppendEmpty().LogRecords().AppendEmpty().Body().SetStr("hi")

	newEnricher(newMeta(), MetricsAuto).EnrichLogs(context.Background(), ld)

	a := rl.Resource().Attributes()
	if v, _ := a.Get("k8s.pod.name"); v.Str() != "web-1" {
		t.Errorf("k8s.pod.name = %q", v.Str())
	}
	if v, _ := a.Get("k8s.container.name"); v.Str() != "app" {
		t.Errorf("k8s.container.name = %q", v.Str())
	}
}

func TestEnrichLogsByPodUID(t *testing.T) {
	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	rl.Resource().Attributes().PutStr("k8s.pod.uid", "pod-uid-2")
	rl.ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()

	newEnricher(newMeta(), MetricsAuto).EnrichLogs(context.Background(), ld)
	if v, _ := rl.Resource().Attributes().Get("k8s.pod.name"); v.Str() != "web-2" {
		t.Errorf("k8s.pod.name = %q", v.Str())
	}
}

func TestEnrichNeverOverwrites(t *testing.T) {
	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	rl.Resource().Attributes().PutStr("container.id", "cafe01")
	rl.Resource().Attributes().PutStr("k8s.pod.name", "sender-chosen")
	rl.ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()

	newEnricher(newMeta(), MetricsAuto).EnrichLogs(context.Background(), ld)
	if v, _ := rl.Resource().Attributes().Get("k8s.pod.name"); v.Str() != "sender-chosen" {
		t.Errorf("overwrote sender attribute: %q", v.Str())
	}
}

func TestEnrichLogsUnresolvedUntouched(t *testing.T) {
	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	rl.Resource().Attributes().PutStr("container.id", "unknown")
	rl.ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()

	newEnricher(newMeta(), MetricsAuto).EnrichLogs(context.Background(), ld)
	if _, ok := rl.Resource().Attributes().Get("k8s.pod.name"); ok {
		t.Error("unresolved resource gained k8s attributes")
	}
}

func TestEnrichLogsLineEnrichment(t *testing.T) {
	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	lr := rl.ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()
	lr.Body().SetStr(`{"level":"error","@t":"2026-01-02T03:04:05Z","msg":"boom"}`)

	e := NewEnricher(Config{Meta: newMeta(), EnrichLines: true})
	e.EnrichLogs(context.Background(), ld)
	if lr.SeverityNumber() != plog.SeverityNumberError {
		t.Errorf("severity = %v (line enrichment not applied)", lr.SeverityNumber())
	}
	if lr.Timestamp() == 0 {
		t.Error("timestamp not set from line")
	}
}

func TestEnrichLogsLineEnrichmentRespectsSender(t *testing.T) {
	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	lr := rl.ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()
	lr.SetSeverityNumber(plog.SeverityNumberInfo)
	lr.Body().SetStr(`{"level":"error","msg":"boom"}`)

	NewEnricher(Config{Meta: newMeta(), EnrichLines: true}).EnrichLogs(context.Background(), ld)
	if lr.SeverityNumber() != plog.SeverityNumberInfo {
		t.Errorf("overrode sender severity: %v", lr.SeverityNumber())
	}
}

// gaugeMetrics builds a metrics payload with one gauge holding a point per
// (container.id label) entry.
func gaugeMetrics(resourceAttrs map[string]string, points ...map[string]any) pmetric.Metrics {
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	for k, v := range resourceAttrs {
		rm.Resource().Attributes().PutStr(k, v)
	}
	g := rm.ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
	g.SetName("app_requests")
	gauge := g.SetEmptyGauge()
	for _, p := range points {
		dp := gauge.DataPoints().AppendEmpty()
		dp.SetDoubleValue(1)
		for k, v := range p {
			dp.Attributes().PutStr(k, v.(string))
		}
	}
	return md
}

func collectPodNames(md pmetric.Metrics) map[string]int {
	out := map[string]int{}
	rms := md.ResourceMetrics()
	for i := 0; i < rms.Len(); i++ {
		name := "<none>"
		if v, ok := rms.At(i).Resource().Attributes().Get("k8s.pod.name"); ok {
			name = v.Str()
		}
		points := 0
		sms := rms.At(i).ScopeMetrics()
		for j := 0; j < sms.Len(); j++ {
			ms := sms.At(j).Metrics()
			for k := 0; k < ms.Len(); k++ {
				points += ms.At(k).Gauge().DataPoints().Len()
			}
		}
		out[name] += points
	}
	return out
}

func TestEnrichMetricsResourceMode(t *testing.T) {
	md := gaugeMetrics(map[string]string{"container.id": "cafe01"},
		map[string]any{"path": "/a"}, map[string]any{"path": "/b"})
	out := newEnricher(newMeta(), MetricsResource).EnrichMetrics(context.Background(), md)
	if got := collectPodNames(out); got["web-1"] != 2 {
		t.Errorf("resource-mode pod points = %+v", got)
	}
}

func TestEnrichMetricsDatapointSplit(t *testing.T) {
	// One incoming resource, points for two different containers/pods.
	md := gaugeMetrics(nil,
		map[string]any{"container.id": "cafe01"},
		map[string]any{"k8s.pod.uid": "pod-uid-2"},
		map[string]any{"container.id": "cafe01"},
		map[string]any{"container.id": "unknown"},
	)
	out := newEnricher(newMeta(), MetricsDatapoint).EnrichMetrics(context.Background(), md)
	got := collectPodNames(out)
	if got["web-1"] != 2 || got["web-2"] != 1 || got["<none>"] != 1 {
		t.Errorf("datapoint-split points = %+v", got)
	}
}

func TestEnrichMetricsAutoFallsBackToSplit(t *testing.T) {
	// No resource-level id → auto splits by data-point id.
	md := gaugeMetrics(nil,
		map[string]any{"container.id": "cafe01"},
		map[string]any{"k8s.pod.uid": "pod-uid-2"},
	)
	out := newEnricher(newMeta(), MetricsAuto).EnrichMetrics(context.Background(), md)
	got := collectPodNames(out)
	if got["web-1"] != 1 || got["web-2"] != 1 {
		t.Errorf("auto-split points = %+v", got)
	}
}

func TestEnrichMetricsAutoUsesResourceWhenPresent(t *testing.T) {
	md := gaugeMetrics(map[string]string{"container.id": "cafe01"},
		map[string]any{"path": "/a"})
	out := newEnricher(newMeta(), MetricsAuto).EnrichMetrics(context.Background(), md)
	if out.ResourceMetrics().Len() != 1 {
		t.Fatalf("auto should not split when resource has id: %d resources", out.ResourceMetrics().Len())
	}
	if v, _ := out.ResourceMetrics().At(0).Resource().Attributes().Get("k8s.pod.name"); v.Str() != "web-1" {
		t.Errorf("resource not enriched: %q", v.Str())
	}
}

// TestEnricherConcurrent exercises the enricher from many goroutines at once —
// the ingest gRPC/HTTP servers call it concurrently. Run it under
// `CGO_ENABLED=1 go test -race` to check for data races; without -race it still
// surfaces panics, deadlocks, or corrupted output.
func TestEnricherConcurrent(t *testing.T) {
	e := NewEnricher(Config{Meta: newMeta(), MetricsMode: MetricsAuto, EnrichLines: true})
	const workers = 32
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				ld := plog.NewLogs()
				rl := ld.ResourceLogs().AppendEmpty()
				rl.Resource().Attributes().PutStr("container.id", "cafe01")
				rl.ScopeLogs().AppendEmpty().LogRecords().AppendEmpty().Body().SetStr(`{"level":"warn"}`)
				e.EnrichLogs(context.Background(), ld)
				if v, ok := rl.Resource().Attributes().Get("k8s.pod.name"); !ok || v.Str() != "web-1" {
					t.Errorf("worker %d: enrichment = %v", w, v.AsRaw())
					return
				}

				md := gaugeMetrics(nil,
					map[string]any{"container.id": "cafe01"},
					map[string]any{"k8s.pod.uid": "pod-uid-2"})
				out := e.EnrichMetrics(context.Background(), md)
				if got := collectPodNames(out); got["web-1"] != 1 || got["web-2"] != 1 {
					t.Errorf("worker %d: split = %+v", w, got)
					return
				}
			}
		}(w)
	}
	wg.Wait()
}

func TestEnrichCustomIDKeys(t *testing.T) {
	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	rl.Resource().Attributes().PutStr("my.cid", "cafe01")
	rl.ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()

	e := NewEnricher(Config{Meta: newMeta(), ContainerIDKeys: []string{"my.cid"}})
	e.EnrichLogs(context.Background(), ld)
	if v, _ := rl.Resource().Attributes().Get("k8s.pod.name"); v.Str() != "web-1" {
		t.Errorf("custom container-id key not honored: %q", v.Str())
	}
}
