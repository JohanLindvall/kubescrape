package otlpingest

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/plog/plogotlp"
	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/JohanLindvall/kubescrape/internal/agent/logscrub"
	"github.com/JohanLindvall/kubescrape/internal/obs"
	"github.com/JohanLindvall/kubescrape/internal/testrace"
	"github.com/JohanLindvall/kubescrape/pkg/kubemeta"
)

// fakeMeta resolves a fixed set of container IDs and pod UIDs.
// fakeMeta resolves a fixed set of IDs. It is read-only after construction so
// it is safe to share across concurrent enrichers (see TestEnricherConcurrent).
type fakeMeta struct {
	containers map[string]*kubemeta.ContainerMetadata
	pods       map[string]*kubemeta.Pod
	podsByIP   map[string]*kubemeta.Pod
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

func (f *fakeMeta) PodByIP(_ context.Context, ip string) (*kubemeta.Pod, error) {
	if p, ok := f.podsByIP[ip]; ok {
		return p, nil
	}
	return nil, fmt.Errorf("pod ip %s not found", ip)
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
		podsByIP: map[string]*kubemeta.Pod{
			"10.1.2.3": {Name: "web-3", Namespace: "default", UID: "pod-uid-3", NodeName: "node1"},
		},
	}
}

func newEnricher(m MetadataSource, mode MetricsMode) *Enricher {
	return NewEnricher(Config{Meta: m, MetricsMode: mode})
}

// Enrichment is LINEAR in the resolved pod's label count and the sender's
// resource width. The merge used to Put the built attributes one key at a
// time, each Put scanning the sender's resource — O(|built| x (|dst|+|built|))
// — and both sides are tenant-authored: a pod's labels are bounded only by the
// API server's object size, and the merge runs once per resource of every
// push, inside the handler's in-flight slot. A few thousand resources naming a
// many-labelled pod held that slot for minutes. Coarse on purpose: the linear
// merge does this in tens of milliseconds, the quadratic one in tens of
// seconds.
func TestEnrichmentIsLinearInThePodsLabelCount(t *testing.T) {
	if testrace.Enabled {
		t.Skip("the race detector multiplies every map operation's cost")
	}
	const labels, resources = 20_000, 3
	pod := kubemeta.Pod{Name: "web-1", Namespace: "default", UID: "pod-uid-1", NodeName: "node1",
		Labels: make(map[string]string, labels)}
	for i := range labels {
		pod.Labels[fmt.Sprintf("label-%d", i)] = "v"
	}
	meta := &fakeMeta{containers: map[string]*kubemeta.ContainerMetadata{
		"cafe01": {Container: kubemeta.Container{Name: "app", ID: "containerd://cafe01"}, Pod: pod},
	}}
	ld := plog.NewLogs()
	for range resources {
		rl := ld.ResourceLogs().AppendEmpty()
		rl.Resource().Attributes().PutStr("container.id", "cafe01")
		rl.ScopeLogs().AppendEmpty().LogRecords().AppendEmpty().Body().SetStr("hi")
	}
	start := time.Now()
	newEnricher(meta, MetricsAuto).EnrichLogs(context.Background(), ld)
	if d := time.Since(start); d > 3*time.Second {
		t.Fatalf("enriching %d resources naming a pod with %d labels took %v: the merge is quadratic again",
			resources, labels, d)
	}
	a := ld.ResourceLogs().At(resources - 1).Resource().Attributes()
	if a.Len() < labels {
		t.Fatalf("the enriched resource carries %d attributes, want at least the %d labels", a.Len(), labels)
	}
	if v, _ := a.Get("k8s.namespace.name"); v.Str() != "default" {
		t.Errorf("k8s.namespace.name = %q after the bulk merge, want the resolved default", v.Str())
	}
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

// The sender is authoritative about what it CALLS itself and about the id it
// was resolved BY; kubescrape is authoritative about the identity it resolved.
// The split is attrs.ReservedIdentity minus the lookup keys — see mergeAttrs,
// where the security argument lives.
func TestEnrichKeepsSenderDescriptionAndOwnsResolvedIdentity(t *testing.T) {
	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	a := rl.Resource().Attributes()
	a.PutStr("container.id", "cafe01")           // the lookup input: stays verbatim
	a.PutStr("service.name", "checkout")         // descriptive: the sender's to choose
	a.PutStr("deployment.environment", "canary") // not identity at all
	a.PutStr("k8s.pod.name", "sender-chosen")    // resolved identity: ours
	rl.ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()

	newEnricher(newMeta(), MetricsAuto).EnrichLogs(context.Background(), ld)

	for k, want := range map[string]string{
		"container.id":           "cafe01",
		"service.name":           "checkout",
		"deployment.environment": "canary",
		"k8s.pod.name":           "web-1",
	} {
		if v, _ := a.Get(k); v.Str() != want {
			t.Errorf("%s = %q, want %q", k, v.Str(), want)
		}
	}
}

// Only the lookup key the resolution was made BY is the sender's to keep. When
// container.id resolved, a k8s.pod.uid beside it is a claim this receiver can
// check against the pod it just read — and one naming a DIFFERENT pod used to
// survive, shipping one resource that named two pods (the resolved name and
// namespace beside a foreign uid). The same rule on the split path's
// sender-own group, where the resource's own id is the lookup input.
func TestOnlyTheResolvingLookupKeyIsExemptFromTheResolvedIdentity(t *testing.T) {
	t.Run("logs", func(t *testing.T) {
		ld := plog.NewLogs()
		rl := ld.ResourceLogs().AppendEmpty()
		a := rl.Resource().Attributes()
		a.PutStr("container.id", "cafe01")
		a.PutStr("k8s.pod.uid", "victim-uid-9")
		rl.ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()

		newEnricher(newMeta(), MetricsAuto).EnrichLogs(context.Background(), ld)

		if v, _ := a.Get("k8s.pod.uid"); v.Str() != "pod-uid-1" {
			t.Errorf("k8s.pod.uid = %q, want the resolved pod-uid-1: container.id resolved, so the uid is "+
				"not the lookup input and a mismatched one is a claim to correct", v.Str())
		}
		if v, _ := a.Get("container.id"); v.Str() != "cafe01" {
			t.Errorf("container.id = %q, want the sender's cafe01 verbatim: it IS the lookup input", v.Str())
		}
	})
	t.Run("split", func(t *testing.T) {
		md := gaugeMetrics(map[string]string{"container.id": "cafe01", "k8s.pod.uid": "victim-uid-9"},
			map[string]any{"path": "/a"})
		out := newEnricher(newMeta(), MetricsDatapoint).EnrichMetrics(context.Background(), md)
		a := out.ResourceMetrics().At(0).Resource().Attributes()
		if v, _ := a.Get("k8s.pod.uid"); v.Str() != "pod-uid-1" {
			t.Errorf("k8s.pod.uid = %q, want the resolved pod-uid-1", v.Str())
		}
		if v, _ := a.Get("container.id"); v.Str() != "cafe01" {
			t.Errorf("container.id = %q, want the sender's cafe01 verbatim", v.Str())
		}
	})
	// The other direction: a pod-uid resolution keeps the sender's uid (its
	// input), and a container.id it could not resolve is left alone — a pod
	// build names no container to correct it with.
	t.Run("pod uid resolved", func(t *testing.T) {
		ld := plog.NewLogs()
		rl := ld.ResourceLogs().AppendEmpty()
		a := rl.Resource().Attributes()
		a.PutStr("container.id", "stale")
		a.PutStr("k8s.pod.uid", "pod-uid-2")
		rl.ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()

		newEnricher(newMeta(), MetricsAuto).EnrichLogs(context.Background(), ld)

		for k, want := range map[string]string{"k8s.pod.uid": "pod-uid-2", "container.id": "stale", "k8s.pod.name": "web-2"} {
			if v, _ := a.Get(k); v.Str() != want {
				t.Errorf("%s = %q, want %q", k, v.Str(), want)
			}
		}
	})
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

	// Line enrichment runs in the server's applyLogChain (one bounded body
	// render shared with log-metrics and the rules), after the chain's scrub.
	s := NewServer(ServerConfig{
		Enricher:    NewEnricher(Config{Meta: newMeta()}),
		EnrichLines: true,
		Exporter:    &captureExporter{},
	})
	s.cfg.Enricher.EnrichLogs(context.Background(), ld)
	s.applyLogChain(ld)
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

	s := NewServer(ServerConfig{
		Enricher:    NewEnricher(Config{Meta: newMeta()}),
		EnrichLines: true,
		Exporter:    &captureExporter{},
	})
	s.cfg.Enricher.EnrichLogs(context.Background(), ld)
	s.applyLogChain(ld)
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
	e := NewEnricher(Config{Meta: newMeta(), MetricsMode: MetricsAuto})
	const workers = 32
	var wg sync.WaitGroup
	for w := range workers {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for range 50 {
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

// A group keyed by a point-level ID must not inherit the source resource's
// own ID attributes — they name a different object.
func TestSplitStripsForeignResourceID(t *testing.T) {
	md := gaugeMetrics(map[string]string{"container.id": "cafe01"},
		map[string]any{"k8s.pod.uid": "pod-uid-2"}, // its own object
		map[string]any{"path": "/x"},               // falls back to the resource's ID
	)
	out := newEnricher(newMeta(), MetricsDatapoint).EnrichMetrics(context.Background(), md)
	rms := out.ResourceMetrics()
	for i := 0; i < rms.Len(); i++ {
		a := rms.At(i).Resource().Attributes()
		pod, _ := a.Get("k8s.pod.name")
		cid, hasCID := a.Get("container.id")
		switch pod.Str() {
		case "web-2": // point-ID group: the resource's container.id was foreign
			if hasCID {
				t.Errorf("web-2 group kept foreign container.id %q", cid.Str())
			}
		case "web-1": // fallback group: the resource's own ID is correct
			if !hasCID || cid.Str() != "cafe01" {
				t.Errorf("web-1 group lost its own container.id: %q", cid.Str())
			}
		default:
			t.Errorf("unexpected group %q: %v", pod.Str(), a.AsRaw())
		}
	}
	if rms.Len() != 2 {
		t.Fatalf("resources = %d, want 2", rms.Len())
	}
}

// countingMeta counts container lookups over the fake.
type countingMeta struct {
	*fakeMeta
	mu    sync.Mutex
	calls int
}

func (c *countingMeta) Container(ctx context.Context, id string, wait time.Duration) (*kubemeta.ContainerMetadata, error) {
	c.mu.Lock()
	c.calls++
	c.mu.Unlock()
	return c.fakeMeta.Container(ctx, id, wait)
}

// N resources sharing one ID in a single request do one metadata lookup and
// one attribute build (the per-request memo).
func TestEnrichLogsMemoizesPerRequest(t *testing.T) {
	meta := &countingMeta{fakeMeta: newMeta()}
	ld := plog.NewLogs()
	for range 3 {
		rl := ld.ResourceLogs().AppendEmpty()
		rl.Resource().Attributes().PutStr("container.id", "cafe01")
		rl.ScopeLogs().AppendEmpty().LogRecords().AppendEmpty().Body().SetStr("hi")
	}
	newEnricher(meta, MetricsAuto).EnrichLogs(context.Background(), ld)
	for i := range 3 {
		a := ld.ResourceLogs().At(i).Resource().Attributes()
		if v, _ := a.Get("k8s.pod.name"); v.Str() != "web-1" {
			t.Errorf("resource %d not enriched: %q", i, v.Str())
		}
	}
	if meta.calls != 1 {
		t.Errorf("container lookups = %d, want 1 (memoized per request)", meta.calls)
	}
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

// TestEnrichMetricsSplitAllTypes routes every OTLP metric type through the
// data-point splitter: sum, histogram, exponential histogram and summary
// points must land on their per-object resources with values intact.
func TestEnrichMetricsSplitAllTypes(t *testing.T) {
	md := pmetric.NewMetrics()
	sm := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty()
	sm.Scope().SetName("test-scope")

	sum := sm.Metrics().AppendEmpty()
	sum.SetName("s_total")
	s := sum.SetEmptySum()
	s.SetIsMonotonic(true)
	s.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
	sdp := s.DataPoints().AppendEmpty()
	sdp.SetDoubleValue(7)
	sdp.Attributes().PutStr("container.id", "cafe01")

	hist := sm.Metrics().AppendEmpty()
	hist.SetName("h")
	h := hist.SetEmptyHistogram()
	h.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
	hdp := h.DataPoints().AppendEmpty()
	hdp.SetCount(3)
	hdp.SetSum(1.5)
	hdp.ExplicitBounds().FromRaw([]float64{1, 2})
	hdp.BucketCounts().FromRaw([]uint64{1, 1, 1})
	hdp.Attributes().PutStr("container.id", "cafe01")

	exph := sm.Metrics().AppendEmpty()
	exph.SetName("eh")
	eh := exph.SetEmptyExponentialHistogram()
	eh.SetAggregationTemporality(pmetric.AggregationTemporalityDelta)
	ehdp := eh.DataPoints().AppendEmpty()
	ehdp.SetCount(2)
	ehdp.SetScale(1)
	ehdp.Attributes().PutStr("k8s.pod.uid", "pod-uid-2")

	summ := sm.Metrics().AppendEmpty()
	summ.SetName("q")
	qdp := summ.SetEmptySummary().DataPoints().AppendEmpty()
	qdp.SetCount(5)
	qdp.SetSum(2.5)
	qdp.Attributes().PutStr("k8s.pod.uid", "pod-uid-2")

	out := newEnricher(newMeta(), MetricsDatapoint).EnrichMetrics(context.Background(), md)

	byPod := map[string]map[string]pmetric.Metric{}
	rms := out.ResourceMetrics()
	for i := 0; i < rms.Len(); i++ {
		pod := "<none>"
		if v, ok := rms.At(i).Resource().Attributes().Get("k8s.pod.name"); ok {
			pod = v.Str()
		}
		if byPod[pod] == nil {
			byPod[pod] = map[string]pmetric.Metric{}
		}
		sms := rms.At(i).ScopeMetrics()
		for j := 0; j < sms.Len(); j++ {
			if sms.At(j).Scope().Name() != "test-scope" {
				t.Errorf("scope name lost: %q", sms.At(j).Scope().Name())
			}
			ms := sms.At(j).Metrics()
			for k := 0; k < ms.Len(); k++ {
				byPod[pod][ms.At(k).Name()] = ms.At(k)
			}
		}
	}

	w1 := byPod["web-1"]
	if len(w1) != 2 {
		t.Fatalf("web-1 metrics = %v", w1)
	}
	if m := w1["s_total"]; m.Type() != pmetric.MetricTypeSum || !m.Sum().IsMonotonic() ||
		m.Sum().AggregationTemporality() != pmetric.AggregationTemporalityCumulative ||
		m.Sum().DataPoints().At(0).DoubleValue() != 7 {
		t.Errorf("sum = %+v", m)
	}
	if m := w1["h"]; m.Type() != pmetric.MetricTypeHistogram ||
		m.Histogram().AggregationTemporality() != pmetric.AggregationTemporalityCumulative ||
		m.Histogram().DataPoints().At(0).Count() != 3 ||
		m.Histogram().DataPoints().At(0).ExplicitBounds().Len() != 2 {
		t.Errorf("histogram = %+v", m)
	}

	w2 := byPod["web-2"]
	if len(w2) != 2 {
		t.Fatalf("web-2 metrics = %v", w2)
	}
	if m := w2["eh"]; m.Type() != pmetric.MetricTypeExponentialHistogram ||
		m.ExponentialHistogram().AggregationTemporality() != pmetric.AggregationTemporalityDelta ||
		m.ExponentialHistogram().DataPoints().At(0).Count() != 2 ||
		m.ExponentialHistogram().DataPoints().At(0).Scale() != 1 {
		t.Errorf("exponential histogram = %+v", m)
	}
	if m := w2["q"]; m.Type() != pmetric.MetricTypeSummary ||
		m.Summary().DataPoints().At(0).Count() != 5 ||
		m.Summary().DataPoints().At(0).Sum() != 2.5 {
		t.Errorf("summary = %+v", m)
	}
}

// TestSplitCountsUnresolved: in split mode a group whose points carry no ID
// and whose peer IP resolves to nothing is forwarded unenriched — the
// resource-mode path counts that as "unresolved" and the split path must too,
// or the ingest counters silently under-report unattributed data.
func TestSplitCountsUnresolved(t *testing.T) {
	e := NewEnricher(Config{Meta: &fakeMeta{}, MetricsMode: MetricsDatapoint})
	before := obs.Ingested.WithLabelValues("unresolved").Value()

	md := pmetric.NewMetrics()
	dp := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().
		Metrics().AppendEmpty().SetEmptyGauge().DataPoints().AppendEmpty()
	dp.SetIntValue(1)

	out := e.EnrichMetrics(context.Background(), md)
	if out.DataPointCount() != 1 {
		t.Fatalf("data points = %d, want 1 (unresolved points must still be forwarded)", out.DataPointCount())
	}
	if got := obs.Ingested.WithLabelValues("unresolved").Value() - before; got != 1 {
		t.Fatalf("kubescrape_ingest_resources_total{unresolved} delta = %v, want 1", got)
	}
}

// forwardScrubbed pushes ld through the receiver's real log path with only the
// scrubber configured (no line enrichment, rules, metrics or lifts) and returns
// once it has been forwarded; the records are scrubbed in place.
func forwardScrubbed(t *testing.T, scrub *logscrub.Scrubber, ld plog.Logs) {
	t.Helper()
	exp := &captureExporter{}
	s := NewServer(ServerConfig{Enricher: NewEnricher(Config{Meta: &fakeMeta{}}), Scrub: scrub, Exporter: exp})
	if err := s.forwardLogs(context.Background(), ld); err != nil {
		t.Fatal(err)
	}
	if len(exp.logs) != 1 {
		t.Fatalf("the push was not forwarded (%d exports)", len(exp.logs))
	}
}

// Scrubbing is the chain's FIRST per-record step and is unconditional: it runs
// whatever else is configured, and on a body of any size — the chain's size
// bound (maxChainBodyBytes) limits what the lift, enrichment, metrics and
// rules READ, never what is redacted before the record is forwarded.
func TestScrubRedactsEveryBodyWhateverTheChainConfiguration(t *testing.T) {
	scrub, err := logscrub.New(logscrub.Config{Builtin: []string{"defaults"}})
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name   string
		enrich bool
	}{{"scrub only", false}, {"scrub with line enrichment", true}} {
		for _, size := range []int{64, maxChainBodyBytes + 1} {
			ld := plog.NewLogs()
			lr := ld.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()
			lr.Body().SetStr(strings.Repeat("x", size) + " password=hunter2")
			exp := &captureExporter{}
			s := NewServer(ServerConfig{
				Enricher:    NewEnricher(Config{Meta: &fakeMeta{}}),
				Scrub:       scrub,
				EnrichLines: tc.enrich,
				Exporter:    exp,
			})
			if err := s.forwardLogs(context.Background(), ld); err != nil {
				t.Fatal(err)
			}
			if len(exp.logs) != 1 {
				t.Fatalf("%s, %d-byte body: not forwarded", tc.name, size)
			}
			got := exp.logs[0].ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0).Body().Str()
			if strings.Contains(got, "hunter2") {
				t.Errorf("%s, %d-byte body: the secret was forwarded unredacted", tc.name, size)
			}
		}
	}
}

// A structured body — what the OTel logging SDKs and the collector's
// json_parser emit — must be redacted like a raw line. Scrubbing only string
// bodies meant the identical message was scrubbed on the tailer path and
// shipped in clear when an SDK sent it as a kvlist.
func TestScrubStructuredLogBody(t *testing.T) {
	scrub, err := logscrub.New(logscrub.Config{Builtin: []string{"defaults"}})
	if err != nil {
		t.Fatal(err)
	}
	ld := plog.NewLogs()
	lr := ld.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()
	body := lr.Body().SetEmptyMap()
	body.PutStr("msg", "auth failed")
	body.PutStr("authorization", "Bearer eyJhbGciOiJIUzI1NiJ9.SECRETPAYLOAD")
	nested := body.PutEmptyMap("ctx")
	nested.PutStr("password", "hunter2")
	// A slice element has no key of its own, so it INHERITS the key of the
	// entry holding it — the only thing that can make a bare value judgable.
	// The elements here match nothing standalone (they are opaque strings, not
	// bearer tokens or AWS key ids): they redact solely because the enclosing
	// key is probed with them, which is the behaviour under test. An element
	// spelled `api_key=sk-1` would match the secret-kv pattern on its own and
	// pass with the inheritance removed entirely.
	keys := body.PutEmptySlice("api_key")
	keys.AppendEmpty().SetStr("sk-live-abc123")
	keys.AppendEmpty().SetStr("sk-live-def456")
	// ...while an ordinary list under an ordinary key must survive intact.
	list := body.PutEmptySlice("args")
	list.AppendEmpty().SetStr("--verbose")

	forwardScrubbed(t, scrub, ld)

	got := lr.Body().Map()
	for _, tc := range []struct{ path, want string }{
		{"authorization", "[REDACTED]"},
	} {
		v, _ := got.Get(tc.path)
		if !strings.Contains(v.Str(), tc.want) {
			t.Errorf("body[%s] = %q; want it redacted", tc.path, v.Str())
		}
	}
	ctx, _ := got.Get("ctx")
	pw, _ := ctx.Map().Get("password")
	if !strings.Contains(pw.Str(), "[REDACTED]") {
		t.Errorf("nested password = %q; want it redacted", pw.Str())
	}
	keyList, _ := got.Get("api_key")
	for i := 0; i < keyList.Slice().Len(); i++ {
		if el := keyList.Slice().At(i).Str(); !strings.Contains(el, "[REDACTED]") {
			t.Errorf("api_key[%d] = %q; a slice element must be probed under the key of the entry holding it, or every credential sent as a list ships in clear", i, el)
		}
	}
	argList, _ := got.Get("args")
	if el := argList.Slice().At(0).Str(); el != "--verbose" {
		t.Errorf("args[0] = %q; an ordinary list element must survive intact", el)
	}
	if msg, _ := got.Get("msg"); msg.Str() != "auth failed" {
		t.Errorf("msg = %q; an ordinary field must survive", msg.Str())
	}
}

// auto mode must not be fooled by a resource-level container.id. Every SDK
// container detector sets one, and it is in the default container-id keys, so
// an exporter that DESCRIBES other objects has a resource ID naming itself
// while each data point names a different pod. Enriching from the resource
// there stamps every described object with the exporter's own identity.
func TestAutoModeSplitsWhenDataPointsCarryIdentity(t *testing.T) {
	meta := &fakeMeta{
		pods: map[string]*kubemeta.Pod{
			"pod-a": {Name: "web-a", Namespace: "apps", UID: "pod-a"},
			"pod-b": {Name: "web-b", Namespace: "apps", UID: "pod-b"},
		},
		containers: map[string]*kubemeta.ContainerMetadata{
			"exporter-cid": {ContainerID: "exporter-cid", Pod: kubemeta.Pod{Name: "ksm-0", Namespace: "monitoring", UID: "ksm-uid"}},
		},
	}
	e := NewEnricher(Config{Meta: meta}) // mode unset => auto

	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr("container.id", "exporter-cid") // the SENDER
	g := rm.ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
	g.SetName("kube_pod_status_ready")
	gauge := g.SetEmptyGauge() // once: SetEmptyGauge resets the points
	for _, uid := range []string{"pod-a", "pod-b"} {
		dp := gauge.DataPoints().AppendEmpty()
		dp.Attributes().PutStr("k8s.pod.uid", uid)
		dp.SetDoubleValue(1)
	}

	out := e.EnrichMetrics(context.Background(), md)
	if out.ResourceMetrics().Len() < 2 {
		t.Fatalf("auto produced %d resources; the described pods must not be collapsed onto the sender's",
			out.ResourceMetrics().Len())
	}
	names := map[string]bool{}
	for i := 0; i < out.ResourceMetrics().Len(); i++ {
		if v, ok := out.ResourceMetrics().At(i).Resource().Attributes().Get("k8s.pod.name"); ok {
			names[v.Str()] = true
		}
	}
	for _, want := range []string{"web-a", "web-b"} {
		if !names[want] {
			t.Errorf("no resource for described pod %s; got %v", want, names)
		}
	}
	if names["ksm-0"] && len(names) == 1 {
		t.Error("every point was attributed to the exporter's own pod")
	}
}

// A user rule whose replacement carries no '=' (the default [REDACTED] does
// not) must still redact a structured body's value. The first version wrote
// back only when it could split the scrubbed probe on '=', so these fell
// through untouched — after Scrub had already counted the redaction.
func TestScrubStructuredBodyWithPlainReplacement(t *testing.T) {
	scrub, err := logscrub.New(logscrub.Config{
		Rules: []logscrub.Rule{{Name: "ssn", Regexp: `[0-9]{3}-[0-9]{2}-[0-9]{4}`}},
	})
	if err != nil {
		t.Fatal(err)
	}
	ld := plog.NewLogs()
	lr := ld.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()
	body := lr.Body().SetEmptyMap()
	body.PutStr("ssn", "123-45-6789")
	body.PutStr("note", "no secret here")

	forwardScrubbed(t, scrub, ld)

	got, _ := lr.Body().Map().Get("ssn")
	if strings.Contains(got.Str(), "123-45-6789") {
		t.Errorf("secret shipped in clear: %q", got.Str())
	}
	if note, _ := lr.Body().Map().Get("note"); note.Str() != "no secret here" {
		t.Errorf("ordinary field rewritten: %q", note.Str())
	}
}

// PeerReject is the guard on the one attribution that can be confidently wrong.
//
// The connection's source address names the SENDER exactly once — on the hop the
// sender itself opened. A proxy, a service mesh that terminates the connection,
// or an internal hop addressed to the wrong port all present their own address,
// and on a receiver deployed as its own workload that address usually belongs to
// that workload. Attributing an application's telemetry to the receiver's own
// pod would be wrong on every resource, plausible-looking, and invisible.
//
// So a vetoed resolution must (a) enrich nothing, (b) count under its OWN
// outcome, and (c) NOT also count as `unresolved` — that counter is per
// resource, and double-tallying it would hide the rejection inside a number
// operators already read as ordinary.
func TestPeerRejectRefusesAndCountsSeparately(t *testing.T) {
	var asked []string
	enr := NewEnricher(Config{
		Meta:           newMeta(),
		PeerIPFallback: true,
		PeerReject: func(pod *kubemeta.Pod) bool {
			asked = append(asked, pod.Name)
			return pod.Name == "web-3"
		},
	})
	ld := plog.NewLogs()
	ld.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords().AppendEmpty().Body().SetStr("hi")

	rejected := obs.Ingested.WithLabelValues("peer_ip_rejected").Value()
	unresolved := obs.Ingested.WithLabelValues("unresolved").Value()
	accepted := obs.Ingested.WithLabelValues("peer_ip").Value()

	enr.EnrichLogs(withPeerIP(context.Background(), "10.1.2.3:41234"), ld)

	if n := ld.ResourceLogs().At(0).Resource().Attributes().Len(); n != 0 {
		t.Fatalf("a vetoed peer still enriched the resource (%d attrs): an application's telemetry would carry the receiver's own identity",
			n)
	}
	if len(asked) != 1 || asked[0] != "web-3" {
		t.Errorf("PeerReject was consulted with %v, want exactly the resolved pod", asked)
	}
	if got := obs.Ingested.WithLabelValues("peer_ip_rejected").Value() - rejected; got != 1 {
		t.Errorf("peer_ip_rejected moved by %v, want 1", got)
	}
	if got := obs.Ingested.WithLabelValues("unresolved").Value() - unresolved; got != 0 {
		t.Errorf("unresolved also moved by %v: a rejection must not be counted twice", got)
	}
	if got := obs.Ingested.WithLabelValues("peer_ip").Value() - accepted; got != 0 {
		t.Errorf("peer_ip moved by %v on a rejected attribution", got)
	}
}

// The veto is memoised with the rest of the per-request cache: the peer is a
// property of the CONNECTION, so a 500-resource payload must consult it once,
// not 500 times (and /v1/pod-ips is deliberately uncacheable).
func TestPeerRejectIsResolvedOncePerRequest(t *testing.T) {
	calls := 0
	enr := NewEnricher(Config{
		Meta:           newMeta(),
		PeerIPFallback: true,
		PeerReject:     func(*kubemeta.Pod) bool { calls++; return true },
	})
	ld := plog.NewLogs()
	for range 50 {
		ld.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords().AppendEmpty().Body().SetStr("hi")
	}
	enr.EnrichLogs(withPeerIP(context.Background(), "10.1.2.3:41234"), ld)
	if calls != 1 {
		t.Errorf("PeerReject consulted %d times for one request", calls)
	}
	rls := ld.ResourceLogs()
	for i := 0; i < rls.Len(); i++ {
		if n := rls.At(i).Resource().Attributes().Len(); n != 0 {
			t.Fatalf("resource %d was enriched despite the veto", i)
		}
	}
}

// A nil PeerReject accepts everything — the node-local case, where the peer is a
// pod on this node by construction.
func TestPeerRejectNilAcceptsEverything(t *testing.T) {
	enr := NewEnricher(Config{Meta: newMeta(), PeerIPFallback: true})
	ld := plog.NewLogs()
	ld.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords().AppendEmpty().Body().SetStr("hi")
	enr.EnrichLogs(withPeerIP(context.Background(), "10.1.2.3:41234"), ld)
	if v, ok := ld.ResourceLogs().At(0).Resource().Attributes().Get("k8s.pod.name"); !ok || v.Str() != "web-3" {
		t.Fatalf("a nil PeerReject blocked an ordinary attribution: %v",
			ld.ResourceLogs().At(0).Resource().Attributes().AsRaw())
	}
}

// gaugeWith builds one ResourceMetrics with the given resource attrs and one
// gauge point carrying the given point attrs.
func gaugeWith(resAttrs, pointAttrs map[string]string) pmetric.Metrics {
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	for k, v := range resAttrs {
		rm.Resource().Attributes().PutStr(k, v)
	}
	dp := rm.ScopeMetrics().AppendEmpty().Metrics().AppendEmpty().SetEmptyGauge().DataPoints().AppendEmpty()
	dp.SetDoubleValue(1)
	for k, v := range pointAttrs {
		dp.Attributes().PutStr(k, v)
	}
	return md
}

func resAttrsOf(md pmetric.Metrics) map[string]any {
	return md.ResourceMetrics().At(0).Resource().Attributes().AsRaw()
}

// auto mode decides between the resource path and the split path by asking
// whether any data point names a FOREIGN object. That question was answered by
// comparing id TOKENS, which got it wrong twice — and both ways destroy the
// sender's own identity, because the split path either clears the copied
// resource (unresolvable group) or overwrites it with the derived identity.
func TestAutoModeKeepsTheSendersOwnIdentity(t *testing.T) {
	meta := &fakeMeta{
		containers: map[string]*kubemeta.ContainerMetadata{
			"cafe01": {Container: kubemeta.Container{Name: "app", ID: "containerd://cafe01"},
				Pod: kubemeta.Pod{Name: "web-1", Namespace: "default", UID: "pod-uid-1", NodeName: "node1"}},
		},
		pods: map[string]*kubemeta.Pod{
			// The SAME pod the container id above resolves to.
			"pod-uid-1": {Name: "web-1", Namespace: "default", UID: "pod-uid-1", NodeName: "node1"},
		},
	}

	for _, tc := range []struct {
		name  string
		point map[string]string
		why   string
	}{
		{
			name:  "unresolvable point id",
			point: map[string]string{"container.id": "deadbeef"},
			why:   "an id that resolves to nothing is not evidence of a foreign object",
		},
		{
			name:  "the sender's own pod under a different id kind",
			point: map[string]string{"k8s.pod.uid": "pod-uid-1"},
			why:   "container.id and k8s.pod.uid naming ONE pod are not two objects",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := newEnricher(meta, MetricsAuto)
			md := gaugeWith(map[string]string{
				"container.id": "cafe01",
				"service.name": "my-chosen-name",
			}, tc.point)

			got := resAttrsOf(e.EnrichMetrics(context.Background(), md))
			if got["service.name"] != "my-chosen-name" {
				t.Errorf("the sender's service.name was destroyed (%v): %s", got["service.name"], tc.why)
			}
			if got["k8s.pod.name"] != "web-1" {
				t.Errorf("the sender was not enriched: k8s.pod.name = %v", got["k8s.pod.name"])
			}
		})
	}

	// ...and a genuinely foreign, RESOLVABLE object still splits.
	meta.pods["pod-uid-2"] = &kubemeta.Pod{Name: "other", Namespace: "default", UID: "pod-uid-2", NodeName: "node1"}
	e := newEnricher(meta, MetricsAuto)
	md := gaugeWith(map[string]string{"container.id": "cafe01", "service.name": "ksm"},
		map[string]string{"k8s.pod.uid": "pod-uid-2"})
	out := e.EnrichMetrics(context.Background(), md)
	if n := out.ResourceMetrics().Len(); n != 1 {
		t.Fatalf("want one split resource, got %d", n)
	}
	if got := resAttrsOf(out)["k8s.pod.name"]; got != "other" {
		t.Errorf("a genuinely foreign object must still be split out: k8s.pod.name = %v", got)
	}
}

// The split path must count a REJECTED peer attribution exactly as the
// resource path does. The ""-group's accounting was an open-coded copy of the
// resource path's (now enrichAttrs) that counted peer_ip and unresolved but
// nothing on a rejection — behind a comment claiming another site counted it — so
// -ingest-metrics-mode=datapoint (and any auto push demoted to split)
// under-reported the one counter Config.PeerReject's doc promises. Both sites
// now share one helper (peerFallback).
func TestSplitPathCountsRejectedPeer(t *testing.T) {
	e := NewEnricher(Config{
		Meta:           newMeta(),
		MetricsMode:    MetricsDatapoint,
		PeerIPFallback: true,
		PeerReject:     func(*kubemeta.Pod) bool { return true },
	})

	md := pmetric.NewMetrics()
	dp := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().
		Metrics().AppendEmpty().SetEmptyGauge().DataPoints().AppendEmpty()
	dp.SetIntValue(1) // no ID anywhere: the points land in the ""-group

	rejected := obs.Ingested.WithLabelValues("peer_ip_rejected").Value()
	unresolved := obs.Ingested.WithLabelValues("unresolved").Value()

	out := e.EnrichMetrics(withPeerIP(context.Background(), "10.1.2.3:41234"), md)

	if got := obs.Ingested.WithLabelValues("peer_ip_rejected").Value() - rejected; got != 1 {
		t.Errorf("peer_ip_rejected moved by %v on the split path, want 1", got)
	}
	if got := obs.Ingested.WithLabelValues("unresolved").Value() - unresolved; got != 0 {
		t.Errorf("unresolved also moved by %v: a rejection must not be counted twice", got)
	}
	if n := out.ResourceMetrics().At(0).Resource().Attributes().Len(); n != 0 {
		t.Errorf("a vetoed peer still enriched the split resource (%d attrs)", n)
	}
}

// A resolved sender keeps its whole OTLP service triple. service.namespace and
// service.instance.id are exempted from the receipt strip
// (senderControlledIdentity) for the same reason service.name is, and the
// resolved-wins overwrite has to mean the same thing or the exemption is a
// no-op on the far more common path: any sender this receiver CAN resolve.
//
// The concrete damage, which is why this is pinned rather than argued: those
// two keys are what attrs.Identity derives service.namespace/service.instance.id
// from, i.e. half the Prometheus job+instance pair. Overwriting them renames a
// resolved sender's job to <k8s-namespace>/<service.name> and pins its instance
// to the container ID — which changes on every container restart, minting a
// fresh series each time.
func TestResolvedSenderKeepsItsOwnServiceTriple(t *testing.T) {
	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	a := rl.Resource().Attributes()
	a.PutStr("container.id", "cafe01")
	a.PutStr("service.name", "checkout")
	a.PutStr("service.namespace", "shop")
	a.PutStr("service.instance.id", "checkout-abcde")
	rl.ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()

	newEnricher(newMeta(), MetricsAuto).EnrichLogs(context.Background(), ld)

	for k, want := range map[string]string{
		"service.name":        "checkout",
		"service.namespace":   "shop",
		"service.instance.id": "checkout-abcde",
	} {
		if v, _ := a.Get(k); v.Str() != want {
			t.Errorf("%s = %q, want %q: a resolved lookup must not take the sender's own service identity", k, v.Str(), want)
		}
	}
	// The resolved identity this receiver IS authoritative about still wins —
	// k8s.namespace.name above all, which is what route keys tenancy on.
	//
	// Note what that means beside TestResolvedNamespaceBeatsASenderSClaimForRouting,
	// which asserts the receiver's "two spellings of the same fact" agree: they
	// agree only while the sender declares neither. A sender that names its own
	// service.namespace makes them differ ON PURPOSE — service.namespace is an
	// OTLP grouping the sender owns, not a second spelling of the Kubernetes
	// namespace — and nothing keys on it, so the disagreement is inert.
	if v, _ := a.Get("k8s.pod.name"); v.Str() != "web-1" {
		t.Errorf("k8s.pod.name = %q, want web-1", v.Str())
	}
	if v, _ := a.Get("k8s.namespace.name"); v.Str() != "default" {
		t.Errorf("k8s.namespace.name = %q, want default: the routing key is still ours", v.Str())
	}
}

// The other half of the same decision: a sender that declares NEITHER still
// gets both filled from the resolution, so nothing regresses for a plain SDK.
// The exemption is "the sender stays authoritative", not "kubescrape stops
// deriving".
func TestResolvedSenderWithoutAServiceTripleStillGetsOne(t *testing.T) {
	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	a := rl.Resource().Attributes()
	a.PutStr("container.id", "cafe01")
	rl.ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()

	newEnricher(newMeta(), MetricsAuto).EnrichLogs(context.Background(), ld)

	for _, k := range []string{"service.namespace", "service.instance.id"} {
		if v, ok := a.Get(k); !ok || v.Str() == "" {
			t.Errorf("%s absent: fill-if-absent must still apply", k)
		}
	}
}

// The other arm of the same rule, pinned beside it so the asymmetry is a
// decision and not an accident. On the datapoint/split path a resource names an
// object OTHER than the sender, so the sender's service triple is its OWN and
// says nothing about the described object: split.go strips the whole of
// attrs.SenderIdentityKeys() — service.name, service.namespace and
// service.instance.id included — and overwrites with the described object's.
//
// "The sender is authoritative about ITSELF" is the same sentence in both
// places; only whose resource it is changes. Leaving the exporter's
// service.instance.id here would put every described object's series on the
// exporter's instance, which is the collision attrs.PrefixInstance exists to
// prevent one level up.
func TestSplitDescribedObjectDoesNotKeepTheSendersServiceTriple(t *testing.T) {
	meta := &fakeMeta{pods: map[string]*kubemeta.Pod{
		"pod-uid-2": {Name: "web-2", Namespace: "default", UID: "pod-uid-2", NodeName: "node1"},
	}}
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	ra := rm.Resource().Attributes()
	ra.PutStr("service.name", "kube-state-metrics")
	ra.PutStr("service.namespace", "monitoring")
	ra.PutStr("service.instance.id", "ksm-0")
	g := rm.ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
	g.SetName("kube_pod_status_ready")
	dp := g.SetEmptyGauge().DataPoints().AppendEmpty()
	dp.SetIntValue(1)
	dp.Attributes().PutStr("k8s.pod.uid", "pod-uid-2") // a DIFFERENT object

	out := newEnricher(meta, MetricsDatapoint).EnrichMetrics(context.Background(), md)

	var found bool
	for i := 0; i < out.ResourceMetrics().Len(); i++ {
		a := out.ResourceMetrics().At(i).Resource().Attributes()
		if v, _ := a.Get("k8s.pod.uid"); v.Str() != "pod-uid-2" {
			continue
		}
		found = true
		for _, k := range []string{"service.namespace", "service.instance.id"} {
			v, _ := a.Get(k)
			if v.Str() == "monitoring" || v.Str() == "ksm-0" {
				t.Errorf("%s = %q: the exporter's own service identity survived onto an object it merely describes", k, v.Str())
			}
		}
		if v, _ := a.Get("service.name"); v.Str() == "kube-state-metrics" {
			t.Errorf("service.name = %q: same", v.Str())
		}
	}
	if !found {
		t.Fatal("no resource for the described pod")
	}
}

// The residual documented at SenderIdentityStrip's lookup-key bullet, pinned as
// BEHAVIOUR so the prose and the code cannot drift apart.
//
// The identity strip stops a sender from DECLARING someone else's namespace.
// It does not stop it from being resolved into one: the metadata service's
// /v1/pods/{ns}/{name} is unauthenticated and hands out container IDs, and the
// container index is cluster-wide, so a stolen id plus no k8s.namespace.name of
// one's own yields the victim's namespace — which internal/agent/route keys
// tenancy on. The strip removes nothing here, because the forged value never
// rides the wire; it is derived, correctly, from a stolen input.
//
// This test asserts the CURRENT, deliberate answer. If a later change closes
// the hole it must fail — at which point the fix is to update it together with
// the three places that describe the residual (this bullet, the AGENTS.md
// ingest bullet, and kubescrape_ingest_identity_stripped_total's help text),
// never to delete it.
func TestStolenLookupIDStillResolvesTheVictimsNamespace(t *testing.T) {
	meta := &fakeMeta{containers: map[string]*kubemeta.ContainerMetadata{
		"victim01": {Container: kubemeta.Container{Name: "api", ID: "containerd://victim01"},
			Pod: kubemeta.Pod{Name: "api-0", Namespace: "payments", UID: "victim-uid", NodeName: "node9"}},
	}}
	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	a := rl.Resource().Attributes()
	a.PutStr("container.id", "victim01") // read off the unauthenticated /v1/pods
	rl.ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()

	newEnricher(meta, MetricsAuto).EnrichLogs(context.Background(), ld)

	v, ok := a.Get("k8s.namespace.name")
	if !ok || v.Str() != "payments" {
		t.Fatalf("k8s.namespace.name = %q (present=%v), want %q — the documented residual changed; "+
			"update SenderIdentityStrip's lookup-key bullet, the AGENTS.md ingest bullet and the "+
			"kubescrape_ingest_identity_stripped_total help text to match", v.Str(), ok, "payments")
	}
}

// scrubBody's contract is EVERY string leaf of a body, and a secret nested past
// the walk's depth bound used to ship in clear, uncounted: the bound was a flat
// 8, far inside what the wire guard admits. It is derived from that guard now,
// so the deepest leaf a PUSHED body can carry is scrubbed — pinned here both
// in-process (maps, three wire levels each) and over the wire at the exact
// limit (arrays, the cheapest nesting at two).
func TestDeepStructuredBodyIsScrubbed(t *testing.T) {
	scrub, err := logscrub.New(logscrub.Config{Builtin: []string{"defaults"}})
	if err != nil {
		t.Fatal(err)
	}
	for _, depth := range []int{9, (maxNestingDepth - 5) / 3} {
		ld := plog.NewLogs()
		lr := ld.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()
		m := lr.Body().SetEmptyMap()
		for i := 1; i < depth; i++ {
			m = m.PutEmptyMap("n")
		}
		m.PutStr("password", "hunter2") // the leaf sits at scrub depth `depth`
		forwardScrubbed(t, scrub, ld)
		if s := lr.Body().AsString(); strings.Contains(s, "hunter2") {
			t.Errorf("map depth %d: the secret survived the scrub: %.120s…", depth, s)
		}
	}

	// Over the wire: the deepest array chain the guard admits must decode, and
	// its leaf must be scrubbed; one level more must be refused at the door.
	leaf := protoField(nil, 1, []byte("password=hunter2")) // AnyValue{string_value}
	deepest := (maxNestingDepth - 5) / 2
	if err := checkNesting(logsRequestAround(nestedAnyValue(deepest+1, leaf))); err == nil {
		t.Fatalf("the wire guard admits a leaf at scrub depth %d; maxBodyScrubDepth no longer covers it", deepest+1)
	}
	body := logsRequestAround(nestedAnyValue(deepest, leaf))
	if err := checkNesting(body); err != nil {
		t.Fatalf("the wire guard refuses a leaf at scrub depth %d: %v", deepest, err)
	}
	req := plogotlp.NewExportRequest()
	if err := req.UnmarshalProto(body); err != nil {
		t.Fatal(err)
	}
	forwardScrubbed(t, scrub, req.Logs())
	v := req.Logs().ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0).Body()
	for range deepest {
		v = v.Slice().At(0)
	}
	if strings.Contains(v.Str(), "hunter2") {
		t.Errorf("a leaf at scrub depth %d — admitted by the wire guard — was not scrubbed: %q", deepest, v.Str())
	}
}

// waitGatedMeta resolves a container id only on a WAITED lookup — the metadata
// service's answer for an id the kubelet posts a moment after the push arrives
// — while pod uids resolve either way.
type waitGatedMeta struct{ *fakeMeta }

func (m waitGatedMeta) Container(ctx context.Context, id string, wait time.Duration) (*kubemeta.ContainerMetadata, error) {
	if wait <= 0 {
		return nil, fmt.Errorf("container %s not yet posted", id)
	}
	return m.fakeMeta.Container(ctx, id, wait)
}

// One payload is attributed by the SAME id whatever the metrics mode. A sender
// carrying both a container id and a pod uid gets container grain when the
// container id resolves — and "resolves" must be one question. Resource
// enrichment asked the waited attribution lookup while the split path asked a
// wait-free probe, so with -ingest-metadata-wait set, a container id the
// kubelet had not posted yet named the container in resource and auto mode and
// only the POD in datapoint mode: a different service.instance.id, no container
// name, for the identical push (resolvableToken is now the one chooser).
func TestTokenChoiceIsModeIndependent(t *testing.T) {
	base := newMeta()
	base.pods["pod-uid-1"] = &base.containers["cafe01"].Pod
	meta := waitGatedMeta{base}
	both := map[string]string{"container.id": "cafe01", "k8s.pod.uid": "pod-uid-1"}

	identity := func(a map[string]any) string {
		return fmt.Sprintf("service.instance.id=%v k8s.container.name=%v", a["service.instance.id"], a["k8s.container.name"])
	}
	const want = "service.instance.id=containerd://cafe01 k8s.container.name=app"

	for _, mode := range []MetricsMode{MetricsResource, MetricsDatapoint, MetricsAuto} {
		e := NewEnricher(Config{Meta: meta, MetricsMode: mode, Wait: 50 * time.Millisecond})
		// Both ids on the RESOURCE (every mode) and on the POINT (the split
		// path's per-point choice) must pick the same object.
		for _, shape := range []struct {
			name     string
			res, dpt map[string]string
		}{
			{"resource ids", both, map[string]string{"path": "/a"}},
			{"point ids", nil, both},
		} {
			if shape.res == nil && mode == MetricsResource {
				continue // resource mode reads no point id (auto demotes this shape to the split)
			}
			out := e.EnrichMetrics(context.Background(), gaugeWith(shape.res, shape.dpt))
			if n := out.ResourceMetrics().Len(); n != 1 {
				t.Fatalf("%s/%s: %d resources, want 1", mode, shape.name, n)
			}
			if got := identity(resAttrsOf(out)); got != want {
				t.Errorf("%s/%s: %s, want %s", mode, shape.name, got, want)
			}
		}
	}

	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	for k, v := range both {
		rl.Resource().Attributes().PutStr(k, v)
	}
	NewEnricher(Config{Meta: meta, Wait: 50 * time.Millisecond}).EnrichLogs(context.Background(), ld)
	if got := identity(rl.Resource().Attributes().AsRaw()); got != want {
		t.Errorf("logs: %s, want %s", got, want)
	}
}

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
