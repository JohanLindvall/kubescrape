package otlpingest

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/JohanLindvall/kubescrape/internal/agent/logscrub"
	"github.com/JohanLindvall/kubescrape/internal/obs"
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
	// render shared with log-metrics and the rules), after EnrichLogs' scrub.
	s := NewServer(ServerConfig{
		Enricher: NewEnricher(Config{Meta: newMeta(), EnrichLines: true}),
		Exporter: &captureExporter{},
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
		Enricher: NewEnricher(Config{Meta: newMeta(), EnrichLines: true}),
		Exporter: &captureExporter{},
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
	for i := 0; i < 3; i++ {
		rl := ld.ResourceLogs().AppendEmpty()
		rl.Resource().Attributes().PutStr("container.id", "cafe01")
		rl.ScopeLogs().AppendEmpty().LogRecords().AppendEmpty().Body().SetStr("hi")
	}
	newEnricher(meta, MetricsAuto).EnrichLogs(context.Background(), ld)
	for i := 0; i < 3; i++ {
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

// A structured body — what the OTel logging SDKs and the collector's
// json_parser emit — must be redacted like a raw line. Scrubbing only string
// bodies meant the identical message was scrubbed on the tailer path and
// shipped in clear when an SDK sent it as a kvlist.
func TestScrubStructuredLogBody(t *testing.T) {
	scrub, err := logscrub.New(logscrub.Config{Builtin: []string{"defaults"}})
	if err != nil {
		t.Fatal(err)
	}
	e := NewEnricher(Config{Scrub: scrub, Meta: &fakeMeta{}})

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

	e.EnrichLogs(context.Background(), ld)

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
	e := NewEnricher(Config{Scrub: scrub, Meta: &fakeMeta{}})

	ld := plog.NewLogs()
	lr := ld.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()
	body := lr.Body().SetEmptyMap()
	body.PutStr("ssn", "123-45-6789")
	body.PutStr("note", "no secret here")

	e.EnrichLogs(context.Background(), ld)

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
	for i := 0; i < 50; i++ {
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
// resource path does. The ""-group's accounting was an open-coded copy of
// applyMetadata's that counted peer_ip and unresolved but nothing on a
// rejection — behind a comment claiming another site counted it — so
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
// the three places that describe the residual (this bullet, the CLAUDE.md
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
			"update SenderIdentityStrip's lookup-key bullet, the CLAUDE.md ingest bullet and the "+
			"kubescrape_ingest_identity_stripped_total help text to match", v.Str(), ok, "payments")
	}
}
