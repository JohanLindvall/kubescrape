package otlpingest

// Allocation budgets for the ingest request path, ENFORCED here rather than
// only reported by a benchmark (a benchmark cannot fail a build). They skip
// under -race, whose bookkeeping allocations would make any ceiling either
// meaningless or wide enough to let a real regression through.

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strconv"
	"testing"

	"github.com/klauspost/compress/gzip"

	"github.com/JohanLindvall/kubescrape/internal/agent/attrs"
	"github.com/JohanLindvall/kubescrape/internal/testrace"
	"github.com/JohanLindvall/kubescrape/pkg/kubemeta"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

// autoDecisionPush is the shape the auto-mode decision walks: one resource
// naming itself, N data points naming the same object by a different id kind
// (an SDK labelling its own metrics), so every point is visited and none is
// foreign.
func autoDecisionPush(points int) pmetric.Metrics {
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr("container.id", "cafe01")
	dps := rm.ScopeMetrics().AppendEmpty().Metrics().AppendEmpty().
		SetEmptyGauge().DataPoints()
	for i := range points {
		dp := dps.AppendEmpty()
		dp.SetIntValue(1)
		dp.Attributes().PutStr("k8s.pod.uid", "pod-uid-1")
		dp.Attributes().PutStr("le", strconv.Itoa(i))
	}
	return md
}

// The auto-mode decision walks EVERY data point of the default mode's default
// path, and it built a kind-tagged token per point purely to compare it against
// the resource's — one heap allocation each, the largest non-pdata allocator
// there (a 4 MiB gRPC push is ~67k points, a 16 MiB HTTP push ~270k). The
// comparison and the memo lookups are in-place now; a token is materialised
// only when one has to be STORED, i.e. once per distinct id.
func TestAutoDecisionWalkAllocationBudget(t *testing.T) {
	if testrace.Enabled {
		t.Skip("the race detector's bookkeeping allocations make the ceiling meaningless")
	}
	const points = 2000
	e := NewEnricher(Config{Meta: newMeta(), MetricsMode: MetricsAuto})
	md := autoDecisionPush(points)
	ctx := context.Background()

	got := testing.AllocsPerRun(20, func() {
		cache := newReqCache()
		if !e.resourceModeSuffices(ctx, cache, md) {
			t.Fatal("this payload must take the resource branch; the budget below measures the wrong walk")
		}
	})
	// The per-request cache and its one materialised token, not a per-point cost.
	if want := 20.0; got > want {
		t.Errorf("decision over %d data points allocates %.0f times, want <= %.0f (it was one per point)", points, got, want)
	}
}

// The peer-IP fallback is OPT-IN and off by default, yet its not-applicable
// answer minted a fresh empty map — two allocations — for every id-less
// resource of every push, and memoised nothing, so each one paid again.
func TestPeerFallbackNotApplicableIsAllocationFree(t *testing.T) {
	if testrace.Enabled {
		t.Skip("the race detector's bookkeeping allocations make the ceiling meaningless")
	}
	e := NewEnricher(Config{Meta: newMeta()}) // PeerIPFallback off: the default
	ctx := context.Background()
	cache := newReqCache()

	got := testing.AllocsPerRun(100, func() {
		if _, resolved, rejected := e.peerAttrs(ctx, cache); resolved || rejected {
			t.Fatal("the fallback is disabled; it must resolve nothing")
		}
	})
	if got != 0 {
		t.Errorf("peerAttrs allocates %.1f times per id-less resource, want 0", got)
	}
}

var builtSink pcommon.Map

// buildFor deep-copied the attribute map of a resource it had just built and
// never shared, doubling the allocations of every resolved attribution (34 ->
// 17 on this pod). A built map is read-only by contract (emptyAttrs), so the
// resource's own map is the answer: buildFor may cost no more than the build.
func TestBuildForDoesNotCopyTheMapItBuilt(t *testing.T) {
	if testrace.Enabled {
		t.Skip("the race detector's bookkeeping allocations make the ceiling meaningless")
	}
	e := NewEnricher(Config{Meta: newMeta()})
	pod := &kubemeta.Pod{
		Name: "web-1", Namespace: "default", UID: "pod-uid-1", NodeName: "node1",
		Labels: map[string]string{"app": "web", "tier": "front"},
		Owners: []kubemeta.Owner{
			{Kind: "ReplicaSet", Name: "web-7d9f", Controller: true},
			{Kind: "Deployment", Name: "web", Controller: true},
		},
	}
	container := &kubemeta.Container{Name: "app", ID: "containerd://cafe01"}

	if v, ok := e.buildFor(pod, container).Get("k8s.pod.name"); !ok || v.Str() != "web-1" {
		t.Fatalf("buildFor rendered no k8s.pod.name: the measurement below would be of nothing")
	}
	build := testing.AllocsPerRun(50, func() {
		r := pcommon.NewResource()
		e.cfg.Attrs.Build(r, attrs.Context{Pod: pod, Container: container})
		builtSink = r.Attributes()
	})
	got := testing.AllocsPerRun(50, func() { builtSink = e.buildFor(pod, container) })
	if got > build {
		t.Errorf("buildFor allocates %.0f times against the build's own %.0f: it is copying what it just built", got, build)
	}
}

// A gzipped HTTP push built a fresh klauspost reader per request — its 32 KiB
// window plus huffman tables, ~37 kB, dropped on the floor per push — while the
// gRPC arm in the same process decompressed through otlpexport's POOLED codec
// readers. The HTTP arm (BodyReader, which the trace tier's internal hop shares)
// now decompresses through the same codec, so what a gzipped read costs beyond
// an identity read of the same payload is the per-message wrapper and nothing
// the size of a decompressor.
func TestGzipBodyReadUsesThePooledDecompressor(t *testing.T) {
	if testrace.Enabled {
		t.Skip("the race detector's bookkeeping allocations make the ceiling meaningless")
	}
	// Small, so the destination buffer is ONE allocation on both arms (the
	// gzip arm has no usable size hint and grows by doubling past 511 bytes):
	// what remains of the difference is the decompressor.
	raw, err := oneLog().MarshalProto()
	if err != nil {
		t.Fatal(err)
	}
	var zb bytes.Buffer
	zw := gzip.NewWriter(&zb)
	if _, err := zw.Write(raw); err != nil {
		t.Fatal(err)
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	br := NewBodyReader(maxIngestBody)
	read := func(body []byte, encoding string) {
		r := httptest.NewRequest(http.MethodPost, "/v1/logs", bytes.NewReader(body))
		r.Header.Set("Content-Type", "application/x-protobuf")
		if encoding != "" {
			r.Header.Set("Content-Encoding", encoding)
		}
		got, _, err := br.Read(r)
		if err != nil || !bytes.Equal(got, raw) {
			t.Fatalf("read %d bytes, err %v; want the %d-byte payload", len(got), err, len(raw))
		}
	}
	perRead := func(body []byte, encoding string) float64 {
		read(body, encoding) // the first read may construct the pooled reader
		const n = 200
		var before, after runtime.MemStats
		runtime.ReadMemStats(&before)
		for range n {
			read(body, encoding)
		}
		runtime.ReadMemStats(&after)
		return float64(after.TotalAlloc-before.TotalAlloc) / n
	}
	identity := perRead(raw, "")
	gzipped := perRead(zb.Bytes(), "gzip")
	if overhead, max := gzipped-identity, 16.0*1024; overhead > max {
		t.Errorf("a gzipped read costs %.0f bytes more than an identity read of the same %d-byte payload, want <= %.0f "+
			"(a fresh decompressor per push is ~37 kB)", overhead, len(raw), max)
	}
}

var reqCacheSink *reqCache

// Most pushes write to ONE of the request cache's maps. The probe memo and the
// counted-outcome marker are filled only on their own paths, so allocating them
// up front cost every push the maps it does not use.
func TestNewReqCacheDoesNotPreallocateUnusedMaps(t *testing.T) {
	if testrace.Enabled {
		t.Skip("the race detector's bookkeeping allocations make the ceiling meaningless")
	}
	got := testing.AllocsPerRun(100, func() { reqCacheSink = newReqCache() })
	if want := 2.0; got > want {
		t.Errorf("newReqCache allocates %.0f times, want <= %.0f (the struct and the ids memo)", got, want)
	}
}

// idLessPush is the plain-SDK sender's shape: one resource with no container
// id or pod uid (no container detector), and points labelled only with the
// application's own dimensions.
func idLessPush(points int) pmetric.Metrics {
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr("service.name", "checkout")
	dps := rm.ScopeMetrics().AppendEmpty().Metrics().AppendEmpty().
		SetEmptyGauge().DataPoints()
	for i := range points {
		dp := dps.AppendEmpty()
		dp.SetIntValue(1)
		dp.Attributes().PutStr("http.route", "/cart")
		dp.Attributes().PutStr("le", strconv.Itoa(i))
	}
	return md
}

// Auto mode — the default — kept a push on the resource branch only when EVERY
// resource carried an id, so the id-less sender above was demoted to the
// splitter. The split's answer for it is the resource branch's (every point
// lands in the "" group, whose copied resource takes the same peer fallback and
// merge), bought with a copy of every point: measured 50,038 allocations for
// these 10k points against 1 on the resource branch.
func TestAutoModeKeepsAnIDLessPushOnTheResourceBranch(t *testing.T) {
	if testrace.Enabled {
		t.Skip("the race detector's bookkeeping allocations make the ceiling meaningless")
	}
	const points = 10_000
	e := NewEnricher(Config{Meta: newMeta(), MetricsMode: MetricsAuto})
	md := idLessPush(points)
	ctx := context.Background()
	if !e.resourceModeSuffices(ctx, newReqCache(), md) {
		t.Fatal("an id-less resource whose points carry no id either must not demote the push")
	}
	got := testing.AllocsPerRun(5, func() {
		if out := e.EnrichMetrics(ctx, md); out.DataPointCount() != points {
			t.Fatalf("forwarded %d points, want %d", out.DataPointCount(), points)
		}
	})
	if want := 10.0; got > want {
		t.Errorf("auto mode allocates %.0f times on an id-less %d-point push, want <= %.0f (the resource "+
			"branch; it was ~5 per point on the splitter)", got, points, want)
	}

	// The demotion that IS needed still happens: an id-less resource with a
	// point naming an object has to split.
	md.ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().At(0).Gauge().DataPoints().At(points-1).
		Attributes().PutStr("k8s.pod.uid", "pod-uid-2")
	if e.resourceModeSuffices(ctx, newReqCache(), md) {
		t.Error("an id-less resource whose point names an object must still take the split path")
	}
}

// idPerPointPush is the datapoint-mode sender's shape: one resource naming
// itself by container id, and every point naming the same object by BOTH id
// kinds — the case where resolvableToken builds a candidate token per kind.
func idPerPointPush(points int) pmetric.Metrics {
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr("container.id", "cafe01")
	dps := rm.ScopeMetrics().AppendEmpty().Metrics().AppendEmpty().
		SetEmptyGauge().DataPoints()
	for i := range points {
		dp := dps.AppendEmpty()
		dp.SetIntValue(1)
		dp.Attributes().PutStr("container.id", "cafe01")
		dp.Attributes().PutStr("k8s.pod.uid", "pod-uid-1")
		dp.Attributes().PutStr("le", strconv.Itoa(i))
	}
	return md
}

// The split path used to build every DATA POINT's id token as a `prefix + v`
// concatenation, which escapes and allocates — once or twice per point, the
// largest non-pdata allocator on that path (~24% of its objects). Tokens are interned
// per request now (reqCache.token), so each distinct id is materialised once
// per push and a point costs only the output slot its move allocates (the
// ceiling TestSplitMovesPointsRatherThanCopyingThem holds for id-less points).
func TestSplitTokenIsBuiltOncePerIDNotPerPoint(t *testing.T) {
	if testrace.Enabled {
		t.Skip("the race detector's bookkeeping allocations make the ceiling meaningless")
	}
	const points = 10_000
	e := NewEnricher(Config{Meta: newMeta(), MetricsMode: MetricsDatapoint})
	md := idPerPointPush(points)
	ctx := context.Background()

	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	out := e.EnrichMetrics(ctx, md)
	runtime.ReadMemStats(&after)

	if out.ResourceMetrics().Len() != 1 || out.DataPointCount() != points {
		t.Fatalf("forwarded %d resources and %d points, want 1 and %d: the sender's own points must share one group",
			out.ResourceMetrics().Len(), out.DataPointCount(), points)
	}
	if v, _ := out.ResourceMetrics().At(0).Resource().Attributes().Get("k8s.pod.name"); v.Str() != "web-1" {
		t.Fatalf("the group was not enriched (k8s.pod.name=%q): the measurement below would be of a different path", v.Str())
	}
	perPoint := float64(after.Mallocs-before.Mallocs) / points
	if want := 2.0; perPoint > want {
		t.Errorf("the split allocates %.2f times per id-carrying point, want <= %.0f (a token per point per id kind "+
			"put it past the id-less ceiling)", perPoint, want)
	}
}

// The splitter MOVES each point into its output group rather than copying it:
// nothing reads the input after the split, and a copy was a second, uncharged
// instance of every point (attributes and exemplars included) resident beside
// the decoded input until the handler returned. What is left per point is the
// output slot pdata's AppendEmpty allocates.
func TestSplitMovesPointsRatherThanCopyingThem(t *testing.T) {
	if testrace.Enabled {
		t.Skip("the race detector's bookkeeping allocations make the ceiling meaningless")
	}
	const points = 10_000
	e := NewEnricher(Config{Meta: newMeta(), MetricsMode: MetricsDatapoint})
	md := idLessPush(points)
	ctx := context.Background()

	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	out := e.EnrichMetrics(ctx, md)
	runtime.ReadMemStats(&after)

	if out.DataPointCount() != points {
		t.Fatalf("forwarded %d points, want %d", out.DataPointCount(), points)
	}
	dp := out.ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().At(0).Gauge().DataPoints().At(points - 1)
	if v, _ := dp.Attributes().Get("le"); v.Str() != strconv.Itoa(points-1) || dp.IntValue() != 1 {
		t.Fatalf("the moved point lost its content: le=%q value=%d", v.Str(), dp.IntValue())
	}
	perPoint := float64(after.Mallocs-before.Mallocs) / points
	if want := 2.0; perPoint > want {
		t.Errorf("the split allocates %.2f times per point, want <= %.0f (it was ~5 when every point, its "+
			"attributes and their values were copied)", perPoint, want)
	}
}
