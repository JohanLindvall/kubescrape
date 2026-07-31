package selfmeta

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/JohanLindvall/kubescrape/pkg/kubemeta"
)

type captureExporter struct {
	md    pmetric.Metrics
	calls int
}

func (c *captureExporter) ExportMetrics(_ context.Context, md pmetric.Metrics) error {
	c.md, c.calls = md, c.calls+1
	return nil
}

// selfMetrics builds a payload shaped like a process's own: one resource
// carrying the identity that process can build unaided.
func selfMetrics() pmetric.Metrics {
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	a := rm.Resource().Attributes()
	a.PutStr("service.name", "kubescrape-agent")
	a.PutStr("service.version", "v1.2.3")
	a.PutStr("k8s.node.name", "node1")
	a.PutStr("service.instance.id", "node1")
	return md
}

func testPod() *kubemeta.Pod {
	return &kubemeta.Pod{
		Name:      "kubescrape-agent-xyz",
		Namespace: "monitoring",
		UID:       "agent-uid",
		NodeName:  "node1",
		Labels:    map[string]string{"app": "kubescrape-agent"},
	}
}

// build is a stand-in for a caller's attribute mapping.
func build(res pcommon.Resource, pod *kubemeta.Pod) {
	a := res.Attributes()
	a.PutStr("k8s.namespace.name", pod.Namespace)
	a.PutStr("k8s.pod.name", pod.Name)
	a.PutStr("k8s.pod.uid", pod.UID)
	a.PutStr("service.name", "from-the-pod") // must lose to what the process set
	a.PutStr("service.instance.id", pod.UID) // ditto: a restart-unstable value
}

func attrOf(t *testing.T, md pmetric.Metrics, key string) string {
	t.Helper()
	v, ok := md.ResourceMetrics().At(0).Resource().Attributes().Get(key)
	if !ok {
		return ""
	}
	return v.AsString()
}

// The pod's attributes are added; the identity the process set for itself is
// left alone — service.instance.id in particular, which must stay stable
// across restarts rather than follow a recreated pod's UID.
func TestWrapFillsOnlyAbsentKeys(t *testing.T) {
	next := &captureExporter{}
	pod := testPod()
	exp := Wrap(next, func() *kubemeta.Pod { return pod }, build)

	if err := exp.ExportMetrics(context.Background(), selfMetrics()); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct{ key, want string }{
		{"k8s.namespace.name", "monitoring"},
		{"k8s.pod.name", "kubescrape-agent-xyz"},
		{"k8s.pod.uid", "agent-uid"},
		{"service.name", "kubescrape-agent"},
		{"service.instance.id", "node1"},
		{"service.version", "v1.2.3"},
	} {
		if got := attrOf(t, next.md, tc.key); got != tc.want {
			t.Errorf("%s = %q, want %q", tc.key, got, tc.want)
		}
	}
}

// Until the lookup succeeds the payload passes through untouched — a
// process's own metrics must keep flowing, since they are how the failure to
// resolve is diagnosed.
func TestWrapPassesThroughUnresolved(t *testing.T) {
	next := &captureExporter{}
	exp := Wrap(next, func() *kubemeta.Pod { return nil }, build)
	if err := exp.ExportMetrics(context.Background(), selfMetrics()); err != nil {
		t.Fatal(err)
	}
	if next.calls != 1 {
		t.Fatalf("calls = %d; want the payload forwarded", next.calls)
	}
	if n := next.md.ResourceMetrics().At(0).Resource().Attributes().Len(); n != 4 {
		t.Fatalf("resource has %d attributes; want the original 4", n)
	}
}

// With the lookup disabled the exporter is not wrapped at all.
func TestWrapDisabled(t *testing.T) {
	next := &captureExporter{}
	if got := Wrap(next, nil, build); got != Exporter(next) {
		t.Error("Wrap wrapped the exporter with no provider")
	}
	if got := Wrap(next, func() *kubemeta.Pod { return nil }, nil); got != Exporter(next) {
		t.Error("Wrap wrapped the exporter with no build func")
	}
}

// The stamped attributes track the provider: a pod resolved (or relabeled)
// after the process started shows up on the next export, with no restart.
func TestWrapFollowsTheProvider(t *testing.T) {
	var pod *kubemeta.Pod
	next := &captureExporter{}
	exp := Wrap(next, func() *kubemeta.Pod { return pod }, build)

	if err := exp.ExportMetrics(context.Background(), selfMetrics()); err != nil {
		t.Fatal(err)
	}
	if got := attrOf(t, next.md, "k8s.pod.name"); got != "" {
		t.Fatalf("k8s.pod.name = %q before the lookup resolved", got)
	}
	pod = testPod()
	if err := exp.ExportMetrics(context.Background(), selfMetrics()); err != nil {
		t.Fatal(err)
	}
	if got := attrOf(t, next.md, "k8s.pod.name"); got != "kubescrape-agent-xyz" {
		t.Fatalf("k8s.pod.name = %q after the lookup resolved", got)
	}
}

// Start yields nil until the first success, then the pod — and it retries: the
// metadata a process needs and the process itself come up together.
func TestStartRetriesUntilResolved(t *testing.T) {
	var calls atomic.Int32
	resolve := func(context.Context) (*kubemeta.Pod, error) {
		if calls.Add(1) < 3 {
			return nil, errors.New("not yet")
		}
		return testPod(), nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	defer func(d time.Duration) { retryEvery = d }(retryEvery)
	retryEvery = time.Millisecond
	pod := Start(ctx, resolve, time.Minute, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if p := pod(); p != nil {
		t.Fatalf("provider returned %+v before any lookup succeeded", p)
	}
	deadline := time.Now().Add(30 * time.Second)
	for pod() == nil && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	p := pod()
	if p == nil {
		t.Fatalf("provider still nil after %d attempts", calls.Load())
	}
	if p.Name != "kubescrape-agent-xyz" {
		t.Fatalf("pod = %+v", p)
	}
}

// A resolve that returns (nil, nil) is a failure, not a success: stamping a
// zero pod would put empty attributes on every metric.
func TestStartTreatsNilPodAsFailure(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pod := Start(ctx, func(context.Context) (*kubemeta.Pod, error) { return nil, nil },
		time.Minute, slog.New(slog.NewTextHandler(io.Discard, nil)))
	time.Sleep(50 * time.Millisecond)
	if p := pod(); p != nil {
		t.Fatalf("provider returned %+v for a nil resolve", p)
	}
}

func TestFillAbsent(t *testing.T) {
	src, dst := pcommon.NewMap(), pcommon.NewMap()
	src.PutStr("a", "from-src")
	src.PutStr("b", "added")
	src.PutInt("n", 7)
	dst.PutStr("a", "kept")

	FillAbsent(src, dst)
	if v, _ := dst.Get("a"); v.AsString() != "kept" {
		t.Errorf("a = %q; an existing key must not be overwritten", v.AsString())
	}
	if v, _ := dst.Get("b"); v.AsString() != "added" {
		t.Errorf("b = %q; want added", v.AsString())
	}
	if v, ok := dst.Get("n"); !ok || v.Int() != 7 {
		t.Errorf("n = %v; non-string values must survive the copy", v)
	}
}
