package selfmeta

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/JohanLindvall/kubescrape/pkg/kubemeta"
)

func discardLog() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

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
// across restarts rather than follow a recreated pod's UID. A `self` attribute
// pipeline can therefore only ADD, never override.
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

// StartPod yields nil until the first success, then the pod — and it retries:
// the metadata a process needs and the process itself come up together. The
// retry interval backs off, so a lookup that can never succeed settles at the
// refresh cadence instead of polling forever.
func TestPollRetriesUntilResolved(t *testing.T) {
	var calls atomic.Int32
	resolve := func(context.Context) (*kubemeta.Pod, error) {
		if calls.Add(1) < 3 {
			return nil, errors.New("not yet")
		}
		return testPod(), nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// Retries would otherwise start at 5s; keep the test on outcomes, not
	// clocks (the tailer's retryBackoff pattern).
	defer func(d time.Duration) { firstRetry = d }(firstRetry)
	firstRetry = time.Millisecond

	pod := StartPod(ctx, resolve, time.Minute, discardLog())
	if p := pod(); p != nil {
		t.Fatalf("provider returned %+v before any lookup succeeded", p)
	}
	deadline := time.Now().Add(30 * time.Second)
	for pod() == nil && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
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
func TestPollTreatsNilValueAsFailure(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pod := StartPod(ctx, func(context.Context) (*kubemeta.Pod, error) { return nil, nil },
		time.Minute, discardLog())
	time.Sleep(50 * time.Millisecond)
	if p := pod(); p != nil {
		t.Fatalf("provider returned %+v for a nil resolve", p)
	}
}

// A refresh of zero disables the lookup: no goroutine, the provider serves the
// caller's initial value forever (how -node-metadata-refresh=0 keeps the bare
// node name).
func TestPollZeroRefreshNeverResolves(t *testing.T) {
	var calls atomic.Int32
	initial := &kubemeta.Pod{Name: "seed"}
	get := Poll(context.Background(), func(context.Context) (*kubemeta.Pod, error) {
		calls.Add(1)
		return testPod(), nil
	}, PollConfig[kubemeta.Pod]{Initial: initial, Log: discardLog()})

	time.Sleep(20 * time.Millisecond)
	if p := get(); p != initial {
		t.Fatalf("provider returned %+v; want the initial value", p)
	}
	if n := calls.Load(); n != 0 {
		t.Fatalf("resolve called %d times with the lookup disabled", n)
	}
}

// A failed REFRESH keeps the last good value: a blip must not strip the
// attributes off a process's metrics.
func TestPollKeepsLastGoodValue(t *testing.T) {
	var calls atomic.Int32
	resolve := func(context.Context) (*kubemeta.Pod, error) {
		if calls.Add(1) == 1 {
			return testPod(), nil
		}
		return nil, errors.New("gone")
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	get := Poll(ctx, resolve, PollConfig[kubemeta.Pod]{Refresh: 5 * time.Millisecond, Log: discardLog()})

	deadline := time.Now().Add(30 * time.Second)
	for calls.Load() < 3 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if p := get(); p == nil || p.Name != "kubescrape-agent-xyz" {
		t.Fatalf("provider returned %+v after failed refreshes; want the last good pod", p)
	}
}

// onFirst runs exactly once, after the first success (the agent hangs its
// readiness gate on it).
func TestPollOnFirstRunsOnceWithTheValue(t *testing.T) {
	var fired atomic.Int32
	var got atomic.Pointer[kubemeta.Pod]
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	Poll(ctx, func(context.Context) (*kubemeta.Pod, error) { return testPod(), nil },
		PollConfig[kubemeta.Pod]{
			Refresh: 5 * time.Millisecond,
			OnFirst: func(p *kubemeta.Pod) { fired.Add(1); got.Store(p) },
			Log:     discardLog(),
		})

	deadline := time.Now().Add(30 * time.Second)
	for fired.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	time.Sleep(30 * time.Millisecond) // several refreshes
	if n := fired.Load(); n != 1 {
		t.Fatalf("onFirst fired %d times; want 1", n)
	}
	if p := got.Load(); p == nil || p.Name != "kubescrape-agent-xyz" {
		t.Fatalf("onFirst got %+v", p)
	}
}

func TestNamespace(t *testing.T) {
	t.Setenv("POD_NAMESPACE", "  monitoring  ")
	if ns := Namespace(); ns != "monitoring" {
		t.Fatalf("Namespace() = %q; want the trimmed env value", ns)
	}
	// No env: falls back to the ServiceAccount projection, and reports empty
	// when that is absent too (the caller must not guess a namespace).
	t.Setenv("POD_NAMESPACE", "")
	if _, err := os.Stat("/var/run/secrets/kubernetes.io/serviceaccount/namespace"); os.IsNotExist(err) {
		if ns := Namespace(); ns != "" {
			t.Fatalf("Namespace() = %q with no env and no projection", ns)
		}
	}
}

// The pod is RE-READ on the refresh cadence, so an edited pod or namespace
// label reaches the metrics it stamps. (The cost of that poll is a conditional
// GET; see the metaclient's own tests.)
func TestStartPodPicksUpChangedMetadata(t *testing.T) {
	var calls atomic.Int32
	resolve := func(context.Context) (*kubemeta.Pod, error) {
		p := testPod()
		if calls.Add(1) > 1 {
			p.Labels = map[string]string{"app": "kubescrape-agent", "team": "platform"}
			p.NamespaceMetadata = &kubemeta.ObjectMeta{Labels: map[string]string{"tier": "prod"}}
		}
		return p, nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	get := StartPod(ctx, resolve, 5*time.Millisecond, discardLog())

	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if p := get(); p != nil && p.NamespaceMetadata != nil {
			if p.Labels["team"] != "platform" || p.NamespaceMetadata.Labels["tier"] != "prod" {
				t.Fatalf("pod = %+v; want the relabelled one", p)
			}
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("changed metadata never picked up after %d lookups", calls.Load())
}
