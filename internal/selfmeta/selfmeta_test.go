package selfmeta

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/JohanLindvall/kubescrape/pkg/kubemeta"
)

func discardLog() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// startPoll is Poll with the resolver goroutine JOINED before the test returns.
// A resolver that outlives its test goes on reading package state — the pacing
// var, once the process hostname — that the NEXT test may be writing, which is
// how a test-only seam in this package became a real data race (`go test -race
// -shuffle=on ./internal/selfmeta`). Production never joins: it cancels the
// context and moves on, which is why the signal is unexported.
func startPoll(t *testing.T, resolve func(context.Context) (*kubemeta.Pod, error), cfg PollConfig[kubemeta.Pod]) func() *kubemeta.Pod {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	stopped := make(chan struct{})
	cfg.stopped = stopped
	get := Poll(ctx, resolve, cfg)
	t.Cleanup(func() {
		cancel()
		select {
		case <-stopped:
		case <-time.After(30 * time.Second):
			t.Error("the resolver goroutine outlived the test that started it")
		}
	})
	return get
}

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

// build is a stand-in for a caller's attribute mapping. Like the real ones it
// must tolerate a nil pod and still emit what it knows without one — here the
// stand-in for an operator's static attributes.
func build(res pcommon.Resource, pod *kubemeta.Pod) {
	a := res.Attributes()
	a.PutStr("k8s.cluster.name", "prod-eu") // static: never depends on the pod
	if pod == nil {
		return
	}
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

// Until the lookup succeeds the payload still ships — a process's own metrics
// must keep flowing, since they are how the failure to resolve is diagnosed —
// and it ships WITH whatever the build knows without a pod. The operator's
// static attributes are how an alert on kubescrape_self_metadata_resolved == 0
// selects the broken agents, so they cannot be conditional on nothing being
// broken.
func TestWrapStampsStaticAttrsWhenUnresolved(t *testing.T) {
	next := &captureExporter{}
	exp := Wrap(next, func() *kubemeta.Pod { return nil }, build)
	if err := exp.ExportMetrics(context.Background(), selfMetrics()); err != nil {
		t.Fatal(err)
	}
	if next.calls != 1 {
		t.Fatalf("calls = %d; want the payload forwarded", next.calls)
	}
	if got := attrOf(t, next.md, "k8s.cluster.name"); got != "prod-eu" {
		t.Errorf("k8s.cluster.name = %q with no pod resolved; want the static attribute", got)
	}
	if got := attrOf(t, next.md, "k8s.pod.name"); got != "" {
		t.Errorf("k8s.pod.name = %q with no pod resolved", got)
	}
	if got := attrOf(t, next.md, "service.name"); got != "kubescrape-agent" {
		t.Errorf("service.name = %q; the process's own identity must survive", got)
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
	// Retries would otherwise start at 5s; keep the test on outcomes, not
	// clocks (the tailer's retryBackoff pattern). Poll reads this on the
	// CALLER's goroutine and startPoll joins the resolver before the test
	// returns, so the override is confined to this test.
	defer func(d time.Duration) { firstRetry = d }(firstRetry)
	firstRetry = time.Millisecond

	pod := startPoll(t, resolve, PollConfig[kubemeta.Pod]{Refresh: time.Minute, Log: discardLog()})
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
	pod := startPoll(t, func(context.Context) (*kubemeta.Pod, error) { return nil, nil },
		podPollConfig(time.Minute, discardLog()))
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
	get := startPoll(t, func(context.Context) (*kubemeta.Pod, error) {
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
	get := startPoll(t, resolve, PollConfig[kubemeta.Pod]{Refresh: 5 * time.Millisecond, Log: discardLog()})

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
	startPoll(t, func(context.Context) (*kubemeta.Pod, error) { return testPod(), nil },
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
	get := startPoll(t, resolve, podPollConfig(5*time.Millisecond, discardLog()))

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

type errExporter struct{ calls int }

func (e *errExporter) ExportMetrics(context.Context, pmetric.Metrics) error {
	e.calls++
	return errors.New("collector down")
}

// Wrap must be TRANSPARENT to export errors, resolved or not. The span-metrics
// generator marks its series delivered — evicting stale ones and clearing
// exemplars — only when Export returns nil, so a wrapper that swallowed a
// failure would destroy observations no backend ever received.
func TestWrapPropagatesExportErrors(t *testing.T) {
	for _, tc := range []struct {
		name string
		pod  func() *kubemeta.Pod
	}{
		{"resolved", func() *kubemeta.Pod { return testPod() }},
		{"unresolved", func() *kubemeta.Pod { return nil }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			next := &errExporter{}
			if err := Wrap(next, tc.pod, build).ExportMetrics(context.Background(), selfMetrics()); err == nil {
				t.Fatal("export error swallowed")
			}
			if next.calls != 1 {
				t.Fatalf("downstream called %d times", next.calls)
			}
		})
	}
}

// The (nil, nil) guard has to hold on the REFRESH path too: a resolve that
// starts returning nothing must leave the last good pod in place rather than
// stamping an empty one. (Deleting the guard in fetch used to leave the whole
// package green.)
func TestPollNilRefreshKeepsLastGoodValue(t *testing.T) {
	var calls atomic.Int32
	resolve := func(context.Context) (*kubemeta.Pod, error) {
		if calls.Add(1) == 1 {
			return testPod(), nil
		}
		return nil, nil // no error, no value: not a successful resolution
	}
	get := startPoll(t, resolve, PollConfig[kubemeta.Pod]{Refresh: 2 * time.Millisecond, Log: discardLog()})

	deadline := time.Now().Add(30 * time.Second)
	for calls.Load() < 4 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if p := get(); p == nil || p.Name != "kubescrape-agent-xyz" {
		t.Fatalf("provider = %+v after nil refreshes; want the last good pod", p)
	}
}

// The "neither namespace source available" branch runs deterministically —
// including inside a pod, where the ServiceAccount projection exists and the
// assertion would otherwise silently pass without executing.
func TestNamespaceWithoutAnySource(t *testing.T) {
	t.Setenv("POD_NAMESPACE", "")
	defer func(p string) { namespaceProjection = p }(namespaceProjection)
	namespaceProjection = filepath.Join(t.TempDir(), "does-not-exist")
	if ns := Namespace(); ns != "" {
		t.Fatalf("Namespace() = %q with no env and no projection; a guessed namespace is worse than none", ns)
	}
}

// The projection is read when the env is unset (the metadata service's only
// source, since its Deployment wires no downward API).
func TestNamespaceFromProjection(t *testing.T) {
	t.Setenv("POD_NAMESPACE", "")
	defer func(p string) { namespaceProjection = p }(namespaceProjection)
	path := filepath.Join(t.TempDir(), "namespace")
	if err := os.WriteFile(path, []byte("monitoring\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	namespaceProjection = path
	if ns := Namespace(); ns != "monitoring" {
		t.Fatalf("Namespace() = %q; want the trimmed projection value", ns)
	}
}

// syncBuffer collects log output written from Poll's background goroutine.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.String()
}

// The resolution report must be honest about WHY a pod is not provably this
// one. Under hostNetwork the kubelet gives the container the NODE's hostname,
// so the pod-name check cannot match — a supported, documented shape
// (deploy/agent.yaml passes POD_NAME precisely for it), yet every agent on
// every node used to warn about "a proxy or NAT hop to the metadata service"
// that the manifests say is not there. The proxy advice belongs to the case
// where it is actually plausible; a custom spec.hostname is legitimate too, so
// neither benign wording may read as a misconfiguration.
//
// Driven through podReport's own hostname field, SYNCHRONOUSLY: the hostname
// source used to be a package-level var, and swapping it here raced every
// resolver goroutine other tests had left running (see startPoll).
func TestPodResolutionReport(t *testing.T) {
	hostNetworkPod := func() *kubemeta.Pod {
		p := testPod()
		p.HostNetwork = true
		return p
	}
	for _, tc := range []struct {
		name     string
		hostname string
		pod      func() *kubemeta.Pod
		wantWarn bool
		want     []string
		reject   []string
	}{
		{
			// The ordinary pod-network case: hostname IS the pod name.
			name: "confirmed", hostname: "kubescrape-agent-xyz", pod: testPod,
			want:   []string{"level=INFO", "confirmed=true"},
			reject: []string{"proxy"},
		},
		{
			// The DaemonSet shape: hostNetwork, hostname is the node name.
			name: "hostNetwork", hostname: "node1", pod: hostNetworkPod,
			want:   []string{"level=INFO", "confirmed=false", "hostNetwork"},
			reject: []string{"proxy", "NAT"},
		},
		{
			// Nothing this process can see explains the mismatch: keep the
			// proxy/NAT advice, and name the legitimate cause beside it.
			name: "unexplained", hostname: "someone-else", pod: testPod,
			wantWarn: true,
			want:     []string{"level=WARN", "spec.hostname", "proxy or NAT hop", "hostname=someone-else"},
		},
		{
			// hostNetwork is not a blanket excuse: a hostNetwork pod whose
			// hostname is neither its own name nor its node's was answered by
			// somebody else, or renamed.
			name: "hostNetwork with a foreign hostname", hostname: "other-node", pod: hostNetworkPod,
			wantWarn: true,
			want:     []string{"level=WARN", "proxy or NAT hop"},
		},
		{
			// An unreadable hostname proves nothing either way — including for
			// a hostNetwork pod, whose node-name match must not be satisfied by
			// the empty string.
			name: "no hostname", hostname: "", pod: hostNetworkPod,
			wantWarn: true,
			want:     []string{"level=WARN"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var out bytes.Buffer
			log := slog.New(slog.NewTextHandler(&out, &slog.HandlerOptions{Level: slog.LevelDebug}))
			podReport{host: func() string { return tc.hostname }, log: log}.write(tc.pod())

			line := out.String()
			if line == "" {
				t.Fatal("no resolution report logged")
			}
			if got := strings.Contains(line, "level=WARN"); got != tc.wantWarn {
				t.Errorf("warned=%v, want %v: %s", got, tc.wantWarn, line)
			}
			for _, want := range tc.want {
				if !strings.Contains(line, want) {
					t.Errorf("report does not contain %q: %s", want, line)
				}
			}
			for _, bad := range tc.reject {
				if strings.Contains(line, bad) {
					t.Errorf("report blames %q in a benign, documented shape: %s", bad, line)
				}
			}
		})
	}
}

// ...and the configuration StartPod ships actually WIRES that report to the
// real hostname: the table above drives podReport directly, so nothing else
// would notice the OnFirst hook being dropped. Deterministic without any seam —
// the resolved pod is named after this machine's own hostname, which is exactly
// what "confirmed" means. Run through podPollConfig, the value StartPod passes
// to Poll, so the only thing this cannot catch is StartPod calling something
// else entirely.
func TestStartPodLogsTheResolutionReport(t *testing.T) {
	host, err := os.Hostname()
	if err != nil || host == "" {
		t.Skipf("no hostname to confirm against: %v", err)
	}
	var out syncBuffer
	log := slog.New(slog.NewTextHandler(&out, &slog.HandlerOptions{Level: slog.LevelDebug}))
	pod := testPod()
	pod.Name = host
	get := startPoll(t, func(context.Context) (*kubemeta.Pod, error) { return pod, nil },
		podPollConfig(time.Minute, log))

	deadline := time.Now().Add(30 * time.Second)
	for out.String() == "" && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if line := out.String(); !strings.Contains(line, "confirmed=true") {
		t.Fatalf("StartPod's resolution report = %q; want the confirmed line", line)
	}
	if p := get(); p == nil || p.Name != host {
		t.Fatalf("provider = %+v", p)
	}
}
