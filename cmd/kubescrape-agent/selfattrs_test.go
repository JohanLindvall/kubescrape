package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/JohanLindvall/kubescrape/internal/agent/attrs"
	"github.com/JohanLindvall/kubescrape/internal/agent/transform"
	"github.com/JohanLindvall/kubescrape/internal/selfmeta"
	"github.com/JohanLindvall/kubescrape/pkg/kubemeta"
	"github.com/JohanLindvall/kubescrape/pkg/metaclient"
)

func selfPodFixture() *kubemeta.Pod {
	return &kubemeta.Pod{
		Name:      "kubescrape-agent-xyz",
		Namespace: "monitoring",
		UID:       "agent-uid",
		NodeName:  "node1",
		PodIP:     "10.1.2.3",
		Labels:    map[string]string{"app.kubernetes.io/name": "kubescrape-agent"},
		Owners:    []kubemeta.Owner{{Kind: "DaemonSet", Name: "kubescrape-agent"}},
	}
}

func built(t *testing.T, b *attrs.Builder, node func() *attrs.NodeInfo) pcommon.Map {
	t.Helper()
	res := pcommon.NewResource()
	selfBuild(b, node)(res, selfPodFixture())
	return res.Attributes()
}

func attrOf(t *testing.T, m pcommon.Map, key string) string {
	t.Helper()
	v, ok := m.Get(key)
	if !ok {
		return ""
	}
	return v.AsString()
}

// The agent's build maps its own pod through the default k8s attribute set
// plus the derived Mimir identity.
func TestSelfBuildDefaults(t *testing.T) {
	a := built(t, nil, nil) // nil builder: defaults, no filter
	for _, tc := range []struct{ key, want string }{
		{"k8s.namespace.name", "monitoring"},
		{"k8s.pod.name", "kubescrape-agent-xyz"},
		{"k8s.pod.uid", "agent-uid"},
		{"k8s.pod.ip", "10.1.2.3"},
		{"k8s.node.name", "node1"},
		{"k8s.daemonset.name", "kubescrape-agent"},
		{"k8s.pod.label.app.kubernetes.io/name", "kubescrape-agent"},
		{"service.namespace", "monitoring"},
	} {
		if got := attrOf(t, a, tc.key); got != tc.want {
			t.Errorf("%s = %q, want %q", tc.key, got, tc.want)
		}
	}
	// Which container of the pod this process is cannot be known; guessing
	// would mislabel every self-metric.
	if got := attrOf(t, a, "k8s.container.name"); got != "" {
		t.Errorf("k8s.container.name = %q; want none", got)
	}
}

// The configured `self` pipeline applies: static attributes (a cluster name,
// typically) and templates reach the agent's own metrics too.
func TestSelfBuildAppliesConfiguredPipeline(t *testing.T) {
	b, err := attrs.NewBuilders(&attrs.Config{
		Static: map[string]string{"k8s.cluster.name": "prod-eu"},
		Pipelines: map[string]*attrs.Config{"self": {
			Attributes: map[string]string{"zone": `{{ with .Node }}{{ index .Labels "zone" }}{{ end }}`},
		}},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	node := func() *attrs.NodeInfo {
		return &attrs.NodeInfo{Name: "node1", Labels: map[string]string{"zone": "eu-1a"}}
	}
	a := built(t, b.Self, node)
	if got := attrOf(t, a, "k8s.cluster.name"); got != "prod-eu" {
		t.Errorf("k8s.cluster.name = %q, want prod-eu", got)
	}
	if got := attrOf(t, a, "zone"); got != "eu-1a" {
		t.Errorf("zone = %q, want eu-1a", got)
	}
}

// selfDescribing decides whether this process resolves its own pod at all. It
// is the gate on a background lookup and a published gauge, and used to be an
// inline boolean in a 600-line run() where a wrong edit was untestable.
func TestSelfDescribing(t *testing.T) {
	set := func(selfMetrics time.Duration, tier, ingest bool) {
		*selfMetricsIntv, *serviceGraphOn, *ingestOn = selfMetrics, tier, ingest
	}
	defer set(*selfMetricsIntv, *serviceGraphOn, *ingestOn)

	for _, tc := range []struct {
		name               string
		selfMetrics        time.Duration
		tier, ingest, want bool
	}{
		{"self-metrics on", time.Minute, false, false, true},
		// The trace tier emits span metrics and edge metrics under its OWN
		// identity (the described services are data-point labels), so it always
		// describes itself even with the self-metrics push off.
		{"the trace tier", 0, true, false, true},
		{"a plain agent with logs/metrics ingest", 0, false, true, false},
		{"nothing self-describing", 0, false, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			set(tc.selfMetrics, tc.tier, tc.ingest)
			if got := selfDescribing(); got != tc.want {
				t.Fatalf("selfDescribing() = %v, want %v", got, tc.want)
			}
		})
	}
}

// A cluster-singleton (-events / -azure-diagnostics) runs the same binary
// under the same service.name as the per-node DaemonSet: taking the node as
// its instance would put two processes on one (job, instance).
func TestAgentSelfResourceSingletonInstance(t *testing.T) {
	defer func(e, a, l, m, c, n, j, i bool) {
		*eventsOn, *azureOn = e, a
		*logsOn, *metricsOn, *cadvisorOn, *nodeOn, *journaldOn, *ingestOn = l, m, c, n, j, i
	}(*eventsOn, *azureOn, *logsOn, *metricsOn, *cadvisorOn, *nodeOn, *journaldOn, *ingestOn)

	*eventsOn, *azureOn = false, false
	res := agentSelfResource("node1")
	if v, _ := res.Attributes().Get("service.instance.id"); v.AsString() != "node1" {
		t.Errorf("per-node agent instance = %q; want the node", v.AsString())
	}

	// -events on the DAEMONSET (agent.extraArgs, a supported escape hatch) is
	// not the singleton: keying on the flag alone flipped every node's
	// instance from the stable node name to a per-restart pod name.
	*eventsOn = true
	*logsOn = true
	res = agentSelfResource("node1")
	if v, _ := res.Attributes().Get("service.instance.id"); v.AsString() != "node1" {
		t.Errorf("node agent with -events instance = %q; want the node", v.AsString())
	}

	// The real singleton: a cluster-scoped pipeline with every per-node one
	// off, exactly what the events Deployment renders.
	*logsOn, *metricsOn, *cadvisorOn, *nodeOn, *journaldOn, *ingestOn = false, false, false, false, false, false
	res = agentSelfResource("node1")
	v, _ := res.Attributes().Get("service.instance.id")
	if v.AsString() == "node1" || v.AsString() == "" {
		t.Errorf("singleton instance = %q; want its own pod, not the node it sits on", v.AsString())
	}
	if n, _ := res.Attributes().Get("k8s.node.name"); n.AsString() != "node1" {
		t.Errorf("k8s.node.name = %q; the node it runs on is still worth reporting", n.AsString())
	}
}

// A hostNetwork singleton with no $POD_NAME has no pod name to use as its
// instance (selfPodName is ""). It must NOT stamp an EMPTY service.instance.id:
// attrs.Identity treats a present key as set and returns early, so the empty
// string would stick and merge this process onto (job, instance="") with every
// other empty-instance process. Leaving the key absent lets Identity derive its
// node fallback instead — worse-labelled but not colliding.
func TestAgentSelfResourceSingletonEmptyPodName(t *testing.T) {
	defer func(e, a, l, m, c, n, j, i bool) {
		*eventsOn, *azureOn = e, a
		*logsOn, *metricsOn, *cadvisorOn, *nodeOn, *journaldOn, *ingestOn = l, m, c, n, j, i
	}(*eventsOn, *azureOn, *logsOn, *metricsOn, *cadvisorOn, *nodeOn, *journaldOn, *ingestOn)
	defer func(f func() string) { selfInstanceName = f }(selfInstanceName)

	// The real singleton shape (a cluster-scoped pipeline, every per-node one
	// off) with no resolvable pod name — the hostNetwork-without-$POD_NAME case.
	*eventsOn, *azureOn = true, false
	*logsOn, *metricsOn, *cadvisorOn, *nodeOn, *journaldOn, *ingestOn = false, false, false, false, false, false
	selfInstanceName = func() string { return "" }

	res := agentSelfResource("node1")
	v, ok := res.Attributes().Get("service.instance.id")
	if !ok {
		t.Fatalf("service.instance.id absent; want the derived node fallback")
	}
	if got := v.AsString(); got != "node1" {
		t.Fatalf("service.instance.id = %q; want the node fallback %q, never an empty string", got, "node1")
	}
}

// The namespace half of the Prometheus job must be present from the FIRST
// export: deriving it later, when /v1/self answers, renames the job of
// already-running cumulative series.
func TestAgentSelfResourceNamespaceAtStartup(t *testing.T) {
	t.Setenv("POD_NAMESPACE", "monitoring")
	res := agentSelfResource("node1")
	a := res.Attributes()
	if v, _ := a.Get("k8s.namespace.name"); v.AsString() != "monitoring" {
		t.Fatalf("k8s.namespace.name = %q at startup", v.AsString())
	}
	if v, _ := a.Get("service.namespace"); v.AsString() != "monitoring" {
		t.Fatalf("service.namespace = %q at startup; the job must not change when the lookup lands", v.AsString())
	}
	if v, _ := a.Get("service.instance.id"); v.AsString() != "node1" {
		t.Fatalf("service.instance.id = %q; a namespace must not displace the node", v.AsString())
	}
}

// The peer-address lookup cannot work for every deployment — a hostNetwork
// agent shares the node IP, a NAT hop or proxy replaces the source address,
// and a dual-stack pod may connect from the family status.podIP does not carry
// — so a 404 falls back to a lookup BY NAME rather than leaving the process
// unattributed for its lifetime.
func TestSelfResolveFallsBackToPodByName(t *testing.T) {
	t.Setenv("POD_NAMESPACE", "monitoring")
	t.Setenv("POD_NAME", "kubescrape-agent-xyz")
	var selfHits, nameHits int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/self":
			selfHits++
			http.Error(w, "no live pod with peer IP", http.StatusNotFound)
		case "/v1/pods/monitoring/kubescrape-agent-xyz":
			nameHits++
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(kubemeta.Pod{Name: "kubescrape-agent-xyz", Namespace: "monitoring", UID: "u1"})
		default:
			http.Error(w, "unexpected "+r.URL.Path, http.StatusNotFound)
		}
	}))
	defer srv.Close()

	pod, err := selfResolve(metaclient.New(metaclient.Config{Base: srv.URL, Timeout: time.Second}))(context.Background())
	if err != nil {
		t.Fatalf("resolve failed despite a resolvable name: %v", err)
	}
	if pod.Name != "kubescrape-agent-xyz" || pod.UID != "u1" {
		t.Fatalf("pod = %+v", pod)
	}
	if selfHits != 1 || nameHits != 1 {
		t.Fatalf("self=%d byName=%d; want the peer-address lookup tried first, then the fallback", selfHits, nameHits)
	}
}

// With no identity to fall back to, the original peer-address error stands —
// the resolver must not invent a pod.
func TestSelfResolveWithoutIdentity(t *testing.T) {
	t.Setenv("POD_NAMESPACE", "")
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "no live pod with peer IP", http.StatusNotFound)
	}))
	defer srv.Close()
	defer func(p string) { selfmeta.SetNamespaceProjectionForTest(p) }(selfmeta.SetNamespaceProjectionForTest(filepath.Join(t.TempDir(), "nope")))

	if pod, err := selfResolve(metaclient.New(metaclient.Config{Base: srv.URL, Timeout: time.Second}))(context.Background()); err == nil {
		t.Fatalf("resolved %+v with no peer match and no name", pod)
	}
}

// recordingExporter is a stand-in for one link of the export chain.
type recordingExporter struct{ metrics int }

func (r *recordingExporter) ExportLogs(context.Context, plog.Logs) error { return nil }
func (r *recordingExporter) ExportMetrics(context.Context, pmetric.Metrics) error {
	r.metrics++
	return nil
}

// The agent's own metrics must keep the DEFAULT (buffered) chain when routing
// is configured: the router fans out by k8s.namespace.name, which these
// resources only acquired when self-attributes started stamping it, so a route
// covering the agent's own namespace would move the fleet's health signal onto
// an unbuffered tenant destination.
func TestSelfSinkBypassesTheRouter(t *testing.T) {
	routerChain, defaultChain := &recordingExporter{}, &recordingExporter{}
	md := pmetric.NewMetrics()
	md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics().AppendEmpty().SetName("kubescrape_up")

	// No routing: the one chain is the chain.
	if got := selfSink(routerChain, defaultChain, false, nil); got != selfmeta.Exporter(routerChain) {
		t.Fatal("selfSink diverted the chain with no routing configured")
	}

	// Routing configured, no transforms: straight to the pre-routing chain.
	if err := selfSink(routerChain, defaultChain, true, nil).ExportMetrics(context.Background(), md); err != nil {
		t.Fatal(err)
	}
	if defaultChain.metrics != 1 || routerChain.metrics != 0 {
		t.Fatalf("default=%d router=%d; self-metrics must skip the router", defaultChain.metrics, routerChain.metrics)
	}

	// Routing plus transforms: still skips the router, and still transforms.
	prog, err := transform.Compile([]byte("metrics: |\n  def transform(batch):\n      for m in batch:\n          m.name = \"renamed\"\n"))
	if err != nil {
		t.Fatal(err)
	}
	w := transform.Wrap(routerChain, nil, prog)
	sink := selfSink(w, defaultChain, true, w)
	if err := sink.ExportMetrics(context.Background(), md); err != nil {
		t.Fatal(err)
	}
	if defaultChain.metrics != 2 || routerChain.metrics != 0 {
		t.Fatalf("default=%d router=%d with transforms; self-metrics must still skip the router",
			defaultChain.metrics, routerChain.metrics)
	}
	if fork, ok := sink.(*transform.Wrapper); !ok || fork.Active() != w.Active() {
		t.Fatal("the self chain must share the reloaded transform program")
	}
}
