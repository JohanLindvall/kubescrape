package main

import (
	"testing"

	"go.opentelemetry.io/collector/pdata/pcommon"

	"github.com/JohanLindvall/kubescrape/internal/agent/attrs"
	"github.com/JohanLindvall/kubescrape/pkg/kubemeta"
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
