package attrs

import (
	"fmt"
	"testing"

	"go.opentelemetry.io/collector/pdata/pcommon"

	"github.com/JohanLindvall/kubescrape/pkg/kubemeta"
)

// benchPod is a realistic pod as the KSM splitter and the tailer see one: two
// owners, five pod labels, two namespace labels.
func benchPod() kubemeta.Pod {
	return kubemeta.Pod{
		Name:      "checkout-7d9f6c5b8d-x2k9v",
		Namespace: "shop",
		UID:       "0f8b2b3a-1c4d-4e5f-9a8b-7c6d5e4f3a2b",
		NodeName:  "aks-user-12345678-vmss000003",
		PodIP:     "10.244.3.17",
		Labels: map[string]string{
			"app.kubernetes.io/name":     "checkout",
			"app.kubernetes.io/instance": "checkout-prod",
			"app.kubernetes.io/version":  "1.42.0",
			"pod-template-hash":          "7d9f6c5b8d",
			"team":                       "payments",
		},
		Owners: []kubemeta.Owner{
			{Kind: "ReplicaSet", Name: "checkout-7d9f6c5b8d"},
			{Kind: "Deployment", Name: "checkout"},
		},
		NamespaceMetadata: &kubemeta.ObjectMeta{
			Labels: map[string]string{
				"kubernetes.io/metadata.name": "shop",
				"tier":                        "prod",
			},
		},
	}
}

func benchContext() Context {
	pod := benchPod()
	return Context{
		Node: &NodeInfo{Name: "aks-user-12345678-vmss000003"},
		Pod:  &pod,
		Container: &kubemeta.Container{
			Name: "checkout", ID: "containerd://3f1c2b", Image: "ghcr.io/shop/checkout:1.42.0", RestartCount: 2,
		},
	}
}

// BenchmarkBuildDefaults is the per-resource cost every producer pays; the KSM
// splitter pays it once per described object (a 12k-pod cluster per scrape).
func BenchmarkBuildDefaults(b *testing.B) {
	ctx := benchContext()
	var bld *Builder
	b.ReportAllocs()
	for b.Loop() {
		res := pcommon.NewResource()
		bld.Build(res, ctx)
	}
}

// benchKeys is the attribute key set a built pod/container resource carries.
func benchKeys() []string {
	return []string{
		"k8s.namespace.name", "k8s.pod.name", "k8s.pod.uid", "k8s.node.name", "k8s.pod.ip",
		"k8s.replicaset.name", "k8s.deployment.name", "service.name", "service.namespace",
		"service.instance.id", "k8s.container.name", "container.id", "container.image.name",
		"k8s.container.restart_count",
		"k8s.pod.label.app.kubernetes.io/name", "k8s.pod.label.app.kubernetes.io/instance",
		"k8s.pod.label.app.kubernetes.io/version", "k8s.pod.label.pod-template-hash",
		"k8s.pod.label.team", "k8s.namespace.label.kubernetes.io/metadata.name",
		"k8s.namespace.label.tier", "k8s.cluster.name", "cloud.region", "service.version",
		"host.name",
	}
}

// BenchmarkFilterKeep is one configured filter evaluated over one resource's
// key set — 25 keys per resource, per described object on a split path.
func BenchmarkFilterKeep(b *testing.B) {
	f, err := NewFilterFromLists(nil, []string{`k8s\.pod\.label\..*`, `k8s\.namespace\.label\..*`})
	if err != nil {
		b.Fatal(err)
	}
	keys := benchKeys()
	b.ReportAllocs()
	for b.Loop() {
		for _, k := range keys {
			_ = f.Keep(k)
		}
	}
}

// BenchmarkFilterApply is the whole-resource call Build makes. The filter keeps
// every key here, so the resource is reusable across iterations and what is
// measured is the per-call closure plus 25 Keep decisions — not a rebuild.
func BenchmarkFilterApply(b *testing.B) {
	f, err := NewFilterFromLists(nil, []string{`k8s\.pod\.label\.internal\..*`, `k8s\.namespace\.label\.internal\..*`})
	if err != nil {
		b.Fatal(err)
	}
	res := pcommon.NewResource()
	for _, k := range benchKeys() {
		res.Attributes().PutStr(k, "v")
	}
	b.ReportAllocs()
	for b.Loop() {
		f.Apply(res)
	}
}

// BenchmarkCachedRegexpHot is a template regexMatch whose pattern is already
// cached — the whole point of the cache.
func BenchmarkCachedRegexpHot(b *testing.B) {
	const pat = `^(prod|stage)-[a-z0-9]+$`
	if _, err := cachedRegexp(pat); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	for b.Loop() {
		if _, err := cachedRegexp(pat); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkCachedRegexpPastCap is the same call once the cache holds more
// distinct patterns than its cap — the data-derived-pattern case the cap exists
// for. An admission stop makes every call a fresh compile. The pattern differs
// from BenchmarkCachedRegexpHot's on purpose: the cache is process-global, so a
// shared one would already be cached from before the fill.
func BenchmarkCachedRegexpPastCap(b *testing.B) {
	for i := range maxRegexKeys + maxRegexKeys/2 {
		if _, err := cachedRegexp(fmt.Sprintf(`^fill-%d-[a-z0-9]+$`, i)); err != nil {
			b.Fatal(err)
		}
	}
	const pat = `^(prod|stage)-[a-z0-9]{1,32}$`
	if _, err := cachedRegexp(pat); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	for b.Loop() {
		if _, err := cachedRegexp(pat); err != nil {
			b.Fatal(err)
		}
	}
}
