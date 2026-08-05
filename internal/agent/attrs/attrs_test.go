package attrs

import (
	"slices"
	"testing"

	"go.opentelemetry.io/collector/pdata/pcommon"

	"github.com/JohanLindvall/kubescrape/internal/owners"
	"github.com/JohanLindvall/kubescrape/pkg/kubemeta"
)

// TestKindTableCoversOwnerResolver pins kindTable against the resolver's
// informer list: a kind added to internal/owners (a new metadata informer /
// owner-chain entry) without a row in kindTable would resolve owners whose
// k8s.<kind>.name attribute is then silently dropped from every resource.
func TestKindTableCoversOwnerResolver(t *testing.T) {
	// GVR resource (plural) -> owner kind; "" marks a resolver GVR that is not
	// an owner kind (Namespace backs namespace METADATA, not the owner chain).
	kindByResource := map[string]string{
		"replicasets":  "ReplicaSet",
		"deployments":  "Deployment",
		"statefulsets": "StatefulSet",
		"daemonsets":   "DaemonSet",
		"jobs":         "Job",
		"cronjobs":     "CronJob",
		"nodes":        "Node",
		"namespaces":   "",
	}
	known := make(map[string]bool)
	for _, gvr := range owners.AllGVRs {
		kind, listed := kindByResource[gvr.Resource]
		if !listed {
			t.Errorf("owners.AllGVRs gained %q: add its kind to kindTable in internal/agent/attrs/attrs.go (and to this map)", gvr.Resource)
			continue
		}
		if kind == "" {
			continue
		}
		known[kind] = true
		if _, ok := KindAttribute(kind); !ok {
			t.Errorf("owner kind %q (owners.AllGVRs %q) has no kindTable row in internal/agent/attrs/attrs.go", kind, gvr.Resource)
		}
	}
	// The reverse: no dead rows describing kinds the resolver never caches.
	for kind := range kindTable {
		if !known[kind] {
			t.Errorf("kindTable kind %q has no owners.AllGVRs informer backing it", kind)
		}
	}
}

func TestPrefixInstance(t *testing.T) {
	// Prepend to an existing instance.
	res := pcommon.NewResource()
	res.Attributes().PutStr("service.instance.id", "cid")
	PrefixInstance(res, "cadvisor")
	if v, _ := res.Attributes().Get("service.instance.id"); v.Str() != "cadvisor-cid" {
		t.Errorf("prefix over existing = %q, want cadvisor-cid", v.Str())
	}
	// No instance derived: the bare prefix must NOT be stamped (a shared
	// meaningless instance is worse than none).
	res = pcommon.NewResource()
	PrefixInstance(res, "cadvisor")
	if v, ok := res.Attributes().Get("service.instance.id"); ok {
		t.Errorf("bare prefix stamped = %q, want unset", v.Str())
	}
	// Empty prefix is a no-op.
	res = pcommon.NewResource()
	res.Attributes().PutStr("service.instance.id", "x")
	PrefixInstance(res, "")
	if v, _ := res.Attributes().Get("service.instance.id"); v.Str() != "x" {
		t.Errorf("empty prefix changed instance to %q", v.Str())
	}
}

func TestPodIPAndServiceName(t *testing.T) {
	res := pcommon.NewResource()
	Pod(res, kubemeta.Pod{
		Name: "p", Namespace: "ns", UID: "u", PodIP: "10.0.0.1",
		Owners: []kubemeta.Owner{{Kind: "ReplicaSet", Name: "rs"}, {Kind: "Deployment", Name: "dep"}},
	})
	a := res.Attributes()
	if v, _ := a.Get("k8s.pod.ip"); v.Str() != "10.0.0.1" {
		t.Errorf("k8s.pod.ip = %q, want 10.0.0.1", v.Str())
	}
	if v, _ := a.Get("service.name"); v.Str() != "dep" {
		t.Errorf("service.name = %q, want dep (owner)", v.Str())
	}
	// No PodIP -> attribute omitted.
	res = pcommon.NewResource()
	Pod(res, kubemeta.Pod{Name: "p", Namespace: "ns", UID: "u"})
	if _, ok := res.Attributes().Get("k8s.pod.ip"); ok {
		t.Error("k8s.pod.ip set despite empty PodIP")
	}
}

// A partially-filled Pod must not mint empty-string resource attributes:
// every field is guarded, not just NodeName/PodIP.
func TestPodEmptyFieldsOmitted(t *testing.T) {
	res := pcommon.NewResource()
	Pod(res, kubemeta.Pod{UID: "u"})
	got := res.Attributes().AsRaw()
	if got["k8s.pod.uid"] != "u" {
		t.Errorf("k8s.pod.uid = %v, want u", got["k8s.pod.uid"])
	}
	for _, absent := range []string{"k8s.namespace.name", "k8s.pod.name", "service.name"} {
		if v, ok := got[absent]; ok {
			t.Errorf("%s = %q set from an empty field; must be omitted", absent, v)
		}
	}
	// The zero Pod yields no attributes at all.
	res = pcommon.NewResource()
	Pod(res, kubemeta.Pod{})
	if n := res.Attributes().Len(); n != 0 {
		t.Errorf("zero Pod produced %d attributes: %v", n, res.Attributes().AsRaw())
	}
}

// ReservedIdentity is the boundary the tailer's pod-annotation filter (and any
// other workload/line-supplied attribute path) consults; the exact key set is
// load-bearing for those consumers, so pin it.
func TestReservedIdentity(t *testing.T) {
	want := []string{
		"container.id",
		"container.name",
		"k8s.container.name",
		"k8s.namespace.name",
		"k8s.node.name",
		"k8s.pod.ip",
		"k8s.pod.name",
		"k8s.pod.uid",
		"service.instance.id",
		"service.namespace",
	}
	if got := ReservedIdentityKeys(); !slices.Equal(got, want) {
		t.Errorf("ReservedIdentityKeys() = %v, want %v", got, want)
	}
	for _, k := range want {
		if !ReservedIdentity(k) {
			t.Errorf("ReservedIdentity(%q) = false, want true", k)
		}
	}
	// service.name is deliberately NOT reserved: descriptive, and overriding
	// it is the pod annotation's documented purpose.
	for _, k := range []string{"service.name", "k8s.cluster.name", ""} {
		if ReservedIdentity(k) {
			t.Errorf("ReservedIdentity(%q) = true, want false", k)
		}
	}
}

func TestIdentity(t *testing.T) {
	inst := func(seed map[string]string) string {
		res := pcommon.NewResource()
		for k, v := range seed {
			res.Attributes().PutStr(k, v)
		}
		Identity(res)
		id, _ := res.Attributes().Get("service.instance.id")
		return id.Str()
	}
	cases := []struct {
		name string
		seed map[string]string
		want string
	}{
		{"container.id wins", map[string]string{"container.id": "abc", "k8s.pod.uid": "u", "k8s.container.name": "c"}, "abc"},
		{"pod.uid + container", map[string]string{"k8s.pod.uid": "u", "k8s.container.name": "c"}, "u/c"},
		{"pod.uid alone", map[string]string{"k8s.pod.uid": "u"}, "u"},
		{"namespace/pod/container", map[string]string{"k8s.namespace.name": "ns", "k8s.pod.name": "p", "k8s.container.name": "c"}, "ns/p/c"},
		{"namespace/pod", map[string]string{"k8s.namespace.name": "ns", "k8s.pod.name": "p"}, "ns/p"},
		{"node fallback", map[string]string{"k8s.node.name": "n1"}, "n1"},
	}
	for _, c := range cases {
		if got := inst(c.seed); got != c.want {
			t.Errorf("%s: service.instance.id = %q, want %q", c.name, got, c.want)
		}
	}

	// service.namespace derived from the k8s namespace.
	res := pcommon.NewResource()
	res.Attributes().PutStr("k8s.namespace.name", "ns")
	Identity(res)
	if v, _ := res.Attributes().Get("service.namespace"); v.Str() != "ns" {
		t.Errorf("service.namespace = %q, want ns", v.Str())
	}
	// An explicit service.instance.id is not overwritten.
	res2 := pcommon.NewResource()
	res2.Attributes().PutStr("k8s.pod.uid", "u")
	res2.Attributes().PutStr("service.instance.id", "preset")
	Identity(res2)
	if v, _ := res2.Attributes().Get("service.instance.id"); v.Str() != "preset" {
		t.Errorf("preset instance overwritten: %q", v.Str())
	}
}

func TestServiceAttrs(t *testing.T) {
	res := pcommon.NewResource()
	Service(res, &kubemeta.Service{Name: "web-svc", UID: "svc-uid"})
	a := res.Attributes()
	if v, _ := a.Get("k8s.service.name"); v.Str() != "web-svc" {
		t.Errorf("k8s.service.name = %q", v.Str())
	}
	if v, _ := a.Get("k8s.service.uid"); v.Str() != "svc-uid" {
		t.Errorf("k8s.service.uid = %q", v.Str())
	}
	// nil Service is a no-op.
	res2 := pcommon.NewResource()
	Service(res2, nil)
	if res2.Attributes().Len() != 0 {
		t.Error("nil service must not set attributes")
	}
}

// FillAbsent is the shared "someone else knows more about this resource" merge
// (ingest enrichment, self-metadata stamping): it adds, never overwrites, and
// carries non-string values across intact.
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
