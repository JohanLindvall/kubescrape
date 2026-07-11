package attrs

import (
	"testing"

	"go.opentelemetry.io/collector/pdata/pcommon"

	"github.com/JohanLindvall/kubescrape/internal/kubemeta"
)

func TestPrefixInstance(t *testing.T) {
	// Prepend to an existing instance.
	res := pcommon.NewResource()
	res.Attributes().PutStr("service.instance.id", "cid")
	PrefixInstance(res, "cadvisor")
	if v, _ := res.Attributes().Get("service.instance.id"); v.Str() != "cadvisor-cid" {
		t.Errorf("prefix over existing = %q, want cadvisor-cid", v.Str())
	}
	// Bare prefix when no instance was derived.
	res = pcommon.NewResource()
	PrefixInstance(res, "cadvisor")
	if v, _ := res.Attributes().Get("service.instance.id"); v.Str() != "cadvisor" {
		t.Errorf("bare prefix = %q, want cadvisor", v.Str())
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
