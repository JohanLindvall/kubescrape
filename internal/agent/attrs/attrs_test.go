package attrs

import (
	"testing"

	"go.opentelemetry.io/collector/pdata/pcommon"
)

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
