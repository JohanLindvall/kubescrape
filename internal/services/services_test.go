package services

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
)

func makeService(uid, name string, selector map[string]string) *corev1.Service {
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:        name,
			Namespace:   "default",
			UID:         types.UID(uid),
			Annotations: map[string]string{"prometheus.io/scrape": "true"},
		},
		Spec: corev1.ServiceSpec{
			Selector: selector,
			Ports: []corev1.ServicePort{
				{Name: "http", Port: 80, TargetPort: intstr.FromString("web")},
				{Name: "grpc", Port: 9000, TargetPort: intstr.FromInt32(9001)},
			},
		},
	}
}

func TestMatching(t *testing.T) {
	ix := NewIndex()
	ix.Upsert(makeService("uid1", "web-svc", map[string]string{"app": "web"}))
	ix.Upsert(makeService("uid2", "strict-svc", map[string]string{"app": "web", "tier": "gold"}))
	ix.Upsert(makeService("uid3", "no-selector", nil))

	got := ix.Matching("default", map[string]string{"app": "web", "extra": "x"})
	if len(got) != 1 || got[0].Name != "web-svc" {
		t.Fatalf("got %+v", got)
	}

	got = ix.Matching("default", map[string]string{"app": "web", "tier": "gold"})
	if len(got) != 2 {
		t.Fatalf("expected both selecting services, got %+v", got)
	}

	if got := ix.Matching("other-ns", map[string]string{"app": "web"}); len(got) != 0 {
		t.Fatalf("wrong namespace matched: %+v", got)
	}
	if got := ix.Matching("default", map[string]string{"app": "api"}); len(got) != 0 {
		t.Fatalf("non-matching labels matched: %+v", got)
	}
}

func TestPortConversion(t *testing.T) {
	ix := NewIndex()
	ix.Upsert(makeService("uid1", "web-svc", map[string]string{"app": "web"}))

	got := ix.Matching("default", map[string]string{"app": "web"})
	if len(got) != 1 || len(got[0].Ports) != 2 {
		t.Fatalf("got %+v", got)
	}
	if p := got[0].Ports[0]; p.Name != "http" || p.Port != 80 || p.TargetPortName != "web" || p.TargetPortNum != 0 {
		t.Errorf("named targetPort = %+v", p)
	}
	if p := got[0].Ports[1]; p.TargetPortName != "" || p.TargetPortNum != 9001 {
		t.Errorf("numeric targetPort = %+v", p)
	}
}

func TestUpsertReplacesAndDeleteRemoves(t *testing.T) {
	ix := NewIndex()
	ix.Upsert(makeService("uid1", "svc", map[string]string{"app": "web"}))
	// Selector change must stop matching old pods.
	ix.Upsert(makeService("uid1", "svc", map[string]string{"app": "api"}))

	if got := ix.Matching("default", map[string]string{"app": "web"}); len(got) != 0 {
		t.Fatalf("stale selector still matches: %+v", got)
	}
	if got := ix.Matching("default", map[string]string{"app": "api"}); len(got) != 1 {
		t.Fatalf("new selector does not match: %+v", got)
	}

	ix.Delete("default", "uid1")
	if got := ix.Matching("default", map[string]string{"app": "api"}); len(got) != 0 {
		t.Fatalf("deleted service still matches: %+v", got)
	}
}
