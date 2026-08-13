package services

// The change token is what the metadata service's monitor→Service cross
// product hangs on (server.monitoredServices). It must move for a change and
// NOT move for a re-delivery: an informer resync hands every Service back
// byte-identical, so a token that counts deliveries rather than changes turns
// `-resync` into a full O(monitors x services) rebuild on essentially every
// agent poll.

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

func svcRV(uid types.UID, name, rv string, labels map[string]string) *corev1.Service {
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "ns", Name: name, UID: uid, ResourceVersion: rv, Labels: labels,
		},
		Spec: corev1.ServiceSpec{
			Selector: map[string]string{"app": "web"},
			Ports:    []corev1.ServicePort{{Name: "http", Port: 80}},
		},
	}
}

func TestGenerationIgnoresAReDeliveryThatChangesNothing(t *testing.T) {
	ix := NewIndex()
	ix.Upsert(svcRV("uid-a", "web", "7", map[string]string{"team": "obs"}))
	after := ix.Generation()

	// The resync: the same object, again, and again.
	ix.Upsert(svcRV("uid-a", "web", "7", map[string]string{"team": "obs"}))
	ix.Upsert(svcRV("uid-a", "web", "7", map[string]string{"team": "obs"}))
	if got := ix.Generation(); got != after {
		t.Fatalf("the change token moved (%d -> %d) for a re-delivery of the indexed object: "+
			"every consumer memo keyed on it rebuilds for nothing", after, got)
	}

	// A real update moves it, and is actually applied.
	ix.Upsert(svcRV("uid-a", "web", "8", map[string]string{"team": "platform"}))
	if got := ix.Generation(); got == after {
		t.Fatal("the change token did not move for a genuine update")
	}
	matched := ix.Matching("ns", map[string]string{"app": "web"})
	if len(matched) != 1 || matched[0].Labels["team"] != "platform" {
		t.Fatalf("the update was not applied: %+v", matched)
	}
}

// A Service with NO resourceVersion is treated as changed every time. Only
// hand-built objects have one — the informer always sets it — and for those the
// version says nothing about the content, so believing it would silently drop
// an in-place fixture edit (internal/services' own same-name regression test
// upserts one UID twice with different annotations and no versions at all).
func TestVersionlessServicesAreAlwaysApplied(t *testing.T) {
	ix := NewIndex()
	ix.Upsert(svcRV("uid-a", "web", "", map[string]string{"team": "obs"}))
	after := ix.Generation()
	ix.Upsert(svcRV("uid-a", "web", "", map[string]string{"team": "platform"}))

	if got := ix.Generation(); got == after {
		t.Error("a versionless re-upsert must count as a change")
	}
	matched := ix.Matching("ns", map[string]string{"app": "web"})
	if len(matched) != 1 || matched[0].Labels["team"] != "platform" {
		t.Fatalf("a versionless update was dropped: %+v", matched)
	}
}
