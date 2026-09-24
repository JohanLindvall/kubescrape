package services

// The change token is what the metadata service's monitor→Service cross
// product hangs on (server.monitoredServices). It must move for a change and
// NOT move for a re-delivery: an informer resync hands every Service back
// byte-identical, so a token that counts deliveries rather than changes turns
// `-resync` into a full O(monitors x services) rebuild on essentially every
// agent poll.

import (
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/JohanLindvall/kubescrape/internal/obs"
	"github.com/JohanLindvall/kubescrape/internal/testrace"
	"github.com/JohanLindvall/kubescrape/pkg/kubemeta"
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
	matched := matching(ix, "ns", map[string]string{"app": "web"})
	if len(matched) != 1 || matched[0].Labels["team"] != "platform" {
		t.Fatalf("the update was not applied: %+v", matched)
	}
}

// The re-delivery is ignored BEFORE the conversion, not merely before the index
// write: the conversion is where the annotation budget's refusal is counted,
// and kubescrape_metadata_annotations_omitted_total is meant to move once per
// informer EVENT. A relist or `-resync` re-delivers every Service
// byte-identical, so converting first re-counted every over-budget Service once
// per re-delivery — a counter that climbs on a timer with nothing changing.
func TestAReDeliveryDoesNotRecountOmittedAnnotations(t *testing.T) {
	omitted := obs.MetadataAnnotationsOmitted.WithLabelValues("Service")
	fat := func(rv string) *corev1.Service {
		svc := svcRV("uid-fat", "fat", rv, nil)
		svc.Annotations = map[string]string{"blob": strings.Repeat("x", kubemeta.MaxAnnotationValueBytes+1)}
		return svc
	}
	ix := NewIndex()
	before := omitted.Value()
	ix.Upsert(fat("7"))
	if got := omitted.Value() - before; got != 1 {
		t.Fatalf("fixture: the first delivery counted %v refusals, want 1", got)
	}
	gen := ix.Generation()

	ix.Upsert(fat("7"))
	ix.Upsert(fat("7"))
	if got := omitted.Value() - before; got != 1 {
		t.Errorf("two re-deliveries of the same object moved the refusal counter to %v, want 1: "+
			"it counts deliveries instead of events", got)
	}
	if got := ix.Generation(); got != gen {
		t.Errorf("the change token moved (%d -> %d) for a re-delivery", gen, got)
	}

	// A genuine update is still an event, and is still counted.
	ix.Upsert(fat("8"))
	if got := omitted.Value() - before; got != 2 {
		t.Errorf("a genuine update moved the refusal counter to %v, want 2", got)
	}
}

// Zero allocations is the only number that says the conversion did not run for
// a re-delivery: CopyMeta, the selector clone and the ports are its whole cost.
func TestUnchangedReDeliveryIsAllocationFree(t *testing.T) {
	if testrace.Enabled {
		t.Skip("-race perturbs allocation counts")
	}
	ix := NewIndex()
	svc := svcRV("uid-a", "web", "7", map[string]string{"team": "obs"})
	ix.Upsert(svc)
	if got := testing.AllocsPerRun(100, func() { ix.Upsert(svc) }); got != 0 {
		t.Errorf("a re-delivery of an unchanged Service allocates %v times, want 0: "+
			"the conversion runs before the short-circuit", got)
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
	matched := matching(ix, "ns", map[string]string{"app": "web"})
	if len(matched) != 1 || matched[0].Labels["team"] != "platform" {
		t.Fatalf("a versionless update was dropped: %+v", matched)
	}
}

// The same rule on the delete side, which is where the two index types had
// drifted: servicemonitors' deleteMonitor returns without bumping when the key
// was absent, while this one bumped before it had established that anything
// was there to remove.
//
// The reachable no-op delete is the tail of the same-name guard above: a
// Service recreated inside a relist gap arrives as an Update with a new UID,
// Upsert drops the predecessor, and the predecessor's own Delete event then
// arrives for a UID that is already gone.
func TestGenerationIgnoresADeleteOfSomethingNeverIndexed(t *testing.T) {
	ix := NewIndex()
	ix.Upsert(svcRV("uid-a", "web", "7", nil))
	after := ix.Generation()

	ix.Delete("ns", "uid-does-not-exist")
	if got := ix.Generation(); got != after {
		t.Errorf("the change token moved (%d -> %d) for a delete of an unknown UID", after, got)
	}
	ix.Delete("no-such-namespace", "uid-a")
	if got := ix.Generation(); got != after {
		t.Errorf("the change token moved (%d -> %d) for a delete in a namespace holding no Services", after, got)
	}

	// The recreation-in-a-relist-gap tail: the successor's Upsert already
	// dropped the predecessor, so its late Delete finds nothing.
	ix.Upsert(svcRV("uid-b", "web", "1", nil)) // same name, new UID
	after = ix.Generation()
	ix.Delete("ns", "uid-a")
	if got := ix.Generation(); got != after {
		t.Errorf("the change token moved (%d -> %d) for the late Delete of a predecessor Upsert had already replaced", after, got)
	}
	if matched := matching(ix, "ns", map[string]string{"app": "web"}); len(matched) != 1 || matched[0].UID != "uid-b" {
		t.Fatalf("the late Delete disturbed the live successor: %+v", matched)
	}

	// ...and a delete that really removes something still moves it.
	ix.Delete("ns", "uid-b")
	if got := ix.Generation(); got == after {
		t.Error("the change token did not move for a delete that removed a Service")
	}
	if matched := matching(ix, "ns", map[string]string{"app": "web"}); len(matched) != 0 {
		t.Fatalf("the Service survived its delete: %+v", matched)
	}
}
