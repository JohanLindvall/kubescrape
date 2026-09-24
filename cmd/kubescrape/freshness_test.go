package main

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/cache"

	"github.com/JohanLindvall/kubescrape/internal/owners"
)

// TestInformerHandlersStampFreshness pins that every event a typed handler
// delivers — add, update, delete, a delete arriving as a tombstone — stamps
// its resource's clock, and that a registered resource reads 0 until then:
// the gauge's whole meaning is "this watch is still delivering".
func TestInformerHandlersStampFreshness(t *testing.T) {
	var f informerFreshness
	note := f.slot("pods")
	if got := f.snapshot()["pods"]; got != 0 {
		t.Fatalf("a resource with no event yet reads %v, want 0", got)
	}
	var events int
	h := typedHandler(note, func(*corev1.Pod) { events++ }, func(*corev1.Pod) { events++ })
	pod := &corev1.Pod{}
	h.OnAdd(pod, false)
	if got := f.snapshot()["pods"]; got == 0 {
		t.Fatal("an add did not stamp the resource's clock")
	}
	h.OnUpdate(pod, pod.DeepCopy())
	h.OnDelete(cache.DeletedFinalStateUnknown{Obj: pod})
	if events != 3 {
		t.Fatalf("handlers ran %d times, want 3: the stamp must not swallow the event", events)
	}
	if _, ok := f.snapshot()["services"]; ok {
		t.Fatal("an unregistered resource appeared in the snapshot")
	}
}

// A periodic RESYNC is the informer replaying its own cache — client-go hands
// the cached object as both halves of the update — and it keeps running while
// the watch behind it has silently stopped. Stamping it kept the freshness
// gauge current on a timer through exactly the stall the gauge exists to show,
// so with -resync at or under the documented alert's window that alert could
// never fire. A distinct object (the watch, or a relist of an unchanged object,
// which is freshly decoded) is a delivery and still stamps; the handler itself
// still runs either way.
func TestAResyncDoesNotStampFreshness(t *testing.T) {
	var f informerFreshness
	var updates int
	typed := typedHandler(f.slot("pods"), func(*corev1.Pod) { updates++ }, func(*corev1.Pod) {})
	var c owners.Changes
	owner := ownerChangeHandler(&c, f.slot("replicasets"))

	pod := &corev1.Pod{}
	rs := &metav1.PartialObjectMetadata{}
	typed.OnUpdate(pod, pod)
	owner.OnUpdate(rs, rs)
	for _, resource := range []string{"pods", "replicasets"} {
		if got := f.snapshot()[resource]; got != 0 {
			t.Errorf("a resync (the same cached object as both halves) stamped %s's clock: a stalled "+
				"watch would read as fresh for as long as the resync timer runs", resource)
		}
	}
	if updates != 1 {
		t.Fatalf("the typed handler ran %d times for the resync, want 1: skipping the stamp must not skip the event", updates)
	}

	typed.OnUpdate(pod, pod.DeepCopy())
	owner.OnUpdate(rs, rs.DeepCopy())
	for _, resource := range []string{"pods", "replicasets"} {
		if got := f.snapshot()[resource]; got == 0 {
			t.Errorf("an update carrying a distinct object did not stamp %s's clock", resource)
		}
	}
}
