package main

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/tools/cache"
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
	h.OnUpdate(pod, pod)
	h.OnDelete(cache.DeletedFinalStateUnknown{Obj: pod})
	if events != 3 {
		t.Fatalf("handlers ran %d times, want 3: the stamp must not swallow the event", events)
	}
	if _, ok := f.snapshot()["services"]; ok {
		t.Fatal("an unregistered resource appeared in the snapshot")
	}
}
