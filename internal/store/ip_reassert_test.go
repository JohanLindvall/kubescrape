package store

import (
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

func ipPodRV(uid, name, ip, rv string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "ns", UID: types.UID(uid), ResourceVersion: rv},
		Spec:       corev1.PodSpec{NodeName: "n1"},
		Status:     corev1.PodStatus{Phase: corev1.PodRunning, PodIP: ip, HostIP: "192.168.1.1"},
	}
}

// A stale pod RE-ASSERTING an address it already held must not steal it from
// the pod that legitimately acquired it later. Plain last-write-wins let any
// unrelated update to a stale pod (a node-lifecycle condition on a NotReady
// node, a resurrect after delete, a transient podIP blip) mis-attribute every
// peer-IP lookup until that pod finally went away.
func TestProbeStaleReassertDoesNotStealIP(t *testing.T) {
	s, _ := newTestStore(time.Minute)
	s.UpsertPod(ipPodRV("a", "a", "10.0.0.5", "1")) // A holds .5
	s.UpsertPod(ipPodRV("b", "b", "10.0.0.5", "1")) // CNI recycles .5 to B
	if np, _ := s.GetPodByIP("10.0.0.5"); np.Pod.Name != "b" {
		t.Fatalf("setup: owner = %q, want b", np.Pod.Name)
	}
	// An unrelated update to the stale pod A (same IP, new resourceVersion).
	s.UpsertPod(ipPodRV("a", "a", "10.0.0.5", "2"))
	if np, _ := s.GetPodByIP("10.0.0.5"); np.Pod.Name != "b" {
		t.Fatalf("owner = %q after a stale re-assert, want b: A stole the live owner's IP", np.Pod.Name)
	}

	// A genuine LATER acquisition still wins (a late-scheduled pod taking a
	// freed address).
	s.UpsertPod(ipPodRV("c", "c", "10.0.0.5", "1"))
	if np, _ := s.GetPodByIP("10.0.0.5"); np.Pod.Name != "c" {
		t.Fatalf("owner = %q, want c: a genuine later acquisition must win", np.Pod.Name)
	}
}
