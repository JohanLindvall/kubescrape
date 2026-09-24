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
// node, a resurrect after delete while its tombstone is retained) mis-attribute
// every peer-IP lookup until that pod finally went away. A podIP blip is NOT
// in that list: see TestAnAddressReportedAgainIsANewAcquisition.
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

// The ordering is ACQUISITION, and an acquisition is an address appearing in a
// record that did not report it last. So an address that leaves a stale pod's
// status and comes back — a blip — or a pod whose record was dropped (no
// tombstone at -cache-ttl 0) and is then resurrected, cannot be told from a
// sandbox re-created onto that address, and takes it back from the later
// acquirer. That is the store's deliberate answer, not an oversight; this pins
// it so that changing the rule is a decision rather than a side effect.
func TestAnAddressReportedAgainIsANewAcquisition(t *testing.T) {
	holder := func(t *testing.T, s *Store) string {
		t.Helper()
		np, ok := s.GetPodByIP("10.0.0.5")
		if !ok {
			t.Fatal("nobody holds 10.0.0.5")
		}
		return np.Pod.Name
	}
	t.Run("blip", func(t *testing.T) {
		s, _ := newTestStore(time.Minute)
		s.UpsertPod(ipPodRV("a", "a", "10.0.0.5", "1"))
		s.UpsertPod(ipPodRV("b", "b", "10.0.0.5", "1"))
		s.UpsertPod(ipPodRV("a", "a", "", "2")) // a stops reporting the address
		if got := holder(t, s); got != "b" {
			t.Fatalf("holder = %q while a reports no address, want b", got)
		}
		s.UpsertPod(ipPodRV("a", "a", "10.0.0.5", "3")) // …and reports it again
		if got := holder(t, s); got != "a" {
			t.Fatalf("holder = %q, want a: an address re-reported after leaving the status is a new "+
				"acquisition (if this is now deliberately b, update claimOneIPLocked's doc)", got)
		}
	})
	t.Run("resurrect with no tombstone", func(t *testing.T) {
		s, _ := newTestStore(0)
		s.UpsertPod(ipPodRV("a", "a", "10.0.0.5", "1"))
		s.UpsertPod(ipPodRV("b", "b", "10.0.0.5", "1"))
		s.DeletePod("a") // -cache-ttl 0: the record is dropped outright
		s.UpsertPod(ipPodRV("a", "a", "10.0.0.5", "2"))
		if got := holder(t, s); got != "a" {
			t.Fatalf("holder = %q, want a: with no tombstone the resurrect has no previous addresses", got)
		}
	})
	t.Run("resurrect over a retained tombstone", func(t *testing.T) {
		s, _ := newTestStore(time.Minute)
		s.UpsertPod(ipPodRV("a", "a", "10.0.0.5", "1"))
		s.UpsertPod(ipPodRV("b", "b", "10.0.0.5", "1"))
		s.DeletePod("a")
		s.UpsertPod(ipPodRV("a", "a", "10.0.0.5", "2"))
		if got := holder(t, s); got != "b" {
			t.Fatalf("holder = %q, want b: the tombstone remembers the address, so this is a re-assert", got)
		}
	})
}
