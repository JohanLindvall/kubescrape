package store

// Regression tests for the pod-IP index across deletion. promoteIPClaimantLocked
// filters candidates on the tombstone marker (expireAt), but DeletePod runs the
// promotion BEFORE stamping it — and with -cache-ttl 0 removes the record
// instead of stamping it at all. Without an explicit exclusion the pod being
// deleted was therefore still an eligible candidate and re-claimed its own IP.

import (
	"strconv"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

func ipPod(uid, name, ip string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: name, Namespace: "ns", UID: types.UID(uid), ResourceVersion: "1",
		},
		Spec:   corev1.PodSpec{NodeName: "n1"},
		Status: corev1.PodStatus{Phase: corev1.PodRunning, PodIP: ip, HostIP: "192.168.1.1"},
	}
}

// A deleted pod must not re-claim the IP entry its own deletion just removed.
func TestDeletePodDoesNotReclaimOwnIP(t *testing.T) {
	s, _ := newTestStore(time.Minute)
	s.UpsertPod(ipPod("a", "a", "10.0.0.5"))
	s.DeletePod("a")

	s.mu.RLock()
	rec, present := s.byPodIP["10.0.0.5"]
	s.mu.RUnlock()
	if present {
		t.Fatalf("byPodIP still maps 10.0.0.5 after its only claimant was deleted: rec=%q deletedAt=%v",
			rec.pod.Name, rec.pod.DeletedAt)
	}
}

// With the tombstone cache disabled the record leaves s.pods entirely, so a
// re-claimed entry would keep serving a DELETED pod (DeletedAt never stamped on
// that path) from GET /v1/pod-ips forever. Pod IPs recycle: that is a
// mis-attribution of another workload's telemetry, not a stale-but-harmless read.
func TestZeroTTLDeletedPodDoesNotResolveByIP(t *testing.T) {
	s := New(0)
	s.UpsertPod(ipPod("a", "a", "10.0.0.5"))
	s.DeletePod("a")

	if np, ok := s.GetPodByIP("10.0.0.5"); ok {
		t.Fatalf("GetPodByIP returned deleted pod %q (deletedAt=%v); the IP index must drop deleted pods immediately",
			np.Pod.Name, np.Pod.DeletedAt)
	}
}

// Sweep never revisits byPodIP, so a re-claimed entry is permanent: one leaked
// entry (pinning the whole record) per deleted pod that owned an IP.
func TestPodIPIndexDoesNotLeakAcrossDeletes(t *testing.T) {
	s, clk := newTestStore(time.Minute)
	const n = 500
	for i := range n {
		uid := "uid" + strconv.Itoa(i)
		ip := "10.1." + strconv.Itoa(i/256) + "." + strconv.Itoa(i%256)
		s.UpsertPod(ipPod(uid, "p"+strconv.Itoa(i), ip))
		s.DeletePod(types.UID(uid))
	}
	clk.Advance(2 * time.Minute)
	s.Sweep()

	s.mu.RLock()
	pods, ips := len(s.pods), len(s.byPodIP)
	s.mu.RUnlock()
	if ips != 0 {
		t.Fatalf("after %d create+delete cycles and a full sweep: pods=%d byPodIP=%d (want byPodIP=0)", n, pods, ips)
	}
}

// When a stale claimant shadowing a live pod on a recycled IP is deleted, the
// LIVE pod must be promoted — deterministically. While the deleted record stayed
// an eligible candidate the winner depended on Go's randomized map iteration, so
// the real owner was orphaned a fraction of the time.
func TestPromotionAfterDeleteIsDeterministic(t *testing.T) {
	const runs = 200
	orphaned := 0
	for range runs {
		s, _ := newTestStore(time.Minute)
		s.UpsertPod(ipPod("live", "live", "10.0.0.5"))
		s.UpsertPod(ipPod("stale", "stale", "10.0.0.5")) // shadows the live claim
		s.DeletePod("stale")
		np, ok := s.GetPodByIP("10.0.0.5")
		if !ok || np.Pod.Name != "live" {
			orphaned++
		}
	}
	if orphaned != 0 {
		t.Fatalf("live IP owner orphaned in %d/%d runs after the stale claimant was deleted", orphaned, runs)
	}
}
