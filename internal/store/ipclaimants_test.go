package store

import (
	"fmt"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
)

// Promotion must not walk every pod in the cluster. It runs on the single
// informer goroutine under the exclusive write lock, once per pod lifecycle
// end, while every reader waits behind it — and a rollout delivers those in
// bursts.
func BenchmarkDeletePodWithManyPods(b *testing.B) {
	for _, n := range []int{1000, 10000} {
		b.Run(fmt.Sprint(n), func(b *testing.B) {
			s := New(time.Minute)
			for i := 0; i < n; i++ {
				s.UpsertPod(ipPod(fmt.Sprint(i), "p-"+fmt.Sprint(i), fmt.Sprintf("10.%d.%d.%d", i/65536, (i/256)%256, i%256)))
			}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				uid := fmt.Sprint(i % n)
				s.DeletePod(types.UID(uid))
				s.UpsertPod(ipPod(uid, "p-"+uid, fmt.Sprintf("10.%d.%d.%d", (i%n)/65536, ((i%n)/256)%256, (i%n)%256)))
			}
		})
	}
}

// The claimant index must not grow by one entry per recycled address, and a
// shadowed claimant must not outlive its pod.
func TestIPClaimantsDoNotLeak(t *testing.T) {
	s := New(time.Minute)
	for i := 0; i < 200; i++ {
		uid := fmt.Sprint(i)
		s.UpsertPod(ipPod(uid, "p-"+uid, "10.0.0.5")) // all claim ONE address
	}
	if got := len(s.ipClaimants["10.0.0.5"]); got != 200 {
		t.Fatalf("claimants = %d, want 200", got)
	}
	for i := 0; i < 200; i++ {
		s.DeletePod(types.UID(fmt.Sprint(i)))
	}
	if got := len(s.ipClaimants); got != 0 {
		t.Fatalf("ipClaimants retained %d addresses after every claimant was deleted: %v", got, s.ipClaimants)
	}
}

// Among live claimants the LATER acquisition wins, exactly as the claim path
// decides it. Promotion used to pick at map-iteration random, reintroducing
// the stale-pod-wins outcome ipSeq exists to prevent.
func TestPromotionPrefersTheLaterAcquirer(t *testing.T) {
	s := New(time.Minute)
	s.UpsertPod(ipPod("holder", "p-holder", "10.0.0.9"))
	s.UpsertPod(ipPod("older", "p-older", "10.0.0.9"))
	s.UpsertPod(ipPod("newest", "p-newest", "10.0.0.9"))

	// The winner releases it; the most recent other acquirer must take over.
	s.DeletePod(types.UID("newest"))
	np, ok := s.GetPodByIP("10.0.0.9")
	if !ok {
		t.Fatal("address unresolvable after its claimant was deleted")
	}
	if np.Pod.Name != "p-older" {
		t.Fatalf("promoted %s; want the later of the remaining acquirers (p-older)", np.Pod.Name)
	}
}

// A dual-stack pod is reachable at every address in status.podIPs, and a
// connection can arrive from the family status.podIP does not carry. Indexing
// only the primary made /v1/self and /v1/pod-ips 404 for the other one, which
// silently disabled agent self-attribution and the ingest peer-IP fallback.
func TestDualStackAddressesResolve(t *testing.T) {
	s := New(time.Minute)
	p := ipPod("ds", "p-ds", "10.0.0.7")
	p.Status.PodIPs = []corev1.PodIP{{IP: "10.0.0.7"}, {IP: "fd00::7"}}
	s.UpsertPod(p)

	for _, ip := range []string{"10.0.0.7", "fd00::7"} {
		np, ok := s.GetPodByIP(ip)
		if !ok || np.Pod.Name != "p-ds" {
			t.Errorf("GetPodByIP(%s) = %+v, ok=%v; both families must resolve", ip, np.Pod.Name, ok)
		}
	}

	// Deleting it releases both, leaving nothing behind.
	s.DeletePod(types.UID("ds"))
	for _, ip := range []string{"10.0.0.7", "fd00::7"} {
		if _, ok := s.GetPodByIP(ip); ok {
			t.Errorf("%s still resolves after the pod was deleted", ip)
		}
	}
	if len(s.ipClaimants) != 0 {
		t.Errorf("claimants left behind: %v", s.ipClaimants)
	}
}
