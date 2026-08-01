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

	// Deleting it releases both, leaving nothing behind. GetPodByIP alone does
	// NOT prove that: with a tombstone TTL the record keeps a DeletedAt stamp
	// and the lookup's tombstone guard rejects it whether or not the index
	// entry is still there. The INDEX is what has to be empty — Sweep never
	// revisits byPodIP, so an entry left behind pins the whole kubemeta.Pod for
	// the process lifetime and blocks the live pod that later takes the
	// address from ever being promoted onto it.
	s.DeletePod(types.UID("ds"))
	for _, ip := range []string{"10.0.0.7", "fd00::7"} {
		if _, ok := s.GetPodByIP(ip); ok {
			t.Errorf("%s still resolves after the pod was deleted", ip)
		}
		if rec, present := s.byPodIP[ip]; present {
			t.Errorf("byPodIP still maps %s to %q after its only claimant was deleted; the tombstone guard hides it from GetPodByIP but the entry never expires",
				ip, rec.pod.Name)
		}
	}
	if len(s.ipClaimants) != 0 {
		t.Errorf("claimants left behind: %v", s.ipClaimants)
	}
}

// Deleting a dual-stack pod must release EVERY address from byPodIP, not just
// the primary. Sweep never revisits byPodIP, so a leftover entry serves a
// deleted pod for the process lifetime — and with -cache-ttl 0 the record is
// removed without a DeletedAt stamp, so even the tombstone guard cannot catch
// it.
func TestDeleteReleasesEveryAddressWithoutTTL(t *testing.T) {
	s := New(0) // no tombstones: the record is removed outright
	p := ipPod("ds", "p-ds", "10.0.0.7")
	p.Status.PodIPs = []corev1.PodIP{{IP: "10.0.0.7"}, {IP: "fd00::7"}}
	s.UpsertPod(p)
	s.DeletePod(types.UID("ds"))

	for _, ip := range []string{"10.0.0.7", "fd00::7"} {
		if np, ok := s.GetPodByIP(ip); ok {
			t.Errorf("%s still resolves to %q after deletion", ip, np.Pod.Name)
		}
	}
	if len(s.byPodIP) != 0 {
		t.Errorf("byPodIP retained %d entries: %v", len(s.byPodIP), s.byPodIP)
	}
}

// And the live pod that takes a released address is promoted onto BOTH
// families, not only the primary.
func TestPromotionCoversEveryAddress(t *testing.T) {
	s := New(time.Minute)
	old := ipPod("old", "p-old", "10.0.0.8")
	old.Status.PodIPs = []corev1.PodIP{{IP: "10.0.0.8"}, {IP: "fd00::8"}}
	s.UpsertPod(old)
	newer := ipPod("new", "p-new", "10.0.0.8")
	newer.Status.PodIPs = []corev1.PodIP{{IP: "10.0.0.8"}, {IP: "fd00::8"}}
	s.UpsertPod(newer)

	s.DeletePod(types.UID("new"))
	for _, ip := range []string{"10.0.0.8", "fd00::8"} {
		np, ok := s.GetPodByIP(ip)
		if !ok || np.Pod.Name != "p-old" {
			t.Errorf("GetPodByIP(%s) = %q ok=%v; the surviving claimant must be promoted on every address",
				ip, np.Pod.Name, ok)
		}
	}
}
