package store

// The pod-IP index's live-beats-terminating precedence, on all three arms that
// implement it: the two cases in claimOneIPLocked and the comparison in
// beatsClaimant that promotion uses.
//
// The rule exists because a DRAINING pod keeps phase Running for its whole grace
// period and goes on reporting a PodIP the CNI has already handed to someone
// else. All three arms could be deleted with the whole store and server suites
// green, and getting it wrong is silent: GET /v1/pod-ips and GET /v1/self hand
// back the draining pod, so the ingest peer-IP fallback stamps its name, UID and
// owners onto the LIVE workload's pushed logs and metrics until the tombstone
// expires.
//
// What the existing coverage misses is the ORDER: TestStaleUpdateCannotReclaim
// RecycledIP and TestLateScheduledPodClaimsRecycledIP both have the live pod
// acquiring the address LAST, so the ipSeq rule underneath already decides them
// and the terminating arms are shadowed. Every case below gives the TERMINATING
// pod the higher ipSeq, which is the only shape in which those arms decide
// anything.

import (
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/types"
)

// A LIVE pod re-asserting an address a TERMINATING pod currently holds takes it,
// even though the terminating pod acquired it later. Without the arm the live
// pod loses on ipSeq and the address keeps resolving to the drainer.
func TestLiveClaimantTakesTheAddressFromATerminatingHolder(t *testing.T) {
	s := New(time.Minute)
	s.UpsertPod(runningPod("live-uid", "live", "1", "10.0.0.5", tOld))       // acquires first
	s.UpsertPod(runningPod("drain-uid", "drain", "1", "10.0.0.5", tOld))     // later acquisition: holds it
	s.UpsertPod(terminatingPod("drain-uid", "drain", "2", "10.0.0.5", tOld)) // now draining

	// A routine update to the live pod — same address, so this is a re-assert
	// and not a new acquisition: its ipSeq does not move.
	s.UpsertPod(runningPod("live-uid", "live", "2", "10.0.0.5", tOld))

	np, ok := s.GetPodByIP("10.0.0.5")
	if !ok || np.Pod.Name != "live" {
		t.Fatalf("GetPodByIP = %q (ok=%v), want live: a draining pod's claim must yield to a live "+
			"pod's, whichever of them acquired the address later", np.Pod.Name, ok)
	}
}

// The mirror: a TERMINATING pod re-asserting must not take the address back from
// a live holder, however much later it acquired it. This is the routine case —
// a drained pod's status updates keep carrying the recycled IP for the whole
// grace period.
func TestTerminatingClaimantDoesNotStealFromALiveHolder(t *testing.T) {
	s := New(time.Minute)
	s.UpsertPod(runningPod("live-uid", "live", "1", "10.0.0.5", tOld))       // acquires first
	s.UpsertPod(runningPod("drain-uid", "drain", "1", "10.0.0.5", tOld))     // later acquisition: holds it
	s.UpsertPod(terminatingPod("drain-uid", "drain", "2", "10.0.0.5", tOld)) // draining
	s.UpsertPod(runningPod("live-uid", "live", "2", "10.0.0.5", tOld))       // live pod takes it back
	if np, _ := s.GetPodByIP("10.0.0.5"); np.Pod.Name != "live" {
		t.Fatalf("setup: owner = %q, want live", np.Pod.Name)
	}

	// The drainer keeps reporting the address it no longer owns.
	s.UpsertPod(terminatingPod("drain-uid", "drain", "3", "10.0.0.5", tOld))

	np, ok := s.GetPodByIP("10.0.0.5")
	if !ok || np.Pod.Name != "live" {
		t.Fatalf("GetPodByIP = %q (ok=%v), want live: a terminating pod re-asserting a recycled "+
			"address must yield to the live owner, not win on its later acquisition", np.Pod.Name, ok)
	}
}

// PROMOTION applies the same precedence (beatsClaimant): when the holder is
// deleted, a live claimant beats a terminating one that acquired the address
// later. Only the ipSeq half of beatsClaimant was pinned, so deleting its
// terminating comparison promoted the drainer with the suite green.
func TestPromotionPrefersALiveClaimantOverATerminatingOne(t *testing.T) {
	s := New(time.Minute)
	s.UpsertPod(runningPod("live-uid", "live", "1", "10.0.0.9", tOld))       // seq 1
	s.UpsertPod(runningPod("drain-uid", "drain", "1", "10.0.0.9", tOld))     // seq 2
	s.UpsertPod(runningPod("owner-uid", "owner", "1", "10.0.0.9", tOld))     // seq 3: holds it
	s.UpsertPod(terminatingPod("drain-uid", "drain", "2", "10.0.0.9", tOld)) // draining, keeps seq 2

	// The holder goes away. The survivors are a live seq-1 claimant and a
	// draining seq-2 one, so ipSeq alone would promote the drainer.
	s.DeletePod(types.UID("owner-uid"))

	np, ok := s.GetPodByIP("10.0.0.9")
	if !ok {
		t.Fatal("the address resolves to nothing after its holder was deleted")
	}
	if np.Pod.Name != "live" {
		t.Fatalf("promoted %q, want live: promotion must prefer a live claimant over a draining one "+
			"before it falls back to the later acquisition", np.Pod.Name)
	}
}
