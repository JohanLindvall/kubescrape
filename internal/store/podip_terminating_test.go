package store

// The pod-IP index's precedence where a TERMINATING pod is involved, on the two
// paths that decide a holder: the claim path (claimOneIPLocked) and promotion,
// both of which call the one rule, beatsClaimant.
//
// The rule is that ACQUISITION ORDER decides and the terminating bit does not.
// It reads backwards at first, because a draining pod keeps phase Running for
// its whole grace period and goes on reporting its PodIP — but an address is
// not released until the sandbox is torn down, so during that window the
// drainer is still the legitimate holder. A pod whose status carries an address
// "the CNI has already handed to someone else" is by construction the EARLIER
// acquirer and loses on ipSeq with no help from the terminating bit; the only
// shapes in which a live-beats-terminating arm decided anything were the ones
// that argument does not describe, and there it INVERTED the ordering.
//
// Getting it wrong is silent either way: GET /v1/pod-ips and GET /v1/self hand
// back the wrong pod, so the ingest peer-IP fallback stamps its name, UID and
// owners onto the other workload's pushed logs and metrics — and
// kubescrape_pod_ip_contested_total does not move, noteContested excluding
// every claim in which either side is terminating.
//
// What the coverage elsewhere misses is the ORDER: TestStaleUpdateCannotReclaim
// RecycledIP and TestLateScheduledPodClaimsRecycledIP both have the live pod
// acquiring the address LAST, so they are decided by ipSeq whichever way the
// terminating bits are read. Every case below gives the TERMINATING pod the
// higher ipSeq, which is the only shape that tells the two rules apart.

import (
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/types"
)

// A DRAINING holder keeps its address against a live pod that acquired the
// address before it and is merely re-asserting: the drainer has not released
// anything yet, and the live pod's claim is the one the ipSeq ordering already
// ruled stale. The hand-off happens one event later, on real evidence — the
// drainer's deletion — via promotion.
//
// Before this, any status churn on the stale pod flipped the index, and the
// flip did NOT heal when the drainer was finally deleted (releaseIPLocked
// promotes only when the leaver still held the address): it stood until some
// new pod genuinely acquired the address.
func TestADrainingHolderKeepsItsAddressUntilItIsReleased(t *testing.T) {
	s := New(time.Minute)
	s.UpsertPod(runningPod("stale-uid", "stale", "1", "10.0.0.5", tOld))     // acquires first: seq 1
	s.UpsertPod(runningPod("drain-uid", "drain", "1", "10.0.0.5", tOld))     // later acquisition: holds it
	s.UpsertPod(terminatingPod("drain-uid", "drain", "2", "10.0.0.5", tOld)) // now draining, still seq 2

	// A routine update to the stale pod — same address, so this is a re-assert
	// and not a new acquisition: its ipSeq does not move.
	s.UpsertPod(runningPod("stale-uid", "stale", "2", "10.0.0.5", tOld))

	np, ok := s.GetPodByIP("10.0.0.5")
	if !ok || np.Pod.Name != "drain" {
		t.Fatalf("GetPodByIP = %q (ok=%v), want drain: a draining pod still holds its address until its "+
			"sandbox is torn down, so an earlier acquirer re-asserting must not take it back", np.Pod.Name, ok)
	}

	// The drainer's deletion is the release, and promotion is the hand-off.
	s.DeletePod(types.UID("drain-uid"))
	np, ok = s.GetPodByIP("10.0.0.5")
	if !ok || np.Pod.Name != "stale" {
		t.Fatalf("GetPodByIP = %q (ok=%v) after the holder was deleted, want stale: the surviving "+
			"claimant must be promoted", np.Pod.Name, ok)
	}
}

// The mirror: a TERMINATING pod re-asserting must not take the address back
// from the pod that acquired it later. This is the routine case — a drained
// pod's status updates keep carrying the recycled IP for the whole grace
// period.
func TestTerminatingClaimantDoesNotStealFromTheLaterAcquirer(t *testing.T) {
	s := New(time.Minute)
	s.UpsertPod(runningPod("drain-uid", "drain", "1", "10.0.0.5", tOld))     // acquires first: seq 1
	s.UpsertPod(runningPod("live-uid", "live", "1", "10.0.0.5", tOld))       // later acquisition: holds it
	s.UpsertPod(terminatingPod("drain-uid", "drain", "2", "10.0.0.5", tOld)) // draining, keeps seq 1

	np, ok := s.GetPodByIP("10.0.0.5")
	if !ok || np.Pod.Name != "live" {
		t.Fatalf("GetPodByIP = %q (ok=%v), want live: a terminating pod re-asserting a recycled "+
			"address must yield to the later acquirer", np.Pod.Name, ok)
	}
}

// The ordinary hand-off still happens at the claim door when the evidence is
// there: a pod that ACQUIRES an address a drainer is holding takes it, because
// its acquisition is later. That is not a contested claim — noteContested
// excludes it, an address changing hands from a drainer being the ordinary way
// one is released rather than a window in which a lookup was wrong.
func TestALaterAcquisitionTakesTheAddressFromADrainer(t *testing.T) {
	s := New(time.Minute)
	s.UpsertPod(runningPod("drain-uid", "drain", "1", "10.0.0.5", tOld))     // seq 1: holds it
	s.UpsertPod(terminatingPod("drain-uid", "drain", "2", "10.0.0.5", tOld)) // draining
	// A pod that had no address at all is scheduled and the CNI hands it the
	// one the drainer is giving up: a genuine acquisition, so seq 2.
	s.UpsertPod(runningPod("next-uid", "next", "1", "", tOld))
	s.UpsertPod(runningPod("next-uid", "next", "2", "10.0.0.5", tOld))

	np, ok := s.GetPodByIP("10.0.0.5")
	if !ok || np.Pod.Name != "next" {
		t.Fatalf("GetPodByIP = %q (ok=%v), want next: a genuine later acquisition must take the "+
			"address from a drainer", np.Pod.Name, ok)
	}
	if got := s.ContestedPodIPs(); got != 0 {
		t.Fatalf("ContestedPodIPs = %d, want 0: a hand-off from a terminating holder is the ordinary "+
			"release, not a window in which a peer-IP lookup could have been wrong", got)
	}
}

// PROMOTION applies the same precedence (beatsClaimant): when the holder is
// deleted, the LATER acquirer wins even when it is draining, for the reason the
// claim path applies — it has not released the address yet, and its own
// deletion brings the promotion round again. Only the claim path was pinned for
// this, so when promotion carried its own copy of the rule, replacing that
// copy's ipSeq comparison with a terminating one promoted the stale pod with the
// suite green.
func TestPromotionPrefersTheLaterAcquirerEvenWhenItIsDraining(t *testing.T) {
	s := New(time.Minute)
	s.UpsertPod(runningPod("stale-uid", "stale", "1", "10.0.0.9", tOld))     // seq 1
	s.UpsertPod(runningPod("drain-uid", "drain", "1", "10.0.0.9", tOld))     // seq 2
	s.UpsertPod(runningPod("owner-uid", "owner", "1", "10.0.0.9", tOld))     // seq 3: holds it
	s.UpsertPod(terminatingPod("drain-uid", "drain", "2", "10.0.0.9", tOld)) // draining, keeps seq 2

	// The holder goes away. The survivors are a live seq-1 claimant and a
	// draining seq-2 one.
	s.DeletePod(types.UID("owner-uid"))

	np, ok := s.GetPodByIP("10.0.0.9")
	if !ok {
		t.Fatal("the address resolves to nothing after its holder was deleted")
	}
	if np.Pod.Name != "drain" {
		t.Fatalf("promoted %q, want drain: promotion must follow the claim path's precedence — the "+
			"later acquisition, whether or not that pod is draining", np.Pod.Name)
	}

	// And when the drainer is deleted in turn, the last claimant standing gets
	// it: nothing is stranded by preferring the drainer above.
	s.DeletePod(types.UID("drain-uid"))
	if np, ok := s.GetPodByIP("10.0.0.9"); !ok || np.Pod.Name != "stale" {
		t.Fatalf("GetPodByIP = %q (ok=%v), want stale: the surviving claimant must be promoted",
			np.Pod.Name, ok)
	}
}

// The informer's initial LIST — every restart of this process — delivers every
// pod as a FIRST SIGHTING, in list (namespace/name) order rather than in the
// order the CNI handed out their addresses, so ipSeq minted there is not
// acquisition evidence. For a pod first seen ALREADY DRAINING beside a live
// claimant of the same (recycled) address, "the drainer is the earlier acquirer
// by construction" does not hold either, and which of the two held the address
// came down to their NAMES: the later-sorting pod won, silently (noteContested
// skips terminating pairs), until the drainer was deleted — its grace period,
// or indefinitely for a pod stuck Terminating on a lost node. A first sighting
// of a drainer therefore carries no sequence at all: the live claimant holds
// the address in BOTH orders, and the drainer's later re-asserts do not take it.
func TestStartupListOrderDoesNotHandTheAddressToAStaleDrainer(t *testing.T) {
	for _, liveFirst := range []bool{true, false} {
		s := New(time.Minute)
		live := func() { s.UpsertPod(runningPod("live-uid", "a-live", "1", "10.0.0.5", tOld)) }
		drain := func() { s.UpsertPod(terminatingPod("drain-uid", "z-drain", "1", "10.0.0.5", tOld)) }
		if liveFirst {
			live()
			drain()
		} else {
			drain()
			live()
		}
		// A routine status update to the drainer, still reporting the address.
		s.UpsertPod(terminatingPod("drain-uid", "z-drain", "2", "10.0.0.5", tOld))

		np, ok := s.GetPodByIP("10.0.0.5")
		if !ok || np.Pod.Name != "a-live" {
			t.Fatalf("liveFirst=%v: GetPodByIP = %q (ok=%v), want a-live: a pod first seen draining carries "+
				"no acquisition evidence, so the list order must not hand it the address", liveFirst, np.Pod.Name, ok)
		}
	}
}

// The no-evidence rule is only about FIRST SIGHTINGS. A drainer first seen
// alone still resolves (nobody else claims the address), and a genuine later
// acquisition still takes the address from it — as does any live pod.
func TestAFirstSeenDrainerStillHoldsAnUnclaimedAddress(t *testing.T) {
	s := New(time.Minute)
	s.UpsertPod(terminatingPod("drain-uid", "drain", "1", "10.0.0.7", tOld))
	if np, ok := s.GetPodByIP("10.0.0.7"); !ok || np.Pod.Name != "drain" {
		t.Fatalf("GetPodByIP = %q (ok=%v), want drain: an address nobody else claims is still its", np.Pod.Name, ok)
	}
	s.UpsertPod(runningPod("next-uid", "next", "1", "", tOld))
	s.UpsertPod(runningPod("next-uid", "next", "2", "10.0.0.7", tOld))
	if np, ok := s.GetPodByIP("10.0.0.7"); !ok || np.Pod.Name != "next" {
		t.Fatalf("GetPodByIP = %q (ok=%v), want next: a genuine later acquisition takes the address", np.Pod.Name, ok)
	}
}
