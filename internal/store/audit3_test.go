package store

// AUDIT round 5 (adversarial review of bbcf4bb): the byPodIP claim is now
// ordered by CreatedAt.

import (
	"testing"
	"time"
)

// TestLateScheduledPodClaimsRecycledIP: CreatedAt orders the CLAIM, but the
// live owner of a recycled IP is not necessarily the newest pod. A pod may be
// created long before it is scheduled (unschedulable, waiting on a PVC or a
// node scale-up) and only then get an IP from the CNI — an IP the pod that just
// died was holding. That dead pod is still in the index with phase Running
// (terminating pods keep it; the delete event has not landed yet), and it has
// the LATER CreatedAt, so the guard now refuses the live owner's claim.
//
// GetPodByIP then answers with the dead pod for as long as its record survives:
// the ingest peer-IP fallback stamps the pending pod's telemetry with the wrong
// k8s.pod.name / service.name. The pre-fix code got this case right (last write
// wins), so this is a new mis-attribution introduced by the ordering, not a
// pre-existing one.
//
// (Related, one line: metav1.Time has SECOND granularity, so two pods created
// within the same second compare equal and the ordering degenerates to
// last-write-wins — the very re-steal the fix exists to prevent.)
func TestLateScheduledPodClaimsRecycledIP(t *testing.T) {
	s := New(time.Minute)

	// A pod created at 10:00, stuck Pending for an hour (no IP yet).
	pending := runningPod("old-uid", "pending", "1", "", tOld)
	s.UpsertPod(pending)

	// A pod created at 11:00 holds 10.0.0.5 and is now terminating (phase stays
	// Running, deletionTimestamp set, until the delete lands).
	s.UpsertPod(terminatingPod("dying-uid", "dying", "1", "10.0.0.5", tNew))

	// The pending pod is finally scheduled and the CNI hands it the freed IP.
	s.UpsertPod(runningPod("old-uid", "pending", "2", "10.0.0.5", tOld))

	np, ok := s.GetPodByIP("10.0.0.5")
	if !ok || np.Pod.UID != "old-uid" {
		t.Fatalf("the live owner of the recycled IP cannot claim it: GetPodByIP = %q (ok=%v), want old-uid; "+
			"the CreatedAt ordering assumes the newest pod is the live owner, which a late-scheduled pod breaks",
			np.Pod.UID, ok)
	}
}
