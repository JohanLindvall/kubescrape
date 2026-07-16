package store

// Regression guards for the byPodIP claim ordering. A CreatedAt-ordered claim
// was tried and abandoned; final semantics: every live pod claims
// (last-write-wins) and a TERMINATING pod yields to a live incumbent.

import (
	"testing"
	"time"
)

// TestLateScheduledPodClaimsRecycledIP: the live owner of a recycled IP is
// not necessarily the newest pod. A pod may be created long before it is
// scheduled (unschedulable, waiting on a PVC or a node scale-up) and only then
// get an IP from the CNI — an IP a just-died pod was holding, still in the
// index with phase Running and a LATER CreatedAt. A CreatedAt-ordered claim
// refused the live owner here (answering the ingest peer-IP fallback with the
// dead pod), which is why that design was abandoned: a pod's age says nothing
// about who currently holds the address. This pins the last-write-wins claim.
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
