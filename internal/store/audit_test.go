package store

import (
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// audit_test.go: targeted tests from the 2026-07 audit of the metadata
// service ingest paths.

// runningPod builds a Running pod. created varies across the pods so the
// tests can model an IP being released by one pod and handed to another; the
// claim itself is last-write-wins with terminating pods yielding (creation
// time deliberately does NOT order it).
func runningPod(uid, name, rv, podIP string, created time.Time) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: name, Namespace: "ns", UID: types.UID(uid), ResourceVersion: rv,
			CreationTimestamp: metav1.NewTime(created),
		},
		Status: corev1.PodStatus{Phase: corev1.PodRunning, PodIP: podIP, HostIP: "192.168.1.1"},
	}
}

// terminatingPod is a Running pod that has been marked for deletion (graceful
// teardown in progress): its status still reports podIP, but a deletionTimestamp
// is set.
func terminatingPod(uid, name, rv, podIP string, created time.Time) *corev1.Pod {
	p := runningPod(uid, name, rv, podIP, created)
	dt := metav1.NewTime(created.Add(time.Hour))
	p.DeletionTimestamp = &dt
	return p
}

var (
	tOld = time.Date(2026, 7, 14, 10, 0, 0, 0, time.UTC)
	tNew = time.Date(2026, 7, 14, 11, 0, 0, 0, time.UTC)
)

// A pod IP is recycled to a new pod while the old pod is still Running per a
// stale informer view. The new claimant wins the index (last upsert), and —
// critically — deleting the OLD pod must not clear the index entry out from
// under the live owner. This is the guarded path (byPodIP[ip] == rec) and it
// holds.
func TestDeleteOfStaleIPClaimantKeepsLiveMapping(t *testing.T) {
	s := New(time.Minute)
	s.UpsertPod(runningPod("a-uid", "old", "1", "10.0.0.5", tOld))
	// CNI hands 10.0.0.5 to the new pod before the old pod's delete arrives.
	s.UpsertPod(runningPod("b-uid", "new", "1", "10.0.0.5", tNew))
	if np, ok := s.GetPodByIP("10.0.0.5"); !ok || np.Pod.UID != "b-uid" {
		t.Fatalf("recycled IP should resolve to the newest claimant: %+v ok=%v", np.Pod, ok)
	}
	// The old pod's delete event finally lands: it no longer owns the index
	// entry and must not clear it.
	s.DeletePod("a-uid")
	if np, ok := s.GetPodByIP("10.0.0.5"); !ok || np.Pod.UID != "b-uid" {
		t.Fatalf("deleting the stale claimant cleared the live mapping: %+v ok=%v", np.Pod, ok)
	}
}

// Regression guard: same scenario, but the STALE pod receives one more
// unrelated update (any status/condition change bumps ResourceVersion —
// routine while a pod terminates) after the new pod claimed the IP. UpsertPod
// used to unconditionally re-stamp byPodIP[ip] for a Running-phase pod, so
// the terminating pod stole the mapping back from the live owner and its
// later deletion removed the entry entirely. The fix: a terminating pod
// (DeletionTimestamp set) yields its IP claim to a live incumbent.
func TestStaleUpdateCannotReclaimRecycledIP(t *testing.T) {
	s := New(time.Minute)
	s.UpsertPod(runningPod("a-uid", "old", "1", "10.0.0.5", tOld))
	// IP recycled to the new pod.
	s.UpsertPod(runningPod("b-uid", "new", "1", "10.0.0.5", tNew))
	// Stale-but-Running update for the old pod (a condition change while it
	// drains, deletionTimestamp now set) tries to re-claim the index entry.
	s.UpsertPod(terminatingPod("a-uid", "old", "2", "10.0.0.5", tOld))
	if np, ok := s.GetPodByIP("10.0.0.5"); !ok || np.Pod.UID != "b-uid" {
		t.Errorf("stale update shadowed the live IP owner: got %q ok=%v, want b-uid", np.Pod.UID, ok)
	}
	// The old pod's deletion now clears the mapping even though a live
	// Running pod still owns the IP.
	s.DeletePod("a-uid")
	if np, ok := s.GetPodByIP("10.0.0.5"); !ok || np.Pod.UID != "b-uid" {
		t.Errorf("live pod lost its IP mapping after the stale claimant's deletion: got %q ok=%v, want b-uid", np.Pod.UID, ok)
	}
}

// A pod whose nodeName changes between upserts (informer replay after a
// forced delete/re-create with the same UID is the only realistic path) must
// leave exactly one node-index entry.
func TestNodeIndexFollowsNodeNameChange(t *testing.T) {
	s, _ := newTestStore(time.Minute)
	s.UpsertPod(makePod("uid1", "pod1", "node1", "1", map[string]string{"app": "id1"}))
	s.UpsertPod(makePod("uid1", "pod1", "node2", "2", map[string]string{"app": "id1"}))

	if got := len(s.PodsOnNode("node1")); got != 0 {
		t.Fatalf("old node still lists the pod: %d", got)
	}
	if got := len(s.PodsOnNode("node2")); got != 1 {
		t.Fatalf("new node has %d pods, want 1", got)
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if _, ok := s.byNode["node1"]; ok {
		t.Fatal("empty node-index bucket for node1 not removed")
	}
}

// DeletePod followed by a re-upsert of the SAME UID with DIFFERENT container
// IDs (delete event raced ahead of the last status update): the pod
// resurrects, the new IDs index live, and the IDs from before the delete keep
// the delete-time tombstone TTL rather than becoming immortal or vanishing.
func TestResurrectWithChangedContainerIDs(t *testing.T) {
	s, clk := newTestStore(time.Minute)
	s.UpsertPod(makePod("uid1", "pod1", "node1", "1", map[string]string{"app": "before1"}))
	s.DeletePod("uid1")
	s.UpsertPod(makePod("uid1", "pod1", "node1", "2", map[string]string{"app": "after2"}))

	res := mustGet(t, s, "after2")
	if res.Pod.DeletedAt != nil {
		t.Fatal("resurrected pod still marked deleted")
	}
	// The pre-delete ID stays resolvable for the TTL stamped at delete time...
	if res := mustGet(t, s, "before1"); res.Pod.UID != "uid1" {
		t.Fatalf("pre-delete ID resolved to %q", res.Pod.UID)
	}
	// ...and expires on schedule.
	clk.Advance(time.Minute + time.Second)
	mustMiss(t, s, "before1")
	if res := mustGet(t, s, "after2"); res.Pod.Name != "pod1" {
		t.Fatal("live ID must survive the old ID's expiry")
	}
}
