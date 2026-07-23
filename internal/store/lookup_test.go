// Tests for the read-side lookups and indexes (lookup.go): container-ID,
// pod-UID/name/IP and node indexes, including pod-IP claim precedence.
package store

import (
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

func TestLookupByContainerID(t *testing.T) {
	s, _ := newTestStore(time.Minute)
	s.UpsertPod(makePod("uid1", "pod1", "node1", "1", map[string]string{"app": "abc123"}))

	for _, id := range []string{"abc123", "containerd://abc123", "docker://abc123"} {
		res := mustGet(t, s, id)
		if res.Pod.Name != "pod1" || res.Container.Name != "app" || res.Container.ID != "abc123" {
			t.Errorf("lookup %q: got pod=%q container=%q id=%q", id, res.Pod.Name, res.Container.Name, res.Container.ID)
		}
		if res.Pod.DeletedAt != nil {
			t.Errorf("live pod has DeletedAt set")
		}
	}
	mustMiss(t, s, "nope")
	mustMiss(t, s, "")
}

func TestPodsOnNode(t *testing.T) {
	s, _ := newTestStore(time.Minute)
	s.UpsertPod(makePod("uid1", "pod1", "node1", "1", map[string]string{"a": "id1"}))
	s.UpsertPod(makePod("uid2", "pod2", "node1", "1", map[string]string{"a": "id2"}))
	s.UpsertPod(makePod("uid3", "pod3", "node2", "1", map[string]string{"a": "id3"}))

	if got := len(s.PodsOnNode("node1")); got != 2 {
		t.Fatalf("node1 has %d pods, want 2", got)
	}
	if got := len(s.PodsOnNode("node2")); got != 1 {
		t.Fatalf("node2 has %d pods, want 1", got)
	}
	if got := len(s.PodsOnNode("other")); got != 0 {
		t.Fatalf("unknown node has %d pods, want 0", got)
	}

	s.DeletePod("uid2")
	if got := len(s.PodsOnNode("node1")); got != 1 {
		t.Fatalf("node1 has %d pods after delete, want 1", got)
	}
}

func TestGetPodByName(t *testing.T) {
	s, clk := newTestStore(time.Minute)
	s.UpsertPod(makePod("uid1", "pod1", "node1", "1", map[string]string{"app": "abc"}))

	np, ok := s.GetPodByName("default", "pod1")
	if !ok || np.Pod.UID != "uid1" {
		t.Fatalf("lookup failed: ok=%v pod=%+v", ok, np.Pod)
	}
	if _, ok := s.GetPodByName("default", "nope"); ok {
		t.Fatal("unknown pod resolved")
	}
	if _, ok := s.GetPodByName("other", "pod1"); ok {
		t.Fatal("wrong namespace resolved")
	}

	// Deleted pods stay resolvable until the tombstone expires...
	s.DeletePod("uid1")
	np, ok = s.GetPodByName("default", "pod1")
	if !ok || np.Pod.DeletedAt == nil {
		t.Fatalf("tombstone lookup: ok=%v deletedAt=%v", ok, np.Pod.DeletedAt)
	}
	// ...unless a new pod with the same name replaces them.
	s.UpsertPod(makePod("uid2", "pod1", "node1", "1", map[string]string{"app": "def"}))
	np, ok = s.GetPodByName("default", "pod1")
	if !ok || np.Pod.UID != "uid2" || np.Pod.DeletedAt != nil {
		t.Fatalf("replacement lookup: %+v", np.Pod)
	}

	// Expiry of the old tombstone must not evict the replacement.
	clk.Advance(2 * time.Minute)
	s.Sweep()
	if np, ok = s.GetPodByName("default", "pod1"); !ok || np.Pod.UID != "uid2" {
		t.Fatalf("replacement gone after sweep: ok=%v", ok)
	}

	s.DeletePod("uid2")
	clk.Advance(2 * time.Minute)
	s.Sweep()
	if _, ok := s.GetPodByName("default", "pod1"); ok {
		t.Fatal("expired tombstone still resolvable")
	}
}

func TestGetPodByUID(t *testing.T) {
	s, clk := newTestStore(time.Minute)
	s.UpsertPod(makePod("uid1", "pod1", "node1", "1", map[string]string{"app": "abc"}))

	np, ok := s.GetPodByUID("uid1")
	if !ok || np.Pod.Name != "pod1" {
		t.Fatalf("GetPodByUID = %+v ok=%v", np.Pod, ok)
	}
	if _, ok := s.GetPodByUID("nope"); ok {
		t.Fatal("unknown uid resolved")
	}

	// Deleted pods stay resolvable until the tombstone expires (as the
	// container endpoint does), then disappear.
	s.DeletePod("uid1")
	if _, ok := s.GetPodByUID("uid1"); !ok {
		t.Fatal("deleted pod not resolvable within TTL")
	}
	clk.Advance(2 * time.Minute)
	s.Sweep()
	if _, ok := s.GetPodByUID("uid1"); ok {
		t.Fatal("expired pod still resolvable by uid")
	}
}

// GetPodByIP resolves only live, non-hostNetwork pods, and follows IP changes.
func TestGetPodByIP(t *testing.T) {
	s := New(time.Minute)
	s.UpsertPod(&corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "p1", Namespace: "ns", UID: "u1", ResourceVersion: "1"},
		Status:     corev1.PodStatus{PodIP: "10.0.0.5", HostIP: "192.168.1.1"},
	})
	if np, ok := s.GetPodByIP("10.0.0.5"); !ok || np.Pod.Name != "p1" {
		t.Fatalf("live pod by IP: %+v, %v", np, ok)
	}

	// hostNetwork pod (PodIP == HostIP) is never indexed.
	s.UpsertPod(&corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "hostnet", Namespace: "ns", UID: "u2", ResourceVersion: "1"},
		Status:     corev1.PodStatus{PodIP: "192.168.1.1", HostIP: "192.168.1.1"},
	})
	if _, ok := s.GetPodByIP("192.168.1.1"); ok {
		t.Fatal("hostNetwork pod must not resolve by IP")
	}

	// IP change moves the mapping.
	s.UpsertPod(&corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "p1", Namespace: "ns", UID: "u1", ResourceVersion: "2"},
		Status:     corev1.PodStatus{PodIP: "10.0.0.6", HostIP: "192.168.1.1"},
	})
	if _, ok := s.GetPodByIP("10.0.0.5"); ok {
		t.Fatal("old IP must not resolve after a change")
	}
	if np, ok := s.GetPodByIP("10.0.0.6"); !ok || np.Pod.Name != "p1" {
		t.Fatalf("new IP: %+v, %v", np, ok)
	}

	// Deleted pods stop resolving immediately (their IP is recycled), even
	// though the tombstone keeps other lookups alive.
	s.DeletePod("u1")
	if _, ok := s.GetPodByIP("10.0.0.6"); ok {
		t.Fatal("deleted pod must not resolve by IP")
	}
	if _, ok := s.GetPodByUID("u1"); !ok {
		t.Fatal("tombstone must still resolve by UID")
	}
}

// A finished pod's status may retain a podIP the CNI has already handed to a
// live pod; the finished pod must neither steal the mapping nor resolve.
func TestGetPodByIPIgnoresFinishedPods(t *testing.T) {
	s := New(time.Minute)
	s.UpsertPod(&corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "live", Namespace: "ns", UID: "live-uid", ResourceVersion: "1"},
		Status:     corev1.PodStatus{Phase: corev1.PodRunning, PodIP: "10.0.0.7", HostIP: "192.168.1.1"},
	})
	// A Succeeded Job pod whose upsert arrives later, still reporting the
	// same (recycled) IP, must not displace the live owner.
	s.UpsertPod(&corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "done", Namespace: "ns", UID: "done-uid", ResourceVersion: "1"},
		Status:     corev1.PodStatus{Phase: corev1.PodSucceeded, PodIP: "10.0.0.7", HostIP: "192.168.1.1"},
	})
	if np, ok := s.GetPodByIP("10.0.0.7"); !ok || np.Pod.UID != "live-uid" {
		t.Fatalf("recycled IP must resolve to the live pod: %+v, %v", np.Pod, ok)
	}

	// A finished pod's IP alone (no live claimant) must not resolve either.
	s.UpsertPod(&corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "failed", Namespace: "ns", UID: "failed-uid", ResourceVersion: "1"},
		Status:     corev1.PodStatus{Phase: corev1.PodFailed, PodIP: "10.0.0.8", HostIP: "192.168.1.1"},
	})
	if _, ok := s.GetPodByIP("10.0.0.8"); ok {
		t.Fatal("finished pod must not resolve by IP")
	}

	// A running pod that finishes releases its mapping on the phase update.
	s.UpsertPod(&corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "job", Namespace: "ns", UID: "job-uid", ResourceVersion: "1"},
		Status:     corev1.PodStatus{Phase: corev1.PodRunning, PodIP: "10.0.0.9", HostIP: "192.168.1.1"},
	})
	if _, ok := s.GetPodByIP("10.0.0.9"); !ok {
		t.Fatal("running pod must resolve by IP")
	}
	s.UpsertPod(&corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "job", Namespace: "ns", UID: "job-uid", ResourceVersion: "2"},
		Status:     corev1.PodStatus{Phase: corev1.PodSucceeded, PodIP: "10.0.0.9", HostIP: "192.168.1.1"},
	})
	if _, ok := s.GetPodByIP("10.0.0.9"); ok {
		t.Fatal("pod that finished must stop resolving by IP")
	}
}

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

// A hostNetwork pod must never claim its IP in the byPodIP index even when the
// upsert carries status.podIP before status.hostIP is populated: the value
// comparison (PodIP == HostIP) misses then, and only spec.hostNetwork tells
// the truth. Peer-IP fallback would otherwise attribute any hostNetwork sender
// on that node to this pod for a status-update window.
func TestHostNetworkPodNeverClaimsIPEvenBeforeHostIPSet(t *testing.T) {
	s := New(time.Minute)
	p := runningPod("u1", "hostnet", "1", "192.168.1.1", tOld)
	p.Spec.HostNetwork = true
	p.Status.HostIP = "" // podIP populated first; hostIP not yet
	s.UpsertPod(p)

	if got, ok := s.GetPodByIP("192.168.1.1"); ok {
		t.Fatalf("hostNetwork pod claimed the node IP: %+v", got.Pod.Name)
	}
}

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
