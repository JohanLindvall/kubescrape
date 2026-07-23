package store

import (
	"context"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

func newTestStore(ttl time.Duration) (*Store, *fakeClock) {
	s := New(ttl)
	clk := &fakeClock{t: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	s.now = clk.Now
	return s, clk
}

func makePod(uid, name, node, rv string, containerIDs map[string]string) *corev1.Pod {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:            name,
			Namespace:       "default",
			UID:             types.UID(uid),
			ResourceVersion: rv,
			Labels:          map[string]string{"app": name},
		},
		Spec: corev1.PodSpec{NodeName: node},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			PodIP: "10.0.0.1",
		},
	}
	for cname, cid := range containerIDs {
		pod.Spec.Containers = append(pod.Spec.Containers, corev1.Container{Name: cname, Image: "img"})
		pod.Status.ContainerStatuses = append(pod.Status.ContainerStatuses, corev1.ContainerStatus{
			Name:        cname,
			ContainerID: "containerd://" + cid,
			State:       corev1.ContainerState{Running: &corev1.ContainerStateRunning{}},
		})
	}
	return pod
}

func mustGet(t *testing.T, s *Store, id string) ContainerResult {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	res, ok, _ := s.GetContainer(ctx, id)
	if !ok {
		t.Fatalf("container %q not found", id)
	}
	return res
}

func mustMiss(t *testing.T, s *Store, id string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, ok, _ := s.GetContainer(ctx, id); ok {
		t.Fatalf("container %q unexpectedly found", id)
	}
}

func TestDeletedPodTombstone(t *testing.T) {
	s, clk := newTestStore(time.Minute)
	s.UpsertPod(makePod("uid1", "pod1", "node1", "1", map[string]string{"app": "abc123"}))
	s.DeletePod("uid1")

	res := mustGet(t, s, "abc123")
	if res.Pod.DeletedAt == nil {
		t.Fatal("tombstoned pod should have DeletedAt set")
	}
	if len(s.PodsOnNode("node1")) != 0 {
		t.Fatal("deleted pod still listed on node")
	}

	clk.Advance(time.Minute + time.Second)
	mustMiss(t, s, "abc123")

	s.Sweep()
	pods, containers := s.Stats()
	if pods != 0 || containers != 0 {
		t.Fatalf("sweep left pods=%d containers=%d", pods, containers)
	}
}

func TestZeroTTLDeletesImmediately(t *testing.T) {
	s, _ := newTestStore(0)
	s.UpsertPod(makePod("uid1", "pod1", "node1", "1", map[string]string{"app": "abc123"}))
	s.DeletePod("uid1")
	mustMiss(t, s, "abc123")
	pods, containers := s.Stats()
	if pods != 0 || containers != 0 {
		t.Fatalf("expected empty store, got pods=%d containers=%d", pods, containers)
	}
}

func TestRestartedContainerOldIDStaysResolvable(t *testing.T) {
	s, clk := newTestStore(time.Minute)
	s.UpsertPod(makePod("uid1", "pod1", "node1", "1", map[string]string{"app": "old111"}))
	// Container restarts: status now reports a new ID and no lastState
	// (simulating the old ID having aged out entirely).
	s.UpsertPod(makePod("uid1", "pod1", "node1", "2", map[string]string{"app": "new222"}))

	if res := mustGet(t, s, "new222"); res.Container.ID != "new222" {
		t.Fatalf("new ID resolves to %q", res.Container.ID)
	}
	// The old ID remains resolvable for the TTL and maps to the same pod.
	res := mustGet(t, s, "old111")
	if res.Pod.UID != "uid1" || res.Container.ID != "old111" {
		t.Fatalf("old ID resolved to pod=%q container=%q", res.Pod.UID, res.Container.ID)
	}

	clk.Advance(time.Minute + time.Second)
	mustMiss(t, s, "old111")
	if res := mustGet(t, s, "new222"); res.Pod.Name != "pod1" {
		t.Fatal("current ID must survive the old ID's expiry")
	}
}

func TestLastTerminationStateIndexed(t *testing.T) {
	s, _ := newTestStore(time.Minute)
	pod := makePod("uid1", "pod1", "node1", "1", map[string]string{"app": "new222"})
	pod.Status.ContainerStatuses[0].RestartCount = 1
	pod.Status.ContainerStatuses[0].LastTerminationState = corev1.ContainerState{
		Terminated: &corev1.ContainerStateTerminated{
			ContainerID: "containerd://old111",
			ExitCode:    137,
		},
	}
	s.UpsertPod(pod)

	res := mustGet(t, s, "old111")
	if res.Container.State != "terminated" || res.Container.ExitCode == nil || *res.Container.ExitCode != 137 {
		t.Fatalf("previous incarnation not reflected: state=%q exitCode=%v", res.Container.State, res.Container.ExitCode)
	}
	if res.Container.RuntimeID != "containerd://old111" {
		t.Fatalf("runtime ID = %q", res.Container.RuntimeID)
	}
}

func TestResurrectAfterOutOfOrderDelete(t *testing.T) {
	s, _ := newTestStore(time.Minute)
	s.UpsertPod(makePod("uid1", "pod1", "node1", "1", map[string]string{"app": "abc"}))
	s.DeletePod("uid1")
	// A late update (event reordering) must bring the pod back to life.
	s.UpsertPod(makePod("uid1", "pod1", "node1", "2", map[string]string{"app": "abc"}))

	res := mustGet(t, s, "abc")
	if res.Pod.DeletedAt != nil {
		t.Fatal("resurrected pod still marked deleted")
	}
	if len(s.PodsOnNode("node1")) != 1 {
		t.Fatal("resurrected pod missing from node index")
	}
}

func TestOwnerRefsCarried(t *testing.T) {
	s, _ := newTestStore(time.Minute)
	pod := makePod("uid1", "pod1", "node1", "1", map[string]string{"app": "abc"})
	ctrl := true
	pod.OwnerReferences = []metav1.OwnerReference{{
		APIVersion: "apps/v1", Kind: "ReplicaSet", Name: "rs1", UID: "rsuid", Controller: &ctrl,
	}}
	s.UpsertPod(pod)

	res := mustGet(t, s, "abc")
	if len(res.OwnerRefs) != 1 || res.OwnerRefs[0].Name != "rs1" {
		t.Fatalf("owner refs = %+v", res.OwnerRefs)
	}
}

// Run is a thin sweeping ticker: it must exit promptly on cancel.
func TestRunExitsOnCancel(t *testing.T) {
	s, _ := newTestStore(time.Minute)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { s.Run(ctx); close(done) }()
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not exit on cancel")
	}
}

// Zero-TTL also applies to the RESTART path: a container ID replaced by a new
// incarnation must expire immediately (expireEntryLocked's ttl<=0 branch), not
// linger, while the new ID keeps resolving.
func TestZeroTTLRestartedContainerDeletesImmediately(t *testing.T) {
	s, _ := newTestStore(0)
	s.UpsertPod(makePod("uid1", "pod1", "node1", "1", map[string]string{"app": "old111"}))
	s.UpsertPod(makePod("uid1", "pod1", "node1", "2", map[string]string{"app": "new222"}))

	mustMiss(t, s, "old111")
	if res := mustGet(t, s, "new222"); res.Container.ID != "new222" {
		t.Fatalf("new ID resolves to %q", res.Container.ID)
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
