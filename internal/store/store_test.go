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
	res, ok := s.GetContainer(ctx, id)
	if !ok {
		t.Fatalf("container %q not found", id)
	}
	return res
}

func mustMiss(t *testing.T, s *Store, id string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, ok := s.GetContainer(ctx, id); ok {
		t.Fatalf("container %q unexpectedly found", id)
	}
}

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

func TestWaitForContainer(t *testing.T) {
	s, _ := newTestStore(time.Minute)

	done := make(chan ContainerResult, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		res, ok := s.GetContainer(ctx, "late999")
		if ok {
			done <- res
		}
		close(done)
	}()

	time.Sleep(50 * time.Millisecond)
	s.UpsertPod(makePod("uid1", "pod1", "node1", "1", map[string]string{"app": "late999"}))

	select {
	case res, ok := <-done:
		if !ok {
			t.Fatal("waiter returned not-found")
		}
		if res.Pod.Name != "pod1" {
			t.Fatalf("got pod %q", res.Pod.Name)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("waiter did not wake up")
	}
}

func TestWaitIsPerContainer(t *testing.T) {
	s, _ := newTestStore(time.Minute)

	got := make(chan bool, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, ok := s.GetContainer(ctx, "wanted1")
		got <- ok
	}()
	other := make(chan bool, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
		defer cancel()
		_, ok := s.GetContainer(ctx, "other2")
		other <- ok
	}()

	time.Sleep(50 * time.Millisecond)
	// Indexing "wanted1" must release only its own waiter.
	s.UpsertPod(makePod("uid1", "pod1", "node1", "1", map[string]string{"app": "wanted1"}))

	if ok := <-got; !ok {
		t.Fatal("waiter for indexed container did not get a result")
	}
	select {
	case ok := <-other:
		if ok {
			t.Fatal("waiter for a different container was satisfied")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("other waiter never timed out")
	}

	// All waiter registrations must be cleaned up.
	s.mu.RLock()
	defer s.mu.RUnlock()
	if len(s.waiters) != 0 {
		t.Fatalf("leaked waiters: %v", s.waiters)
	}
}

func TestManyWaitersSameContainer(t *testing.T) {
	s, _ := newTestStore(time.Minute)

	const n = 20
	results := make(chan bool, n)
	for i := 0; i < n; i++ {
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			res, ok := s.GetContainer(ctx, "shared123")
			results <- ok && res.Pod.Name == "pod1"
		}()
	}

	time.Sleep(50 * time.Millisecond)
	s.UpsertPod(makePod("uid1", "pod1", "node1", "1", map[string]string{"app": "shared123"}))

	for i := 0; i < n; i++ {
		select {
		case ok := <-results:
			if !ok {
				t.Fatalf("waiter %d did not get the pod", i)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("waiter %d never woke up", i)
		}
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if len(s.waiters) != 0 {
		t.Fatalf("leaked waiters: %v", s.waiters)
	}
}

func TestWaitTimesOut(t *testing.T) {
	s, _ := newTestStore(time.Minute)
	start := time.Now()
	mustMiss(t, s, "never")
	if time.Since(start) > time.Second {
		t.Fatal("wait took far longer than its context timeout")
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
