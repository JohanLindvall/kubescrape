package store

import (
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/JohanLindvall/kubescrape/internal/testrace"
)

// resyncPod is the shape a resync re-delivers: an ordinary application pod
// with labels, annotations, owner references and two containers — the fields
// kubeconvert.FromPod deep-copies.
func resyncPod(rv string) *corev1.Pod {
	c := func(name, id string) (corev1.Container, corev1.ContainerStatus) {
		return corev1.Container{
				Name:  name,
				Image: "registry.example.com/" + name + ":v1.2.3",
				Ports: []corev1.ContainerPort{{Name: "metrics", ContainerPort: 9090}},
			}, corev1.ContainerStatus{
				Name:        name,
				ContainerID: "containerd://" + id,
				State:       corev1.ContainerState{Running: &corev1.ContainerStateRunning{}},
			}
	}
	app, appSt := c("app", "aaaa1111")
	side, sideSt := c("sidecar", "bbbb2222")
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "web-abc123", Namespace: "prod", UID: types.UID("uid-1"),
			ResourceVersion: rv,
			Labels: map[string]string{
				"app": "web", "pod-template-hash": "6f9c8d7b5",
				"app.kubernetes.io/name": "web", "app.kubernetes.io/managed-by": "Helm",
			},
			Annotations: map[string]string{
				"prometheus.io/scrape": "true", "prometheus.io/port": "9090",
			},
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "apps/v1", Kind: "ReplicaSet", Name: "web-abc", UID: "rs-1"}},
		},
		Spec: corev1.PodSpec{NodeName: "node-1", Containers: []corev1.Container{app, side}},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning, PodIP: "10.1.2.3",
			ContainerStatuses: []corev1.ContainerStatus{appSt, sideSt},
		},
	}
}

// A resync re-delivers every pod in the cluster byte-identical, and UpsertPod
// has always had a short-circuit for it — but the short-circuit only skipped
// the INDEXING. The conversion ran first, so each of those deliveries deep-
// copied a pod's labels, annotations, owner refs and containers to discard
// them on the next line, and took the store's exclusive lock to discover there
// was nothing to do.
//
// Allocations are the proxy: FromPod's copies are the whole cost, and zero of
// them is the only number that says the conversion did not run.
func TestResyncOfAnUnchangedPodConvertsNothing(t *testing.T) {
	if testrace.Enabled {
		t.Skip("-race perturbs allocation counts")
	}
	s, _ := newTestStore(time.Minute)
	pod := resyncPod("100042")
	s.UpsertPod(pod)

	got := testing.AllocsPerRun(100, func() { s.UpsertPod(pod) })
	if got != 0 {
		t.Errorf("a resync delivery of an unchanged pod allocates %v times, want 0: the conversion runs before the short-circuit", got)
	}
	if n, _ := s.Stats(); n != 1 {
		t.Fatalf("the resyncs changed the store: %d pods", n)
	}
}

// The fast path must not swallow the two cases that look like a resync from
// the resourceVersion alone.
func TestResyncFastPathStillResurrectsAndStillUpdates(t *testing.T) {
	s, clk := newTestStore(time.Minute)
	s.UpsertPod(resyncPod("1"))
	s.DeletePod("uid-1")
	if got := s.PodsOnNode("node-1"); len(got) != 0 {
		t.Fatalf("deleted pod still on the node: %+v", got)
	}

	// A tombstoned record carries the resourceVersion of the delivery that
	// tombstoned it; the upsert that follows must resurrect it, not be read as
	// a resync of a live record.
	s.UpsertPod(resyncPod("1"))
	if got := s.PodsOnNode("node-1"); len(got) != 1 {
		t.Fatalf("a re-delivery after a delete did not resurrect the pod: %+v", got)
	}
	if res := mustGet(t, s, "aaaa1111"); res.Pod.DeletedAt != nil {
		t.Errorf("resurrected pod still carries DeletedAt %v", res.Pod.DeletedAt)
	}

	// And a delivery that really did change something is still applied.
	moved := resyncPod("2")
	moved.Spec.NodeName = "node-2"
	s.UpsertPod(moved)
	if got := s.PodsOnNode("node-1"); len(got) != 0 {
		t.Errorf("pod still listed on its old node: %+v", got)
	}
	if got := s.PodsOnNode("node-2"); len(got) != 1 {
		t.Errorf("pod not listed on its new node: %+v", got)
	}
	clk.Advance(time.Second)
}
