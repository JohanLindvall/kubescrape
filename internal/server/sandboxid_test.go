package server

import (
	"context"
	"errors"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/JohanLindvall/kubescrape/internal/store"
	"github.com/JohanLindvall/kubescrape/pkg/metaclient"
)

// A pod's SANDBOX (pause) container id is not resolvable by container id, and
// the agent's cgroup sampler leans on exactly that.
//
// internal/agent/cgroupstats discovers cgroups, not containerStatuses: the
// sandbox's cgroup is an ordinary child of the pod slice, so it looks like any
// other container and would mint a second OTLP resource per pod — with the
// pause container's four megabytes reported under a workload-shaped
// identity — if anything downstream were willing to name it. Nothing is,
// because the sampler exports only what RESOLVES, and this is the proof that a
// sandbox id does not: through the real store, the real HTTP handler and the
// real metaclient, not a fake that was told to say no.
//
// It is a contract test in both directions. If the store ever began indexing
// sandbox ids (a CRI status field appearing in kubemeta.Pod.Containers, say),
// the sampler's phantom-resource-per-pod would come back silently — the six
// gauges would simply start appearing for objects with no workload behind
// them — and this is what would fail instead.
func TestSandboxContainerIDDoesNotResolve(t *testing.T) {
	const (
		appID     = "1111111111111111111111111111111111111111111111111111111111111111"
		sandboxID = "9999999999999999999999999999999999999999999999999999999999999999"
	)
	st := store.New(time.Minute)
	st.UpsertPod(&corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "web-1", Namespace: "default", UID: types.UID("pod-uid"), ResourceVersion: "1",
		},
		Spec: corev1.PodSpec{NodeName: "node1", Containers: []corev1.Container{{Name: "app", Image: "img"}}},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			// The kubelet reports the WORKLOAD containers here and never the
			// sandbox: the pause container is the runtime's, not the pod spec's.
			ContainerStatuses: []corev1.ContainerStatus{{
				Name:        "app",
				ContainerID: "containerd://" + appID,
				State:       corev1.ContainerState{Running: &corev1.ContainerStateRunning{}},
			}},
		},
	})
	srv := testServer(t, st, closedChan())
	cl := metaclient.New(metaclient.Config{Base: srv.URL, Timeout: 5 * time.Second})
	ctx := context.Background()

	// The workload container resolves, so the fixture is not vacuous.
	if md, err := cl.Container(ctx, appID, 0); err != nil || md == nil {
		t.Fatalf("Container(app) = %v, %v; want the workload container to resolve", md, err)
	}
	// The sandbox does not — a not-found, indistinguishable from any other
	// unknown id, which is exactly what the sampler treats as "do not export".
	md, err := cl.Container(ctx, sandboxID, 0)
	if err == nil {
		t.Fatalf("the pod sandbox resolved to %+v; every pod would then export a phantom second resource carrying the pause container's numbers", md)
	}
	var se *metaclient.StatusError
	if !errors.As(err, &se) || se.Code != 404 {
		t.Fatalf("Container(sandbox) failed with %v, want a 404 (the sampler retries a transport failure and declines a miss)", err)
	}
}
