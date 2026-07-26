package server

// A terminating pod (deletionTimestamp set, phase still Running for the whole
// grace period) must leave the scrape targets — but stay fully resolvable.

import (
	"net/http"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/JohanLindvall/kubescrape/internal/store"
	"github.com/JohanLindvall/kubescrape/pkg/kubemeta"
)

// upsertWebPod stores the annotated pod addPod builds, optionally marked for
// deletion. The resourceVersion changes so the store does not short-circuit
// the second upsert as a resync.
func upsertWebPod(st *store.Store, rv string, deletionTS *metav1.Time) {
	st.UpsertPod(&corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "web-abc-xyz",
			Namespace:         "default",
			UID:               types.UID("pod-uid"),
			ResourceVersion:   rv,
			DeletionTimestamp: deletionTS,
			Labels:            map[string]string{"app": "web"},
			Annotations: map[string]string{
				"prometheus.io/scrape": "true",
				"prometheus.io/port":   "9090",
			},
		},
		Spec: corev1.PodSpec{
			NodeName:   "node1",
			Containers: []corev1.Container{{Name: "app", Image: "img"}},
		},
		Status: corev1.PodStatus{
			// A draining pod keeps reporting Running with its IP and a Ready
			// condition until the kubelet finishes tearing it down.
			Phase:      corev1.PodRunning,
			PodIP:      "10.1.2.3",
			Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}},
			ContainerStatuses: []corev1.ContainerStatus{{
				Name:        "app",
				ContainerID: "containerd://cafe01",
				State:       corev1.ContainerState{Running: &corev1.ContainerStateRunning{}},
			}},
		},
	})
}

func nodeTargets(t *testing.T, url string) []kubemeta.ScrapeTarget {
	t.Helper()
	var resp struct {
		Targets []kubemeta.ScrapeTarget `json:"targets"`
	}
	getJSON(t, url, http.StatusOK, &resp)
	return resp.Targets
}

// Scraping a draining pod produces nothing but connection failures for the
// length of the grace period — `up=0` churn on every rollout. Prometheus'
// endpoints discovery drops terminating endpoints; so do we. The pod must
// nevertheless stay resolvable by container ID, UID and name: its last log
// lines are still being shipped and have to be attributable.
func TestTerminatingPodLeavesTargetsButStaysResolvable(t *testing.T) {
	st := store.New(time.Minute)
	upsertWebPod(st, "1", nil)
	srv := testServer(t, st, closedChan())

	if got := nodeTargets(t, srv.URL+"/v1/nodes/node1/targets"); len(got) != 1 {
		t.Fatalf("live pod targets = %+v, want 1", got)
	}

	// The pod is deleted gracefully: deletionTimestamp set, phase unchanged.
	deletionTS := metav1.NewTime(time.Now())
	upsertWebPod(st, "2", &deletionTS)

	if got := nodeTargets(t, srv.URL+"/v1/nodes/node1/targets"); len(got) != 0 {
		t.Fatalf("terminating pod still yields targets: %+v", got)
	}

	// ... but every metadata lookup still resolves it, with the timestamp
	// visible on the wire and the tombstone marker still unset.
	var md kubemeta.ContainerMetadata
	getJSON(t, srv.URL+"/v1/containers/cafe01", http.StatusOK, &md)
	if md.Pod.Name != "web-abc-xyz" || md.Container.Name != "app" {
		t.Fatalf("container lookup of a terminating pod = %+v", md)
	}
	if md.Pod.DeletionTimestamp == nil || !md.Pod.DeletionTimestamp.Equal(deletionTS.UTC()) {
		t.Errorf("deletionTimestamp = %v, want %v", md.Pod.DeletionTimestamp, deletionTS.UTC())
	}
	if md.Pod.DeletedAt != nil {
		t.Errorf("DeletedAt must stay nil until the pod is actually gone: %v", md.Pod.DeletedAt)
	}
	if !md.Pod.Ready {
		t.Error("Ready must mirror the PodReady condition, which a draining pod still reports")
	}

	var pod kubemeta.Pod
	getJSON(t, srv.URL+"/v1/pods/default/web-abc-xyz", http.StatusOK, &pod)
	if pod.UID != "pod-uid" {
		t.Errorf("pod-name lookup = %+v", pod)
	}
	getJSON(t, srv.URL+"/v1/pod-uids/pod-uid", http.StatusOK, &pod)
	if pod.Name != "web-abc-xyz" {
		t.Errorf("pod-uid lookup = %+v", pod)
	}
}

// Readiness rides along as metadata: a pod failing its readiness probes is
// reported not-ready but is STILL scraped (Prometheus scrapes not-ready
// endpoints too — that is how you see why they are unhealthy).
func TestNotReadyPodStillYieldsTargets(t *testing.T) {
	st := store.New(time.Minute)
	st.UpsertPod(&corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "sick", Namespace: "default", UID: types.UID("sick-uid"), ResourceVersion: "1",
			Annotations: map[string]string{"prometheus.io/scrape": "true", "prometheus.io/port": "9090"},
		},
		Spec: corev1.PodSpec{NodeName: "node1", Containers: []corev1.Container{{Name: "app"}}},
		Status: corev1.PodStatus{
			Phase:      corev1.PodRunning,
			PodIP:      "10.1.2.9",
			Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionFalse}},
		},
	})
	srv := testServer(t, st, closedChan())

	if got := nodeTargets(t, srv.URL+"/v1/nodes/node1/targets"); len(got) != 1 {
		t.Fatalf("not-ready pod targets = %+v, want 1", got)
	} else if got[0].Pod.Ready {
		t.Errorf("pod on the target should report Ready=false: %+v", got[0].Pod)
	}
}
