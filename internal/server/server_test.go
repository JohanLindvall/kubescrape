package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"

	"github.com/JohanLindvall/kubescrape/internal/kubemeta"
	"github.com/JohanLindvall/kubescrape/internal/services"
	"github.com/JohanLindvall/kubescrape/internal/store"
)

type stubResolver struct{}

func (stubResolver) Resolve(_ string, refs []metav1.OwnerReference) []kubemeta.Owner {
	out := make([]kubemeta.Owner, 0, len(refs))
	for _, r := range refs {
		out = append(out, kubemeta.Owner{APIVersion: r.APIVersion, Kind: r.Kind, Name: r.Name, UID: string(r.UID)})
	}
	return out
}

func (stubResolver) Namespace(name string) *kubemeta.ObjectMeta {
	return &kubemeta.ObjectMeta{UID: "ns-" + name, Labels: map[string]string{"kubernetes.io/metadata.name": name}}
}

func (stubResolver) Node(name string) *kubemeta.ObjectMeta {
	if name == "node1" {
		return &kubemeta.ObjectMeta{UID: "node-uid", Labels: map[string]string{"agentpool": "system"}}
	}
	return nil
}

func closedChan() <-chan struct{} {
	ch := make(chan struct{})
	close(ch)
	return ch
}

func testServer(t *testing.T, st *store.Store, ready <-chan struct{}) *httptest.Server {
	return testServerWithServices(t, st, services.NewIndex(), ready)
}

func testServerWithServices(t *testing.T, st *store.Store, idx *services.Index, ready <-chan struct{}) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(New(Config{
		Store:    st,
		Services: idx,
		Resolver: stubResolver{},
		MaxWait:  500 * time.Millisecond,
		Ready:    ready,
	}).Handler())
	t.Cleanup(srv.Close)
	return srv
}

func addPod(st *store.Store) {
	ctrl := true
	st.UpsertPod(&corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "web-abc-xyz",
			Namespace:       "default",
			UID:             types.UID("pod-uid"),
			ResourceVersion: "1",
			Labels:          map[string]string{"app": "web"},
			Annotations: map[string]string{
				"prometheus.io/scrape": "true",
				"prometheus.io/port":   "9090",
			},
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "apps/v1", Kind: "ReplicaSet", Name: "web-abc", UID: "rs-uid", Controller: &ctrl,
			}},
		},
		Spec: corev1.PodSpec{
			NodeName:   "node1",
			Containers: []corev1.Container{{Name: "app", Image: "img"}},
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			PodIP: "10.1.2.3",
			ContainerStatuses: []corev1.ContainerStatus{{
				Name:        "app",
				ContainerID: "containerd://cafe01",
				State:       corev1.ContainerState{Running: &corev1.ContainerStateRunning{}},
			}},
		},
	})
}

func getJSON(t *testing.T, url string, wantStatus int, v any) {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != wantStatus {
		t.Fatalf("GET %s: status %d, want %d", url, resp.StatusCode, wantStatus)
	}
	if v != nil {
		if err := json.NewDecoder(resp.Body).Decode(v); err != nil {
			t.Fatalf("decoding response: %v", err)
		}
	}
}

func TestContainerEndpoint(t *testing.T) {
	st := store.New(time.Minute)
	addPod(st)
	srv := testServer(t, st, closedChan())

	var md kubemeta.ContainerMetadata
	getJSON(t, srv.URL+"/v1/containers/cafe01", http.StatusOK, &md)
	if md.Pod.Name != "web-abc-xyz" || md.Container.Name != "app" || md.ContainerID != "cafe01" {
		t.Fatalf("metadata = %+v", md)
	}
	if len(md.Pod.Owners) != 1 || md.Pod.Owners[0].Kind != "ReplicaSet" {
		t.Fatalf("owners = %+v", md.Pod.Owners)
	}
	if md.Pod.NamespaceMetadata == nil || md.Pod.NamespaceMetadata.UID != "ns-default" {
		t.Fatalf("namespace metadata = %+v", md.Pod.NamespaceMetadata)
	}

	// Prefixed and URL-escaped forms also resolve.
	getJSON(t, srv.URL+"/v1/containers/containerd%3A%2F%2Fcafe01", http.StatusOK, nil)
	getJSON(t, srv.URL+"/v1/containers/docker://cafe01", http.StatusOK, nil)
}

func TestContainerNotFound(t *testing.T) {
	st := store.New(time.Minute)
	srv := testServer(t, st, closedChan())

	start := time.Now()
	getJSON(t, srv.URL+"/v1/containers/nope?wait=50ms", http.StatusNotFound, nil)
	if elapsed := time.Since(start); elapsed < 40*time.Millisecond {
		t.Fatalf("returned after %v; should have waited ~50ms", elapsed)
	}
	getJSON(t, srv.URL+"/v1/containers/nope?wait=0", http.StatusNotFound, nil)
}

func TestContainerWaitsForLateMetadata(t *testing.T) {
	st := store.New(time.Minute)
	srv := testServer(t, st, closedChan())

	go func() {
		time.Sleep(50 * time.Millisecond)
		addPod(st)
	}()
	var md kubemeta.ContainerMetadata
	getJSON(t, srv.URL+"/v1/containers/cafe01", http.StatusOK, &md)
	if md.Pod.Name != "web-abc-xyz" {
		t.Fatalf("metadata = %+v", md)
	}
}

func TestBadWaitParameter(t *testing.T) {
	st := store.New(time.Minute)
	srv := testServer(t, st, closedChan())
	getJSON(t, srv.URL+"/v1/containers/x?wait=bogus", http.StatusBadRequest, nil)
	getJSON(t, srv.URL+"/v1/containers/x?wait=-1s", http.StatusBadRequest, nil)
}

func TestPodEndpoint(t *testing.T) {
	st := store.New(time.Minute)
	addPod(st)
	srv := testServer(t, st, closedChan())

	var pod kubemeta.Pod
	getJSON(t, srv.URL+"/v1/pods/default/web-abc-xyz", http.StatusOK, &pod)
	if pod.UID != "pod-uid" || len(pod.Owners) != 1 || pod.NamespaceMetadata == nil {
		t.Fatalf("pod = %+v", pod)
	}
	getJSON(t, srv.URL+"/v1/pods/default/nope", http.StatusNotFound, nil)
}

func TestNodeMetadataEndpoint(t *testing.T) {
	st := store.New(time.Minute)
	srv := testServer(t, st, closedChan())

	var meta kubemeta.NodeMetadata
	getJSON(t, srv.URL+"/v1/nodes/node1/metadata", http.StatusOK, &meta)
	if meta.Name != "node1" || meta.Labels["agentpool"] != "system" {
		t.Fatalf("node metadata = %+v", meta)
	}
	getJSON(t, srv.URL+"/v1/nodes/ghost/metadata", http.StatusNotFound, nil)
}

func TestNodeTargets(t *testing.T) {
	st := store.New(time.Minute)
	addPod(st)
	srv := testServer(t, st, closedChan())

	var resp struct {
		Node    string                  `json:"node"`
		Targets []kubemeta.ScrapeTarget `json:"targets"`
	}
	getJSON(t, srv.URL+"/v1/nodes/node1/targets", http.StatusOK, &resp)
	if resp.Node != "node1" || len(resp.Targets) != 1 {
		t.Fatalf("response = %+v", resp)
	}
	tg := resp.Targets[0]
	if tg.URL != "http://10.1.2.3:9090/metrics" {
		t.Errorf("URL = %q", tg.URL)
	}
	if len(tg.Pod.Owners) != 1 {
		t.Errorf("owners missing from target pod: %+v", tg.Pod.Owners)
	}

	getJSON(t, srv.URL+"/v1/nodes/empty-node/targets", http.StatusOK, &resp)
	if len(resp.Targets) != 0 {
		t.Errorf("empty node returned %d targets", len(resp.Targets))
	}
}

func TestNodeTargetsFromService(t *testing.T) {
	st := store.New(time.Minute)
	// The pod is NOT annotated for scraping itself.
	st.UpsertPod(&corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "svc-web-1",
			Namespace:       "default",
			UID:             types.UID("pod2-uid"),
			ResourceVersion: "1",
			Labels:          map[string]string{"app": "svc-web"},
		},
		Spec: corev1.PodSpec{
			NodeName: "node1",
			Containers: []corev1.Container{{
				Name:  "app",
				Image: "img",
				Ports: []corev1.ContainerPort{{Name: "metrics", ContainerPort: 9090}},
			}},
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			PodIP: "10.1.2.4",
		},
	})
	idx := services.NewIndex()
	idx.Upsert(&corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "svc-web",
			Namespace: "default",
			UID:       types.UID("svc-uid"),
			Annotations: map[string]string{
				"prometheus.io/scrape": "true",
			},
		},
		Spec: corev1.ServiceSpec{
			Selector: map[string]string{"app": "svc-web"},
			Ports:    []corev1.ServicePort{{Name: "http", Port: 80, TargetPort: intstr.FromString("metrics")}},
		},
	})
	srv := testServerWithServices(t, st, idx, closedChan())

	var resp struct {
		Targets []kubemeta.ScrapeTarget `json:"targets"`
	}
	getJSON(t, srv.URL+"/v1/nodes/node1/targets", http.StatusOK, &resp)
	if len(resp.Targets) != 1 {
		t.Fatalf("targets = %+v", resp.Targets)
	}
	tg := resp.Targets[0]
	if tg.Source != "service" || tg.URL != "http://10.1.2.4:9090/metrics" {
		t.Errorf("target = %+v", tg)
	}
	if tg.Service == nil || tg.Service.Name != "svc-web" {
		t.Errorf("service metadata = %+v", tg.Service)
	}
	if tg.Pod.Name != "svc-web-1" || tg.Pod.NamespaceMetadata == nil {
		t.Errorf("pod metadata = %+v", tg.Pod)
	}
}

func TestNotReady(t *testing.T) {
	st := store.New(time.Minute)
	addPod(st)
	srv := testServer(t, st, make(chan struct{})) // never ready

	getJSON(t, srv.URL+"/readyz", http.StatusServiceUnavailable, nil)
	getJSON(t, srv.URL+"/v1/nodes/node1/targets", http.StatusServiceUnavailable, nil)
	getJSON(t, srv.URL+"/v1/containers/cafe01?wait=20ms", http.StatusServiceUnavailable, nil)
	getJSON(t, srv.URL+"/healthz", http.StatusOK, nil)
}

func TestReadyEndpoints(t *testing.T) {
	st := store.New(time.Minute)
	srv := testServer(t, st, closedChan())
	getJSON(t, srv.URL+"/readyz", http.StatusOK, nil)
}
