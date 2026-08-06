package server

// Benchmarks for GET /v1/nodes/{node}/targets, the one route every agent in the
// fleet re-fetches every scrape cycle and the only one that re-serves the whole
// node's pod set. They REPORT; the ceilings that fail a build live beside them
// in targetsscaling_test.go.

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"

	"github.com/JohanLindvall/kubescrape/internal/servicemonitors"
	"github.com/JohanLindvall/kubescrape/internal/services"
	"github.com/JohanLindvall/kubescrape/internal/store"
)

// targetsFixture builds a node's worth of annotated pods, a namespace's worth
// of Services, and a set of cluster-wide ServiceMonitors selecting all of them
// — the shape a kube-prometheus-stack migration produces.
type targetsFixture struct {
	pods     int
	services int
	monitors int
	cacheTTL time.Duration
	// sharedSelector makes EVERY Service select the pods, so one monitor
	// reaches each pod through all of them — the shape that must dedup without
	// being reported as a shadowed monitor.
	sharedSelector bool
}

func (f targetsFixture) build(t testing.TB) *Server {
	t.Helper()
	st := store.New(time.Minute)
	for i := range f.pods {
		st.UpsertPod(&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name: fmt.Sprintf("web-%d", i), Namespace: "prod",
				UID: types.UID(fmt.Sprintf("pod-uid-%d", i)), ResourceVersion: "1",
				Labels: map[string]string{"app": "web", "pod-template-hash": fmt.Sprintf("%d", i%8)},
				Annotations: map[string]string{
					"prometheus.io/scrape":         "true",
					"prometheus.io/port":           "9090",
					"app.kubernetes.io/managed-by": "helm",
				},
				OwnerReferences: []metav1.OwnerReference{{
					APIVersion: "apps/v1", Kind: "ReplicaSet", Name: "web", UID: "rs-uid",
				}},
			},
			Spec: corev1.PodSpec{
				NodeName: "node1",
				Containers: []corev1.Container{{
					Name: "app", Image: "img",
					Ports: []corev1.ContainerPort{{Name: "metrics", ContainerPort: 9090}},
				}},
			},
			Status: corev1.PodStatus{
				Phase: corev1.PodRunning, PodIP: fmt.Sprintf("10.1.%d.%d", i/256, i%256),
				ContainerStatuses: []corev1.ContainerStatus{{
					Name: "app", ContainerID: fmt.Sprintf("containerd://c0ffee%06d", i),
					State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}},
				}},
			},
		})
	}
	svcs := services.NewIndex()
	for i := range f.services {
		// One Service selects the pods; the rest are the namespace's ordinary
		// population, which Matching used to walk once per pod.
		selector := map[string]string{"app": fmt.Sprintf("other-%d", i)}
		if i == 0 || f.sharedSelector {
			selector = map[string]string{"app": "web"}
		}
		svcs.Upsert(&corev1.Service{
			ObjectMeta: metav1.ObjectMeta{
				Name: fmt.Sprintf("svc-%d", i), Namespace: "prod",
				UID:    types.UID(fmt.Sprintf("svc-uid-%d", i)),
				Labels: map[string]string{"team": "obs"},
			},
			Spec: corev1.ServiceSpec{
				Selector: selector,
				Ports:    []corev1.ServicePort{{Name: "http", Port: 80, TargetPort: intstr.FromString("metrics")}},
			},
		})
	}
	monitors := servicemonitors.NewIndex()
	for i := range f.monitors {
		if err := monitors.Upsert(&unstructured.Unstructured{Object: map[string]any{
			"metadata": map[string]any{"name": fmt.Sprintf("sm-%d", i), "namespace": "monitoring"},
			"spec": map[string]any{
				"selector":          map[string]any{"matchLabels": map[string]any{"team": "obs"}},
				"namespaceSelector": map[string]any{"any": true},
				"endpoints":         []any{map[string]any{"port": "http"}},
			},
		}}); err != nil {
			t.Fatal(err)
		}
	}
	return New(Config{
		Store: st, Services: svcs, Monitors: monitors, Resolver: stubResolver{},
		MaxWait: time.Second, CacheTTL: f.cacheTTL, Ready: closedChan(),
	})
}

func benchTargets(b *testing.B, f targetsFixture, conditional bool) {
	s := f.build(b)
	srv := httptest.NewServer(s.Handler())
	b.Cleanup(srv.Close)
	url := srv.URL + "/v1/nodes/node1/targets"

	var etag string
	if conditional {
		resp, err := http.Get(url)
		if err != nil {
			b.Fatal(err)
		}
		etag = resp.Header.Get("ETag")
		_ = resp.Body.Close()
		if etag == "" {
			b.Fatal("no ETag to revalidate with")
		}
	}
	b.ReportAllocs()
	for b.Loop() {
		req, _ := http.NewRequest(http.MethodGet, url, nil)
		if conditional {
			req.Header.Set("If-None-Match", etag)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			b.Fatal(err)
		}
		_ = resp.Body.Close()
	}
}

func BenchmarkNodeTargets(b *testing.B) {
	for _, tc := range []struct {
		name string
		f    targetsFixture
	}{
		{"110pods", targetsFixture{pods: 110, cacheTTL: 10 * time.Second}},
		{"110pods_200services", targetsFixture{pods: 110, services: 200, cacheTTL: 10 * time.Second}},
		{"110pods_1000services", targetsFixture{pods: 110, services: 1000, cacheTTL: 10 * time.Second}},
		{"110pods_50monitors", targetsFixture{pods: 110, services: 1, monitors: 50, cacheTTL: 10 * time.Second}},
	} {
		b.Run(tc.name, func(b *testing.B) { benchTargets(b, tc.f, false) })
	}
}

// The 304 is what an agent's poll actually is: metaclient caches by URL and
// revalidates, and with a 10s TTL under a 30s scrape interval every poll after
// the first is a conditional GET.
func BenchmarkNodeTargetsRevalidation(b *testing.B) {
	benchTargets(b, targetsFixture{pods: 110, cacheTTL: 10 * time.Second}, true)
}
