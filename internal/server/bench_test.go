package server

// Benchmarks for GET /v1/nodes/{node}/targets, the one route every agent in the
// fleet re-fetches every scrape cycle and the only one that re-serves the whole
// node's pod set. They REPORT; the ceilings that fail a build live beside them
// in targetsscaling_test.go.

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
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
	// ownerGen overrides the owner change token. nil means a constant, which is
	// the truth for stubResolver; a test that needs owner metadata to change
	// supplies its own.
	ownerGen func() uint64
	// noOwnerGen leaves Config.OwnerGeneration nil, which DISABLES the memo's
	// change-token path and drops it to the wall-clock fallback. For the tests
	// that exist to guard that fallback's arithmetic — it is still the
	// behaviour whenever a deployment has not wired every source.
	noOwnerGen bool
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
				Labels: map[string]string{"app": "web", "pod-template-hash": strconv.Itoa(i % 8)},
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
	ownerGen := f.ownerGen
	if ownerGen == nil && !f.noOwnerGen {
		ownerGen = func() uint64 { return 0 }
	}
	// OwnerGeneration is wired because leaving it nil disables the ETag memo's
	// change-token path, and a fixture that silently measures the wall-clock
	// fallback would make every assertion about the memo describe the old
	// design. stubResolver reads nothing that changes, so a constant is the
	// truth here — tests that need owner metadata to CHANGE drive it themselves.
	return New(Config{
		Store: st, Services: svcs, Monitors: monitors, Resolver: stubResolver{},
		OwnerGeneration: ownerGen,
		MaxWait:         time.Second, CacheTTL: f.cacheTTL, Ready: closedChan(),
	})
}

// benchTargets drives the route over real HTTP. advance is how far the server's
// clock moves BETWEEN requests, which is the whole difference between the two
// revalidation shapes below: the memo's lifetime is measured on that clock, and
// a benchmark that leaves it still is measuring a request no conforming client
// can send.
func benchTargets(b *testing.B, f targetsFixture, conditional bool, advance time.Duration) {
	b.Helper()
	s := f.build(b)
	now := time.Now()
	s.now = func() time.Time { return now }
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
	builds := s.targetBuilds.Load()
	n := 0
	b.ReportAllocs()
	for b.Loop() {
		now = now.Add(advance)
		req, _ := http.NewRequest(http.MethodGet, url, nil)
		if conditional {
			req.Header.Set("If-None-Match", etag)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			b.Fatal(err)
		}
		_ = resp.Body.Close()
		n++
	}
	// The derivations the memo did or did not avoid, so the two shapes below
	// report the thing that separates them and not only their durations.
	if n > 0 {
		b.ReportMetric(float64(s.targetBuilds.Load()-builds)/float64(n), "derivations/op")
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
		b.Run(tc.name, func(b *testing.B) { benchTargets(b, tc.f, false, 0) })
	}
}

// The 304 is what an agent's poll actually is: metaclient caches by URL and
// revalidates, and with a 10s TTL under a 30s scrape interval every poll after
// the first is a conditional GET. What that 304 COSTS, though, depends entirely
// on how much time passed since the node's list was last derived — so the two
// sub-benchmarks are the two ends of that, and only one of them is a shape the
// DaemonSet can produce:
//
//   - memo_hit freezes the clock, so every revalidation lands inside the memo's
//     window and is answered without deriving anything. It is what a caller
//     asking FASTER than the max-age it was given gets: a second agent during a
//     rolling update, an operator's curl loop, a client whose cache was
//     evicted.
//   - agent_cadence advances the clock by one -scrape-interval per request,
//     which is what one DaemonSet agent per node produces. The memo's lifetime
//     is the max-age the response advertises, so a client that honours that
//     max-age asks again exactly when — or after — the memo lapses, and the
//     answer is derived, marshalled and hashed in full before the empty 304 is
//     written. TestNodeTargetsMemoCannotServeAConformingClient carries the
//     argument; this reports the bill.
//
// Reading only the frozen one, as this benchmark did while claiming to measure
// an agent's poll, understates a fleet's steady-state load on the metadata
// service by the whole derivation.
func BenchmarkNodeTargetsRevalidation(b *testing.B) {
	f := targetsFixture{pods: 110, cacheTTL: 10 * time.Second}
	b.Run("memo_hit", func(b *testing.B) { benchTargets(b, f, true, 0) })
	b.Run("agent_cadence", func(b *testing.B) { benchTargets(b, f, true, 30*time.Second) })
}

// BenchmarkEntityTag measures the ETag digest directly. It runs over the FULL
// body of every cached response including every 304 revalidation, so the digest
// is on the hot path for the route that re-serializes every pod document on the
// node each scrape cycle. Sizes bracket a small pod document and a node's whole
// target list.
func BenchmarkEntityTag(b *testing.B) {
	for _, size := range []int{512, 8 << 10, 256 << 10} {
		body := make([]byte, size)
		for i := range body {
			body[i] = byte('a' + i%26)
		}
		b.Run(strconv.Itoa(size)+"B", func(b *testing.B) {
			b.SetBytes(int64(size))
			b.ReportAllocs()
			for b.Loop() {
				tagSink = entityTag(body)
			}
		})
	}
}

var tagSink string
