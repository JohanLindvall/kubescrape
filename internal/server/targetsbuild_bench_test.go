package server

// The SERVER-SIDE cost of GET /v1/nodes/{node}/targets, measured without the
// HTTP transport.
//
// BenchmarkNodeTargets beside this drives the route over a real httptest
// server, which is the right instrument for "what does an agent's poll cost
// end to end" and the WRONG one for "did the derivation get cheaper": on a
// loaded machine the loopback syscalls are ~9% of samples and ±15-30% of the
// wall clock, which is more noise than any derivation change is likely to be
// signal. These call the three stages the handler actually spends its time in
// — derive, marshal, hash — directly, and separately, so a change can be
// attributed to one of them.
//
// The store is deliberately FLEET-SIZED rather than node-sized: a cluster's
// pods all live in the one singleton's store, and PodsOnNode has to find one
// node's slice of them. A fixture holding only the profiled node's pods cannot
// see a term that scales with the cluster.

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"

	"github.com/JohanLindvall/kubescrape/internal/services"
	"github.com/JohanLindvall/kubescrape/internal/store"
	"github.com/JohanLindvall/kubescrape/pkg/kubemeta"
)

// fleetFixture spreads podsPerNode × nodes pods over the store, in namespaces
// namespaces, with one opt-in Service per namespace among otherServices
// ordinary ones.
type fleetFixture struct {
	nodes         int
	podsPerNode   int
	namespaces    int
	otherServices int
}

func (f fleetFixture) build(t testing.TB) *Server {
	t.Helper()
	if f.namespaces < 1 {
		f.namespaces = 1
	}
	st := store.New(time.Minute)
	n := 0
	for node := range f.nodes {
		for range f.podsPerNode {
			ns := fmt.Sprintf("team-%d", n%f.namespaces)
			st.UpsertPod(&corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name: fmt.Sprintf("web-%d", n), Namespace: ns,
					UID: types.UID(fmt.Sprintf("pod-uid-%d", n)), ResourceVersion: "1",
					Labels: map[string]string{
						"app": "web", "pod-template-hash": fmt.Sprintf("%d", n%8),
						"app.kubernetes.io/instance": fmt.Sprintf("rel-%d", n%16),
					},
					Annotations: map[string]string{
						"prometheus.io/scrape":         "true",
						"prometheus.io/port":           "9090",
						"app.kubernetes.io/managed-by": "helm",
						"checksum/config":              "0123456789abcdef0123456789abcdef",
					},
					OwnerReferences: []metav1.OwnerReference{{
						APIVersion: "apps/v1", Kind: "ReplicaSet",
						Name: fmt.Sprintf("web-%d", n%16), UID: types.UID(fmt.Sprintf("rs-%d", n%16)),
					}},
				},
				Spec: corev1.PodSpec{
					NodeName: fmt.Sprintf("node-%d", node),
					Containers: []corev1.Container{{
						Name: "app", Image: "registry.example.com/team/app:v1.2.3",
						Ports: []corev1.ContainerPort{{Name: "metrics", ContainerPort: 9090}},
					}},
				},
				Status: corev1.PodStatus{
					Phase: corev1.PodRunning, PodIP: fmt.Sprintf("10.%d.%d.%d", n/65536, (n/256)%256, n%256),
					ContainerStatuses: []corev1.ContainerStatus{{
						Name: "app", ContainerID: fmt.Sprintf("containerd://c0ffee%010d", n),
						State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}},
					}},
				},
			})
			n++
		}
	}
	svcs := services.NewIndex()
	for ns := range f.namespaces {
		namespace := fmt.Sprintf("team-%d", ns)
		for i := range f.otherServices + 1 {
			selector := map[string]string{"app": fmt.Sprintf("other-%d", i)}
			annotations := map[string]string(nil)
			if i == 0 {
				selector = map[string]string{"app": "web"}
				annotations = map[string]string{"prometheus.io/scrape": "true", "prometheus.io/port": "http"}
			}
			svcs.Upsert(&corev1.Service{
				ObjectMeta: metav1.ObjectMeta{
					Name: fmt.Sprintf("svc-%d", i), Namespace: namespace,
					UID:         types.UID(fmt.Sprintf("svc-%s-%d", namespace, i)),
					Labels:      map[string]string{"team": "obs"},
					Annotations: annotations,
				},
				Spec: corev1.ServiceSpec{
					Selector: selector,
					Ports:    []corev1.ServicePort{{Name: "http", Port: 80, TargetPort: intstr.FromString("metrics")}},
				},
			})
		}
	}
	return New(Config{
		Store: st, Services: svcs, Resolver: stubResolver{},
		MaxWait: time.Second, CacheTTL: 10 * time.Second, Ready: closedChan(),
	})
}

var (
	buildSink []kubemeta.ScrapeTarget
	bodySink  []byte
	etagSink  string
)

// The three stages, separately and then together, over a fleet-sized store.
// "build" is nodeTargets (PodsOnNode, the Services snapshot, per-pod owner and
// namespace enrichment, target derivation and the order); "marshal" is the
// json.Marshal the handler runs on the result; "hash" is the ETag digest.
func BenchmarkNodeTargetsBuild(b *testing.B) {
	for _, f := range []fleetFixture{
		{nodes: 200, podsPerNode: 25, namespaces: 20, otherServices: 20},
		{nodes: 200, podsPerNode: 110, namespaces: 20, otherServices: 20},
		{nodes: 20, podsPerNode: 110, namespaces: 4, otherServices: 200},
	} {
		name := fmt.Sprintf("%dnodes_%dpods_%dns_%dsvc", f.nodes, f.podsPerNode, f.namespaces, f.otherServices)
		s := f.build(b)
		node := fmt.Sprintf("node-%d", f.nodes/2)
		// Prove the fixture is actually producing targets: a benchmark over an
		// empty list would report a beautiful number for nothing.
		if got, _ := s.nodeTargets(node); len(got) == 0 {
			b.Fatalf("%s: fixture produced no targets", name)
		}
		b.Run(name+"/derive", func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				buildSink, _ = s.nodeTargets(node)
			}
		})
		targets, _ := s.nodeTargets(node)
		doc := map[string]any{"node": node, "targets": targets}
		body, err := json.Marshal(doc)
		if err != nil {
			b.Fatal(err)
		}
		b.Run(name+"/marshal", func(b *testing.B) {
			b.SetBytes(int64(len(body)))
			b.ReportAllocs()
			for b.Loop() {
				bodySink, _ = json.Marshal(doc)
			}
		})
		b.Run(name+"/hash", func(b *testing.B) {
			b.SetBytes(int64(len(body)))
			b.ReportAllocs()
			for b.Loop() {
				etagSink = entityTag(body)
			}
		})
		b.Run(name+"/all", func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				t, _ := s.nodeTargets(node)
				bodySink, _ = json.Marshal(map[string]any{"node": node, "targets": t})
				etagSink = entityTag(bodySink)
			}
		})
	}
}
