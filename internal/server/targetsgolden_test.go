package server

// The byte-for-byte contract of GET /v1/nodes/{node}/targets.
//
// Every optimisation on this route — the per-pod Service pre-filter, the
// per-request enrichment memo, the PodMonitor snapshot memo, the monitor cross
// product's namespace snapshots — is an argument that some work was REPEATED
// rather than that some work was NEEDED. An argument like that is only worth
// what its evidence is, and the evidence is this: the whole response, hashed.
//
// The fixture is deliberately awkward rather than typical. It carries the
// shapes each of those arguments turns on:
//
//   - a Service that SELECTS a pod while opting into nothing (neither the
//     scrape annotation nor a ServiceMonitor). It is the one the pre-filter
//     drops, and the whole claim is that dropping it is invisible here.
//   - a Service that opts in only through a ServiceMonitor, so the filter's
//     second arm is load-bearing.
//   - a pod annotation and a Service annotation resolving to ONE url, which is
//     where the dedup's carryForward donates the Service view — the field a
//     reordered or shortened match list would silently drop.
//   - two namespaces, so a per-namespace snapshot cannot be confused with a
//     global one, and one pod per namespace sharing an owner chain with the
//     others, so an enrichment memo keyed too loosely shows up as a wrong
//     owner rather than as a missing one.
//   - a terminating pod, a Succeeded pod and a hostNetwork pod, so the
//     scrapeable gate stays where it is.
//
// The recorded tag is the ETag the route serves, which IS the body digest
// (entityTag over the marshalled document), so any difference in any byte of
// any target — order included — fails this test. Regenerate it only with a
// deliberate wire change, and say so in the commit.

import (
	"encoding/json"
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

// goldenTargetsETag is the digest of the whole node-targets document the
// fixture below produces. See the file comment before changing it.
const goldenTargetsETag = `"c680f9a97c2cfbeb83121039db23d0e5"`

// goldenTargetsCount is carried beside the tag purely so a failure says
// something before the digest does.
const goldenTargetsCount = 8

func goldenTargetsServer(t testing.TB) *Server {
	t.Helper()
	st := store.New(time.Minute)

	pod := func(ns, name, ip string, labels, annotations map[string]string, mutate func(*corev1.Pod)) {
		p := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name: name, Namespace: ns, UID: types.UID("uid-" + ns + "-" + name),
				ResourceVersion: "1", Labels: labels, Annotations: annotations,
				OwnerReferences: []metav1.OwnerReference{{
					APIVersion: "apps/v1", Kind: "ReplicaSet", Name: "rs-" + ns, UID: types.UID("rs-" + ns),
				}},
			},
			Spec: corev1.PodSpec{NodeName: "node1", Containers: []corev1.Container{{
				Name: "app", Image: "img",
				Ports: []corev1.ContainerPort{
					{Name: "metrics", ContainerPort: 9090},
					{Name: "admin", ContainerPort: 9091},
				},
			}}},
			Status: corev1.PodStatus{Phase: corev1.PodRunning, PodIP: ip,
				ContainerStatuses: []corev1.ContainerStatus{{
					Name: "app", ContainerID: "containerd://" + name,
					State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}},
				}}},
		}
		if mutate != nil {
			mutate(p)
		}
		st.UpsertPod(p)
	}
	scrapeAnn := map[string]string{"prometheus.io/scrape": "true", "prometheus.io/port": "9090"}

	// prod: an annotated pod that a scrape-annotated Service ALSO reaches on the
	// same url (the carryForward donation), plus an unannotated pod reachable
	// only through a monitor-selected Service.
	pod("prod", "web-a", "10.1.0.1", map[string]string{"app": "web"}, scrapeAnn, nil)
	pod("prod", "web-b", "10.1.0.2", map[string]string{"app": "web"}, nil, nil)
	// A pod nothing opts in: the pre-filter's whole point is that this one costs
	// a map lookup and not a scan.
	pod("prod", "idle", "10.1.0.3", map[string]string{"app": "idle"}, nil, nil)
	// Excluded by the scrapeable gate, three ways.
	pod("prod", "draining", "10.1.0.4", map[string]string{"app": "web"}, scrapeAnn, func(p *corev1.Pod) {
		now := metav1.NewTime(time.Unix(1700000000, 0))
		p.DeletionTimestamp = &now
	})
	pod("prod", "done", "10.1.0.5", map[string]string{"app": "web"}, scrapeAnn, func(p *corev1.Pod) {
		p.Status.Phase = corev1.PodSucceeded
	})
	pod("prod", "hostnet", "10.9.9.9", map[string]string{"app": "web"}, scrapeAnn, func(p *corev1.Pod) {
		p.Spec.HostNetwork = true
	})
	// staging: one pod a PodMonitor selects directly, sharing nothing with prod
	// but the owner KIND.
	pod("staging", "api", "10.2.0.1", map[string]string{"app": "api"}, scrapeAnn, nil)

	svcs := services.NewIndex()
	svc := func(ns, name string, selector, annotations, labels map[string]string) {
		svcs.Upsert(&corev1.Service{
			ObjectMeta: metav1.ObjectMeta{
				Name: name, Namespace: ns, UID: types.UID("svc-" + ns + "-" + name),
				Labels: labels, Annotations: annotations,
			},
			Spec: corev1.ServiceSpec{Selector: selector, Ports: []corev1.ServicePort{
				{Name: "http", Port: 80, TargetPort: intstr.FromString("metrics")},
				{Name: "admin", Port: 81, TargetPort: intstr.FromString("admin")},
			}},
		})
	}
	// Opts in by annotation, and resolves to the same url as the pod annotation.
	svc("prod", "a-annotated", map[string]string{"app": "web"},
		map[string]string{"prometheus.io/scrape": "true", "prometheus.io/port": "80"}, nil)
	// Opts into NOTHING and selects the same pods: the one the filter drops.
	svc("prod", "b-plain", map[string]string{"app": "web"}, nil, map[string]string{"team": "other"})
	// Opts in only through the ServiceMonitor below.
	svc("prod", "c-monitored", map[string]string{"app": "web"}, nil, map[string]string{"team": "obs"})
	// Annotated but selects nobody on this node.
	svc("prod", "d-elsewhere", map[string]string{"app": "nobody"},
		map[string]string{"prometheus.io/scrape": "true"}, nil)
	svc("staging", "e-plain", map[string]string{"app": "api"}, nil, nil)

	mons := servicemonitors.NewIndex()
	if err := mons.Upsert(&unstructured.Unstructured{Object: map[string]any{
		"metadata": map[string]any{"name": "sm", "namespace": "monitoring"},
		"spec": map[string]any{
			"selector":          map[string]any{"matchLabels": map[string]any{"team": "obs"}},
			"namespaceSelector": map[string]any{"any": true},
			"endpoints":         []any{map[string]any{"port": "admin", "path": "/sm"}},
		},
	}}); err != nil {
		t.Fatal(err)
	}
	if err := mons.UpsertPodMonitor(&unstructured.Unstructured{Object: map[string]any{
		"metadata": map[string]any{"name": "pm", "namespace": "monitoring"},
		"spec": map[string]any{
			"selector":            map[string]any{"matchLabels": map[string]any{"app": "api"}},
			"namespaceSelector":   map[string]any{"any": true},
			"podMetricsEndpoints": []any{map[string]any{"port": "admin", "path": "/pm"}},
		},
	}}); err != nil {
		t.Fatal(err)
	}
	return New(Config{
		Store: st, Services: svcs, Monitors: mons, Resolver: stubResolver{},
		MaxWait: time.Second, CacheTTL: 10 * time.Second, Ready: closedChan(),
	})
}

func TestNodeTargetsResponseIsByteStable(t *testing.T) {
	s := goldenTargetsServer(t)
	targets, built := s.nodeTargets("node1")
	if !built {
		t.Fatal("the fixture's node has pods; built = false")
	}
	if len(targets) != goldenTargetsCount {
		for _, tg := range targets {
			t.Logf("target url=%s source=%s monitor=%s service=%v", tg.URL, tg.Source, tg.Monitor, tg.Service != nil)
		}
		t.Fatalf("targets = %d, want %d", len(targets), goldenTargetsCount)
	}
	body, err := json.Marshal(map[string]any{"node": "node1", "targets": targets})
	if err != nil {
		t.Fatal(err)
	}
	if got := entityTag(body); got != goldenTargetsETag {
		t.Errorf("node-targets digest = %s, want %s\nbody:\n%s", got, goldenTargetsETag, body)
	}
}
