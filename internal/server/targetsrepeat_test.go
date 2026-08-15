package server

// The targets route's REPEATED work: the four things it used to recompute per
// pod or per request that are functions of state which changes far less often.
//
// Each of these pins a SHAPE — how many selector evaluations, how many owner
// resolutions, how many index snapshots — rather than a duration or an
// allocation count, because a wall-clock ceiling passes at whatever fixture
// size the repeat happens to be cheap at, and that is exactly how a per-pod
// scan over a namespace's whole Service population survived two rounds of
// benchmarking on the route it lives in. targetscost_test.go holds the
// response-shape ceilings; targetsgolden_test.go pins that none of this changes
// a byte.

import (
	"fmt"
	"strconv"
	"sync/atomic"
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
	"github.com/JohanLindvall/kubescrape/pkg/kubemeta"
)

// A namespace's Service population is namespace-wide while a node's pods are a
// sliver of it, so a per-pod walk of that population makes the request scale
// with the product. Nothing on this node opts into anything, so the answer is
// an empty list either way — which is the point: the cost was paid before the
// question was asked.
func TestServiceScanDoesNotScaleWithPodsTimesServices(t *testing.T) {
	const services = 2000
	evalsAt := func(pods int) int64 {
		s := scanShapeServer(t, pods, services)
		before := s.svcSelectorEvals.Load()
		targets, _ := s.nodeTargets("node1")
		if len(targets) != 0 {
			t.Fatalf("fixture opts nothing in; got %d targets", len(targets))
		}
		return s.svcSelectorEvals.Load() - before
	}
	one, many := evalsAt(1), evalsAt(110)
	// The pre-filter is a single pass over the namespace, so the per-pod walk
	// sees only the Services that can opt a pod in — here, none.
	if one != 0 || many != 0 {
		t.Errorf("selector evaluations = %d at 1 pod and %d at 110 pods, want 0 and 0: "+
			"the per-pod walk must see only opt-in Services, and this namespace has none of %d",
			one, many, services)
	}
}

// …and the narrowing must keep the Services that DO opt in, or the walk would
// be cheap and wrong. One annotated Service among 2,000 is still walked, once
// per pod.
func TestOptInServiceIsStillWalkedPerPod(t *testing.T) {
	const pods, services = 5, 2000
	s := scanShapeServer(t, pods, services)
	// Annotate one of them, in place of the plain Service already indexed.
	s.services.Upsert(&corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name: "svc-7", Namespace: "prod", UID: types.UID("svc-uid-7"),
			Annotations: map[string]string{"prometheus.io/scrape": "true"},
		},
		Spec: corev1.ServiceSpec{
			Selector: map[string]string{"app": "web"},
			Ports:    []corev1.ServicePort{{Name: "http", Port: 80, TargetPort: intstr.FromString("metrics")}},
		},
	})
	before := s.svcSelectorEvals.Load()
	targets, _ := s.nodeTargets("node1")
	if got := s.svcSelectorEvals.Load() - before; got != pods {
		t.Errorf("selector evaluations = %d, want %d (one opt-in Service per pod)", got, pods)
	}
	if len(targets) != pods {
		t.Errorf("targets = %d, want %d: the opt-in Service must still produce them", len(targets), pods)
	}
}

// countingResolver records what the targets path asks the owner/namespace
// caches for. The real resolver answers each of these with a lister Get plus
// kubemeta.CopyMeta — two map clones and the annotation denylist filter — per
// object in the chain.
type countingResolver struct {
	stubResolver
	resolves   atomic.Int64
	namespaces atomic.Int64
}

func (c *countingResolver) Resolve(namespace string, refs []metav1.OwnerReference) []kubemeta.Owner {
	c.resolves.Add(1)
	return c.stubResolver.Resolve(namespace, refs)
}

func (c *countingResolver) Namespace(name string) *kubemeta.ObjectMeta {
	c.namespaces.Add(1)
	return c.stubResolver.Namespace(name)
}

// A node's pods are a handful of workloads' replicas in one or two namespaces.
// Resolving the chain and the namespace per POD does the same lister walk and
// the same metadata copies over and over for an answer that cannot differ
// within one request.
func TestOwnerAndNamespaceAreResolvedPerDistinctChain(t *testing.T) {
	const pods, replicaSets = 110, 8
	res := &countingResolver{}
	s := repeatFixture(t, pods, replicaSets, res)

	targets, _ := s.nodeTargets("node1")
	if len(targets) != pods {
		t.Fatalf("targets = %d, want %d", len(targets), pods)
	}
	if got := res.resolves.Load(); got != replicaSets {
		t.Errorf("owner-chain resolutions = %d, want %d (one per distinct chain, not one per pod)", got, replicaSets)
	}
	if got := res.namespaces.Load(); got != 1 {
		t.Errorf("namespace resolutions = %d, want 1 (every pod is in one namespace)", got)
	}

	// A SECOND request must resolve again: the memo is per request precisely so
	// that an edited Deployment or namespace label reaches the next response
	// instead of being pinned for the process' life.
	res.resolves.Store(0)
	res.namespaces.Store(0)
	s.nodeTargets("node1")
	if got := res.resolves.Load(); got != replicaSets {
		t.Errorf("owner-chain resolutions on the second request = %d, want %d: the memo must not outlive a request", got, replicaSets)
	}
	if got := res.namespaces.Load(); got != 1 {
		t.Errorf("namespace resolutions on the second request = %d, want 1", got)
	}
}

// The memo's key is the whole resolution input. Two pods whose owner references
// differ in ANY field the resolver reads must get their own answer — a key over
// the controller UID alone would hand one workload's labels to another's pods,
// which is the failure the counter above cannot see.
func TestEnrichMemoKeysOnTheWholeOwnerReference(t *testing.T) {
	for _, tc := range []struct {
		name string
		a, b metav1.OwnerReference
	}{
		{"name", ref("apps/v1", "ReplicaSet", "web-a", "uid"), ref("apps/v1", "ReplicaSet", "web-b", "uid")},
		{"kind", ref("apps/v1", "ReplicaSet", "web", "uid"), ref("apps/v1", "StatefulSet", "web", "uid")},
		{"apiVersion", ref("apps/v1", "ReplicaSet", "web", "uid"), ref("apps/v1beta1", "ReplicaSet", "web", "uid")},
		{"uid", ref("apps/v1", "ReplicaSet", "web", "uid-a"), ref("apps/v1", "ReplicaSet", "web", "uid-b")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var c enrichCache
			s := New(Config{Resolver: stubResolver{}, Ready: closedChan()})
			pa := kubemeta.Pod{Namespace: "prod", Name: "a"}
			pb := kubemeta.Pod{Namespace: "prod", Name: "b"}
			s.enrichCached(&c, &pa, []metav1.OwnerReference{tc.a})
			s.enrichCached(&c, &pb, []metav1.OwnerReference{tc.b})
			if len(pa.Owners) != 1 || len(pb.Owners) != 1 {
				t.Fatalf("owners = %v / %v", pa.Owners, pb.Owners)
			}
			if fmt.Sprint(pa.Owners[0]) == fmt.Sprint(pb.Owners[0]) {
				t.Errorf("both pods resolved to %+v: refs differing in %s must not share a memo entry",
					pa.Owners[0], tc.name)
			}
		})
	}
	// …and the namespace half is keyed on the namespace, so two namespaces are
	// two answers.
	var c enrichCache
	s := New(Config{Resolver: stubResolver{}, Ready: closedChan()})
	pa := kubemeta.Pod{Namespace: "prod"}
	pb := kubemeta.Pod{Namespace: "staging"}
	s.enrichCached(&c, &pa, nil)
	s.enrichCached(&c, &pb, nil)
	if pa.NamespaceMetadata == nil || pb.NamespaceMetadata == nil || pa.NamespaceMetadata == pb.NamespaceMetadata {
		t.Errorf("namespace metadata %v / %v: two namespaces must not share a memo entry",
			pa.NamespaceMetadata, pb.NamespaceMetadata)
	}
}

func ref(apiVersion, kind, name, uid string) metav1.OwnerReference {
	return metav1.OwnerReference{APIVersion: apiVersion, Kind: kind, Name: name, UID: types.UID(uid)}
}

// The PodMonitor list is identical across every node and every request until a
// monitor changes, and rendering it costs a sort, a name string per monitor and
// a namespace slice per monitor.
func TestPodMonitorSnapshotIsRenderedPerMonitorChange(t *testing.T) {
	s := repeatFixture(t, 1, 1, stubResolver{})
	for i := range 5 {
		if err := s.monitors.UpsertPodMonitor(podMonitorObj(strconv.Itoa(i))); err != nil {
			t.Fatal(err)
		}
	}
	for range 10 {
		s.nodeTargets("node1")
	}
	if got := s.pmBuilds.Load(); got != 1 {
		t.Errorf("PodMonitor snapshots after ten requests = %d, want 1", got)
	}
	// A change must be picked up at once — the memo is exact, not merely recent.
	if err := s.monitors.UpsertPodMonitor(podMonitorObj("new")); err != nil {
		t.Fatal(err)
	}
	s.nodeTargets("node1")
	if got := s.pmBuilds.Load(); got != 2 {
		t.Errorf("PodMonitor snapshots after a monitor change = %d, want 2", got)
	}
	if got := len(s.allPodMonitors()); got != 6 {
		t.Errorf("PodMonitors = %d, want 6: the re-render must see the new monitor", got)
	}
}

// The monitor→Service cross product takes one Service snapshot per RUN of
// monitors sharing a namespace set, not one per monitor. services.Index.Reads
// counts locked reads, which is exactly the snapshot count.
func TestMonitorCrossProductSnapshotsPerNamespaceSet(t *testing.T) {
	const monitors = 50
	s := repeatFixture(t, 1, 1, stubResolver{})
	for i := range monitors {
		if err := s.monitors.Upsert(clusterWideMonitor(fmt.Sprintf("sm-%02d", i))); err != nil {
			t.Fatal(err)
		}
	}
	before := s.services.Reads()
	if got := len(s.monitoredServices()); got == 0 {
		t.Fatal("the fixture's monitors select its Service")
	}
	if got := s.services.Reads() - before; got != 1 {
		t.Errorf("Service index reads for one rebuild = %d, want 1: %d cluster-wide monitors share one namespace set",
			got, monitors)
	}
}

// …and a monitor naming a DIFFERENT namespace set must get its own snapshot, or
// it would be matched against somebody else's Services. Index.All orders
// monitors by (namespace, name), so this fixture alternates the two sets.
func TestMonitorCrossProductDoesNotShareASnapshotAcrossNamespaceSets(t *testing.T) {
	st := store.New(time.Minute)
	svcs := services.NewIndex()
	for _, ns := range []string{"alpha", "beta"} {
		svcs.Upsert(&corev1.Service{
			ObjectMeta: metav1.ObjectMeta{
				Name: "svc", Namespace: ns, UID: types.UID("svc-" + ns),
				Labels:      map[string]string{"team": "obs"},
				Annotations: map[string]string{},
			},
			Spec: corev1.ServiceSpec{
				Selector: map[string]string{"app": "web"},
				Ports:    []corev1.ServicePort{{Name: "http", Port: 80, TargetPort: intstr.FromString("metrics")}},
			},
		})
	}
	mons := servicemonitors.NewIndex()
	// Sorted by (namespace, name): alpha/m-own, alpha/n-any, beta/m-own,
	// beta/n-any — so the namespace set changes on every step and a run cache
	// that forgot to compare keys would answer three of the four wrongly.
	for _, ns := range []string{"alpha", "beta"} {
		if err := mons.Upsert(ownNamespaceMonitor(ns, "m-own")); err != nil {
			t.Fatal(err)
		}
		if err := mons.Upsert(clusterWideMonitorIn(ns, "n-any")); err != nil {
			t.Fatal(err)
		}
	}
	s := New(Config{Store: st, Services: svcs, Monitors: mons, Resolver: stubResolver{}, Ready: closedChan()})

	got := s.monitoredServices()
	// alpha's Service is selected by alpha/m-own plus both cluster-wide ones;
	// beta's by beta/m-own plus both cluster-wide ones.
	for _, want := range []struct {
		uid      string
		monitors []string
	}{
		{"svc-alpha", []string{"alpha/m-own", "alpha/n-any", "beta/n-any"}},
		{"svc-beta", []string{"alpha/n-any", "beta/m-own", "beta/n-any"}},
	} {
		var names []string
		for _, me := range got[want.uid] {
			names = append(names, me.monitor)
		}
		if fmt.Sprint(names) != fmt.Sprint(want.monitors) {
			t.Errorf("%s is monitored by %v, want %v", want.uid, names, want.monitors)
		}
	}
}

// --- fixtures -------------------------------------------------------------

// scanShapeServer builds a node of pods that opt into nothing, in a namespace
// carrying a large Service population that selects them.
func scanShapeServer(t testing.TB, pods, svcCount int) *Server {
	t.Helper()
	st := store.New(time.Minute)
	for i := range pods {
		st.UpsertPod(scanPod(i))
	}
	svcs := services.NewIndex()
	for i := range svcCount {
		svcs.Upsert(&corev1.Service{
			ObjectMeta: metav1.ObjectMeta{
				Name: fmt.Sprintf("svc-%d", i), Namespace: "prod", UID: types.UID(fmt.Sprintf("svc-uid-%d", i)),
			},
			Spec: corev1.ServiceSpec{
				// Every one of them SELECTS the pods: the narrowing must be by
				// opt-in, and a fixture whose selectors miss would pass with no
				// narrowing at all.
				Selector: map[string]string{"app": "web"},
				Ports:    []corev1.ServicePort{{Name: "http", Port: 80, TargetPort: intstr.FromString("metrics")}},
			},
		})
	}
	return New(Config{
		Store: st, Services: svcs, Monitors: servicemonitors.NewIndex(), Resolver: stubResolver{},
		MaxWait: time.Second, Ready: closedChan(),
	})
}

func scanPod(i int) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: fmt.Sprintf("web-%d", i), Namespace: "prod",
			UID: types.UID(fmt.Sprintf("pod-uid-%d", i)), ResourceVersion: "1",
			Labels: map[string]string{"app": "web"},
		},
		Spec: corev1.PodSpec{NodeName: "node1", Containers: []corev1.Container{{
			Name: "app", Image: "img",
			Ports: []corev1.ContainerPort{{Name: "metrics", ContainerPort: 9090}},
		}}},
		Status: corev1.PodStatus{Phase: corev1.PodRunning, PodIP: fmt.Sprintf("10.1.%d.%d", i/256, i%256)},
	}
}

// repeatFixture is a node of annotated pods spread over replicaSets owners in
// ONE namespace, plus one Service a monitor can select.
func repeatFixture(t testing.TB, pods, replicaSets int, res MetadataResolver) *Server {
	t.Helper()
	st := store.New(time.Minute)
	for i := range pods {
		p := scanPod(i)
		p.Annotations = map[string]string{"prometheus.io/scrape": "true", "prometheus.io/port": "9090"}
		p.OwnerReferences = []metav1.OwnerReference{{
			APIVersion: "apps/v1", Kind: "ReplicaSet",
			Name: fmt.Sprintf("web-%d", i%replicaSets), UID: types.UID(fmt.Sprintf("rs-%d", i%replicaSets)),
		}}
		st.UpsertPod(p)
	}
	svcs := services.NewIndex()
	svcs.Upsert(&corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name: "svc", Namespace: "prod", UID: "svc-uid", Labels: map[string]string{"team": "obs"},
		},
		Spec: corev1.ServiceSpec{
			Selector: map[string]string{"app": "web"},
			Ports:    []corev1.ServicePort{{Name: "http", Port: 80, TargetPort: intstr.FromString("metrics")}},
		},
	})
	return New(Config{
		Store: st, Services: svcs, Monitors: servicemonitors.NewIndex(), Resolver: res,
		MaxWait: time.Second, Ready: closedChan(),
	})
}

func clusterWideMonitor(name string) *unstructured.Unstructured {
	return clusterWideMonitorIn("monitoring", name)
}

func clusterWideMonitorIn(namespace, name string) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"metadata": map[string]any{"name": name, "namespace": namespace},
		"spec": map[string]any{
			"selector":          map[string]any{"matchLabels": map[string]any{"team": "obs"}},
			"namespaceSelector": map[string]any{"any": true},
			"endpoints":         []any{map[string]any{"port": "http"}},
		},
	}}
}

func ownNamespaceMonitor(namespace, name string) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"metadata": map[string]any{"name": name, "namespace": namespace},
		"spec": map[string]any{
			"selector":  map[string]any{"matchLabels": map[string]any{"team": "obs"}},
			"endpoints": []any{map[string]any{"port": "http"}},
		},
	}}
}

func podMonitorObj(name string) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"metadata": map[string]any{"name": "pm-" + name, "namespace": "monitoring"},
		"spec": map[string]any{
			"selector":            map[string]any{"matchLabels": map[string]any{"app": "nothing"}},
			"namespaceSelector":   map[string]any{"any": true},
			"podMetricsEndpoints": []any{map[string]any{"port": "metrics"}},
		},
	}}
}
