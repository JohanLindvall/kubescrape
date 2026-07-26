package owners

import (
	"reflect"
	"slices"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/cache"

	"github.com/JohanLindvall/kubescrape/pkg/kubemeta"
)

func fakeResolver(objects map[string]*metav1.PartialObjectMetadata) *Resolver {
	return &Resolver{get: func(gvr schema.GroupVersionResource, namespace, name string) *metav1.PartialObjectMetadata {
		return objects[gvr.Resource+"/"+namespace+"/"+name]
	}}
}

func obj(uid string, labels map[string]string, ownerRefs ...metav1.OwnerReference) *metav1.PartialObjectMetadata {
	return &metav1.PartialObjectMetadata{ObjectMeta: metav1.ObjectMeta{
		UID:             types.UID(uid),
		Labels:          labels,
		OwnerReferences: ownerRefs,
	}}
}

func TestResolveReplicaSetToDeployment(t *testing.T) {
	ctrl := true
	r := fakeResolver(map[string]*metav1.PartialObjectMetadata{
		"replicasets/default/web-abc": obj("rs-uid", map[string]string{"app": "web"}, metav1.OwnerReference{
			APIVersion: "apps/v1", Kind: "Deployment", Name: "web", UID: "dep-uid", Controller: &ctrl,
		}),
		"deployments/default/web": obj("dep-uid", map[string]string{"team": "core"}),
	})
	got := r.Resolve("default", []metav1.OwnerReference{{
		APIVersion: "apps/v1", Kind: "ReplicaSet", Name: "web-abc", UID: "rs-uid", Controller: &ctrl,
	}})
	want := []kubemeta.Owner{
		{APIVersion: "apps/v1", Kind: "ReplicaSet", Name: "web-abc", UID: "rs-uid", Controller: true,
			Labels: map[string]string{"app": "web"}},
		{APIVersion: "apps/v1", Kind: "Deployment", Name: "web", UID: "dep-uid", Controller: true,
			Labels: map[string]string{"team": "core"}},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v\nwant %+v", got, want)
	}
}

func TestResolveJobToCronJob(t *testing.T) {
	ctrl := true
	r := fakeResolver(map[string]*metav1.PartialObjectMetadata{
		"jobs/batch-ns/backup-123": obj("job-uid", nil, metav1.OwnerReference{
			APIVersion: "batch/v1", Kind: "CronJob", Name: "backup", UID: "cj-uid", Controller: &ctrl,
		}),
	})
	got := r.Resolve("batch-ns", []metav1.OwnerReference{{
		APIVersion: "batch/v1", Kind: "Job", Name: "backup-123", UID: "job-uid", Controller: &ctrl,
	}})
	if len(got) != 2 || got[1].Kind != "CronJob" || got[1].Name != "backup" {
		t.Fatalf("got %+v", got)
	}
	// CronJob metadata is not cached in this test; the owner still appears
	// with its identity from the Job's owner reference.
	if got[1].Labels != nil {
		t.Fatalf("unexpected labels on uncached parent: %+v", got[1].Labels)
	}
}

func TestResolveUnknownParentKeepsDirectOwner(t *testing.T) {
	r := fakeResolver(nil) // ReplicaSet not in cache
	got := r.Resolve("default", []metav1.OwnerReference{{
		APIVersion: "apps/v1", Kind: "ReplicaSet", Name: "gone", UID: "rs-uid",
	}})
	if len(got) != 1 || got[0].Kind != "ReplicaSet" {
		t.Fatalf("got %+v", got)
	}
}

func TestResolveNonFollowedKinds(t *testing.T) {
	r := fakeResolver(nil)
	got := r.Resolve("default", []metav1.OwnerReference{{
		APIVersion: "apps/v1", Kind: "DaemonSet", Name: "ds", UID: "ds-uid",
	}})
	if len(got) != 1 || got[0].Kind != "DaemonSet" {
		t.Fatalf("got %+v", got)
	}
	if got := r.Resolve("default", nil); got != nil {
		t.Fatalf("no refs should resolve to nil, got %+v", got)
	}
}

// StatefulSets and DaemonSets own their pods DIRECTLY, with no intermediate
// ReplicaSet/Job to hang the workload's identity on. Without metadata
// informers for them, a Deployment pod carried its workload's labels and
// annotations while every StatefulSet and DaemonSet pod carried a bare
// reference — so any attribute or alert derived from an owner label simply
// had no value for those workloads.
func TestResolveDirectOwnerWorkloadLabels(t *testing.T) {
	ctrl := true
	r := fakeResolver(map[string]*metav1.PartialObjectMetadata{
		"statefulsets/db/postgres": obj("sts-uid", map[string]string{"app": "postgres", "team": "data"}),
		"daemonsets/kube-system/fluentd": obj("ds-uid", map[string]string{"app": "fluentd"},
			// A DaemonSet's own owners (an operator CR, say) are not followed,
			// exactly like a Deployment's.
			metav1.OwnerReference{APIVersion: "example.com/v1", Kind: "Agent", Name: "logging", UID: "cr-uid"}),
	})

	sts := r.Resolve("db", []metav1.OwnerReference{{
		APIVersion: "apps/v1", Kind: "StatefulSet", Name: "postgres", UID: "sts-uid", Controller: &ctrl,
	}})
	want := []kubemeta.Owner{{
		APIVersion: "apps/v1", Kind: "StatefulSet", Name: "postgres", UID: "sts-uid", Controller: true,
		Labels: map[string]string{"app": "postgres", "team": "data"},
	}}
	if !reflect.DeepEqual(sts, want) {
		t.Errorf("statefulset owner\n got %+v\nwant %+v", sts, want)
	}

	ds := r.Resolve("kube-system", []metav1.OwnerReference{{
		APIVersion: "apps/v1", Kind: "DaemonSet", Name: "fluentd", UID: "ds-uid", Controller: &ctrl,
	}})
	if len(ds) != 1 || ds[0].Labels["app"] != "fluentd" {
		t.Errorf("daemonset owner = %+v; want the DaemonSet's own labels and no parent", ds)
	}

	// The kinds must also be WATCHED, or the listers backing the resolver are
	// empty in production no matter what kindGVR maps.
	for _, gvr := range []schema.GroupVersionResource{StatefulSetGVR, DaemonSetGVR} {
		if !slices.Contains(AllGVRs, gvr) {
			t.Errorf("%s missing from AllGVRs: no informer would populate its cache", gvr.Resource)
		}
	}
}

func TestNodeMetadata(t *testing.T) {
	r := fakeResolver(map[string]*metav1.PartialObjectMetadata{
		"nodes//node1": obj("node-uid", map[string]string{"agentpool": "system"}),
	})
	got := r.Node("node1")
	if got == nil || got.UID != "node-uid" || got.Labels["agentpool"] != "system" {
		t.Fatalf("got %+v", got)
	}
	if r.Node("missing") != nil {
		t.Fatal("unknown node should resolve to nil")
	}
}

func TestNamespaceMetadata(t *testing.T) {
	r := fakeResolver(map[string]*metav1.PartialObjectMetadata{
		"namespaces//prod": obj("ns-uid", map[string]string{"env": "prod"}),
	})
	got := r.Namespace("prod")
	if got == nil || got.UID != "ns-uid" || got.Labels["env"] != "prod" {
		t.Fatalf("got %+v", got)
	}
	if r.Namespace("missing") != nil {
		t.Fatal("unknown namespace should resolve to nil")
	}
}

// NewFromListers backs the resolver with real informer listers: verify both
// the namespaced and cluster-key Get paths and the nil-lister guard.
func TestNewFromListers(t *testing.T) {
	rsGVR := schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "replicasets"}
	indexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc})
	rs := obj("rs-uid", nil)
	rs.Name = "web-abc"
	rs.Namespace = "default"
	if err := indexer.Add(rs); err != nil {
		t.Fatal(err)
	}
	r := NewFromListers(map[schema.GroupVersionResource]cache.GenericLister{
		rsGVR: cache.NewGenericLister(indexer, rsGVR.GroupResource()),
	})

	ctrl := true
	got := r.Resolve("default", []metav1.OwnerReference{{
		APIVersion: "apps/v1", Kind: "ReplicaSet", Name: "web-abc", UID: "rs-uid", Controller: &ctrl,
	}})
	if len(got) != 1 || got[0].Name != "web-abc" || got[0].Kind != "ReplicaSet" {
		t.Fatalf("resolved = %+v", got)
	}

	// Unknown resource kinds (no lister) and unknown names resolve to the
	// direct reference only, without panicking.
	got = r.Resolve("default", []metav1.OwnerReference{{
		APIVersion: "batch/v1", Kind: "Job", Name: "j1", UID: "j-uid", Controller: &ctrl,
	}})
	if len(got) != 1 || got[0].Kind != "Job" {
		t.Fatalf("no-lister resolve = %+v", got)
	}
	if meta := r.Namespace("missing"); meta != nil {
		t.Fatalf("missing namespace = %+v", meta)
	}
}

// A same-name owner recreated with a new UID must not lend its labels (or
// parents) to a reference naming the OLD UID — reachable while a pod
// tombstone outlives its deleted owner.
func TestResolveUIDMismatchKeepsRefIdentity(t *testing.T) {
	ctrl := true
	r := fakeResolver(map[string]*metav1.PartialObjectMetadata{
		"replicasets/default/web-abc": obj("NEW-uid", map[string]string{"gen": "2"}, metav1.OwnerReference{
			APIVersion: "apps/v1", Kind: "Deployment", Name: "web", UID: "dep-uid", Controller: &ctrl,
		}),
	})
	got := r.Resolve("default", []metav1.OwnerReference{{
		APIVersion: "apps/v1", Kind: "ReplicaSet", Name: "web-abc", UID: "OLD-uid", Controller: &ctrl,
	}})
	want := []kubemeta.Owner{
		{APIVersion: "apps/v1", Kind: "ReplicaSet", Name: "web-abc", UID: "OLD-uid", Controller: true},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v\nwant %+v (new object's labels must not attach to the old UID)", got, want)
	}
}
