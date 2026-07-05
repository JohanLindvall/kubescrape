package owners

import (
	"reflect"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"

	"github.com/JohanLindvall/kubescrape/internal/kubemeta"
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
