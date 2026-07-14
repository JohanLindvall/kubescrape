package owners

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// audit_test.go: targeted tests from the 2026-07 audit.

// A malformed ownerRef cycle (RS -> Deployment -> RS) must terminate and not
// duplicate owners. The resolver only follows one hop (parents are added with
// follow=false) and dedups by UID, so this holds by construction — this test
// pins it.
func TestResolveOwnerCycleTerminates(t *testing.T) {
	ctrl := true
	r := fakeResolver(map[string]*metav1.PartialObjectMetadata{
		"replicasets/default/web-abc": obj("rs-uid", map[string]string{"app": "web"}, metav1.OwnerReference{
			APIVersion: "apps/v1", Kind: "Deployment", Name: "web", UID: "dep-uid", Controller: &ctrl,
		}),
		"deployments/default/web": obj("dep-uid", nil, metav1.OwnerReference{
			APIVersion: "apps/v1", Kind: "ReplicaSet", Name: "web-abc", UID: "rs-uid", Controller: &ctrl,
		}),
	})
	got := r.Resolve("default", []metav1.OwnerReference{{
		APIVersion: "apps/v1", Kind: "ReplicaSet", Name: "web-abc", UID: "rs-uid", Controller: &ctrl,
	}})
	if len(got) != 2 || got[0].Kind != "ReplicaSet" || got[1].Kind != "Deployment" {
		t.Fatalf("cycle resolve = %+v", got)
	}
}

// A self-referencing owner (A owned by A) must not recurse or duplicate.
func TestResolveSelfOwnedTerminates(t *testing.T) {
	ctrl := true
	r := fakeResolver(map[string]*metav1.PartialObjectMetadata{
		"replicasets/default/selfie": obj("self-uid", nil, metav1.OwnerReference{
			APIVersion: "apps/v1", Kind: "ReplicaSet", Name: "selfie", UID: "self-uid", Controller: &ctrl,
		}),
	})
	got := r.Resolve("default", []metav1.OwnerReference{{
		APIVersion: "apps/v1", Kind: "ReplicaSet", Name: "selfie", UID: "self-uid", Controller: &ctrl,
	}})
	if len(got) != 1 || got[0].UID != "self-uid" {
		t.Fatalf("self-owned resolve = %+v", got)
	}
}

// Direct-ref duplicates (two refs naming the same UID) collapse to one owner.
func TestResolveDuplicateRefs(t *testing.T) {
	ref := metav1.OwnerReference{APIVersion: "apps/v1", Kind: "ReplicaSet", Name: "web-abc", UID: "rs-uid"}
	got := fakeResolver(nil).Resolve("default", []metav1.OwnerReference{ref, ref})
	if len(got) != 1 {
		t.Fatalf("duplicate refs resolve = %+v", got)
	}
}

// An unparseable apiVersion in an owner ref must not panic and keeps the
// reference's own identity.
func TestResolveGarbageAPIVersion(t *testing.T) {
	got := fakeResolver(nil).Resolve("default", []metav1.OwnerReference{{
		APIVersion: "a/b/c", Kind: "ReplicaSet", Name: "x", UID: "u",
	}})
	if len(got) != 1 || got[0].Name != "x" {
		t.Fatalf("garbage apiVersion resolve = %+v", got)
	}
}

// Missing intermediate: the ReplicaSet is GC'd while the Deployment lives (or
// the pod is a tombstone outliving its RS). The chain then degrades to the bare
// RS reference — the Deployment is unreachable, since only the RS's own
// ownerRefs name it. Unavoidable with metadata-only informers, but it has a
// visible consequence: attrs.ServiceName ignores ReplicaSet owners, so such a
// pod's service.name silently falls back to the POD name.
func TestResolveMissingIntermediateDropsGrandparent(t *testing.T) {
	ctrl := true
	r := fakeResolver(map[string]*metav1.PartialObjectMetadata{
		// Deployment present, ReplicaSet absent from the cache.
		"deployments/default/web": obj("dep-uid", map[string]string{"app": "web"}),
	})
	got := r.Resolve("default", []metav1.OwnerReference{{
		APIVersion: "apps/v1", Kind: "ReplicaSet", Name: "web-abc", UID: "rs-uid", Controller: &ctrl,
	}})
	if len(got) != 1 || got[0].Kind != "ReplicaSet" || got[0].Labels != nil {
		t.Fatalf("resolve = %+v; want the bare RS ref with no labels", got)
	}
}

// A namespace missing from the cache (deleted mid-request) yields nil metadata,
// not a panic or an empty-but-present object.
func TestNamespaceMissingIsNil(t *testing.T) {
	if m := fakeResolver(nil).Namespace("gone"); m != nil {
		t.Fatalf("Namespace(gone) = %+v; want nil", m)
	}
	if m := fakeResolver(nil).Node("gone"); m != nil {
		t.Fatalf("Node(gone) = %+v; want nil", m)
	}
}
