// Package owners resolves related-object metadata for pods from
// metadata-only informer caches: the full ownership chain
// (Pod -> ReplicaSet -> Deployment, Pod -> Job -> CronJob) including the
// owners' labels and annotations, and the metadata of the pod's namespace.
package owners

import (
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/tools/cache"

	"github.com/JohanLindvall/kubescrape/internal/kubemeta"
)

// GVRs whose metadata the resolver consumes. Main wires one metadata
// informer per entry of AllGVRs.
var (
	ReplicaSetGVR = appsv1.SchemeGroupVersion.WithResource("replicasets")
	DeploymentGVR = appsv1.SchemeGroupVersion.WithResource("deployments")
	JobGVR        = batchv1.SchemeGroupVersion.WithResource("jobs")
	CronJobGVR    = batchv1.SchemeGroupVersion.WithResource("cronjobs")
	NamespaceGVR  = corev1.SchemeGroupVersion.WithResource("namespaces")
	NodeGVR       = corev1.SchemeGroupVersion.WithResource("nodes")

	AllGVRs = []schema.GroupVersionResource{
		ReplicaSetGVR, DeploymentGVR, JobGVR, CronJobGVR, NamespaceGVR, NodeGVR,
	}
)

// getFunc fetches cached metadata for an object, nil if unknown. namespace is
// empty for cluster-scoped resources.
type getFunc func(gvr schema.GroupVersionResource, namespace, name string) *metav1.PartialObjectMetadata

// Resolver resolves pod owner chains and namespace metadata.
type Resolver struct {
	get getFunc
}

// NewFromListers builds a Resolver backed by metadata informer listers,
// keyed by resource.
func NewFromListers(listers map[schema.GroupVersionResource]cache.GenericLister) *Resolver {
	return &Resolver{get: func(gvr schema.GroupVersionResource, namespace, name string) *metav1.PartialObjectMetadata {
		lister := listers[gvr]
		if lister == nil {
			return nil
		}
		var obj any
		var err error
		if namespace == "" {
			obj, err = lister.Get(name)
		} else {
			obj, err = lister.ByNamespace(namespace).Get(name)
		}
		if err != nil {
			return nil
		}
		m, ok := obj.(*metav1.PartialObjectMetadata)
		if !ok {
			return nil
		}
		return m
	}}
}

// Resolve returns the owner chain for an object in namespace with the given
// direct owner references. Direct owners always appear; for ReplicaSets and
// Jobs their own owners (Deployments, CronJobs) are appended when known.
// Owners of kinds with a cached metadata informer carry their labels and
// annotations.
func (r *Resolver) Resolve(namespace string, refs []metav1.OwnerReference) []kubemeta.Owner {
	if len(refs) == 0 {
		return nil
	}
	out := make([]kubemeta.Owner, 0, len(refs)+1)
	seen := make(map[string]struct{}, len(refs)+1)
	var add func(ref metav1.OwnerReference, follow bool)
	add = func(ref metav1.OwnerReference, follow bool) {
		if _, ok := seen[string(ref.UID)]; ok {
			return
		}
		seen[string(ref.UID)] = struct{}{}
		owner := kubemeta.Owner{
			APIVersion: ref.APIVersion,
			Kind:       ref.Kind,
			Name:       ref.Name,
			UID:        string(ref.UID),
			Controller: ref.Controller != nil && *ref.Controller,
		}
		if gvr, ok := kindGVR(ref); ok {
			if m := r.get(gvr, namespace, ref.Name); m != nil {
				owner.Labels = copyMap(m.Labels)
				owner.Annotations = copyMap(m.Annotations)
				out = append(out, owner)
				if follow {
					for _, parent := range m.OwnerReferences {
						add(parent, false)
					}
				}
				return
			}
		}
		out = append(out, owner)
	}
	for _, ref := range refs {
		add(ref, followable(ref))
	}
	return out
}

// Namespace returns the metadata of a namespace, or nil if unknown.
func (r *Resolver) Namespace(name string) *kubemeta.ObjectMeta {
	return r.clusterScoped(NamespaceGVR, name)
}

// Node returns the metadata of a node, or nil if unknown.
func (r *Resolver) Node(name string) *kubemeta.ObjectMeta {
	return r.clusterScoped(NodeGVR, name)
}

func (r *Resolver) clusterScoped(gvr schema.GroupVersionResource, name string) *kubemeta.ObjectMeta {
	m := r.get(gvr, "", name)
	if m == nil {
		return nil
	}
	return &kubemeta.ObjectMeta{
		UID:         string(m.UID),
		Labels:      copyMap(m.Labels),
		Annotations: copyMap(m.Annotations),
	}
}

// kindGVR maps an owner reference to the resource whose metadata informer
// caches it, for kinds the service watches.
func kindGVR(ref metav1.OwnerReference) (schema.GroupVersionResource, bool) {
	gv, err := schema.ParseGroupVersion(ref.APIVersion)
	if err != nil {
		return schema.GroupVersionResource{}, false
	}
	switch {
	case gv.Group == appsv1.GroupName && ref.Kind == "ReplicaSet":
		return ReplicaSetGVR, true
	case gv.Group == appsv1.GroupName && ref.Kind == "Deployment":
		return DeploymentGVR, true
	case gv.Group == batchv1.GroupName && ref.Kind == "Job":
		return JobGVR, true
	case gv.Group == batchv1.GroupName && ref.Kind == "CronJob":
		return CronJobGVR, true
	}
	return schema.GroupVersionResource{}, false
}

// followable reports whether ref's own owners belong in the chain
// (ReplicaSet -> Deployment, Job -> CronJob).
func followable(ref metav1.OwnerReference) bool {
	gv, err := schema.ParseGroupVersion(ref.APIVersion)
	if err != nil {
		return false
	}
	return (gv.Group == appsv1.GroupName && ref.Kind == "ReplicaSet") ||
		(gv.Group == batchv1.GroupName && ref.Kind == "Job")
}

func copyMap(m map[string]string) map[string]string {
	if len(m) == 0 {
		return nil
	}
	out := make(map[string]string, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}
