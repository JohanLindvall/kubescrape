// Package owners resolves related-object metadata for pods from
// metadata-only informer caches: the full ownership chain
// (Pod -> ReplicaSet -> Deployment, Pod -> Job -> CronJob, and the
// direct-owner kinds Pod -> StatefulSet / DaemonSet) including the owners'
// labels and annotations, and the metadata of the pod's namespace.
package owners

import (
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/tools/cache"

	"github.com/JohanLindvall/kubescrape/pkg/kubemeta"
)

// GVRs whose metadata the resolver consumes. Main wires one metadata
// informer per entry of AllGVRs.
var (
	ReplicaSetGVR  = appsv1.SchemeGroupVersion.WithResource("replicasets")
	DeploymentGVR  = appsv1.SchemeGroupVersion.WithResource("deployments")
	StatefulSetGVR = appsv1.SchemeGroupVersion.WithResource("statefulsets")
	DaemonSetGVR   = appsv1.SchemeGroupVersion.WithResource("daemonsets")
	JobGVR         = batchv1.SchemeGroupVersion.WithResource("jobs")
	CronJobGVR     = batchv1.SchemeGroupVersion.WithResource("cronjobs")
	NamespaceGVR   = corev1.SchemeGroupVersion.WithResource("namespaces")
	NodeGVR        = corev1.SchemeGroupVersion.WithResource("nodes")
)

// ownerKindRow is one owner-reference kind the resolver can enrich. Matching
// is by group and kind, deliberately ignoring the reference's version: a
// v1beta1 ReplicaSet is still cached by the same metadata informer.
type ownerKindRow struct {
	group string
	kind  string
	gvr   schema.GroupVersionResource
	// follow marks kinds whose own owners belong in the chain
	// (ReplicaSet -> Deployment, Job -> CronJob).
	follow bool
}

// ownerKinds is the ONE table behind kindGVR, followable and the owner half
// of AllGVRs — the three used to be parallel structures a new kind had to be
// added to separately. Adding an owner kind is one row here, plus BOTH shipped
// ClusterRoles — deploy/kubernetes.yaml and charts/kubescrape/templates/service.yaml
// — and internal/agent/attrs's kindTable, whose test cross-checks AllGVRs.
//
// Both ClusterRoles, because main wires one metadata informer per AllGVRs entry
// and a missing rule 403-loops its initial LIST: that informer's HasSynced never
// flips, WaitForCacheSync never returns, and /readyz answers 503 for the
// process' life — exactly the failure the missing podmonitors rule once caused.
// This comment named only deploy/kubernetes.yaml, which is the copy hack/e2e.sh
// exercises; the chart's is the one every Helm install runs, and nothing read
// either (manifestcheck extracts flags and skips RBAC; chartcheck's goldens are
// regenerated from the chart itself). auditfix_test.go now cross-checks both,
// in the shape internal/agent/attrs's kindTable check already had.
//
// StatefulSets and DaemonSets own their pods DIRECTLY (no intermediate
// object), so their rows exist for their labels/annotations, not for a chain
// to follow; without them the pods of every StatefulSet and DaemonSet in the
// cluster carried a bare owner reference while Deployment pods carried the
// workload's labels and annotations.
var ownerKinds = []ownerKindRow{
	{appsv1.GroupName, "ReplicaSet", ReplicaSetGVR, true},
	{appsv1.GroupName, "Deployment", DeploymentGVR, false},
	{appsv1.GroupName, "StatefulSet", StatefulSetGVR, false},
	{appsv1.GroupName, "DaemonSet", DaemonSetGVR, false},
	{batchv1.GroupName, "Job", JobGVR, true},
	{batchv1.GroupName, "CronJob", CronJobGVR, false},
}

// AllGVRs lists every resource main wires a metadata informer for: the owner
// kinds in ownerKinds order, then Namespace and Node (cached for their
// metadata, never matched as owners). The order is part of the surface —
// keep it stable.
var AllGVRs = func() []schema.GroupVersionResource {
	out := make([]schema.GroupVersionResource, 0, len(ownerKinds)+2)
	for _, k := range ownerKinds {
		out = append(out, k.gvr)
	}
	return append(out, NamespaceGVR, NodeGVR)
}()

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
// Owners of kinds with a cached metadata informer (ReplicaSets, Deployments,
// StatefulSets, DaemonSets, Jobs, CronJobs) carry their labels and
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
			// The cache is keyed by namespace+name; cross-check the UID so a
			// deleted-and-recreated owner with the same name (new UID) does
			// not lend its labels/annotations/parents to the old reference
			// (reachable while a pod tombstone outlives its owner).
			if m := r.get(gvr, namespace, ref.Name); m != nil &&
				(ref.UID == "" || ref.UID == m.UID) {
				owner.Labels, owner.Annotations = kubemeta.CopyMeta(m.Labels, m.Annotations)
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
	labels, annotations := kubemeta.CopyMeta(m.Labels, m.Annotations)
	return &kubemeta.ObjectMeta{
		UID:         string(m.UID),
		Labels:      labels,
		Annotations: annotations,
	}
}

// ownerKind resolves an owner reference against ownerKinds, nil for kinds
// the service does not watch (and for an unparseable apiVersion).
func ownerKind(ref metav1.OwnerReference) *ownerKindRow {
	gv, err := schema.ParseGroupVersion(ref.APIVersion)
	if err != nil {
		return nil
	}
	for i := range ownerKinds {
		if gv.Group == ownerKinds[i].group && ref.Kind == ownerKinds[i].kind {
			return &ownerKinds[i]
		}
	}
	return nil
}

// kindGVR maps an owner reference to the resource whose metadata informer
// caches it, for kinds the service watches.
func kindGVR(ref metav1.OwnerReference) (schema.GroupVersionResource, bool) {
	if k := ownerKind(ref); k != nil {
		return k.gvr, true
	}
	return schema.GroupVersionResource{}, false
}

// followable reports whether ref's own owners belong in the chain
// (ReplicaSet -> Deployment, Job -> CronJob).
func followable(ref metav1.OwnerReference) bool {
	k := ownerKind(ref)
	return k != nil && k.follow
}
