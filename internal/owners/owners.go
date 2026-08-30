// Package owners resolves related-object metadata for pods from
// metadata-only informer caches: the full ownership chain
// (Pod -> ReplicaSet -> Deployment, Pod -> Job -> CronJob, and the
// direct-owner kinds Pod -> StatefulSet / DaemonSet) including the owners'
// labels and annotations, and the metadata of the pod's namespace.
package owners

import (
	"context"
	"log/slog"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/tools/cache"

	"github.com/JohanLindvall/kubescrape/internal/logdedupe"
	"github.com/JohanLindvall/kubescrape/internal/obs"
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
	// warnDedupe and debugDedupe throttle the per-object failure log. nil is
	// legal (the failure is still counted) so a Resolver built with a
	// hand-written get — the tests' shape — needs no wiring; NewFromListers
	// always sets both.
	//
	// TWO tables, not one, because the two levels are independent conditions
	// competing for one bounded resource: not_found is ordinary and can be
	// plentiful (every pod whose owner is tombstoned), so a shared table would
	// let ordinary chatter saturate it and SUPPRESS the RBAC-shaped warning
	// that is the whole reason this reporting exists. Saturation suppresses and
	// never clears (logdedupe's rule), which is exactly what makes starving one
	// condition with another permanent.
	warnDedupe  *logdedupe.Table
	debugDedupe *logdedupe.Table
	// capThrottle throttles the MaxOwners line. Keyless on purpose — see
	// ownersCapWarnEvery.
	capThrottle logdedupe.Throttle
	// log is the failure logger, nil meaning slog.Default(). There is no
	// constructor parameter for it: this package is wired once, from main, at a
	// point where the process logger is already the default.
	log *slog.Logger
}

// resolveWarnEvery bounds how often ONE object may log a failed metadata read.
// Every one of these conditions is a STATE, not an event — a ClusterRole
// missing a rule stays missing, an unsynced informer stays unsynced — and the
// noticing code runs once per owner reference per pod per request, on a route
// every agent in the fleet polls each scrape cycle. The counter carries the
// rate; the line only has to name the object often enough to be found.
const resolveWarnEvery = 5 * time.Minute

// maxResolveWarnKeys bounds the throttle table. Keys are (kind, reason,
// object), so a cluster whose owner informer is entirely empty would otherwise
// mint one per workload.
const maxResolveWarnKeys = 512

// MaxOwners bounds how many entries ONE pod's resolved owner chain may carry.
//
// Kubernetes bounds neither the ownerReferences count nor what each owner may
// annotate, and Resolve attaches every WATCHED owner's labels and annotations
// (non-controller references included — only `follow` is restricted). The pod
// object's own ~1.5 MiB ceiling permits thousands of references, and the owner
// objects can be SHARED by every pod on the node: a tenant with edit rights in
// one namespace creates N fat ReplicaSets and points every pod's
// ownerReferences at all of them, and each pod document — which
// /v1/nodes/{node}/targets carries once per scrapeable pod, re-derived and
// re-marshalled on every agent poll — grows by N annotation sets. At N=100 with
// a 200 KiB annotation apiece that measured ~25 MB per pod document, in the
// singleton the chart requests 128Mi for with no memory limit. Beside this,
// kubemeta's MaxAnnotationBytes bounds what ONE of those objects contributes;
// together they make a pod document a constant this service chose.
//
// 8, against a legitimate case of ONE. A pod has one controller reference, and
// the chain this resolver appends to it is at most one deep (ReplicaSet ->
// Deployment, Job -> CronJob), so the honest maximum for a Kubernetes-managed
// pod is 2. Extra NON-controller references exist — some operators add one for
// garbage collection — but a handful is the whole of it, so 8 is 4x the
// realistic worst case and costs nothing real; anything past it is a shape
// Kubernetes itself never produces.
//
// The refusal is DIAGNOSABLE, not silent: the refused references are counted
// (kubescrape_owner_resolve_failures_total{reason="owners_capped"}), reported
// once per resolution in a throttled Warn, and named on the served document by
// kubemeta.Pod.OwnersOmitted — a pod whose chain is short must not read as a
// pod that has no owners.
const MaxOwners = 8

// ownersCapWarnEvery throttles the cap's log line. It is KEYLESS
// (logdedupe.Throttle, not the keyed tables above) for a reason the two tables'
// own comment gives: the condition is a tenant's object shape, so a keyed
// throttle would mint one entry per pod name and SATURATE the warn table,
// permanently suppressing the RBAC-shaped warning that is the whole reason
// that table exists. One line per interval names the namespace, the first
// reference and the counts; the counter carries the rate.
const ownersCapWarnEvery = 5 * time.Minute

// NewFromListers builds a Resolver backed by metadata informer listers,
// keyed by resource.
func NewFromListers(listers map[schema.GroupVersionResource]cache.GenericLister) *Resolver {
	r := &Resolver{
		warnDedupe:  logdedupe.New(maxResolveWarnKeys, resolveWarnEvery),
		debugDedupe: logdedupe.New(maxResolveWarnKeys, resolveWarnEvery),
	}
	r.get = func(gvr schema.GroupVersionResource, namespace, name string) *metav1.PartialObjectMetadata {
		lister := listers[gvr]
		if lister == nil {
			// The kind is in AllGVRs but main started no informer for it: a
			// wiring bug that silently strips every pod of that owner's labels.
			r.report(gvr, namespace, name, reasonNoInformer, nil)
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
			// NotFound is the ONE ordinary outcome here (a deleted owner under
			// a pod tombstone, a cache still filling), so it is counted and
			// logged at Debug; anything else is a broken cache — the shape a
			// revoked RBAC rule takes — and warns.
			if apierrors.IsNotFound(err) {
				r.report(gvr, namespace, name, reasonNotFound, nil)
			} else {
				r.report(gvr, namespace, name, reasonListerError, err)
			}
			return nil
		}
		m, ok := obj.(*metav1.PartialObjectMetadata)
		if !ok {
			// The informer was built for a different type than the metadata
			// informer this package assumes; also a wiring bug.
			r.report(gvr, namespace, name, reasonWrongType, nil)
			return nil
		}
		return m
	}
	return r
}

// The reason label values of kubescrape_owner_resolve_failures_total. They are
// spelled once here because obs.go's help text enumerates them for the
// operator, and a value that exists in only one of the two places is a
// dashboard filter that matches nothing.
const (
	reasonNotFound      = "not_found"
	reasonListerError   = "lister_error"
	reasonNoInformer    = "no_informer"
	reasonWrongType     = "wrong_type"
	reasonBadAPIVersion = "bad_api_version"
	reasonUIDMismatch   = "uid_mismatch"
	reasonOwnersCapped  = "owners_capped"
)

// report counts one failed metadata read and names the object in a throttled
// log line.
//
// The three wiring/RBAC reasons WARN — nobody meant those to happen, and each
// degrades attribution for every pod under that owner — while the ordinary
// ones stay at Debug: not_found is expected at a low rate on any cluster, and
// an operator who wants the per-object detail is already reading an incident.
//
// The kind label comes from the GVR, never from the reference, so the metric's
// cardinality is bounded by AllGVRs however exotic a pod's ownerReferences are.
func (r *Resolver) report(gvr schema.GroupVersionResource, namespace, name, reason string, err error) {
	r.reportObj(gvrKind(gvr), namespace, name, reason, err)
}

// reportObj is report with the kind already resolved, for the one caller whose
// failure happens BEFORE a GVR exists (an ownerReference whose apiVersion does
// not parse).
func (r *Resolver) reportObj(kind, namespace, name, reason string, err error) {
	obs.OwnerResolveFailures.WithLabelValues(kind, reason).Inc()

	warn := reason == reasonListerError || reason == reasonNoInformer || reason == reasonWrongType
	log := r.log
	if log == nil {
		log = slog.Default()
	}
	// The level check comes BEFORE the throttle, and before the args below are
	// built, for two reasons that both bite in production. slog evaluates its
	// arguments eagerly, so the note string would be concatenated on every
	// not_found at Info; and a throttle slot spent on a line the handler then
	// drops is a slot the next distinct object cannot have.
	if !warn && !log.Enabled(context.Background(), slog.LevelDebug) {
		return
	}
	table := r.debugDedupe
	if warn {
		table = r.warnDedupe
	}
	if table == nil {
		return
	}
	allow, saturated := table.Allow(kind + "\x00" + reason + "\x00" + namespace + "/" + name)
	if !allow && !saturated {
		return
	}
	if saturated {
		// Named, because the two tables saturate independently and an operator
		// reading this needs to know which half of the reporting has gone
		// quiet.
		table := "debug"
		if warn {
			table = "warn"
		}
		log.Warn("owner-metadata dedupe table is full; further distinct objects are suppressed",
			"table", table, "count", maxResolveWarnKeys)
	}
	if !allow {
		return
	}
	// The object is named by kind/namespace/name rather than by a formatted
	// ref so the fields stay greppable; namespace is empty for Namespace and
	// Node, which is the honest rendering of a cluster-scoped object.
	args := []any{"kind", kind, "reason", reason, "namespace", namespace, "name", name,
		"note", "the pod is still served, with the bare owner reference and no owner labels or annotations; " +
			"service.name and the workload labels derived from it are degraded until this resolves. Further " +
			"reports for this object are suppressed for " + resolveWarnEvery.String()}
	if err != nil {
		args = append(args, "error", err)
	}
	if warn {
		log.Warn("reading an owner's cached metadata failed", args...)
		return
	}
	log.Debug("an owner's cached metadata was not available", args...)
}

// gvrKind names the Kind behind a watched resource, for the metric's `kind`
// label. Unknown resources cannot reach it (get is only ever called with an
// AllGVRs entry), but a fallback beats an empty label if one ever does.
func gvrKind(gvr schema.GroupVersionResource) string {
	for i := range ownerKinds {
		if ownerKinds[i].gvr == gvr {
			return ownerKinds[i].kind
		}
	}
	switch gvr {
	case NamespaceGVR:
		return "Namespace"
	case NodeGVR:
		return "Node"
	}
	return "unknown"
}

// Resolve returns the owner chain for an object in namespace with the given
// direct owner references, and how many entries MaxOwners refused. Direct
// owners always appear; for ReplicaSets and Jobs their own owners (Deployments,
// CronJobs) are appended when known. Owners of kinds with a cached metadata
// informer (ReplicaSets, Deployments, StatefulSets, DaemonSets, Jobs, CronJobs)
// carry their labels and annotations.
//
// The chain is capped at MaxOwners entries, in the references' own order — the
// API object's, so the answer is the same for every request describing the pod
// and the served document's ETag does not churn. The second return value is
// what the caller puts on kubemeta.Pod.OwnersOmitted so a truncated chain
// cannot read as a complete one.
//
// The cap is applied ONLY to the emitted list. Every reference is still walked,
// still counted when it fails to resolve, and still deduplicated by UID: the
// bound exists to keep the DOCUMENT a constant size, not to stop looking.
func (r *Resolver) Resolve(namespace string, refs []metav1.OwnerReference) ([]kubemeta.Owner, int) {
	if len(refs) == 0 {
		return nil, 0
	}
	capacity := min(len(refs)+1, MaxOwners)
	out := make([]kubemeta.Owner, 0, capacity)
	seen := make(map[string]struct{}, len(refs)+1)
	omitted := 0
	var add func(ref metav1.OwnerReference, follow bool)
	add = func(ref metav1.OwnerReference, follow bool) {
		if _, ok := seen[string(ref.UID)]; ok {
			return
		}
		seen[string(ref.UID)] = struct{}{}
		if len(out) >= MaxOwners {
			// Counted per REFUSED reference (the kind label is knownKind's, so
			// a hostile CRD kind cannot mint a label value), logged once per
			// resolution below: a pod naming thousands of owners must not cost
			// thousands of log lines, and must not spend the keyed throttle
			// tables that the RBAC-shaped warnings depend on.
			obs.OwnerResolveFailures.WithLabelValues(knownKind(ref.Kind), reasonOwnersCapped).Inc()
			omitted++
			return
		}
		owner := kubemeta.Owner{
			APIVersion: ref.APIVersion,
			Kind:       ref.Kind,
			Name:       ref.Name,
			UID:        string(ref.UID),
			Controller: ref.Controller != nil && *ref.Controller,
		}
		if gvr, ok := r.kindGVR(ref); ok {
			// The cache is keyed by namespace+name; cross-check the UID so a
			// deleted-and-recreated owner with the same name (new UID) does
			// not lend its labels/annotations/parents to the old reference
			// (reachable while a pod tombstone outlives its owner).
			m := r.get(gvr, namespace, ref.Name)
			if m != nil && ref.UID != "" && ref.UID != m.UID {
				// The read SUCCEEDED and was refused, which is a different
				// story from a miss (r.get has already counted those): this pod
				// loses its owner's labels because the owner was recreated
				// under its old name. Counting it here is what tells the two
				// apart when a chain silently loses its metadata.
				r.report(gvr, namespace, ref.Name, reasonUIDMismatch, nil)
			} else if m != nil {
				owner.Labels, owner.Annotations = kubemeta.CopyMeta(m.Labels, m.Annotations)
				r.countAnnotationsOmitted(gvrKind(gvr), owner.Annotations)
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
	if omitted > 0 {
		r.reportOwnersCapped(namespace, refs, len(out), omitted)
	}
	return out, omitted
}

// reportOwnersCapped names the MaxOwners refusal once per resolution, through
// the KEYLESS throttle: the condition is one tenant's object shape and the
// useful information is one line, while a keyed throttle would mint an entry
// per pod and saturate the table the RBAC warnings share (see
// ownersCapWarnEvery).
//
// The line names the FIRST reference rather than the refused ones: the refused
// ones are the tail of a list nobody wrote by hand, and the first is the
// controller — the object an operator can actually go and look at. Nothing
// tenant-controlled is logged unclipped: a reference name is a DNS subdomain,
// bounded by the API server at 253 bytes.
func (r *Resolver) reportOwnersCapped(namespace string, refs []metav1.OwnerReference, served, omitted int) {
	if !r.capThrottle.Allow(ownersCapWarnEvery) {
		return
	}
	log := r.log
	if log == nil {
		log = slog.Default()
	}
	log.Warn("an object names more owners than the resolver serves; the chain is truncated",
		"namespace", namespace, "refs", len(refs), "firstRef", knownKind(refs[0].Kind)+"/"+refs[0].Name,
		"served", served, "omitted", omitted, "limit", MaxOwners,
		"note", "every ScrapeTarget embeds the whole pod document and each resolved owner adds its labels and "+
			"annotations to it, so an unbounded ownerReferences list is an unbounded response; the served "+
			"document says so through kubemeta.Pod.OwnersOmitted. Further reports are suppressed for "+
			ownersCapWarnEvery.String())
}

// countAnnotationsOmitted counts an object whose annotations kubemeta's budget
// refused part of. It is here, and in the two sibling doors (the pod
// conversion's caller and the Service index), because pkg/kubemeta cannot
// import internal/obs and only the caller knows the object's KIND.
func (r *Resolver) countAnnotationsOmitted(kind string, annotations map[string]string) {
	if kubemeta.AnnotationsOmitted(annotations) {
		obs.MetadataAnnotationsOmitted.WithLabelValues(kind).Inc()
	}
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
	r.countAnnotationsOmitted(gvrKind(gvr), annotations)
	return &kubemeta.ObjectMeta{
		UID:         string(m.UID),
		Labels:      labels,
		Annotations: annotations,
	}
}

// ownerKind resolves an owner reference against ownerKinds. A nil row with a
// nil error is the ordinary "kind the service does not watch"; a non-nil error
// is an apiVersion that does not parse, which the caller reports because such a
// reference can never match a watched kind and so silently costs its pod the
// owner's metadata.
func ownerKind(ref metav1.OwnerReference) (*ownerKindRow, error) {
	gv, err := schema.ParseGroupVersion(ref.APIVersion)
	if err != nil {
		return nil, err
	}
	for i := range ownerKinds {
		if gv.Group == ownerKinds[i].group && ref.Kind == ownerKinds[i].kind {
			return &ownerKinds[i], nil
		}
	}
	return nil, nil
}

// kindGVR maps an owner reference to the resource whose metadata informer
// caches it, for kinds the service watches.
//
// It is a METHOD, and the only reporting caller of ownerKind, because Resolve
// calls it exactly once per reference — followable's own lookup stays silent,
// or a malformed apiVersion on a top-level reference would be counted twice.
func (r *Resolver) kindGVR(ref metav1.OwnerReference) (schema.GroupVersionResource, bool) {
	k, err := ownerKind(ref)
	if err != nil {
		// The kind label is knownKind's, not the reference's: ownerReferences
		// name arbitrary CRD kinds, and a hostile or merely creative one must
		// not mint an unbounded label value on a metric.
		r.reportObj(knownKind(ref.Kind), "", ref.Name, reasonBadAPIVersion, err)
		return schema.GroupVersionResource{}, false
	}
	if k == nil {
		return schema.GroupVersionResource{}, false
	}
	return k.gvr, true
}

// followable reports whether ref's own owners belong in the chain
// (ReplicaSet -> Deployment, Job -> CronJob).
func followable(ref metav1.OwnerReference) bool {
	k, _ := ownerKind(ref)
	return k != nil && k.follow
}

// knownKind bounds a reference-supplied Kind to the set this package watches,
// so the metric's cardinality stays a property of ownerKinds rather than of
// whatever CRDs a cluster installs.
func knownKind(kind string) string {
	for i := range ownerKinds {
		if ownerKinds[i].kind == kind {
			return kind
		}
	}
	return "unknown"
}
