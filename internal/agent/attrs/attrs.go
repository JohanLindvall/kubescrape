// Package attrs maps kubescrape metadata onto OpenTelemetry resource
// attributes, following the k8s semantic conventions (and the
// k8sattributes-processor conventions for labels).
package attrs

import (
	"maps"
	"slices"

	"go.opentelemetry.io/collector/pdata/pcommon"

	"github.com/JohanLindvall/kubescrape/pkg/kubemeta"
)

// kindTable is the ONE enumeration of the Kubernetes object kinds this package
// maps to attributes: the k8s.<kind>.name each becomes, and whether the kind
// names the pod's WORKLOAD (what service.name derives from). ServiceName and
// KindAttribute both read it, so adding a kind is one row here — and
// TestKindTableCoversOwnerResolver cross-checks the rows against
// internal/owners.AllGVRs, so a kind added to the owner resolver without a row
// fails a test naming this file.
//
// workload is false for ReplicaSet (its Deployment is the workload; a bare
// ReplicaSet owner falls through to the pod name) and Node (a Node owner ref —
// mirror/static pods — is placement, not a workload).
var kindTable = map[string]struct {
	attr     string // the k8s.<kind>.name resource attribute
	workload bool   // ServiceName derives from owners of this kind
}{
	"ReplicaSet":  {attr: "k8s.replicaset.name"},
	"Deployment":  {attr: "k8s.deployment.name", workload: true},
	"StatefulSet": {attr: "k8s.statefulset.name", workload: true},
	"DaemonSet":   {attr: "k8s.daemonset.name", workload: true},
	"Job":         {attr: "k8s.job.name", workload: true},
	"CronJob":     {attr: "k8s.cronjob.name", workload: true},
	"Node":        {attr: "k8s.node.name"},
}

// ServiceName derives the OTLP service.name for a pod: the name of its
// workload owner (Deployment/StatefulSet/DaemonSet/Job/CronJob), falling back
// to the pod name. A ReplicaSet owner is not used (its Deployment is).
func ServiceName(pod kubemeta.Pod) string {
	name := pod.Name
	for _, o := range pod.Owners {
		if e, ok := kindTable[o.Kind]; ok && e.workload {
			name = o.Name
		}
	}
	return name
}

// KindAttribute maps a Kubernetes object kind to its k8s.<kind>.name resource
// attribute (e.g. "Deployment" -> "k8s.deployment.name"); ok is false for a kind
// with no such attribute. Shared by the pod owner-chain and the events exporter.
func KindAttribute(kind string) (string, bool) {
	e, ok := kindTable[kind]
	return e.attr, ok
}

// Pod sets the pod-level resource attributes. Empty fields are omitted, never
// stamped as "": a partially-filled Pod must not mint empty-string resource
// attributes (an empty k8s.namespace.name would still participate in routing
// and identity derivation as if it were a value).
func Pod(res pcommon.Resource, pod kubemeta.Pod) {
	a := res.Attributes()
	if pod.Namespace != "" {
		a.PutStr("k8s.namespace.name", pod.Namespace)
	}
	if pod.Name != "" {
		a.PutStr("k8s.pod.name", pod.Name)
	}
	if pod.UID != "" {
		a.PutStr("k8s.pod.uid", pod.UID)
	}
	if pod.NodeName != "" {
		a.PutStr("k8s.node.name", pod.NodeName)
	}
	if pod.PodIP != "" {
		a.PutStr("k8s.pod.ip", pod.PodIP)
	}

	for _, o := range pod.Owners {
		if attr, ok := KindAttribute(o.Kind); ok {
			a.PutStr(attr, o.Name)
		}
	}
	if name := ServiceName(pod); name != "" {
		a.PutStr("service.name", name)
	}

	for k, v := range pod.Labels {
		a.PutStr(prefixedKey(podLabelKeys, podLabelPrefix, k), v)
	}
	if pod.NamespaceMetadata != nil {
		for k, v := range pod.NamespaceMetadata.Labels {
			a.PutStr(prefixedKey(nsLabelKeys, nsLabelPrefix, k), v)
		}
	}
}

// The prefixed attribute key for a label is memoized: label VALUES are data,
// but label KEYS are a property of the cluster's workloads and low-cardinality
// across it, while the concatenation is one allocation per label per RESOURCE
// — paid once per described object on a split path, where a 12k-object scrape
// makes it 84k allocations a cycle. The cache evicts (genCache), so a workload
// minting label keys from data cannot grow it without bound.
const (
	podLabelPrefix = "k8s.pod.label."
	nsLabelPrefix  = "k8s.namespace.label."
	maxLabelKeys   = 4096
)

var (
	podLabelKeys = newGenCache[string](maxLabelKeys)
	nsLabelKeys  = newGenCache[string](maxLabelKeys)
)

func prefixedKey(c *genCache[string], prefix, key string) string {
	if full, ok := c.load(key); ok {
		return full
	}
	full := prefix + key
	c.store(key, full)
	return full
}

// Container adds the container-level resource attributes on top of Pod's.
func Container(res pcommon.Resource, c kubemeta.Container) {
	a := res.Attributes()
	a.PutStr("k8s.container.name", c.Name)
	if c.ID != "" {
		a.PutStr("container.id", c.ID)
	}
	if c.Image != "" {
		a.PutStr("container.image.name", c.Image)
	}
	if c.RestartCount > 0 {
		a.PutInt("k8s.container.restart_count", int64(c.RestartCount))
	}
}

// Service adds attributes identifying the Service a scrape target was
// discovered through.
func Service(res pcommon.Resource, svc *kubemeta.Service) {
	if svc == nil {
		return
	}
	a := res.Attributes()
	a.PutStr("k8s.service.name", svc.Name)
	a.PutStr("k8s.service.uid", svc.UID)
}

// Identity sets service.namespace and service.instance.id so a Prometheus
// backend (e.g. Mimir) derives a unique job (service.namespace/service.name)
// and instance (service.instance.id). It reads the identity attributes already
// on the resource, so it works for pod/container/node resources as well as
// pre-populated ones (kube-state-metrics splitters, ingest, log metrics).
// Neither attribute is overwritten if already set, so a template still wins.
//
// service.instance.id falls back in order: container.id, pod.uid[/container],
// namespace/pod[/container], node.name — mirroring the cmb-alloy pipeline.
func Identity(res pcommon.Resource) {
	a := res.Attributes()
	get := func(k string) string {
		if v, ok := a.Get(k); ok {
			return v.AsString()
		}
		return ""
	}
	ns := get("k8s.namespace.name")
	if ns != "" {
		if _, ok := a.Get("service.namespace"); !ok {
			a.PutStr("service.namespace", ns)
		}
	}
	if _, ok := a.Get("service.instance.id"); ok {
		return
	}
	pod, container := get("k8s.pod.name"), get("k8s.container.name")
	uid, cid, node := get("k8s.pod.uid"), get("container.id"), get("k8s.node.name")
	var inst string
	switch {
	case cid != "":
		inst = cid
	case uid != "" && container != "":
		inst = uid + "/" + container
	case uid != "":
		inst = uid
	case ns != "" && pod != "" && container != "":
		inst = ns + "/" + pod + "/" + container
	case ns != "" && pod != "":
		inst = ns + "/" + pod
	case node != "":
		inst = node
	}
	if inst != "" {
		a.PutStr("service.instance.id", inst)
	}
}

// FillAbsent adds src's attributes to dst, never overwriting a key dst already
// has. It is how every "someone else knows more about this resource" merge in
// the agent is applied — the ingest enricher over a sender's resource, the
// self-metadata stamping over a process's own identity — because in both the
// existing value is the authoritative one.
func FillAbsent(src, dst pcommon.Map) {
	src.Range(func(k string, v pcommon.Value) bool {
		if _, exists := dst.Get(k); !exists {
			v.CopyTo(dst.PutEmpty(k))
		}
		return true
	})
}

// PrefixInstance prepends prefix (+ "-") to service.instance.id so resources
// produced by an exporter that DESCRIBES other objects — cadvisor, or a
// kube-state-metrics splitter — get an instance distinct from those objects'
// own self-scraped metrics (which share the same service.name / namespace).
// Without it the two collide on (job, instance) with different resource
// attributes, flapping target_info. Mirrors cmb-alloy's instance_prefix. A
// resource with no service.instance.id is left alone — a bare prefix would
// stamp every such resource with the same meaningless instance. No-op for "".
func PrefixInstance(res pcommon.Resource, prefix string) {
	if prefix == "" {
		return
	}
	a := res.Attributes()
	if v, ok := a.Get("service.instance.id"); ok {
		a.PutStr("service.instance.id", prefix+"-"+v.AsString())
	}
}

// reservedIdentity is the set behind ReservedIdentity/ReservedIdentityKeys.
var reservedIdentity = map[string]struct{}{
	"k8s.namespace.name":  {},
	"k8s.pod.name":        {},
	"k8s.pod.uid":         {},
	"k8s.pod.ip":          {},
	"k8s.container.name":  {},
	"k8s.node.name":       {},
	"container.id":        {},
	"container.name":      {},
	"service.namespace":   {},
	"service.instance.id": {},
}

// ReservedIdentity reports whether key is a RESOLVED-identity resource
// attribute — one the agent derives from the API server — which a workload
// (pod annotation) or a log line must therefore never set.
//
// The boundary is security, not tidiness: `k8s.namespace.name` is what
// internal/agent/route keys tenancy on, so an unfiltered write would let
// anyone who can annotate a pod in their OWN namespace — an ordinary
// namespace-scoped action — send that pod's telemetry to a different tenant's
// endpoint under that tenant's header, and remove it from their own. The
// service.instance.id / service.namespace / k8s.pod.* / container.* keys forge
// series identity in the backend and on every log-derived metric bound to the
// resource. The operator's attrs.Filter cannot stand in for this check: it
// runs inside Builder.Build, BEFORE workload- or line-supplied attributes are
// applied on top.
//
// `service.name` is deliberately NOT reserved — it is descriptive, and
// overriding it (renaming the workload's own series, not moving them to
// another owner) is the pod annotation's documented purpose.
func ReservedIdentity(key string) bool {
	_, bad := reservedIdentity[key]
	return bad
}

// ReservedIdentityKeys returns the reserved key set, sorted, for callers that
// enumerate it (documentation, tests) rather than test membership.
func ReservedIdentityKeys() []string {
	return slices.Sorted(maps.Keys(reservedIdentity))
}
