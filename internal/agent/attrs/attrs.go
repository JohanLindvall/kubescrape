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

	var nsLabels map[string]string
	if pod.NamespaceMetadata != nil {
		nsLabels = pod.NamespaceMetadata.Labels
	}
	if len(pod.Labels)+len(nsLabels) > bulkLabelThreshold {
		putLabelsBulk(a, pod.Labels, nsLabels)
		return
	}
	for k, v := range pod.Labels {
		a.PutStr(prefixedKey(podLabelKeys, podLabelPrefix, k), v)
	}
	for k, v := range nsLabels {
		a.PutStr(prefixedKey(nsLabelKeys, nsLabelPrefix, k), v)
	}
}

// bulkLabelThreshold is the label count above which Pod stamps labels in ONE
// linear pass instead of one PutStr each.
//
// pcommon.Map.PutStr scans the map for an existing key before it appends, so n
// labels cost O(n^2) key compares — and a label COUNT is tenant-authored and
// bounded only by the API server's object size (the metadata service
// deliberately does not cap labels: they are selection input). Build runs per
// RESOURCE per cycle (per pod and per container on the cadvisor scrape, per
// target, per described object on a split), so one pod with tens of thousands
// of labels spent seconds of the node agent's scrape budget on every cycle:
// measured 0.5 s at 10k labels and 7 s at 40k, against ~14 ms and ~43 ms on
// the bulk path. pdata has no append-without-lookup, and Map.FromRaw is its
// only O(n) bulk write.
//
// Below the threshold the PutStr loop stays: it is the allocation-budgeted
// path every real pod takes (TestBuildAllocationBudget), and at a few dozen
// labels the quadratic term is noise. Merge takes its bulk path above the same
// source size, for the same quadratic.
const bulkLabelThreshold = 64

// putLabelsBulk writes the prefixed pod and namespace labels into a with the
// same result the PutStr loop gives — a label OVERWRITES a same-named
// attribute already on the resource, and every other attribute keeps its type
// and value — in time linear in the attribute count: the map round-trips
// through AsRaw/FromRaw, which rebuilds it in one pass.
//
// Two differences from the PutStr loop, neither observable to anything in this
// repo: attribute ORDER follows Go map iteration (the labels' order was
// already random, and every resource identity fold is order-independent), and
// a key the resource carried TWICE collapses to its last value, where Get
// would have read the first — Build's inputs never carry a duplicate.
func putLabelsBulk(a pcommon.Map, podLabels, nsLabels map[string]string) {
	raw := a.AsRaw()
	for k, v := range podLabels {
		raw[prefixedKey(podLabelKeys, podLabelPrefix, k)] = v
	}
	for k, v := range nsLabels {
		raw[prefixedKey(nsLabelKeys, nsLabelPrefix, k)] = v
	}
	// FromRaw fails only on a value type AsRaw never produces, so the error is
	// unreachable here; the map is fully rebuilt either way.
	_ = a.FromRaw(raw)
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
// Empty fields are omitted, never stamped as "", for Pod's reason: an empty
// k8s.container.name (or container.id) would enter series identity and
// log-metric label sets as a value rather than an absence. (There is no
// sender value for an empty stamp to blank on the ingest path: the
// application-facing receivers strip a sender's own k8s.container.name at
// receipt, before enrichment runs — otlpingest.Enricher.SenderIdentityStrip.)
func Container(res pcommon.Resource, c kubemeta.Container) {
	a := res.Attributes()
	if c.Name != "" {
		a.PutStr("k8s.container.name", c.Name)
	}
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
// discovered through. Empty fields are omitted for Pod's reason — a Service
// resolved by name but not by UID must not put an empty k8s.service.uid into
// the series identity, where it reads as a value rather than as an absence.
func Service(res pcommon.Resource, svc *kubemeta.Service) {
	if svc == nil {
		return
	}
	a := res.Attributes()
	if svc.Name != "" {
		a.PutStr("k8s.service.name", svc.Name)
	}
	if svc.UID != "" {
		a.PutStr("k8s.service.uid", svc.UID)
	}
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
// has — Merge with nothing replaced. The self-metadata stamping applies it
// over a process's own identity, where the existing value is the
// authoritative one.
func FillAbsent(src, dst pcommon.Map) { Merge(src, dst, nil) }

// Merge writes src's attributes into dst: a key dst lacks is added, and a key
// dst already has is replaced only when replace(key) reports true (nil
// replaces nothing). src is only READ. It is the one "someone else knows more
// about this resource" merge: FillAbsent, the ingest enricher's resolved-wins
// merge over a sender's resource, and the split path's overwrite of a
// described object's.
//
// It is linear in the attribute count whatever the sizes. The obvious per-key
// loop is not: pcommon.Map's Get and PutEmpty each SCAN for the key, so it
// costs O(|src| x (|dst|+|src|)) key compares — and on the ingest path both
// sides are tenant-authored: src carries the resolved pod's labels (a count
// bounded only by the API server's object size; attrs.Pod already stamps them
// in one pass for that reason) and dst is the sender's resource, merged once
// per resource of every push. Above bulkLabelThreshold src attributes the
// merge therefore rebuilds dst in one pass (mergeBulk); at or below it the
// loop stays — allocation-free, order-preserving, and linear in |dst| for a
// src that small.
func Merge(src, dst pcommon.Map, replace func(key string) bool) {
	if src.Len() > bulkLabelThreshold {
		mergeBulk(src, dst, replace)
		return
	}
	src.Range(func(k string, v pcommon.Value) bool {
		if _, exists := dst.Get(k); !exists || (replace != nil && replace(k)) {
			v.CopyTo(dst.PutEmpty(k))
		}
		return true
	})
}

// mergeBulk is Merge in one pass. Each output key's source is decided through
// a Go map, the output is allocated by ONE Map.FromRaw of placeholder values —
// pdata's only linear bulk write — and the values then go in: dst's are MOVED
// (a pointer move, so a sender's structured value is never boxed through
// AsRaw nor deep-copied, the cost attrs.Pod's AsRaw round trip can afford on
// label strings and this path cannot on sender content), src's are copied.
//
// Two differences from the per-key loop, neither observable to anything in
// this repo: attribute ORDER follows Go map iteration (every resource
// identity fold is order-independent), and a key dst carried TWICE collapses
// to one entry — the first, which is what Get reads and what the loop would
// have written into — where the loop left the later copy on the wire beside
// it. When nothing is added or replaced dst is left exactly as it was, as the
// loop leaves it.
func mergeBulk(src, dst pcommon.Map, replace func(key string) bool) {
	type slot struct {
		v       pcommon.Value
		fromSrc bool
	}
	slots := make(map[string]slot, dst.Len()+src.Len())
	dst.Range(func(k string, v pcommon.Value) bool {
		if _, dup := slots[k]; !dup {
			slots[k] = slot{v: v}
		}
		return true
	})
	changed := false
	src.Range(func(k string, v pcommon.Value) bool {
		if _, exists := slots[k]; !exists || (replace != nil && replace(k)) {
			slots[k] = slot{v: v, fromSrc: true}
			changed = true
		}
		return true
	})
	if !changed {
		return
	}
	keys := make(map[string]any, len(slots))
	for k := range slots {
		keys[k] = nil
	}
	out := pcommon.NewMap()
	// Every value is nil, which FromRaw maps to an empty Value: it cannot fail.
	_ = out.FromRaw(keys)
	out.Range(func(k string, ov pcommon.Value) bool {
		if s := slots[k]; s.fromSrc {
			s.v.CopyTo(ov)
		} else {
			s.v.MoveTo(ov)
		}
		return true
	})
	out.MoveTo(dst)
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

// identityKeys is the set behind IdentityKeys: every FIXED resource-attribute
// key the built-in mapping can emit, i.e. everything Pod, Container, Service,
// ServiceName and Identity put — the attributes that describe the resource's
// OWN object. The kind attributes are derived from kindTable (itself
// test-pinned against owners.AllGVRs), and
// TestIdentityKeysCoverEveryFixedEmission pins the rest against the actual
// emissions, so a key added to any of those functions without landing here
// fails a test naming this file.
//
// Deliberately NOT here: the label attributes (k8s.pod.label.*,
// k8s.namespace.label.*), whose keys are data (one per cluster label key) and
// cannot be enumerated — consumers of this list strip by exact key.
var identityKeys = func() []string {
	set := map[string]struct{}{
		// Pod
		"k8s.namespace.name": {},
		"k8s.pod.name":       {},
		"k8s.pod.uid":        {},
		"k8s.node.name":      {},
		"k8s.pod.ip":         {},
		// Container
		"k8s.container.name":          {},
		"container.id":                {},
		"container.image.name":        {},
		"k8s.container.restart_count": {},
		// Service
		"k8s.service.name": {},
		"k8s.service.uid":  {},
		// ServiceName (via Pod) and Identity
		"service.name":        {},
		"service.namespace":   {},
		"service.instance.id": {},
	}
	for _, e := range kindTable {
		set[e.attr] = struct{}{}
	}
	return slices.Sorted(maps.Keys(set))
}()

// IdentityKeys returns (a copy of) every fixed resource-attribute key this
// package's built-in mapping can EMIT, sorted — the builder's own emission
// set, and nothing a sender alone might set. It is NOT the list a receiver
// strips: SenderIdentityKeys extends it with the sender-only keys
// (container.name), and that superset is what the ingest splitter removes
// from a sender's resource before re-labelling it as a described object's.
// This one stays exported as the independent oracle for that superset —
// otlpingest's senderidentity_test asserts the splitter's list covers every
// key the builder can emit, which comparing SenderIdentityKeys with itself
// could not.
func IdentityKeys() []string {
	return slices.Clone(identityKeys)
}

// senderOnlyIdentityKeys are identity attributes this package never EMITS but
// which name a resource's own object just as surely, because SDK detectors set
// them. They exist so SenderIdentityKeys can be a superset of IdentityKeys
// without anyone hand-copying a list: a receiver deciding "does this attribute
// claim WHO the resource is" has to answer yes for these too.
var senderOnlyIdentityKeys = []string{
	// container.name: the bare semconv sibling of k8s.container.name. The
	// builder never emits it (kubemeta names containers under the k8s.* form),
	// but Go's resource.WithContainer, the Java ContainerResource detector and
	// the collector's resourcedetection processor all do.
	"container.name",
}

// SenderIdentityKeys returns (a copy of) every resource-attribute key that
// names WHOSE data a resource is — IdentityKeys plus the semconv siblings a
// SENDER may set that this package never emits, sorted.
//
// It is the list a receiver uses when it must treat a sender's identity
// attributes as claims rather than facts: the ingest splitter strips exactly
// these from a resource it is about to re-label as a DESCRIBED object's,
// because the overwrite that follows only replaces keys the builder emits for
// that object, so any key the described object lacks would survive from the
// exporter onto the object it describes.
//
// It lives here, next to the mapping it is derived from, because a hand-copied
// copy of it in the consumer had already drifted once — a sender's
// k8s.service.name/uid leaked onto every split-described object when Service
// grew those keys. TestSenderIdentityKeysCoverEveryBuilderIdentityKey pins it
// in both directions.
func SenderIdentityKeys() []string {
	out := append(slices.Clone(identityKeys), senderOnlyIdentityKeys...)
	slices.Sort(out)
	return out
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

// reservedPlumbing is the set behind ReservedPlumbing: kubescrape's OWN
// control-plane resource attributes, which describe how the pipeline should
// treat a payload rather than what the payload is ABOUT. They must never be set
// by an untrusted input surface. The literals are duplicated here (rather than
// imported from internal/agent/route and internal/agent/transform, which
// would be an import cycle — both depend on this package) and pinned to the
// originals by TestReservedPlumbingMatchesTheMarkers so they cannot drift.
var reservedPlumbing = map[string]struct{}{
	"kubescrape.route":    {}, // == route.ScriptMarker
	"__kubescrape_drop__": {}, // == transform.DropMarker
}

// ReservedPlumbing reports whether key is one of kubescrape's own control-plane
// markers — the router's route selector and the transform prune marker — which
// a workload (pod annotation) or a log line must NEVER set.
//
// The boundary is security, the sibling of ReservedIdentity's: the router
// honours route.ScriptMarker on a resource BEFORE its namespace globs, so an
// unfiltered write lets anyone who can annotate a pod in their own namespace
// steer that pod's telemetry to a different tenant's route and its tenant
// headers (X-Scope-OrgID) — the same attack ReservedIdentity blocks via the
// k8s.namespace.name relabel, reached here more directly. The application-facing
// ingest listeners already strip these keys (otlpingest.ReservedAttrs); this is
// the predicate the per-node input surfaces (the tailer's pod annotation, a
// logAttributes rule) share so a fourth surface cannot drift from the strip.
func ReservedPlumbing(key string) bool {
	_, bad := reservedPlumbing[key]
	return bad
}

// ReservedPlumbingKeys returns the reserved plumbing set, sorted, for callers
// that enumerate it (documentation, the drift cross-check test).
func ReservedPlumbingKeys() []string {
	return slices.Sorted(maps.Keys(reservedPlumbing))
}
