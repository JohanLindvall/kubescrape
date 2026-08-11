package promscrape

// The cadvisor scrape batcher: cgroup-identity routing of kubelet series
// into one OTLP resource per pod/container, with metadata enrichment via a
// TTL cache over the metadata service.

import (
	"context"
	"strconv"
	"strings"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/JohanLindvall/kubescrape/internal/obs"
	"github.com/JohanLindvall/kubescrape/pkg/kubemeta"
	"github.com/JohanLindvall/kubescrape/pkg/metaclient"

	"github.com/JohanLindvall/kubescrape/pkg/cgroupid"
)

// podMetaCacheTTL bounds how long resolved (or not-found) metadata is reused
// across cadvisor scrape cycles.
const podMetaCacheTTL = time.Minute

// podCacheMaxEntries bounds the metadata cache. Each entry holds a full
// kubemeta.Pod document, and the objects a splitter describes are the whole
// cluster's, not this node's.
const podCacheMaxEntries = 8192

// podCacheLowWater is what a size trim leaves behind. Trimming BELOW the cap
// rather than down to it amortizes the O(n) sweep over ~2000 inserts instead of
// running it on every insert while full, which matters because the sweep holds
// cacheMu — shared by every concurrent scrape goroutine's lookup. Same shape,
// and the same reason, as metaclient's evictLowWater.
const podCacheLowWater = podCacheMaxEntries * 3 / 4

// podCacheSweepEvery is how often expired entries are swept below the cap.
const podCacheSweepEvery = time.Minute

type podCacheEntry struct {
	pod       *kubemeta.Pod       // nil: lookup failed / unknown
	container *kubemeta.Container // set for container-ID entries
	fetched   time.Time
	// answered records WHY a negative entry is negative, which is the one thing
	// a caller cannot reconstruct from a nil pod: true means the metadata
	// service replied and its reply was 404 (this id is not a container of any
	// pod it knows — a pod sandbox's permanent condition), false means it could
	// not be reached or could not answer (transport failure, 5xx, an
	// undecodable body).
	//
	// It has to live IN the cache, not just in the return value, because a
	// negative entry is served for podMetaCacheTTL: reconstructing the reason
	// as "definitive" on a cache hit would turn one unreachable minute into a
	// definitive verdict for every lookup inside it — which is exactly the
	// distinction internal/agent/cgroupstats' retry policy is built on.
	answered bool
}

// cadvisorBatcher implements sink, routing each point into a ResourceMetrics
// chosen by the sample's namespace/pod/container labels. Those labels move
// into the resource attributes (enriched from the metadata service); the
// remaining labels stay on the data point.
type cadvisorBatcher struct {
	s        *Scraper
	ctx      context.Context
	startTS  pcommon.Timestamp
	scrapeTS pcommon.Timestamp

	md     pmetric.Metrics
	scopes map[string]pmetric.ScopeMetrics // resource key -> scope
	byKey  map[string]pmetric.Metric       // resource key + metric name
	points int
	bytes  int

	// Last-seen memos, mirroring the plain batcher's metricByName/remember:
	// cadvisor series arrive grouped by family and cgroup, so consecutive
	// samples often share the identity and metric — a struct compare replaces
	// the key-building allocations and map probes.
	lastIdent    cadvisorIdentity
	lastScope    pmetric.ScopeMetrics
	lastScopeOK  bool
	lastMetIdent cadvisorIdentity
	lastName     string
	lastMetric   pmetric.Metric
	lastPodScope bool
	lastOK       bool
	// keyBuf is the scratch buffer for identity/metric keys: probes use
	// map[string(keyBuf)] (no allocation); the string materializes only when a
	// new resource or metric is inserted.
	keyBuf []byte

	// cgroupMemo caches cgroupid.Parse per raw "id" value: each container's
	// cgroup path recurs in every family of the scrape (~60×), and the parse
	// (plus the systemd layout's uid underscore rewrite) is the expensive part
	// of identityOf. The mapping is pure, so the memo survives reset() and
	// lives for the batcher's one scrape.
	cgroupMemo map[string]cgroupPair
}

type cgroupPair struct {
	podUID, containerID string
	// podContainer is cgroupid.Parse's shape verdict: the container scope is the
	// pod slice's immediate child. It is memoized with the ids because isSandbox
	// needs it per sample and re-deriving it would mean a second walk.
	podContainer bool
}

func newCadvisorBatcher(s *Scraper, scrape time.Time, ctx context.Context) *cadvisorBatcher {
	cb := &cadvisorBatcher{
		s:          s,
		ctx:        ctx,
		startTS:    pcommon.NewTimestampFromTime(s.cfg.StartTime),
		scrapeTS:   pcommon.NewTimestampFromTime(scrape),
		cgroupMemo: make(map[string]cgroupPair, 256),
	}
	cb.reset()
	return cb
}

func (cb *cadvisorBatcher) reset() {
	cb.md = pmetric.NewMetrics()
	if cb.scopes == nil {
		cb.scopes = make(map[string]pmetric.ScopeMetrics)
		cb.byKey = make(map[string]pmetric.Metric)
	} else {
		clear(cb.scopes)
		clear(cb.byKey)
	}
	cb.points = 0
	cb.bytes = 0
	// The memoized handles point into the previous batch's payload.
	cb.lastScopeOK = false
	cb.lastOK = false
}

func (cb *cadvisorBatcher) take() pmetric.Metrics {
	md := cb.md
	cb.reset()
	return md
}

func (cb *cadvisorBatcher) count() int { return cb.points }

func (cb *cadvisorBatcher) size() int { return cb.bytes }

// cadvisorIdentity is the resource identity of one cadvisor sample: the
// namespace/pod/container labels plus the pod UID and container ID parsed
// from the cgroup path in the "id" label.
type cadvisorIdentity struct {
	namespace, pod, container string
	podUID, containerID       string
	image                     string // "image" label, container rows only
	hasCgroup                 bool   // an "id" label was present
	// sandbox marks a row describing the pod's SANDBOX (see isSandbox). It is
	// deliberately NOT part of appendKey — the pause row still folds into the
	// pod's resource, which is the intended behaviour — but it must survive to
	// putFilteredLabels: with the pod-cgroup row of the same family the two
	// identities are otherwise byte-identical (the podUID key branch omits
	// namespace/pod, and the sandbox's containerID and image are cleared in
	// identityOf), so both land on one metric and the redundant-label elision
	// then removes the only labels that told them apart — two data points with
	// identical attribute sets in one metric, which is one series downstream.
	sandbox bool
}

func (cb *cadvisorBatcher) identityOf(labels []Label) cadvisorIdentity {
	var ident cadvisorIdentity
	labelledPOD, podContainer := false, false
	for _, l := range labels {
		switch l.Name {
		case "namespace":
			ident.namespace = l.Value
		case "pod":
			ident.pod = l.Value
		case "container":
			if l.Value == "POD" {
				labelledPOD = true
			} else {
				ident.container = l.Value
			}
		case "image":
			ident.image = l.Value
		case "id":
			ident.hasCgroup = true
			pair, ok := cb.cgroupMemo[l.Value]
			if !ok {
				pair.podUID, pair.containerID, pair.podContainer = cgroupid.Parse(l.Value)
				if len(cb.cgroupMemo) < maxTrackedFamilies {
					cb.cgroupMemo[l.Value] = pair
				}
			}
			ident.podUID, ident.containerID = pair.podUID, pair.containerID
			podContainer = pair.podContainer
		}
	}
	// A sandbox row is pod-level: its cgroup names the PAUSE container, whose ID
	// is not among the pod's container statuses, so a lookup can only miss and
	// mint an unresolved resource of its own; the image label names the pause
	// image, never the workload. Clearing both is what makes the row share the
	// pod's resource — where putFilteredLabels then KEEPS its `id`, the one
	// label separating the pause point from the pod-cgroup point of the same
	// family.
	if isSandbox(ident, labelledPOD, podContainer) {
		ident.containerID = ""
		ident.image = ""
		ident.sandbox = true
	}
	return ident
}

// isSandbox reports whether a cadvisor row describes the pod's sandbox (the
// pause / infra container) rather than a workload container, a runtime helper
// cgroup, or a cgroup cadvisor could not attribute to a pod at all. labelledPOD
// is the container="POD" label, which the caller has already stripped from
// ident; podContainer is cgroupid.Parse's shape verdict — the row's cgroup is a
// container scope DIRECTLY beneath a pod slice.
//
// The predicate identifies a sandbox POSITIVELY and everything that fails it
// keeps its own resource, because the two errors are not symmetric. Folding is
// DESTRUCTIVE: the row shares the pod's resource, and scope() fills a resource
// from the FIRST identity that reaches it, so a wrongly folded row arriving
// before the pod's own rows would name the resource that every container_*
// series of that pod is exported under. Not folding costs one extra resource.
// An absence-based rule ("no container name" ⇒ sandbox) got this backwards.
//
// Two shapes, one per runtime family, and both must be caught or the pod grows a
// phantom third resource carrying the pause image and an unresolvable id:
//
//   - dockershim and CRI-O label the infra container container="POD". CRI-O is
//     current and still does; dockershim was removed in Kubernetes 1.24. The
//     convention is not extinct — it was never containerd's, which is why this,
//     as the ONLY predicate, had stopped firing in practice: a live containerd
//     v1.33 node emits ZERO such rows.
//
//   - containerd's sandbox is not a CRI *container*: it carries the pod's
//     io.kubernetes.pod.{name,namespace} labels but no io.kubernetes.container.name,
//     so cadvisor emits namespace/pod with container="" — the same empty
//     container label the POD CGROUP row carries. Only the cgroup path separates
//     the two: the pod slice parses to (uid, ""), the pause scope beneath it to
//     (uid, <container id>).
//
// The pod ATTRIBUTION is the load-bearing check, and BOTH arms require it.
// cadvisor learns `pod`/`namespace` and `container` from the SAME CRI labels, so
// a row carrying the pod but no container name is the sandbox by construction,
// while every impostor is a RAW cgroup cadvisor could not attribute at all and
// so carries neither: raw-handler rows, CRI-O's crio-conmon-<id>.scope, a
// kata/gVisor helper scope parked in the pod slice, a container whose CRI lookup
// failed. Requiring it is also what BOUNDS a mis-fold: a folded row always names
// the pod whose resource it joins, so whichever row creates that resource it
// resolves to the same pod, and the worst case is one extra data point —
// distinguishable, since putFilteredLabels keeps a sandbox row's `id` — never a
// stripped identity.
//
// The pause IMAGE corroborates but is never required: containerd's sandbox_image
// is configurable and a fleet mirroring it under its own name must not regrow
// the phantom. It is the SOLE evidence in one place only — a cgroup root the pod
// segment does not parse out of (a custom --cgroup-root), where there is no uid
// to place the scope by — and it is never consulted for a row that names its
// container, so an unlucky workload image cannot swallow a real container.
//
// Known gaps, all failing the safe way (the row keeps its own resource, which is
// the pre-fold behaviour: a phantom, not a loss):
//
//   - a custom --cgroup-root AND a custom sandbox_image together satisfy neither
//     arm;
//   - a runtime whose sandbox row carries no pod attribution at all.
//
// Pre-existing and NOT about the fold: an unrecognised child of a pod slice
// whose name yields no container id (kata's kata_<sandbox-id>) parses as the
// POD's own cgroup and shares its resource — the path alone cannot tell it from
// the pod slice, and this predicate never sees it.
func isSandbox(ident cadvisorIdentity, labelledPOD, podContainer bool) bool {
	if ident.namespace == "" || ident.pod == "" {
		// cadvisor could not attribute the cgroup to a pod, so nothing here says
		// WHICH pod's sandbox it would be — and a fold must name the resource it
		// joins.
		return false
	}
	if labelledPOD {
		return true // the runtime said so
	}
	if ident.container != "" || ident.containerID == "" {
		return false // a named workload container, or not a container cgroup at all
	}
	if podContainer {
		return true
	}
	// No parseable pod segment (a custom --cgroup-root): the image is what is
	// left. A parseable one that placed the scope somewhere OTHER than directly
	// under the pod slice is a helper cgroup, not the sandbox.
	return ident.podUID == "" && isPauseImage(ident.image)
}

// isPauseImage reports whether an image reference names a sandbox image.
// Registry and tag are stripped so every mirror spelling matches
// (registry.k8s.io/pause:3.10, gcr.io/google_containers/pause-amd64:3.1,
// rancher/mirrored-pause:3.6, registry:5000/pause@sha256:…), and the match is on
// the repository's LAST path segment rather than a substring of the whole
// reference: a registry host or a namespace containing "pause" must not read as
// the sandbox. Allocation-free — it runs inside identityOf, once per sample.
func isPauseImage(image string) bool {
	if i := strings.IndexByte(image, '@'); i >= 0 {
		image = image[:i] // digest
	}
	if i := strings.LastIndexByte(image, '/'); i >= 0 {
		image = image[i+1:] // registry host (which may carry a :port) and namespace
	}
	if i := strings.IndexByte(image, ':'); i >= 0 {
		image = image[:i] // tag
	}
	return image == "pause" || strings.HasPrefix(image, "pause-") || strings.HasSuffix(image, "-pause")
}

// rollup reports whether the sample belongs to a cgroup above pod level.
func (id cadvisorIdentity) rollup() bool {
	return id.hasCgroup && id.pod == "" && id.podUID == "" && id.containerID == ""
}

// appendKey appends the resource identity key to b. The cgroup-derived
// identity is preferred: the pod UID disambiguates same-name pod recreations
// and the container ID distinguishes restarted container incarnations. It
// appends into a caller-owned scratch buffer so the per-sample map probes can
// use map[string(buf)] lookups without materializing a string (the parser's
// keyBuf discipline).
func (id cadvisorIdentity) appendKey(b []byte) []byte {
	// The cgroup-derived parts (uid, container id) are validated hex/UUID and
	// cannot contain the separator; namespace/pod/container come from exporter
	// LABELS, so they are length-prefixed to keep hostile values from aliasing
	// another identity.
	if id.podUID != "" {
		b = append(b, 'u', 0)
		b = append(b, id.podUID...)
		b = append(b, 0)
		b = append(b, id.containerID...)
		b = append(b, 0)
		return appendLP(b, id.container)
	}
	// containerID must participate: a non-pod cgroup with a parseable container
	// ID (a standalone, non-k8s container) has no namespace/pod/container labels,
	// and omitting the ID would merge every such container into one anonymous
	// resource with indistinguishable, conflicting series.
	b = append(b, 'n', 0)
	b = appendLP(b, id.namespace)
	b = appendLP(b, id.pod)
	b = appendLP(b, id.container)
	return append(b, id.containerID...)
}

// appendLP appends one length-prefixed key part — the package's ONE
// injective-join rule for anything data-derived that lands in a map key or a
// cache-key fingerprint (resource identities here and in labelKey, the TLS
// client key via lp, the relabel-chain fingerprint). Delimiter joins are not
// collision-proof: exposition label values and secret PEM may contain any
// delimiter byte, and two configurations serialising to one key silently merge.
func appendLP(b []byte, v string) []byte {
	b = strconv.AppendInt(b, int64(len(v)), 10)
	b = append(b, ':')
	return append(b, v...)
}

// isIdentityLabel reports whether a label moved into the resource.
func isIdentityLabel(name string) bool {
	return name == "namespace" || name == "pod" || name == "container"
}

// redundantOnPodRow reports whether a label duplicates (or, for network rows'
// pause-container image/name, contradicts) the resolved resource identity of a
// pod- or container-identified row: the cgroup path in "id" is already parsed
// into pod uid + container.id, "name" is the runtime container name behind
// container.id, "image" lands on the resource. cmb-alloy deletes all three.
// Rollup rows keep "id" — there it is the only distinguisher between cgroups
// sharing the node-level resource.
func redundantOnPodRow(name string) bool {
	return name == "id" || name == "name" || name == "image"
}

// podScoped reports whether the sample resolved to a pod- or container-level
// resource (as opposed to a rollup cgroup or a machine_* row).
func (id cadvisorIdentity) podScoped() bool {
	return id.pod != "" || id.podUID != "" || id.containerID != "" || id.container != ""
}

// scope returns the ScopeMetrics for the resource identified by ident,
// creating it (with metadata-service enrichment) on first use per batch. The
// previous sample's identity is memoized: a repeat costs a struct compare
// instead of building the key.
func (cb *cadvisorBatcher) scope(ident cadvisorIdentity) pmetric.ScopeMetrics {
	if cb.lastScopeOK && ident == cb.lastIdent {
		return cb.lastScope
	}
	cb.keyBuf = ident.appendKey(cb.keyBuf[:0])
	sm, ok := cb.scopes[string(cb.keyBuf)] // no alloc: map read elides the copy
	if !ok {
		key := string(cb.keyBuf) // materialize once per new resource per batch
		rm := cb.md.ResourceMetrics().AppendEmpty()
		cb.fillResource(rm.Resource(), ident)
		sm = rm.ScopeMetrics().AppendEmpty()
		sm.Scope().SetName(scopeNameCadvisor)
		sm.Scope().SetVersion(obs.ScopeVersion)
		cb.scopes[key] = sm
		// One resource per pod/container: its attributes count toward the chunk
		// size (see convert.go).
		cb.bytes += resourceBytes(rm.Resource(), scopeNameCadvisor)
	}
	cb.lastIdent, cb.lastScope, cb.lastScopeOK = ident, sm, true
	return sm
}

// fillResource builds the resource attributes for one identity, preferring
// the exact container incarnation (by container ID from the cgroup path),
// then the pod (by name, cross-checked against the cgroup pod UID), then the
// raw label identity.
func (cb *cadvisorBatcher) fillResource(res pcommon.Resource, ident cadvisorIdentity) {
	// The verdict is the cgroup sampler's business, not a scraped row's: a
	// cadvisor row is exported either way, with the label identity as its
	// fallback.
	_, _ = cb.s.fillIdentityResource(cb.ctx, res, ident)
}

// FillContainerResource builds the resource attributes describing one container
// identified by its CGROUP PATH — the runtime container id and the pod uid of
// the slice it sits in — using exactly the machinery a cadvisor row goes
// through: the same metadata lookup, the same TTL cache, the same
// unresolved-row fallback and the same `cadvisor` attribute builder (hence the
// same instance prefix).
//
// It exists for internal/agent/cgroupstats, whose six gauges are only worth
// anything if they JOIN the cadvisor series they explain — and two series join
// when their resource attributes, and so the derived Prometheus job and
// instance, are byte-identical. A second implementation would agree with this
// one until the first edit to either; sharing the body means the sampler cannot
// drift from the scrape even in principle.
//
// The first bool is what a cgroup path costs. A cadvisor row carries namespace,
// pod and container LABELS, so an unresolvable one still falls back to a
// resource with a service.name (the pod name) and keeps its Prometheus `job`. A
// cgroup path yields only two ids, so the same fallback here produces a resource
// with no service.name at all — a series attributed to nothing, joining none of
// the cadvisor series the sampler exists to explain, and one successful later
// lookup away from silently becoming a DIFFERENT series. So the verdict is
// reported and the caller declines to export what did not resolve; see
// cgroupstats.Resolver.
//
// The second bool CLASSIFIES a failure, and it exists because the two ways to
// not resolve want opposite retry policies. See cgroupstats.Resolver for the
// contract and containerMeta for where the classification comes from.
func (s *Scraper) FillContainerResource(ctx context.Context, res pcommon.Resource, containerID, podUID string) (ok, answered bool) {
	return s.fillIdentityResource(ctx, res, cadvisorIdentity{containerID: containerID, podUID: podUID})
}

// fillIdentityResource is fillResource's body, lifted onto the Scraper so the
// exported seam above and the batcher below are literally one code path. It
// reports whether the metadata service PLACED the identity (as opposed to the
// resource having been built from the caller's own labels), and whether it
// ANSWERED at all.
func (s *Scraper) fillIdentityResource(ctx context.Context, res pcommon.Resource, ident cadvisorIdentity) (bool, bool) {
	// Exact container incarnation via the cgroup container ID, else the pod.
	actx, resolved, answered := s.resolveContext(ctx, ident.containerID, ident.namespace, ident.pod, ident.podUID, ident.container, res)
	actx.Node = s.nodeInfo()

	if !resolved && (ident.pod != "" || ident.podUID != "" || ident.containerID != "") {
		// Metadata unavailable (or a same-name pod replaced this one, or a
		// standalone non-k8s container the metadata service cannot know): keep
		// the identity from the labels and the cgroup path — container.id is a
		// containerID-only row's ONLY distinguisher, since its id/name/image
		// labels are elided from the data points as pod-scoped-redundant.
		a := res.Attributes()
		if ident.namespace != "" {
			a.PutStr("k8s.namespace.name", ident.namespace)
		}
		if ident.pod != "" {
			a.PutStr("k8s.pod.name", ident.pod)
		}
		if ident.podUID != "" {
			a.PutStr("k8s.pod.uid", ident.podUID)
		}
		if ident.container != "" {
			a.PutStr("k8s.container.name", ident.container)
		}
		if ident.containerID != "" {
			a.PutStr("container.id", ident.containerID)
		}
		// The image label is elided from container-row data points as
		// resource-redundant; on an unresolved resource it is the only source.
		if ident.image != "" && (ident.container != "" || ident.containerID != "") {
			a.PutStr("container.image.name", ident.image)
		}
		// service.name is otherwise obtained only as a SIDE EFFECT of attrs.Pod
		// running inside Build, which needs resolved metadata — so an
		// unresolved row carried no service.name at all, and the OTLP→Prometheus
		// translation gives it no `job` label. Metadata resolution is not
		// stable (a lookup fails, a pod is replaced by one of the same name),
		// so the SAME series gained and lost its job label as rows resolved and
		// stopped resolving: two different Prometheus series, flapping.
		//
		// attrs.ServiceName's documented fallback chain ends at the pod name,
		// which is exactly what is known here. Build does not overwrite an
		// attribute a caller already set, so a later successful resolution
		// still yields the owner-derived name.
		if ident.pod != "" {
			a.PutStr("service.name", ident.pod)
		}
	}
	s.attrsFor(pipelineCadvisor).Build(res, actx)
	return resolved, answered
}

// metric returns the (per-resource) metric for one sample's identity, plus
// whether the sample's row is pod/container-scoped (its id/name/image labels
// are then redundant with the resource and elided from the data points).
func (cb *cadvisorBatcher) metric(ident cadvisorIdentity, name string, meta metricMeta, shape func(pmetric.Metric)) (pmetric.Metric, bool) {
	// Last-seen fast path: consecutive samples of the same resource and family
	// skip the key building and the map probe entirely.
	if cb.lastOK && name == cb.lastName && ident == cb.lastMetIdent {
		return cb.lastMetric, cb.lastPodScope
	}
	cb.keyBuf = ident.appendKey(cb.keyBuf[:0])
	cb.keyBuf = append(cb.keyBuf, 0)
	cb.keyBuf = append(cb.keyBuf, name...)
	m, ok := cb.byKey[string(cb.keyBuf)] // no alloc: map read elides the copy
	if !ok {
		key := string(cb.keyBuf) // materialize once per new metric per batch
		// scope() reuses keyBuf, so the key must be materialized first.
		m = cb.scope(ident).Metrics().AppendEmpty()
		m.SetName(name)
		shape(m)
		cb.byKey[key] = m
		// One descriptor per resource — including its description and unit,
		// which a per-pod/container batch repeats for every resource.
		cb.bytes += chargeDescriptor(m, name, meta)
	}
	cb.lastMetIdent, cb.lastName, cb.lastMetric, cb.lastPodScope, cb.lastOK = ident, name, m, ident.podScoped(), true
	return m, cb.lastPodScope
}

// drop applies the rollup filter (see KubeletConfig.DisableRollups) to one
// sample's identity.
func (cb *cadvisorBatcher) drop(name string, ident cadvisorIdentity) bool {
	if !cb.s.cfg.Kubelet.DisableRollups {
		return false
	}
	if ident.rollup() {
		return true // above pod level
	}
	// Pod level. TWO row shapes reach this branch and only one of them is a
	// duplicate: the pod CGROUP row is the sum of the pod's containers, while the
	// folded SANDBOX row (isSandbox cleared its container id, which is what lands
	// it here) is a COMPONENT of that sum — the pause container's own usage, which
	// nothing else reports once the pod row is gone.
	//
	// It is dropped anyway, deliberately. The flag's contract is "per-WORKLOAD-
	// container series only", and the sandbox is pod infrastructure: ~1 mcore and
	// a few MiB that no dashboard queries, its series distinguishable from the pod
	// row's only by the raw cgroup path in `id`. Keeping it would also spend back
	// most of what the flag saves here — one point per family per pod, the same
	// order as the pod-cgroup row it would stand in for — so the pod level would be
	// halved rather than dropped. Families with no per-container breakdown pass.
	if ident.hasCgroup && ident.container == "" && ident.containerID == "" &&
		(ident.pod != "" || ident.podUID != "") && !podScopedFamily(name) {
		return true
	}
	return false
}

// podScopedFamily reports whether a cadvisor metric only exists at pod
// level. Network counters are measured on the pod sandbox network
// namespace; there are no per-container rows to roll up.
func podScopedFamily(name string) bool {
	return strings.HasPrefix(name, "container_network_")
}

func (cb *cadvisorBatcher) addNumber(s Sample, monotonic bool) {
	ident := cb.identityOf(s.Labels) // computed once per sample
	if cb.drop(s.Name, ident) {
		return
	}
	m, podScoped := cb.metric(ident, s.Name, sampleMeta(s), func(m pmetric.Metric) {
		shapeNumber(m, monotonic)
	})

	dp, ok := numberDataPoint(m, cb.startTS)
	if !ok {
		return
	}
	dp.SetDoubleValue(s.Value)
	dp.SetTimestamp(pointTS(s.TimestampMs, cb.scrapeTS))
	cb.putFilteredLabels(dp.Attributes(), s.Labels, podScoped, ident.sandbox)
	cb.points++
	cb.bytes += numberBytes(s)
}

func (cb *cadvisorBatcher) addHistogram(family string, acc *histAcc) {
	ident := cb.identityOf(acc.labels)
	if cb.drop(family, ident) {
		return
	}
	m, podScoped := cb.metric(ident, family, acc.meta, shapeHistogram)
	dp, ok := histogramDataPoint(m, cb.startTS)
	if !ok {
		return
	}
	dp.SetTimestamp(pointTS(acc.ts, cb.scrapeTS))
	fillHistogramPoint(dp, acc)
	cb.putFilteredLabels(dp.Attributes(), acc.labels, podScoped, ident.sandbox)
	cb.points++
	cb.bytes += histBytes(acc)
}

func (cb *cadvisorBatcher) addSummary(family string, acc *summAcc) {
	ident := cb.identityOf(acc.labels)
	if cb.drop(family, ident) {
		return
	}
	m, podScoped := cb.metric(ident, family, acc.meta, shapeSummary)
	dp, ok := summaryDataPoint(m, cb.startTS)
	if !ok {
		return
	}
	dp.SetTimestamp(pointTS(acc.ts, cb.scrapeTS))
	fillSummaryPoint(dp, acc)
	cb.putFilteredLabels(dp.Attributes(), acc.labels, podScoped, ident.sandbox)
	cb.points++
	cb.bytes += summBytes(acc)
}

func (cb *cadvisorBatcher) putFilteredLabels(attrs pcommon.Map, labels []Label, podScoped, sandbox bool) {
	for _, l := range labels {
		// `id` is kept on a SANDBOX row: it is the one label distinguishing the
		// pause point from the pod-cgroup point of the same family, which share
		// a resource by design. Eliding it left two data points with identical
		// attributes in one metric.
		keepSandboxID := sandbox && l.Name == "id"
		if isIdentityLabel(l.Name) || (podScoped && redundantOnPodRow(l.Name) && !keepSandboxID) {
			continue
		}
		attrs.PutStr(l.Name, l.Value)
	}
}

// podMeta resolves pod metadata by name with a small TTL cache; nil when
// unknown. The second value is podCacheEntry.answered: on a nil pod it says
// whether the metadata service ANSWERED (a 404) or could not be asked.
func (s *Scraper) podMeta(ctx context.Context, namespace, pod string) (*kubemeta.Pod, bool) {
	key := "n\x00" + namespace + "/" + pod
	if e, ok := s.cacheGet(key); ok {
		return e.pod, e.pod != nil || e.answered
	}
	meta, err := s.metaSource().PodByName(ctx, namespace, pod)
	answered := true
	if err != nil {
		meta = nil
		if ctx.Err() != nil {
			// A cancellation is not an answer, and it is not cached either.
			return nil, false
		}
		answered = metaclient.IsNotFound(err)
	}
	s.cachePut(key, podCacheEntry{pod: meta, fetched: time.Now(), answered: answered})
	return meta, meta != nil || answered
}

// containerMeta resolves the exact container incarnation by runtime ID; nil
// when unknown. The lookup is non-blocking (wait 0): the scraped series only
// reference containers that already exist.
//
// The second value classifies a MISS, and it is what makes the cgroup sampler's
// retry policy possible (internal/agent/cgroupstats): a 404 is a definitive
// statement about this container id — nothing about the node changing will make
// a pause container's id appear in a pod's containerStatuses — while a
// transport failure, a 5xx or an undecodable body is a statement about the
// metadata SERVICE, and the retry that follows must be soon rather than rare.
// Both still produce a nil here, and the cadvisor path still treats them
// alike: the row is exported with its label identity either way.
func (s *Scraper) containerMeta(ctx context.Context, containerID string) (*kubemeta.ContainerMetadata, bool) {
	key := "c\x00" + containerID
	if e, ok := s.cacheGet(key); ok {
		if e.pod == nil {
			return nil, e.answered
		}
		return &kubemeta.ContainerMetadata{ContainerID: containerID, Container: *e.container, Pod: *e.pod}, true
	}
	md, err := s.metaSource().Container(ctx, containerID, 0)
	if err != nil {
		if ctx.Err() != nil {
			return nil, false // do not negative-cache cancellations
		}
		answered := metaclient.IsNotFound(err)
		s.cachePut(key, podCacheEntry{fetched: time.Now(), answered: answered})
		return nil, answered
	}
	s.cachePut(key, podCacheEntry{pod: &md.Pod, container: &md.Container, fetched: time.Now(), answered: true})
	return md, true
}

func (s *Scraper) cacheGet(key string) (podCacheEntry, bool) {
	s.cacheMu.Lock()
	defer s.cacheMu.Unlock()
	e, ok := s.podCache[key]
	if !ok || time.Since(e.fetched) >= podMetaCacheTTL {
		return podCacheEntry{}, false
	}
	return e, true
}

func (s *Scraper) cachePut(key string, e podCacheEntry) {
	s.cacheMu.Lock()
	defer s.cacheMu.Unlock()
	s.podCache[key] = e
	s.evictCacheLocked(time.Now())
}

// evictCacheLocked bounds the cache: expired entries first, then arbitrary ones
// down to the low-water mark. Caller holds cacheMu.
//
// Expiry alone is NOT a bound and cannot be one. The entries a splitter's
// enrichment inserts are all fetched inside one TTL window, so above the cap
// EVERY insert swept the whole map and deleted nothing — 1.3 s per insert at 12k
// objects, 5.2 s at 20k, all of it holding the mutex the concurrent scrape
// goroutines share — and the map never left the branch, so a 12k-pod cluster
// also retained 12k full pod documents. The cadence keeps the sweep amortized
// and the low-water trim is what makes the cap real; an arbitrary eviction costs
// the next lookup a metadata request, which is the price of the hard bound.
func (s *Scraper) evictCacheLocked(now time.Time) {
	if len(s.podCache) <= podCacheMaxEntries {
		if now.Sub(s.cacheSwept) < podCacheSweepEvery {
			return
		}
		s.cacheSwept = now
		s.sweepExpiredLocked(now)
		return
	}
	// The hard trim sweeps on its way to the cap, so the cadence counts from it
	// too: leaving cacheSwept behind made the next below-cap insert re-run a full
	// sweep however recently this one finished.
	s.cacheSwept = now
	s.sweepExpiredLocked(now)
	for k := range s.podCache {
		if len(s.podCache) <= podCacheLowWater {
			break
		}
		delete(s.podCache, k)
	}
}

// sweepExpiredLocked drops entries past podMetaCacheTTL — the same predicate
// cacheGet reads them by, so nothing usable is thrown away. Caller holds
// cacheMu.
func (s *Scraper) sweepExpiredLocked(now time.Time) {
	for k, e := range s.podCache {
		if now.Sub(e.fetched) >= podMetaCacheTTL {
			delete(s.podCache, k)
		}
	}
}
