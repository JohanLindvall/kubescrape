package promscrape

// The cadvisor scrape batcher: cgroup-identity routing of kubelet series
// into one OTLP resource per pod/container, enriched through the package's one
// identity path (metaresolve.go) — the TTL-cached metadata lookup the summary
// batcher, the splitters and the cgroup sampler resolve through too.

import (
	"context"
	"log/slog"
	"slices"
	"strconv"
	"strings"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/JohanLindvall/kubescrape/internal/obs"
	"github.com/JohanLindvall/kubescrape/pkg/cgroupid"
)

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

func newCadvisorBatcher(ctx context.Context, s *Scraper, scrape time.Time) *cadvisorBatcher {
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
	// identities are otherwise byte-identical (both carry the pod's uid,
	// namespace and pod with an empty container name — a sandbox's
	// container="POD" is never stored — and identityOf clears the sandbox's
	// containerID and image, so appendKey yields one key for both), so both
	// land on one metric and the redundant-label elision then removes the only
	// labels that told them apart — two data points with identical attribute
	// sets in one metric, which is one series downstream.
	sandbox bool
	// pathVouched is the CALLER's statement that the cgroup path this identity
	// was built from really names the container it carries the id of — the one
	// thing a cadvisor ROW has to prove with labels (see lookupContainerID) and
	// that internal/agent/cgroupstats proves by construction: it walks the pod
	// slices itself and collapses a supervisor scope onto the container's own
	// scope by container id (discover.go's scan.byID and repointLocked), so what
	// reaches FillContainerResource has already been disambiguated. identityOf
	// never sets it — nothing in an exposition can vouch for itself.
	pathVouched bool
}

// lookupContainerID is the container id that may be RESOLVED through the
// metadata service, as opposed to the one that is rendered onto the resource:
// empty unless the row carried evidence that the id names a container the row
// is ABOUT — its own pod attribution (namespace+pod), or a container name.
//
// This is the same positive-evidence rule isSandbox applies, against the same
// impostor and for a symmetrical reason. CRI-O's crio-conmon-<containerID>.scope
// is the SUPERVISOR's cgroup, carrying the supervised container's id in its own
// name, and pkg/cgroupid.Parse extracts that id deliberately (it says so, and
// hands the exclusion problem here). Looking it up anyway builds the impostor's
// resource out of the REAL container's metadata: a second ResourceMetrics whose
// attribute set is byte-identical to the container's, carrying the same
// cumulative container_* families with the supervisor's unrelated values — one
// Prometheus series receiving two conflicting samples per scrape, so rate()
// fabricates a counter reset on every one of them. isSandbox declining to FOLD
// such a row is not enough on its own: that only buys the row its own resource
// KEY, and this is what decides what fills it.
//
// A row that names nothing therefore keeps the identity its cgroup path gives
// it (pod uid + container id) and no more. That costs the one case where the id
// really is the row's own — a container whose CRI lookup inside cadvisor failed,
// so the labels never arrived — which loses enrichment on a resource that was
// unattributed anyway. The other direction corrupts a REAL container's
// cumulative series, and the path cannot tell the two apart: Parse's
// podContainer is true for a conmon scope by construction.
func (id cadvisorIdentity) lookupContainerID() string {
	if id.pathVouched || id.container != "" || (id.namespace != "" && id.pod != "") {
		return id.containerID
	}
	return ""
}

// keepsCgroupID reports whether the row's `id` label must survive the
// redundant-label elision in putFilteredLabels. Two shapes need it, and both
// need it for the same reason — their data points would otherwise render an
// attribute set identical to another row's on a resource they share, or that
// looks exactly like theirs:
//
//   - the SANDBOX row, which shares the POD's resource by design and is then
//     distinguished from the pod-cgroup point only by its cgroup path;
//   - any row cadvisor could not attribute to a pod, whose identity therefore
//     came from the cgroup PATH alone. Its resource carries no k8s.pod.name and
//     no container name, so an unattributable child of a pod slice would
//     otherwise be a point-for-point twin of every other one under that pod.
func (id cadvisorIdentity) keepsCgroupID() bool {
	return id.sandbox || id.namespace == "" || id.pod == ""
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
// Declining to fold is only HALF of what an impostor needs, and the other half
// is lookupContainerID: refusing the fold buys the row its own resource key,
// while the lookup is what FILLS that resource — and a conmon scope's cgroup
// path carries the supervised container's real id, so resolving it built the
// impostor a byte-identical twin of the container it supervises. The two rules
// are the same positive-evidence rule applied to the two halves of the same
// question, and neither is sufficient alone.
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
// Not about the fold, and no longer a shared resource: an unrecognised child of
// a pod slice whose name yields no container id (kata's kata_<sandbox-id>)
// parses as the POD's own cgroup, and the path alone cannot tell it from the pod
// slice — this predicate never sees it. It no longer SHARES the pod's resource,
// though: appendKey carries namespace and pod, which such a row does not have,
// so it gets its own unattributed resource instead of racing the pod-cgroup row
// to name the pod's.
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

// archSuffixes are the GOARCH values Kubernetes publishes pause images for.
var archSuffixes = []string{"amd64", "arm64", "arm", "ppc64le", "s390x", "386", "riscv64"}

// isArchPause reports the "pause-<arch>" shape only.
func isArchPause(image string) bool {
	rest, ok := strings.CutPrefix(image, "pause-")
	if !ok {
		return false
	}
	return slices.Contains(archSuffixes, rest)
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
	// The prefix arm exists for the arch-suffixed sandbox images (pause-amd64,
	// pause-arm64, ...), so it is restricted to those rather than accepting any
	// repository that merely BEGINS "pause-" — registry.k8s.io/pause-monitor is
	// a plausible workload name and was read as the sandbox image.
	return image == "pause" || isArchPause(image) || strings.HasSuffix(image, "-pause")
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
		b = appendLP(b, id.container)
		// namespace and pod participate HERE TOO, and not only in the sibling
		// arm. They are what cadvisor could not supply for an unattributable
		// child of a pod slice (kata's kata_<sandbox-id>, which parses to
		// (uid, "") exactly as the POD's own cgroup row does), so without them
		// that row shared the pod-level resource's key — and scope() fills a
		// resource from the FIRST ident it sees, so on a scrape where the helper
		// row came first the pod-level resource was built with only k8s.pod.uid:
		// no k8s.namespace.name, no k8s.pod.name and hence no service.name, i.e.
		// no Prometheus job on container_network_* and every pod-cgroup rollup
		// row, flapping with cadvisor's row order between scrapes. A row cadvisor
		// could not attribute now keeps its own resource, which is the same
		// fail-safe isSandbox's documented gaps take. It is NOT counted by
		// obs.CadvisorUnresolved: naming no pod and vouching for no container,
		// it is never looked up (see fillResource), and a counter read as "the
		// metadata service is not answering" must not move for a row nobody
		// asked about; it is named at Debug instead. Container rows are
		// unaffected either way (their container id is in the key), and on a
		// runtime that attributes every row — every containerd node — nothing
		// groups differently.
		b = appendLP(b, id.namespace)
		return appendLP(b, id.pod)
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
		// size (see otlppoint.go).
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
	// The verdict does not change what is EXPORTED — a cadvisor row ships
	// either way, with the label identity as its fallback — but it is reported,
	// because an unattributed row loses its owner chain, its pod labels and its
	// derived service.name, and until this counter existed a node whose
	// cadvisor series had lost every workload label moved nothing at all (see
	// obs.CadvisorUnresolved).
	//
	// Once per RESOURCE, never per sample: this runs from scope() only when a
	// pod or container is first seen in a chunk.
	//
	// Counted only for an identity the metadata service was (or, past a spent
	// allowance, would have been) ASKED about. A row cadvisor itself could not
	// attribute — CRI-O's crio-conmon-<id>.scope, kata's kata_<id> helper —
	// carries no namespace, pod or container label, so lookupContainerID
	// withholds its id and resolveContext issues no request at all. Counting
	// it put two increments per pod per scrape on every healthy CRI-O or kata
	// node, on a counter whose help, CONFIGURATION.md and FIRST-RUN.md all
	// read a sustained rate as "the metadata service is not answering".
	resolved, _ := cb.s.fillIdentityResource(cb.ctx, res, ident)
	switch {
	case !resolved && ident.podScoped() && ident.lookedUp():
		cb.s.reportCadvisorIdentity(ident)
	case !resolved && ident.podScoped():
		cb.s.debugUnattributable(ident)
	case ident.sandbox:
		cb.s.debugSandboxFold(ident)
	}
}

// lookedUp reports whether resolveContext asks the metadata service anything
// for this identity: a container id the row vouches for, or a pod name.
func (id cadvisorIdentity) lookedUp() bool {
	return id.lookupContainerID() != "" || id.pod != ""
}

// reportCadvisorIdentity counts a cadvisor resource the metadata service did
// not place, and — at Debug — names it. The counter is the rate; the Debug line
// is the only thing that says WHICH container, which is the whole question when
// half a node's series lose their labels and the other half keep them.
//
// Not throttled, because it is Debug and because the count per scrape is
// bounded by the node's container count: this is not a per-item path (one call
// per resource per exported chunk) and an operator who turned Debug on during
// an incident wants every one of them.
func (s *Scraper) reportCadvisorIdentity(ident cadvisorIdentity) {
	level := "pod"
	if ident.containerID != "" || ident.container != "" {
		level = "container"
	}
	obs.CadvisorUnresolved.WithLabelValues(level).Inc()
	if !s.log.Enabled(context.Background(), slog.LevelDebug) {
		return
	}
	// objectLevel, not "level": slog's own severity key IS "level", and a second
	// pair of that name on the line makes a logfmt reader resolve the record's
	// severity to "container". The line then reads as DEBUG to a human and as
	// level="container" to Loki, so a severity filter silently drops it. The
	// METRIC label stays "level" — a metric has no reserved key, and
	// METRICS.md documents it under that name.
	s.log.Debug("the metadata service did not place a cadvisor row; it is exported with the identity its own labels carried",
		"objectLevel", level, "namespace", ident.namespace, "pod", ident.pod,
		"container", ident.container, "id", ident.containerID, "uid", ident.podUID)
}

// debugSandboxFold reports, per pod per chunk, that a sandbox row was folded
// into the pod's resource. It answers the question the fold's own doc comment
// spends thirty lines on — "why does this pod have a resource carrying `pause`,
// or why does it NOT" — with the evidence for the pod actually in front of the
// operator. A row that declines the fold gets its own resource and shows up as
// an ordinary unresolved one above, so the two branches are both visible.
func (s *Scraper) debugSandboxFold(ident cadvisorIdentity) {
	if !s.log.Enabled(context.Background(), slog.LevelDebug) {
		return
	}
	s.log.Debug("folded a pod sandbox row into the pod's resource",
		"namespace", ident.namespace, "pod", ident.pod, "uid", ident.podUID)
}

// debugUnattributable names, at Debug, a pod-scoped row that was exported with
// its cgroup-path identity WITHOUT a lookup — the row named no pod and vouched
// for no container, so there was nothing to ask. The counterpart of
// reportCadvisorIdentity's line, for the rows that counter deliberately skips.
func (s *Scraper) debugUnattributable(ident cadvisorIdentity) {
	if !s.log.Enabled(context.Background(), slog.LevelDebug) {
		return
	}
	s.log.Debug("a cadvisor row names no pod or container, so it was not looked up; it is exported with its cgroup-path identity",
		"uid", ident.podUID, "id", ident.containerID)
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
	cb.putFilteredLabels(dp.Attributes(), s.Labels, podScoped, ident.keepsCgroupID())
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
	cb.putFilteredLabels(dp.Attributes(), acc.labels, podScoped, ident.keepsCgroupID())
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
	cb.putFilteredLabels(dp.Attributes(), acc.labels, podScoped, ident.keepsCgroupID())
	cb.points++
	cb.bytes += summBytes(acc)
}

func (cb *cadvisorBatcher) putFilteredLabels(attrs pcommon.Map, labels []Label, podScoped, keepID bool) {
	for _, l := range labels {
		// `id` survives the elision for the rows keepsCgroupID names: it is then
		// the one label telling their points apart from the ones they would
		// otherwise duplicate.
		keepCgroupID := keepID && l.Name == "id"
		if isIdentityLabel(l.Name) || (podScoped && redundantOnPodRow(l.Name) && !keepCgroupID) {
			continue
		}
		attrs.PutStr(l.Name, l.Value)
	}
}
