// Package otlpingest receives OTLP pushed by applications — logs and metrics on
// the node agent's -ingest listeners, traces on the trace tier's application
// ports (ServerConfig.Traces) — and enriches each resource with Kubernetes
// metadata deduced from a container ID or pod UID already present on the data
// (or, opt-in, from the connection's peer address), then hands the result to
// the shared exporter. It closes the "apps push OTLP for enrichment" gap that
// otherwise requires a separate collector with the k8sattributes processor.
//
// The sender is authoritative for the DESCRIPTIVE attributes it chose to
// declare, which enrichment never overwrites. Two exceptions, both deliberate:
// the RESOLVED-IDENTITY keys (k8s.namespace.name and its siblings), which this
// receiver just read from the API server for that resource and which overwrite
// the sender's claim (Enricher.resolvedWins — routing keys tenancy on them);
// and the split path, where a resource describes an object OTHER than the
// sender and that object's resolved identity replaces the copied sender's
// (split.go, overwriteAttrs). On the application-facing listeners the sender's
// Kubernetes identity CLAIM is stripped at receipt as well, before anything
// resolves — nothing at an unauthenticated door can verify it
// (ServerConfig.ReservedAttrs, wired from Enricher.SenderIdentityStrip).
package otlpingest

import (
	"context"
	"log/slog"
	"slices"
	"strings"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/JohanLindvall/kubescrape/internal/agent/attrs"
	"github.com/JohanLindvall/kubescrape/internal/logdedupe"
	"github.com/JohanLindvall/kubescrape/internal/obs"
	"github.com/JohanLindvall/kubescrape/pkg/kubemeta"
)

// MetadataSource resolves pod/container metadata; implemented by
// metaclient.Client.
type MetadataSource interface {
	Container(ctx context.Context, id string, wait time.Duration) (*kubemeta.ContainerMetadata, error)
	PodByUID(ctx context.Context, uid string) (*kubemeta.Pod, error)
	PodByIP(ctx context.Context, ip string) (*kubemeta.Pod, error)
}

// MetricsMode selects how metric resources are enriched.
type MetricsMode string

const (
	// MetricsResource reads the ID from the incoming resource attributes and
	// enriches the resource in place (the OTLP norm: one resource per object).
	MetricsResource MetricsMode = "resource"
	// MetricsDatapoint reads the ID from each data point's attributes and
	// splits the points into one resource per distinct object.
	MetricsDatapoint MetricsMode = "datapoint"
	// MetricsAuto enriches from the resource attributes when an ID is present
	// there, otherwise falls back to per-data-point splitting.
	MetricsAuto MetricsMode = "auto"
)

// Config configures the enricher.
type Config struct {
	// ContainerIDKeys are the attribute keys inspected for a container ID
	// (checked first — a container ID resolves the exact incarnation).
	ContainerIDKeys []string
	// PodUIDKeys are the attribute keys inspected for a pod UID.
	PodUIDKeys []string
	// Wait is how long a metadata lookup may block for not-yet-known objects
	// (0 = never block; pushed telemetry normally lags pod creation).
	Wait time.Duration
	// MetricsMode selects resource-level vs data-point enrichment.
	MetricsMode MetricsMode
	// PeerIPFallback resolves the sending pod by the connection's peer IP
	// when the resource carries no container ID or pod UID, and merges its
	// k8s attributes per mergeAttrs (the sender's descriptive attributes are
	// kept; the resolved-identity keys overwrite the sender's). Opt-in: peer
	// IPs can be rewritten by NAT, and hostNetwork senders share the node IP
	// (those never resolve — the metadata service only indexes pod-IP-owning
	// pods).
	//
	// It is only ever correct at FIRST RECEIPT. The peer address names the
	// process at the other end of THIS connection, so it means the sender
	// exactly once: on the hop the sender itself opened. Any relay of the same
	// payload — an internal re-shard hop, a proxy, a mesh sidecar that
	// terminates — presents its own address, and attributing an application's
	// telemetry to a relay's pod is silent, plausible-looking, wrong data. That
	// is why the enricher runs on the tier's application-facing listener and on
	// nothing else, and why PeerReject exists as the backstop.
	PeerIPFallback bool
	// PeerReject vetoes a peer-IP attribution after the lookup resolves. It is
	// the explicit failure mode for an address that was rewritten in flight: the
	// receiver knows which pods are its OWN workload's, and a connection whose
	// source is one of those did not come from an application. Rejecting is
	// counted (kubescrape_ingest_resources_total{outcome="peer_ip_rejected"}) and
	// leaves the resource unenriched, which is a visible gap rather than a
	// confident lie.
	//
	// nil accepts every resolution (the node-local case, where the peer is a pod
	// on this node by construction).
	PeerReject func(pod *kubemeta.Pod) bool
	// Attrs builds the k8s resource attributes (nil = built-in defaults).
	Attrs *attrs.Builder
	// NodeInfo supplies the agent node's metadata for attribute templates.
	NodeInfo func() *attrs.NodeInfo

	Meta   MetadataSource
	Logger *slog.Logger
}

// DefaultContainerIDKeys and DefaultPodUIDKeys are the resource-attribute
// keys inspected when Config leaves ContainerIDKeys/PodUIDKeys unset. Exported
// so the agent's flag defaults (-ingest-container-id-keys /
// -ingest-pod-uid-keys) are BUILT from them rather than restating them — the
// two spellings had nothing keeping them equal. Treat as immutable.
var (
	DefaultContainerIDKeys = []string{"container.id", "k8s.container.id"}
	DefaultPodUIDKeys      = []string{"k8s.pod.uid"}
)

func (c Config) containerIDKeys() []string {
	if len(c.ContainerIDKeys) > 0 {
		return c.ContainerIDKeys
	}
	return DefaultContainerIDKeys
}

func (c Config) podUIDKeys() []string {
	if len(c.PodUIDKeys) > 0 {
		return c.PodUIDKeys
	}
	return DefaultPodUIDKeys
}

func (c Config) metricsMode() MetricsMode {
	if c.MetricsMode == "" {
		return MetricsAuto
	}
	return c.MetricsMode
}

// Enricher attributes pushed telemetry.
type Enricher struct {
	cfg             Config
	containerIDKeys []string
	podUIDKeys      []string
	mode            MetricsMode
	log             *slog.Logger

	// peerWarnGate/lookupWarnGate/waitWarnGate throttle their warnings; the
	// enricher is shared by concurrent handlers, which is the throttles'
	// contract.
	peerWarnGate   logdedupe.Throttle
	lookupWarnGate logdedupe.Throttle
	waitWarnGate   logdedupe.Throttle
	// metaWarnGate throttles the metadata-service-is-broken warning. Keyless:
	// there is one metadata service and one condition, and every id in every
	// push meets it at once.
	metaWarnGate logdedupe.Throttle
	// splitCapWarnGate throttles the splitter's degradation line. Keyed by
	// which bound bound (groups vs copied bytes), so the one that fires
	// constantly cannot hide the other.
	splitCapWarnGate *logdedupe.Table
}

// NewEnricher creates an Enricher.
func NewEnricher(cfg Config) *Enricher {
	log := cfg.Logger
	if log == nil {
		log = slog.Default()
	}
	return &Enricher{
		cfg:             cfg,
		containerIDKeys: cfg.containerIDKeys(),
		podUIDKeys:      cfg.podUIDKeys(),
		mode:            cfg.metricsMode(),
		log:             log,
		// Two keys: the group cap and the copied-bytes cap (noteSplitCapped).
		splitCapWarnGate: logdedupe.New(2, lookupBudgetWarnEvery),
	}
}

// EnrichLogs enriches every resource in ld in place, merging per mergeAttrs
// (the resolved-identity keys overwrite the sender's; everything else the
// sender set is kept). It is resource-only, like EnrichTraces: the per-record
// half of the log path — scrub, lift, line enrichment, log-metrics, rules —
// is the server's applyLogChain (ServerConfig.Scrub, EnrichLines and the
// rest), which runs right after this. Resource enrichment reads no body, so
// scrubbing there rather than here changes nothing it could see.
func (e *Enricher) EnrichLogs(ctx context.Context, ld plog.Logs) {
	// One lookup + attribute build per distinct ID across the request.
	cache := newReqCache()
	rls := ld.ResourceLogs()
	for i := 0; i < rls.Len(); i++ {
		e.enrichAttrs(ctx, rls.At(i).Resource().Attributes(), cache)
	}
}

// usesPeerIP reports whether this enricher ever reads the connection's peer
// address — its opt-in peer-IP fallback (Config.PeerIPFallback), the only
// reader — so the transports can skip stamping it (Server.stampPeer). Safe on
// a nil receiver.
func (e *Enricher) usesPeerIP() bool { return e != nil && e.cfg.PeerIPFallback }

// EnrichTraces enriches every resource in td in place (traces are otherwise a
// passthrough signal).
func (e *Enricher) EnrichTraces(ctx context.Context, td ptrace.Traces) {
	cache := newReqCache()
	rss := td.ResourceSpans()
	for i := 0; i < rss.Len(); i++ {
		e.enrichAttrs(ctx, rss.At(i).Resource().Attributes(), cache)
	}
}

// EnrichMetrics enriches md according to the configured mode, returning the
// (possibly regrouped) metrics to export. Export the RETURNED value: when the
// push is regrouped (datapoint mode, or auto demoted to it) md is consumed —
// its data points are moved into the result, not copied (splitAndEnrich).
func (e *Enricher) EnrichMetrics(ctx context.Context, md pmetric.Metrics) pmetric.Metrics {
	switch e.mode {
	case MetricsDatapoint:
		return e.splitAndEnrich(ctx, newReqCache(), md)
	case MetricsResource:
		e.enrichMetricResources(ctx, newReqCache(), md)
		return md
	default: // auto
		// One cache for the decision AND the enrichment that follows — on BOTH
		// branches, so the resolvability probes the decision makes are not paid
		// for twice (a demoted payload's groupers reuse them) and the lookup
		// budget spans the whole push rather than re-arming at the demotion.
		cache := newReqCache()
		if e.resourceModeSuffices(ctx, cache, md) {
			e.enrichMetricResources(ctx, cache, md)
			return md
		}
		return e.splitAndEnrich(ctx, cache, md)
	}
}

// enrichMetricResources enriches each ResourceMetrics from its own resource
// attributes, against the caller's request cache (the auto-mode decision's, so
// its lookups are reused, or a fresh one).
func (e *Enricher) enrichMetricResources(ctx context.Context, cache *reqCache, md pmetric.Metrics) {
	rms := md.ResourceMetrics()
	for i := 0; i < rms.Len(); i++ {
		e.enrichAttrs(ctx, rms.At(i).Resource().Attributes(), cache)
	}
}

// enrichAttrs resolves the id on a resource's attributes a and merges the k8s
// attributes it maps to per mergeAttrs: the resolved-identity keys overwrite
// the sender's claim, every other attribute the sender set is kept. The token
// comes from resolvableToken — the one chooser the auto-mode decision and the
// split path use too, so one payload is attributed by the same id whatever the
// mode — and a resource carrying no id falls back to the connection's peer.
//
// The outcome is counted exactly ONCE per call, i.e. per RESOURCE:
// kubescrape_ingest_resources_total counts resources, so a resource carrying
// two unresolvable ids must not tally two (attrsFor, which resolvableToken
// probes the container id through, counts nothing). enriched keys on RESOLVED,
// not on a non-empty rendering: the operator's attribute filter can empty the
// build for an object the lookup found, and that must not read as unresolved.
// cache memoises the lookups per token for the duration of one request.
func (e *Enricher) enrichAttrs(ctx context.Context, a pcommon.Map, cache *reqCache) {
	tok := e.resolvableToken(ctx, cache, a)
	if tok == "" {
		// Resolved by the connection, not by an attribute: no key on a is the
		// lookup input, so none is exempt from the overwrite.
		if built, resolved := e.peerFallback(ctx, cache); resolved {
			e.mergeAttrs(built, a, nil)
		}
		return
	}
	r := e.attrsFor(ctx, cache, tok)
	if !r.resolved {
		obs.Ingested.WithLabelValues("unresolved").Inc()
		return
	}
	obs.Ingested.WithLabelValues("enriched").Inc()
	e.mergeAttrs(r.built, a, e.lookupKeysOf(tok))
}

// valueUnder returns the first non-empty string value under keys.
func valueUnder(a pcommon.Map, keys []string) (string, bool) {
	for _, k := range keys {
		if v, ok := a.Get(k); ok && v.Str() != "" {
			return v.Str(), true
		}
	}
	return "", false
}

// idValue returns the first id in a, container keys first, in PARTS: the kind
// prefix and the raw id value, with no token built. The auto-mode decision
// visits every data point of a push, and building the concatenated token there
// was one heap allocation each — the largest non-pdata allocator on the default
// mode's default path. It does not decide BETWEEN the two kinds (that needs a
// lookup: resolvableToken); the point walk only asks whether a point names an
// id, and which one it names first.
func (e *Enricher) idValue(a pcommon.Map) (prefix, val string, ok bool) {
	if v, ok := valueUnder(a, e.containerIDKeys); ok {
		return tokContainer, v, true
	}
	if v, ok := valueUnder(a, e.podUIDKeys); ok {
		return tokPodUID, v, true
	}
	return "", "", false
}

// hasID reports whether a carries a container id or pod uid.
func (e *Enricher) hasID(a pcommon.Map) bool {
	_, _, ok := e.idValue(a)
	return ok
}

// resolvableToken picks the id token to attribute a resource (or data point)
// by, preferring the container id — it names the exact incarnation — but
// falling back to the pod uid when the container id does not resolve. A stale
// container id (the container restarted, or its tombstone expired) must not
// veto a pod uid the sender also supplied. The lookup only runs when BOTH kinds
// are present, so the common single-id case costs nothing extra.
//
// It is the ONE token chooser: resource enrichment (enrichAttrs), the auto-mode
// decision's resource id (resourceModeSuffices) and the split path's per-point
// and resource-level ids all call it, because each used to choose its own way
// and the same payload was attributed differently by mode. The last of those
// splits was the LOOKUP: resource enrichment asked the waited attribution
// lookup whether the container id resolved while the split path asked a
// wait-free probe, so with -ingest-metadata-wait set, a container id the kubelet
// had not yet posted named the container in resource/auto mode and only the pod
// in datapoint mode (TestTokenChoiceIsModeIndependent). The question is now
// always attrsFor's: memoised per request, clamped by the per-push wait budget,
// and the very lookup the chosen container token is then attributed with, so it
// costs nothing extra when the container id resolves.
//
// It runs per DATA POINT on the split path, so its tokens come from the
// request's interner (reqCache.token) rather than a concatenation, and the
// pod-uid token is not built at all when the container id resolves.
func (e *Enricher) resolvableToken(ctx context.Context, cache *reqCache, a pcommon.Map) string {
	cVal, cOK := valueUnder(a, e.containerIDKeys)
	uVal, uOK := valueUnder(a, e.podUIDKeys)
	switch {
	case cOK && uOK:
		if cTok := cache.token(tokContainer, cVal); e.attrsFor(ctx, cache, cTok).resolved {
			return cTok
		}
		return cache.token(tokPodUID, uVal)
	case cOK:
		return cache.token(tokContainer, cVal)
	case uOK:
		return cache.token(tokPodUID, uVal)
	}
	return ""
}

// buildFor renders the configured k8s resource attributes for a resolved pod
// (and, when the ID named one, its exact container) — the one build shared by
// the token path (attrsFor) and the peer-IP path (peerAttrs).
//
// It returns the scratch resource's OWN attribute map rather than a copy of it:
// the resource is local and never escapes any other way, and a built map is
// READ-ONLY by contract (see emptyAttrs — every consumer Ranges over it and
// writes to its own destination). The copy doubled the allocations of every
// resolved attribution for nothing (TestBuildForDoesNotCopyTheMapItBuilt).
func (e *Enricher) buildFor(pod *kubemeta.Pod, container *kubemeta.Container) pcommon.Map {
	r := pcommon.NewResource()
	actx := attrs.Context{Pod: pod, Container: container}
	if e.cfg.NodeInfo != nil {
		actx.Node = e.cfg.NodeInfo()
	}
	e.cfg.Attrs.Build(r, actx)
	return r.Attributes()
}

// emptyAttrs is the shared "nothing was built" attribute map. Every consumer of
// a built map only READS it (mergeAttrs/overwriteAttrs Range over the source) —
// the contract that also lets buildFor hand out the map it built — so the
// not-applicable answers need no map of their own — and they are the
// common ones: an unresolved token per distinct id, and the peer fallback on
// every id-less resource of every push while the fallback is off, which is the
// default.
var emptyAttrs = pcommon.NewMap()

// mergeAttrs adds src's attributes to dst. The sender stays authoritative about
// ITSELF — attrs.FillAbsent's rule, which the self-metadata stamping still
// applies verbatim — with ONE class of exception this receiver cannot share:
// the RESOLVED-IDENTITY keys (attrs.ReservedIdentity), which it just derived
// from the API server for this very resource, overwrite whatever the sender
// claimed.
//
// The exception is a security boundary, not tidiness, and it is the same one
// attrs.ReservedIdentity documents for the pod-annotation surface.
// internal/agent/route keys TENANCY on k8s.namespace.name: a plain
// FillAbsent left a sender's own value in place, so a pod in namespace
// `attacker` could push `k8s.namespace.name: payments` beside its real
// container.id and have its records exported to the payments route, under that
// route's endpoint and X-Scope-OrgID — while the SAME resource carried
// `service.namespace: default`, this receiver's own resolution of the same
// fact, written under a different key. One resource, two answers, and the one
// the router read was the sender's. The sibling keys forge series identity in
// the backend and on every log-derived metric bound to the resource.
//
// The OTLP service triple is deliberately NOT in that set: service.name is
// descriptive, and a sender renaming its own series is what the key is for,
// while service.namespace and service.instance.id are the two dimensions
// beside it — see senderControlledIdentity, whose exemption resolvedWins
// applies here so the receipt strip and this overwrite agree about what a
// sender owns.
//
// The residual, stated: this can only correct a resource the lookup RESOLVED,
// and it can only correct the keys the builder actually emitted for it (an
// operator who filters k8s.namespace.name out of the ingest pipeline's
// resourceAttributes leaves the sender's copy as the only one there is). For an
// UNRESOLVABLE resource the sender's declaration is the only namespace that
// exists, which is why the application-facing listeners also strip these keys
// at receipt — see Enricher.SenderIdentityStrip.
//
// by is the lookup-key set the resolution was made BY (e.containerIDKeys or
// e.podUIDKeys, per the kind of token that resolved; nil when the attribution
// came from the connection's peer address) — see resolvedWins for why only
// that kind is exempt.
//
// attrs.Merge does the writing, linear in both sides: a per-key Put loop is
// quadratic, and here BOTH are tenant-authored (the resolved pod's label
// count, the sender's resource width), merged once per resource of every push.
func (e *Enricher) mergeAttrs(src, dst pcommon.Map, by []string) {
	attrs.Merge(src, dst, func(k string) bool { return e.resolvedWins(k, by) })
}

// lookupKeysOf is the configured attribute-key set a kind-tagged token's value
// is read from: the pod-uid keys for a pod-uid token, the container-id keys
// otherwise.
func (e *Enricher) lookupKeysOf(token string) []string {
	if strings.HasPrefix(token, tokPodUID) {
		return e.podUIDKeys
	}
	return e.containerIDKeys
}

// resolvedWins reports whether THIS receiver's resolution of a key outranks
// the sender's claim about it.
//
// Reserved-identity keys yes (see mergeAttrs) — with TWO exceptions. The
// receipt strip is this very predicate (Enricher.SenderIdentityStrip filters
// attrs.ReservedIdentityKeys() through it), so the strip and the merge cannot
// disagree about what a sender owns: if they did, the exemption would hold only
// on the path it is least needed and evaporate on the common one. The one input
// they legitimately differ in is the lookup input's WIDTH, below: the strip
// runs before anything resolves and passes BOTH kinds as `by` (stripping a
// lookup key there would leave enrichment nothing to resolve by), while the
// merge passes the kind that resolved.
//
// The LOOKUP INPUT — `by`, the key set of the KIND the resolution was made BY
// (container.id or k8s.pod.uid, per-Enricher configuration via
// -ingest-container-id-keys / -ingest-pod-uid-keys — which is why this hangs
// off the Enricher rather than being a package function): the answer is a
// function of the value the sender wrote there, so there is no independent
// truth to correct it with, and rewriting it into this agent's own spelling (a
// raw `cafe01` becoming `containerd://cafe01`) would silently change a join key
// the sender's other telemetry uses. The OTHER kind is not exempt, because that
// premise is false for it: when container.id resolved, a sender's k8s.pod.uid
// is a claim this receiver can check against the pod it just read, and leaving
// a non-matching one in place shipped one resource naming two pods (the
// resolved k8s.pod.name/namespace beside a foreign uid). An honest sender's
// matching uid is overwritten with the same value. The reverse direction
// changes nothing today — a pod-uid resolution builds no container.id — and a
// peer-IP attribution (by == nil) exempts nothing, since no attribute was its
// input.
//
// senderControlledIdentity (service.namespace / service.instance.id): the
// sender names ITSELF to the backend with the OTLP service triple, and
// service.name — the half that picks which workload's series a sample joins —
// is sender-controlled by design. Neither of the other two steers anything in
// kubescrape (routing keys on k8s.namespace.name), so there is nothing here to
// "correct"; taking them costs a resolved sender its declared job+instance
// pair, renaming its job to <k8s-namespace>/<service.name> and pinning its
// instance to the container ID, which changes on every restart and mints a
// fresh series each time. The k8s.*/container.* siblings are different in kind
// and still lose: those are facts about the cluster this receiver just read
// from the API server, and one of them is what route keys tenancy on.
//
// The DESCRIBING arm deliberately differs: on the datapoint/split path a
// resource names an object OTHER than the sender, so the sender's service
// triple is its own and not the described object's — split.go strips the whole
// of attrs.SenderIdentityKeys() (service.name included) before overwriteAttrs
// rather than merging. "The sender is authoritative about itself" is the same
// rule in both places; only the question of whose resource it is changes.
func (e *Enricher) resolvedWins(k string, by []string) bool {
	if !attrs.ReservedIdentity(k) {
		return false
	}
	if slices.Contains(senderControlledIdentity, k) {
		return false
	}
	return !slices.Contains(by, k)
}

// overwriteAttrs sets src's attributes on dst, replacing what the sender set.
// Used only where the resource describes an object OTHER than the sender (the
// datapoint-split path): there the sender's identity attributes name itself,
// not the object, so they are not authoritative. Keys absent from src are left
// alone. attrs.Merge for mergeAttrs' reason: linear in both sides.
func overwriteAttrs(src, dst pcommon.Map) { attrs.Merge(src, dst, replaceAll) }

// replaceAll is overwriteAttrs' replace predicate: a package func, so passing
// it costs no closure.
func replaceAll(string) bool { return true }

// peerRejectWarnEvery throttles the rejected-peer warning. A relay in front of
// the listener rewrites EVERY connection, so the condition is either absent or
// universal; one line per push would bury the diagnosis in its own symptom.
const peerRejectWarnEvery = time.Minute

// peerAttrs returns the k8s attributes of the pod owning the connection's peer
// IP, resolved at most ONCE per request, and reports whether the lookup
// RESOLVED and whether the resolution was REJECTED (see Config.PeerReject) as
// opposed to simply not resolving. resolved comes from the lookup, not from
// the rendering: the operator's attribute filter can empty the build for a pod
// the lookup found.
//
// The peer is a property of the connection, so every resource in a payload has
// the same one — yet this ran per resource, and /v1/pod-ips is deliberately
// uncacheable (recycled IPs need immediacy), so 500 ID-less resources meant 500
// serial round trips inside the handler. Worse, two resources of one payload
// could be attributed differently if the index changed mid-request. The cache
// is per request (the enricher itself is shared by concurrent handlers, so
// nothing may be memoised on it).
func (e *Enricher) peerAttrs(ctx context.Context, cache *reqCache) (built pcommon.Map, resolved, rejected bool) {
	// The memo covers the NOT-APPLICABLE answers too (the fallback is off, or
	// the transport recorded no address): every id-less resource of a push asks
	// again, and a fresh empty map per ask was two allocations each on the
	// default configuration, where the feature is disabled.
	if cache.peerDone {
		return cache.peer, cache.peerResolved, cache.peerRejected
	}
	var ip string
	if e.cfg.PeerIPFallback {
		ip = peerIP(ctx)
	}
	if ip == "" {
		cache.peer, cache.peerDone = emptyAttrs, true
		return emptyAttrs, false, false
	}
	built = emptyAttrs
	pod, err := e.cfg.Meta.PodByIP(ctx, ip)
	if err != nil {
		// Reported exactly as lookupByID reports its own: every id-less sender
		// takes this path while the fallback is on, so a metadata service that
		// is not answering would otherwise read only as outcome=unresolved —
		// the same thing a hostNetwork peer's ordinary 404 says, and the blind
		// spot noteLookupFailed exists to close. It skips 404s itself and is
		// throttled, and this runs at most once per request (the memo below).
		e.log.Debug("ingest: pod-ip lookup failed", "peer", ip, "error", err)
		e.noteLookupFailed(err)
	} else if pod != nil {
		if e.cfg.PeerReject != nil && e.cfg.PeerReject(pod) {
			// The address did not come from an application: something between
			// the sender and this listener replaced it, and the pod it now names
			// is one of ours. Attributing the sender's telemetry to that pod
			// would be wrong in the worst way — confident, plausible, and
			// wrong on every resource — so nothing is merged.
			rejected = true
			// NOT counted here: peerAttrs memoises per REQUEST, and every
			// sibling outcome (enriched, unresolved, peer_ip) is counted per
			// RESOURCE. Counting it here made one label of one metric mean a
			// different denominator from the rest, so the ratios an operator
			// builds from them were wrong on any multi-resource push. The
			// caller tallies it; the warn stays here, where its once-per-request
			// throttle is what is wanted.
			e.warnPeerRejected(ip, pod)
		} else {
			resolved = true
			built = e.buildFor(pod, nil)
		}
	}
	cache.peer, cache.peerResolved, cache.peerRejected, cache.peerDone = built, resolved, rejected, true
	return built, resolved, rejected
}

// peerFallback is peerAttrs plus the outcome accounting: peer_ip when the
// attribution lands (the caller merges the returned attributes), peer_ip_rejected
// when Config.PeerReject vetoed it, unresolved otherwise — counted per call,
// i.e. per RESOURCE on the resource path and per ""-group on the split path,
// like every other outcome of kubescrape_ingest_resources_total.
//
// ONE helper for the two sites because they had drifted: the resource path
// (enrichAttrs) counted all three outcomes while the splitter's ""-group
// counted peer_ip and unresolved but NOTHING for a rejected peer — behind a
// comment claiming the rejection "has already been counted", which no site did
// — so -ingest-metrics-mode=datapoint (and an auto push demoted to split)
// under-reported the exact counter Config.PeerReject's doc promises.
func (e *Enricher) peerFallback(ctx context.Context, cache *reqCache) (pcommon.Map, bool) {
	built, resolved, rejected := e.peerAttrs(ctx, cache)
	switch {
	case resolved:
		obs.Ingested.WithLabelValues("peer_ip").Inc()
	case rejected:
		obs.Ingested.WithLabelValues("peer_ip_rejected").Inc()
	default:
		obs.Ingested.WithLabelValues("unresolved").Inc()
	}
	return built, resolved
}

func (e *Enricher) warnPeerRejected(ip string, pod *kubemeta.Pod) {
	if !e.peerWarnGate.Allow(peerRejectWarnEvery) {
		return
	}
	e.log.Warn("refusing to attribute pushed telemetry by peer IP: the connection's source address belongs to this receiver's own workload, so it was rewritten in flight (a proxy, a mesh sidecar, or an internal hop addressed to the application port). Those resources stay unenriched; give senders a resource-level container.id or k8s.pod.uid, or make the path preserve the client address",
		"peer", ip, "namespace", pod.Namespace, "pod", pod.Name)
}

// sameObject reports whether two kind-tagged ID tokens name the same
// Kubernetes object for ATTRIBUTION. It is the one predicate behind both the
// auto-mode foreign-point decision (foreignID) and the split path's
// merge-vs-overwrite choice (metricGrouper.resource).
//
// It existed TWICE and the copies DISAGREED: the auto-mode one matched on pod
// UID alone, the split path's required pod UID AND an equal
// k8s.container.name. So for container A's ID on the resource and container
// B's ID on the data points — two containers of ONE pod, a pod-internal
// exporter describing its co-container — auto mode said "not foreign", took
// the resource branch, and stamped container A's identity on container B's
// points, while explicit datapoint mode split them correctly. The identical
// payload, attributed differently by mode: the exact bug class
// resolvableToken's comment records as fixed for the token-choice half.
//
// The rules, and why each is what it is:
//
//   - Both tokens must RESOLVE. An unresolved side is no evidence they match
//     (and no evidence they differ — foreignID handles that asymmetry).
//   - Same pod UID. Tokens are KIND-TAGGED, so container.id and k8s.pod.uid
//     naming one pod differ as strings; comparing the RESOLVED object is the
//     point of this predicate.
//   - Two DIFFERENT container names are two objects. Container B's series
//     must not carry container A's identity, however much pod they share.
//   - A pod-level token beside a container-level one is the SAME object at
//     pod grain: a sender that identifies its resource by container.id (every
//     SDK container detector) and labels points with its own k8s.pod.uid is
//     describing ITSELF, and calling that foreign demoted it to the split
//     path, where the overwrite destroyed the service.name it chose
//     (TestAutoModeKeepsTheSendersOwnIdentity pins it).
//
// The identity compared is the LOOKUP RESULT's (idResult.podUID/container),
// never the built attribute rendering: the operator's enable/disable filter
// (or defaults: false) can remove k8s.pod.uid from what the builder emits, and
// reading its absence as "different objects" made a rendering config demote a
// sender describing itself to the split path — the exact misattribution
// TestAutoModeKeepsTheSendersOwnIdentity pins for the default config.
//
// Both lookups are memoised per request through the same cache the enrichment
// uses (attrsFor), so a KSM-shaped payload costs no extra round trips — and
// the resource branch that usually follows reuses the built attributes.
func (e *Enricher) sameObject(ctx context.Context, cache *reqCache, a, b string) bool {
	if a == "" || b == "" {
		return false
	}
	if a == b {
		return true
	}
	return sameResolved(e.attrsFor(ctx, cache, a), e.attrsFor(ctx, cache, b))
}

// sameResolved is sameObject's rule applied to two lookup OUTCOMES, for callers
// that already hold one (foreignPointID reads its side out of the memo rather
// than re-keying it with a materialised token).
func sameResolved(ra, rb idResult) bool {
	if !ra.resolved || !rb.resolved {
		return false // at least one did not resolve: no evidence they match
	}
	if ra.podUID == "" || rb.podUID == "" || ra.podUID != rb.podUID {
		return false
	}
	return ra.container == rb.container || ra.container == "" || rb.container == ""
}
