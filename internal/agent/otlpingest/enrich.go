// Package otlpingest receives OTLP logs and metrics pushed by applications on
// the node and enriches each resource with Kubernetes metadata deduced from a
// container ID or pod UID already present on the data, then hands the result
// to the shared exporter. It closes the "apps push OTLP for enrichment" gap
// that otherwise requires a separate collector with the k8sattributes
// processor.
//
// Enrichment never overwrites an attribute the sender already set: the sender
// is authoritative for anything it chose to declare.
package otlpingest

import (
	"context"
	"log/slog"
	"strings"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/JohanLindvall/kubescrape/internal/agent/attrs"
	"github.com/JohanLindvall/kubescrape/internal/agent/logenrich"
	"github.com/JohanLindvall/kubescrape/internal/agent/logscrub"
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
	// EnrichLines parses each pushed log record's body for a timestamp,
	// severity, trace/span IDs and structured fields (as -logs-enrich does),
	// filling only fields the sender left unset.
	EnrichLines bool
	// Scrub redacts sensitive values from pushed log bodies before enrichment
	// copies from them (nil disables).
	Scrub *logscrub.Scrubber
	// PeerIPFallback resolves the sending pod by the connection's peer IP
	// when the resource carries no container ID or pod UID, and merges its
	// k8s attributes (never overwriting sender values). Opt-in: peer IPs can
	// be rewritten by NAT, and hostNetwork senders share the node IP (those
	// never resolve — the metadata service only indexes pod-IP-owning pods).
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

func (c Config) containerIDKeys() []string {
	if len(c.ContainerIDKeys) > 0 {
		return c.ContainerIDKeys
	}
	return []string{"container.id", "k8s.container.id"}
}

func (c Config) podUIDKeys() []string {
	if len(c.PodUIDKeys) > 0 {
		return c.PodUIDKeys
	}
	return []string{"k8s.pod.uid"}
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

	// peerWarnGate throttles the rejected-peer warning; the enricher is shared
	// by concurrent handlers, which is the throttle's contract.
	peerWarnGate logdedupe.Throttle
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
	}
}

// EnrichLogs enriches every resource in ld in place. When line enrichment is
// enabled, each record's body is additionally parsed for a timestamp,
// severity, trace/span IDs and structured fields (as the tailer does),
// without overwriting values the sender already set.
func (e *Enricher) EnrichLogs(ctx context.Context, ld plog.Logs) {
	// One lookup + attribute build per distinct ID across the request.
	cache := map[string]pcommon.Map{}
	rls := ld.ResourceLogs()
	for i := 0; i < rls.Len(); i++ {
		rl := rls.At(i)
		e.enrichResource(ctx, rl.Resource(), cache)
		if !e.cfg.EnrichLines && e.cfg.Scrub == nil {
			continue
		}
		sls := rl.ScopeLogs()
		for j := 0; j < sls.Len(); j++ {
			lrs := sls.At(j).LogRecords()
			for k := 0; k < lrs.Len(); k++ {
				lr := lrs.At(k)
				if e.cfg.Scrub != nil {
					// Scrub BEFORE ApplyBody: enrich copies body slices
					// (exception attributes) that must not carry secrets.
					e.scrubBody(lr.Body(), 0)
				}
				if e.cfg.EnrichLines {
					logenrich.ApplyBody(lr)
				}
			}
		}
	}
}

// maxBodyScrubDepth bounds the walk over a structured body. Bodies come from
// unauthenticated senders, so the recursion needs a ceiling; real structured
// logs nest a handful of levels at most.
const maxBodyScrubDepth = 8

// scrubBody redacts every string leaf of a log body, whatever shape it has.
//
// The OTel logging SDKs and the collector's json_parser/transform emit
// STRUCTURED bodies — a map or a slice — for exactly the records most likely to
// carry credentials as a field. Scrubbing only ValueTypeStr meant the same
// message redacted on the tailer path (where it is a raw line) and shipped in
// clear when an SDK sent it as a kvlist, with nothing counted and the choice
// invisible to the operator.
func (e *Enricher) scrubBody(v pcommon.Value, depth int) { e.scrubValue("", v, depth) }

// scrubValue redacts v, using key for context when v is a map entry.
//
// The key matters: the patterns are written for LINES, where a secret appears
// as `password=hunter2`. Split across a map entry the value alone is an opaque
// string no pattern can judge, so a keyed entry is probed as "key=value" and
// only the value replaced. Self-contained secrets (bearer tokens, AWS keys, PEM
// blocks) still match the value on its own, which is tried first.
func (e *Enricher) scrubValue(key string, v pcommon.Value, depth int) {
	if depth > maxBodyScrubDepth {
		return
	}
	switch v.Type() {
	case pcommon.ValueTypeStr:
		if scrubbed := e.cfg.Scrub.Scrub(v.Str()); scrubbed != v.Str() {
			v.SetStr(scrubbed)
			return
		}
		if key == "" {
			return
		}
		probe := key + "=" + v.Str()
		scrubbed := e.cfg.Scrub.Scrub(probe)
		if scrubbed == probe {
			return
		}
		// Take the tail after the key we prefixed — NOT after the first '='
		// anywhere, which for a key like "auth=token" yielded a value of
		// "token=[REDACTED]". And when the pattern consumed the key too (a
		// user rule whose replacement carries no '=', which the default
		// [REDACTED] does not), fall back to redacting the whole value: the
		// old code left it UNTOUCHED while Scrub had already counted a
		// redaction, so the metric reported a redaction that never happened
		// and the secret shipped in clear.
		if tail, ok := strings.CutPrefix(scrubbed, key+"="); ok {
			v.SetStr(tail)
			return
		}
		v.SetStr(scrubbed)
	case pcommon.ValueTypeMap:
		m := v.Map()
		m.Range(func(k string, mv pcommon.Value) bool {
			e.scrubValue(k, mv, depth+1)
			return true
		})
	case pcommon.ValueTypeSlice:
		sl := v.Slice()
		for i := 0; i < sl.Len(); i++ {
			// A slice element has no key of its own; it inherits the key of the
			// entry holding the slice ("args": ["api_key=sk-1"]).
			e.scrubValue(key, sl.At(i), depth+1)
		}
	}
}

// EnrichTraces enriches every resource in td in place (traces are otherwise a
// passthrough signal).
func (e *Enricher) EnrichTraces(ctx context.Context, td ptrace.Traces) {
	cache := map[string]pcommon.Map{}
	rss := td.ResourceSpans()
	for i := 0; i < rss.Len(); i++ {
		e.enrichResource(ctx, rss.At(i).Resource(), cache)
	}
}

// EnrichMetrics enriches md according to the configured mode, returning the
// (possibly regrouped) metrics to export.
func (e *Enricher) EnrichMetrics(ctx context.Context, md pmetric.Metrics) pmetric.Metrics {
	switch e.mode {
	case MetricsDatapoint:
		return e.splitAndEnrich(ctx, md)
	case MetricsResource:
		e.enrichMetricResources(ctx, md)
		return md
	default: // auto
		// One cache for the decision AND the enrichment that follows, so the
		// resolvability probes the decision makes are not paid for twice.
		cache := map[string]pcommon.Map{}
		if e.resourceModeSuffices(ctx, cache, md) {
			e.enrichMetricResourcesWith(ctx, cache, md)
			return md
		}
		return e.splitAndEnrich(ctx, md)
	}
}

// enrichMetricResources enriches each ResourceMetrics from its own resource
// attributes.
func (e *Enricher) enrichMetricResources(ctx context.Context, md pmetric.Metrics) {
	e.enrichMetricResourcesWith(ctx, map[string]pcommon.Map{}, md)
}

// enrichMetricResourcesWith is enrichMetricResources against a caller-supplied
// cache, so the auto-mode decision's lookups are reused.
func (e *Enricher) enrichMetricResourcesWith(ctx context.Context, cache map[string]pcommon.Map, md pmetric.Metrics) {
	rms := md.ResourceMetrics()
	for i := 0; i < rms.Len(); i++ {
		e.enrichResource(ctx, rms.At(i).Resource(), cache)
	}
}

// resourceModeSuffices reports whether enriching each ResourceMetrics from its
// own resource attributes attributes everything correctly — i.e. every resource
// carries an ID and NO data point carries one of its own.
//
// The data-point half is not optional. A resource-level container.id is set
// automatically by every SDK container detector (Go's resource.WithContainerID,
// Java's ContainerResource, the collector's resourcedetection/container), and it
// is in the default -ingest-container-id-keys. An exporter that DESCRIBES other
// objects — the kube-state-metrics shape this mode exists for — therefore has a
// resource ID naming ITSELF while each data point names a different pod. Asking
// only about resources sent that straight down the resource branch and stamped
// every point with the exporter's own pod and service.name, silently, with
// kubescrape_ingest_resources_total{enriched} reading healthy. The same payload
// in explicit datapoint mode split correctly, which is what
// TestSplitResourceUsesDescribedObjectIdentity pins.
func (e *Enricher) resourceModeSuffices(ctx context.Context, cache map[string]pcommon.Map, md pmetric.Metrics) bool {
	rms := md.ResourceMetrics()
	for i := 0; i < rms.Len(); i++ {
		rm := rms.At(i)
		resID, ok := e.findID(rm.Resource().Attributes())
		if !ok {
			return false
		}
		// FOREIGN, not merely present. A point ID equal to the resource's own
		// describes the sender itself — an app labelling its metrics with its
		// container id, which SDK metric views do — and the resource branch
		// attributes it identically while leaving the sender authoritative
		// about itself. Demoting it to the split path instead regrouped its
		// points and OVERWROTE its service.name/k8s.* with the derived ones
		// (overwriteAttrs, correct only for a describing exporter), so an
		// ordinary sender silently changed job identity by adding a label.
		if e.anyForeignDataPointID(ctx, cache, rm, resID) {
			return false
		}
	}
	return true
}

// anyForeignDataPointID reports whether any data point in rm carries an ID
// attribute naming a DIFFERENT object than resID (one pass, first hit wins).
func (e *Enricher) anyForeignDataPointID(ctx context.Context, cache map[string]pcommon.Map, rm pmetric.ResourceMetrics, resID string) bool {
	sms := rm.ScopeMetrics()
	for i := 0; i < sms.Len(); i++ {
		ms := sms.At(i).Metrics()
		for j := 0; j < ms.Len(); j++ {
			if e.metricPointsHaveForeignID(ctx, cache, ms.At(j), resID) {
				return true
			}
		}
	}
	return false
}

func (e *Enricher) metricPointsHaveForeignID(ctx context.Context, cache map[string]pcommon.Map, m pmetric.Metric, resID string) bool {
	has := func(a pcommon.Map) bool {
		tok, ok := e.findID(a)
		return ok && e.foreignID(ctx, cache, tok, resID)
	}
	switch m.Type() {
	case pmetric.MetricTypeGauge:
		dps := m.Gauge().DataPoints()
		for i := 0; i < dps.Len(); i++ {
			if has(dps.At(i).Attributes()) {
				return true
			}
		}
	case pmetric.MetricTypeSum:
		dps := m.Sum().DataPoints()
		for i := 0; i < dps.Len(); i++ {
			if has(dps.At(i).Attributes()) {
				return true
			}
		}
	case pmetric.MetricTypeHistogram:
		dps := m.Histogram().DataPoints()
		for i := 0; i < dps.Len(); i++ {
			if has(dps.At(i).Attributes()) {
				return true
			}
		}
	case pmetric.MetricTypeExponentialHistogram:
		dps := m.ExponentialHistogram().DataPoints()
		for i := 0; i < dps.Len(); i++ {
			if has(dps.At(i).Attributes()) {
				return true
			}
		}
	case pmetric.MetricTypeSummary:
		dps := m.Summary().DataPoints()
		for i := 0; i < dps.Len(); i++ {
			if has(dps.At(i).Attributes()) {
				return true
			}
		}
	}
	return false
}

// enrichResource resolves the ID on res and merges the k8s attributes it maps
// to, without overwriting attributes the sender already set. cache memoizes
// the built attributes per ID token for the duration of one request.
func (e *Enricher) enrichResource(ctx context.Context, res pcommon.Resource, cache map[string]pcommon.Map) {
	e.applyMetadata(ctx, res.Attributes(), cache)
}

// applyMetadata looks up the ID in a and merges the derived k8s attributes
// into a, leaving existing keys untouched. It reports whether an ID resolved.
func (e *Enricher) applyMetadata(ctx context.Context, a pcommon.Map, cache map[string]pcommon.Map) bool {
	cTok, cOK := e.tokenFrom(a, e.containerIDKeys, tokContainer)
	uTok, uOK := e.tokenFrom(a, e.podUIDKeys, tokPodUID)
	if !cOK && !uOK {
		built := e.peerFallback(ctx, cache)
		if built.Len() > 0 {
			mergeAttrs(built, a)
			return true
		}
		return false
	}
	// Try the container id first, then fall back to the pod uid: a stale
	// container id the store no longer knows must not block a resolvable pod
	// uid the sender also provided. Probe with attrsFor (no counting) and count
	// exactly ONCE below — kubescrape_ingest_resources_total counts RESOURCES,
	// so a resource carrying two unresolvable ids must not tally two.
	if cOK {
		if built := e.attrsFor(ctx, cache, cTok); built.Len() > 0 {
			obs.Ingested.WithLabelValues("enriched").Inc()
			mergeAttrs(built, a)
			return true
		}
	}
	if uOK {
		if built := e.attrsFor(ctx, cache, uTok); built.Len() > 0 {
			obs.Ingested.WithLabelValues("enriched").Inc()
			mergeAttrs(built, a)
			return true
		}
	}
	obs.Ingested.WithLabelValues("unresolved").Inc()
	return false
}

// tokenFrom returns the first non-empty value under keys as a kind-tagged
// token. The concatenation allocates; the loop over keys does not.
func (e *Enricher) tokenFrom(a pcommon.Map, keys []string, prefix string) (string, bool) {
	for _, k := range keys {
		if v, ok := a.Get(k); ok && v.Str() != "" {
			return prefix + v.Str(), true
		}
	}
	return "", false
}

// resolves reports whether token names an object the metadata service knows,
// without building or caching its attributes. Used to choose between a
// container id and a pod uid when a sender supplies both.
func (e *Enricher) resolves(ctx context.Context, cache map[string]pcommon.Map, token string) bool {
	// Memoised per request, INCLUDING the negative answer. metaclient caches
	// 200s, but the case this probe exists for — a stale container id — answers
	// 404, which is never cached, so on the split path (one call per DATA
	// POINT) a payload of 500 points issued 500 live GETs from inside the
	// handler for the same dead id.
	if cache != nil {
		if m, ok := cache[probeKey(token)]; ok {
			return m.Len() > 0
		}
	}
	pod, _ := e.lookupByID(ctx, token)
	if cache != nil {
		m := pcommon.NewMap()
		if pod != nil {
			m.PutBool("resolved", true) // non-empty marks the positive answer
		}
		cache[probeKey(token)] = m
	}
	return pod != nil
}

// probeKey namespaces a resolvability answer so it cannot collide with the
// built-attribute entry for the same token.
func probeKey(token string) string { return "\x00probe:" + token }

// resolvableToken picks the id token to attribute a resource (or data point)
// by, preferring the container id — it names the exact incarnation — but
// falling back to the pod uid when the container id does not resolve. A stale
// container id (the container restarted, or its tombstone expired) must not
// veto a pod uid the sender also supplied. The probe only runs when BOTH kinds
// are present, so the common single-id case costs nothing extra.
//
// Split mode needs this as much as resource mode: without it an identical
// payload was attributed differently by mode, and the split path additionally
// reduced the resource to the bare unresolved id, discarding every attribute
// the sender had set.
func (e *Enricher) resolvableToken(ctx context.Context, cache map[string]pcommon.Map, a pcommon.Map) string {
	cTok, cOK := e.tokenFrom(a, e.containerIDKeys, tokContainer)
	uTok, uOK := e.tokenFrom(a, e.podUIDKeys, tokPodUID)
	switch {
	case cOK && uOK:
		if e.resolves(ctx, cache, cTok) {
			return cTok
		}
		return uTok
	case cOK:
		return cTok
	default:
		return uTok // "" when neither is present
	}
}

// attrsFor resolves and caches a token's k8s attributes WITHOUT counting the
// outcome, for callers that probe more than one candidate token for a single
// resource and must count exactly once themselves.
func (e *Enricher) attrsFor(ctx context.Context, cache map[string]pcommon.Map, token string) pcommon.Map {
	if built, ok := cache[token]; ok {
		return built
	}
	built := pcommon.NewMap()
	if pod, container := e.lookupByID(ctx, token); pod != nil {
		built = e.buildFor(pod, container)
	}
	cache[token] = built
	return built
}

// buildFor renders the configured k8s resource attributes for a resolved pod
// (and, when the ID named one, its exact container) — the one build shared by
// the token path (attrsFor) and the peer-IP path (peerAttrs).
func (e *Enricher) buildFor(pod *kubemeta.Pod, container *kubemeta.Container) pcommon.Map {
	r := pcommon.NewResource()
	actx := attrs.Context{Pod: pod, Container: container}
	if e.cfg.NodeInfo != nil {
		actx.Node = e.cfg.NodeInfo()
	}
	e.cfg.Attrs.Build(r, actx)
	built := pcommon.NewMap()
	r.Attributes().CopyTo(built)
	return built
}

// countedKey namespaces the "this token's outcome has been tallied" marker so
// it cannot collide with the token's built-attribute entry nor the resolvability
// probe. It is what decouples COUNTING from CACHING: the shared sameObject
// resolves its candidate tokens through attrsFor, which populates cache[token]
// WITHOUT counting, so a "was cache[token] present?" test tallied nothing for a
// token sameObject had already resolved.
func countedKey(token string) string { return "\x00counted:" + token }

// builtAttrs returns the k8s attributes for a kind-tagged ID token — attrsFor
// plus the outcome counting, for single-token callers: the metadata lookup,
// the attribute build and the enriched/unresolved tally each happen once per
// distinct token per cache (so the per-resource counters stay per-resource;
// resource() is memoized by id). An empty map means the ID did not resolve.
//
// The tally is gated on a SEPARATE counted-marker, NOT on whether attrsFor had
// to build the attributes. The split path calls sameObject (merge-vs-overwrite)
// BEFORE builtAttrs for the same id, and sameObject resolves both tokens through
// attrsFor — so by the time builtAttrs runs the id is already in the cache. The
// old "was it cached?" gate therefore tallied NOTHING for every described
// object on the datapoint/split path (foreign objects AND same-pod merges),
// silently zeroing the enriched/unresolved signal for exactly the mode it
// matters most. The marker fires the FIRST time builtAttrs sees a token per
// request and never again, so it stays once-per-object and cannot double-count
// when both sameObject and builtAttrs — or two groupers sharing the cache — run
// for the same id.
func (e *Enricher) builtAttrs(ctx context.Context, cache map[string]pcommon.Map, token string) pcommon.Map {
	built := e.attrsFor(ctx, cache, token)
	ck := countedKey(token)
	if _, counted := cache[ck]; !counted {
		cache[ck] = pcommon.NewMap() // presence marks the outcome as tallied
		if built.Len() > 0 {
			obs.Ingested.WithLabelValues("enriched").Inc()
		} else {
			obs.Ingested.WithLabelValues("unresolved").Inc()
		}
	}
	return built
}

// mergeAttrs adds src's attributes to dst, never overwriting keys the sender
// already set. The sender is authoritative about ITSELF, which is the same
// rule the self-metadata stamping applies — hence the shared implementation.
func mergeAttrs(src, dst pcommon.Map) { attrs.FillAbsent(src, dst) }

// overwriteAttrs sets src's attributes on dst, replacing what the sender set.
// Used only where the resource describes an object OTHER than the sender (the
// datapoint-split path): there the sender's identity attributes name itself,
// not the object, so they are not authoritative. Keys absent from src are left
// alone.
func overwriteAttrs(src, dst pcommon.Map) {
	src.Range(func(k string, v pcommon.Value) bool {
		v.CopyTo(dst.PutEmpty(k))
		return true
	})
}

// peerCacheKey is the reserved key under which the peer-IP attribution is
// memoised in a request's attribute cache. A real token is "c:"/"u:" prefixed,
// so this cannot collide. peerRejectKey records that the same lookup was
// REJECTED, so the outcome is memoised as precisely as the success is.
const (
	peerCacheKey  = "\x00peer"
	peerRejectKey = "\x00peer-rejected"
)

// peerRejectWarnEvery throttles the rejected-peer warning. A relay in front of
// the listener rewrites EVERY connection, so the condition is either absent or
// universal; one line per push would bury the diagnosis in its own symptom.
const peerRejectWarnEvery = time.Minute

// peerAttrs returns the k8s attributes of the pod owning the connection's peer
// IP, resolved at most ONCE per request, and reports whether the resolution was
// REJECTED (see Config.PeerReject) as opposed to simply not resolving.
//
// The peer is a property of the connection, so every resource in a payload has
// the same one — yet this ran per resource, and /v1/pod-ips is deliberately
// uncacheable (recycled IPs need immediacy), so 500 ID-less resources meant 500
// serial round trips inside the handler. Worse, two resources of one payload
// could be attributed differently if the index changed mid-request. The cache
// is per request (the enricher itself is shared by concurrent handlers, so
// nothing may be memoised on it).
func (e *Enricher) peerAttrs(ctx context.Context, cache map[string]pcommon.Map) (pcommon.Map, bool) {
	if !e.cfg.PeerIPFallback {
		return pcommon.NewMap(), false
	}
	ip := peerIP(ctx)
	if ip == "" {
		return pcommon.NewMap(), false
	}
	if cache != nil {
		if m, ok := cache[peerCacheKey]; ok {
			_, rejected := cache[peerRejectKey]
			return m, rejected
		}
	}
	built := pcommon.NewMap()
	rejected := false
	if pod, err := e.cfg.Meta.PodByIP(ctx, ip); err == nil && pod != nil {
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
			built = e.buildFor(pod, nil)
		}
	}
	if cache != nil {
		cache[peerCacheKey] = built
		if rejected {
			cache[peerRejectKey] = pcommon.NewMap()
		}
	}
	return built, rejected
}

// peerFallback is peerAttrs plus the outcome accounting: peer_ip when the
// attribution lands (the caller merges the returned attributes), peer_ip_rejected
// when Config.PeerReject vetoed it, unresolved otherwise — counted per call,
// i.e. per RESOURCE on the resource path and per ""-group on the split path,
// like every other outcome of kubescrape_ingest_resources_total.
//
// ONE helper for the two sites because they had drifted: the resource path
// (applyMetadata) counted all three outcomes while the splitter's ""-group
// counted peer_ip and unresolved but NOTHING for a rejected peer — behind a
// comment claiming the rejection "has already been counted", which no site did
// — so -ingest-metrics-mode=datapoint (and an auto push demoted to split)
// under-reported the exact counter Config.PeerReject's doc promises.
func (e *Enricher) peerFallback(ctx context.Context, cache map[string]pcommon.Map) pcommon.Map {
	built, rejected := e.peerAttrs(ctx, cache)
	switch {
	case built.Len() > 0:
		obs.Ingested.WithLabelValues("peer_ip").Inc()
	case rejected:
		obs.Ingested.WithLabelValues("peer_ip_rejected").Inc()
	default:
		obs.Ingested.WithLabelValues("unresolved").Inc()
	}
	return built
}

func (e *Enricher) warnPeerRejected(ip string, pod *kubemeta.Pod) {
	if !e.peerWarnGate.Allow(peerRejectWarnEvery) {
		return
	}
	e.log.Warn("refusing to attribute pushed telemetry by peer IP: the connection's source address belongs to this receiver's own workload, so it was rewritten in flight (a proxy, a mesh sidecar, or an internal hop addressed to the application port). Those resources stay unenriched; give senders a resource-level container.id or k8s.pod.uid, or make the path preserve the client address",
		"peerIP", ip, "resolvedPod", pod.Namespace+"/"+pod.Name)
}

// idToken tags an ID value with its kind so a later lookup knows which
// endpoint to use, without re-scanning the key set.
const (
	tokContainer = "c\x00"
	tokPodUID    = "u\x00"
)

// foreignID reports whether a data-point token names a DIFFERENT OBJECT than
// the resource's token — the question the auto-mode decision actually needs.
//
// An UNRESOLVABLE point token is not evidence of a foreign object: it used to
// demote the payload from the resource path (which would have enriched the
// sender correctly) to the split path, where a group whose id resolves to
// nothing has its copied resource CLEARED — deleting every attribute the
// sender set, so service.name vanished and the Prometheus job became
// unknown_service. A token that DOES resolve is foreign exactly when it names
// a different object than the resource's — one predicate, sameObject, shared
// with the split path (see its comment for the drift this repaired).
func (e *Enricher) foreignID(ctx context.Context, cache map[string]pcommon.Map, tok, resID string) bool {
	if tok == resID {
		return false
	}
	if !e.resolves(ctx, cache, tok) {
		return false // unresolvable: not evidence of anything
	}
	return !e.sameObject(ctx, cache, tok, resID)
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
// Both lookups are memoised per request through the same cache the enrichment
// uses (attrsFor), so a KSM-shaped payload costs no extra round trips — and
// the resource branch that usually follows reuses the built attributes.
func (e *Enricher) sameObject(ctx context.Context, cache map[string]pcommon.Map, a, b string) bool {
	if a == "" || b == "" {
		return false
	}
	if a == b {
		return true
	}
	ra := e.attrsFor(ctx, cache, a)
	rb := e.attrsFor(ctx, cache, b)
	if ra.Len() == 0 || rb.Len() == 0 {
		return false // at least one did not resolve: no evidence they match
	}
	ua, oka := ra.Get("k8s.pod.uid")
	ub, okb := rb.Get("k8s.pod.uid")
	if !oka || !okb || ua.Str() != ub.Str() {
		return false
	}
	ca, _ := ra.Get("k8s.container.name")
	cb, _ := rb.Get("k8s.container.name")
	return ca.Str() == cb.Str() || ca.Str() == "" || cb.Str() == ""
}

// findID reports the first container ID or pod UID found in a, as a
// kind-tagged token (container keys first — a container ID names the exact
// incarnation).
func (e *Enricher) findID(a pcommon.Map) (token string, ok bool) {
	if tok, ok := e.tokenFrom(a, e.containerIDKeys, tokContainer); ok {
		return tok, true
	}
	return e.tokenFrom(a, e.podUIDKeys, tokPodUID)
}

// lookupByID resolves a kind-tagged ID token to metadata (nil pod on miss).
func (e *Enricher) lookupByID(ctx context.Context, token string) (*kubemeta.Pod, *kubemeta.Container) {
	switch {
	case len(token) >= 2 && token[:2] == tokContainer:
		id := token[2:]
		md, err := e.cfg.Meta.Container(ctx, id, e.cfg.Wait)
		if err != nil {
			e.log.Debug("ingest: container lookup failed", "id", id, "error", err)
			return nil, nil
		}
		return &md.Pod, &md.Container
	case len(token) >= 2 && token[:2] == tokPodUID:
		uid := token[2:]
		pod, err := e.cfg.Meta.PodByUID(ctx, uid)
		if err != nil {
			e.log.Debug("ingest: pod-uid lookup failed", "uid", uid, "error", err)
			return nil, nil
		}
		return pod, nil
	}
	return nil, nil
}
