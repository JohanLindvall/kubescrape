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

	// peerWarnGate/lookupWarnGate throttle their warnings; the enricher is
	// shared by concurrent handlers, which is the throttles' contract.
	peerWarnGate   logdedupe.Throttle
	lookupWarnGate logdedupe.Throttle
}

// reqCache is the state one push's enrichment shares across every decision it
// makes: memoised lookups (N resources naming one id cost one round trip) and
// the budgets bounding what a single request may cost. One per request — the
// Enricher itself is shared by concurrent handlers and memoises nothing.
type reqCache struct {
	// ids memoises attribution lookups (issued with the full Config.Wait) per
	// kind-tagged token.
	ids map[string]idResult
	// probes memoises wait-free resolvability answers, negatives included:
	// metaclient never caches a 404, so without the negative entry the split
	// path issued one live GET per DATA POINT for the same dead id.
	probes map[string]bool
	// counted marks tokens whose enriched/unresolved outcome has been tallied.
	// It decouples COUNTING from CACHING: sameObject resolves its candidate
	// tokens through attrsFor, which fills ids WITHOUT counting, so a "was the
	// token already cached?" gate tallied nothing for a token sameObject had
	// already resolved.
	counted map[string]struct{}
	// lookups counts the live metadata lookups issued — probes and attribution
	// builds alike — bounded by maxLookupsPerRequest; probeLookups counts the
	// probe half alone, bounded by maxProbeLookupsPerRequest so a walk of
	// unresolvable point ids can never spend the attribution's share.
	lookups      int
	probeLookups int

	// tokBuf renders a kind-tagged token for a MAP LOOKUP without allocating
	// (map[string(buf)] reads do not copy). Only the auto-mode point walk uses
	// it, and only within one call — a token that has to be STORED is
	// materialised as a string first.
	tokBuf []byte

	// peer memoises the peer-IP attribution: the peer is a property of the
	// CONNECTION, so every resource in a payload has the same one (and
	// /v1/pod-ips is deliberately uncacheable — recycled IPs need immediacy).
	peer         pcommon.Map
	peerResolved bool
	peerRejected bool
	peerDone     bool

	// splitGroups/splitCopied are the splitter's per-payload budgets: the group
	// count and the estimated bytes of minted copies (split.go). They live here
	// because both must span every input ResourceMetrics of one push.
	splitGroups int
	splitCopied int
}

// newReqCache allocates only what EVERY push writes. probes and counted are
// filled on their own paths (the resolvability walk, the described-object
// tally) and a push that takes neither used to pay for both maps regardless —
// on the request path, per push, on the unauthenticated listener. Reading a nil
// map is legal, so only the writers check.
func newReqCache() *reqCache {
	return &reqCache{ids: map[string]idResult{}}
}

// idResult is one kind-tagged token's lookup outcome. resolved and the
// identity fields come from the LOOKUP RESULT, never from built: built is the
// operator-FILTERED attribute rendering (enable/disable lists, defaults:
// false can empty it), so an empty built does not mean the object is unknown —
// reading it that way let a resourceAttributes filter change attribution
// decisions (sameObject, the auto-mode demotion, which resource a split point
// lands on) and count resolved lookups as unresolved.
type idResult struct {
	built     pcommon.Map // rendered k8s attributes; may be empty for a resolved object
	resolved  bool
	podUID    string
	container string // container name; "" when the token names a whole pod
}

// maxLookupsPerRequest bounds the live metadata lookups one push may trigger.
// The listeners are unauthenticated and lookups run serially inside the
// handler, so without a bound a payload naming tens of thousands of DISTINCT
// bogus ids (each a memo miss by construction) held its in-flight slot for the
// whole walk and the metadata service for one GET each. Twice maxSplitGroups
// because one attributable object may legitimately cost two lookups — the
// wait-free resolvability probe and the attribution build. Past the budget an
// id is treated as unresolvable.
const maxLookupsPerRequest = 2 * maxSplitGroups

// maxProbeLookupsPerRequest is the share of maxLookupsPerRequest the
// resolvability PROBES may spend; the remainder is reserved for ATTRIBUTION.
//
// The two halves are not interchangeable. A probe answers "is this id worth
// splitting on", and an unresolvable one is deliberately not evidence of
// anything — so the auto-mode decision walks EVERY distinct data-point id
// before the resource's own attribution is even attempted. Sharing one
// allowance let a push of invented point ids exhaust it during that walk and
// leave the sender itself unattributed: a resolvable resource-level
// container.id resolving to nothing, exported wholly unenriched and counted
// unresolved, indistinguishable from an id the cluster never had.
const maxProbeLookupsPerRequest = maxLookupsPerRequest - maxSplitGroups

// lookupBudgetWarnEvery throttles the over-budget warning: past the budget
// EVERY further distinct id takes that path, and the diagnosis is per push,
// not per id.
const lookupBudgetWarnEvery = time.Minute

// warnLookupBudget names WHICH allowance bound, since the two degrade
// different things: the probe share only makes further point ids read as
// unresolvable to the split/auto DECISION (which is not evidence of anything),
// while the request total takes attribution with it.
func (e *Enricher) warnLookupBudget(exhausted string, bound int) {
	if !e.lookupWarnGate.Allow(lookupBudgetWarnEvery) {
		return
	}
	e.log.Warn("ingest: a push named more distinct ids than one request may look up; the remainder is treated as unresolvable",
		"exhausted", exhausted, "budget", bound)
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
	cache := newReqCache()
	rls := ld.ResourceLogs()
	for i := 0; i < rls.Len(); i++ {
		rl := rls.At(i)
		e.enrichResource(ctx, rl.Resource(), cache)
		if e.cfg.Scrub == nil {
			continue
		}
		sls := rl.ScopeLogs()
		for j := 0; j < sls.Len(); j++ {
			lrs := sls.At(j).LogRecords()
			for k := 0; k < lrs.Len(); k++ {
				// Scrub BEFORE anything reads the body: enrichment (which
				// runs later, in the server's applyLogChain — one bounded
				// body render shared with log-metrics and the rules) copies
				// body slices (exception attributes) that must not carry
				// secrets, and the metric/rule chain matches against the
				// same scrubbed view.
				e.scrubBody(lrs.At(k).Body(), 0)
			}
		}
	}
}

// LinesEnabled reports whether per-line body enrichment (-enrich) is on. The
// enrichment itself runs in the server's applyLogChain — beside log-metrics
// and the rules, over ONE bounded rendering of the body — but the flag lives
// in this config.
func (e *Enricher) LinesEnabled() bool { return e.cfg.EnrichLines }

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
	cache := newReqCache()
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
		return e.splitAndEnrich(ctx, newReqCache(), md)
	case MetricsResource:
		e.enrichMetricResources(ctx, md)
		return md
	default: // auto
		// One cache for the decision AND the enrichment that follows — on BOTH
		// branches, so the resolvability probes the decision makes are not paid
		// for twice (a demoted payload's groupers reuse them) and the lookup
		// budget spans the whole push rather than re-arming at the demotion.
		cache := newReqCache()
		if e.resourceModeSuffices(ctx, cache, md) {
			e.enrichMetricResourcesWith(ctx, cache, md)
			return md
		}
		return e.splitAndEnrich(ctx, cache, md)
	}
}

// enrichMetricResources enriches each ResourceMetrics from its own resource
// attributes.
func (e *Enricher) enrichMetricResources(ctx context.Context, md pmetric.Metrics) {
	e.enrichMetricResourcesWith(ctx, newReqCache(), md)
}

// enrichMetricResourcesWith is enrichMetricResources against a caller-supplied
// cache, so the auto-mode decision's lookups are reused.
func (e *Enricher) enrichMetricResourcesWith(ctx context.Context, cache *reqCache, md pmetric.Metrics) {
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
//
// The two halves run as two PASSES, cheapest first. Interleaving them let an
// early resource's point walk run to completion before a later resource that
// carries no ID at all — which alone forces false — was ever looked at, and that
// walk spends the request's lookup budget on probes whose answer cannot change
// the outcome.
func (e *Enricher) resourceModeSuffices(ctx context.Context, cache *reqCache, md pmetric.Metrics) bool {
	rms := md.ResourceMetrics()
	for i := 0; i < rms.Len(); i++ {
		if !e.hasID(rms.At(i).Resource().Attributes()) {
			return false
		}
	}
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
func (e *Enricher) anyForeignDataPointID(ctx context.Context, cache *reqCache, rm pmetric.ResourceMetrics, resID string) bool {
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

func (e *Enricher) metricPointsHaveForeignID(ctx context.Context, cache *reqCache, m pmetric.Metric, resID string) bool {
	has := func(a pcommon.Map) bool {
		prefix, val, ok := e.idValue(a)
		return ok && e.foreignPointID(ctx, cache, prefix, val, resID)
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
func (e *Enricher) enrichResource(ctx context.Context, res pcommon.Resource, cache *reqCache) {
	e.applyMetadata(ctx, res.Attributes(), cache)
}

// applyMetadata looks up the ID in a and merges the derived k8s attributes
// into a, leaving existing keys untouched. It reports whether an ID resolved.
func (e *Enricher) applyMetadata(ctx context.Context, a pcommon.Map, cache *reqCache) bool {
	cTok, cOK := e.tokenFrom(a, e.containerIDKeys, tokContainer)
	uTok, uOK := e.tokenFrom(a, e.podUIDKeys, tokPodUID)
	if !cOK && !uOK {
		built, resolved := e.peerFallback(ctx, cache)
		if resolved {
			mergeAttrs(built, a)
			return true
		}
		return false
	}
	// Try the container id first, then fall back to the pod uid: a stale
	// container id the store no longer knows must not block a resolvable pod
	// uid the sender also provided. Probe with attrsFor (no counting) and count
	// exactly ONCE below — kubescrape_ingest_resources_total counts RESOURCES,
	// so a resource carrying two unresolvable ids must not tally two. enriched
	// keys on RESOLVED, not on a non-empty rendering: the operator's attribute
	// filter can empty the build for an object the lookup found, and that must
	// not read as unresolved.
	if cOK {
		if r := e.attrsFor(ctx, cache, cTok); r.resolved {
			obs.Ingested.WithLabelValues("enriched").Inc()
			mergeAttrs(r.built, a)
			return true
		}
	}
	if uOK {
		if r := e.attrsFor(ctx, cache, uTok); r.resolved {
			obs.Ingested.WithLabelValues("enriched").Inc()
			mergeAttrs(r.built, a)
			return true
		}
	}
	obs.Ingested.WithLabelValues("unresolved").Inc()
	return false
}

// tokenFrom returns the first non-empty value under keys as a kind-tagged
// token. The concatenation allocates; the loop over keys does not — which is
// why the auto-mode point walk goes through idValue instead and materialises a
// token only when one has to be STORED.
func (e *Enricher) tokenFrom(a pcommon.Map, keys []string, prefix string) (string, bool) {
	if v, ok := valueUnder(a, keys); ok {
		return prefix + v, true
	}
	return "", false
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

// idValue is findID in PARTS: the kind prefix and the raw id value, with no
// token built. The auto-mode decision visits every data point of a push, and
// the concatenation findID does was one heap allocation each — the largest
// non-pdata allocator on the default mode's default path.
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

// tokenIs reports whether the kind-tagged token tok is prefix+val, comparing in
// place rather than building the concatenation to compare it against.
func tokenIs(tok, prefix, val string) bool {
	return len(tok) == len(prefix)+len(val) && tok[:len(prefix)] == prefix && tok[len(prefix):] == val
}

// resolves reports whether token names an object the metadata service knows,
// without building or caching its attributes. Used to choose between a
// container id and a pod uid when a sender supplies both, and by the auto-mode
// foreign-point walk.
//
// It is a PROBE — it asks "does this resolve", never "attribute this record" —
// so it never passes Config.Wait: the server-side wait exists to hold an
// ATTRIBUTION until a not-yet-posted id appears, and each waited request is
// parked in the metadata service's waiter map for the full wait — a hold an
// unauthenticated sender controls, one per distinct id it invents.
func (e *Enricher) resolves(ctx context.Context, cache *reqCache, token string) bool {
	if r, ok := cache.ids[token]; ok {
		return r.resolved // already attributed: the full lookup answers the probe
	}
	// Memoised per request, INCLUDING the negative answer. metaclient caches
	// 200s, but the case this probe exists for — a stale container id — answers
	// 404, which is never cached, so on the split path (one call per DATA
	// POINT) a payload of 500 points issued 500 live GETs from inside the
	// handler for the same dead id.
	if v, ok := cache.probes[token]; ok {
		return v
	}
	pod, _ := e.lookupByID(ctx, cache, token, 0, true)
	if cache.probes == nil {
		cache.probes = map[string]bool{}
	}
	cache.probes[token] = pod != nil
	return pod != nil
}

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
func (e *Enricher) resolvableToken(ctx context.Context, cache *reqCache, a pcommon.Map) string {
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

// attrsFor resolves and caches a token's lookup outcome WITHOUT counting it,
// for callers that probe more than one candidate token for a single resource
// and must count exactly once themselves. The attribution lookup carries the
// configured wait — a not-yet-posted id the sender is about to be attributed
// by may legitimately appear within it.
func (e *Enricher) attrsFor(ctx context.Context, cache *reqCache, token string) idResult {
	if r, ok := cache.ids[token]; ok {
		return r
	}
	r := idResult{built: emptyAttrs}
	if pod, container := e.lookupByID(ctx, cache, token, e.cfg.Wait, false); pod != nil {
		r.resolved = true
		r.podUID = pod.UID
		if container != nil {
			r.container = container.Name
		}
		r.built = e.buildFor(pod, container)
	}
	cache.ids[token] = r
	return r
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

// builtAttrs returns the lookup outcome for a kind-tagged ID token — attrsFor
// plus the outcome counting, for single-token callers: the metadata lookup,
// the attribute build and the enriched/unresolved tally each happen once per
// distinct token per cache (so the per-resource counters stay per-resource;
// resource() is memoized by id).
//
// The tally is gated on the cache's counted set, NOT on whether attrsFor had
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
func (e *Enricher) builtAttrs(ctx context.Context, cache *reqCache, token string) idResult {
	r := e.attrsFor(ctx, cache, token)
	if _, counted := cache.counted[token]; !counted {
		if cache.counted == nil {
			cache.counted = map[string]struct{}{}
		}
		cache.counted[token] = struct{}{}
		if r.resolved {
			obs.Ingested.WithLabelValues("enriched").Inc()
		} else {
			obs.Ingested.WithLabelValues("unresolved").Inc()
		}
	}
	return r
}

// emptyAttrs is the shared "nothing was built" attribute map. Every consumer of
// a built map only READS it (mergeAttrs/overwriteAttrs Range over the source),
// so the not-applicable answers need no map of their own — and they are the
// common ones: an unresolved token per distinct id, and the peer fallback on
// every id-less resource of every push while the fallback is off, which is the
// default.
var emptyAttrs = pcommon.NewMap()

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
// (applyMetadata) counted all three outcomes while the splitter's ""-group
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
func (e *Enricher) foreignID(ctx context.Context, cache *reqCache, tok, resID string) bool {
	if tok == resID {
		return false
	}
	if !e.resolves(ctx, cache, tok) {
		return false // unresolvable: not evidence of anything
	}
	return !e.sameObject(ctx, cache, tok, resID)
}

// foreignPointID is foreignID for a data point's id, taking the id in PARTS.
// Same answer, same order of decisions — but the kind-tagged token is only
// materialised on a MEMO MISS, i.e. at most once per distinct id per push,
// instead of once per data point. Every earlier exit reads the memo through
// map[string(buf)], which does not copy the key.
func (e *Enricher) foreignPointID(ctx context.Context, cache *reqCache, prefix, val, resID string) bool {
	if tokenIs(resID, prefix, val) {
		return false // the sender's own id: the resource branch attributes it
	}
	buf := append(cache.tokBuf[:0], prefix...)
	buf = append(buf, val...)
	cache.tokBuf = buf
	if r, ok := cache.ids[string(buf)]; ok {
		if !r.resolved {
			return false // unresolvable: not evidence of anything
		}
		return !sameResolved(r, e.attrsFor(ctx, cache, resID))
	}
	if resolved, ok := cache.probes[string(buf)]; ok && !resolved {
		return false
	}
	return e.foreignID(ctx, cache, string(buf), resID)
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

// findID reports the first container ID or pod UID found in a, as a
// kind-tagged token (container keys first — a container ID names the exact
// incarnation).
func (e *Enricher) findID(a pcommon.Map) (token string, ok bool) {
	if tok, ok := e.tokenFrom(a, e.containerIDKeys, tokContainer); ok {
		return tok, true
	}
	return e.tokenFrom(a, e.podUIDKeys, tokPodUID)
}

// lookupByID resolves a kind-tagged ID token to metadata (nil pod on miss),
// charging the request's lookup budget: past it every id reads as unresolvable.
// wait is the caller's — probes pass 0, attribution passes Config.Wait — and
// probe says which half of the budget the call spends (see
// maxProbeLookupsPerRequest).
func (e *Enricher) lookupByID(ctx context.Context, cache *reqCache, token string, wait time.Duration, probe bool) (*kubemeta.Pod, *kubemeta.Container) {
	if cache.lookups >= maxLookupsPerRequest {
		e.warnLookupBudget("request", maxLookupsPerRequest)
		return nil, nil
	}
	if probe && cache.probeLookups >= maxProbeLookupsPerRequest {
		e.warnLookupBudget("probes", maxProbeLookupsPerRequest)
		return nil, nil
	}
	cache.lookups++
	if probe {
		cache.probeLookups++
	}
	switch {
	case len(token) >= 2 && token[:2] == tokContainer:
		id := token[2:]
		md, err := e.cfg.Meta.Container(ctx, id, wait)
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
