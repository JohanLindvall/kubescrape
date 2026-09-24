package otlpingest

// The per-request metadata lookup layer: the kind-tagged id tokens, the request
// cache every decision of one push shares — memoised lookups, and the budgets
// bounding what one push may cost the metadata service in lookups and in
// server-side wait — and the lookups themselves.

import (
	"context"
	"strings"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"

	"github.com/JohanLindvall/kubescrape/internal/obs"
	"github.com/JohanLindvall/kubescrape/pkg/kubemeta"
	"github.com/JohanLindvall/kubescrape/pkg/metaclient"
)

// tokContainer and tokPodUID prefix an ID value with its kind — a kind-tagged
// token — so a later lookup knows which endpoint to use without re-scanning the
// key set.
const (
	tokContainer = "c\x00"
	tokPodUID    = "u\x00"
)

// splitToken splits a kind-tagged token into its kind prefix (tokContainer or
// tokPodUID) and the raw id value, reporting false for anything else. Every
// reader of a token goes through it rather than slicing at a fixed offset, so
// nothing depends on the two prefixes happening to share a length.
func splitToken(token string) (kind, id string, ok bool) {
	if id, ok := strings.CutPrefix(token, tokContainer); ok {
		return tokContainer, id, true
	}
	if id, ok := strings.CutPrefix(token, tokPodUID); ok {
		return tokPodUID, id, true
	}
	return "", "", false
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
	// waitSpent is the server-side wait time this request's attribution
	// lookups have actually consumed, bounded by lookupWaitBudgetFactor x
	// Config.Wait (containerLookup): the count budgets above bound how many
	// lookups a push may issue, this bounds how long they may BLOCK. Elapsed
	// time, not requested waits — a lookup that resolves fast spends only
	// what it waited.
	waitSpent time.Duration

	// tokBuf renders a kind-tagged token for a MAP LOOKUP without allocating
	// (map[string(buf)] reads do not copy). The auto-mode point walk
	// (foreignPointID) and the token interner (token) use it, each only within
	// one call — a token that has to be STORED is materialised as a string
	// first.
	tokBuf []byte
	// tokens interns the kind-tagged tokens the split path resolves per DATA
	// POINT (token), so each distinct id is materialised once per push rather
	// than once per point. Bounded by maxInternedTokens: past it a token is
	// allocated plainly, so a push naming a quarter-million distinct fabricated
	// ids cannot grow a second unbounded map beside the memos the lookup
	// budgets already bound. Lazily allocated — only the split path writes it.
	tokens map[string]string

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

// maxInternedTokens bounds reqCache.tokens. The lookup budget is the natural
// size: past maxLookupsPerRequest distinct ids nothing new resolves anyway, and
// interning is only an allocation saving, never a correctness requirement.
const maxInternedTokens = maxLookupsPerRequest

// token returns the kind-tagged token prefix+val, interned for the request: a
// repeat of an id already seen costs a map probe that does not copy the key,
// not the concatenation. See reqCache.tokens for the bound.
func (c *reqCache) token(prefix, val string) string {
	buf := append(c.tokBuf[:0], prefix...)
	buf = append(buf, val...)
	c.tokBuf = buf
	if t, ok := c.tokens[string(buf)]; ok {
		return t
	}
	t := string(buf)
	if len(c.tokens) < maxInternedTokens {
		if c.tokens == nil {
			c.tokens = map[string]string{}
		}
		c.tokens[t] = t
	}
	return t
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

// lookupWaitBudgetFactor sizes the per-request WAIT-TIME budget for
// attribution lookups, as a multiple of Config.Wait. The count budget above
// bounds how many lookups one push may issue but not how long each may park:
// a waited container lookup sits in the metadata service's waiter map for the
// full -ingest-metadata-wait, serially, inside this handler — so with a
// non-default wait, a push naming distinct fabricated ids held its in-flight
// slot (and, on HTTP, its byte-budget charge) for count x wait, and ~32 such
// sockets shed the node's whole ingest. Four waits covers the legitimate
// shape — a push racing the kubelet posting a few of its OWN ids, each wait
// released the moment the id appears — without letting invented ids stack
// maxLookupsPerRequest waits. The budget is charged by time actually ELAPSED,
// never by waits requested, so a lookup that resolves fast spends almost
// nothing; past it a lookup proceeds with wait 0, which still resolves every
// already-posted id.
const lookupWaitBudgetFactor = 4

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

// warnWaitBudget is the wait variant: the third allowance degrades the least —
// past it lookups still run and still resolve already-posted ids, they just no
// longer park in the metadata service's waiter map for ids that may never
// appear.
func (e *Enricher) warnWaitBudget() {
	if !e.waitWarnGate.Allow(lookupBudgetWarnEvery) {
		return
	}
	e.log.Warn("ingest: a push's distinct ids exhausted the per-request metadata wait budget; further lookups run without waiting (already-posted ids still resolve)",
		"exhausted", "wait", "budget", lookupWaitBudgetFactor*e.cfg.Wait)
}

// noteLookupFailed reports a metadata lookup that failed for a reason OTHER
// than "the object is unknown".
//
// The two are not the same event and only one is actionable. A 404 is ordinary:
// an id races the API server, a sender names a container that has already gone,
// a pod uid belongs to another node — the Debug line above is the right level
// for it, and kubescrape_ingest_resources_total{outcome="unresolved"} carries
// the rate. Anything else — a refused connection, a 5xx, a body that does not
// decode — means the metadata service is not answering THIS agent, and the
// visible symptom is every pushed resource silently losing its Kubernetes
// attribution while the counter that moves says only "unresolved", the same
// thing it says for a stale id. That was diagnosable only at Debug, which is
// not on during the outage it explains.
//
// Throttled and unkeyed: the condition is the metadata service, and during an
// outage every id in every push takes this path.
func (e *Enricher) noteLookupFailed(err error) {
	if metaclient.IsNotFound(err) {
		return
	}
	if !e.metaWarnGate.Allow(lookupBudgetWarnEvery) {
		return
	}
	e.log.Warn("ingest: the metadata service is not answering lookups, so pushed telemetry is being forwarded "+
		"without Kubernetes attribution (it is not dropped). Check the metadata service and -metadata-endpoint",
		"error", err)
}

// resolves reports whether token names an object the metadata service knows,
// without building or caching its attributes — the auto-mode foreign-point
// walk's question (foreignID), asked of every distinct point id of a push.
//
// It is a PROBE — it asks "does this resolve", never "attribute this record" —
// so it never passes Config.Wait: the server-side wait exists to hold an
// ATTRIBUTION until a not-yet-posted id appears, and each waited request is
// parked in the metadata service's waiter map for the full wait — a hold an
// unauthenticated sender controls, one per distinct id it invents. (Choosing
// between a container id and a pod uid is NOT a probe, and does not use this:
// see resolvableToken.)
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

// maxLookupIDBytes bounds the sender-supplied id a lookup is issued for. A
// container id is 64 hex characters behind a runtime prefix ("containerd://"),
// a pod uid is 36; nothing a runtime or the API server mints comes near 256.
//
// The bound is on what an UNAUTHENTICATED sender can make this process send and
// log: the id becomes a URL path segment (up to 3x its length once escaped),
// the metadata service refuses any request line past its 8 KiB header bound,
// and both the client's failure Warn and noteLookupFailed's render that URL —
// so a multi-MiB container.id was a guaranteed refusal that cost a round trip,
// a lookup-budget slot and two multi-MiB log lines per throttle window. An id
// over the bound is simply unresolvable: the resource is forwarded
// unenriched and counted unresolved, like any other id the service does not
// know.
const maxLookupIDBytes = 256

// lookupByID resolves a kind-tagged ID token to metadata (nil pod on miss),
// charging the request's lookup budget: past it every id reads as unresolvable.
// wait is the caller's — probes pass 0, attribution passes Config.Wait — and
// probe says which half of the budget the call spends (see
// maxProbeLookupsPerRequest); a positive wait is additionally clamped against
// the request's wait-time budget (containerLookup).
func (e *Enricher) lookupByID(ctx context.Context, cache *reqCache, token string, wait time.Duration, probe bool) (*kubemeta.Pod, *kubemeta.Container) {
	kind, id, ok := splitToken(token)
	if !ok || len(id) > maxLookupIDBytes {
		// Unresolvable by construction, and refused before it costs a lookup or
		// the budget: see maxLookupIDBytes.
		return nil, nil
	}
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
	if kind == tokContainer {
		md, err := e.containerLookup(ctx, cache, id, wait)
		if err != nil {
			e.log.Debug("ingest: container lookup failed", "id", clipForLog(id), "error", err)
			e.noteLookupFailed(err)
			return nil, nil
		}
		return &md.Pod, &md.Container
	}
	pod, err := e.cfg.Meta.PodByUID(ctx, id)
	if err != nil {
		e.log.Debug("ingest: pod-uid lookup failed", "uid", clipForLog(id), "error", err)
		e.noteLookupFailed(err)
		return nil, nil
	}
	return pod, nil
}

// containerLookup issues the container lookup, charging the request's
// wait-time budget for the server-side block a positive wait buys (see
// lookupWaitBudgetFactor). The clamp is against time already ELAPSED, and the
// spend is measured around the call itself: a lookup that resolves the moment
// the id appears — the case the wait exists for — burns only that moment, so
// the whole budget stays available for the ids that actually park.
func (e *Enricher) containerLookup(ctx context.Context, cache *reqCache, id string, wait time.Duration) (*kubemeta.ContainerMetadata, error) {
	if wait <= 0 {
		return e.cfg.Meta.Container(ctx, id, 0)
	}
	remaining := lookupWaitBudgetFactor*e.cfg.Wait - cache.waitSpent
	if remaining <= 0 {
		e.warnWaitBudget()
		return e.cfg.Meta.Container(ctx, id, 0)
	}
	if wait > remaining {
		wait = remaining
	}
	start := time.Now()
	md, err := e.cfg.Meta.Container(ctx, id, wait)
	cache.waitSpent += time.Since(start)
	return md, err
}
