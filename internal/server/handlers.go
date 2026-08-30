// Package server exposes the metadata store over HTTP.
package server

// The HTTP handlers for the v1 metadata endpoints, plus the shared
// response helpers (caching headers, wait budgets, JSON writing).

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/JohanLindvall/haste/xxh3"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/validate/content"

	"github.com/JohanLindvall/kubescrape/internal/agent/attrs"
	"github.com/JohanLindvall/kubescrape/internal/logdedupe"
	"github.com/JohanLindvall/kubescrape/internal/obs"
	"github.com/JohanLindvall/kubescrape/internal/peerip"
	"github.com/JohanLindvall/kubescrape/internal/scrape"
	"github.com/JohanLindvall/kubescrape/internal/servicemonitors"
	"github.com/JohanLindvall/kubescrape/internal/services"
	"github.com/JohanLindvall/kubescrape/internal/store"
	"github.com/JohanLindvall/kubescrape/pkg/kubemeta"
)

// handleContainer serves GET /v1/containers/{id}?wait=2s.
//
// The ID may include the runtime prefix ("containerd://..."), URL-escaped or
// not. If the ID is unknown the request blocks up to the wait budget for the
// metadata to appear (covering the gap between container start and the API
// server reporting the container ID).
func (s *Server) handleContainer(w http.ResponseWriter, r *http.Request) {
	wait, err := s.waitBudget(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	id := kubemeta.NormalizeContainerID(r.PathValue("id"))
	if id == "" {
		writeError(w, http.StatusBadRequest, "empty container id")
		return
	}
	if len(id) > maxContainerIDLen {
		// A real runtime ID is 64 hex characters; a kilobytes-long path
		// segment is hostile input, not a container that might yet appear.
		writeError(w, http.StatusBadRequest, "container id too long")
		return
	}
	// …and the id that passed that check is a SLICE of the path: NormalizeContainerID
	// returns what follows the last colon, so `<16 KB>:<64hex>` yields a 64-byte
	// string whose backing array is the whole 16 KB. It outlives the release
	// below — it is this handler's local for the whole wait AND the store's
	// waiter-map key — so it is copied out here, where the length check has just
	// bounded what the copy can cost.
	id = strings.Clone(id)

	// Everything this handler still needs from the request head has been read
	// above, and both of its parking spots are below. Drop the rest before
	// either: net/http's parse of a head expands it by 20-30x and the store's
	// waiter cap is a COUNT, so a request that holds its parsed head across the
	// wait budget makes that count bound a number rather than the process
	// (releaseParkedHead carries the measurements). "id" is the route's wildcard
	// (`GET /v1/containers/{id...}`), whose matched value is one of the copies.
	releaseParkedHead(r, "id")

	ctx, cancel := context.WithTimeout(r.Context(), wait)
	defer cancel()

	// The one Debug seam on this route, and the reason it is a seam rather
	// than a line per request: /v1/containers is polled by every agent for
	// every log file, so an unconditional entry line is a flood at Info cost.
	// slog evaluates arguments EAGERLY, so even the time.Now() below is behind
	// the level check; everything emitted from here reports a TRANSITION (a
	// lookup that actually blocked, or one that was refused), never the warm
	// path.
	debug := s.log().Enabled(ctx, slog.LevelDebug)
	var started time.Time
	if debug {
		started = time.Now()
	}

	// Don't report "not found" from a cache that hasn't finished its initial
	// sync; spend the wait budget on readiness first if needed. A drain ends
	// that wait too — the shutdown path must not leave a request parked here
	// (see Server.Drain), and its refusal carries Retry-After because the next
	// pod behind the Service can answer at once, unlike a sync that is merely
	// slow.
	if err := s.waitReady(ctx); err != nil {
		// errDraining: the next pod behind the Service can answer at once.
		// ErrTooManyWaiters: the blocked-lookup cap is saturated, exactly as it
		// is when the store refuses below — same refusal, same signal to the
		// agent's backoff. errNotSynced is the one that carries no Retry-After:
		// this pod is merely still filling its caches.
		if errors.Is(err, errDraining) || errors.Is(err, store.ErrTooManyWaiters) {
			w.Header().Set("Retry-After", "1")
		}
		writeError(w, http.StatusServiceUnavailable, err.Error())
		return
	}

	res, ok, err := s.store.GetContainer(ctx, id)
	if err != nil {
		// The store REFUSED to wait — the waiter cap is saturated
		// (ErrTooManyWaiters) or shutdown has drained the waiters
		// (ErrShuttingDown). Either way retryable, never a 404: the container
		// may exist momentarily, and on the shutdown path the next pod behind
		// the Service can answer at once.
		//
		// Both are counted (kubescrape_container_lookups_shed_total and
		// _drained_total), and the counters are what an alert reads; this line
		// is what an incident reads, because a shed storm's counter says how
		// many and never WHICH — and "the agent's first poll returned nothing"
		// is answered by seeing the ids that were refused.
		if debug {
			s.log().Debug("container lookup refused before it could wait",
				"id", id, "wait", wait, "error", err)
		}
		w.Header().Set("Retry-After", "1")
		writeError(w, http.StatusServiceUnavailable, err.Error())
		return
	}
	if debug {
		// A lookup that PARKED and one that hit the warm index are the same
		// call here — the store does not report which it did — so the elapsed
		// time is the discriminator, and blockedLookupFloor is what keeps the
		// warm path (microseconds) from emitting anything. Reporting per
		// transition rather than per request is the whole discipline: this
		// fires for a lookup that waited, whatever it then returned.
		if waited := time.Since(started); waited >= blockedLookupFloor {
			s.log().Debug("container lookup blocked and then woke",
				"id", id, "waited", waited.Round(time.Millisecond), "wait", wait, "found", ok)
		}
	}
	if !ok {
		// A lookup that BLOCKED for its whole budget and a lookup that missed
		// instantly are the same 404 to
		// kubescrape_http_requests_total{code="404"}, and they mean opposite
		// things (see obs.ContainerLookupTimeouts). DeadlineExceeded rather
		// than any ctx error: a client that hangs up mid-wait cancels, which
		// says nothing about the store, and wait>0 excludes the ?wait=0
		// pollers whose context is already expired on arrival.
		if wait > 0 && errors.Is(ctx.Err(), context.DeadlineExceeded) {
			obs.ContainerLookupTimeouts.Inc()
			// Keyless: container ids churn, so a keyed table would fill with
			// keys that never repeat. The counter carries the rate and the
			// line carries one example to look up.
			if s.warnLookupTimeout.Allow(lookupTimeoutWarnEvery) {
				s.log().Warn("container lookup timed out: the id never appeared in the store within the wait budget",
					"id", id, "wait", wait,
					"note", "the agent retries, and the log lines of that container stay unattributed until it "+
						"resolves. A one-off is normal (the wait covers the gap between a container starting "+
						"and the kubelet posting its id, and a rotated file's id may never return); a sustained "+
						"rate means this replica's pod informer is not seeing those pods. Further reports are "+
						"suppressed for "+lookupTimeoutWarnEvery.String())
			}
		}
		writeError(w, http.StatusNotFound, fmt.Sprintf("container %q not found", id))
		return
	}
	s.enrich(&res.Pod, res.OwnerRefs)
	s.writeCached(w, r, kubemeta.ContainerMetadata{
		ContainerID: res.Container.ID,
		Container:   res.Container,
		Pod:         res.Pod,
	}, false)
}

// lookupTimeoutWarnEvery bounds the container-lookup timeout warning. Like
// every other repeating condition in this file it is a STATE — a pod the
// informer cannot see stays unseen — and the noticing code runs once per
// agent per file per retry, so an unthrottled line is a flood proportional to
// fleet size.
const lookupTimeoutWarnEvery = 5 * time.Minute

// blockedLookupFloor is the elapsed time above which a container lookup is
// reported (at Debug) as having BLOCKED. A warm hit is a read-locked map probe
// — microseconds — so anything past a millisecond either parked on the waiter
// channel or spent time in the readiness park, which are the two transitions
// worth a per-request line. The floor is deliberately generous: a busy
// scheduler can stretch a warm lookup, and a false line here is noise on the
// route with the highest request rate in the process.
const blockedLookupFloor = time.Millisecond

// cachePolicy selects the cache headers a pod response carries.
type cachePolicy int

const (
	// cacheNone sends no freshness lifetime: the pod-IP index exists for
	// IMMEDIACY (IPs recycle; deleted pods drop out at once) and a cached 200
	// would let metaclient re-serve the OLD owner of a recycled IP for up to
	// the TTL. It says so EXPLICITLY (`no-store`) rather than by omission:
	// a response carrying no freshness information may still be stored and
	// heuristically freshened by a shared cache (RFC 9111 4.2.2), which is
	// precisely the staleness this route exists to avoid.
	cacheNone cachePolicy = iota
	// cacheShared is the standard metadata caching: max-age + ETag, so repeat
	// lookups are served locally and revalidate as 304s.
	cacheShared
	// cachePrivate is cacheShared plus `private`, for a response that
	// identifies the CALLER (/v1/self). A per-client cache — metaclient, one
	// per process, always asking about the same pod — may hold it; a SHARED
	// cache must not, or one caller's identity would be handed to the next.
	cachePrivate
)

// noStore is the Cache-Control this policy needs on a response that carries no
// freshness lifetime — the cacheNone 200s and every 404. 404 is one of the
// statuses a cache may store and heuristically freshen when nothing says
// otherwise (RFC 9111 4.2.2 over RFC 9110 15.1), so silence is not the same as
// "do not store": a cached "no live pod with IP x" outlives the pod that took
// the recycled address, and a cached /v1/self answer names whoever asked
// first. cacheShared keeps its historical silence — those 404s are the
// container/pod/uid/node lookups, whose 200s are cached on purpose and whose
// misses are cheap to re-ask.
func (p cachePolicy) noStore() string {
	switch p {
	case cacheNone:
		return "no-store"
	case cachePrivate:
		// The answer identifies the CALLER; a shared cache must not hold it
		// even for the time a heuristic would grant.
		return "private, no-store"
	}
	return ""
}

// servePod is the shared body of the pod endpoints: readiness gate, lookup,
// 404, owner/namespace enrichment, then the write its policy calls for.
// notFound is evaluated lazily so the success path never formats it.
func (s *Server) servePod(w http.ResponseWriter, r *http.Request, policy cachePolicy, lookup func() (store.NodePod, bool), notFound func() string) {
	if !s.requireReady(w, "") {
		return
	}
	np, ok := lookup()
	if !ok {
		// Errors are never cached: a 404 means "not attributable yet", and
		// holding onto it would delay the recovery it is waiting for. Said
		// explicitly where a heuristic could store it anyway — see noStore.
		if cc := policy.noStore(); cc != "" {
			w.Header().Set("Cache-Control", cc)
		}
		writeError(w, http.StatusNotFound, notFound())
		return
	}
	s.enrich(&np.Pod, np.OwnerRefs)
	if policy == cacheNone {
		w.Header().Set("Cache-Control", policy.noStore())
		writeJSON(w, http.StatusOK, np.Pod)
		return
	}
	s.writeCached(w, r, np.Pod, policy == cachePrivate)
}

// handlePod serves GET /v1/pods/{namespace}/{name}: full metadata for one
// pod looked up by name (used by the agent to attribute cadvisor series).
// Deleted pods stay resolvable until their tombstone expires.
func (s *Server) handlePod(w http.ResponseWriter, r *http.Request) {
	namespace, name := r.PathValue("namespace"), r.PathValue("name")
	s.servePod(w, r, cacheShared,
		func() (store.NodePod, bool) { return s.store.GetPodByName(namespace, name) },
		func() string { return fmt.Sprintf("pod %s/%s not found", namespace, name) })
}

// handlePodByUID serves GET /v1/pod-uids/{uid}: full metadata for one pod
// looked up by UID (used by the OTLP ingest enricher to attribute pushed
// telemetry). Deleted pods stay resolvable until their tombstone expires.
func (s *Server) handlePodByUID(w http.ResponseWriter, r *http.Request) {
	uid := r.PathValue("uid")
	s.servePod(w, r, cacheShared,
		func() (store.NodePod, bool) { return s.store.GetPodByUID(uid) },
		func() string { return fmt.Sprintf("pod uid %q not found", uid) })
}

// handlePodByIP serves GET /v1/pod-ips/{ip}: the LIVE pod owning a pod IP
// (the agent's opt-in peer-IP attribution for pushed OTLP). Deleted pods and
// hostNetwork pods never resolve.
func (s *Server) handlePodByIP(w http.ResponseWriter, r *http.Request) {
	ip := r.PathValue("ip")
	s.servePod(w, r, cacheNone, // see cacheNone: recycled IPs need immediacy
		func() (store.NodePod, bool) { return s.store.GetPodByIP(ip) },
		func() string { return fmt.Sprintf("no live pod with IP %q", ip) })
}

// handleSelf serves GET /v1/self: full metadata for the pod the CALLER runs
// in, attributed by the connection's source address. It exists so an agent can
// stamp its own pod's Kubernetes attributes onto the telemetry it generates
// about ITSELF (self-metrics, span metrics) without a downward-API env var
// wired into every deployment that runs the binary.
//
// The address comes from the connection (r.RemoteAddr) and NEVER from a
// header for the LOOKUP: X-Forwarded-For is caller-controlled, and this
// endpoint hands out whatever pod owns the address it is given. It resolves
// through the same live-only pod-IP index as /v1/pod-ips, so a caller on
// hostNetwork (sharing the node IP), one behind SNAT, and one from outside the
// cluster all get a 404 rather than someone else's identity.
//
// WHAT THIS ROUTE CANNOT DO, stated plainly because the answer is an identity:
// a hop that RE-ORIGINATES the request is itself usually a pod, and its address
// is a perfectly good pod IP, so the answer names THE HOP. The connection is
// all there is — nothing distinguishes "this address is my caller" from "this
// address is a proxy in front of my caller" — and the corroboration the agent
// does on its side (selfmeta.verified, pod name against hostname) needs
// something the caller would have to already know, which is the very thing
// this route exists to supply.
//
// So the one hop that CAN be recognised is refused: a request carrying a
// forwarding header (Forwarded, X-Forwarded-For, X-Real-Ip) says, in the hop's
// own words, that the connection is not the caller's. The header is still never
// READ for an address — it selects nothing and names nobody, so no caller can
// use one to be told about somebody else; its mere PRESENCE is the whole
// effect, and the refusal it produces is one a caller can only inflict on
// itself.
//
// THAT COSTS SOMETHING, and it is paid by a deployment that works today: a
// forwarding header is evidence of a HOP, not evidence of address REWRITING,
// and a sidecar in the caller's OWN network namespace — a mesh proxy on the
// agent's pod — appends X-Forwarded-For while leaving the source address
// exactly as it was. Such a caller was answered 200, correctly, and is now
// answered 404. The trade is taken because the two outcomes are not symmetric.
// The 404 is loud and recoverable: the agent falls back to a lookup BY NAME and
// ends up with the SAME pod, at one extra request per -self-attributes-refresh
// and a visible kubescrape_self_metadata_lookups_total{outcome="by_name"}. The
// answer it prevents is silent and permanent: a proxy pod's name, uid and
// namespace stamped on every self-metric the caller ever exports, 200, cached,
// nothing counted anywhere. The fallback is not ASSUMED to cover the sidecar —
// cmd/kubescrape-agent's TestSelfResolveSurvivesASidecarThatAppendsForwardedFor
// drives this handler with that header through a real store and requires the
// agent to come out holding its own identity.
//
// A hop that adds no header remains indistinguishable from the caller, and
// TestASilentReOriginatingHopIsAnsweredWithTheHopsOwnIdentity pins that limit
// rather than leaving it to be rediscovered as a bug.
//
// The response is cached like any other metadata 200, but PRIVATE: it names
// the caller, so only a per-client cache may hold it. That is what lets a
// caller re-read its own pod cheaply — the poll becomes a conditional GET, and
// a 304 says "your labels and namespace metadata are unchanged" — instead of
// choosing between stale attributes and a full document every interval.
func (s *Server) handleSelf(w http.ResponseWriter, r *http.Request) {
	ip := peerip.From(r.RemoteAddr)
	if ip == "" {
		// net/http builds RemoteAddr from the accepted connection, so this is
		// a "cannot happen" branch — which is exactly why it is reported
		// rather than left as a bare 400: reaching it means the listener is
		// not what this code assumes (a custom net.Listener, a Unix socket),
		// and every self-attribution on it fails identically and silently.
		obs.SelfLookupRefused.WithLabelValues("unparseable_peer").Inc()
		if s.warnSelfPeer.Allow(selfWarnEvery) {
			s.log().Warn("/v1/self cannot read the connection's source address, so no caller can be attributed",
				"peer", clipSegment(r.RemoteAddr),
				"note", "further reports are suppressed for "+selfWarnEvery.String())
		}
		writeError(w, http.StatusBadRequest, fmt.Sprintf("unparseable peer address %q", r.RemoteAddr))
		return
	}
	via := forwardedVia(r)
	if via != "" {
		// The refusal is deliberate and it COSTS something (see the doc above:
		// a mesh sidecar in the caller's own network namespace lands here),
		// so it is counted and named. Without this the only trace was a 404 an
		// operator cannot tell from "this agent is on hostNetwork", on a route
		// whose whole job is to hand out an identity.
		obs.SelfLookupRefused.WithLabelValues("forwarded").Inc()
		if s.warnSelfForwarded.Allow(selfWarnEvery) {
			s.log().Warn("/v1/self refused: the request carries a forwarding header, so the connection is a "+
				"hop's and not the caller's",
				"header", via, "peer", ip,
				"note", "the caller falls back to a lookup by name ($POD_NAMESPACE/$POD_NAME), which resolves "+
					"to the same pod at one extra request per -self-attributes-refresh; if a service mesh adds "+
					"the header on the caller's own pod this is the only cost. Further reports are suppressed "+
					"for "+selfWarnEvery.String())
		}
	}
	s.servePod(w, r, cachePrivate,
		func() (store.NodePod, bool) {
			if via != "" {
				return store.NodePod{}, false
			}
			np, ok := s.store.GetPodByIP(ip)
			if !ok {
				// EXPECTED for a hostNetwork agent (it shares the node
				// address) and for one behind SNAT, so it is counted and not
				// logged: the remedy is the by-name fallback the agent
				// already runs, and this rate is how an operator tells that
				// fallback's population from a genuine attribution outage.
				obs.SelfLookupRefused.WithLabelValues("no_pod").Inc()
			}
			return np, ok
		},
		func() string {
			if via != "" {
				return fmt.Sprintf("request carries %s, so this connection belongs to a hop and not to the "+
					"caller; /v1/self can only attribute a direct connection", via)
			}
			return fmt.Sprintf("no live pod with peer IP %q", ip)
		})
}

// selfWarnEvery bounds the /v1/self refusal warnings. Every agent re-reads its
// own pod on -self-attributes-refresh (1m), so both conditions repeat once per
// agent per minute for as long as they last.
const selfWarnEvery = 15 * time.Minute

// forwardedHeaders are the headers a hop sets when it re-originates a request.
// Any one of them present is the hop declaring itself, which is the only
// evidence /v1/self can have that the address it is holding is not its
// caller's.
var forwardedHeaders = []string{"Forwarded", "X-Forwarded-For", "X-Real-Ip"}

// forwardedVia names the forwarding header the request carries, or "".
func forwardedVia(r *http.Request) string {
	for _, h := range forwardedHeaders {
		if r.Header.Get(h) != "" {
			return h
		}
	}
	return ""
}

// handleNodeMetadata serves GET /v1/nodes/{node}/metadata: the node's
// labels and annotations (used by the agent for node-level attributes).
func (s *Server) handleNodeMetadata(w http.ResponseWriter, r *http.Request) {
	if !s.requireReady(w, "") {
		return
	}
	node := r.PathValue("node")
	meta := s.resolver.Node(node)
	if meta == nil {
		writeError(w, http.StatusNotFound, fmt.Sprintf("node %q not found", node))
		return
	}
	s.writeCached(w, r, kubemeta.NodeMetadata{Name: node, ObjectMeta: *meta}, false)
}

// handleNodeTargets serves GET /v1/nodes/{node}/targets.
func (s *Server) handleNodeTargets(w http.ResponseWriter, r *http.Request) {
	if !s.requireReady(w, "") {
		return
	}
	node := r.PathValue("node")
	// A conditional GET answered from the ETag memo never builds the response
	// at all — see nodeTargetsNotModified, including which callers can actually
	// reach it (a DaemonSet agent polling on its scrape interval is not one of
	// them).
	if s.nodeTargetsNotModified(w, r, node) {
		return
	}
	// Sampled before the derivation reads a single source (see targetsValidity).
	valid := s.targetsValidity()
	targets, built := s.nodeTargets(node)
	// Cached like the other metadata 200s (Cache-Control max-age + ETag): every
	// agent re-fetches its target list every cycle, and the response embeds the
	// COMPLETE pod document per target — without revalidation that is the whole
	// node's pod set re-sent 30x/min regardless of change, the one metadata
	// route that had no 304 path. Staleness is bounded by the TTL (default 10s,
	// under the default scrape interval and additive to the agent's own polling
	// lag); the server-side list itself still drops deleted/finished/terminating
	// pods immediately (the invariant is about what a fresh response contains).
	doc := map[string]any{"node": node, "targets": targets}
	if s.cacheTTL <= 0 {
		writeJSON(w, http.StatusOK, doc)
		return
	}
	body, err := json.Marshal(doc)
	if err != nil {
		s.reportEncodeFailure("node targets", err)
		writeError(w, http.StatusInternalServerError, "encoding response")
		return
	}
	etag := entityTag(body)
	// Memoised BEFORE the response is written: the client may revalidate the
	// instant it holds the ETag, and remembering afterwards leaves a window in
	// which the tag this server just handed out is one it cannot recognise.
	if built {
		s.rememberNodeTargets(node, etag, valid)
	}
	s.writeCachedBody(w, r, body, etag, false)
}

// nodeTargets derives the scrape targets of every scrapeable pod on a node,
// deduped and in a deterministic order. built reports whether the node has pods
// at all — the ETag memo is only for nodes that do (see rememberNodeTargets).
func (s *Server) nodeTargets(node string) (targets []kubemeta.ScrapeTarget, built bool) {
	s.targetBuilds.Add(1)
	pods := s.store.PodsOnNode(node)
	targets = make([]kubemeta.ScrapeTarget, 0)
	if len(pods) == 0 { // an empty node cannot match any monitored service
		return targets, false
	}
	monitored := s.monitoredServices()
	// Hoisted out of the per-pod loop: PodMonitors() copies and SORTS the whole
	// monitor list on every call, so a node with 110 pods and 200 monitors did
	// 110 sorts and 22k selector evaluations per request, per agent, per scrape
	// cycle. The sibling ServiceMonitor match is memoised for the same reason.
	allPodMonitors := s.allPodMonitors()
	// ONE snapshot of each namespace's Services for the whole request, NARROWED
	// to the ones that can opt a pod in. The per-pod Matching() call the
	// snapshot replaced took the index RLock and walked every Service in the
	// pod's namespace, so a 1,000-Service namespace cost 110 lock round trips
	// and 110 map walks per request; the narrowing is what takes the remaining
	// per-pod WALK off the same shape (see optInServices).
	optIn := optInServices(s.services.InNamespaces(podNamespaces(pods)), monitored)

	var d targetDedup
	// The monitor endpoints are swept once per matched SERVICE, so a pod behind
	// two Services reaches the merge twice with the same declaration; the merge
	// is a fold and cannot tell (monitorOffers' doc has the whole story).
	var offers monitorOffers
	var matched []*services.Service
	var podMonitors []podMonitorRef
	// enrich is memoised for the request: a node's pods share a handful of
	// owner chains and one or two namespaces (see enrichCache).
	var enr enrichCache
	var evals int64
	for _, np := range pods {
		if !scrape.Scrapeable(np.Pod) {
			continue // finished/deleted pods can never yield targets
		}
		// Cheap pre-check before the (per-pod) enrichment work: does the pod
		// or any service selecting it opt into scraping? matched holds only
		// opt-in Services, so its emptiness IS "no Service opts this pod in" —
		// the separate scan for an annotated-or-monitored member is what the
		// narrowing above replaced.
		inNamespace := optIn[np.Pod.Namespace]
		evals += int64(len(inNamespace))
		matched = matchingServices(inNamespace, np.Pod.Labels, matched[:0])
		podAnnotated := np.Pod.Annotations[scrape.AnnotationScrape] == "true"
		podMonitors = podMonitorsFor(np.Pod, allPodMonitors, podMonitors[:0])
		if !podAnnotated && len(matched) == 0 && len(podMonitors) == 0 {
			continue
		}
		s.enrichCached(&enr, &np.Pod, np.OwnerRefs)

		d.reset(&targets)
		offers.reset()
		for _, t := range scrape.PodTargets(np.Pod) {
			d.add(t)
		}
		for _, svc := range matched {
			for _, t := range scrape.ServiceTargets(np.Pod, svc) {
				d.add(t)
			}
			for _, sme := range monitored[svc.UID] {
				// The URL — a scrape target's identity — resolves without
				// building the target, so a monitor endpoint this pod already
				// holds costs a map lookup instead of a 592-byte target with the
				// whole pod document embedded, a fresh Service view and a copy of
				// the relabeling rules. Sweeping N cluster-wide ServiceMonitors
				// over the same Services, the response is byte-identical at every
				// N while the loop used to build 125 targets per pod to keep 2.
				url, ok := scrape.MonitorTargetURL(np.Pod, svc, *sme.endpoint)
				if !ok {
					// The pod is Scrapeable (checked above) and svc is
					// non-nil, so the ONLY way to get here is the port: the
					// endpoint names one this pod does not declare. That pod
					// is simply absent from the target list — no scrape
					// fails, nothing is logged by the agent, and the series
					// never appear.
					s.reportUnresolvedEndpoint("servicemonitor", sme.monitor, sme.endpoint, &np.Pod)
					continue
				}
				// Two Services selecting one pod offer each of their monitors'
				// endpoints twice; the merge below is a fold and would serve
				// the union doubled and count the conflict twice.
				if !offers.first(url, sme.endpoint) {
					continue
				}
				if held, taken := d.monitorHolder(url); taken {
					// The endpoint's configuration merges into the holder — no
					// target is materialised either way — and only auth/TLS
					// material both monitors declare differently is a loss
					// worth reporting, attributed to the monitor whose
					// material is actually served (which may be a merged
					// contributor's, not the URL holder's).
					rep := scrape.MergeMonitorEndpoint(held, sme.monitor, sme.endpoint)
					d.charge(rep.Bytes)
					if rep.AuthAdopted {
						d.adoptedAuth(url, sme.monitor)
					}
					if rep.AuthConflict {
						s.reportAuthConflict("servicemonitor", d.servingAuth(url, held.Monitor), sme.monitor, url)
					}
					if rep.RelabelCapped {
						s.reportRelabelCapped("servicemonitor", sme.monitor, url)
					}
					if rep.ContributorsCapped {
						s.reportContributorsCapped("servicemonitor", sme.monitor, url,
							d.firstContribCap("servicemonitor", url))
					}
					continue
				}
				for _, t := range scrape.MonitorTargets(np.Pod, svc, sme.monitor, *sme.endpoint) {
					d.add(t)
				}
			}
		}
		for _, pm := range podMonitors {
			for i := range pm.monitor.Endpoints {
				ep := &pm.monitor.Endpoints[i]
				url, ok := scrape.PodMonitorTargetURL(np.Pod, *ep)
				if !ok {
					s.reportUnresolvedEndpoint("podmonitor", pm.name, ep, &np.Pod)
					continue
				}
				if held, taken := d.monitorHolder(url); taken {
					rep := scrape.MergeMonitorEndpoint(held, pm.name, ep)
					d.charge(rep.Bytes)
					if rep.AuthAdopted {
						d.adoptedAuth(url, pm.name)
					}
					if rep.AuthConflict {
						s.reportAuthConflict("podmonitor", d.servingAuth(url, held.Monitor), pm.name, url)
					}
					if rep.RelabelCapped {
						s.reportRelabelCapped("podmonitor", pm.name, url)
					}
					if rep.ContributorsCapped {
						s.reportContributorsCapped("podmonitor", pm.name, url,
							d.firstContribCap("podmonitor", url))
					}
					continue
				}
				for _, t := range scrape.PodMonitorTargets(np.Pod, pm.name, *ep) {
					d.add(t)
				}
			}
		}
		// After every door has offered: the ceiling binds across all of them,
		// so no single door can report it (targetDedup.capped's own comment).
		// Reported HERE and not inside add, so /v1/explain — which derives
		// through the same accumulator — stays read-only, like the two sibling
		// decision signals it suppresses by not calling them.
		if d.capped > 0 {
			s.reportPodCapped(&np.Pod, &d)
		}
	}
	s.svcSelectorEvals.Add(evals)
	// Deterministic order: PodsOnNode iterates a map, and writeCached's ETag is
	// a body hash — an order that shuffled per request would mint a fresh ETag
	// every time and defeat the 304 revalidation entirely. URL first, then
	// monitor/source, then the embedded pod's UID: the UID tiebreak is what
	// makes the order TOTAL in the one case URL+monitor+source cannot separate
	// (two hostNetwork pods sharing the node IP with the same annotated port —
	// identical URL, empty Monitor, Source "pod", but different pod documents).
	//
	// Sorted through an index PERMUTATION rather than by moving the targets
	// themselves. A ScrapeTarget is 616 bytes and embeds the whole pod
	// document by value, so a comparison sort over the elements does its
	// ~n log n swaps as 616-byte typedmemmoves — every one of them a write
	// barrier, taken while this same request is allocating the pod copies that
	// keep the GC marking. Sorting int32 indices does those swaps 8 bytes at a
	// time and applies the result in n element moves, and it drops sort.Slice's
	// reflect swapper (3 allocs) with it.
	sortTargets(targets)
	// Every declaration on the node has been offered, so the served list is
	// final and the identities it exports can be checked against each other.
	// AFTER the sort, so the members of a group are listed in the order the
	// response lists them and one collision reads the same way on every request.
	for _, c := range d.inst.Collisions(targets) {
		s.reportInstanceCollision(c)
	}
	return targets, true
}

// sortTargets orders a node's target list by (URL, Monitor, Source, pod UID)
// — the total order handleNodeTargets' ETag depends on — without ever moving a
// ScrapeTarget through a comparison sort.
//
// The element is 616 bytes and embeds the pod document, so sorting the slice
// directly pays ~n log n write-barriered 616-byte moves; sorting a permutation
// of int32 indices pays them 8 bytes at a time and then applies the answer in
// n moves. The MACHINE-INDEPENDENT half of that, which is what this repo quotes:
// 3 allocations (sort.Slice's reflect swapper) become 1, pinned by
// TestSortTargetsDoesNotAllocateAReflectSwapper. A CPU profile of the route put
// sort.Slice at 9.1% of handleNodeTargets with 7.0 points of that in
// typedmemmove and write-barrier flushing, and an isolated single-run
// comparison at n=110 read 62 µs against 19 µs — indicative only, on a machine
// whose demonstrated benchmark noise floor is far larger than that gap.
//
// slices.SortFunc over the targets THEMSELVES is not the fix and measured worse
// than sort.Slice: its comparator takes the element BY VALUE, so every
// comparison copies 616 bytes.
func sortTargets(targets []kubemeta.ScrapeTarget) {
	if len(targets) < 2 {
		return
	}
	idx := make([]int32, len(targets))
	for i := range idx {
		idx[i] = int32(i)
	}
	slices.SortFunc(idx, func(a, b int32) int {
		// One Compare per field, not an inequality test followed by a Compare:
		// the URLs of two targets usually DIFFER, so the guard form paid the
		// string comparison twice on the field that decides almost every call.
		x, y := &targets[a], &targets[b]
		if c := strings.Compare(x.URL, y.URL); c != 0 {
			return c
		}
		if c := strings.Compare(x.Monitor, y.Monitor); c != 0 {
			return c
		}
		if c := strings.Compare(x.Source, y.Source); c != 0 {
			return c
		}
		return strings.Compare(x.Pod.UID, y.Pod.UID)
	})
	permuteTargets(targets, idx)
}

// permuteTargets rewrites s so that s[i] becomes the element idx[i] named,
// following each cycle of the permutation with one element of scratch. It
// CONSUMES idx (visited slots are marked -1), which is why it is unexported and
// called only from sortTargets.
func permuteTargets(s []kubemeta.ScrapeTarget, idx []int32) {
	for i := range idx {
		if idx[i] < 0 {
			continue // already moved as part of an earlier cycle
		}
		j := int32(i)
		tmp := s[i]
		for {
			k := idx[j]
			idx[j] = -1
			if k == int32(i) {
				s[j] = tmp // the cycle closes on the element we lifted out
				break
			}
			s[j] = s[k]
			j = k
		}
	}
}

// podNamespaces lists the distinct namespaces of a node's pods, for the one
// Services snapshot the whole request works from.
func podNamespaces(pods []store.NodePod) []string {
	seen := make(map[string]struct{}, 8)
	out := make([]string, 0, 8)
	for _, np := range pods {
		if _, dup := seen[np.Pod.Namespace]; dup {
			continue
		}
		seen[np.Pod.Namespace] = struct{}{}
		out = append(out, np.Pod.Namespace)
	}
	return out
}

// optInServices narrows each namespace's Service snapshot to the Services that
// can OPT A POD IN — scrape-annotated, or selected by a ServiceMonitor.
//
// It is the difference between O(pods × services) and O(services) per request.
// The selector scan below runs per pod, and it ran over the namespace's ENTIRE
// Service population before deciding the pod was uninteresting: a node whose
// pods are neither annotated nor selected by anything paid the whole scan on
// every request of every scrape cycle — measured at 110 pods, on the derivation
// alone: 26 µs with no Services in the namespace, 8.0 ms at 1,000, 18.5 ms at
// 2,000 and 38 ms at 4,000, for a response of `{"node":"node1","targets":[]}`.
// A Service population is a namespace-wide property while the pods on a node
// are a sliver of it, so the term that grows is the one that had no business
// being inside the loop.
//
// Narrowing here cannot change what is served, because a Service that opts into
// nothing contributes nothing downstream: scrape.ServiceTargets returns nil
// unless the Service carries prometheus.io/scrape="true", and the monitor sweep
// iterates monitored[svc.UID], which is empty by construction for the rest. It
// cannot reorder anything either — the snapshot is sorted by name and filtering
// preserves order, which is what keeps the encounter order (hence which monitor
// names a merged target, and which Service the dedup's carryForward donates)
// deterministic.
//
// /v1/explain deliberately does NOT narrow: it reports every Service whose
// selector matches, annotated or not, because "this Service selects your pod
// and opts into nothing" is exactly the answer an operator came for. It bounds
// how many it LISTS instead (maxExplainServices, with the remainder counted
// into servicesNotShown) — an unnarrowed walk over a tenant-grown population
// is the right answer; materialising all of it into an unauthenticated
// response is not.
func optInServices(byNamespace map[string][]*services.Service, monitored map[string][]monitorEndpoint) map[string][]*services.Service {
	var out map[string][]*services.Service
	for ns, list := range byNamespace {
		var kept []*services.Service
		for _, svc := range list {
			if svc.Annotations[scrape.AnnotationScrape] == "true" || len(monitored[svc.UID]) > 0 {
				kept = append(kept, svc)
			}
		}
		if len(kept) == 0 {
			// A namespace with no opt-in Service is absent rather than empty:
			// reading a missing key yields the nil slice the loop wants, and
			// most namespaces on most nodes are this case.
			continue
		}
		if out == nil {
			out = make(map[string][]*services.Service, len(byNamespace))
		}
		out[ns] = kept
	}
	return out
}

// matchingServices filters a namespace's Services down to the ones selecting a
// pod, appending into a caller-owned scratch slice (one per request, not one
// per pod). The snapshot is already sorted by name, so the result is too: map
// iteration order must not decide which Service a URL-deduped target is
// attributed to.
func matchingServices(inNamespace []*services.Service, podLabels map[string]string, out []*services.Service) []*services.Service {
	for _, svc := range inNamespace {
		if svc.Selects(podLabels) {
			out = append(out, svc)
		}
	}
	return out
}

// targetDedup collapses the duplicate targets ONE pod can produce, in the order
// they are offered (pod annotations, then per Service the service annotations
// and the ServiceMonitor endpoints, then the PodMonitors). One URL on one pod
// yields exactly one target.
//
//   - The same endpoint reachable via pod and service annotations is ONE target
//     described twice; keep the first (pod source wins), and let a monitor
//     UPGRADE it — a ServiceMonitor/PodMonitor endpoint's bearerTokenSecret,
//     insecureSkipVerify and metricRelabelings live only on the monitor-derived
//     target, and an org-wide `prometheus.io/scrape: "true"` annotation
//     coexisting with prometheus-operator CRDs is common. Dropping the monitor
//     target meant scraping a token-protected endpoint unauthenticated (401 —
//     total metric loss, visible only as up=0) and, worse, exporting the very
//     series a drop rule asked to remove.
//
//   - Two MONITORS resolving to one URL are two independent scrape declarations
//     for a scrape that happens ONCE: prometheus-operator emits a job per
//     (monitor, endpoint) and distinguishes their series by the `job` label,
//     while a kubescrape target's exported identity is
//     (url.full, service.instance.id = host:port, pod, service) with NO monitor
//     component — so serving both would scrape the endpoint twice and export two
//     byte-identical series identities in one payload, which a backend reads as
//     a conflict rather than as two targets (promscrape's fillTargetResource
//     comment describes fixing exactly that, and scheduleKey's says the metadata
//     service dedupes same-URL targets within a pod). The single target instead
//     honours BOTH declarations: the first monitor by encounter order —
//     matched Services sorted by name first, then the monitor index's
//     (namespace, name) order within each Service, ServiceMonitors before
//     PodMonitors, so deterministically — names the target, and every later
//     endpoint MERGES
//     into it (scrape.MergeMonitorEndpoint: relabel chains concatenate, the
//     finer explicit cadence wins with its own timeout, one-sided auth/TLS is
//     adopted; a bare or identical endpoint merges silently). Only auth/TLS
//     material both sides declare DIFFERENTLY cannot be honoured twice by one
//     scrape: the holder's is served and the loser is COUNTED and LOGGED
//     (obs.MonitorTargetShadowed), because a scrape running with a credential
//     one of its CRs did not choose must not be something only a packet
//     capture can reveal.
type targetDedup struct {
	// out is the destination slice, re-pointed per pod (dedup is per pod: two
	// hostNetwork pods legitimately share the node IP and every URL on it).
	out *[]kubemeta.ScrapeTarget
	// urlOwner indexes the entry currently HOLDING a URL — the annotation
	// target, or the monitor one that claimed it.
	urlOwner map[string]int
	// authOwner names the monitor whose auth/TLS a URL's target actually
	// SERVES, when it was ADOPTED from a merged contributor rather than
	// declared by the URL holder itself (MergeMonitorEndpoint's authAdopted).
	// The conflict warning names this monitor as "serving" — naming the
	// holder pointed operators at a monitor with no auth material at all.
	// Lazily allocated: most pods never merge an auth group.
	authOwner map[string]string
	// base is where this pod's targets start in *out — the dedup is per pod
	// while the slice accumulates across the node, and collisions() needs the
	// pod's own window.
	base int
	// inst is the identity scan's scratch, kept here so ONE map serves a whole
	// request: the targets path scans the node's finished list once, explain
	// scans the one pod it derived.
	inst scrape.InstanceScan
	// capped counts THIS pod's targets refused by the per-pod ceiling. The
	// ceiling lives here, at the accumulation seam, rather than at any single
	// door: scrape.MaxPortsPerPod was first applied only where the pod
	// ANNOTATION resolves ports, which left the ServiceMonitor endpoint list —
	// a second door needing no annotation on the pod at all — free to produce
	// unbounded full-pod targets and reopen the O(N²) response it was added to
	// close. Everything that materialises a target for a pod goes through add.
	//
	// It is READ by explainPod, which puts it on the document (cappedTargets)
	// beside the per-entry notes add's verdict feeds: for a long time it was
	// write-only, and the metric's own help text ("see /v1/explain for the
	// pod") sent the operator to a document that reported every refused
	// endpoint as resolving.
	capped int
	// cappedBySize is how many of capped were refused by the BYTE ceiling
	// (scrape.MaxTargetBytesPerPod) rather than by the count one. Both are
	// per-pod ceilings on the same accumulation and both move the same
	// counter, but they are different questions to an operator — "you declared
	// more than 16 ports" versus "your pod document is too big to copy that
	// many times" — and the second one binds at a target count that looks
	// perfectly ordinary. It is what the warning names and what /v1/explain
	// reports beside the total.
	cappedBySize int
	// podBytes is scrape.PodDocBytes for the pod currently being derived,
	// measured ONCE per pod (lazily, off the first target offered) rather than
	// once per target: every target of a pod embeds the same document, and this
	// derivation runs for every pod on the node on every agent poll. The walk
	// is allocation-free and touches the map ENTRIES, not their bytes, so a
	// 200 KiB annotation costs one addition — 0 allocs/op and ~O(labels +
	// annotations + containers) per pod, against the json.Marshal per pod the
	// obvious implementation would pay.
	//
	// Zero means "not measured yet": PodDocBytes has a fixed floor and can
	// never return 0, so the sentinel cannot be confused with an answer.
	podBytes int
	// bytes is what THIS pod's accepted targets have charged against
	// scrape.MaxTargetBytesPerPod. Charged on the NEW-URL arm only, like the
	// count ceiling: a merge adds no target and therefore no copy of the pod
	// document (what it CAN grow — the merged relabel chain and the contributor
	// list — has its own two ceilings, which is why those still earn their
	// keep beside this one).
	bytes int
	// diagnostic marks a derivation run for /v1/explain rather than for a
	// served response: capped still counts (the document names the refusals),
	// but obs.ScrapeTargetsCapped must NOT move. The counter is per-derivation
	// on the targets path, so a browser or a dashboard polling explain would
	// otherwise add a second, human-driven source to a rate an operator reads
	// as "endpoints refused per scrape cycle" — and the counter's own help
	// text points at explain, so the inflation lands exactly on the pods being
	// investigated. The two sibling decision signals on this derivation
	// (obs.TargetIdentityCollisions via reportInstanceCollision, and the
	// auth-conflict warn/obs.MonitorTargetShadowed) are suppressed by explain
	// simply not calling them; this one is inside add, so it needs the flag.
	//
	// Set once by explainPod BEFORE reset and never cleared: it is a property
	// of the derivation, not of the pod being reset onto.
	diagnostic bool
	// contribCapReported holds the (kind, URL) pairs this POD has already
	// reported as contributor-capped, so the LOG side of that report runs once
	// per URL per derivation instead of once per refused monitor.
	//
	// It exists because the report's throttle key had to be BUILT before the
	// throttle could refuse it: `kind + "\x00" + url` is an allocation, and the
	// condition fires once for every monitor past scrape.MaxContributorsPerTarget
	// — so the guard that bounds an abuse allocated in proportion to it
	// (measured +68 allocs/op on a 100-monitor pile-up, exactly N-32, and 68
	// mutex round trips through the dedupe table with them). The counter still
	// moves per refusal; only the line is folded.
	//
	// A struct key rather than a concatenated one: Go hashes a comparable
	// struct without materialising it, so the lookup that decides is free and
	// only the first refusal of a URL pays anything. Lazily allocated — a pod
	// that never fills a contributor list never allocates it — and bounded by
	// the pod's own URL count, which scrape.MaxPortsPerPod already caps.
	contribCapReported map[contribCapKey]struct{}
}

// contribCapKey identifies one contributor-ceiling report within a pod's
// derivation. The kind is part of it because obs.MonitorContributorsCapped is
// labelled by kind and the two monitor kinds reach a shared target
// independently.
type contribCapKey struct{ kind, url string }

// firstContribCap reports whether this pod's derivation has yet logged the
// contributor ceiling for (kind, url), recording it if not.
func (d *targetDedup) firstContribCap(kind, url string) bool {
	k := contribCapKey{kind: kind, url: url}
	if _, dup := d.contribCapReported[k]; dup {
		return false
	}
	if d.contribCapReported == nil {
		d.contribCapReported = make(map[contribCapKey]struct{}, 1)
	}
	d.contribCapReported[k] = struct{}{}
	return true
}

// reset points the dedup at a fresh pod. The maps are reused across pods:
// allocated once per request rather than once per pod.
func (d *targetDedup) reset(out *[]kubemeta.ScrapeTarget) {
	d.out = out
	d.base = len(*out)
	d.capped = 0
	d.cappedBySize = 0
	d.podBytes = 0
	d.bytes = 0
	if d.urlOwner == nil {
		d.urlOwner = make(map[string]int, 4)
	} else {
		clear(d.urlOwner)
	}
	clear(d.authOwner)
	clear(d.contribCapReported)
}

// collisions reports the served targets of THIS pod that the URL dedup keeps
// apart and the exported (job, instance) cannot — same address, different path
// or scheme (scrape/instance.go carries the whole argument, including why they
// are served rather than merged away).
//
// It is the same scan the targets path runs, over the dedup's own window, so
// the two cannot disagree about what a collision IS: explain exists to explain
// what nodeTargets serves, and a second implementation shaped like this one is
// exactly the drift explain_parity_test.go was written after.
//
// The SCOPE does differ, and honestly: this endpoint derives one pod, so a
// collision whose other member is a DIFFERENT pod — two hostNetwork replicas of
// one workload — is invisible here and is reported by the targets path, which
// holds the node's whole list.
func (d *targetDedup) collisions() []scrape.InstanceCollision {
	return d.inst.Collisions((*d.out)[d.base:])
}

// adoptedAuth records/answers who supplies a URL's served auth (see authOwner).
func (d *targetDedup) adoptedAuth(url, monitor string) {
	if d.authOwner == nil {
		d.authOwner = make(map[string]string, 1)
	}
	d.authOwner[url] = monitor
}

func (d *targetDedup) servingAuth(url, holder string) string {
	if m, ok := d.authOwner[url]; ok {
		return m
	}
	return holder
}

// monitorHolder returns the MONITOR-derived target already holding a URL, when
// there is one: an incoming monitor endpoint for that URL merges into it
// instead of being materialised. It is the cheap check the caller makes BEFORE
// building anything: the answer needs the URL and nothing else, and
// MergeMonitorEndpoint's own bare-endpoint gate keeps the common shape — N
// cluster-wide monitors declaring nothing beyond the scrape itself — at a map
// lookup instead of a 592-byte target with the whole pod document embedded.
//
// A URL held by an ANNOTATION target is deliberately not a hit — the monitor
// target upgrades it wholesale (see add), and that upgrade is why the caller
// must still build one.
func (d *targetDedup) monitorHolder(url string) (*kubemeta.ScrapeTarget, bool) {
	i, taken := d.urlOwner[url]
	if !taken {
		return nil, false
	}
	// By POINTER: a ScrapeTarget is 592 bytes with the pod document embedded,
	// and this runs per (pod, monitor endpoint). The pointer is into d.out and
	// dies before the next append can reallocate it.
	held := &(*d.out)[i]
	if !configuredTarget(held) {
		return nil, false
	}
	return held, true
}

// targetVerdict is add's answer: accepted, or WHICH of the two per-pod ceilings
// refused it. A bool cannot carry the second half, and the second half is the
// whole difference between "you declared more ports than the ceiling admits"
// and "your pod document is too large to copy this many times" — two different
// remedies, reported to the operator through two different wordings
// (scrape.CeilingNote and scrape.SizeCeilingNote).
type targetVerdict int

const (
	targetAccepted targetVerdict = iota
	targetRefusedCount
	targetRefusedBytes
)

// ok reports whether the target was accepted into the served list.
func (v targetVerdict) ok() bool { return v == targetAccepted }

// note is the /v1/explain wording for this verdict against subject ("this
// endpoint", "port 8080, 8081"), empty when the target was accepted. The
// wordings are internal/scrape's — the one spelling, beside the ceilings
// themselves.
func (v targetVerdict) note(subject string, podBytes int) string {
	switch v {
	case targetRefusedCount:
		return scrape.CeilingNote(subject)
	case targetRefusedBytes:
		return scrape.SizeCeilingNote(subject, podBytes)
	}
	return ""
}

// add offers a target to the accumulator, reporting whether it was ACCEPTED and
// — when it was not — which ceiling refused it, so this pod will not be scraped
// on that URL. The verdict is returned rather than merely counted because the
// ceilings bind HERE and nowhere else: with two doors contributing, each
// individually under both, no door can tell which of its own targets survived,
// so /v1/explain can only name a refused port or endpoint by asking the
// accumulator. The served path ignores the result (it reports the refusal
// through obs.ScrapeTargetsCapped and reportPodCapped).
func (d *targetDedup) add(t kubemeta.ScrapeTarget) targetVerdict {
	i, taken := d.urlOwner[t.URL]
	if !taken {
		// Both per-pod ceilings, applied on the NEW-URL arm only: a target that
		// merges into a URL this pod already holds costs no extra response
		// bytes, so it must not be refused (16 entries collapsing to 3 URLs
		// legitimately yields 3). Refusing here loses that endpoint — which is
		// why it is counted and reported by /v1/explain rather than silent.
		if len(*d.out)-d.base >= scrape.MaxPortsPerPod {
			return d.refuse(targetRefusedCount)
		}
		// Measured once per pod, off the first target offered: every target of
		// this pod embeds the same document (see podBytes).
		if d.podBytes == 0 {
			d.podBytes = scrape.PodDocBytes(&t.Pod)
		}
		// The pod's FIRST target is unconditional. The byte budget exists to
		// bound the MULTIPLIER — N copies of one document — and a pod whose
		// annotations are large but honest must still be scraped somewhere,
		// or the ceiling silently stops collecting from a workload that did
		// nothing wrong. Its cost is still CHARGED, so the second target is
		// measured against the truth.
		first := len(*d.out) == d.base
		// The pod document is the FLOOR of any target's cost, so once it alone
		// no longer fits, nothing offered afterwards can: refuse without
		// walking the target's own fields. That is the arm a pile-up of
		// distinct URLs runs down, and the cost of refusing must not scale
		// with what is being refused.
		if !first && d.bytes+d.podBytes > scrape.MaxTargetBytesPerPod {
			return d.refuse(targetRefusedBytes)
		}
		cost := scrape.TargetDocBytes(&t, d.podBytes)
		// And the whole document, for the target whose OWN fields are what
		// overflow: a 2 KiB path beside a 16 KiB merged chain is inside every
		// per-field ceiling and still 18 KiB per target.
		if !first && d.bytes+cost > scrape.MaxTargetBytesPerPod {
			return d.refuse(targetRefusedBytes)
		}
		d.bytes += cost
		d.urlOwner[t.URL] = len(*d.out)
		*d.out = append(*d.out, t)
		return targetAccepted
	}
	// pod source wins over service source, and a monitor wins over both.
	held := &(*d.out)[i]
	if configuredTarget(&t) && !configuredTarget(held) {
		carryForward(&t, held)
		*held = t
		return targetAccepted
	}
	// The holder keeps the URL — and carries forward from the target it
	// displaces, on this path too. The preference is about which DECLARATION
	// wins, never about discarding a view of the endpoint the winner has no way
	// to hold: a pod-annotation target arrives first and carries no Service, so
	// the service-annotation target for the same URL used to be dropped whole
	// and every sample of that endpoint lost k8s.service.name and
	// k8s.service.uid — the identical loss carryForward was written for on the
	// replace path, on the arm nobody had looked at. Which Service donates is
	// deterministic: matchingServices preserves the snapshot's name order.
	carryForward(held, &t)
	return targetAccepted
}

// charge spends n bytes of THIS pod's byte budget on a target the accumulator
// already holds — the MERGE arm, where nothing is appended and the count
// ceiling has nothing to look at.
//
// The budget's claim is that it charges the whole target document, and until
// this existed that claim stopped at the merge: a target N monitors fold into
// grows by its merged relabel chain and its contributor list, both bounded but
// both on TOP of the budget (see scrape.MergeReport.Bytes for the size). It
// refuses nothing — a refused merge would drop relabel rules a monitor asked
// for, i.e. change what is EXPORTED in order to bound a response — it only
// makes the pod's remaining budget honest, so the next NEW url is measured
// against what is really being served.
func (d *targetDedup) charge(n int) { d.bytes += n }

// refuse records one refusal by ceiling v and returns it. Both ceilings move
// the SAME counter — a refused target is a refused target, and the rate an
// operator alerts on is "endpoints this node is not scraping" — while
// cappedBySize keeps the two apart for the warning and for /v1/explain, which
// have to name the remedy.
func (d *targetDedup) refuse(v targetVerdict) targetVerdict {
	d.capped++
	if v == targetRefusedBytes {
		d.cappedBySize++
	}
	if !d.diagnostic {
		// See diagnostic: explain derives through this same seam, and a
		// read-only diagnostic must not move a decision counter.
		obs.ScrapeTargetsCapped.Inc()
	}
	return v
}

// carryForward moves the fields the losing target had and the winner lacks.
//
// Today that is exactly the Service: a PODMONITOR selects pods directly and its
// targets carry none, so replacing a service-annotation target with one wholesale
// stripped k8s.service.name and k8s.service.uid from every sample of that
// endpoint (promscrape's fillTargetResource reads target.Service). The
// preference's own justification — that the winner carries strictly MORE — is
// true of the ServiceMonitor arm, which sets Service, and false both of the
// PodMonitor one and of a pod-annotation target displacing a service-annotation
// one, which is why add calls this on both of its paths.
func carryForward(winner, loser *kubemeta.ScrapeTarget) {
	if winner.Service == nil {
		winner.Service = loser.Service
	}
}

// unresolvedWarnEvery and maxUnresolvedWarnKeys bound the unresolved-endpoint
// warning. Keys are (kind, monitor) — cluster OBJECTS — so the live set is
// bounded by the monitor CRs however many pods they select, however often
// those pods are replaced, and however many endpoints each CR declares.
//
// The port SPELLING used to be the third part of the key, and it is content
// rather than identity: one ServiceMonitor may declare as many endpoints as
// fit in an etcd object, each naming a distinct port string of a length its
// author picks. That mints keys — arbitrarily many, arbitrarily long — in a
// bounded table that SUPPRESSES on saturation (internal/logdedupe's rule), so
// anyone able to write one monitor in one namespace could stop this warning
// from ever reporting anyone else's broken endpoint. The port still rides the
// LINE, clipped: the same trade internal/agent/promscrape's warnOnce made when
// it took a monitor's duration value out of its key. The cost is that a
// monitor with several unresolved endpoints reports one of them per window
// instead of each — the line names which, and the remedy is the same CR.
const (
	unresolvedWarnEvery   = 30 * time.Minute
	maxUnresolvedWarnKeys = 1024
)

// maxLoggedPortBytes bounds the port spelling on the line. A CR field is
// bounded only by the object's own size, and a megabyte of it in a log record
// is a second flood in the shape of one line. The sibling constant is
// internal/agent/promscrape's maxLoggedValueBytes, which bounds the same class
// of value for the same reason.
const maxLoggedPortBytes = 96

// clipPort renders a CR-supplied port spelling for a log attribute: bounded,
// and cut on a rune boundary so a clipped UTF-8 sequence does not reach the
// log pipeline as a replacement character.
func clipPort(v string) string {
	if len(v) <= maxLoggedPortBytes {
		return v
	}
	cut := maxLoggedPortBytes
	for cut > 0 && !utf8.RuneStart(v[cut]) {
		cut--
	}
	return v[:cut] + "…"
}

// reportUnresolvedEndpoint warns that a monitor endpoint names a port the pod
// its selector matched does not declare, so that pod produces no target for it.
//
// This is the commonest ServiceMonitor mistake there is — a `port:` naming the
// Service port that does not exist, or a container port that was renamed — and
// it was the least visible outcome on the whole derivation: prometheus-operator
// emits no scrape config for such an endpoint either, so there is no failing
// scrape, no up=0, and no counter anywhere. The pod is just missing, which
// looks exactly like a pod nobody asked to scrape. /v1/explain says so per pod,
// but only to someone who already suspects this pod.
//
// An endpoint naming NEITHER port nor targetPort is deliberately skipped: that
// one already rides Endpoint.Ignored ("port(unset)"), which the metadata
// service logs once per changed monitor and counts as
// kubescrape_monitor_fields_ignored_total. Reporting it here too would say the
// same thing per pod per cycle.
func (s *Server) reportUnresolvedEndpoint(kind, monitor string, ep *servicemonitors.Endpoint, pod *kubemeta.Pod) {
	if ep.Port == "" && ep.TargetPort == nil {
		return
	}
	port := endpointPortSpelling(ep)
	// IDENTITY only: see maxUnresolvedWarnKeys for why the port spelling is on
	// the line but not in the key.
	allow, saturated := s.warnUnresolved.Allow(kind + "\x00" + monitor)
	if saturated {
		s.log().Warn("unresolved-endpoint warning dedupe table is full; further distinct endpoints are suppressed",
			"keys", maxUnresolvedWarnKeys)
	}
	if !allow {
		return
	}
	s.log().Warn("monitor endpoint names a port the selected pod does not declare, so that pod yields no scrape target",
		"kind", kind, "monitor", monitor, "port", clipPort(port),
		"namespace", pod.Namespace, "pod", pod.Name,
		"note", "the pod is simply absent from the target list — no scrape fails and nothing is exported for it. "+
			"GET /v1/explain/"+pod.Namespace+"/"+pod.Name+" lists the pod's declared ports beside this verdict. "+
			"A ServiceMonitor `port` names a SERVICE port (resolved to a pod port through its targetPort); a "+
			"PodMonitor `port` names a CONTAINER port. Further warnings for this endpoint are suppressed for "+
			unresolvedWarnEvery.String())
}

// endpointPortSpelling renders the port an endpoint names, the way its CR
// spells it, so the warning and the CR can be matched up by eye.
func endpointPortSpelling(ep *servicemonitors.Endpoint) string {
	if ep.Port != "" {
		return ep.Port
	}
	if ep.TargetPort != nil {
		return "targetPort:" + ep.TargetPort.String()
	}
	return ""
}

// cappedWarnEvery and maxCappedWarnKeys bound the per-pod ceiling warning.
//
// The refusal is re-derived on every targets request of every agent whose node
// holds the pod, so this is a STEADY state exactly like the collision and
// shadowed-monitor warnings beside it, and the same throttle applies. Thirty
// minutes because the remedy is a configuration change, and nothing about the
// condition changes in between.
const (
	cappedWarnEvery   = 30 * time.Minute
	maxCappedWarnKeys = 1024
)

// cappedWarnKey identifies the ceiling refusal by the WORKLOAD, not by the pod:
// every replica of a Deployment carries the same annotations and the same
// monitors select all of them, so keying on the pod name would emit one line
// per replica and another full set on every rollout — the mistake this file
// records three times already (warnTarget, reportAuthConflict, collisionWarnKey).
// attrs.ServiceName resolves the workload owner (the Deployment, not the
// per-revision ReplicaSet), so the key survives a rollout.
func cappedWarnKey(pod *kubemeta.Pod) string {
	return pod.Namespace + "\x00" + attrs.ServiceName(*pod)
}

// reportPodCapped warns that ONE pod produced more scrape targets than the
// per-pod ceiling admits, so some of its endpoints are not scraped at all.
//
// obs.ScrapeTargetsCapped already carries the rate, and its help sends the
// operator to /v1/explain — which needs a pod NAME the counter cannot supply.
// A refused endpoint is indistinguishable from one that was never configured:
// the target simply is not in the list, no scrape fails, and the series it
// would have produced never appear. This line is the only thing that names the
// pod to look at.
func (s *Server) reportPodCapped(pod *kubemeta.Pod, d *targetDedup) {
	allow, saturated := s.warnPodCapped.Allow(cappedWarnKey(pod))
	if saturated {
		s.log().Warn("per-pod target-ceiling warning dedupe table is full; further distinct workloads are suppressed",
			"keys", maxCappedWarnKeys)
	}
	if !allow {
		return
	}
	// Both ceilings on one line, because a reader has to be able to tell which
	// one bound: "dropped=14 limit=16" against a pod serving TWO targets is a
	// wild-goose chase, and the byte ceiling binds at a target count that looks
	// entirely ordinary. podBytes is the pod document's measured size — a
	// number derived from the pod's annotations, never any of their bytes.
	s.log().Warn("pod produced more scrape targets than the per-pod ceilings admit; the excess endpoints are NOT scraped",
		"namespace", pod.Namespace, "pod", pod.Name, "dropped", d.capped,
		"droppedBySize", d.cappedBySize, "limit", scrape.MaxPortsPerPod,
		"podBytes", d.podBytes, "byteLimit", scrape.MaxTargetBytesPerPod,
		"note", "GET /v1/explain/"+pod.Namespace+"/"+pod.Name+" names the refused ports and endpoints; the "+
			"ceilings are per POD across every door (pod and Service annotations, ServiceMonitor and PodMonitor "+
			"endpoints), and they exist because every target embeds the whole pod document — so a pod contributes "+
			"at most 16 targets AND at most 256 KiB of them, whichever binds first. droppedBySize is how many the "+
			"BYTE ceiling refused: those are answered by shrinking the pod's labels and annotations (podBytes "+
			"measures them) or by splitting its ports across workloads, not by declaring fewer ports. The first "+
			"target of a pod is always served. Further warnings for "+
			"this workload are suppressed for "+cappedWarnEvery.String())
}

// collisionWarnEvery bounds how often ONE colliding configuration may log. Like
// the shadowed-monitor warning this is a STEADY state rather than an event: the
// collision is re-derived on every targets request of every agent whose node
// holds one of the pods, so an unthrottled line is a permanent flood
// proportional to fleet size.
const collisionWarnEvery = 30 * time.Minute

// maxCollisionWarnKeys bounds the throttle table. Every component of a key is
// something an operator WROTE (see collisionWarnKey), so the live set is
// bounded by the cluster's configuration: a workload contributes one key per
// annotated port per set of colliding declarations, whatever its replica count,
// however often its pods are replaced and however many nodes it runs on. This
// is belt and braces against a cluster that churns those declarations.
const maxCollisionWarnKeys = 1024

// collisionWarnKey identifies a collision by the CONFIGURATION that produced
// it, never by the pods it currently lands on — the mistake this repo has
// already recorded twice (the agent's warnTarget: "The URL embeds the pod IP,
// so keying on it was wrong twice over: the table grew one entry per pod
// incarnation for the process' whole life, and the warning re-fired on every
// pod restart"; reportAuthConflict keys on the monitor PAIR because pairs are
// bounded by the indexed monitors).
//
// Keying on the pod and its address was the same mistake a third time: measured
// at 20 replicas of one misconfigured Deployment, 20 WARN lines — and 20 more
// the moment a rollout gave them new names and new IPs, with the table at 40.
// The throttle's own doc says it exists because an unthrottled line is a flood
// proportional to fleet size, which is what that key left it as.
//
// What an operator has to fix is a workload's annotation, a Service's or a
// monitor CR's, so the key is what those say: the JOB (namespace plus the
// workload owner's name, which outlives every pod under it — attrs.ServiceName
// takes the Deployment, not the per-revision ReplicaSet, so a rollout keeps the
// key), the PORT, and each declaration's scheme, path, source and monitor. The
// pod IP is the one part of the address that is an incarnation, and it is
// exactly what is cut out.
func collisionWarnKey(c scrape.InstanceCollision) string {
	var b strings.Builder
	b.WriteString(c.Job)
	b.WriteByte(0)
	b.WriteString(portOf(c.Address))
	for _, ct := range c.Targets {
		scheme, path := splitAtAddress(ct.URL, c.Address)
		for _, part := range [...]string{scheme, path, ct.Source, ct.Monitor} {
			b.WriteByte(0)
			b.WriteString(part)
		}
	}
	return b.String()
}

// portOf is the port half of a host:port. The host half is a pod IP, which is
// an incarnation rather than configuration; the port was annotated.
func portOf(address string) string {
	if i := strings.LastIndexByte(address, ':'); i >= 0 {
		return address[i+1:]
	}
	return ""
}

// splitAtAddress cuts a member's URL around the address the whole group shares,
// leaving its two CONFIGURED halves — the scheme prefix and the path. They are
// substrings, so this allocates nothing. A URL not containing the address is
// unreachable for a derived target (the address is what built it); it degrades
// to keying on the whole URL rather than dropping the member from the key.
func splitAtAddress(url, address string) (scheme, path string) {
	i := strings.Index(url, address)
	if i < 0 {
		return url, ""
	}
	return url[:i], url[i+len(address):]
}

// reportInstanceCollision warns that two served targets collapse onto one
// exported series identity. BOTH are still served — scrape/instance.go carries
// the argument for that — so this warning is the operator's only notice that
// the two are about to overwrite each other.
//
// Every other symptom is anonymous: `up` alternating 0 and 1 at one timestamp,
// a shared counter alternating between two endpoints' values so rate() reads
// resets, or a backend's duplicate-sample rejection — none of which names the
// pods, and none of which names the two declarations.
func (s *Server) reportInstanceCollision(c scrape.InstanceCollision) {
	// Counted BEFORE the throttle, like MonitorTargetShadowed: the log line is
	// suppressed for collisionWarnEvery per configuration, so a counter behind
	// the throttle would move once every half hour and there would be nothing
	// an operator could alert on between. The price is that the rate tracks
	// how often the node's target list is rebuilt rather than how broken the
	// configuration is, which the metric's own help says in as many words.
	obs.TargetIdentityCollisions.Inc()
	allow, saturated := s.warnCollide.Allow(collisionWarnKey(c))
	if saturated {
		s.log().Warn("colliding-target warning dedupe table is full; further distinct collisions are suppressed",
			"keys", maxCollisionWarnKeys)
	}
	if !allow {
		return
	}
	// Each member names its pod: a group can span pods (two hostNetwork
	// replicas of one workload), and those members agree on everything else.
	//
	// BOUNDED, and the bound is on the log line rather than on the group: a
	// collision group is every target on the node sharing one (job, instance),
	// which on a hostNetwork workload is one member per replica per port —
	// ~1,760 on a full node, each ~100 bytes, in ONE record. Two examples name
	// the configuration (a collision needs two), the count says how big it
	// really is, and the pair the operator fixes is in the same place either
	// way. Same lesson as the ceilings this campaign added: a throttle bounds
	// how OFTEN a line is written, never how LARGE it is.
	const maxMembers = 2
	members := make([]string, 0, min(len(c.Targets), maxMembers))
	for _, ct := range c.Targets[:min(len(c.Targets), maxMembers)] {
		who := ct.Source
		if ct.Monitor != "" {
			who += " " + ct.Monitor
		}
		members = append(members, ct.URL+" ["+who+" on "+ct.Pod+"]")
	}
	if over := len(c.Targets) - len(members); over > 0 {
		members = append(members, "and "+strconv.Itoa(over)+" more targets on this identity")
	}
	s.log().Warn("two scrape targets export the same series identity; both are scraped and their samples collide",
		"job", c.Job, "instance", c.Address, "targets", strings.Join(members, ", "),
		"note", "the exported identity is the pod's workload plus host:port and does NOT include the path, so up, "+
			"scrape_duration_seconds and scrape_samples_scraped arrive once per target with one identity and one "+
			"timestamp, and any metric name the endpoints share becomes one series carrying both their values; "+
			"give the endpoints separate container ports, or drop one declaration — and where the two targets name "+
			"different pods, the two hostNetwork pods are annotated for one port on one node. Further warnings for "+
			"this configuration are suppressed for "+collisionWarnEvery.String())
}

// configuredTarget reports whether t carries endpoint configuration that only
// a ServiceMonitor/PodMonitor can supply. Losing any of it to URL dedup changes
// what is scraped or what is exported: without the bearer token the scrape 401s
// (total metric loss for the target), without insecureSkipVerify an https
// endpoint with a private CA fails, and without the metricRelabelings the
// series a drop rule targets are exported anyway.
func configuredTarget(t *kubemeta.ScrapeTarget) bool {
	// Monitor != "" is the test, NOT a list of the config fields that happen to
	// exist today: an enumeration silently goes stale every time an endpoint
	// field is interpreted (it already had, for basicAuth, authorization, the
	// tlsConfig material and interval/scrapeTimeout — each of which was being
	// dropped by the dedup this function exists to prevent). Only monitor-derived
	// targets carry endpoint configuration at all, so the source IS the signal.
	return t.Monitor != ""
}

// handleScrapeAuth serves GET /v1/scrape-auth/{namespace}/{name}/{key}: the
// bearer token a monitor endpoint's bearerTokenSecret references. Disabled
// (404) unless the service runs with -scrape-auth-secrets. Responses are
// never cacheable — a rotated token must not be re-served from a cache.
//
// This is the only AUTHENTICATED route: it is the only one serving Secret
// material, and the service holds cluster-wide `secrets: get`. Clients send
// the shared token from -scrape-auth-token-file as
// `Authorization: Bearer <token>`; anything else is a 401 (see auth.go).
func (s *Server) handleScrapeAuth(w http.ResponseWriter, r *http.Request) {
	// Set once, at the top, so EVERY exit inherits it. The 404s here are
	// heuristically storable (RFC 9111 4.2.2 over RFC 9110 15.1), and this
	// route's whole point is that a rotated or newly-granted credential takes
	// effect now — a cached "secret not found" from the startup window, or from
	// before an RBAC fix, outlives the condition that caused it and shows up
	// only as up=0. cachePolicy.noStore says the same thing for the pod routes;
	// this one was the exception.
	w.Header().Set("Cache-Control", "no-store")
	if s.secrets == nil {
		// The feature is off, so there is nothing to protect; keep the
		// pre-existing "not enabled" 404 rather than a misleading 401.
		//
		// Counted and warned, because this is a two-sided configuration
		// mismatch that nothing else reports: an agent only asks because a
		// monitor endpoint THIS SERVICE served it names a credential, so every
		// such scrape is about to run without one and sit at up=0 — and on the
		// agent the 404 is indistinguishable from "that ref does not exist".
		// The ref is not logged: it has not been through IsPathSegmentName yet
		// and the condition is a property of this process, not of the request.
		obs.ScrapeAuthFailures.WithLabelValues("disabled").Inc()
		if s.warnAuthOff.Allow(scrapeAuthWarnEvery) {
			s.log().Warn("a scrape-auth credential was requested but this service does not serve them; "+
				"every monitor endpoint declaring auth or TLS material will be scraped without it",
				"flag", "-scrape-auth-secrets",
				"note", "the agents were served monitor targets carrying secret refs, so enable "+
					"-scrape-auth-secrets (plus its secrets RBAC and -scrape-auth-token-file) or remove the "+
					"auth/TLS clauses from those monitors; further reports are suppressed for "+
					scrapeAuthWarnEvery.String())
		}
		writeError(w, http.StatusNotFound, "scrape auth secrets are not enabled (-scrape-auth-secrets)")
		return
	}
	// Authenticate BEFORE any lookup: an unauthenticated client must not be
	// able to probe which secret refs a monitor names (403 vs 404) either.
	if !s.authorizedForScrapeAuth(r) {
		// NEITHER the presented token NOR the Authorization header is ever
		// logged; what an operator has to fix is one agent's
		// -scrape-auth-token-file, so the line carries the peer address (the
		// only thing on the request that names that agent) and whether a
		// credential was presented at all. Those two cases have different
		// remedies: `missing` is an agent that was never given the flag,
		// `mismatch` is a token file that does not match this service's — the
		// shape a rotation gets wrong, which the service's own 5-minute grace
		// window is meant to cover.
		credential := "missing"
		if r.Header.Get("Authorization") != "" {
			credential = "mismatch"
		}
		obs.ScrapeAuthFailures.WithLabelValues("unauthorized").Inc()
		// Keyless: the condition is one misconfiguration, and a per-peer table
		// would be keyed by something that grows with the fleet. The counter
		// carries the rate; the line only has to name one example.
		if s.warnAuthToken.Allow(scrapeAuthWarnEvery) {
			s.log().Warn("scrape-auth request rejected: the caller did not present an accepted bearer token",
				"peer", peerip.From(r.RemoteAddr), "credential", credential,
				"tokenFile", "-scrape-auth-token-file",
				"note", "the agent's -scrape-auth-token-file must hold the same token as this service's; "+
					"a rotation is covered for five minutes on both sides, so a persistent rate is a "+
					"mismatch and not a rotation. Further reports are suppressed for "+scrapeAuthWarnEvery.String())
		}
		writeUnauthorized(w)
		return
	}
	ns, name, key := r.PathValue("namespace"), r.PathValue("name"), r.PathValue("key")
	// Scope to secrets a monitor endpoint actually references — the endpoint
	// must not become a read-any-cluster-secret oracle for anything that can
	// reach the (unauthenticated, cluster-internal) service.
	// The readiness gate sits AFTER the disabled-feature 404 and the anonymous
	// 401 (so neither changes), and BEFORE the allowlist — this is the route
	// whose missing gate is requireReady's war story. Retry-After: 1 because
	// unlike the metadata routes this 503 replaced a DEFINITIVE-looking 403,
	// so it says out loud when to come back.
	if !s.requireReady(w, "1") {
		return
	}
	if s.monitors == nil {
		// -scrape-auth-secrets without -servicemonitors: the allowlist that
		// bounds this route is built from indexed monitors, so nothing can
		// ever be served and every credential-bearing scrape 401s. Same shape
		// as the disabled case above, and just as silent before this.
		obs.ScrapeAuthFailures.WithLabelValues("no_monitors").Inc()
		if s.warnAuthNoMonitors.Allow(scrapeAuthWarnEvery) {
			s.log().Warn("a scrape-auth credential was requested but no monitors are indexed, so no secret ref "+
				"can be allowlisted",
				"flag", "-servicemonitors",
				"note", "-scrape-auth-secrets serves only Secret keys an indexed ServiceMonitor/PodMonitor "+
					"endpoint references; further reports are suppressed for "+scrapeAuthWarnEvery.String())
		}
		writeError(w, http.StatusNotFound, "no monitors indexed")
		return
	}
	// The allowlist key is a flat "ns/name/key" join checked against three
	// SEPARATELY-CHOSEN path segments, and Go's ServeMux unescapes %2F inside a
	// single wildcard segment — so without this, three segments could be
	// re-cut: GET /v1/scrape-auth/tenant%2Fvictim/creds/token matches the entry
	// a monitor in namespace `tenant` mints for a bearerTokenSecret named
	// "victim/creds", and reaches SecretReader.Get with namespace
	// "tenant/victim". The shipped client-go reader rejects that namespace
	// before it sends anything, but Secrets is a pluggable interface and a
	// reader implemented over a lister keyed by "ns/name" — the obvious
	// optimisation for a per-scrape-cycle path — would perform the read.
	//
	// So both ends refuse the ambiguity: servicemonitors' secretRef.ref
	// declines to MINT such an entry, and this declines to match one. The check
	// is the API server's own for a name used as a path segment (content.
	// IsPathSegmentName, which validation/path now merely aliases): "/", "%",
	// "." and ".." are exactly what re-cutting needs.
	for what, v := range map[string]string{"namespace": ns, "name": name, "key": key} {
		if errs := content.IsPathSegmentName(v); len(errs) > 0 {
			// Counted with the other refusals: no agent this repo ships can
			// produce one (the ref comes from a monitor CR, whose fields are
			// already Kubernetes names), so a rate here is either a hand-built
			// request or the re-cutting attack this check exists for. The
			// value is CLIPPED before it reaches the log — it is a raw path
			// segment, bounded only by the header limit.
			obs.ScrapeAuthFailures.WithLabelValues("bad_request").Inc()
			if s.warnAuthSegment.Allow(scrapeAuthWarnEvery) {
				s.log().Warn("scrape-auth request rejected: a path segment cannot name a Kubernetes object",
					"segment", what, "value", clipSegment(v), "error", errs[0],
					"note", "further reports are suppressed for "+scrapeAuthWarnEvery.String())
			}
			writeError(w, http.StatusBadRequest,
				fmt.Sprintf("invalid %s %q: %s", what, v, errs[0]))
			return
		}
	}
	if ref := ns + "/" + name + "/" + key; !s.monitors.AuthSecretRefs().Has(ref) {
		// The allowlist is derived from the INDEXED monitors, so a miss is
		// usually not a hostile probe but the index disagreeing with what the
		// agent was served: the monitor failed to parse (and was therefore
		// DELETED from the index, dropping its targets with it), its namespace
		// is outside -monitor-namespaces, or the ref really is a typo. All
		// three end the same way — the scrape runs unauthenticated — and none
		// of them was visible here.
		//
		// Per-ref, because two broken credentials must not mask each other —
		// but through the miss's OWN table, and under a CLIPPED key. These
		// three segments are the caller's, not the operators' configuration:
		// they have passed IsPathSegmentName by now and nothing bounds their
		// length or their number, so keying the shared warnRefs table by them
		// handed anyone holding the scrape-auth token a way to saturate it and
		// suppress the RBAC-failure and non-UTF-8 warnings for every real ref
		// (see Server.warnAuthDenied).
		obs.ScrapeAuthFailures.WithLabelValues("not_allowed").Inc()
		allow, saturated := s.warnAuthDenied.Allow(
			clipSegment(ns) + "\x00" + clipSegment(name) + "\x00" + clipSegment(key))
		if saturated {
			s.log().Warn("scrape-auth allowlist-miss warning table is full; further distinct refs are "+
				"suppressed (the rate stays on kubescrape_scrape_auth_failures_total)",
				"refs", maxScrapeAuthDeniedRefs)
		}
		if allow {
			s.log().Warn("scrape-auth request refused: no indexed monitor endpoint references this secret key",
				"namespace", clipSegment(ns), "name", clipSegment(name), "key", clipSegment(key),
				"note", "check that the monitor naming it parsed (kubescrape_monitor_parse_errors_total) and "+
					"that its namespace is permitted by -monitor-namespaces; the target is scraped without the "+
					"credential meanwhile. Further failures for this ref are suppressed for "+
					scrapeAuthWarnEvery.String())
		}
		writeError(w, http.StatusForbidden, "secret is not referenced by any monitor endpoint")
		return
	}
	val, err := s.secrets.Get(r.Context(), ns, name, key)
	if err != nil {
		// Classify. This is the one route that hard-fails on EXTERNAL state, so
		// collapsing every cause into 404 made an RBAC denial — the likeliest
		// real failure, since -scrape-auth-secrets needs a `secrets get` grant
		// the operator adds by hand — read as "no such secret", and put a
		// permissions bug into the metadata_requests_total{outcome="not_found"}
		// stream that obs.go documents as the container-attribution signal.
		//
		// A missing key or a genuinely absent Secret is the client's 404;
		// anything else (forbidden, timeout, apiserver down) is ours, and is
		// retryable.
		status, reason := http.StatusBadGateway, "upstream"
		if apierrors.IsNotFound(err) || errors.Is(err, ErrSecretKeyNotFound) {
			status, reason = http.StatusNotFound, "not_found"
		}
		if status != http.StatusNotFound {
			// The service is uniquely positioned to explain this one: the agent
			// sees only the status code. Log it, and count it apart from the
			// client-caused misses.
			//
			// THROTTLED, because this is the steady state of the failure the
			// route's own doc calls the likeliest: an RBAC grant that was never
			// added means every agent on every node re-asks each scrape cycle
			// and each one would log a line, forever. The counter is the
			// alerting signal; the log only has to say it once in a while, and
			// per REF so a second broken credential is not masked by the first.
			s.warnScrapeAuth("read:"+ns+"/"+name+"/"+key, func() {
				s.log().Warn("resolving scrape-auth secret",
					"namespace", ns, "name", name, "key", key, "error", err,
					"note", "further failures for this ref are suppressed for "+scrapeAuthWarnEvery.String())
			})
			w.Header().Set("Retry-After", "5")
		}
		obs.ScrapeAuthFailures.WithLabelValues(reason).Inc()
		writeError(w, status, fmt.Sprintf("secret %s/%s key %s: %v", ns, name, key, err))
		return
	}
	// The value is about to be marshalled into a JSON string. encoding/json
	// replaces every invalid UTF-8 byte with U+FFFD and reports no error, so a
	// credential created from raw bytes (kubectl create secret
	// --from-file=password=<binary>) would reach the agent silently corrupted,
	// with a 200 and up=0 as the only evidence. Refuse loudly instead: the
	// alternative — base64 on the wire — is a format change every deployed
	// agent would have to learn in lockstep.
	if !utf8.ValidString(val) {
		obs.ScrapeAuthFailures.WithLabelValues("not_utf8").Inc()
		// Throttled through the same table as the failure above, but under its
		// OWN key prefix: sharing the bare ref let a class transition (an RBAC
		// gap fixed, revealing a non-UTF-8 value) be swallowed for the whole
		// window, reporting neither condition.
		s.warnScrapeAuth("utf8:"+ns+"/"+name+"/"+key, func() {
			s.log().Warn("scrape-auth secret value is not valid UTF-8 and cannot be served as JSON",
				"namespace", ns, "name", name, "key", key,
				"note", "further failures for this ref are suppressed for "+scrapeAuthWarnEvery.String())
		})
		writeError(w, http.StatusUnprocessableEntity, fmt.Sprintf(
			"secret %s/%s key %s is not valid UTF-8; kubescrape serves credentials as JSON strings",
			ns, name, key))
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"value": val})
}

// clipSegment bounds a caller-supplied path segment before it reaches a log
// line. A namespace, a Secret name and a Secret key are all DNS-subdomain-ish
// in practice (253 bytes at most), but nothing on the wire enforces that here:
// the segment arrives from a URL path bounded only by the request-head limit,
// and a log line is the one place a 16 KB value costs something in every
// direction at once. The truncation is marked, so a clipped value is never
// mistaken for the whole one.
func clipSegment(v string) string {
	const max = 253
	if len(v) <= max {
		return v
	}
	return v[:max] + "…(truncated)"
}

// waitBudget determines how long a container lookup may block: MaxWait by
// default, optionally shortened by ?wait= (a Go duration or plain seconds).
func (s *Server) waitBudget(r *http.Request) (time.Duration, error) {
	v := r.URL.Query().Get("wait")
	if v == "" {
		return s.maxWait, nil
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		secs, ierr := strconv.Atoi(v)
		if ierr != nil {
			return 0, fmt.Errorf("invalid wait parameter %q: use a duration like 2s", v)
		}
		// Reject negatives BEFORE the multiplication: a large-enough negative
		// (?wait=-9223372037) overflows time.Duration(secs)*time.Second and
		// wraps POSITIVE, slipping past the d < 0 check below — inconsistent
		// with the pinned negatives-are-rejected invariant even though the
		// clamp bounds it. Then guard positive overflow, and let the shared
		// duration clamp below apply — clamping by TRUNCATED whole seconds here
		// would turn a sub-second maxWait into 0 (non-blocking) for ?wait=1.
		if secs < 0 {
			return 0, fmt.Errorf("wait parameter must not be negative")
		}
		if secs > int(math.MaxInt64/int64(time.Second)) {
			d = s.maxWait
		} else {
			d = time.Duration(secs) * time.Second
		}
	}
	if d < 0 {
		return 0, fmt.Errorf("wait parameter must not be negative")
	}
	if d > s.maxWait {
		d = s.maxWait
	}
	return d, nil
}

// reportEncodeFailure reports a response this process could not serialise.
//
// It is an ERROR and it is a bug: every value written here is a kubemeta
// document built from informer objects, holding nothing encoding/json refuses.
// If it ever happens it happens for EVERY request on that route — a permanent
// 500 whose only other trace is
// kubescrape_http_requests_total{code="500"} — so it is throttled rather than
// unconditional, and the throttle is keyless because one broken document
// breaks its whole route.
func (s *Server) reportEncodeFailure(what string, err error) {
	if s.warnEncode.Allow(encodeWarnEvery) {
		s.log().Error("encoding a response failed, so this route answers 500 until the offending object changes",
			"what", what, "error", err,
			"note", "this cannot happen for a well-formed metadata document; further reports are suppressed for "+
				encodeWarnEvery.String())
	}
}

// encodeWarnEvery bounds the encode-failure report; see reportEncodeFailure.
const encodeWarnEvery = 5 * time.Minute

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	// A failure here is almost always the CLIENT going away mid-write, which
	// is not this server's business and would flood on a rolling agent update.
	// The three errors that mean the VALUE cannot be encoded are a different
	// thing entirely — a bug that makes the route answer a truncated 200
	// forever — so those, and only those, are reported.
	if err := json.NewEncoder(w).Encode(v); err != nil && unencodable(err) {
		encodeFailed(err)
	}
}

// unencodable reports an error that names the VALUE rather than the connection:
// encoding/json returns these three before writing anything, so they are the
// ones that mean "this document can never be served".
func unencodable(err error) bool {
	var (
		unsupportedType  *json.UnsupportedTypeError
		unsupportedValue *json.UnsupportedValueError
		marshaler        *json.MarshalerError
	)
	return errors.As(err, &unsupportedType) || errors.As(err, &unsupportedValue) || errors.As(err, &marshaler)
}

// encodeFailed is writeJSON's report. writeJSON is a package function (it
// predates the Server and is called from handlers that have no reason to be
// methods), so it cannot reach a Server's throttle; this one is package-level
// for the same reason, and the condition it reports is a property of the
// PROCESS — one unserialisable document — not of a Server instance.
func encodeFailed(err error) {
	if writeJSONWarn.Allow(encodeWarnEvery) {
		slog.Error("encoding a response failed after the status line was written, so the client received a "+
			"truncated body",
			"error", err,
			"note", "this cannot happen for a well-formed metadata document; further reports are suppressed for "+
				encodeWarnEvery.String())
	}
}

// writeJSONWarn throttles encodeFailed; see it for why this is package-level.
var writeJSONWarn logdedupe.Throttle

// writeCached serves a 200 metadata response with standard HTTP cache headers
// (Cache-Control max-age + ETag), so the agent's client can serve repeat
// lookups locally and revalidate cheaply with If-None-Match (304). With a zero
// TTL it falls back to a plain uncached JSON write.
//
// private marks a response that identifies the CALLER (/v1/self): per-client
// caches may store it, shared ones must not.
func (s *Server) writeCached(w http.ResponseWriter, r *http.Request, v any, private bool) {
	if s.cacheTTL <= 0 {
		if private {
			// The caching knob is off, but the response still names its caller:
			// without a freshness lifetime a shared cache MAY store a 200
			// heuristically (RFC 9111 4.2.2), which is exactly the response
			// that must never be handed to the next caller.
			w.Header().Set("Cache-Control", "private, no-store")
		}
		writeJSON(w, http.StatusOK, v)
		return
	}
	body, err := json.Marshal(v)
	if err != nil {
		s.reportEncodeFailure("metadata response", err)
		writeError(w, http.StatusInternalServerError, "encoding response")
		return
	}
	s.writeCachedBody(w, r, body, entityTag(body), private)
}

// writeCachedBody serves an already-encoded body under its entity tag: the
// cache headers, then either the 304 a matching validator asks for or the body.
// handleNodeTargets encodes for itself, because it memoises the tag and has to
// do so before the client can act on it.
func (s *Server) writeCachedBody(w http.ResponseWriter, r *http.Request, body []byte, etag string, private bool) {
	h := w.Header()
	h.Set("Content-Type", "application/json")
	h.Set("Cache-Control", s.cacheControl(private))
	h.Set("ETag", etag)
	if match := r.Header.Get("If-None-Match"); match != "" && etagMatches(match, etag) {
		w.WriteHeader(http.StatusNotModified)
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(body)
}

// entityTag is the ETag of a response body: the 128-bit digest as 32 hex
// digits, high half first, in quotes.
//
// Rendered into a fixed-size array rather than with fmt or two FormatUint
// calls: the width is constant, so the whole tag is one allocation (the
// returned string) on a path that runs for every cached response including
// every 304 revalidation. Zero-PADDING is the part that matters for
// correctness — FormatUint drops leading zeros, which would let two distinct
// digests render as the same tag whenever one half is small.
func entityTag(body []byte) string { return formatTag(bodyHash(body)) }

// formatTag renders one digest. Split from entityTag so the padding can be
// asserted on a CHOSEN digest (TestEntityTagIsFixedWidthHex) — searching for a
// body that hashes to a half with leading zeros is not a test's job.
func formatTag(h xxh3.Uint128) string {
	var half [8]byte
	var out [2 + 32]byte
	out[0] = '"'
	binary.BigEndian.PutUint64(half[:], h.Hi)
	hex.Encode(out[1:17], half[:])
	binary.BigEndian.PutUint64(half[:], h.Lo)
	hex.Encode(out[17:33], half[:])
	out[33] = '"'
	return string(out[:])
}

// cacheControl renders the freshness lifetime a cached 200 advertises.
//
// max-age has second granularity: a sub-second TTL truncates to 0, which tells
// the client not to cache AT ALL — the opposite of a short cache, and silently
// (the ETag is still computed on every response). Round up so any non-zero TTL
// caches for at least a second; 0 disables caching before we get here.
func (s *Server) cacheControl(private bool) string {
	maxAge := int(s.cacheTTL.Seconds())
	if maxAge < 1 {
		maxAge = 1
	}
	cc := "max-age=" + strconv.Itoa(maxAge)
	if private {
		cc = "private, " + cc
	}
	return cc
}

// nodeTargetsETag remembers what one node's target list was called, and when.
type nodeTargetsETag struct {
	etag    string
	builtAt time.Time
	valid   targetsValidity
}

// targetsValidity is the composite change token of every source nodeTargets
// reads. Component-wise rather than hashed or summed on purpose: four uint64s
// compare in four instructions, and any folding of them into one introduces an
// aliasing chance for the one thing this must never do — call a changed cluster
// unchanged.
//
// wired reports that EVERY source publishes a token. It is not a formality: an
// unwired source is indistinguishable from one that never changes, so a missing
// one would silently turn this into "always valid" and serve a frozen target
// list until the TTL fallback happened to save it. Absent by CONFIGURATION is a
// different thing and is fine — a nil Monitors index means monitor-derived
// targets cannot exist, so its constant zero is the truth.
type targetsValidity struct {
	pods, owners, services, monitors uint64
	wired                            bool
}

// targetsValidity samples every source nodeTargets derives from. Callers load
// it BEFORE reading the data (see store.Store's gen field): a change landing
// mid-derivation then leaves the memo tagged with the older token and the next
// revalidation rebuilds, which is the safe direction to be wrong in.
func (s *Server) targetsValidity() targetsValidity {
	if s.ownerGeneration == nil || s.store == nil || s.services == nil {
		return targetsValidity{}
	}
	v := targetsValidity{
		pods:     s.store.Generation(),
		owners:   s.ownerGeneration(),
		services: s.services.Generation(),
		wired:    true,
	}
	if s.monitors != nil {
		v.monitors = s.monitors.Generation()
	}
	return v
}

// maxNodeTargetETags bounds the memo. An entry is a node name and a 16-hex tag,
// and one is minted only for a node the store HAS pods on — a caller inventing
// node names gets an empty list, which costs a map lookup to produce and never
// reaches the memo. The cap is belt and braces against a cluster far larger
// than this service is built for; at the cap, entries already staler than the
// TTL (useless: they can only produce a rebuild) are dropped, and if that
// frees no room the new tag is simply not memoised — that node's
// revalidations rebuild until room appears.
const maxNodeTargetETags = 8192

// nodeTargetsNotModified answers a conditional GET for a node's targets from
// the ETag memo, without building the response — reporting whether it did.
//
// The ETag is a body hash, so a 304 otherwise costs EVERYTHING a 200 costs:
// PodsOnNode, per-pod owner and namespace enrichment, target derivation, the
// sort and a full json.Marshal, and then the body is discarded (measured at
// 110 pods: 1.87 ms, 1.90 MB and 7,553 allocations to send an empty 304, with
// json.Marshal at 48.6% of the request's CPU).
//
// TWO WAYS TO VALIDATE, and which one runs decides whether this is worth
// having at all.
//
// THE CHANGE TOKEN is the one that matters. Every source nodeTargets reads —
// the pod store, the owner and namespace informer caches, services.Index and
// servicemonitors.Index — publishes a generation that advances only on a real
// change, and the memo records what they read at build time. If all four still
// agree, the client's copy is not merely young: it is PROVABLY current, so the
// 304 grants a full max-age measured from now, exactly as the 200 does. No
// clock is consulted.
//
// THE WALL CLOCK is the fallback for when some source is not wired (see
// targetsValidity.wired) — it must not be entered by assuming an unwired token
// means "unchanged". It bounds staleness by builtAt+TTL, the last instant the
// store is known to have agreed with the tag, and hands out only the REMAINING
// window floored to whole seconds. Less than the 200's window, deliberately:
// the revalidating client is not necessarily the requester whose build stamped
// builtAt (any other caller rebuilds the unchanged list and refreshes the
// memo), so re-stamping a full max-age on an up-to-TTL-old memo would entitle a
// lapsed client to its copy until builtAt+2×TTL while the list may have changed
// just after builtAt. Once nothing remains the memo is ignored and the response
// is rebuilt, which is where a changed target list becomes a 200.
//
// WHY THE TOKEN WAS ADDED: with only the clock, this was unreachable by the
// caller it exists for. The memo lived for cacheTTL from the build and cacheTTL
// is also the max-age the 200 advertises, so a client honouring its own cache
// asked again exactly when — or after — the memo lapsed; the two windows were
// the same window. A DaemonSet agent polling on its scrape interval therefore
// NEVER hit it and re-paid the whole derivation, sort and marshal on every
// poll (468 µs / 165 KiB / 1,259 allocations at 5,000 pods; 1.85 ms / 650 KiB /
// 4,945 at 22,000, with marshal 54-71% of it), multiplied by every node in the
// fleet, in the singleton the chart requests 128Mi for. What reached it was
// only a caller asking FASTER than the max-age it was given.
// TestNodeTargetsMemoServesAConformingClient pins the new behaviour,
// TestNodeTargetsMemoRebuildsWhenAnySourceChanges pins the safety property, and
// BenchmarkNodeTargetsRevalidation reports both shapes side by side.
//
// Only the tag is memoised, never the body: the body is the whole node's pod
// set (2.21 MB at 110 pods), and holding one per node would put hundreds of
// megabytes of response bodies in a process with a 128Mi request.
func (s *Server) nodeTargetsNotModified(w http.ResponseWriter, r *http.Request, node string) bool {
	if s.cacheTTL <= 0 {
		return false
	}
	match := r.Header.Get("If-None-Match")
	if match == "" {
		return false
	}
	// Sampled BEFORE the memo is read, for the same reason a build samples it
	// before reading the store: if a change lands between the two, the token
	// compared is the older one and the answer is a rebuild.
	cur := s.targetsValidity()

	s.targetsMu.Lock()
	e, ok := s.targetsETags[node]
	s.targetsMu.Unlock()
	if !ok || !etagMatches(match, e.etag) {
		return false
	}

	var maxAge int
	if cur.wired && e.valid.wired {
		// The token is AUTHORITATIVE when it is available, in both directions.
		// A mismatch means rebuild — falling through to the clock here would
		// answer 304 for a list that provably changed, purely because the memo
		// happened to be young, which is the one thing this must never do.
		if e.valid != cur {
			return false
		}
		// Unchanged, so the copy is current as of NOW and the grant is the full
		// window — the same one a 200 hands out, for the same reason. builtAt
		// moves with it so the fallback below stays truthful if a source is
		// later unwired.
		maxAge = int(s.cacheTTL / time.Second)
		now := s.now()
		s.targetsMu.Lock()
		if cached, still := s.targetsETags[node]; still && cached.etag == e.etag {
			cached.builtAt = now
			s.targetsETags[node] = cached
		}
		s.targetsMu.Unlock()
	} else {
		// No token: the wall-clock fallback. The remaining window, floored —
		// the client's expiry must not pass builtAt+TTL (the bound argued
		// above). A remainder under a second grants nothing, so the memo counts
		// as expired (which subsumes the age >= TTL check) and the store is
		// consulted.
		maxAge = int((s.cacheTTL - s.now().Sub(e.builtAt)) / time.Second)
	}
	if maxAge < 1 {
		return false
	}
	h := w.Header()
	h.Set("Content-Type", "application/json")
	h.Set("Cache-Control", "max-age="+strconv.Itoa(maxAge))
	h.Set("ETag", e.etag)
	w.WriteHeader(http.StatusNotModified)
	return true
}

// rememberNodeTargets records what a node's freshly built target list is
// called, so the next revalidation inside the TTL can be answered without
// building it again.
func (s *Server) rememberNodeTargets(node, etag string, valid targetsValidity) {
	if etag == "" { // caching disabled, or the response failed to encode
		return
	}
	now := s.now()
	s.targetsMu.Lock()
	defer s.targetsMu.Unlock()
	if s.targetsETags == nil {
		s.targetsETags = make(map[string]nodeTargetsETag)
	}
	if len(s.targetsETags) >= maxNodeTargetETags {
		// Drop what is already useless (a stale entry can only produce a
		// rebuild anyway). If that frees nothing, skip the memo rather than
		// grow: rebuilding is the correct answer, just the slow one.
		for k, v := range s.targetsETags {
			if now.Sub(v.builtAt) >= s.cacheTTL {
				delete(s.targetsETags, k)
			}
		}
		if len(s.targetsETags) >= maxNodeTargetETags {
			return
		}
	}
	s.targetsETags[node] = nodeTargetsETag{etag: etag, builtAt: now, valid: valid}
}
