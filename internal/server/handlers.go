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
	"math"
	"net/http"
	"sort"
	"strconv"
	"time"
	"unicode/utf8"

	"github.com/zeebo/xxh3"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/validate/content"

	"github.com/JohanLindvall/kubescrape/internal/obs"
	"github.com/JohanLindvall/kubescrape/internal/peerip"
	"github.com/JohanLindvall/kubescrape/internal/scrape"
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

	ctx, cancel := context.WithTimeout(r.Context(), wait)
	defer cancel()

	// Don't report "not found" from a cache that hasn't finished its initial
	// sync; spend the wait budget on readiness first if needed. A drain ends
	// that wait too — the shutdown path must not leave a request parked here
	// (see Server.Drain), and its refusal carries Retry-After because the next
	// pod behind the Service can answer at once, unlike a sync that is merely
	// slow.
	if err := s.waitReady(ctx); err != nil {
		if errors.Is(err, errDraining) {
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
		w.Header().Set("Retry-After", "1")
		writeError(w, http.StatusServiceUnavailable, err.Error())
		return
	}
	if !ok {
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
// header: X-Forwarded-For is caller-controlled, and this endpoint hands out
// whatever pod owns the address it is given. It resolves through the same
// live-only pod-IP index as /v1/pod-ips, so a caller behind an address-
// rewriting hop — or one on hostNetwork, sharing the node IP — gets a 404
// rather than someone else's identity.
//
// The response is cached like any other metadata 200, but PRIVATE: it names
// the caller, so only a per-client cache may hold it. That is what lets a
// caller re-read its own pod cheaply — the poll becomes a conditional GET, and
// a 304 says "your labels and namespace metadata are unchanged" — instead of
// choosing between stale attributes and a full document every interval.
func (s *Server) handleSelf(w http.ResponseWriter, r *http.Request) {
	ip := peerip.From(r.RemoteAddr)
	if ip == "" {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("unparseable peer address %q", r.RemoteAddr))
		return
	}
	s.servePod(w, r, cachePrivate,
		func() (store.NodePod, bool) { return s.store.GetPodByIP(ip) },
		func() string { return fmt.Sprintf("no live pod with peer IP %q", ip) })
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
	// at all — see nodeTargetsNotModified. Every agent poll after the first is
	// one of these.
	if s.nodeTargetsNotModified(w, r, node) {
		return
	}
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
		writeError(w, http.StatusInternalServerError, "encoding response")
		return
	}
	etag := entityTag(body)
	// Memoised BEFORE the response is written: the client may revalidate the
	// instant it holds the ETag, and remembering afterwards leaves a window in
	// which the tag this server just handed out is one it cannot recognise.
	if built {
		s.rememberNodeTargets(node, etag)
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
	// ONE snapshot of each namespace's Services for the whole request. The
	// per-pod Matching() call it replaces took the index RLock and walked every
	// Service in the pod's namespace, so a 1,000-Service namespace cost 110 lock
	// round trips and 110 map walks per request — on the DEFAULT annotation
	// path, while its two siblings on the same request (monitoredServices, the
	// PodMonitor list) had both already been hoisted out of this loop.
	svcByNamespace := s.services.InNamespaces(podNamespaces(pods))

	var d targetDedup
	// The monitor endpoints are swept once per matched SERVICE, so a pod behind
	// two Services reaches the merge twice with the same declaration; the merge
	// is a fold and cannot tell (monitorOffers' doc has the whole story).
	var offers monitorOffers
	var matched []*services.Service
	var podMonitors []podMonitorRef
	for _, np := range pods {
		if !scrape.Scrapeable(np.Pod) {
			continue // finished/deleted pods can never yield targets
		}
		// Cheap pre-check before the (per-pod) enrichment work: does the pod
		// or any service selecting it opt into scraping?
		matched = matchingServices(svcByNamespace[np.Pod.Namespace], np.Pod.Labels, matched[:0])
		podAnnotated := np.Pod.Annotations[scrape.AnnotationScrape] == "true"
		svcAnnotated := false
		for _, svc := range matched {
			if svc.Annotations[scrape.AnnotationScrape] == "true" || len(monitored[svc.UID]) > 0 {
				svcAnnotated = true
				break
			}
		}
		podMonitors = podMonitorsFor(np.Pod, allPodMonitors, podMonitors[:0])
		if !podAnnotated && !svcAnnotated && len(podMonitors) == 0 {
			continue
		}
		s.enrich(&np.Pod, np.OwnerRefs)

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
					adopted, conflict := scrape.MergeMonitorEndpoint(held, sme.monitor, sme.endpoint)
					if adopted {
						d.adoptedAuth(url, sme.monitor)
					}
					if conflict {
						s.reportAuthConflict("servicemonitor", d.servingAuth(url, held.Monitor), sme.monitor, url)
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
					continue
				}
				if held, taken := d.monitorHolder(url); taken {
					adopted, conflict := scrape.MergeMonitorEndpoint(held, pm.name, ep)
					if adopted {
						d.adoptedAuth(url, pm.name)
					}
					if conflict {
						s.reportAuthConflict("podmonitor", d.servingAuth(url, held.Monitor), pm.name, url)
					}
					continue
				}
				for _, t := range scrape.PodMonitorTargets(np.Pod, pm.name, *ep) {
					d.add(t)
				}
			}
		}
	}
	// Deterministic order: PodsOnNode iterates a map, and writeCached's ETag is
	// a body hash — an order that shuffled per request would mint a fresh ETag
	// every time and defeat the 304 revalidation entirely. URL first, then
	// monitor/source, then the embedded pod's UID: the UID tiebreak is what
	// makes the order TOTAL in the one case URL+monitor+source cannot separate
	// (two hostNetwork pods sharing the node IP with the same annotated port —
	// identical URL, empty Monitor, Source "pod", but different pod documents).
	sort.Slice(targets, func(i, j int) bool {
		if targets[i].URL != targets[j].URL {
			return targets[i].URL < targets[j].URL
		}
		if targets[i].Monitor != targets[j].Monitor {
			return targets[i].Monitor < targets[j].Monitor
		}
		if targets[i].Source != targets[j].Source {
			return targets[i].Source < targets[j].Source
		}
		return targets[i].Pod.UID < targets[j].Pod.UID
	})
	return targets, true
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
}

// reset points the dedup at a fresh pod. The maps are reused across pods:
// allocated once per request rather than once per pod.
func (d *targetDedup) reset(out *[]kubemeta.ScrapeTarget) {
	d.out = out
	if d.urlOwner == nil {
		d.urlOwner = make(map[string]int, 4)
	} else {
		clear(d.urlOwner)
	}
	clear(d.authOwner)
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

func (d *targetDedup) add(t kubemeta.ScrapeTarget) {
	i, taken := d.urlOwner[t.URL]
	if !taken {
		d.urlOwner[t.URL] = len(*d.out)
		*d.out = append(*d.out, t)
		return
	}
	// pod source wins over service source, and a monitor wins over both.
	if held := &(*d.out)[i]; configuredTarget(&t) && !configuredTarget(held) {
		carryForward(&t, held)
		*held = t
	}
}

// carryForward moves the fields the replaced target had and the winner lacks.
//
// Today that is exactly the Service: a PODMONITOR selects pods directly and its
// targets carry none, so replacing a service-annotation target with one wholesale
// stripped k8s.service.name and k8s.service.uid from every sample of that
// endpoint (promscrape's fillTargetResource reads target.Service). The
// preference's own justification — that the monitor target carries strictly
// MORE — is true of the ServiceMonitor arm, which sets Service, and false of
// the PodMonitor one.
func carryForward(winner, replaced *kubemeta.ScrapeTarget) {
	if winner.Service == nil {
		winner.Service = replaced.Service
	}
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
		writeError(w, http.StatusNotFound, "scrape auth secrets are not enabled (-scrape-auth-secrets)")
		return
	}
	// Authenticate BEFORE any lookup: an unauthenticated client must not be
	// able to probe which secret refs a monitor names (403 vs 404) either.
	if !s.authorizedForScrapeAuth(r) {
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
			writeError(w, http.StatusBadRequest,
				fmt.Sprintf("invalid %s %q: %s", what, v, errs[0]))
			return
		}
	}
	if _, ok := s.monitors.AuthSecretRefs()[ns+"/"+name+"/"+key]; !ok {
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

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

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
// The ETag is a body hash, so a 304 otherwise cost EVERYTHING a 200 costs:
// PodsOnNode, per-pod owner and namespace enrichment, target derivation, the
// sort and a full json.Marshal, and then the body was discarded (measured at
// 110 pods: 1.87 ms, 1.90 MB and 7,553 allocations to send an empty 304, with
// json.Marshal at 48.6% of the request's CPU). With a 10s TTL under a 30s
// scrape interval EVERY agent poll is exactly this request.
//
// Staleness stays bounded by the TTL because every grant — the 200's and this
// 304's alike — expires by builtAt+TTL, the last instant the store is known to
// have agreed with the tag. The 200 hands out exactly that window. The 304 must
// hand out LESS: the revalidating client is not necessarily the requester whose
// build stamped builtAt (any other caller — a second agent during a rolling
// update, an operator's curl, a restarted client — rebuilds the unchanged list
// and refreshes the memo), so re-stamping a FULL max-age on an up-to-TTL-old
// memo would entitle a lapsed client to its copy until builtAt+2×TTL while the
// list may have changed just after builtAt. The 304 therefore advertises only
// the REMAINING window, floored to whole seconds (rounding can only shorten
// the grant, never extend it); once nothing remains the memo is ignored and
// the response is rebuilt, which is where a changed target list becomes a 200.
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
	s.targetsMu.Lock()
	e, ok := s.targetsETags[node]
	s.targetsMu.Unlock()
	if !ok || !etagMatches(match, e.etag) {
		return false
	}
	// The remaining window, floored: the client's expiry must not pass
	// builtAt+TTL (the bound argued above). A remainder under a second grants
	// nothing, so the memo counts as expired — which subsumes the age >= TTL
	// check — and the store is consulted.
	maxAge := int((s.cacheTTL - s.now().Sub(e.builtAt)) / time.Second)
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
func (s *Server) rememberNodeTargets(node, etag string) {
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
	s.targetsETags[node] = nodeTargetsETag{etag: etag, builtAt: now}
}
