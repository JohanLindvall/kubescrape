// Package server exposes the metadata store over HTTP.
package server

// The HTTP handlers for the v1 metadata endpoints, plus the shared
// response helpers (caching headers, wait budgets, JSON writing).

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"sort"
	"strconv"
	"time"
	"unicode/utf8"

	apierrors "k8s.io/apimachinery/pkg/api/errors"

	"github.com/JohanLindvall/kubescrape/internal/obs"
	"github.com/JohanLindvall/kubescrape/internal/peerip"
	"github.com/JohanLindvall/kubescrape/internal/scrape"
	"github.com/JohanLindvall/kubescrape/internal/servicemonitors"
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
	// sync; spend the wait budget on readiness first if needed.
	if !s.waitReady(ctx) {
		writeError(w, http.StatusServiceUnavailable, "informer caches not synced")
		return
	}

	res, ok, err := s.store.GetContainer(ctx, id)
	if err != nil {
		// Waiter cap: shed the blocking lookup as retryable, never as 404 —
		// the container may exist momentarily, the store is just saturated
		// with blocked lookups.
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
	if !s.isReady() {
		writeError(w, http.StatusServiceUnavailable, "informer caches not synced")
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
	if !s.isReady() {
		writeError(w, http.StatusServiceUnavailable, "informer caches not synced")
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
	if !s.isReady() {
		writeError(w, http.StatusServiceUnavailable, "informer caches not synced")
		return
	}
	node := r.PathValue("node")
	pods := s.store.PodsOnNode(node)
	targets := make([]kubemeta.ScrapeTarget, 0)
	var monitored map[string][]monitorEndpoint
	// Hoisted out of the per-pod loop: PodMonitors() copies and SORTS the whole
	// monitor list on every call, so a node with 110 pods and 200 monitors did
	// 110 sorts and 22k selector evaluations per request, per agent, per scrape
	// cycle. The sibling ServiceMonitor match is memoised for the same reason.
	var allPodMonitors []*servicemonitors.PodMonitor
	if len(pods) > 0 { // an empty node cannot match any monitored service
		monitored = s.monitoredServices()
		if s.monitors != nil {
			allPodMonitors = s.monitors.PodMonitors()
		}
	}
	for _, np := range pods {
		if !scrape.Scrapeable(np.Pod) {
			continue // finished/deleted pods can never yield targets
		}
		// Cheap pre-check before the (per-pod) enrichment work: does the pod
		// or any service selecting it opt into scraping?
		matched := s.services.Matching(np.Pod.Namespace, np.Pod.Labels)
		// Map iteration order in the services index must not decide which
		// Service a URL-deduped target is attributed to. Guarded because the
		// overwhelmingly common case is 0 or 1 matching Service, where
		// sort.Slice still allocates the reflect swapper and the comparison
		// closure — per pod, per node, per scrape cycle — to sort nothing.
		if len(matched) > 1 {
			sort.Slice(matched, func(i, j int) bool {
				if matched[i].Namespace != matched[j].Namespace {
					return matched[i].Namespace < matched[j].Namespace
				}
				return matched[i].Name < matched[j].Name
			})
		}
		podAnnotated := np.Pod.Annotations[scrape.AnnotationScrape] == "true"
		svcAnnotated := false
		for _, svc := range matched {
			if svc.Annotations[scrape.AnnotationScrape] == "true" || len(monitored[svc.UID]) > 0 {
				svcAnnotated = true
				break
			}
		}
		podMonitors := s.podMonitorsFor(np.Pod, allPodMonitors)
		if !podAnnotated && !svcAnnotated && len(podMonitors) == 0 {
			continue
		}
		s.enrich(&np.Pod, np.OwnerRefs)

		podTargets := scrape.PodTargets(np.Pod)
		for _, svc := range matched {
			podTargets = append(podTargets, scrape.ServiceTargets(np.Pod, svc)...)
			for _, sme := range monitored[svc.UID] {
				podTargets = append(podTargets, scrape.MonitorTargets(np.Pod, svc, sme.monitor, sme.endpoint)...)
			}
		}
		for _, pm := range podMonitors {
			for _, ep := range pm.Endpoints {
				podTargets = append(podTargets, scrape.PodMonitorTargets(np.Pod, pm.Namespace+"/"+pm.Name, ep)...)
			}
		}
		// The same endpoint can be reachable via pod and service annotations;
		// keep the first occurrence (pod source wins).
		//
		// EXCEPT when a later duplicate carries monitor configuration the
		// earlier one cannot: a ServiceMonitor/PodMonitor endpoint's
		// bearerTokenSecret, insecureSkipVerify and metricRelabelings live only
		// on the monitor-derived target, and monitors are appended last. An
		// org-wide `prometheus.io/scrape: "true"` annotation coexisting with
		// prometheus-operator CRDs is common, and dropping the monitor target
		// meant scraping a token-protected endpoint unauthenticated (401 — total
		// metric loss, visible only as up=0) and, worse, exporting the very
		// series a drop rule asked to remove. The URL is identical either way,
		// so keeping the configured variant scrapes the same endpoint, correctly.
		seen := make(map[string]int, len(podTargets)) // URL -> index in targets
		for _, t := range podTargets {
			i, dup := seen[t.URL]
			if !dup {
				seen[t.URL] = len(targets)
				targets = append(targets, t)
				continue
			}
			if configuredTarget(t) && !configuredTarget(targets[i]) {
				targets[i] = t
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
	// Cached like the other metadata 200s (Cache-Control max-age + ETag): every
	// agent re-fetches its target list every cycle, and the response embeds the
	// COMPLETE pod document per target — without revalidation that is the whole
	// node's pod set re-sent 30x/min regardless of change, the one metadata
	// route that had no 304 path. Staleness is bounded by the TTL (default 10s,
	// under the default scrape interval and additive to the agent's own polling
	// lag); the server-side list itself still drops deleted/finished/terminating
	// pods immediately (the invariant is about what a fresh response contains).
	s.writeCached(w, r, map[string]any{
		"node":    node,
		"targets": targets,
	}, false)
}

// configuredTarget reports whether t carries endpoint configuration that only
// a ServiceMonitor/PodMonitor can supply. Losing any of it to URL dedup changes
// what is scraped or what is exported: without the bearer token the scrape 401s
// (total metric loss for the target), without insecureSkipVerify an https
// endpoint with a private CA fails, and without the metricRelabelings the
// series a drop rule targets are exported anyway.
func configuredTarget(t kubemeta.ScrapeTarget) bool {
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
	// Gated AFTER the disabled-feature 404 and the anonymous 401 (so neither
	// changes), and BEFORE the allowlist. The allowlist is derived from a LIVE
	// servicemonitors.Index, which exists but is EMPTY until the informers sync
	// — so between ListenAndServe and the ready close, a perfectly valid ref
	// got a definitive 403 "not referenced by any monitor endpoint", an
	// assertion the server cannot yet make. Every sibling handler returns a
	// retryable 503 in that window; this was the one route that did not, and it
	// turned a startup race into what reads like a monitor misconfiguration.
	if !s.isReady() {
		w.Header().Set("Retry-After", "1")
		writeError(w, http.StatusServiceUnavailable, "informer caches not synced")
		return
	}
	if s.monitors == nil {
		writeError(w, http.StatusNotFound, "no monitors indexed")
		return
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
	etag := `"` + strconv.FormatUint(bodyHash(body), 16) + `"`
	// max-age has second granularity: a sub-second TTL truncates to 0, which
	// tells the client not to cache AT ALL — the opposite of a short cache, and
	// silently (the ETag is still computed on every response). Round up so any
	// non-zero TTL caches for at least a second; 0 disables caching before we
	// get here.
	maxAge := int(s.cacheTTL.Seconds())
	if maxAge < 1 {
		maxAge = 1
	}
	cc := "max-age=" + strconv.Itoa(maxAge)
	if private {
		cc = "private, " + cc
	}
	h := w.Header()
	h.Set("Content-Type", "application/json")
	h.Set("Cache-Control", cc)
	h.Set("ETag", etag)
	if match := r.Header.Get("If-None-Match"); match != "" && etagMatches(match, etag) {
		w.WriteHeader(http.StatusNotModified)
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(body)
}
