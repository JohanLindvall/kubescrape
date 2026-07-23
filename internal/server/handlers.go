// Package server exposes the metadata store over HTTP.
package server

// The HTTP handlers for the v1 metadata endpoints, plus the shared
// response helpers (caching headers, wait budgets, JSON writing).

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"sort"
	"strconv"
	"time"

	"github.com/JohanLindvall/kubescrape/internal/scrape"
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
	})
}

// handlePod serves GET /v1/pods/{namespace}/{name}: full metadata for one
// pod looked up by name (used by the agent to attribute cadvisor series).
// Deleted pods stay resolvable until their tombstone expires.
// servePod is the shared body of the three pod endpoints: readiness gate,
// lookup, 404, owner/namespace enrichment, then a cached write. notFound is
// evaluated lazily so the success path never formats it.
func (s *Server) servePod(w http.ResponseWriter, r *http.Request, cached bool, lookup func() (store.NodePod, bool), notFound func() string) {
	if !s.isReady() {
		writeError(w, http.StatusServiceUnavailable, "informer caches not synced")
		return
	}
	np, ok := lookup()
	if !ok {
		writeError(w, http.StatusNotFound, notFound())
		return
	}
	s.enrich(&np.Pod, np.OwnerRefs)
	if !cached {
		// The pod-IP index exists for IMMEDIACY (IPs recycle; deleted pods drop
		// out at once) — a cached 200 would let metaclient re-serve the OLD
		// owner of a recycled IP for up to the metadata TTL.
		writeJSON(w, http.StatusOK, np.Pod)
		return
	}
	s.writeCached(w, r, np.Pod)
}

func (s *Server) handlePod(w http.ResponseWriter, r *http.Request) {
	namespace, name := r.PathValue("namespace"), r.PathValue("name")
	s.servePod(w, r, true,
		func() (store.NodePod, bool) { return s.store.GetPodByName(namespace, name) },
		func() string { return fmt.Sprintf("pod %s/%s not found", namespace, name) })
}

// handlePodByUID serves GET /v1/pod-uids/{uid}: full metadata for one pod
// looked up by UID (used by the OTLP ingest enricher to attribute pushed
// telemetry). Deleted pods stay resolvable until their tombstone expires.
func (s *Server) handlePodByUID(w http.ResponseWriter, r *http.Request) {
	uid := r.PathValue("uid")
	s.servePod(w, r, true,
		func() (store.NodePod, bool) { return s.store.GetPodByUID(uid) },
		func() string { return fmt.Sprintf("pod uid %q not found", uid) })
}

// handlePodByIP serves GET /v1/pod-ips/{ip}: the LIVE pod owning a pod IP
// (the agent's opt-in peer-IP attribution for pushed OTLP). Deleted pods and
// hostNetwork pods never resolve.
func (s *Server) handlePodByIP(w http.ResponseWriter, r *http.Request) {
	ip := r.PathValue("ip")
	s.servePod(w, r, false, // never cached: see servePod
		func() (store.NodePod, bool) { return s.store.GetPodByIP(ip) },
		func() string { return fmt.Sprintf("no live pod with IP %q", ip) })
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
	s.writeCached(w, r, kubemeta.NodeMetadata{Name: node, ObjectMeta: *meta})
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
	if len(pods) > 0 { // an empty node cannot match any monitored service
		monitored = s.monitoredServices()
	}
	for _, np := range pods {
		if !scrape.Scrapeable(np.Pod) {
			continue // finished/deleted pods can never yield targets
		}
		// Cheap pre-check before the (per-pod) enrichment work: does the pod
		// or any service selecting it opt into scraping?
		matched := s.services.Matching(np.Pod.Namespace, np.Pod.Labels)
		// Map iteration order in the services index must not decide which
		// Service a URL-deduped target is attributed to.
		sort.Slice(matched, func(i, j int) bool {
			if matched[i].Namespace != matched[j].Namespace {
				return matched[i].Namespace < matched[j].Namespace
			}
			return matched[i].Name < matched[j].Name
		})
		podAnnotated := np.Pod.Annotations[scrape.AnnotationScrape] == "true"
		svcAnnotated := false
		for _, svc := range matched {
			if svc.Annotations[scrape.AnnotationScrape] == "true" || len(monitored[svc.UID]) > 0 {
				svcAnnotated = true
				break
			}
		}
		if !podAnnotated && !svcAnnotated {
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
		// The same endpoint can be reachable via pod and service
		// annotations; keep the first occurrence (pod source wins).
		seen := make(map[string]struct{}, len(podTargets))
		for _, t := range podTargets {
			if _, dup := seen[t.URL]; dup {
				continue
			}
			seen[t.URL] = struct{}{}
			targets = append(targets, t)
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"node":    node,
		"targets": targets,
	})
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
		// Guard the multiplication against overflow, then let the shared
		// duration clamp below apply — clamping by TRUNCATED whole seconds here
		// would turn a sub-second maxWait into 0 (non-blocking) for ?wait=1.
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
func (s *Server) writeCached(w http.ResponseWriter, r *http.Request, v any) {
	if s.cacheTTL <= 0 {
		writeJSON(w, http.StatusOK, v)
		return
	}
	body, err := json.Marshal(v)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "encoding response")
		return
	}
	etag := `"` + strconv.FormatUint(fnvHash(body), 16) + `"`
	// max-age has second granularity: a sub-second TTL truncates to 0, which
	// tells the client not to cache AT ALL — the opposite of a short cache, and
	// silently (the ETag is still computed on every response). Round up so any
	// non-zero TTL caches for at least a second; 0 disables caching before we
	// get here.
	maxAge := int(s.cacheTTL.Seconds())
	if maxAge < 1 {
		maxAge = 1
	}
	h := w.Header()
	h.Set("Content-Type", "application/json")
	h.Set("Cache-Control", "max-age="+strconv.Itoa(maxAge))
	h.Set("ETag", etag)
	if match := r.Header.Get("If-None-Match"); match != "" && etagMatches(match, etag) {
		w.WriteHeader(http.StatusNotModified)
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(body)
}
