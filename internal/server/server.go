// Package server exposes the metadata store over HTTP.
package server

import (
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/JohanLindvall/kubescrape/internal/obs"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"

	"github.com/JohanLindvall/kubescrape/internal/kubemeta"
	"github.com/JohanLindvall/kubescrape/internal/scrape"
	"github.com/JohanLindvall/kubescrape/internal/servicemonitors"
	"github.com/JohanLindvall/kubescrape/internal/services"
	"github.com/JohanLindvall/kubescrape/internal/store"
)

// MetadataResolver enriches pods with related-object metadata: the full
// owner chain, the pod's namespace metadata and node metadata.
type MetadataResolver interface {
	Resolve(namespace string, refs []metav1.OwnerReference) []kubemeta.Owner
	Namespace(name string) *kubemeta.ObjectMeta
	Node(name string) *kubemeta.ObjectMeta
}

// Config configures the HTTP server.
type Config struct {
	Store    *store.Store
	Services *services.Index
	// Monitors serves ServiceMonitor-derived targets (nil = disabled).
	Monitors *servicemonitors.Index
	Resolver MetadataResolver
	// MaxWait is the default and maximum time a container lookup may block
	// waiting for metadata to appear. Requests may shorten it with ?wait=.
	MaxWait time.Duration
	// CacheTTL sets the max-age on metadata responses (Cache-Control + ETag),
	// letting the agent's HTTP client serve repeat lookups locally. 0 disables
	// cache headers.
	CacheTTL time.Duration
	// Ready is closed once the informer caches have synced.
	Ready  <-chan struct{}
	Logger *slog.Logger
}

// Server serves container metadata and node scrape targets.
type Server struct {
	store    *store.Store
	services *services.Index
	monitors *servicemonitors.Index
	resolver MetadataResolver
	maxWait  time.Duration
	cacheTTL time.Duration
	ready    <-chan struct{}
	log      *slog.Logger
}

// New creates a Server.
func New(cfg Config) *Server {
	log := cfg.Logger
	if log == nil {
		log = slog.Default()
	}
	return &Server{
		store:    cfg.Store,
		services: cfg.Services,
		monitors: cfg.Monitors,
		resolver: cfg.Resolver,
		maxWait:  cfg.MaxWait,
		cacheTTL: cfg.CacheTTL,
		ready:    cfg.Ready,
		log:      log,
	}
}

// Handler returns the HTTP routes.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/containers/{id...}", counted("/v1/containers", s.handleContainer))
	mux.HandleFunc("GET /v1/pods/{namespace}/{name}", counted("/v1/pods", s.handlePod))
	mux.HandleFunc("GET /v1/pod-uids/{uid}", counted("/v1/pod-uids", s.handlePodByUID))
	mux.HandleFunc("GET /v1/pod-ips/{ip}", counted("/v1/pod-ips", s.handlePodByIP))
	mux.HandleFunc("GET /v1/nodes/{node}/targets", counted("/v1/nodes/targets", s.handleNodeTargets))
	mux.HandleFunc("GET /v1/nodes/{node}/metadata", counted("/v1/nodes/metadata", s.handleNodeMetadata))
	mux.Handle("GET /metrics", obs.Handler())
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, _ *http.Request) {
		if !s.isReady() {
			http.Error(w, "informer caches not synced", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	return mux
}

// counted wraps a handler with the per-pattern request counter.
func counted(pattern string, h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		rec := &statusRecorder{ResponseWriter: w, code: http.StatusOK}
		h(rec, r)
		obs.HTTPRequests.WithLabelValues(pattern, strconv.Itoa(rec.code)).Inc()
	}
}

type statusRecorder struct {
	http.ResponseWriter
	code int
}

func (r *statusRecorder) WriteHeader(code int) {
	r.code = code
	r.ResponseWriter.WriteHeader(code)
}

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
	id := r.PathValue("id")
	if kubemeta.NormalizeContainerID(id) == "" {
		writeError(w, http.StatusBadRequest, "empty container id")
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

	res, ok := s.store.GetContainer(ctx, id)
	if !ok {
		writeError(w, http.StatusNotFound, fmt.Sprintf("container %q not found", kubemeta.NormalizeContainerID(id)))
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
func (s *Server) handlePod(w http.ResponseWriter, r *http.Request) {
	if !s.isReady() {
		writeError(w, http.StatusServiceUnavailable, "informer caches not synced")
		return
	}
	namespace, name := r.PathValue("namespace"), r.PathValue("name")
	np, ok := s.store.GetPodByName(namespace, name)
	if !ok {
		writeError(w, http.StatusNotFound, fmt.Sprintf("pod %s/%s not found", namespace, name))
		return
	}
	s.enrich(&np.Pod, np.OwnerRefs)
	s.writeCached(w, r, np.Pod)
}

// handlePodByUID serves GET /v1/pod-uids/{uid}: full metadata for one pod
// looked up by UID (used by the OTLP ingest enricher to attribute pushed
// telemetry). Deleted pods stay resolvable until their tombstone expires.
func (s *Server) handlePodByUID(w http.ResponseWriter, r *http.Request) {
	if !s.isReady() {
		writeError(w, http.StatusServiceUnavailable, "informer caches not synced")
		return
	}
	uid := r.PathValue("uid")
	np, ok := s.store.GetPodByUID(uid)
	if !ok {
		writeError(w, http.StatusNotFound, fmt.Sprintf("pod uid %q not found", uid))
		return
	}
	s.enrich(&np.Pod, np.OwnerRefs)
	s.writeCached(w, r, np.Pod)
}

// handlePodByIP serves GET /v1/pod-ips/{ip}: the LIVE pod owning a pod IP
// (the agent's opt-in peer-IP attribution for pushed OTLP). Deleted pods and
// hostNetwork pods never resolve.
func (s *Server) handlePodByIP(w http.ResponseWriter, r *http.Request) {
	if !s.isReady() {
		writeError(w, http.StatusServiceUnavailable, "informer caches not synced")
		return
	}
	ip := r.PathValue("ip")
	np, ok := s.store.GetPodByIP(ip)
	if !ok {
		writeError(w, http.StatusNotFound, fmt.Sprintf("no live pod with IP %q", ip))
		return
	}
	s.enrich(&np.Pod, np.OwnerRefs)
	s.writeCached(w, r, np.Pod)
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
	monitored := s.monitoredServices()
	for _, np := range pods {
		// Cheap pre-check before the (per-pod) enrichment work: does the pod
		// or any service selecting it opt into scraping?
		matched := s.services.Matching(np.Pod.Namespace, np.Pod.Labels)
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

// monitorEndpoint pairs a ServiceMonitor endpoint with its monitor name.
type monitorEndpoint struct {
	monitor  string
	endpoint servicemonitors.Endpoint
}

// monitoredServices maps Service UIDs to the ServiceMonitor endpoints
// selecting them, resolved once per request.
func (s *Server) monitoredServices() map[string][]monitorEndpoint {
	if s.monitors == nil {
		return nil
	}
	out := map[string][]monitorEndpoint{}
	for _, m := range s.monitors.All() {
		for _, svc := range s.services.All(m.ServiceNamespaces()) {
			if !m.Selector.Matches(labels.Set(svc.Labels)) {
				continue
			}
			name := m.Namespace + "/" + m.Name
			for _, ep := range m.Endpoints {
				out[svc.UID] = append(out[svc.UID], monitorEndpoint{monitor: name, endpoint: ep})
			}
		}
	}
	return out
}

// enrich fills in owner-chain and namespace metadata on a pod.
func (s *Server) enrich(pod *kubemeta.Pod, refs []metav1.OwnerReference) {
	pod.Owners = s.resolver.Resolve(pod.Namespace, refs)
	pod.NamespaceMetadata = s.resolver.Namespace(pod.Namespace)
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
		d = time.Duration(secs) * time.Second
	}
	if d < 0 {
		return 0, fmt.Errorf("wait parameter must not be negative")
	}
	if d > s.maxWait {
		d = s.maxWait
	}
	return d, nil
}

func (s *Server) isReady() bool {
	select {
	case <-s.ready:
		return true
	default:
		return false
	}
}

func (s *Server) waitReady(ctx context.Context) bool {
	select {
	case <-s.ready:
		return true
	default:
	}
	select {
	case <-s.ready:
		return true
	case <-ctx.Done():
		return false
	}
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
	h := w.Header()
	h.Set("Content-Type", "application/json")
	h.Set("Cache-Control", "max-age="+strconv.Itoa(int(s.cacheTTL.Seconds())))
	h.Set("ETag", etag)
	if match := r.Header.Get("If-None-Match"); match != "" && match == etag {
		w.WriteHeader(http.StatusNotModified)
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(body)
}

func fnvHash(b []byte) uint64 {
	h := fnv.New64a()
	_, _ = h.Write(b)
	return h.Sum64()
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}
