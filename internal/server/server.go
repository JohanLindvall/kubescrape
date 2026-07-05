// Package server exposes the metadata store over HTTP.
package server

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/JohanLindvall/kubescrape/internal/kubemeta"
	"github.com/JohanLindvall/kubescrape/internal/scrape"
	"github.com/JohanLindvall/kubescrape/internal/services"
	"github.com/JohanLindvall/kubescrape/internal/store"
)

// MetadataResolver enriches pods with related-object metadata: the full
// owner chain and the pod's namespace metadata.
type MetadataResolver interface {
	Resolve(namespace string, refs []metav1.OwnerReference) []kubemeta.Owner
	Namespace(name string) *kubemeta.ObjectMeta
}

// Config configures the HTTP server.
type Config struct {
	Store    *store.Store
	Services *services.Index
	Resolver MetadataResolver
	// MaxWait is the default and maximum time a container lookup may block
	// waiting for metadata to appear. Requests may shorten it with ?wait=.
	MaxWait time.Duration
	// Ready is closed once the informer caches have synced.
	Ready  <-chan struct{}
	Logger *slog.Logger
}

// Server serves container metadata and node scrape targets.
type Server struct {
	store    *store.Store
	services *services.Index
	resolver MetadataResolver
	maxWait  time.Duration
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
		resolver: cfg.Resolver,
		maxWait:  cfg.MaxWait,
		ready:    cfg.Ready,
		log:      log,
	}
}

// Handler returns the HTTP routes.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/containers/{id...}", s.handleContainer)
	mux.HandleFunc("GET /v1/nodes/{node}/targets", s.handleNodeTargets)
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
	writeJSON(w, http.StatusOK, kubemeta.ContainerMetadata{
		ContainerID: res.Container.ID,
		Container:   res.Container,
		Pod:         res.Pod,
	})
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
	for _, np := range pods {
		// Cheap pre-check before the (per-pod) enrichment work: does the pod
		// or any service selecting it opt into scraping?
		matched := s.services.Matching(np.Pod.Namespace, np.Pod.Labels)
		podAnnotated := np.Pod.Annotations[scrape.AnnotationScrape] == "true"
		svcAnnotated := false
		for _, svc := range matched {
			if svc.Annotations[scrape.AnnotationScrape] == "true" {
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

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}
