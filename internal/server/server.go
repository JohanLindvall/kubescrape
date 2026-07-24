// Package server exposes the metadata store over HTTP.
package server

import (
	"context"
	"hash/fnv"
	"log/slog"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/JohanLindvall/kubescrape/internal/obs"
	"github.com/JohanLindvall/kubescrape/internal/scrape"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"

	"github.com/JohanLindvall/kubescrape/internal/servicemonitors"
	"github.com/JohanLindvall/kubescrape/internal/services"
	"github.com/JohanLindvall/kubescrape/internal/store"
	"github.com/JohanLindvall/kubescrape/pkg/kubemeta"
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
	// Secrets serves monitor endpoints' bearer-token Secrets to agents
	// (GET /v1/scrape-auth/...); nil disables the endpoint (404). Opt-in via
	// -scrape-auth-secrets — it requires secrets RBAC and ships secret
	// material over the cluster-internal HTTP channel.
	Secrets SecretReader
}

// SecretReader resolves one Secret key's value.
type SecretReader interface {
	Get(ctx context.Context, namespace, name, key string) (string, error)
}

// Server serves container metadata and node scrape targets.
type Server struct {
	secrets  SecretReader
	store    *store.Store
	services *services.Index
	monitors *servicemonitors.Index
	resolver MetadataResolver
	maxWait  time.Duration
	cacheTTL time.Duration
	ready    <-chan struct{}
	log      *slog.Logger
	now      func() time.Time

	// monMu guards the monitoredServices cache: the monitor→services match
	// is O(monitors × services) and identical across the per-node target
	// requests, so it is rebuilt at most once per cacheTTL (the staleness
	// horizon the metadata cache already accepts). monBuilds counts rebuilds
	// for tests.
	monMu      sync.Mutex
	monCache   map[string][]monitorEndpoint
	monBuiltAt time.Time
	monBuilds  atomic.Int64
}

// New creates a Server.
func New(cfg Config) *Server {
	log := cfg.Logger
	if log == nil {
		log = slog.Default()
	}
	return &Server{
		secrets:  cfg.Secrets,
		store:    cfg.Store,
		services: cfg.Services,
		monitors: cfg.Monitors,
		resolver: cfg.Resolver,
		maxWait:  cfg.MaxWait,
		cacheTTL: cfg.CacheTTL,
		ready:    cfg.Ready,
		log:      log,
		now:      time.Now,
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
	mux.HandleFunc("GET /v1/scrape-auth/{namespace}/{name}/{key}", counted("/v1/scrape-auth", s.handleScrapeAuth))
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	mux.Handle("GET /metrics", obs.RuntimeHandler())
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

// HTTPServer wraps s.Handler() in an http.Server with hardened timeouts. The
// metadata service fronts a whole DaemonSet fleet, so a single slow, buggy or
// hostile client must never pin connections and goroutines indefinitely:
//
//   - ReadHeaderTimeout kills Slowloris-style header trickling.
//   - IdleTimeout reaps parked keep-alive connections.
//   - ReadTimeout and WriteTimeout bound trickled request bodies (which the
//     handlers never read, but net/http drains before connection reuse) and
//     stuck response writes. Both MUST exceed MaxWait: the container endpoint
//     legitimately holds a request for up to MaxWait, WriteTimeout's clock
//     starts when the request headers are read, and a ReadTimeout shorter
//     than the handler's runtime cancels the request context (net/http's
//     background read hits the whole-request read deadline mid-handler),
//     which would abort legitimate waits early. Hence MaxWait + slack, never
//     a fixed constant.
func (s *Server) HTTPServer(addr string) *http.Server {
	const slack = 30 * time.Second
	return &http.Server{
		Addr:              addr,
		Handler:           s.Handler(),
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       s.maxWait + slack,
		WriteTimeout:      s.maxWait + slack,
		IdleTimeout:       120 * time.Second,
	}
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

// maxContainerIDLen bounds the container-ID path segment. Real runtime IDs
// are 64 hex characters (plus an optional scheme prefix, stripped before this
// check); anything longer is rejected up front so hostile IDs never reach the
// store's waiter map.
const maxContainerIDLen = 256

// monitorEndpoint pairs a ServiceMonitor endpoint with its monitor name.
type monitorEndpoint struct {
	monitor  string
	endpoint servicemonitors.Endpoint
}

// monitoredServices maps Service UIDs to the ServiceMonitor endpoints
// selecting them. The map is rebuilt at most once per cacheTTL (with a zero
// TTL, once per request); callers must treat it as read-only.
func (s *Server) monitoredServices() map[string][]monitorEndpoint {
	if s.monitors == nil {
		return nil
	}
	if s.cacheTTL <= 0 {
		return s.buildMonitoredServices()
	}
	s.monMu.Lock()
	defer s.monMu.Unlock()
	if now := s.now(); s.monBuiltAt.IsZero() || now.Sub(s.monBuiltAt) >= s.cacheTTL {
		s.monCache = s.buildMonitoredServices()
		s.monBuiltAt = now
	}
	return s.monCache
}

// buildMonitoredServices resolves the monitor→services match from scratch.
func (s *Server) buildMonitoredServices() map[string][]monitorEndpoint {
	s.monBuilds.Add(1)
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

// podMonitorsFor returns the PodMonitors selecting a pod (namespace +
// label selector).
func (s *Server) podMonitorsFor(pod kubemeta.Pod) []*servicemonitors.PodMonitor {
	if s.monitors == nil {
		return nil
	}
	var out []*servicemonitors.PodMonitor
	for _, m := range s.monitors.PodMonitors() {
		if nss := m.PodNamespaces(); nss != nil && !slices.Contains(nss, pod.Namespace) {
			continue
		}
		if !m.Selector.Matches(labels.Set(pod.Labels)) {
			continue
		}
		out = append(out, m)
	}
	return out
}

// proberTarget pairs a Probe with the prober port resolved on one pod.
type proberTarget struct {
	probe *servicemonitors.Probe
	port  int32
}

// probesFor returns the Probes whose PROBER Service is backed by this pod:
// probing stays node-local by scheduling each probe onto the agents running
// a prober replica.
func (s *Server) probesFor(pod kubemeta.Pod) []proberTarget {
	if s.monitors == nil {
		return nil
	}
	var out []proberTarget
	for _, p := range s.monitors.Probes() {
		if p.ProberNS != pod.Namespace {
			continue
		}
		for _, svc := range s.services.Matching(pod.Namespace, pod.Labels) {
			if svc.Name != p.ProberService {
				continue
			}
			if port, ok := proberPodPort(pod, svc, p); ok {
				out = append(out, proberTarget{probe: p, port: port})
			}
			break
		}
	}
	return out
}

// proberPodPort resolves the pod port the prober listens on: an explicit
// numeric port from prober.url, a named service port, or the service's only
// port.
func proberPodPort(pod kubemeta.Pod, svc *services.Service, p *servicemonitors.Probe) (int32, bool) {
	if p.ProberPort != nil {
		if n, ok := scrape.MonitorPortNumber(*p.ProberPort); ok {
			return n, true
		}
		for _, sp := range svc.Ports {
			if sp.Name == p.ProberPort.StrVal {
				return scrape.TargetPodPort(pod, sp)
			}
		}
		return 0, false
	}
	if len(svc.Ports) == 1 {
		return scrape.TargetPodPort(pod, svc.Ports[0])
	}
	return 0, false
}

// enrich fills in owner-chain and namespace metadata on a pod.
func (s *Server) enrich(pod *kubemeta.Pod, refs []metav1.OwnerReference) {
	pod.Owners = s.resolver.Resolve(pod.Namespace, refs)
	pod.NamespaceMetadata = s.resolver.Namespace(pod.Namespace)
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

// etagMatches evaluates an If-None-Match header against the current entity
// tag per RFC 9110: a comma-separated list of entity tags compared weakly (a
// W/ prefix is ignored), with "*" matching any current representation. Our
// ETags are quoted hex with no embedded commas, so splitting on commas is
// exact.
func etagMatches(header, etag string) bool {
	for _, candidate := range strings.Split(header, ",") {
		candidate = strings.TrimSpace(candidate)
		if candidate == "*" {
			return true
		}
		if strings.TrimPrefix(candidate, "W/") == etag {
			return true
		}
	}
	return false
}

func fnvHash(b []byte) uint64 {
	h := fnv.New64a()
	_, _ = h.Write(b)
	return h.Sum64()
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}
