// Package server exposes the metadata store over HTTP.
package server

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cespare/xxhash/v2"

	"github.com/JohanLindvall/kubescrape/internal/logdedupe"
	"github.com/JohanLindvall/kubescrape/internal/obs"

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
	Ready <-chan struct{}
	// Secrets serves monitor endpoints' bearer-token Secrets to agents
	// (GET /v1/scrape-auth/...); nil disables the endpoint (404). Opt-in via
	// -scrape-auth-secrets — it requires secrets RBAC and ships secret
	// material over the cluster-internal HTTP channel.
	Secrets SecretReader
	// ScrapeAuthToken is the shared bearer token clients must present on
	// GET /v1/scrape-auth (`Authorization: Bearer <token>`), read from
	// -scrape-auth-token-file. One of it or ScrapeAuthTokens is REQUIRED
	// whenever Secrets is set (see Validate); both guard that route only — the
	// rest of the API carries no secret material and stays open.
	ScrapeAuthToken string
	// ScrapeAuthTokens, when set, supersedes ScrapeAuthToken: it is evaluated
	// per request and every returned token is accepted. This is what makes
	// ROTATION a non-event — the caller returns the current token plus the
	// previous one for a grace window, so re-reading agents and the re-read
	// service file never have to flip in lockstep.
	ScrapeAuthTokens func() []string
	// Log receives the handful of server-side events an agent cannot diagnose
	// from a status code alone (a Secret read that failed for a reason other
	// than "no such key"). nil uses slog.Default().
	Log *slog.Logger
}

// Validate reports a configuration that must not start the process.
//
// The one rule: a Server that can read Secrets must know the token guarding
// them. Without it GET /v1/scrape-auth would serve every Secret key a monitor
// endpoint references to anything that can reach the service — a silent,
// cluster-wide secret leak, which is exactly the failure mode a "the flag was
// not set" default must never produce.
func (c Config) Validate() error {
	if c.Secrets != nil && c.ScrapeAuthToken == "" && c.ScrapeAuthTokens == nil {
		return errors.New("-scrape-auth-secrets requires -scrape-auth-token-file: " +
			"/v1/scrape-auth serves monitor Secret keys and must not be reachable unauthenticated")
	}
	return nil
}

// ErrSecretKeyNotFound reports that the Secret exists but does not carry the
// requested key. SecretReader implementations must wrap it (%w) for that case,
// so handleScrapeAuth can answer 404 — a client-caused miss — and reserve the
// retryable 502 for failures that are the CLUSTER's: a forbidden read, a
// timeout, an unreachable API server. Collapsing the two made an RBAC denial
// indistinguishable from a typo in a monitor's secret ref.
var ErrSecretKeyNotFound = errors.New("key not in secret")

// SecretReader resolves one Secret key's value.
type SecretReader interface {
	Get(ctx context.Context, namespace, name, key string) (string, error)
}

// Server serves container metadata and node scrape targets.
type Server struct {
	secrets SecretReader
	// scrapeAuthTokens yields the tokens accepted on /v1/scrape-auth (see
	// auth.go); more than one only during a rotation grace window.
	scrapeAuthTokens func() []string
	store            *store.Store
	services         *services.Index
	monitors         *servicemonitors.Index
	resolver         MetadataResolver
	maxWait          time.Duration
	cacheTTL         time.Duration
	ready            <-chan struct{}
	now              func() time.Time
	logger           *slog.Logger

	// monMu guards the monitoredServices cache: the monitor→services match is
	// O(monitors × services) and identical across the per-node target requests,
	// so it is rebuilt only when the monitors or the Services actually change
	// (monGen/svcGen, the two indexes' change tokens). monBuilds counts
	// rebuilds for tests.
	//
	// It used to be rebuilt once per cacheTTL, which made ONE knob select both
	// the HTTP cache lifetime and this memo's: -metadata-cache-ttl=0 — an
	// operator disabling client caching for freshness — rebuilt the whole cross
	// product per REQUEST (measured 77 ms / 129 MB / 197k allocations at 50
	// monitors × 2,000 Services, plus 50 separate services.Index RLocks per
	// request contending with the informer's writes). The generations also
	// remove the up-to-TTL lag before a newly created ServiceMonitor or Service
	// produced targets: the memo is now exact rather than merely recent.
	monMu     sync.Mutex
	monCache  map[string][]monitorEndpoint
	monGen    uint64
	svcGen    uint64
	monValid  bool
	monBuilds atomic.Int64

	// targetsMu guards the per-node ETag memo (see nodeTargetsNotModified).
	// targetBuilds counts full derivations of a node's target list, for tests:
	// it is the thing the memo exists to avoid.
	targetsMu    sync.Mutex
	targetsETags map[string]nodeTargetsETag
	targetBuilds atomic.Int64

	// warnRefs throttles the per-ref scrape-auth failure log, warnShadowed the
	// per-pair shadowed-monitor one. Both are concurrency-safe on their own, so
	// they need no mutex here.
	warnRefs     *logdedupe.Table
	warnShadowed *logdedupe.Table
}

// New creates a Server.
func New(cfg Config) *Server {
	tokens := cfg.ScrapeAuthTokens
	if tokens == nil && cfg.ScrapeAuthToken != "" {
		token := cfg.ScrapeAuthToken
		tokens = func() []string { return []string{token} }
	}
	return &Server{
		secrets:          cfg.Secrets,
		scrapeAuthTokens: tokens,
		store:            cfg.Store,
		services:         cfg.Services,
		monitors:         cfg.Monitors,
		resolver:         cfg.Resolver,
		maxWait:          cfg.MaxWait,
		cacheTTL:         cfg.CacheTTL,
		ready:            cfg.Ready,
		now:              time.Now,
		logger:           cfg.Log,
		warnRefs:         logdedupe.New(maxScrapeAuthWarnRefs, scrapeAuthWarnEvery),
		warnShadowed:     logdedupe.New(maxShadowedWarnPairs, shadowWarnEvery),
	}
}

// log returns the configured logger, or the process default.
func (s *Server) log() *slog.Logger {
	if s.logger != nil {
		return s.logger
	}
	return slog.Default()
}

// scrapeAuthWarnEvery bounds how often one secret ref may log a resolution
// failure. An RBAC grant that was never added is a STEADY state, not an event:
// every agent on every node re-asks each scrape cycle, so an unthrottled line
// is a permanent flood proportional to fleet size. The counter carries the rate;
// the log only has to name the ref often enough to be found.
const scrapeAuthWarnEvery = 5 * time.Minute

// maxScrapeAuthWarnRefs bounds the throttle table. Keys come from the
// AuthSecretRefs allowlist, so they are already bounded by the indexed monitors
// — this is belt and braces against a monitor set that churns.
const maxScrapeAuthWarnRefs = 1024

// warnScrapeAuth throttles the per-ref scrape-auth failure log, emitting the
// one-time saturation notice when the table fills.
//
// The saturation POLICY — suppress further keys, never clear the table — lives
// in internal/logdedupe, along with the reason. This file had its own copy that
// cleared when full, which at cap+1 distinct refs re-fills, overflows and
// clears on every cycle: the flood the throttle exists to prevent, re-armed,
// and silently. The agent had already reached the opposite conclusion and
// written it down; now there is one table type and one answer.
func (s *Server) warnScrapeAuth(ref string, emit func()) {
	allow, saturated := s.warnRefs.Allow(ref)
	if saturated {
		s.log().Warn("scrape-auth warning dedupe table is full; further distinct refs are suppressed",
			"refs", maxScrapeAuthWarnRefs)
	}
	if allow {
		emit()
	}
}

// shadowWarnEvery bounds how often one conflicting (winner, loser) pair may
// log. Like the scrape-auth throttle, this is a STEADY state rather than an
// event: the pair is re-decided on every targets request of every agent that
// has the pod on its node, so an unthrottled line is a permanent flood
// proportional to fleet size. The counter carries the rate.
const shadowWarnEvery = 30 * time.Minute

// maxShadowedWarnPairs bounds the throttle table. Keys are monitor PAIRS, so
// they are bounded by the indexed monitors; this is belt and braces against a
// monitor set that churns.
const maxShadowedWarnPairs = 1024

// reportAuthConflict records a monitor endpoint whose auth/TLS material could
// not be merged into the target already holding the same URL on that pod:
// both declare it and it differs, so the holder's is served (see
// scrape.MergeMonitorEndpoint — every other endpoint group merges, silently).
// The scrape is now running with a credential/TLS config one of its CRs did
// not choose, which must not be discoverable only by a packet capture. The
// winner may equal the loser: one monitor's two endpoints resolving to one URL
// with different material is the same unhonoured declaration.
func (s *Server) reportAuthConflict(kind, winner, loser, url string) {
	obs.MonitorTargetShadowed.WithLabelValues(kind).Inc()
	if allow, saturated := s.warnShadowed.Allow(kind + "\x00" + winner + "\x00" + loser); allow || saturated {
		if saturated {
			s.log().Warn("conflicting-monitor warning dedupe table is full; further distinct pairs are suppressed",
				"pairs", maxShadowedWarnPairs)
		}
		if allow {
			s.log().Warn("two monitor endpoints declare different auth/TLS for one scrape URL; the first monitor's is served",
				"kind", kind, "serving", winner, "conflicting", loser, "url", url,
				"note", "the conflicting endpoint's auth/TLS is NOT applied (its other configuration merges); "+
					"further warnings for this pair are suppressed for "+shadowWarnEvery.String())
		}
	}
}

// Handler returns the HTTP routes.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/containers/{id...}", counted("/v1/containers", s.handleContainer))
	mux.HandleFunc("GET /v1/pods/{namespace}/{name}", counted("/v1/pods", s.handlePod))
	mux.HandleFunc("GET /v1/pod-uids/{uid}", counted("/v1/pod-uids", s.handlePodByUID))
	mux.HandleFunc("GET /v1/pod-ips/{ip}", counted("/v1/pod-ips", s.handlePodByIP))
	mux.HandleFunc("GET /v1/self", counted("/v1/self", s.handleSelf))
	mux.HandleFunc("GET /v1/nodes/{node}/targets", counted("/v1/nodes/targets", s.handleNodeTargets))
	mux.HandleFunc("GET /v1/nodes/{node}/metadata", counted("/v1/nodes/metadata", s.handleNodeMetadata))
	mux.HandleFunc("GET /v1/scrape-auth/{namespace}/{name}/{key}", counted("/v1/scrape-auth", s.handleScrapeAuth))
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

// maxContainerIDLen bounds the container-ID path segment
// (kubemeta.MaxContainerIDLen carries the 64-hex-runtime rationale): over it
// the request 400s up front (handlers.go writes StatusBadRequest), so hostile
// IDs never reach the store's waiter map.
const maxContainerIDLen = kubemeta.MaxContainerIDLen

// monitorEndpoint pairs a ServiceMonitor endpoint with its monitor name.
//
// The endpoint is a POINTER into the indexed monitor, which is treat-as-
// immutable and outlives this map; stampEndpoint only reads it. By value the
// pair was 304 bytes and the memo holds one per (monitor, service, endpoint) —
// a cluster-wide 50 monitors over 2,000 Services is 100,000 pairs, measured at
// 40.2 MB resident for as long as the memo lives, with both the old and the new
// map alive across a rebuild (an 80 MB transient peak) against a 128Mi request.
type monitorEndpoint struct {
	monitor  string
	endpoint *servicemonitors.Endpoint
}

// monitoredServices maps Service UIDs to the ServiceMonitor endpoints
// selecting them. It is rebuilt only when the monitor or Service index has
// changed since the last build; callers must treat it as read-only.
func (s *Server) monitoredServices() map[string][]monitorEndpoint {
	if s.monitors == nil {
		return nil
	}
	// Read the change tokens BEFORE the build: a change landing during it is
	// then recorded as unbuilt and rebuilds on the next call, rather than being
	// stamped as already-included and lost until the next unrelated change.
	monGen, svcGen := s.monitors.Generation(), s.services.Generation()
	s.monMu.Lock()
	defer s.monMu.Unlock()
	if !s.monValid || s.monGen != monGen || s.svcGen != svcGen {
		s.monCache = s.buildMonitoredServices()
		s.monGen, s.svcGen, s.monValid = monGen, svcGen, true
	}
	return s.monCache
}

// buildMonitoredServices resolves the monitor→services match from scratch.
func (s *Server) buildMonitoredServices() map[string][]monitorEndpoint {
	s.monBuilds.Add(1)
	out := map[string][]monitorEndpoint{}
	for _, m := range s.monitors.All() {
		name := m.Namespace + "/" + m.Name
		for _, svc := range s.services.All(m.ServiceNamespaces()) {
			if !m.Selector.Matches(labels.Set(svc.Labels)) {
				continue
			}
			for i := range m.Endpoints {
				out[svc.UID] = append(out[svc.UID], monitorEndpoint{monitor: name, endpoint: &m.Endpoints[i]})
			}
		}
	}
	return out
}

// podMonitorRef is one indexed PodMonitor with its "namespace/name" already
// rendered: the name is stamped on every target the monitor produces, and
// building it per (pod, monitor) is one allocation per pair on the targets path.
type podMonitorRef struct {
	monitor *servicemonitors.PodMonitor
	name    string
}

// allPodMonitors snapshots the indexed PodMonitors for one request.
func (s *Server) allPodMonitors() []podMonitorRef {
	if s.monitors == nil {
		return nil
	}
	all := s.monitors.PodMonitors()
	out := make([]podMonitorRef, 0, len(all))
	for _, m := range all {
		out = append(out, podMonitorRef{monitor: m, name: m.Namespace + "/" + m.Name})
	}
	return out
}

// podMonitorsFor filters the request's PodMonitors down to the ones selecting a
// pod (namespace + label selector), appending into a caller-owned scratch slice.
func podMonitorsFor(pod kubemeta.Pod, all []podMonitorRef, out []podMonitorRef) []podMonitorRef {
	for _, ref := range all {
		if nss := ref.monitor.PodNamespaces(); nss != nil && !slices.Contains(nss, pod.Namespace) {
			continue
		}
		if !ref.monitor.Selector.Matches(labels.Set(pod.Labels)) {
			continue
		}
		out = append(out, ref)
	}
	return out
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

// requireReady gates a handler on the initial informer sync, reporting whether
// the caller may proceed; false means the retryable JSON 503 was already
// written. retryAfter, when non-empty, rides as a Retry-After header on that
// 503 (handleScrapeAuth sends "1"; the other routes historically send none).
//
// ONE policy for every instant (non-blocking) gate, because the copies drifted
// once: handleScrapeAuth skipped the gate entirely and answered a definitive
// 403 ("secret is not referenced by any monitor endpoint") during the sync
// window — its allowlist comes from a LIVE servicemonitors.Index that exists
// but is merely EMPTY until the informers sync, so the assertion could not yet
// be made, and a startup race read as a monitor misconfiguration. Every other
// handler was returning the retryable 503 this helper now owns.
//
// handleContainer is the deliberate exception, not a fifth copy: it spends its
// per-request wait budget on readiness first (waitReady) rather than failing
// fast, because its caller legitimately blocks for metadata to appear.
func (s *Server) requireReady(w http.ResponseWriter, retryAfter string) bool {
	if s.isReady() {
		return true
	}
	if retryAfter != "" {
		w.Header().Set("Retry-After", retryAfter)
	}
	writeError(w, http.StatusServiceUnavailable, "informer caches not synced")
	return false
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

// bodyHash is the ETag digest. xxhash rather than hash/fnv: FNV-1a is a
// byte-at-a-time loop (~1.2 GB/s) and this runs over the FULL body of every
// cached response, including every 304 revalidation — most visibly on
// /v1/nodes/{node}/targets, which re-serializes every pod document on the node
// each scrape cycle. ETags are opaque to clients (etagMatches only string-
// compares, W/ prefix stripped), so the digest function is a free choice; the
// only visible effect is one full 200 per cached client at the upgrade
// boundary, exactly as any other ETag change would produce.
func bodyHash(b []byte) uint64 { return xxhash.Sum64(b) }

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}
