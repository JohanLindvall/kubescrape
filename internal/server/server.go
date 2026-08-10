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

	// inFlight counts /v1 requests currently inside a handler. It exists for
	// ONE line: when http.Server.Shutdown hits its budget it stops waiting and
	// returns, leaving those handlers running — and the process exit a moment
	// later is what cuts their connections without a response. That used to be
	// completely silent: a parked container lookup got "Empty reply from
	// server" and the process exited 0 with nothing logged. The number is what
	// makes the warning actionable. Two atomics on a path that already bumps a
	// counter vec; the health and debug routes are not wrapped and never block.
	inFlight atomic.Int64

	// draining is closed by Drain. It releases the OTHER place a container
	// lookup parks: waitReady, which holds the request for its whole wait
	// budget while the initial informer sync is outstanding. The store's waiter
	// is the one everybody thinks of, but a SIGTERM before the caches sync
	// never reaches it — and that is the case most correlated with an
	// API-server outage, i.e. exactly when a rollout is likeliest to be killed
	// mid-startup. readyParked is how many requests are sitting there, for the
	// count Drain reports.
	drainOnce   sync.Once
	draining    chan struct{}
	readyParked atomic.Int64
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
		draining:         make(chan struct{}),
	}
}

// Drain releases every request parked waiting for metadata and refuses the
// later ones, returning how many were released. Idempotent; safe to call from
// any goroutine.
//
// It covers BOTH parking spots a container lookup has, which is the whole
// point: the store's per-ID waiter (Store.Drain) and this Server's wait for the
// initial informer sync (waitReady). The second one is easy to forget and is
// the one that bites on the worst path — a SIGTERM arriving before the caches
// sync leaves no store waiters at all, because no request has got that far, so
// draining only the store would still leave every lookup parked for its full
// wait budget and cut without a response when the process exits.
//
// It must run BEFORE http.Server.Shutdown: Shutdown WAITS for these handlers
// and, at its deadline, returns without closing anything, so a request left
// parked is cut by the process exit rather than answered.
//
// Idempotent means the COUNT too, not just the close: a second call reports 0.
// The whole body is inside the guard because the number is a log line's — the
// caller logs "released blocked container lookups" when it is nonzero — and a
// readyParked read left outside would let a second Drain re-report parks the
// first call had already released and re-log the loss they represent. The
// store's half returns 0 on its own second call; this keeps the two halves
// answering the same way.
func (s *Server) Drain() int {
	released := 0
	s.drainOnce.Do(func() {
		// Read the readiness parks BEFORE the close: they wake the instant the
		// channel closes and decrement on their way out, so a count taken
		// afterwards shrinks while it is being read. A request parking in that
		// same instant is missed instead — neither direction is worth a lock on
		// the request path. The store's half IS exact, being taken under its
		// write lock.
		released = int(s.readyParked.Load())
		close(s.draining)
		if s.store != nil {
			released += s.store.Drain()
		}
	})
	return released
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
	mux.HandleFunc("GET /v1/containers/{id...}", s.counted("/v1/containers", s.handleContainer))
	mux.HandleFunc("GET /v1/pods/{namespace}/{name}", s.counted("/v1/pods", s.handlePod))
	mux.HandleFunc("GET /v1/pod-uids/{uid}", s.counted("/v1/pod-uids", s.handlePodByUID))
	mux.HandleFunc("GET /v1/pod-ips/{ip}", s.counted("/v1/pod-ips", s.handlePodByIP))
	mux.HandleFunc("GET /v1/self", s.counted("/v1/self", s.handleSelf))
	mux.HandleFunc("GET /v1/nodes/{node}/targets", s.counted("/v1/nodes/targets", s.handleNodeTargets))
	mux.HandleFunc("GET /v1/nodes/{node}/metadata", s.counted("/v1/nodes/metadata", s.handleNodeMetadata))
	mux.HandleFunc("GET /v1/explain/{namespace}/{name}", s.counted("/v1/explain", s.handleExplain))
	mux.HandleFunc("GET /v1/scrape-auth/{namespace}/{name}/{key}", s.counted("/v1/scrape-auth", s.handleScrapeAuth))
	// The debug homepage (forms for the parameterised routes above), plus a
	// root redirect so a bare port-forward lands somewhere useful.
	mux.HandleFunc("GET /debug", s.handleDebugHome)
	mux.HandleFunc("GET /debug/{$}", s.handleDebugHome)
	mux.HandleFunc("GET /{$}", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/debug", http.StatusTemporaryRedirect)
	})
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	// /readyz reports the INITIAL sync and nothing after it, on purpose: see
	// the latch comment in cmd/kubescrape's run. An API server that goes away
	// later must not flip this to 503 — that would delete the Service's
	// endpoints and cut every agent off a cache that is still serving useful
	// data. kubescrape_apiserver_reachable is where that condition is reported.
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, _ *http.Request) {
		if !s.isReady() {
			http.Error(w, errNotSynced.Error(), http.StatusServiceUnavailable)
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

// counted wraps a handler with the per-pattern request counter and the
// in-flight gauge shutdown reports from.
func (s *Server) counted(pattern string, h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		s.inFlight.Add(1)
		defer s.inFlight.Add(-1)
		rec := &statusRecorder{ResponseWriter: w, code: http.StatusOK}
		h(rec, r)
		obs.HTTPRequests.WithLabelValues(pattern, strconv.Itoa(rec.code)).Inc()
	}
}

// InFlight reports how many /v1 requests are inside a handler right now. Read
// by the shutdown path to say how many were still running when the budget ran
// out — and so will be cut, without a response, by the process exit.
func (s *Server) InFlight() int64 { return s.inFlight.Load() }

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
	// namespaces is monitor.PodNamespaces() resolved ONCE per request: the
	// common no-namespaceSelector shape allocates a one-element slice per
	// call, and podMonitorsFor runs per (pod, monitor) pair on the targets
	// hot path — the same per-pair cost `name` was hoisted for.
	namespaces []string
}

// allPodMonitors snapshots the indexed PodMonitors for one request.
func (s *Server) allPodMonitors() []podMonitorRef {
	if s.monitors == nil {
		return nil
	}
	all := s.monitors.PodMonitors()
	out := make([]podMonitorRef, 0, len(all))
	for _, m := range all {
		out = append(out, podMonitorRef{monitor: m, name: m.Namespace + "/" + m.Name, namespaces: m.PodNamespaces()})
	}
	return out
}

// podMonitorsFor filters the request's PodMonitors down to the ones selecting a
// pod (namespace + label selector), appending into a caller-owned scratch slice.
func podMonitorsFor(pod kubemeta.Pod, all []podMonitorRef, out []podMonitorRef) []podMonitorRef {
	for _, ref := range all {
		if ref.namespaces != nil && !slices.Contains(ref.namespaces, pod.Namespace) {
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
	writeError(w, http.StatusServiceUnavailable, errNotSynced.Error())
	return false
}

// waitReady spends a container lookup's wait budget on the initial informer
// sync, returning nil once the caches are synced.
//
// It is the SECOND place such a request parks (the store's per-ID waiter is the
// first), and it is drained the same way: Drain closes s.draining and every
// parked request answers errDraining — a retryable 503 — instead of holding the
// handler until the process exits and cuts it without a status.
func (s *Server) waitReady(ctx context.Context) error {
	select {
	case <-s.ready:
		return nil
	default:
	}
	s.readyParked.Add(1)
	defer s.readyParked.Add(-1)
	select {
	case <-s.ready:
		return nil
	case <-s.draining:
		// Both may be closed: a drain that arrives just as the caches sync must
		// still serve the request, so readiness wins the race explicitly rather
		// than by select's coin flip.
		if s.isReady() {
			return nil
		}
		return errDraining
	case <-ctx.Done():
		return errNotSynced
	}
}

// The two refusals waitReady can produce. Both are retryable 503s, and they say
// different things to whoever is holding the request: errNotSynced means this
// pod is still filling its caches (wait), errDraining means this pod is going
// away (retry — the next pod behind the Service can answer at once).
var (
	errNotSynced = errors.New("informer caches not synced")
	errDraining  = errors.New("server is shutting down")
)

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
