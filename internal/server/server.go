// Package server exposes the metadata store over HTTP.
package server

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/JohanLindvall/haste/xxh3"

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

	// pmMu guards the PodMonitor snapshot, held on the monitor index's change
	// token for the same reason monCache is: the list is identical across every
	// node and every request until a monitor changes, and allPodMonitors was
	// hoisted out of the per-pod loop but not out of the per-REQUEST path — so
	// a 200-PodMonitor cluster re-sorted 200 pointers and minted 200 name
	// strings and 200 namespace slices on every agent's poll (measured 15,576 B
	// and 204 allocations per request at 200 monitors, for an answer that is a
	// pure function of the index contents). pmBuilds counts snapshots for tests.
	pmMu     sync.Mutex
	pmCache  []podMonitorRef
	pmGen    uint64
	pmValid  bool
	pmBuilds atomic.Int64

	// targetsMu guards the per-node ETag memo (see nodeTargetsNotModified).
	// targetBuilds counts full derivations of a node's target list, for tests:
	// it is the thing the memo exists to avoid.
	targetsMu    sync.Mutex
	targetsETags map[string]nodeTargetsETag
	targetBuilds atomic.Int64

	// svcSelectorEvals counts Service selector evaluations on the targets path,
	// for tests: it is the term that used to be O(pods × services), and the
	// property worth pinning is its SHAPE, which nothing else can see. A
	// wall-clock or allocation assertion would pass at any fixture size the
	// scan happens to be cheap at. Accumulated per request and added once, so
	// the count costs one atomic per derivation rather than one per pod.
	svcSelectorEvals atomic.Int64

	// warnRefs throttles the per-ref scrape-auth failure log, warnShadowed the
	// per-pair shadowed-monitor one and warnCollide the per-(pod, host:port)
	// colliding-identity one. All three are concurrency-safe on their own, so
	// they need no mutex here.
	warnRefs     *logdedupe.Table
	warnShadowed *logdedupe.Table
	warnCollide  *logdedupe.Table

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
		warnCollide:      logdedupe.New(maxCollisionWarnKeys, collisionWarnEvery),
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

// maxHeaderBytes bounds the WIRE size of a request head. Over it net/http writes
// its own 431 and never reaches a handler, and the same limit covers the request
// LINE as well as the header block.
//
// It is not the ADMISSION, and the gap is wider than net/http's documented slop.
// Server.initialReadLimitSize adds 4 KiB, which puts a head arriving on a FRESH
// connection at 12288 bytes admitted and 12289 refused. On a REUSED one the
// limit is set on the connReader, which counts bytes taken from the SOCKET, and
// a fill issued while the PREVIOUS request's head was being parsed can already
// have pulled up to a whole 4 KiB bufio buffer of this head in — bytes that
// limit never sees. Measured against this package's own listener: behind a
// 34-byte prelude the same server admits 16350 and refuses 16351, tracking
// 12288 + 4096 - len(prelude) byte for byte. So the real admission is ~16 KB,
// and that is the wire term everything below is stated against
// (internal/server/waitercost_test.go's maxAdmittedHead re-derives the number
// from a live listener rather than from this arithmetic).
//
// 8 KiB is the bound nginx and Apache have shipped for years, and the fattest
// thing this API legitimately carries is a ServiceAccount JWT of about 1 KB on
// the one authenticated route.
//
// That refusal is INVISIBLE to kubescrape_http_requests_total, and there is no
// way to make it otherwise: net/http writes the 431 from its own connection
// loop, before the handler chain this process installs exists for that request.
// An operator looking for evidence of it reads the client's side, or the ingress
// in front. (Refusing a second, WIDTH-based half here and counting THAT was
// tried and removed: it made the metric look like the refusal signal while
// carrying only whichever half a hostile sender did not use.)
//
// It is a wire bound and NOT a cost bound, and nothing here pretends otherwise
// any more. What a parked request COSTS is bounded by releaseParkedHead
// instead — see store.WaiterCostBytes for why the cost has to be bounded at
// all — and the reason it cannot be bounded by counting anything on the parsed
// request is worth writing down, because two separate attempts at counting
// header fields failed at it:
//
// net/http's parse expands a head by 20 to 30 times, and most of that expansion
// is INVISIBLE to any arithmetic over the request it hands the handler. Measured
// as retained heap per parsed request (internal/server, go1.26, amd64):
//
//	head on the wire                            r.Header holds   retained
//	2242 distinct 4-byte lines (12 KiB)         2242 keys         239 KB
//	one key repeated 1024x (4 KiB!)             1 key             124 KB
//	Transfer-Encoding + a 12 KiB Trailer list   0 keys            217 KB
//	Transfer-Encoding + 1000 `Trailer: a` lines 0 keys             23 KB
//
// The bottom two are the shape a field count cannot see AT ALL: net/http deletes
// Host, Transfer-Encoding and Trailer from r.Header (request.go's readRequest,
// transfer.go's parseTransferEncoding and fixTrailer) and expands the trailer
// LIST into r.Trailer, so a two-line header block leaves len(r.Header) == 0
// while the request retains a fifth of a megabyte. And the retention is not only
// the entries: net/textproto pre-sizes the header map and its value-string block
// from a peek at the upcoming LINE count (readMIMEHeader/upcomingHeaderKeys, up
// to 1000 lines), and that capacity survives every one of those deletions —
// which is how a request with nothing left to price still retains 23 KB.
//
// So there is no arithmetic over r.Header, r.Trailer and the URL that bounds
// this. What bounds it is not HOLDING it.
const maxHeaderBytes = 8 << 10

// releaseParkedHead drops the sender-chosen parse products before a handler
// PARKS, so that what a parked request costs is a property of this process
// rather than of the request — which is what makes store.DefaultMaxWaiters, a
// COUNT, a memory bound. wildcards names the route's path wildcards, whose
// matched values are one of those products (see below).
//
// Measured on the shapes tabulated at maxHeaderBytes, as retained heap per
// parsed request: 239 KB, 124 KB, 217 KB and 23 KB before, 1.3-2.2 KB after.
//
// The HEADER block was only half of it, and the half that is easy to see. The
// other half is the REQUEST LINE, and every copy of it is retained by something
// that looks harmless:
//
//   - net/textproto reads the line as ONE string, and r.Method, r.RequestURI,
//     r.Proto, r.URL.RawPath and r.URL.RawQuery are all SLICES of it — a Go
//     string slice keeps its whole backing array alive, so a three-byte
//     r.Method pins the whole 16 KB.
//   - url.Parse's setPath UNESCAPES the path into a second copy whenever it
//     contains a %-escape (and ServeMux's multi-wildcard match pathUnescapes it
//     into a third, r.matches, reachable only through SetPathValue).
//   - kubemeta.NormalizeContainerID returns what follows the LAST colon, which
//     is again a slice: a well-formed 64-hex id parked as the store's waiter
//     key can pin the whole path it was cut out of (handleContainer clones it
//     for that reason).
//
// So `GET /v1/containers/<12167 x A>%41:<64hex>?wait=9s` — inside the byte
// bound, a valid id, indistinguishable from an agent's poll to any count —
// parked at 63 KB against 30 KB for that poll, i.e. 98% of the 64 KiB
// store.WaiterCostBytes budgets for it. Bounding the URI text (parkedURIBytes,
// COPIED, since a truncation that merely reslices pins exactly what it was
// truncating) takes the same request to 46 KB on the same listener, and the
// request's OWN retention — measured without the connection, through the real
// mux — from 37.8 KB to 1.9 KB.
//
// 46 rather than 30 because ONE copy of the line survives everything this
// function can do: `c.lastMethod = req.Method` (net/http server.go) parks a
// slice of the line on the CONNECTION, which outlives the request and is
// unexported. That residue is bounded by the admission, and it does not ADD to
// the validator below — the two share the one ~16 KB head — so the worst
// admissible parked lookup is an ordinary poll plus one wire, whichever term
// the sender spends it on: 30 KB + 16 KB = 47 KB, against the 64 KiB
// store.WaiterCostBytes budgets.
//
// ONE thing is deliberately kept in full: the If-None-Match validator, because
// the response this handler will eventually write is a conditional one
// (writeCachedBody) and dropping it would silently turn every agent's 304 into
// a full document — a bandwidth regression with no error anywhere, which is why
// TestIfNoneMatchMultipleETags (an existing conditional-GET test on this very
// route) is what fails if this line goes. It is one string, allocated at its
// own length by net/textproto and bounded by the byte bound above, and it is
// therefore the ONLY term of a parked request that the sender still sizes —
// which is exactly the property waitercost_test.go's fuzz target measures.
//
// Safety: net/http does not read req.Header after a handler starts. It populates
// response.wants10KeepAlive/wantsClose ahead of the call for exactly that reason
// (server.go, "so we're not reading from req.Header after their Handler starts",
// issue 14940), and the only later touch of either map is body.Close() merging
// REAL trailers into r.Trailer, which assigns straight through a nil map
// (transfer.go's mergeSetHeader). Header.Get on a nil map returns "" rather than
// panicking, so a handler reached through some future path that reads a header
// after parking degrades to "absent", never to a crash. The same holds for the
// line: net/http reads neither r.URL nor r.RequestURI after the handler returns
// (finishRequest and shouldReuseConnection go by the response), and the fields
// are left POPULATED — truncated, never nil — so a logging middleware still
// prints a recognisable request.
func releaseParkedHead(r *http.Request, wildcards ...string) {
	if match := r.Header.Get("If-None-Match"); match != "" {
		r.Header = http.Header{"If-None-Match": []string{match}}
	} else {
		r.Header = nil
	}
	r.Trailer = nil

	// The request line. Clone even the tiny fields: their size is not the
	// point, their BACKING ARRAY is.
	r.Method = strings.Clone(r.Method)
	r.Proto = strings.Clone(r.Proto)
	r.Host = clipParked(r.Host)
	r.RequestURI = clipParked(r.RequestURI)
	if r.URL != nil {
		// A fresh URL rather than an edit in place: r.URL may be shared with a
		// shallow copy of this request (http.Request.WithContext), and User is
		// dropped whole — nothing downstream reads it.
		r.URL = &url.URL{
			Scheme:   strings.Clone(r.URL.Scheme),
			Opaque:   clipParked(r.URL.Opaque),
			Host:     clipParked(r.URL.Host),
			Path:     clipParked(r.URL.Path),
			RawPath:  "",
			RawQuery: clipParked(r.URL.RawQuery),
			Fragment: clipParked(r.URL.Fragment),
		}
	}
	// ServeMux's matched wildcard values: an unescaped copy of the path, held in
	// an unexported field whose only door is SetPathValue. The guard keeps a
	// handler reached without a pattern (a direct call in a test) from
	// allocating r.otherValues to store the emptiness.
	for _, name := range wildcards {
		if r.PathValue(name) != "" {
			r.SetPathValue(name, "")
		}
	}
}

// parkedURIBytes bounds the URI text a parked request keeps for diagnostics.
// An agent's poll is ~90 bytes of URI ("/v1/containers/<64hex>?wait=5s"), so
// nothing this API issues is truncated at all; the bound exists for the ~16 KB
// a head is admitted at.
const parkedURIBytes = 256

// clipParked returns a COPY of s, truncated to parkedURIBytes. The copy is the
// point: s is typically a slice of the request line, and reslicing it — which
// is what any truncation that returns s[:n] does — keeps the whole line alive,
// truncating the value while retaining every byte of the cost.
func clipParked(s string) string {
	return strings.Clone(s[:min(len(s), parkedURIBytes)])
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
//   - MaxHeaderBytes bounds the wire size of a head; releaseParkedHead is what
//     makes a parked request's COST bounded rather than only its count.
func (s *Server) HTTPServer(addr string) *http.Server {
	const slack = 30 * time.Second
	return &http.Server{
		Addr:              addr,
		Handler:           s.Handler(),
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       s.maxWait + slack,
		WriteTimeout:      s.maxWait + slack,
		IdleTimeout:       120 * time.Second,
		MaxHeaderBytes:    maxHeaderBytes,
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
//
// The Service snapshot is taken once per RUN of monitors sharing a namespace
// set, not once per monitor. services.All allocates and fills a slice of every
// Service in the named namespaces, and the shape this cross product exists for
// is a fleet of cluster-wide monitors (`namespaceSelector.any: true`, what
// kube-prometheus-stack ships): 200 of them over 2,000 Services took 200 copies
// of the same 2,000-element list — measured 41.5 MB and ~89 ms per rebuild, of
// which the snapshots are the overwhelming majority, and a rebuild is triggered
// by ANY Service change while monMu is held against every concurrent poll.
//
// ONE entry rather than a map of them, deliberately: Index.All returns monitors
// in (namespace, name) order, which puts monitors sharing a namespace set in a
// run for both shapes that matter — every monitor cluster-wide (one run), and
// monitors selecting their own namespace (one run per namespace) — so a
// one-entry cache collapses the repeats without ever holding more than a single
// snapshot alive. A map keyed by the namespace set would retain one snapshot
// per distinct set for the whole build, which on a heavily overlapping set of
// matchNames is the same 41.5 MB, live at once instead of collectable.
//
// The monitor ORDER is untouched, and must be: the order endpoints land in
// out[uid] is the encounter order MergeMonitorEndpoint folds in, so grouping
// monitors by namespace set — the obvious alternative — would silently change
// which monitor names a merged target and how its relabel chains concatenate.
//
// The monitors of one run now see ONE point-in-time view of their namespaces
// instead of a fresh read apiece, which is if anything more coherent; a Service
// change arriving mid-build is handled where it always was, by monitoredServices
// reading the change tokens BEFORE the build and rebuilding on the next call.
func (s *Server) buildMonitoredServices() map[string][]monitorEndpoint {
	s.monBuilds.Add(1)
	out := map[string][]monitorEndpoint{}
	var (
		runKey   []byte
		runSvcs  []*services.Service
		runValid bool
		key      []byte
	)
	for _, m := range s.monitors.All() {
		name := m.Namespace + "/" + m.Name
		namespaces := m.ServiceNamespaces()
		key = appendNamespaceSetKey(key[:0], namespaces)
		if !runValid || string(key) != string(runKey) {
			runSvcs = s.services.All(namespaces)
			runKey = append(runKey[:0], key...)
			runValid = true
		}
		for _, svc := range runSvcs {
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

// appendNamespaceSetKey renders a monitor's resolved namespace set into dst.
//
// nil means EVERY namespace and is not the same question as any explicit list,
// so it gets its own marker byte rather than an empty join — otherwise a
// cluster-wide monitor and one naming no namespace at all would share a
// snapshot, and the cluster-wide one would be answered with nothing.
func appendNamespaceSetKey(dst []byte, namespaces []string) []byte {
	if namespaces == nil {
		return append(dst, 0x01)
	}
	dst = append(dst, 0x02)
	for _, ns := range namespaces {
		dst = append(dst, ns...)
		dst = append(dst, 0x00)
	}
	return dst
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

// allPodMonitors returns the indexed PodMonitors, rendered once per change of
// the monitor index rather than once per request (see pmMu).
//
// THE RESULT IS SHARED AND MUST BE TREATED AS READ-ONLY — the same contract
// monitoredServices and servicemonitors.PodMonitors already carry.
// podMonitorsFor only reads it, copying the refs it keeps into a caller-owned
// slice.
func (s *Server) allPodMonitors() []podMonitorRef {
	if s.monitors == nil {
		return nil
	}
	// The token BEFORE the lock, exactly as monitoredServices does: a change
	// landing during the render is then recorded as unbuilt and re-rendered on
	// the next call, rather than stamped as already-included.
	gen := s.monitors.Generation()
	s.pmMu.Lock()
	defer s.pmMu.Unlock()
	if s.pmValid && s.pmGen == gen {
		return s.pmCache
	}
	s.pmBuilds.Add(1)
	all := s.monitors.PodMonitors()
	out := make([]podMonitorRef, 0, len(all))
	for _, m := range all {
		out = append(out, podMonitorRef{monitor: m, name: m.Namespace + "/" + m.Name, namespaces: m.PodNamespaces()})
	}
	s.pmCache, s.pmGen, s.pmValid = out, gen, true
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

// enrichCache memoises ONE request's related-object resolutions.
//
// A node's pods are not a random sample of the cluster: they are a handful of
// workloads' replicas in one or two namespaces, so the same owner chain and the
// same namespace are resolved over and over. Each resolution is a lister Get
// plus kubemeta.CopyMeta — two map clones and the annotation denylist filter —
// per owner in the chain, and the chain is followed (ReplicaSet → Deployment),
// so a 110-pod node owned by 8 ReplicaSets under one Deployment performed 110
// namespace resolutions where 1 suffices and 110 chain resolutions where 8 do.
//
// PER REQUEST, never per process: the resolvers read live informer caches, and
// a memo that outlived the request would stop a label edit on a Deployment or a
// namespace from ever reaching the documents this route serves.
//
// The resolved values are SHARED by every pod that keys to them. Nothing
// mutates them — enrich is the only writer of Pod.Owners and
// Pod.NamespaceMetadata in the process, and both are read-only from there on
// (marshalled and discarded) — which is the same treat-as-immutable contract
// the store's records and the Service snapshots are served under.
type enrichCache struct {
	// owners is keyed by the resolution's full input: the namespace and every
	// owner reference. Keying on the pod's controller UID alone would be
	// smaller and wrong — Resolve takes the WHOLE ref list and reads each ref's
	// name and kind, and it cross-checks the UID against the cached object, so
	// two refs agreeing on UID but not on name are two different answers.
	owners map[string][]kubemeta.Owner
	// namespaces caches the pod's namespace metadata. A nil value is a real
	// answer (the namespace is not in the informer cache), so presence is read
	// from the lookup's second result, never from the value.
	namespaces map[string]*kubemeta.ObjectMeta
	// key is the owner key's scratch buffer, reused across pods: a map read
	// through map[string(key)] does not copy, so only a genuinely new chain
	// allocates.
	key []byte
}

// enrichCached is enrich through a request-scoped memo (see enrichCache).
func (s *Server) enrichCached(c *enrichCache, pod *kubemeta.Pod, refs []metav1.OwnerReference) {
	c.key = appendOwnerKey(c.key[:0], pod.Namespace, refs)
	if owners, ok := c.owners[string(c.key)]; ok {
		pod.Owners = owners
	} else {
		pod.Owners = s.resolver.Resolve(pod.Namespace, refs)
		if c.owners == nil {
			c.owners = make(map[string][]kubemeta.Owner, 8)
		}
		c.owners[string(c.key)] = pod.Owners
	}
	if meta, ok := c.namespaces[pod.Namespace]; ok {
		pod.NamespaceMetadata = meta
	} else {
		pod.NamespaceMetadata = s.resolver.Namespace(pod.Namespace)
		if c.namespaces == nil {
			c.namespaces = make(map[string]*kubemeta.ObjectMeta, 2)
		}
		c.namespaces[pod.Namespace] = pod.NamespaceMetadata
	}
}

// appendOwnerKey renders (namespace, owner refs) into dst.
//
// NUL-separated, because it is the one byte none of these fields can contain: a
// namespace and an owner name are DNS labels, a kind and an apiVersion are Go
// identifiers and a group/version pair, and a UID is hex with dashes. The
// controller flag rides as a byte because Resolve copies it onto the answer.
func appendOwnerKey(dst []byte, namespace string, refs []metav1.OwnerReference) []byte {
	dst = append(dst, namespace...)
	for i := range refs {
		ref := &refs[i]
		dst = append(dst, 0x00)
		dst = append(dst, ref.APIVersion...)
		dst = append(dst, 0x00)
		dst = append(dst, ref.Kind...)
		dst = append(dst, 0x00)
		dst = append(dst, ref.Name...)
		dst = append(dst, 0x00)
		dst = append(dst, ref.UID...)
		if ref.Controller != nil && *ref.Controller {
			dst = append(dst, 0x01)
		} else {
			dst = append(dst, 0x02)
		}
	}
	return dst
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
//
// It is also CAPPED the same way, and by the same counter: a park here costs
// exactly what a waiter in the store costs, on the same unauthenticated route,
// so it draws a slot from the store's blocked-lookup budget (Store.TryPark) and
// answers store.ErrTooManyWaiters — a retryable 503 + Retry-After, counted on
// kubescrape_container_lookups_shed_total — when the budget is spent. Before
// that, this spot was uncapped while the OTHER one carried the flag's promise:
// 1200 concurrent lookups all parked here at a cap of 4, 71 MB of them, with
// the blocked-lookup gauge reading 0. And this is the startup window, when a
// whole agent fleet is likeliest to be asking at once.
//
// …and it carries GetContainer's other guard too: a lookup that CANNOT block
// does not pay for the park. See the ctx check below.
func (s *Server) waitReady(ctx context.Context) error {
	select {
	case <-s.ready:
		return nil
	default:
	}
	if ctx.Err() != nil {
		// A lookup that cannot block must not pay for the parking protocol —
		// store.GetContainer's guard, on the spot that is reached FIRST. Both
		// TryPark and Unpark take the store's EXCLUSIVE lock, which during the
		// initial sync is the lock the pod informer needs once per pod to fill
		// the very caches this request is waiting for, and the park below could
		// never wait on anything for a ctx that is already done.
		//
		// The shape is not hypothetical: the agent's cadvisor batcher and two
		// ingest-enricher paths poll this route with `?wait=0`, and -cadvisor is
		// on by default. Without this, every one of those on every node spends
		// two exclusive acquisitions during the restart window, can be REFUSED
		// by a cap it declined to use — taking a slot from the blocking lookups
		// the cap exists to protect — and moves
		// kubescrape_container_lookups_shed_total, which is the abuse signal a
		// rolling update must not page like.
		//
		// errNotSynced, not a miss: this pod's caches genuinely are not filled,
		// so the honest answer is the retryable 503 the ctx.Done() arm below
		// returns. (That arm and s.draining could both be ready at once there,
		// with select picking arbitrarily; this makes the wait=0 case one
		// answer rather than a coin flip.)
		return errNotSynced
	}
	if s.store != nil {
		if !s.store.TryPark() {
			return store.ErrTooManyWaiters
		}
		defer s.store.Unpark()
	}
	s.readyParked.Add(1)
	defer s.readyParked.Add(-1)
	// Re-check: the caches may have synced while the slot was being taken, and
	// a request that can be served must not spend a wait budget on the select.
	select {
	case <-s.ready:
		return nil
	default:
	}
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

// bodyHash is the ETag digest: xxh3, 128 bits wide.
//
// Not hash/fnv, because FNV-1a is a byte-at-a-time loop (~1.2 GB/s) and this
// runs over the FULL body of every cached response, including every 304
// revalidation — most visibly on /v1/nodes/{node}/targets, which re-serializes
// every pod document on the node each scrape cycle.
//
// 128 bits rather than 64 because of what a collision COSTS here, not because
// one is likely: two bodies sharing a digest make writeCached answer a
// revalidation with 304, and the agent then keeps serving the PREVIOUS node's
// target list — a wrong answer that persists until the body changes again and
// looks exactly like a correctly-cached response while it lasts. A digest is
// also the one place where the input is attacker-influenced in principle (a pod
// annotation lands in the body), and 64-bit xxhash is not collision-resistant
// against a chosen input. The width is close to free: xxh3 computes the 128-bit
// result in the same pass as the 64-bit one.
//
// ETags stay opaque to clients (etagMatches only string-compares, W/ prefix
// stripped), so widening the digest costs one full 200 per cached client at the
// upgrade boundary, exactly as any other ETag change would.
func bodyHash(b []byte) xxh3.Uint128 { return xxh3.Sum128(b) }

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}
