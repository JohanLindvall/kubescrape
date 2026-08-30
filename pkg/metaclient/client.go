// Package metaclient is the HTTP client for the kubescrape metadata service:
// it resolves a container ID, pod UID or pod IP to the pod/container metadata
// the service derives from its informer caches.
//
// Container lookups may block: a container ID can reach a node's agent up to a
// second before the kubelet has posted it to the API server, so the service
// holds the request until the ID appears or the wait elapses (see Container).
//
// Responses carrying Cache-Control/ETag are cached, so repeat lookups are
// served locally or revalidated with a conditional GET. The client has no
// metrics dependency; set Config.Observe to feed lookup outcomes into whatever
// metrics library the caller uses.
package metaclient

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/JohanLindvall/kubescrape/pkg/kubemeta"
)

// Request outcomes reported to Client.Observe.
const (
	// OutcomeOK is a fetch that hit the service and returned metadata.
	OutcomeOK = "ok"
	// OutcomeCached is a fetch served from the local cache without a request.
	OutcomeCached = "cached"
	// OutcomeNotModified is a conditional GET the service answered with 304.
	OutcomeNotModified = "not_modified"
	// OutcomeNotFound is a 404 (the object is unknown to the service).
	OutcomeNotFound = "not_found"
	// OutcomeError is a transport failure or an unexpected status.
	OutcomeError = "error"
)

// Client talks to a kubescrape metadata service. Responses carrying
// Cache-Control/ETag are cached so repeat lookups (common on the concurrent
// ingest and cadvisor paths) are served locally or revalidated cheaply with a
// conditional GET.
type Client struct {
	base string
	http *http.Client
	now  func() time.Time

	// observeFn is Config.Observe. Set once at construction and never after,
	// so it needs no synchronization and no "set it before sharing" contract
	// for a caller to get wrong.
	observeFn func(outcome string)

	// mu guards cache and lastSweep. It is an RWMutex because the HIT path —
	// the inner loop of the concurrent ingest enrichment, up to
	// maxLookupsPerRequest lookups times -ingest-max-in-flight handlers per push
	// wave — only READS, and every one of those hits used to serialise on an
	// exclusive acquisition to refresh an idle stamp (see usedStampGranularity).
	mu    sync.RWMutex
	cache map[string]cacheEntry
	// lastSweep is when expired entries were last swept (see evictLocked).
	lastSweep time.Time

	// log reports the responses this client did not expect. It is never a line
	// per request: metaclient is called from the concurrent ingest enrichment
	// and the cadvisor batchers, so anything per-lookup is a flood
	// proportional to the node's pod count. Everything below is either
	// throttled (a condition that persists) or a periodic aggregate.
	log *slog.Logger
	// statusWarn/decodeWarn/evictWarn throttle the three repeatable
	// complaints; cacheReport paces the Debug summary.
	//
	// A LOCAL throttle rather than internal/logdedupe, which owns this rule for
	// the rest of the repo: pkg/ must never import internal/ (the compiler
	// allows it inside this module and it breaks every external consumer), so
	// the type is re-stated here — deliberately with the SAME loser rule, which
	// is the half of it that is easy to get wrong.
	statusWarn  throttle
	decodeWarn  throttle
	evictWarn   throttle
	cacheReport throttle

	// Cumulative lookup outcomes, for the Debug cache summary. Atomics rather
	// than fields under c.mu: the hit path holds only the RLock, and turning it
	// exclusive for a diagnostic counter is exactly the serialisation the
	// RWMutex above exists to avoid.
	hits, revalidated, fetched, missing, failed atomic.Uint64

	// token, when set, authenticates requests to /v1/scrape-auth (the only
	// authenticated endpoint: it returns Secret VALUES, while the metadata
	// endpoints return object metadata the node's kubelet already has). It is
	// re-read per use so a rotated Secret is picked up without a restart.
	token func() string
}

// observe reports an outcome when a hook is installed, and tallies it for the
// Debug cache summary.
func (c *Client) observe(outcome string) {
	switch outcome {
	case OutcomeCached:
		c.hits.Add(1)
	case OutcomeNotModified:
		c.revalidated.Add(1)
	case OutcomeOK:
		c.fetched.Add(1)
	case OutcomeNotFound:
		c.missing.Add(1)
	default:
		c.failed.Add(1)
	}
	c.reportCache()
	if c.observeFn != nil {
		c.observeFn(outcome)
	}
}

// cacheReportInterval paces the Debug cache summary. The question it answers —
// "is the ETag path working, or is every lookup a full body?" — is a RATIO, so
// one line a minute carrying the cumulative tallies is strictly more useful
// than a line per request.
const cacheReportInterval = time.Minute

// warnInterval throttles the three complaints that can persist: an unexpected
// status, an undecodable body, and the cache running at its hard cap. Each is
// a property of the deployment (a misdirected endpoint, a wrong service, a
// node whose live URL count exceeds the cap), so it repeats on every lookup
// until someone changes something.
const warnInterval = 5 * time.Minute

// reportCache writes the periodic Debug summary of the response cache.
//
// Logger.Enabled is checked BEFORE the throttle, and the order is load-bearing
// because this runs once per lookup on the concurrent ingest and cadvisor
// paths. It used to be the other way round, on the stated grounds that "Allow
// is a single atomic load while Enabled is an interface call" — which is not
// what allow does: its FIRST statement is time.Now().UnixNano(), and a clock
// read is the expensive half. Measured on this repo's reference machine,
// serially: time.Now().UnixNano() 57.9 ns, the whole throttle 63.3 ns,
// Logger.Enabled 8.3 ns. Under -race-free parallel load the gap widens rather
// than closing (time.Now 27.5 ns/op at 4 procs, Enabled 10.6 ns/op), so the
// swap is worth strictly more on the path that motivated the ordering.
//
// It is also better on the behaviour the old order apologised for: no throttle
// slot is spent while Debug is off, so raising the level mid-incident produces
// a summary on the next lookup instead of up to a minute later.
func (c *Client) reportCache() {
	log := c.logger()
	if !log.Enabled(context.Background(), slog.LevelDebug) {
		return
	}
	if !c.cacheReport.allow(cacheReportInterval) {
		return
	}
	c.mu.RLock()
	entries := len(c.cache)
	c.mu.RUnlock()
	// Cumulative, not per-window: two consecutive lines differ by the window,
	// and a cumulative number cannot be misread as a rate.
	log.Debug("metadata cache", "entries", entries, "maxEntries", maxCacheEntries,
		"hits", c.hits.Load(), "notModified", c.revalidated.Load(), "fetched", c.fetched.Load(),
		"notFound", c.missing.Load(), "errors", c.failed.Load())
}

// logger is the client's logger, defaulting to the process default.
func (c *Client) logger() *slog.Logger {
	if c.log == nil {
		return slog.Default()
	}
	return c.log
}

// throttle gates a repeating complaint to at most once per interval across
// concurrent callers: a clock read, one atomic load and a CompareAndSwap, no
// mutex. The clock read is unconditional and is the dear part (57.9 ns against
// the atomic's ~1 ns on this repo's reference machine), so a caller on a hot
// path must decide whether it would log AT ALL before consulting this — see
// reportCache, which had that backwards.
//
// It is internal/logdedupe.Throttle re-stated, INCLUDING the rule that makes it
// correct — a caller whose CompareAndSwap loses raced a winner that is about to
// log the same condition and must stay silent, because logging on a lost race
// is the duplicate the throttle exists to prevent. It is copied rather than
// imported because this package is public and pkg/ must never import internal/.
type throttle struct{ last atomic.Int64 }

func (t *throttle) allow(interval time.Duration) bool {
	now := time.Now().UnixNano()
	last := t.last.Load()
	return now-last >= int64(interval) && t.last.CompareAndSwap(last, now)
}

type cacheEntry struct {
	// decoded is the response body unmarshaled once into the caller's result
	// type (a pointer), stored so cache hits and 304s skip the JSON decode. It
	// is never mutated after storing; hits receive a SHALLOW copy — maps/slices
	// are shared under the same treat-as-immutable contract the store uses.
	// The raw body is NOT retained: every caller of a given URL asks for the
	// same result type, so the decoded value serves every hit and a body kept
	// beside it was ~18% of the cache's footprint for nothing (an entry that
	// cannot be copied out is dropped and re-fetched — see lookupEntry).
	decoded any
	etag    string
	expires time.Time
	// used is when this entry was last READ or written. Eviction sweeps on
	// it, not on expires: an EXPIRED entry is exactly the state If-None-Match
	// is built on — it still carries the ETag that turns the next read into a
	// 304 — so sweeping by expiry threw away the revalidation state for every
	// URL polled slower than the sweep (the 1m self and node-metadata reads
	// against a 10s TTL), and each of those re-fetched a full body forever.
	// What the sweep is actually for is the dead container nobody asks about
	// again, and that is idleness.
	used time.Time
}

// Config configures a Client. Everything a caller can influence lives here, so
// nothing has to be assigned to a Client after construction — the hooks below
// used to be a public field and a setter, both carrying an unwritten
// "install it before you share the client" contract that no compiler enforces.
type Config struct {
	// Base is the metadata service's URL, e.g. "http://kubescrape.monitoring".
	// A trailing slash is trimmed.
	Base string

	// Timeout is the overall per-request timeout. It MUST exceed the wait
	// passed to Container, or a blocking container lookup can never succeed —
	// Container refuses a wait at or past it rather than letting the client
	// deadline mask the server's wait as a transport error.
	Timeout time.Duration

	// Observe, if set, is called once per lookup with the outcome (one of the
	// Outcome* constants). It is the hook callers use to feed their own metrics
	// without this package depending on a metrics library. Keep it cheap and
	// non-blocking: it runs on the caller's goroutine, inside the lookup.
	Observe func(outcome string)

	// ScrapeAuthToken supplies the bearer token sent with /v1/scrape-auth
	// requests (the only authenticated endpoint: it returns Secret VALUES,
	// while the metadata endpoints return object metadata the node's kubelet
	// already has). It is called PER REQUEST, so a rotated token file takes
	// effect without a restart; nil — or a func returning "" — sends no
	// Authorization header, which the service rejects when the endpoint is on.
	ScrapeAuthToken func() string

	// Transport overrides the HTTP transport. nil takes NewTransport()'s, which
	// is tuned for this client's load and, deliberately, sets no proxy — see
	// there before supplying your own.
	Transport http.RoundTripper

	// Log receives this client's complaints about responses it did not expect:
	// a status that is neither a 200, a 304 nor a 404, a body it could not
	// decode, and the response cache running at its hard cap. nil takes
	// slog.Default().
	//
	// Every one of those is THROTTLED (a misdirected endpoint produces one per
	// lookup, on every node), and there is deliberately no per-request line at
	// any level: the cache's behaviour is reported as a periodic Debug
	// aggregate instead. A 404 is not logged at all — it is the expected answer
	// for a container the kubelet has not posted yet, a pod IP that is not a
	// pod's, and a hostNetwork caller asking /v1/self — and Observe's
	// OutcomeNotFound is where its rate lives.
	Log *slog.Logger
}

// NewTransport returns the transport New uses when Config.Transport is nil. It
// is exported so a caller that needs to change one field (a TLS config, a
// dialer) can start from these settings rather than from
// http.DefaultTransport's, which are wrong for this client in two ways the
// comments below explain.
func NewTransport() *http.Transport {
	// A dedicated transport: DefaultTransport's MaxIdleConnsPerHost of 2
	// forces most connections to close under the highly concurrent ingest
	// enrichment load (everything goes to the one metadata-service host).
	return &http.Transport{
		// No proxy, deliberately. The metadata service is always cluster-local,
		// and a proxy hop REPLACES the source address /v1/self attributes the
		// caller by — a cluster-wide HTTP_PROXY (which the conventional
		// NO_PROXY=.svc,.cluster.local does not exclude for a bare
		// "kubescrape.monitoring") would silently stamp the PROXY's pod onto
		// every agent's own metrics, with the resolved gauge reading 1.
		Proxy: nil,
		DialContext: (&net.Dialer{
			Timeout:   10 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          128,
		MaxIdleConnsPerHost:   64,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}
}

// New creates a client for the service at cfg.Base.
func New(cfg Config) *Client {
	rt := cfg.Transport
	if rt == nil {
		rt = NewTransport()
	}
	return &Client{
		log:       cfg.Log,
		base:      strings.TrimRight(cfg.Base, "/"),
		http:      &http.Client{Timeout: cfg.Timeout, Transport: rt},
		now:       time.Now,
		cache:     make(map[string]cacheEntry),
		observeFn: cfg.Observe,
		token:     cfg.ScrapeAuthToken,
	}
}

// ScrapeAuth fetches a monitor endpoint's bearer token by its
// "namespace/name/key" Secret reference (served only when the metadata
// service runs with -scrape-auth-secrets). Responses are no-store; callers
// cache briefly themselves.
func (c *Client) ScrapeAuth(ctx context.Context, ref string) (string, error) {
	// Escape each segment of the ref, keeping the "/" separators as route
	// structure: a well-formed ns/name/key passes through byte-for-byte (DNS
	// names and Secret keys need no escaping), while a hostile segment cannot
	// splice extra path segments, a query or a fragment into the request. This
	// is the one endpoint method whose argument carries slashes of its own;
	// the others PathEscape their argument whole.
	segs := strings.Split(ref, "/")
	for i, seg := range segs {
		segs[i] = url.PathEscape(seg)
	}
	var out struct {
		Value string `json:"value"`
	}
	// authed: this is the ONE authenticated route; the bearer decision is made
	// here, where the URL is built, not by re-matching the path later.
	if err := c.get(ctx, c.base+"/v1/scrape-auth/"+strings.Join(segs, "/"), true, &out); err != nil {
		return "", err
	}
	return out.Value, nil
}

// Container fetches metadata for a container ID, letting the service wait up
// to wait for the metadata to appear. wait must be shorter than the
// configured Config.Timeout, which covers the whole request — server-side
// wait included: a wait at or past it can never be honored (the client
// deadline fires while the server is still legitimately holding the request),
// so the combination is refused by name instead of surfacing as a timeout.
func (c *Client) Container(ctx context.Context, id string, wait time.Duration) (*kubemeta.ContainerMetadata, error) {
	if wait > 0 && c.http.Timeout > 0 && wait >= c.http.Timeout {
		c.observe(OutcomeError)
		return nil, fmt.Errorf("metaclient: Container wait %v must be shorter than Config.Timeout %v (the timeout covers the whole request, so the server-side wait could never elapse)", wait, c.http.Timeout)
	}
	u := fmt.Sprintf("%s/v1/containers/%s?wait=%s", c.base, url.PathEscape(kubemeta.NormalizeContainerID(id)), wait)
	var md kubemeta.ContainerMetadata
	if err := c.getJSON(ctx, u, &md); err != nil {
		return nil, err
	}
	return &md, nil
}

// PodByName fetches metadata for one pod by namespace and name.
func (c *Client) PodByName(ctx context.Context, namespace, name string) (*kubemeta.Pod, error) {
	u := fmt.Sprintf("%s/v1/pods/%s/%s", c.base, url.PathEscape(namespace), url.PathEscape(name))
	var pod kubemeta.Pod
	if err := c.getJSON(ctx, u, &pod); err != nil {
		return nil, err
	}
	return &pod, nil
}

// PodByUID fetches metadata for one pod by UID.
func (c *Client) PodByUID(ctx context.Context, uid string) (*kubemeta.Pod, error) {
	u := fmt.Sprintf("%s/v1/pod-uids/%s", c.base, url.PathEscape(uid))
	var pod kubemeta.Pod
	if err := c.getJSON(ctx, u, &pod); err != nil {
		return nil, err
	}
	return &pod, nil
}

// PodByIP fetches metadata for the live pod owning a pod IP (404 for
// unknown, deleted, or hostNetwork pods).
func (c *Client) PodByIP(ctx context.Context, ip string) (*kubemeta.Pod, error) {
	u := fmt.Sprintf("%s/v1/pod-ips/%s", c.base, url.PathEscape(ip))
	var pod kubemeta.Pod
	if err := c.getJSON(ctx, u, &pod); err != nil {
		return nil, err
	}
	return &pod, nil
}

// Self fetches metadata for the pod the CALLER runs in: the service attributes
// the request by its connection's source address (GET /v1/self), so no
// downward-API identity has to be wired into the caller's deployment. It fails
// with a 404 error when the caller is not a live, non-hostNetwork pod — the
// caller runs outside Kubernetes, on hostNetwork, or behind a hop that
// rewrites the source address.
//
// The response is cached like the other metadata lookups, and may be: it is
// marked `private` — one Client belongs to one process, which is the one pod
// the answer describes — so re-reading it to pick up a relabelled pod or
// namespace costs a conditional GET, usually answered 304.
func (c *Client) Self(ctx context.Context) (*kubemeta.Pod, error) {
	var pod kubemeta.Pod
	if err := c.getJSON(ctx, c.base+"/v1/self", &pod); err != nil {
		return nil, err
	}
	return &pod, nil
}

// Node fetches the labels and annotations of a node.
func (c *Client) Node(ctx context.Context, name string) (*kubemeta.NodeMetadata, error) {
	u := fmt.Sprintf("%s/v1/nodes/%s/metadata", c.base, url.PathEscape(name))
	var meta kubemeta.NodeMetadata
	if err := c.getJSON(ctx, u, &meta); err != nil {
		return nil, err
	}
	return &meta, nil
}

// NodeTargets fetches the Prometheus scrape targets (with embedded pod
// metadata) for a node.
func (c *Client) NodeTargets(ctx context.Context, node string) ([]kubemeta.ScrapeTarget, error) {
	var resp struct {
		Targets []kubemeta.ScrapeTarget `json:"targets"`
	}
	u := fmt.Sprintf("%s/v1/nodes/%s/targets", c.base, url.PathEscape(node))
	if err := c.getJSON(ctx, u, &resp); err != nil {
		return nil, err
	}
	return resp.Targets, nil
}

// getJSON is the request path of the unauthenticated metadata endpoints.
func (c *Client) getJSON(ctx context.Context, u string, v any) error {
	return c.get(ctx, u, false, v)
}

// get fetches u into v. authed marks u as the one authenticated route
// (/v1/scrape-auth): the decision is made where the URL is BUILT, so the
// route is spelled in exactly one place rather than re-derived by matching
// the URL here.
func (c *Client) get(ctx context.Context, u string, authed bool, v any) error {
	key := cacheKey(u)

	// Fresh cache entry: serve without a request (and without re-decoding —
	// the decoded value is stored once and shallow-copied out).
	entry, cached, fresh := c.lookupEntry(key, v)
	if fresh {
		c.observe(OutcomeCached)
		copyDecoded(v, entry.decoded)
		return nil
	}
	return c.fetch(ctx, u, key, entry, cached, authed, v)
}

// lookupEntry reads the cache under the lock, classifying the entry as fresh
// (serve locally), stale-but-present (revalidate with If-None-Match), or
// absent. An entry whose decoded value is not v's type is dropped and reported
// absent, so this call re-fetches it from scratch: the cache holds one decoded
// value per URL and every caller of an endpoint asks for the same type, making
// that unreachable in practice — but a value that cannot be copied out must
// never be served, and dropping it also re-populates the entry usefully.
func (c *Client) lookupEntry(key string, v any) (entry cacheEntry, cached, fresh bool) {
	now := c.now()
	c.mu.RLock()
	entry, cached = c.cache[key]
	c.mu.RUnlock()
	if !cached {
		return cacheEntry{}, false, false
	}
	if !sameType(v, entry.decoded) {
		c.mu.Lock()
		// Re-check under the write lock: another goroutine may have replaced the
		// entry with one this caller CAN use while the lock was released, and
		// dropping that would throw away a good 200.
		if cur, ok := c.cache[key]; ok && !sameType(v, cur.decoded) {
			delete(c.cache, key)
		}
		c.mu.Unlock()
		return cacheEntry{}, false, false
	}
	// The stamp feeds eviction and nothing else, against a 5-minute idle window,
	// so it is refreshed COARSELY: writing it on every hit is what forced the
	// exclusive lock onto the read path, and an entry read at all is re-stamped
	// within a fraction of the window either way. (It also keeps a
	// revalidated-but-stale entry alive, which is why it is not gated on
	// freshness.)
	if now.Sub(entry.used) >= usedStampGranularity {
		c.mu.Lock()
		if cur, ok := c.cache[key]; ok && cur.used.Equal(entry.used) {
			cur.used = now
			c.cache[key] = cur
		}
		c.mu.Unlock()
		entry.used = now
	}
	return entry, true, now.Before(entry.expires)
}

// usedStampGranularity is how stale an entry's idle stamp may get before a hit
// refreshes it. Small against cacheMaxIdle (so a live entry is never swept) and
// large against a lookup (so the refresh is rare enough that the hit path is a
// read-lock acquisition and nothing more).
const usedStampGranularity = cacheMaxIdle / 10

// fetch performs the HTTP request (revalidating with the entry's ETag when
// cached), stores a cacheable 200, and decodes into v.
func (c *Client) fetch(ctx context.Context, u, key string, entry cacheEntry, cached, authed bool, v any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		// Config.Observe is documented as called once per lookup, and every
		// other exit from this function honours that. This one did not, so a
		// lookup that failed before the request was even built — a malformed
		// base URL, most likely, which fails EVERY lookup — bumped nothing:
		// kubescrape_metadata_requests_total stayed flat while no metadata
		// resolved at all, which is the worst possible time for the counter
		// that exists to show exactly that.
		c.observe(OutcomeError)
		return err
	}
	// Revalidate a stale-but-present entry cheaply.
	if cached && entry.etag != "" {
		req.Header.Set("If-None-Match", entry.etag)
	}
	// Only the secret-bearing endpoint is authenticated (authed, decided where
	// the URL was built); sending the token to every metadata URL would spread
	// it further than it needs to go.
	if authed && c.token != nil {
		if tok := c.token(); tok != "" {
			req.Header.Set("Authorization", "Bearer "+tok)
		}
	}
	resp, err := c.http.Do(req)
	if err != nil {
		c.observe(OutcomeError)
		return err
	}
	defer func() {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
		_ = resp.Body.Close()
	}()

	switch {
	case resp.StatusCode == http.StatusNotModified && cached:
		// Unchanged: extend the cached entry's freshness and serve it. Only
		// refresh the entry that this request actually validated — a
		// concurrent goroutine may have stored a newer 200 body under the
		// same key while the lock was released, which must not be clobbered
		// with the pre-request entry.
		expires := c.now().Add(maxAge(resp))
		c.mu.Lock()
		if cur, ok := c.cache[key]; ok && cur.etag == entry.etag {
			cur.expires = expires
			cur.used = c.now()
			c.cache[key] = cur
		}
		c.mu.Unlock()
		c.observe(OutcomeNotModified)
		// lookupEntry only reports cached for an entry of v's type.
		copyDecoded(v, entry.decoded)
		return nil
	case resp.StatusCode == http.StatusOK:
		// BOUNDED. The error path already caps its read; the success path did
		// not, so an endpoint pointed at something that streams (a log tail, a
		// proxy error page, another service entirely) could grow the agent's
		// heap without limit — on the concurrent ingest path, on every node.
		// A pod document is kilobytes; the whole node-targets response is the
		// largest legitimate body and stays far under this.
		body, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes))
		if err != nil {
			c.observe(OutcomeError)
			return fmt.Errorf("metaclient: reading %s: %w", u, err)
		}
		if ttl := maxAge(resp); ttl > 0 {
			// Decode into a value the CACHE owns, not into v: the caller may
			// overwrite its struct after the call, which must not reach the
			// cached copy. v gets a shallow copy of the owned value.
			dec := reflect.New(reflect.TypeOf(v).Elem())
			if err := json.Unmarshal(body, dec.Interface()); err != nil {
				c.observe(OutcomeError)
				c.warnUndecodable(u, body, err)
				return decodeError(u, err)
			}
			c.mu.Lock()
			c.cache[key] = cacheEntry{decoded: dec.Interface(), etag: resp.Header.Get("ETag"), expires: c.now().Add(ttl), used: c.now()}
			dropped, held := c.evictLocked()
			c.mu.Unlock()
			// Emitted with the lock RELEASED. c.mu is the EXCLUSIVE lock every
			// concurrent metadata lookup on this node serialises through (the
			// ingest handlers, the cadvisor batchers), and an slog record is a
			// handler call plus a write to stderr — a write that BLOCKS when
			// nothing drains the other end (a stalled log collector, a full log
			// disk, a pipe with no reader). Emitting it inside the critical
			// section would park every lookup behind a log line, and it fires
			// precisely when the cache is thrashing, i.e. when lookups are
			// already at their most expensive. Decide under the lock, report
			// after: the shape bearer.Rotating.Tokens and servicegraph's store
			// already use.
			c.reportEviction(dropped, held)
			c.observe(OutcomeOK)
			reflect.ValueOf(v).Elem().Set(dec.Elem())
			return nil
		}
		if err := json.Unmarshal(body, v); err != nil {
			// Match the TTL path: an undecodable 200 is an error, not "ok".
			c.observe(OutcomeError)
			c.warnUndecodable(u, body, err)
			return decodeError(u, err)
		}
		c.observe(OutcomeOK)
		return nil
	default:
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		if resp.StatusCode == http.StatusNotFound {
			// NOT logged: a 404 is the metadata service's normal answer for a
			// container the kubelet has not posted yet, an IP that is not a
			// live pod's, and a hostNetwork caller asking /v1/self. Its rate is
			// Observe's OutcomeNotFound and the caller decides what it means.
			c.observe(OutcomeNotFound)
		} else {
			c.observe(OutcomeError)
			// Anything else is a deployment fault the caller cannot diagnose
			// from its own error handling: a 401/403 (the scrape-auth token),
			// a 503 (an unready replica still in the Service's endpoints), a
			// 5xx, or an HTML error page from something that is not kubescrape
			// at all. The BODY is deliberately not on the line — it is
			// whatever answered, and this client is pointed at a URL an
			// operator supplied.
			if c.statusWarn.allow(warnInterval) {
				c.logger().Warn("the metadata service returned an unexpected status; metadata for this object cannot be resolved",
					"url", u, "status", resp.StatusCode)
			}
		}
		return &StatusError{Code: resp.StatusCode, URL: u, Body: strings.TrimSpace(string(body))}
	}
}

// warnUndecodable reports a 200 whose body this client could not decode.
//
// The typed *DecodeError already names the package and the URL, but only to
// whoever prints the error — and the two agent paths that call this most
// (ingest enrichment, the cadvisor batchers) classify the failure into a
// counter and move on, so a -metadata-endpoint pointed at the wrong Service
// produced a rising kubescrape_metadata_requests_total{outcome="error"} and
// not one line saying what came back.
//
// truncated is the tell for the other shape: a body at exactly the read cap
// was CUT, so the JSON error is a consequence of the cap rather than of the
// payload, and "unexpected end of JSON input" would otherwise send an operator
// looking for a malformed document.
func (c *Client) warnUndecodable(u string, body []byte, err error) {
	if !c.decodeWarn.allow(warnInterval) {
		return
	}
	c.logger().Warn("the metadata service's response could not be decoded; this endpoint is probably not a kubescrape metadata service",
		"url", u, "error", err, "bytes", len(body), "truncated", len(body) >= maxResponseBytes)
}

// maxResponseBytes caps a metadata response body. The biggest legitimate one
// is a node's full target list; anything approaching this is a misdirected
// endpoint, and an unbounded read of it is an OOM on every node at once.
const maxResponseBytes = 64 << 20

// cacheSweepEvery is how often IDLE entries are swept below the cap.
const cacheSweepEvery = time.Minute

// cacheMaxIdle is how long an unread entry is kept. Comfortably above the
// slowest poll any caller makes (the 1m self-attributes and node-metadata
// refreshes), so a periodic reader always still finds its ETag and gets a
// 304 instead of a full body; a container nobody looks up again is gone
// within two sweeps of it.
const cacheMaxIdle = 5 * time.Minute

// maxCacheEntries bounds the response cache. Without a cap the map grows by
// one entry per distinct container/pod URL ever fetched — a steady leak on
// nodes with pod churn (dead containers are never requested again).
const maxCacheEntries = 4096

// evictLowWater is the size eviction trims down to once the cap is exceeded.
// Trimming below the cap (rather than to it) amortizes the two O(n) map sweeps
// over ~1000 inserts instead of running them on every insert while full — this
// matters because the sweeps hold the mutex shared across concurrent ingest and
// cadvisor lookups.
const evictLowWater = maxCacheEntries * 3 / 4

// evictLocked trims the cache when it exceeds the cap: IDLE entries first,
// then arbitrary ones. An arbitrary eviction discards the ETag with the
// entry, so that URL's next lookup is a full 200 re-fetch, not a cheap
// revalidation — the price of the hard bound, paid only when 4096+ distinct
// URLs are genuinely live. Caller holds the mutex.
//
// It DECIDES and returns; it does not log. Everything here runs under the
// exclusive lock shared by every concurrent lookup, so the report is the
// caller's to emit once the lock is gone (see reportEviction). dropped is how
// many entries the ARBITRARY trim discarded, held how many remain.
func (c *Client) evictLocked() (dropped, held int) {
	now := c.now()
	// Sweep idle entries periodically even below the cap. The cap alone
	// only ran at 4096 entries, so on a node with pod churn the cache held
	// thousands of dead containers' FULL pod documents — tens of MB of heap
	// that nothing would ever ask for again — until the next insert crossed the
	// threshold.
	if len(c.cache) <= maxCacheEntries {
		if now.Sub(c.lastSweep) < cacheSweepEvery {
			return 0, len(c.cache)
		}
		c.lastSweep = now
		for k, e := range c.cache {
			if now.Sub(e.used) > cacheMaxIdle {
				delete(c.cache, k)
			}
		}
		return 0, len(c.cache)
	}
	// The hard-trim path performs the same idle sweep on its way to the cap,
	// so the periodic cadence counts from it too: leaving lastSweep behind made
	// the next below-cap insert re-run a full idle sweep immediately, however
	// recently this one finished.
	c.lastSweep = now
	for k, e := range c.cache {
		if now.Sub(e.used) > cacheMaxIdle {
			delete(c.cache, k)
		}
	}
	before := len(c.cache)
	for k := range c.cache {
		if len(c.cache) <= evictLowWater {
			break
		}
		delete(c.cache, k)
	}
	return before - len(c.cache), len(c.cache)
}

// reportEviction warns that the ARBITRARY trim ran, which has a cost nothing
// else reports: an entry evicted that way takes its ETag with it, so that
// URL's next lookup is a full 200 instead of a 304. On a node whose live URL
// count sits above the cap that is a permanent state — every lookup re-fetches
// a full body from the singleton — and the only symptom is load on the
// metadata service. Throttled, because it then happens on roughly every
// insert.
//
// Called with c.mu NOT held; see the call site.
func (c *Client) reportEviction(dropped, held int) {
	if dropped <= 0 || !c.evictWarn.allow(warnInterval) {
		return
	}
	c.logger().Warn("the metadata response cache is at its hard cap; evicted entries lose their ETag, so their next lookup re-fetches a full body",
		"dropped", dropped, "entries", held, "maxEntries", maxCacheEntries)
}

// sameType reports whether a cached decoded value can be copied into dst: both
// must be pointers to the same type.
func sameType(dst, src any) bool {
	if src == nil {
		return false
	}
	dv, sv := reflect.ValueOf(dst), reflect.ValueOf(src)
	return dv.Kind() == reflect.Pointer && dv.Type() == sv.Type()
}

// copyDecoded sets *dst = *src. The copy is shallow: maps and slices stay
// shared with the cached value, which is never mutated (the store's
// shallow-copy contract). Types are checked by lookupEntry.
func copyDecoded(dst, src any) {
	reflect.ValueOf(dst).Elem().Set(reflect.ValueOf(src).Elem())
}

// cacheKey identifies the resource independent of transient request params
// (the container endpoint's ?wait= must not fragment the cache).
func cacheKey(u string) string {
	// Only the container endpoint ever carries a query (?wait=); everything
	// else skips the parse/re-encode round trip.
	i := strings.IndexByte(u, '?')
	if i < 0 {
		return u
	}
	// The common real query is exactly "wait=..." — cut it without the
	// url.Parse/Encode round trip (~500ns and 5 allocs per Container lookup
	// on the concurrent ingest path). Anything else (hypothetical future
	// params) takes the exact strip-and-normalize below.
	if q := u[i+1:]; strings.HasPrefix(q, "wait=") && !strings.ContainsAny(q, "&;") {
		return u[:i]
	}
	parsed, err := url.Parse(u)
	if err != nil {
		return u
	}
	q := parsed.Query()
	q.Del("wait")
	parsed.RawQuery = q.Encode()
	return parsed.String()
}

// maxCacheTTL caps the freshness lifetime this client will honour. The header
// is the SERVER's, and time.Duration is int64 NANOseconds: 1.85e10 seconds
// overflows it, so an unclamped `secs * time.Second` turns a large max-age into
// either a ~49-year TTL (the entry is served forever, never revalidated, and
// the idle sweep cannot reclaim it while something keeps reading it) or a
// NEGATIVE one (caching silently off — the opposite of what the header asked
// for). Clamping is not only overflow insurance: no metadata document is worth
// holding for a day, and a header past this is a misdirected or hostile
// endpoint, which the response-size and ETag paths already guard against.
const maxCacheTTL = 24 * time.Hour

// maxAge extracts the Cache-Control max-age; zero when absent or unparseable,
// and zero whenever no-store or no-cache is also present — either directive
// forbids serving a stored response without asking again, and this client's
// only storage is its cache, so both mean "do not cache". kubescrape's own
// server never combines them with a max-age, but this package is public.
//
// The value is clamped to maxCacheTTL BEFORE the multiply (see the constant).
func maxAge(resp *http.Response) time.Duration {
	var ttl time.Duration
	for _, part := range strings.Split(resp.Header.Get("Cache-Control"), ",") {
		part = strings.TrimSpace(part)
		if part == "no-store" || part == "no-cache" {
			return 0
		}
		if v, ok := strings.CutPrefix(part, "max-age="); ok {
			if secs, err := strconv.Atoi(v); err == nil && secs > 0 {
				if secs > int(maxCacheTTL/time.Second) {
					ttl = maxCacheTTL
					continue
				}
				ttl = time.Duration(secs) * time.Second
			}
		}
	}
	return ttl
}

// StatusError is a non-200 response from the metadata service.
type StatusError struct {
	Code int
	// URL is the request that produced the status. Its sibling *DecodeError
	// carried one and this did not, so the commonest failure an operator meets
	// — a 401 on /v1/scrape-auth, a 503 from an unready replica — rendered as
	// "metadata service returned 503:" with nothing naming the endpoint or the
	// object, wherever a caller logged it. Empty for a StatusError a caller
	// constructed itself (tests do, to drive the IsNotFound path).
	URL  string
	Body string
}

func (e *StatusError) Error() string {
	if e.URL == "" {
		return fmt.Sprintf("metadata service returned %d: %s", e.Code, e.Body)
	}
	return fmt.Sprintf("metadata service at %s returned %d: %s", e.URL, e.Code, e.Body)
}

// DecodeError reports a metadata-service response this client could not decode.
// The wrapped error is encoding/json's, which on its own says only `invalid
// character 'x' ...` — nothing about this package, the endpoint or the URL, so a
// consumer had no way to tell a kubescrape response apart from any other JSON
// its process decodes. The sibling failure paths return typed errors; this one
// used to leave the package bare.
type DecodeError struct {
	// URL is the request whose body could not be decoded.
	URL string
	// Err is encoding/json's error.
	Err error
}

func (e *DecodeError) Error() string {
	return fmt.Sprintf("metaclient: decoding the metadata service's response from %s: %v", e.URL, e.Err)
}

func (e *DecodeError) Unwrap() error { return e.Err }

// decodeError wraps a JSON decode failure with the URL that produced it.
func decodeError(u string, err error) error { return &DecodeError{URL: u, Err: err} }

// IsNotFound reports whether err is (or wraps) a 404 from the metadata
// service.
func IsNotFound(err error) bool {
	var se *StatusError
	return errors.As(err, &se) && se.Code == http.StatusNotFound
}
