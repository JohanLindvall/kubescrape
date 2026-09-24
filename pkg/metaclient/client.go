// Package metaclient is the HTTP client for the kubescrape metadata service:
// it resolves a container ID, pod UID or pod IP to the pod/container metadata
// the service derives from its informer caches.
//
// Container lookups may block: a container ID can reach a node's agent up to a
// second before the kubelet has posted it to the API server, so the service
// holds the request until the ID appears or the wait elapses (see Container).
//
// A 200 carrying a positive Cache-Control max-age (and neither no-store nor
// no-cache) is cached, so repeat lookups are served locally until it expires
// and then revalidated — with a conditional GET when it carried an ETag; a
// response without such a max-age is not cached at all. The client has no
// metrics dependency; set Config.Observe to feed lookup outcomes into whatever
// metrics library the caller uses.
//
// # Results are shared: treat them as read-only
//
// A cached response is decoded ONCE and every lookup of it — by any goroutine,
// for as long as the entry lives — receives a SHALLOW copy of that one value.
// The returned struct is the caller's own (reassigning a field of it affects
// nobody else), but everything it reaches through a map, slice or pointer is
// the cache's: a Pod's Labels, Annotations, PodIPs, Containers, Owners and
// NamespaceMetadata (for ContainerMetadata, those of its Pod, and its
// Container's Ports), a NodeMetadata's maps, and the slice NodeTargets returns
// together with each target's embedded Pod. Writing into any of them corrupts
// what every later lookup of that object returns, and doing so while another
// goroutine reads it is a data race — for a map, the runtime's fatal
// "concurrent map writes". Clone before modifying (maps.Clone, slices.Clone, or
// a deep copy for nested values). Copying on every hit instead would cost the
// allocations the cache exists to save, on the paths that call this most.
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
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/JohanLindvall/kubescrape/pkg/kubemeta"
)

// Request outcomes reported to Config.Observe.
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

// Client talks to a kubescrape metadata service. Responses carrying a positive
// Cache-Control max-age (without no-store or no-cache) are cached so repeat
// lookups (common on the concurrent ingest and cadvisor paths) are served
// locally or, once stale, revalidated cheaply with a conditional GET.
//
// A Client is safe for concurrent use, and the values its lookups return share
// their maps and slices with its cache and with every other caller: treat them
// as read-only and clone before modifying (see the package documentation).
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
	// the rest of the repo: pkg/ must never import internal/, so the type is
	// re-stated here — deliberately with the SAME loser rule, which is the half
	// of it that is easy to get wrong. (Go itself permits the import, even in
	// an external consumer's build: the internal rule is checked against the
	// IMPORTER's path, which is inside this module. The cost is what the rule
	// exists to avoid — internal packages and their dependencies dragged into
	// every consumer's build, and internal APIs, which promise nothing, made
	// part of this package's public behaviour. pkg/boundary_test.go enforces
	// it.)
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

// warnInterval throttles the three complaints that can persist: an unexpected
// status, an undecodable body, and the cache running at its hard cap. Each is
// a property of the deployment (a misdirected endpoint, a wrong service, a
// node whose live URL count exceeds the cap), so it repeats on every lookup
// until someone changes something.
const warnInterval = 5 * time.Minute

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
// That includes measuring on the MONOTONIC clock (an offset from
// throttleEpoch, plus one so zero keeps meaning "never fired"): wall-clock
// UnixNano stamps let a backwards clock step silence a fired throttle for the
// size of the step.
type throttle struct{ last atomic.Int64 }

// throttleEpoch is the process-local origin throttle measures from; time.Since
// on it reads only the runtime's monotonic nanotime.
var throttleEpoch = time.Now()

func (t *throttle) allow(interval time.Duration) bool {
	now := int64(time.Since(throttleEpoch)) + 1
	last := t.last.Load()
	return (last == 0 || now-last >= int64(interval)) && t.last.CompareAndSwap(last, now)
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
		// TestClientNeverUsesAnEnvironmentProxy pins it (the server half is
		// internal/server's TestASilentReOriginatingHopIsAnsweredWithTheHopsOwnIdentity).
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

// ScrapeAuth fetches the value of a secret key a monitor endpoint references —
// its bearer token, basic-auth username or password, authorization
// credentials, or TLS CA, client certificate or private key — by its
// "namespace/name/key" Secret reference (served only when the metadata
// service runs with -scrape-auth-secrets, and only for a reference some
// indexed monitor names). Responses are no-store; callers cache briefly
// themselves.
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
	// authed: this is the ONE authenticated route; the bearer decision is made
	// here, where the URL is built, not by re-matching the path later.
	out, err := getObject[scrapeAuthResponse](ctx, c, c.base+"/v1/scrape-auth/"+strings.Join(segs, "/"), request{authed: true})
	if err != nil {
		return "", err
	}
	return out.Value, nil
}

// scrapeAuthResponse is the /v1/scrape-auth document.
type scrapeAuthResponse struct {
	Value string `json:"value"`
}

// Container fetches metadata for a container ID, letting the service wait up
// to wait for the metadata to appear. wait must be shorter than the
// configured Config.Timeout, which covers the whole request — server-side
// wait included: a wait at or past it can never be honored (the client
// deadline fires while the server is still legitimately holding the request),
// so the combination is refused by name instead of surfacing as a timeout.
//
// The result shares its maps and slices with the cache: treat it as read-only
// (see the package documentation).
func (c *Client) Container(ctx context.Context, id string, wait time.Duration) (*kubemeta.ContainerMetadata, error) {
	if wait > 0 && c.http.Timeout > 0 && wait >= c.http.Timeout {
		c.observe(OutcomeError)
		return nil, fmt.Errorf("metaclient: Container wait %v must be shorter than Config.Timeout %v (the timeout covers the whole request, so the server-side wait could never elapse)", wait, c.http.Timeout)
	}
	key := c.base + "/v1/containers/" + url.PathEscape(kubemeta.NormalizeContainerID(id))
	return getObject[kubemeta.ContainerMetadata](ctx, c, key, request{wait: wait, hasWait: true})
}

// PodByName fetches metadata for one pod by namespace and name. The result
// shares its maps and slices with the cache: treat it as read-only.
func (c *Client) PodByName(ctx context.Context, namespace, name string) (*kubemeta.Pod, error) {
	return getObject[kubemeta.Pod](ctx, c, c.base+"/v1/pods/"+url.PathEscape(namespace)+"/"+url.PathEscape(name), request{})
}

// PodByUID fetches metadata for one pod by UID. The result shares its maps and
// slices with the cache: treat it as read-only.
func (c *Client) PodByUID(ctx context.Context, uid string) (*kubemeta.Pod, error) {
	return getObject[kubemeta.Pod](ctx, c, c.base+"/v1/pod-uids/"+url.PathEscape(uid), request{})
}

// PodByIP fetches metadata for the live pod owning a pod IP (404 for
// unknown, deleted, or hostNetwork pods). The result shares its maps and
// slices with the cache: treat it as read-only.
func (c *Client) PodByIP(ctx context.Context, ip string) (*kubemeta.Pod, error) {
	return getObject[kubemeta.Pod](ctx, c, c.base+"/v1/pod-ips/"+url.PathEscape(ip), request{})
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
// namespace costs a conditional GET, usually answered 304. Like every lookup's,
// the result shares its maps and slices with the cache: treat it as read-only.
func (c *Client) Self(ctx context.Context) (*kubemeta.Pod, error) {
	return getObject[kubemeta.Pod](ctx, c, c.base+"/v1/self", request{})
}

// Node fetches the labels and annotations of a node. The result shares its
// maps with the cache: treat it as read-only.
func (c *Client) Node(ctx context.Context, name string) (*kubemeta.NodeMetadata, error) {
	return getObject[kubemeta.NodeMetadata](ctx, c, c.base+"/v1/nodes/"+url.PathEscape(name)+"/metadata", request{})
}

// NodeTargets fetches the Prometheus scrape targets (with embedded pod
// metadata) for a node. The returned slice IS the cache's — its backing array
// and every target's embedded Pod are shared with every other caller — so it
// must not be sorted, compacted, appended to or written through: copy it
// (slices.Clone, plus a deep copy of any Pod field you change) first.
func (c *Client) NodeTargets(ctx context.Context, node string) ([]kubemeta.ScrapeTarget, error) {
	resp, err := getObject[kubemeta.NodeTargets](ctx, c, c.base+"/v1/nodes/"+url.PathEscape(node)+"/targets", request{})
	if err != nil {
		return nil, err
	}
	return resp.Targets, nil
}

// getObject is every lookup's body: fetch the resource cached under key (see
// get) into a fresh T and return it. A function rather than a method because a
// method cannot take a type parameter.
func getObject[T any](ctx context.Context, c *Client, key string, req request) (*T, error) {
	v := new(T)
	if err := c.get(ctx, key, req, v); err != nil {
		return nil, err
	}
	return v, nil
}

// request carries what a lookup sends BESIDE its cache key. Neither field is
// part of the resource's identity, so neither is in the key.
type request struct {
	// authed marks the one authenticated route (/v1/scrape-auth): the
	// decision is made where the URL is BUILT, so the route is spelled in
	// exactly one place rather than re-derived by matching the URL later.
	authed bool
	// wait, when hasWait is set, is the container endpoint's server-side
	// wait. It is rendered as "?wait=" only when a request is actually made —
	// a cache hit never formats it — and two lookups of one container that
	// differ only in their wait share one entry. hasWait rather than a zero
	// check because ?wait=0s is not the same request as no wait at all: the
	// service applies its own -wait-timeout to a lookup that names none.
	wait    time.Duration
	hasWait bool
}

// requestURL is the URL requested for key: the key plus any transient query.
func (r request) requestURL(key string) string {
	if !r.hasWait {
		return key
	}
	return key + "?wait=" + r.wait.String()
}

// get fetches the resource cached under key into v. key is the request URL
// WITHOUT a query; req carries the rest (see request).
func (c *Client) get(ctx context.Context, key string, req request, v any) error {
	// Fresh cache entry: serve without a request (and without re-decoding —
	// the decoded value is stored once and shallow-copied out).
	entry, cached, fresh := c.lookupEntry(key, v)
	if fresh {
		c.observe(OutcomeCached)
		copyDecoded(v, entry.decoded)
		return nil
	}
	return c.fetch(ctx, req.requestURL(key), key, entry, cached, req.authed, v)
}

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
		// largest legitimate body and stays far under this — while its pods'
		// own LABELS stay modest. Those are the one part of a pod document the
		// metadata service does not bound (they are selection input), and every
		// pod rides its node's response on its first target unconditionally,
		// so a node carrying a few dozen pods with ~1.5 MiB of labels each
		// reaches this cap: the decode then fails and that node's agent
		// schedules no annotation or monitor target at all (an accepted
		// residual, docs/CONFIGURATION.md).
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
				return c.undecodable(u, body, err)
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
			return c.undecodable(u, body, err)
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

// undecodable is the ONE exit for a 200 whose body this client could not
// decode, on both 200 arms (cached and not): the lookup is an error outcome —
// never "ok" — the condition is warned (throttled), and the caller gets the
// typed *DecodeError. The two arms used to spell the three steps out each.
func (c *Client) undecodable(u string, body []byte, err error) error {
	c.observe(OutcomeError)
	c.warnUndecodable(u, body, err)
	return decodeError(u, err)
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
// endpoint — or pods carrying enormous label sets, the one unbounded part of a
// pod document (see the read above) — and an unbounded read of it is an OOM on
// every node at once.
const maxResponseBytes = 64 << 20

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
