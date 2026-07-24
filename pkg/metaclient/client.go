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
// metrics dependency; set Client.Observe to feed lookup outcomes into whatever
// metrics library the caller uses.
package metaclient

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"reflect"
	"strconv"
	"strings"
	"sync"
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

	// Observe, if set, is called once per lookup with the outcome (one of the
	// Outcome* constants). It is the hook callers use to feed their own
	// metrics without this package depending on a metrics library. Set it
	// before the client is shared between goroutines — it is read without
	// synchronization — and keep it cheap and non-blocking (it runs on the
	// caller's goroutine).
	Observe func(outcome string)

	mu    sync.Mutex
	cache map[string]cacheEntry
}

// observe reports an outcome when a hook is installed.
func (c *Client) observe(outcome string) {
	if c.Observe != nil {
		c.Observe(outcome)
	}
}

type cacheEntry struct {
	body []byte
	// decoded is the body unmarshaled once into the caller's result type
	// (a pointer), stored so cache hits and 304s skip the JSON decode. It is
	// never mutated after storing; hits receive a SHALLOW copy — maps/slices
	// are shared under the same treat-as-immutable contract the store uses.
	decoded any
	etag    string
	expires time.Time
}

// New creates a client for the service at base (e.g.
// "http://kubescrape.monitoring"). The overall request timeout must exceed
// the wait passed to Container.
func New(base string, timeout time.Duration) *Client {
	// A dedicated transport: DefaultTransport's MaxIdleConnsPerHost of 2
	// forces most connections to close under the highly concurrent ingest
	// enrichment load (everything goes to the one metadata-service host).
	transport := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
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
	return &Client{
		base:  strings.TrimRight(base, "/"),
		http:  &http.Client{Timeout: timeout, Transport: transport},
		now:   time.Now,
		cache: make(map[string]cacheEntry),
	}
}

// ScrapeAuth fetches a monitor endpoint's bearer token by its
// "namespace/name/key" Secret reference (served only when the metadata
// service runs with -scrape-auth-secrets). Responses are no-store; callers
// cache briefly themselves.
func (c *Client) ScrapeAuth(ctx context.Context, ref string) (string, error) {
	var out struct {
		Value string `json:"value"`
	}
	if err := c.getJSON(ctx, c.base+"/v1/scrape-auth/"+ref, &out); err != nil {
		return "", err
	}
	return out.Value, nil
}

// Container fetches metadata for a container ID, letting the service wait up
// to wait for the metadata to appear.
func (c *Client) Container(ctx context.Context, id string, wait time.Duration) (*kubemeta.ContainerMetadata, error) {
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

func (c *Client) getJSON(ctx context.Context, u string, v any) error {
	key := cacheKey(u)

	// Fresh cache entry: serve without a request (and without re-decoding —
	// the decoded value is stored once and shallow-copied out).
	entry, cached, fresh := c.lookupEntry(key)
	if fresh {
		c.observe(OutcomeCached)
		return entry.serve(v)
	}
	return c.fetch(ctx, u, key, entry, cached, v)
}

// lookupEntry reads the cache under the lock, classifying the entry as fresh
// (serve locally), stale-but-present (revalidate with If-None-Match), or
// absent.
func (c *Client) lookupEntry(key string) (entry cacheEntry, cached, fresh bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	entry, cached = c.cache[key]
	fresh = cached && c.now().Before(entry.expires)
	return entry, cached, fresh
}

// serve copies the entry's decoded value into v (shallow — maps/slices are
// shared under the treat-as-immutable contract), falling back to re-decoding
// the stored body for a type mismatch.
func (e cacheEntry) serve(v any) error {
	if shallowCopy(v, e.decoded) {
		return nil
	}
	return json.Unmarshal(e.body, v)
}

// fetch performs the HTTP request (revalidating with the entry's ETag when
// cached), stores a cacheable 200, and decodes into v.
func (c *Client) fetch(ctx context.Context, u, key string, entry cacheEntry, cached bool, v any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return err
	}
	// Revalidate a stale-but-present entry cheaply.
	if cached && entry.etag != "" {
		req.Header.Set("If-None-Match", entry.etag)
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
			c.cache[key] = cur
		}
		c.mu.Unlock()
		c.observe(OutcomeNotModified)
		if shallowCopy(v, entry.decoded) {
			return nil
		}
		return json.Unmarshal(entry.body, v)
	case resp.StatusCode == http.StatusOK:
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			c.observe(OutcomeError)
			return err
		}
		if ttl := maxAge(resp); ttl > 0 {
			// Decode into a value the CACHE owns, not into v: the caller may
			// overwrite its struct after the call, which must not reach the
			// cached copy. v gets a shallow copy of the owned value.
			dec := reflect.New(reflect.TypeOf(v).Elem())
			if err := json.Unmarshal(body, dec.Interface()); err != nil {
				c.observe(OutcomeError)
				return err
			}
			c.mu.Lock()
			c.cache[key] = cacheEntry{body: body, decoded: dec.Interface(), etag: resp.Header.Get("ETag"), expires: c.now().Add(ttl)}
			c.evictLocked()
			c.mu.Unlock()
			c.observe(OutcomeOK)
			reflect.ValueOf(v).Elem().Set(dec.Elem())
			return nil
		}
		if err := json.Unmarshal(body, v); err != nil {
			// Match the TTL path: an undecodable 200 is an error, not "ok".
			c.observe(OutcomeError)
			return err
		}
		c.observe(OutcomeOK)
		return nil
	default:
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		if resp.StatusCode == http.StatusNotFound {
			c.observe(OutcomeNotFound)
		} else {
			c.observe(OutcomeError)
		}
		return &StatusError{Code: resp.StatusCode, Body: strings.TrimSpace(string(body))}
	}
}

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

// evictLocked trims the cache when it exceeds the cap: expired entries first,
// then arbitrary ones (they re-fetch cheaply via ETag revalidation). Caller
// holds the mutex.
func (c *Client) evictLocked() {
	if len(c.cache) <= maxCacheEntries {
		return
	}
	now := c.now()
	for k, e := range c.cache {
		if now.After(e.expires) {
			delete(c.cache, k)
		}
	}
	for k := range c.cache {
		if len(c.cache) <= evictLowWater {
			break
		}
		delete(c.cache, k)
	}
}

// shallowCopy sets *dst = *src when both are pointers to the same type,
// reporting success. The copy is shallow: maps and slices stay shared with the
// cached value, which is never mutated (the store's shallow-copy contract).
func shallowCopy(dst, src any) bool {
	if src == nil {
		return false
	}
	dv, sv := reflect.ValueOf(dst), reflect.ValueOf(src)
	if dv.Kind() != reflect.Pointer || dv.Type() != sv.Type() {
		return false
	}
	dv.Elem().Set(sv.Elem())
	return true
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

// maxAge extracts the Cache-Control max-age; zero when absent or unparseable.
func maxAge(resp *http.Response) time.Duration {
	for _, part := range strings.Split(resp.Header.Get("Cache-Control"), ",") {
		part = strings.TrimSpace(part)
		if v, ok := strings.CutPrefix(part, "max-age="); ok {
			if secs, err := strconv.Atoi(v); err == nil && secs > 0 {
				return time.Duration(secs) * time.Second
			}
		}
	}
	return 0
}

// StatusError is a non-200 response from the metadata service.
type StatusError struct {
	Code int
	Body string
}

func (e *StatusError) Error() string {
	return fmt.Sprintf("metadata service returned %d: %s", e.Code, e.Body)
}

// IsNotFound reports whether err is (or wraps) a 404 from the metadata
// service.
func IsNotFound(err error) bool {
	var se *StatusError
	return errors.As(err, &se) && se.Code == http.StatusNotFound
}
