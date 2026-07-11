// Package metaclient is the HTTP client for the kubescrape metadata service.
package metaclient

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/JohanLindvall/kubescrape/internal/kubemeta"
	"github.com/JohanLindvall/kubescrape/internal/obs"
)

// Client talks to a kubescrape metadata service. Responses carrying
// Cache-Control/ETag are cached so repeat lookups (common on the concurrent
// ingest and cadvisor paths) are served locally or revalidated cheaply with a
// conditional GET.
type Client struct {
	base string
	http *http.Client
	now  func() time.Time

	mu    sync.Mutex
	cache map[string]cacheEntry
}

type cacheEntry struct {
	body    []byte
	etag    string
	expires time.Time
}

// New creates a client for the service at base (e.g.
// "http://kubescrape.monitoring"). The overall request timeout must exceed
// the wait passed to Container.
func New(base string, timeout time.Duration) *Client {
	return &Client{
		base:  strings.TrimRight(base, "/"),
		http:  &http.Client{Timeout: timeout},
		now:   time.Now,
		cache: make(map[string]cacheEntry),
	}
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

	// Fresh cache entry: serve without a request.
	c.mu.Lock()
	entry, cached := c.cache[key]
	if cached && c.now().Before(entry.expires) {
		body := entry.body
		c.mu.Unlock()
		obs.MetadataRequests.WithLabelValues("cached").Inc()
		return json.Unmarshal(body, v)
	}
	c.mu.Unlock()

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
		obs.MetadataRequests.WithLabelValues("error").Inc()
		return err
	}
	defer func() {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
		_ = resp.Body.Close()
	}()

	switch {
	case resp.StatusCode == http.StatusNotModified && cached:
		// Unchanged: extend the cached entry's freshness and serve it.
		entry.expires = c.now().Add(maxAge(resp))
		c.mu.Lock()
		c.cache[key] = entry
		c.mu.Unlock()
		obs.MetadataRequests.WithLabelValues("not_modified").Inc()
		return json.Unmarshal(entry.body, v)
	case resp.StatusCode == http.StatusOK:
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			obs.MetadataRequests.WithLabelValues("error").Inc()
			return err
		}
		if ttl := maxAge(resp); ttl > 0 {
			c.mu.Lock()
			c.cache[key] = cacheEntry{body: body, etag: resp.Header.Get("ETag"), expires: c.now().Add(ttl)}
			c.mu.Unlock()
		}
		obs.MetadataRequests.WithLabelValues("ok").Inc()
		return json.Unmarshal(body, v)
	default:
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		if resp.StatusCode == http.StatusNotFound {
			obs.MetadataRequests.WithLabelValues("not_found").Inc()
		} else {
			obs.MetadataRequests.WithLabelValues("error").Inc()
		}
		return &StatusError{Code: resp.StatusCode, Body: strings.TrimSpace(string(body))}
	}
}

// cacheKey identifies the resource independent of transient request params
// (the container endpoint's ?wait= must not fragment the cache).
func cacheKey(u string) string {
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
