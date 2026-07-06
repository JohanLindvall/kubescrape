// Package metaclient is the HTTP client for the kubescrape metadata service.
package metaclient

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/JohanLindvall/kubescrape/internal/kubemeta"
	"github.com/JohanLindvall/kubescrape/internal/obs"
)

// Client talks to a kubescrape metadata service.
type Client struct {
	base string
	http *http.Client
}

// New creates a client for the service at base (e.g.
// "http://kubescrape.monitoring"). The overall request timeout must exceed
// the wait passed to Container.
func New(base string, timeout time.Duration) *Client {
	return &Client{
		base: strings.TrimRight(base, "/"),
		http: &http.Client{Timeout: timeout},
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
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return err
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
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		if resp.StatusCode == http.StatusNotFound {
			obs.MetadataRequests.WithLabelValues("not_found").Inc()
		} else {
			obs.MetadataRequests.WithLabelValues("error").Inc()
		}
		return &StatusError{Code: resp.StatusCode, Body: strings.TrimSpace(string(body))}
	}
	obs.MetadataRequests.WithLabelValues("ok").Inc()
	return json.NewDecoder(resp.Body).Decode(v)
}

// StatusError is a non-200 response from the metadata service.
type StatusError struct {
	Code int
	Body string
}

func (e *StatusError) Error() string {
	return fmt.Sprintf("metadata service returned %d: %s", e.Code, e.Body)
}

// IsNotFound reports whether err is a 404 from the metadata service.
func IsNotFound(err error) bool {
	se, ok := err.(*StatusError)
	return ok && se.Code == http.StatusNotFound
}
