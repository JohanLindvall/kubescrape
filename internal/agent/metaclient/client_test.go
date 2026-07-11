package metaclient

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

// cachingServer serves a pod body with Cache-Control + ETag and honors
// If-None-Match, counting how many requests actually reached it.
func cachingServer(t *testing.T, etag, body string) (*httptest.Server, *int32) {
	t.Helper()
	var hits int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&hits, 1)
		w.Header().Set("Cache-Control", "max-age=60")
		w.Header().Set("ETag", etag)
		if r.Header.Get("If-None-Match") == etag {
			w.WriteHeader(http.StatusNotModified)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(srv.Close)
	return srv, &hits
}

func TestClientServesFromCacheWithinTTL(t *testing.T) {
	srv, hits := cachingServer(t, `"v1"`, `{"name":"web","uid":"u1"}`)
	c := New(srv.URL, 5*time.Second)

	ctx := context.Background()
	if p, err := c.PodByUID(ctx, "u1"); err != nil || p.Name != "web" {
		t.Fatalf("first: pod=%v err=%v", p, err)
	}
	if p, err := c.PodByUID(ctx, "u1"); err != nil || p.Name != "web" {
		t.Fatalf("second: pod=%v err=%v", p, err)
	}
	if n := atomic.LoadInt32(hits); n != 1 {
		t.Fatalf("server hits = %d; want 1 (second served from cache)", n)
	}
}

func TestClientRevalidatesAfterTTL(t *testing.T) {
	srv, hits := cachingServer(t, `"v1"`, `{"name":"web","uid":"u1"}`)
	c := New(srv.URL, 5*time.Second)
	now := time.Now()
	c.now = func() time.Time { return now }

	ctx := context.Background()
	_, _ = c.PodByUID(ctx, "u1") // populate cache (hit 1)
	now = now.Add(2 * time.Minute)
	// Stale: a conditional request is made; the server returns 304 and the
	// value is still served from the cached body.
	if p, err := c.PodByUID(ctx, "u1"); err != nil || p.Name != "web" {
		t.Fatalf("revalidated pod=%v err=%v", p, err)
	}
	if n := atomic.LoadInt32(hits); n != 2 {
		t.Fatalf("server hits = %d; want 2 (one populate + one revalidate)", n)
	}
	// Freshness extended by the 304: the next call serves from cache again.
	if _, err := c.PodByUID(ctx, "u1"); err != nil {
		t.Fatal(err)
	}
	if n := atomic.LoadInt32(hits); n != 2 {
		t.Fatalf("server hits = %d; want 2 (304 refreshed freshness)", n)
	}
}

func TestClientCacheIgnoresWaitParam(t *testing.T) {
	var hits int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		atomic.AddInt32(&hits, 1)
		w.Header().Set("Cache-Control", "max-age=60")
		w.Header().Set("ETag", `"v1"`)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"containerId":"cafe01","pod":{"name":"web"}}`))
	}))
	t.Cleanup(srv.Close)
	c := New(srv.URL, 5*time.Second)

	ctx := context.Background()
	// Different wait values must resolve to the same cache entry.
	if _, err := c.Container(ctx, "cafe01", 5*time.Second); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Container(ctx, "cafe01", 0); err != nil {
		t.Fatal(err)
	}
	if n := atomic.LoadInt32(&hits); n != 1 {
		t.Fatalf("server hits = %d; want 1 (wait param must not fragment the cache)", n)
	}
}

func TestClientDoesNotCacheWithoutHeaders(t *testing.T) {
	var hits int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		atomic.AddInt32(&hits, 1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"name":"web"}`))
	}))
	t.Cleanup(srv.Close)
	c := New(srv.URL, 5*time.Second)

	ctx := context.Background()
	_, _ = c.PodByUID(ctx, "u1")
	_, _ = c.PodByUID(ctx, "u1")
	if n := atomic.LoadInt32(&hits); n != 2 {
		t.Fatalf("server hits = %d; want 2 (no cache headers = no caching)", n)
	}
}

func TestClientEndpoints(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/v1/pods/ns1/pod1":
			_, _ = w.Write([]byte(`{"name":"pod1","namespace":"ns1","uid":"u1"}`))
		case "/v1/nodes/node1/metadata":
			_, _ = w.Write([]byte(`{"name":"node1","labels":{"zone":"eu-1"}}`))
		case "/v1/nodes/node1/targets":
			_, _ = w.Write([]byte(`{"targets":[{"url":"http://10.0.0.5:8080/metrics","pod":{"name":"pod1","namespace":"ns1"}}]}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()
	c := New(srv.URL, time.Second)

	pod, err := c.PodByName(context.Background(), "ns1", "pod1")
	if err != nil || pod.Name != "pod1" || pod.UID != "u1" {
		t.Fatalf("PodByName = %+v, %v", pod, err)
	}
	node, err := c.Node(context.Background(), "node1")
	if err != nil || node.Labels["zone"] != "eu-1" {
		t.Fatalf("Node = %+v, %v", node, err)
	}
	targets, err := c.NodeTargets(context.Background(), "node1")
	if err != nil || len(targets) != 1 || targets[0].Pod.Name != "pod1" {
		t.Fatalf("NodeTargets = %+v, %v", targets, err)
	}

	// 404s surface as StatusError, recognized by IsNotFound even when wrapped.
	_, err = c.PodByName(context.Background(), "ns1", "missing")
	if !IsNotFound(err) {
		t.Fatalf("IsNotFound(%v) = false", err)
	}
	if !IsNotFound(fmt.Errorf("looking up pod: %w", err)) {
		t.Fatal("IsNotFound must unwrap")
	}
	if IsNotFound(errors.New("other")) {
		t.Fatal("IsNotFound on unrelated error")
	}
	var se *StatusError
	if !errors.As(err, &se) || se.Code != http.StatusNotFound || se.Error() == "" {
		t.Fatalf("StatusError = %+v", err)
	}
}

// The response cache is bounded: churning through more distinct URLs than the
// cap does not grow the map without limit.
func TestCacheEviction(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "max-age=3600")
		w.Header().Set("ETag", `"x"`)
		_, _ = w.Write([]byte(`{"name":"p","namespace":"ns","uid":"u"}`))
	}))
	defer srv.Close()

	c := New(srv.URL, time.Second)
	for i := 0; i < maxCacheEntries+100; i++ {
		if _, err := c.PodByUID(context.Background(), fmt.Sprintf("uid-%d", i)); err != nil {
			t.Fatal(err)
		}
	}
	c.mu.Lock()
	n := len(c.cache)
	c.mu.Unlock()
	if n > maxCacheEntries {
		t.Fatalf("cache size = %d, want <= %d", n, maxCacheEntries)
	}
	if n == 0 {
		t.Fatal("cache unexpectedly empty")
	}
}
