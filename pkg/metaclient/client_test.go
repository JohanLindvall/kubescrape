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

// A 304 must not clobber a newer 200 entry that a concurrent goroutine stored
// while the revalidating request was in flight: only the entry whose ETag this
// request actually validated may have its freshness extended.
func TestClient304DoesNotClobberNewerEntry(t *testing.T) {
	c := &Client{now: time.Now, http: &http.Client{Timeout: 5 * time.Second}, cache: make(map[string]cacheEntry)}
	now := time.Now()
	c.now = func() time.Time { return now }

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "max-age=60")
		if r.Header.Get("If-None-Match") == `"v1"` {
			// Simulate the race: while this revalidation is in flight (the
			// client lock is released), a concurrent goroutine stores a newer
			// 200 entry under the same key.
			key := cacheKey("http://" + r.Host + r.URL.Path)
			c.mu.Lock()
			c.cache[key] = cacheEntry{
				body:    []byte(`{"name":"web-v2","uid":"u1"}`),
				etag:    `"v2"`,
				expires: time.Now().Add(time.Hour), // fresh vs. the fake clock
			}
			c.mu.Unlock()
			w.WriteHeader(http.StatusNotModified)
			return
		}
		w.Header().Set("ETag", `"v1"`)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"name":"web-v1","uid":"u1"}`))
	}))
	t.Cleanup(srv.Close)
	c.base = srv.URL
	key := cacheKey(c.base + "/v1/pod-uids/u1")

	ctx := context.Background()
	if p, err := c.PodByUID(ctx, "u1"); err != nil || p.Name != "web-v1" {
		t.Fatalf("populate: pod=%+v err=%v", p, err)
	}
	now = now.Add(2 * time.Minute) // entry goes stale -> next call revalidates

	// The revalidating request itself still serves the body it validated.
	if p, err := c.PodByUID(ctx, "u1"); err != nil || p.Name != "web-v1" {
		t.Fatalf("revalidate: pod=%+v err=%v", p, err)
	}
	// But the concurrently stored newer entry must survive the 304.
	c.mu.Lock()
	entry := c.cache[key]
	c.mu.Unlock()
	if entry.etag != `"v2"` {
		t.Fatalf("cache etag = %s; 304 clobbered the newer 200 entry", entry.etag)
	}
	if p, err := c.PodByUID(ctx, "u1"); err != nil || p.Name != "web-v2" {
		t.Fatalf("post-304 cache read: pod=%+v err=%v", p, err)
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

func TestPodByIP(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/pod-ips/10.0.0.9" {
			_, _ = w.Write([]byte(`{"name":"web-9","namespace":"ns","uid":"u9"}`))
			return
		}
		http.Error(w, "no live pod", http.StatusNotFound)
	}))
	defer srv.Close()

	c := New(srv.URL, time.Second)
	pod, err := c.PodByIP(context.Background(), "10.0.0.9")
	if err != nil || pod.Name != "web-9" {
		t.Fatalf("pod=%+v err=%v", pod, err)
	}
	if _, err := c.PodByIP(context.Background(), "10.9.9.9"); !IsNotFound(err) {
		t.Fatalf("want 404, got %v", err)
	}
}

func BenchmarkCacheHitPod(b *testing.B) {
	s := newSrv(b)
	s.maxAge = "3600"
	s.body = `{"name":"web-abc","namespace":"prod","uid":"u1","nodeName":"n1","podIP":"10.0.0.1",` +
		`"labels":{"app":"web","tier":"fe","team":"core"},"annotations":{"prometheus.io/scrape":"true"},` +
		`"createdAt":"2026-07-01T10:00:00Z","phase":"Running","containers":[` +
		`{"name":"app","image":"img:1","id":"c1","ports":[{"name":"http","port":8080}]},` +
		`{"name":"sidecar","image":"img2:1","id":"c2"}]}`
	c := New(s.URL, 5*time.Second)
	ctx := context.Background()
	if _, err := c.PodByUID(ctx, "u1"); err != nil { // populate
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p, err := c.PodByUID(ctx, "u1")
		if err != nil || p.Name != "web-abc" {
			b.Fatal(err)
		}
	}
}

// Cache hits hand out SHALLOW copies of one decoded value: a caller
// overwriting top-level fields of its result must not corrupt what the next
// caller receives (maps/slices are shared under the treat-as-immutable
// contract; the struct itself is not).
func TestCacheHitShallowCopyIsolation(t *testing.T) {
	s := newSrv(t)
	s.maxAge = "3600"
	s.body = `{"name":"web","uid":"u1","labels":{"app":"web"}}`
	c := New(s.URL, 5*time.Second)
	ctx := context.Background()

	p1, err := c.PodByUID(ctx, "u1")
	if err != nil {
		t.Fatal(err)
	}
	p1.Name = "CLOBBERED"
	p1.Labels = nil

	p2, err := c.PodByUID(ctx, "u1")
	if err != nil {
		t.Fatal(err)
	}
	if p2.Name != "web" || p2.Labels["app"] != "web" {
		t.Fatalf("cache corrupted by caller mutation: %+v", p2)
	}
}

func BenchmarkCacheKeyWait(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if cacheKey("http://x/v1/containers/abcdef0123?wait=2s") != "http://x/v1/containers/abcdef0123" {
			b.Fatal("bad key")
		}
	}
}
