package metaclient

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/JohanLindvall/kubescrape/pkg/kubemeta"
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
	c := New(Config{Base: srv.URL, Timeout: 5 * time.Second})

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
	c := New(Config{Base: srv.URL, Timeout: 5 * time.Second})
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
			key := "http://" + r.Host + r.URL.Path
			c.mu.Lock()
			c.cache[key] = cacheEntry{
				decoded: &kubemeta.Pod{Name: "web-v2", UID: "u1"},
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
	key := c.base + "/v1/pod-uids/u1"

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
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		w.Header().Set("Cache-Control", "max-age=60")
		w.Header().Set("ETag", `"v1"`)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"containerId":"cafe01","pod":{"name":"web"}}`))
	}))
	t.Cleanup(srv.Close)
	c := New(Config{Base: srv.URL, Timeout: 5 * time.Second})

	ctx := context.Background()
	// Different wait values must resolve to the same cache entry. Both waits
	// sit under the 5s Timeout: Container now refuses wait >= Timeout (the
	// deadline would fire before the server's wait elapses — pinned by
	// TestContainerRefusesWaitAtOrPastTimeout), so this test may not use 5s.
	if _, err := c.Container(ctx, "cafe01", 2*time.Second); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Container(ctx, "cafe01", 0); err != nil {
		t.Fatal(err)
	}
	if n := hits.Load(); n != 1 {
		t.Fatalf("server hits = %d; want 1 (wait param must not fragment the cache)", n)
	}
}

// Config.Timeout covers the whole request, server-side wait included, so a
// wait at or past it can never be honored: the client deadline fires while
// the server is still legitimately holding the request, and the misuse reads
// as a transport error. Container refuses the combination by name instead.
func TestContainerRefusesWaitAtOrPastTimeout(t *testing.T) {
	srv, _ := cachingServer(t, `"v1"`, `{"containerId":"abc"}`)
	var outcomes []string
	c := New(Config{Base: srv.URL, Timeout: time.Second,
		Observe: func(o string) { outcomes = append(outcomes, o) }})
	ctx := context.Background()

	for _, wait := range []time.Duration{time.Second, 2 * time.Second} {
		_, err := c.Container(ctx, "abc", wait)
		if err == nil {
			t.Fatalf("wait %v with Timeout 1s was accepted", wait)
		}
		// The error names both values and the required relationship.
		for _, want := range []string{wait.String(), "1s", "shorter"} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("error %q does not name %q", err, want)
			}
		}
	}
	// The refusal is still a lookup: Observe is called once per lookup on
	// every path, this one included.
	if fmt.Sprint(outcomes) != fmt.Sprint([]string{OutcomeError, OutcomeError}) {
		t.Errorf("outcomes = %v, want two %s", outcomes, OutcomeError)
	}

	// Under the timeout the lookup proceeds.
	if _, err := c.Container(ctx, "abc", 500*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	// With no client timeout there is nothing for the wait to conflict with.
	c2 := New(Config{Base: srv.URL, Timeout: 0})
	if _, err := c2.Container(ctx, "abc", time.Hour); err != nil {
		t.Fatal(err)
	}
}

// no-store / no-cache forbid serving a stored response, and either beats an
// accompanying max-age regardless of directive order. Unreachable against
// kubescrape's own server (it never combines them with a max-age), but this
// package is public and other servers do.
func TestNoStoreNoCacheNeverCached(t *testing.T) {
	for _, cc := range []string{"no-store, max-age=60", "max-age=60, no-cache", "private, no-store"} {
		t.Run(cc, func(t *testing.T) {
			var hits atomic.Int32
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				hits.Add(1)
				w.Header().Set("Cache-Control", cc)
				w.Header().Set("ETag", `"v1"`)
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(`{"name":"web","uid":"u1"}`))
			}))
			t.Cleanup(srv.Close)
			c := New(Config{Base: srv.URL, Timeout: 5 * time.Second})
			for i := range 2 {
				if p, err := c.PodByUID(context.Background(), "u1"); err != nil || p.Name != "web" {
					t.Fatalf("lookup %d: pod=%+v err=%v", i, p, err)
				}
			}
			if n := hits.Load(); n != 2 {
				t.Fatalf("hits = %d, want 2 (%q must not be cached)", n, cc)
			}
		})
	}
}

func TestClientDoesNotCacheWithoutHeaders(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"name":"web"}`))
	}))
	t.Cleanup(srv.Close)
	c := New(Config{Base: srv.URL, Timeout: 5 * time.Second})

	ctx := context.Background()
	_, _ = c.PodByUID(ctx, "u1")
	_, _ = c.PodByUID(ctx, "u1")
	if n := hits.Load(); n != 2 {
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
	c := New(Config{Base: srv.URL, Timeout: time.Second})

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

	c := New(Config{Base: srv.URL, Timeout: time.Second})
	for i := range maxCacheEntries + 100 {
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

	c := New(Config{Base: srv.URL, Timeout: time.Second})
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
	c := New(Config{Base: s.URL, Timeout: 5 * time.Second})
	ctx := context.Background()
	if _, err := c.PodByUID(ctx, "u1"); err != nil { // populate
		b.Fatal(err)
	}
	b.ReportAllocs()
	for b.Loop() {
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
	c := New(Config{Base: s.URL, Timeout: 5 * time.Second})
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

// The flip side of the shallow copy, pinned so that moving to deep copies is a
// decision rather than an accident: two hits share ONE Labels map and one
// NodeTargets backing array with the cache. That sharing is the documented
// read-only contract (see the package doc) — a caller that writes into either
// corrupts every later lookup, and the hit path's allocation budget
// (TestCacheHitAllocationBudget) is what copying would cost.
func TestCacheHitsShareMapsAndSlicesWithTheCache(t *testing.T) {
	s := newSrv(t)
	s.maxAge = "3600"
	s.body = `{"name":"web","uid":"u1","labels":{"app":"web"},"targets":[{"url":"http://a"},{"url":"http://b"}]}`
	c := New(Config{Base: s.URL, Timeout: 5 * time.Second})
	ctx := context.Background()

	p1, err := c.PodByUID(ctx, "u1")
	if err != nil {
		t.Fatal(err)
	}
	p2, err := c.PodByUID(ctx, "u1")
	if err != nil {
		t.Fatal(err)
	}
	if p1 == p2 {
		t.Fatal("two lookups returned the same *Pod: the struct must be the caller's own")
	}
	if reflect.ValueOf(p1.Labels).UnsafePointer() != reflect.ValueOf(p2.Labels).UnsafePointer() {
		t.Error("two hits returned different Labels maps: the hit path now deep-copies — update the " +
			"package doc's read-only contract and TestCacheHitAllocationBudget together with it")
	}

	t1, err := c.NodeTargets(ctx, "n1")
	if err != nil {
		t.Fatal(err)
	}
	t2, err := c.NodeTargets(ctx, "n1")
	if err != nil {
		t.Fatal(err)
	}
	if len(t1) != 2 || len(t2) != 2 {
		t.Fatalf("targets = %d, %d; want 2 each", len(t1), len(t2))
	}
	if &t1[0] != &t2[0] {
		t.Error("two NodeTargets hits returned different backing arrays: the hit path now copies — " +
			"update the NodeTargets doc and the package doc with it")
	}
	if n := s.hits.Load(); n != 2 {
		t.Errorf("server hits = %d, want 2 (one per URL): the second lookups must be cache hits", n)
	}
}

// Concurrent lookups of the same STALE URL each issue their own conditional
// GET — there is deliberately no single-flight (the requests are cheap 304s;
// the trade-off is noted in the audit rather than fixed here). This test PINS
// that behaviour deterministically: the handler holds every revalidation until
// all n have arrived, so exactly n conditional GETs reach the service. If
// single-flighting is ever added, the barrier never fills, the test fails
// loudly on its timeout, and the assertion is the one to flip.
func TestConcurrentRevalidationNoSingleFlight(t *testing.T) {
	const n = 8
	var hits, arrived, timedOut atomic.Int32
	all := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.Header().Set("Cache-Control", "max-age=60")
		w.Header().Set("ETag", `"v1"`)
		if r.Header.Get("If-None-Match") == `"v1"` {
			// The barrier: nothing answers until every revalidation is in
			// flight at once. NewTransport sets no MaxConnsPerHost, so n
			// concurrent connections are allowed.
			if arrived.Add(1) == n {
				close(all)
			}
			select {
			case <-all:
			case <-time.After(2 * time.Second):
				timedOut.Add(1)
			}
			w.WriteHeader(http.StatusNotModified)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"name":"web","uid":"u1"}`))
	}))
	t.Cleanup(srv.Close)

	c := New(Config{Base: srv.URL, Timeout: 5 * time.Second})
	base := time.Now()
	now := base
	var mu sync.Mutex
	c.now = func() time.Time { mu.Lock(); defer mu.Unlock(); return now }

	if _, err := c.PodByUID(context.Background(), "u1"); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	now = base.Add(2 * time.Minute) // entry is now stale
	mu.Unlock()

	var wg sync.WaitGroup
	for range n {
		wg.Go(func() {
			if p, err := c.PodByUID(context.Background(), "u1"); err != nil || p.Name != "web" {
				t.Errorf("revalidating lookup: pod=%+v err=%v", p, err)
			}
		})
	}
	wg.Wait()

	if k := timedOut.Load(); k != 0 {
		t.Fatalf("%d revalidations waited out the barrier: only %d of %d concurrent lookups reached the "+
			"service — single-flight was added, so flip this test", k, arrived.Load(), n)
	}
	// 1 initial fetch + exactly n concurrent revalidations, every one a
	// correct 304 against the same ETag: the herd is a documented efficiency
	// gap, not a correctness bug.
	if got := hits.Load(); got != n+1 {
		t.Fatalf("hits = %d; want %d (one populate + one revalidation per concurrent lookup)", got, n+1)
	}
}

// The Observe hook must be optional (nil) on every outcome path, including
// errors.
func TestObserveNilSafe(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "nope", http.StatusNotFound)
	}))
	t.Cleanup(srv.Close)
	c := New(Config{Base: srv.URL, Timeout: time.Second, Observe: nil})
	if _, err := c.PodByUID(context.Background(), "u1"); !IsNotFound(err) {
		t.Fatalf("err = %v; want 404", err)
	}
}

// 404s are never cached (only 200s with a max-age are). Every lookup of an
// unresolvable ID therefore costs a full round trip — relevant for the peer-IP
// fallback, where a hostNetwork or non-pod sender pushing at a high rate
// re-queries /v1/pod-ips/{ip} for every single resource it ever pushes.
func TestNotFoundIsNotCached(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		http.Error(w, "no live pod", http.StatusNotFound)
	}))
	t.Cleanup(srv.Close)

	c := New(Config{Base: srv.URL, Timeout: time.Second})
	for range 3 {
		if _, err := c.PodByIP(context.Background(), "10.0.0.9"); !IsNotFound(err) {
			t.Fatalf("err = %v; want 404", err)
		}
	}
	if got := hits.Load(); got != 3 {
		t.Fatalf("hits = %d, want 3 (negative results are not cached)", got)
	}
}

// The container endpoint's wait is part of the REQUEST, never of the cache
// key: every request carries it — a zero wait included, spelled ?wait=0s,
// because a lookup naming no wait gets the service's own -wait-timeout — while
// lookups differing only in their wait share one entry, so a stale one is
// REVALIDATED under the new wait rather than fetched again from scratch.
func TestContainerWaitIsSentButNotKeyed(t *testing.T) {
	var mu sync.Mutex
	var queries, conds []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		queries = append(queries, r.URL.RawQuery)
		conds = append(conds, r.Header.Get("If-None-Match"))
		mu.Unlock()
		w.Header().Set("Cache-Control", "max-age=10")
		w.Header().Set("ETag", `"v1"`)
		if r.Header.Get("If-None-Match") == `"v1"` {
			w.WriteHeader(http.StatusNotModified)
			return
		}
		_, _ = w.Write([]byte(`{"containerId":"abc","container":{"name":"c"},"pod":{"name":"p","namespace":"ns"}}`))
	}))
	t.Cleanup(srv.Close)
	c := New(Config{Base: srv.URL, Timeout: 5 * time.Second})
	now := time.Now()
	c.now = func() time.Time { return now }
	ctx := context.Background()

	if _, err := c.Container(ctx, "abc", 0); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Container(ctx, "abc", 2*time.Second); err != nil { // a fresh hit: no request
		t.Fatal(err)
	}
	now = now.Add(11 * time.Second)
	if _, err := c.Container(ctx, "abc", 250*time.Millisecond); err != nil { // stale: revalidated
		t.Fatal(err)
	}

	mu.Lock()
	defer mu.Unlock()
	if want := []string{"wait=0s", "wait=250ms"}; !slices.Equal(queries, want) {
		t.Fatalf("queries = %q, want %q: the wait must be sent on every request and only there", queries, want)
	}
	if want := []string{"", `"v1"`}; !slices.Equal(conds, want) {
		t.Fatalf("If-None-Match = %q, want %q: a lookup under a different wait must revalidate the "+
			"one cached entry, not fetch a second one", conds, want)
	}
}

// ---------------------------------------------------------------------------
// M2. ETag / 304 / max-age combinations.
// ---------------------------------------------------------------------------

type srv struct {
	*httptest.Server
	hits     atomic.Int64
	conds    atomic.Int64 // requests carrying If-None-Match
	etag     string
	maxAge   string // "" = no Cache-Control
	body     string
	code     int
	mu       sync.Mutex
	lastETag string
}

func newSrv(t testing.TB) *srv {
	s := &srv{body: `{"name":"p","namespace":"ns"}`, code: 200}
	s.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.hits.Add(1)
		s.mu.Lock()
		etag, maxAge, body, code := s.etag, s.maxAge, s.body, s.code
		s.lastETag = r.Header.Get("If-None-Match")
		s.mu.Unlock()
		if r.Header.Get("If-None-Match") != "" {
			s.conds.Add(1)
		}
		if maxAge != "" {
			w.Header().Set("Cache-Control", "max-age="+maxAge)
		}
		if etag != "" {
			w.Header().Set("ETag", etag)
		}
		if code == 304 || (etag != "" && r.Header.Get("If-None-Match") == etag && code == 200) {
			w.WriteHeader(http.StatusNotModified)
			return
		}
		w.WriteHeader(code)
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(s.Close)
	return s
}

func TestAudit_ETagRevalidation(t *testing.T) {
	s := newSrv(t)
	s.etag, s.maxAge = `"v1"`, "10"
	var outcomes []string
	c := New(Config{Base: s.URL, Timeout: 5 * time.Second,
		Observe: func(o string) { outcomes = append(outcomes, o) }})
	now := time.Now()
	c.now = func() time.Time { return now }

	ctx := context.Background()
	if _, err := c.PodByName(ctx, "ns", "p"); err != nil {
		t.Fatal(err)
	}
	// Fresh: served from cache, no request.
	if _, err := c.PodByName(ctx, "ns", "p"); err != nil {
		t.Fatal(err)
	}
	if s.hits.Load() != 1 {
		t.Fatalf("fresh cache hit still made %d requests", s.hits.Load())
	}
	// Stale: conditional GET -> 304, and the entry's freshness is extended.
	now = now.Add(11 * time.Second)
	if _, err := c.PodByName(ctx, "ns", "p"); err != nil {
		t.Fatal(err)
	}
	if s.conds.Load() != 1 {
		t.Fatalf("stale entry did not revalidate with If-None-Match")
	}
	if _, err := c.PodByName(ctx, "ns", "p"); err != nil {
		t.Fatal(err)
	}
	if s.hits.Load() != 2 {
		t.Fatalf("the 304 did not refresh the entry's expiry (hits=%d)", s.hits.Load())
	}
	want := []string{OutcomeOK, OutcomeCached, OutcomeNotModified, OutcomeCached}
	if fmt.Sprint(outcomes) != fmt.Sprint(want) {
		t.Fatalf("outcomes = %v, want %v", outcomes, want)
	}
}

// TestAudit_ETagWithoutMaxAge: an ETag with no Cache-Control is never cached, so
// it can never be revalidated either — every lookup is a full GET.
func TestAudit_ETagWithoutMaxAge(t *testing.T) {
	s := newSrv(t)
	s.etag, s.maxAge = `"v1"`, "" // ETag but no max-age
	c := New(Config{Base: s.URL, Timeout: 5 * time.Second})
	ctx := context.Background()
	for range 3 {
		if _, err := c.PodByName(ctx, "ns", "p"); err != nil {
			t.Fatal(err)
		}
	}
	if s.conds.Load() != 0 {
		t.Fatalf("If-None-Match sent for an entry that was never cached")
	}
	if s.hits.Load() != 3 {
		t.Fatalf("hits = %d, want 3", s.hits.Load())
	}
	t.Logf("CONTRACT: an ETag without Cache-Control:max-age is not cached at all (%d full GETs) — "+
		"caching is gated on max-age>0 only", s.hits.Load())
}

// TestAudit_MaxAgeWithoutETag: cacheable but not revalidatable — a stale entry
// must fall back to a full GET, not send an empty If-None-Match.
func TestAudit_MaxAgeWithoutETag(t *testing.T) {
	s := newSrv(t)
	s.etag, s.maxAge = "", "10"
	c := New(Config{Base: s.URL, Timeout: 5 * time.Second})
	now := time.Now()
	c.now = func() time.Time { return now }
	ctx := context.Background()
	if _, err := c.PodByName(ctx, "ns", "p"); err != nil {
		t.Fatal(err)
	}
	now = now.Add(11 * time.Second)
	if _, err := c.PodByName(ctx, "ns", "p"); err != nil {
		t.Fatal(err)
	}
	if s.conds.Load() != 0 {
		t.Fatal("sent If-None-Match with no ETag")
	}
	if s.hits.Load() != 2 {
		t.Fatalf("hits = %d, want 2", s.hits.Load())
	}
}

// TestAudit_UnsolicitedNotModified: a 304 with no cached entry must surface as
// an error, not as an empty/zero object silently unmarshalled.
func TestAudit_UnsolicitedNotModified(t *testing.T) {
	s := newSrv(t)
	s.code = 304
	var outcome string
	c := New(Config{Base: s.URL, Timeout: 5 * time.Second,
		Observe: func(o string) { outcome = o }})
	pod, err := c.PodByName(context.Background(), "ns", "p")
	if err == nil {
		t.Fatalf("BUG: an unsolicited 304 with no cached entry returned pod %+v and no error", pod)
	}
	var se *StatusError
	if !as(err, &se) || se.Code != 304 {
		t.Fatalf("err = %v (%T), want *StatusError{304}", err, err)
	}
	if outcome != OutcomeError {
		t.Errorf("outcome = %q, want %q", outcome, OutcomeError)
	}
}

func as(err error, target **StatusError) bool {
	se, ok := err.(*StatusError)
	if ok {
		*target = se
	}
	return ok
}

// A changed ETag replaces the cached body: once the entry goes stale, the
// revalidation carries the OLD ETag, the service answers 200 with a new one,
// and both that lookup and every later hit serve the NEW body.
func TestAudit_ETagChangeRefetches(t *testing.T) {
	s := newSrv(t)
	s.etag, s.maxAge = `"v1"`, "10"
	c := New(Config{Base: s.URL, Timeout: 5 * time.Second})
	now := time.Now()
	c.now = func() time.Time { return now }
	ctx := context.Background()
	p, err := c.PodByName(ctx, "ns", "p")
	if err != nil || p.Name != "p" {
		t.Fatalf("%+v %v", p, err)
	}
	// The pod changes; the service issues a new ETag.
	s.mu.Lock()
	s.etag, s.body = `"v2"`, `{"name":"p2","namespace":"ns"}`
	s.mu.Unlock()
	now = now.Add(11 * time.Second)
	p, err = c.PodByName(ctx, "ns", "p")
	if err != nil {
		t.Fatal(err)
	}
	if p.Name != "p2" {
		t.Fatalf("stale body served after the ETag changed: %+v", p)
	}
	// And the new body must be what the cache now holds.
	p, err = c.PodByName(ctx, "ns", "p")
	if err != nil || p.Name != "p2" {
		t.Fatalf("cache not updated with the new body: %+v %v", p, err)
	}
}

// ---------------------------------------------------------------------------
// M3. Concurrency.
// ---------------------------------------------------------------------------

func TestAudit_ConcurrentSameURL(t *testing.T) {
	s := newSrv(t)
	s.etag, s.maxAge = `"v1"`, "60"
	var observed atomic.Int64
	c := New(Config{Base: s.URL, Timeout: 5 * time.Second,
		Observe: func(string) { observed.Add(1) }})

	const n = 64
	var wg sync.WaitGroup
	start := make(chan struct{})
	for range n {
		wg.Go(func() {
			<-start
			pod, err := c.PodByName(context.Background(), "ns", "p")
			if err != nil {
				t.Errorf("PodByName: %v", err)
				return
			}
			if pod.Name != "p" {
				t.Errorf("pod = %+v", pod)
			}
		})
	}
	close(start)
	wg.Wait()
	if observed.Load() != n {
		t.Errorf("Observe called %d times for %d lookups", observed.Load(), n)
	}
	t.Logf("NO SINGLEFLIGHT: %d concurrent lookups of one URL produced %d server requests "+
		"(the cache dedupes only AFTER the first response lands)", n, s.hits.Load())
}

func TestAudit_ConcurrentMixedURLsRace(t *testing.T) {
	s := newSrv(t)
	s.etag, s.maxAge = `"v1"`, "1"
	c := New(Config{Base: s.URL, Timeout: 5 * time.Second})
	var wg sync.WaitGroup
	for i := range 32 {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ctx := context.Background()
			for j := range 20 {
				switch j % 4 {
				case 0:
					_, _ = c.Container(ctx, fmt.Sprintf("containerd://c%d", i%4), time.Second)
				case 1:
					_, _ = c.PodByName(ctx, "ns", fmt.Sprintf("p%d", i%4))
				case 2:
					_, _ = c.PodByUID(ctx, fmt.Sprintf("u%d", i%4))
				case 3:
					_, _ = c.PodByIP(ctx, fmt.Sprintf("10.0.0.%d", i%4))
				}
			}
		}(i)
	}
	wg.Wait()
}

// ---------------------------------------------------------------------------
// M4. Errors and Observe.
// ---------------------------------------------------------------------------

func TestAudit_ObserveOutcomes(t *testing.T) {
	s := newSrv(t)
	var got []string
	c := New(Config{Base: s.URL, Timeout: 5 * time.Second,
		Observe: func(o string) { got = append(got, o) }})
	ctx := context.Background()

	s.mu.Lock()
	s.code = 404
	s.mu.Unlock()
	_, err := c.PodByName(ctx, "ns", "gone")
	if !IsNotFound(err) {
		t.Fatalf("IsNotFound(%v) = false", err)
	}

	s.mu.Lock()
	s.code = 500
	s.mu.Unlock()
	if _, err := c.PodByName(ctx, "ns", "boom"); err == nil {
		t.Fatal("500 returned nil error")
	}

	// Transport failure.
	dead := New(Config{Base: "http://127.0.0.1:1", Timeout: 200 * time.Millisecond,
		Observe: func(o string) { got = append(got, o) }})
	if _, err := dead.PodByName(ctx, "ns", "x"); err == nil {
		t.Fatal("dead server returned nil error")
	}

	want := []string{OutcomeNotFound, OutcomeError, OutcomeError}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("outcomes = %v, want %v", got, want)
	}
}

// TestAudit_MalformedJSONObservedOK pins that a 200 whose body will not decode
// is still reported as OutcomeOK (and cached).
func TestAudit_MalformedJSONObservedNotCached(t *testing.T) {
	s := newSrv(t)
	s.maxAge = "10"
	s.body = `{"name": ` // truncated JSON
	var got []string
	c := New(Config{Base: s.URL, Timeout: 5 * time.Second,
		Observe: func(o string) { got = append(got, o) }})
	if _, err := c.PodByName(context.Background(), "ns", "p"); err == nil {
		t.Fatal("truncated JSON decoded without error")
	}
	if len(got) != 1 || got[0] != OutcomeError {
		t.Fatalf("outcome = %v, want [%s]: a body that fails to decode must not be reported ok", got, OutcomeError)
	}
	// The malformed body must NOT be cached (decode-before-store): the second
	// call reaches the server again instead of re-failing from a poisoned
	// entry for the whole TTL — the same stance as 404s, which are never
	// cached either.
	if _, err := c.PodByName(context.Background(), "ns", "p"); err == nil {
		t.Fatal("second call decoded without error")
	}
	if s.hits.Load() != 2 {
		t.Fatalf("hits = %d, want 2 (malformed 200 must not be cached)", s.hits.Load())
	}
}

// TestAudit_ContainerNormalizesID: the runtime prefix must be stripped before
// the URL is built, so a caller passing "containerd://x" and one passing "x"
// share a cache entry and hit the same endpoint.
func TestAudit_ContainerNormalizesID(t *testing.T) {
	s := newSrv(t)
	s.maxAge = "60"
	s.body = `{"containerId":"abc","container":{"name":"c"},"pod":{"name":"p","namespace":"ns"}}`
	var paths []string
	var mu sync.Mutex
	s.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		paths = append(paths, r.URL.Path)
		mu.Unlock()
		w.Header().Set("Cache-Control", "max-age=60")
		_, _ = w.Write([]byte(s.body))
	})
	c := New(Config{Base: s.URL, Timeout: 5 * time.Second})
	ctx := context.Background()
	if _, err := c.Container(ctx, "containerd://abc", time.Second); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Container(ctx, "abc", 2*time.Second); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(paths) != 1 {
		t.Fatalf("made %d requests (%v); the normalized ID + wait-stripped key must share one entry", len(paths), paths)
	}
	if paths[0] != "/v1/containers/abc" {
		t.Fatalf("path = %q, want /v1/containers/abc", paths[0])
	}
}

// The cache stores ONE decoded value per URL and no raw body beside it, so an
// entry that cannot be copied into the caller's result type is dropped and
// re-fetched (unconditionally — an If-None-Match would only earn another 304 it
// could not decode). Unreachable in practice: every caller of an endpoint asks
// for the same type.
func TestCacheTypeMismatchRefetches(t *testing.T) {
	s := newSrv(t)
	s.etag, s.maxAge = `"v1"`, "3600"
	s.body = `{"name":"web","namespace":"ns","uid":"u1"}`
	c := New(Config{Base: s.URL, Timeout: 5 * time.Second})
	ctx := context.Background()
	u := c.base + "/v1/pods/ns/web"

	var first kubemeta.Pod
	if err := c.get(ctx, u, request{}, &first); err != nil {
		t.Fatal(err)
	}
	if n := s.hits.Load(); n != 1 {
		t.Fatalf("hits = %d after populate, want 1", n)
	}

	// Same URL, different result type: the cached (fresh) entry cannot serve it.
	var other struct {
		Name string `json:"name"`
	}
	if err := c.get(ctx, u, request{}, &other); err != nil {
		t.Fatal(err)
	}
	if other.Name != "web" {
		t.Fatalf("mismatched-type lookup returned %+v", other)
	}
	if n := s.hits.Load(); n != 2 {
		t.Fatalf("hits = %d; a type-mismatched entry must be re-fetched, not served from a retained body", n)
	}
	s.mu.Lock()
	cond := s.lastETag
	s.mu.Unlock()
	if cond != "" {
		t.Fatalf("re-fetch sent If-None-Match %q; it must not revalidate an entry it cannot decode", cond)
	}

	// The re-fetch re-populated the cache under the new type: no more requests.
	var again struct {
		Name string `json:"name"`
	}
	if err := c.get(ctx, u, request{}, &again); err != nil {
		t.Fatal(err)
	}
	if n := s.hits.Load(); n != 2 || again.Name != "web" {
		t.Fatalf("hits = %d, name = %q; the re-fetch should have cached the new type", n, again.Name)
	}
}

// A slow poller must keep its ETag. The sweep used to delete on EXPIRY —
// which is precisely the stale-but-revalidatable state If-None-Match exists
// for — so every URL read less often than the 1m sweep (the 1m self and
// node-metadata refreshes against a 10s TTL) re-fetched a full body forever.
func TestSweepKeepsStaleEntriesForRevalidation(t *testing.T) {
	srv, hits := cachingServer(t, `"v1"`, `{"name":"web","uid":"u1"}`)
	c := New(Config{Base: srv.URL, Timeout: 5 * time.Second})
	now := time.Now()
	c.now = func() time.Time { return now }

	ctx := context.Background()
	_, _ = c.PodByUID(ctx, "u1") // populate (hit 1)

	// Three reads at a 1m cadence: each is stale (10s TTL), so each is a
	// conditional GET the server answers 304 — never a full re-fetch.
	for i := range 3 {
		now = now.Add(time.Minute)
		if p, err := c.PodByUID(ctx, "u1"); err != nil || p.Name != "web" {
			t.Fatalf("read %d: pod=%v err=%v", i, p, err)
		}
	}
	if n := atomic.LoadInt32(hits); n != 4 {
		t.Fatalf("server hits = %d; want 4 (one populate + three revalidations)", n)
	}
	if got := len(c.cache); got != 1 {
		t.Fatalf("cache entries = %d; want 1 — the entry the slow poller keeps revalidating", got)
	}

	// Left alone past the idle window, it IS swept: a dead container's pod
	// document must not be held forever.
	now = now.Add(cacheMaxIdle + time.Minute)
	c.mu.Lock()
	c.evictLocked()
	c.mu.Unlock()
	if got := len(c.cache); got != 0 {
		t.Fatalf("cache entries = %d; want 0 — an idle entry is never asked for again", got)
	}
}

// A decode failure used to leave this package bare: the consumer saw
// encoding/json's `invalid character 'x' ...` with nothing naming metaclient,
// the endpoint or the URL, while every sibling failure path returned a typed
// error. Both cache arms (a cacheable 200 and a no-store one) go through the
// same wrapper.
func TestDecodeErrorNamesThePackageAndTheURL(t *testing.T) {
	for _, maxAge := range []string{"10", ""} {
		name := "cacheable"
		if maxAge == "" {
			name = "no-store"
		}
		t.Run(name, func(t *testing.T) {
			s := newSrv(t)
			s.maxAge = maxAge
			s.body = `{"name": ` // truncated JSON
			c := New(Config{Base: s.URL, Timeout: 5 * time.Second})

			_, err := c.PodByName(context.Background(), "ns", "p")
			if err == nil {
				t.Fatal("truncated JSON decoded without error")
			}
			var de *DecodeError
			if !errors.As(err, &de) {
				t.Fatalf("err = %v (%T), want a *DecodeError", err, err)
			}
			if !strings.Contains(de.URL, "/v1/pods/ns/p") {
				t.Errorf("DecodeError.URL = %q, want the requested endpoint", de.URL)
			}
			msg := err.Error()
			if !strings.Contains(msg, "metaclient") {
				t.Errorf("the error does not name this package: %q", msg)
			}
			if !strings.Contains(msg, de.URL) {
				t.Errorf("the error does not name the URL: %q", msg)
			}
			// The json error itself must stay reachable.
			var se *json.SyntaxError
			if !errors.As(err, &se) && !errors.Is(err, io.ErrUnexpectedEOF) && de.Unwrap() == nil {
				t.Errorf("the wrapped encoding/json error is unreachable: %v", err)
			}
		})
	}
}

// The over-cap eviction performs the same idle sweep the periodic branch does,
// so it must advance lastSweep: left behind, the next below-cap insert re-ran
// a full O(n) idle sweep under the mutex shared with the concurrent ingest and
// cadvisor lookups, however recently the hard trim had swept.
func TestOverCapEvictionAdvancesSweepCadence(t *testing.T) {
	c := New(Config{Base: "http://unused", Timeout: time.Second})
	now := time.Now()
	c.now = func() time.Time { return now }

	c.mu.Lock()
	for i := 0; i <= maxCacheEntries; i++ {
		c.cache[fmt.Sprintf("u%d", i)] = cacheEntry{used: now}
	}
	c.evictLocked()
	got, sweep := len(c.cache), c.lastSweep
	c.mu.Unlock()

	if got > evictLowWater {
		t.Fatalf("cache entries = %d after the over-cap eviction, want <= %d", got, evictLowWater)
	}
	if !sweep.Equal(now) {
		t.Fatalf("lastSweep = %v, want %v: the over-cap eviction swept idles without recording it", sweep, now)
	}
}
