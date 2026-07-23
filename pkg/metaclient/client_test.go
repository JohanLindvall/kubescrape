package metaclient

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
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

// Concurrent lookups of the same STALE URL each issue their own conditional
// GET — there is deliberately no single-flight. This test documents that
// behavior (the requests are cheap 304s; the trade-off is noted in the audit
// rather than fixed here). If single-flighting is ever added, flip the
// assertion.
func TestConcurrentRevalidationNoSingleFlight(t *testing.T) {
	var hits int32
	var inflight, maxInflight int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&hits, 1)
		cur := atomic.AddInt32(&inflight, 1)
		for {
			old := atomic.LoadInt32(&maxInflight)
			if cur <= old || atomic.CompareAndSwapInt32(&maxInflight, old, cur) {
				break
			}
		}
		time.Sleep(20 * time.Millisecond) // widen the herd window
		atomic.AddInt32(&inflight, -1)
		w.Header().Set("Cache-Control", "max-age=60")
		w.Header().Set("ETag", `"v1"`)
		if r.Header.Get("If-None-Match") == `"v1"` {
			w.WriteHeader(http.StatusNotModified)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"name":"web","uid":"u1"}`))
	}))
	t.Cleanup(srv.Close)

	c := New(srv.URL, 5*time.Second)
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

	const n = 8
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := c.PodByUID(context.Background(), "u1"); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()

	// 1 initial fetch + up to n concurrent revalidations. All must succeed and
	// every revalidation is correct (304 against the same ETag); the herd is a
	// documented efficiency gap, not a correctness bug.
	got := atomic.LoadInt32(&hits)
	if got < 2 || got > n+1 {
		t.Fatalf("hits = %d; want between 2 and %d", got, n+1)
	}
	t.Logf("stale revalidation herd: %d requests (max %d in flight) for one stale entry", got-1, atomic.LoadInt32(&maxInflight))
}

// The Observe hook must be optional (nil) on every outcome path, including
// errors.
func TestObserveNilSafe(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "nope", http.StatusNotFound)
	}))
	t.Cleanup(srv.Close)
	c := New(srv.URL, time.Second)
	c.Observe = nil
	if _, err := c.PodByUID(context.Background(), "u1"); !IsNotFound(err) {
		t.Fatalf("err = %v; want 404", err)
	}
}

// 404s are never cached (only 200s with a max-age are). Every lookup of an
// unresolvable ID therefore costs a full round trip — relevant for the peer-IP
// fallback, where a hostNetwork or non-pod sender pushing at a high rate
// re-queries /v1/pod-ips/{ip} for every single resource it ever pushes.
func TestNotFoundIsNotCached(t *testing.T) {
	var hits int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		atomic.AddInt32(&hits, 1)
		http.Error(w, "no live pod", http.StatusNotFound)
	}))
	t.Cleanup(srv.Close)

	c := New(srv.URL, time.Second)
	for i := 0; i < 3; i++ {
		if _, err := c.PodByIP(context.Background(), "10.0.0.9"); !IsNotFound(err) {
			t.Fatalf("err = %v; want 404", err)
		}
	}
	if got := atomic.LoadInt32(&hits); got != 3 {
		t.Fatalf("hits = %d, want 3 (negative results are not cached)", got)
	}
}

func TestAudit_CacheKeyStripsOnlyWait(t *testing.T) {
	cases := []struct{ in, want string }{
		{"http://h/v1/containers/a?wait=5s", "http://h/v1/containers/a"},
		{"http://h/v1/containers/a?wait=1s", "http://h/v1/containers/a"},
		{"http://h/v1/containers/a", "http://h/v1/containers/a"},
		{"http://h/v1/pods/ns/p?x=1&wait=2s", "http://h/v1/pods/ns/p?x=1"},
		{"http://h/v1/pods/ns/p?b=2&a=1", "http://h/v1/pods/ns/p?a=1&b=2"}, // normalized order
		{"http://h/v1/pods/ns/p?a=1&b=2", "http://h/v1/pods/ns/p?a=1&b=2"},
	}
	for _, tc := range cases {
		if got := cacheKey(tc.in); got != tc.want {
			t.Errorf("cacheKey(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
	// Two container lookups differing only in ?wait= must share one cache entry.
	if cacheKey("http://h/v1/containers/a?wait=5s") != cacheKey("http://h/v1/containers/a?wait=250ms") {
		t.Error("?wait= fragments the cache")
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
	c := New(s.URL, 5*time.Second)
	now := time.Now()
	c.now = func() time.Time { return now }
	var outcomes []string
	c.Observe = func(o string) { outcomes = append(outcomes, o) }

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
	c := New(s.URL, 5*time.Second)
	ctx := context.Background()
	for i := 0; i < 3; i++ {
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
	c := New(s.URL, 5*time.Second)
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
	c := New(s.URL, 5*time.Second)
	var outcome string
	c.Observe = func(o string) { outcome = o }
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

// TestAudit_BodyChangesUnderSameETag: the server rotates the body but keeps the
// ETag; the client must keep serving the cached body (correct HTTP semantics).
func TestAudit_ETagChangeRefetches(t *testing.T) {
	s := newSrv(t)
	s.etag, s.maxAge = `"v1"`, "10"
	c := New(s.URL, 5*time.Second)
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
	c := New(s.URL, 5*time.Second)
	var observed atomic.Int64
	c.Observe = func(string) { observed.Add(1) }

	const n = 64
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			pod, err := c.PodByName(context.Background(), "ns", "p")
			if err != nil {
				t.Errorf("PodByName: %v", err)
				return
			}
			if pod.Name != "p" {
				t.Errorf("pod = %+v", pod)
			}
		}()
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
	c := New(s.URL, 5*time.Second)
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ctx := context.Background()
			for j := 0; j < 20; j++ {
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
	c := New(s.URL, 5*time.Second)
	var got []string
	c.Observe = func(o string) { got = append(got, o) }
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
	dead := New("http://127.0.0.1:1", 200*time.Millisecond)
	dead.Observe = func(o string) { got = append(got, o) }
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
	c := New(s.URL, 5*time.Second)
	var got []string
	c.Observe = func(o string) { got = append(got, o) }
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
	c := New(s.URL, 5*time.Second)
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
