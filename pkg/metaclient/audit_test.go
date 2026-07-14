package metaclient

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// audit_test.go: targeted tests from the 2026-07 audit.

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

// cacheKey must strip only ?wait= and leave other query params intact (and
// stable), so hypothetical future params don't alias distinct resources.
func TestCacheKeyStripsOnlyWait(t *testing.T) {
	a := cacheKey("http://x/v1/containers/abc?wait=2s")
	b := cacheKey("http://x/v1/containers/abc?wait=5s")
	c := cacheKey("http://x/v1/containers/abc")
	if a != b || a != c {
		t.Fatalf("wait variants not collapsed: %q %q %q", a, b, c)
	}
	d := cacheKey("http://x/v1/containers/abc?foo=1&wait=2s")
	e := cacheKey("http://x/v1/containers/abc?wait=9s&foo=1")
	if d != e {
		t.Fatalf("other params not stable: %q vs %q", d, e)
	}
	if d == a {
		t.Fatalf("distinct param sets aliased: %q", d)
	}
}

// A 304 must extend the entry's freshness, so the NEXT lookup inside the new
// max-age is served locally with no request at all.
func TestNotModifiedRefreshesFreshness(t *testing.T) {
	var hits int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&hits, 1)
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
	c.now = func() time.Time { return now }

	if _, err := c.PodByUID(context.Background(), "u1"); err != nil { // 200, cached
		t.Fatal(err)
	}
	now = base.Add(2 * time.Minute)                                   // stale
	if _, err := c.PodByUID(context.Background(), "u1"); err != nil { // 304, refresh
		t.Fatal(err)
	}
	now = base.Add(2*time.Minute + 30*time.Second) // inside the refreshed max-age
	if _, err := c.PodByUID(context.Background(), "u1"); err != nil {
		t.Fatal(err)
	}
	if got := atomic.LoadInt32(&hits); got != 2 {
		t.Fatalf("hits = %d, want 2: the 304 must have refreshed the entry's freshness", got)
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
