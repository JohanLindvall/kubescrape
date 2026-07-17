package metaclient

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// M1. Cache keying.
// ---------------------------------------------------------------------------

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
