package metaclient

// The Cache-Control max-age is the SERVER's number, and time.Duration is int64
// NANOseconds: `secs * time.Second` wraps from ~1.845e10 seconds up. This
// package is public — Config.Base points wherever the embedder says — and it
// already bounds the response body and the ETag path against an endpoint that
// is not kubescrape's, so the freshness lifetime is bounded too.

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync/atomic"
	"testing"
	"time"
)

func TestMaxAgeIsClamped(t *testing.T) {
	for _, tc := range []struct {
		header string
		want   time.Duration
		why    string
	}{
		{"max-age=60", time.Minute, "an ordinary value passes through"},
		{"max-age=86400", maxCacheTTL, "exactly the cap"},
		{"max-age=86401", maxCacheTTL, "one second past the cap"},
		// The two overflow shapes the finding names. Unclamped, the first wraps
		// POSITIVE to ~49 years (an entry that is never revalidated again) and
		// the second wraps NEGATIVE (ttl > 0 is false, so caching is silently
		// off — the opposite of what the header asked for).
		{"max-age=20000000000", maxCacheTTL, "wraps positive to ~49 years unclamped"},
		{"max-age=9300000000", maxCacheTTL, "wraps negative unclamped"},
		{"max-age=9223372036854775807", maxCacheTTL, "int64 max"},
		// Unchanged behaviour, asserted here so the clamp cannot be read as
		// having widened anything.
		{"max-age=0", 0, "zero is not a TTL"},
		{"max-age=-5", 0, "negative is not a TTL"},
		{"max-age=99999999999999999999999", 0, "past int64: Atoi fails, no TTL"},
		{"no-store, max-age=20000000000", 0, "no-store beats any max-age"},
		{"", 0, "absent"},
	} {
		resp := &http.Response{Header: http.Header{}}
		if tc.header != "" {
			resp.Header.Set("Cache-Control", tc.header)
		}
		if got := maxAge(resp); got != tc.want {
			t.Errorf("maxAge(%q) = %v, want %v (%s)", tc.header, got, tc.want, tc.why)
		}
	}
}

// The clamp seen from the outside: an entry stored under a wrapped-positive
// max-age is never revalidated again, because it is fresh for ~49 years and the
// idle sweep cannot reclaim an entry that is being read.
func TestOverflowingMaxAgeDoesNotPinAnEntryForever(t *testing.T) {
	var hits int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		n := atomic.AddInt32(&hits, 1)
		w.Header().Set("Cache-Control", "max-age=20000000000")
		w.Header().Set("ETag", `"v`+strconv.Itoa(int(n))+`"`)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"name":"web","uid":"u1"}`))
	}))
	t.Cleanup(srv.Close)

	c := New(Config{Base: srv.URL, Timeout: 5 * time.Second})
	now := time.Now()
	c.now = func() time.Time { return now }
	ctx := context.Background()
	if _, err := c.PodByUID(ctx, "u1"); err != nil {
		t.Fatal(err)
	}
	if _, err := c.PodByUID(ctx, "u1"); err != nil {
		t.Fatal(err)
	}
	if n := atomic.LoadInt32(&hits); n != 1 {
		t.Fatalf("hits = %d, want 1: the entry is cached for the clamped lifetime", n)
	}
	// A day and an hour later the entry must be revalidated. Unclamped it is
	// still fresh — for another 49 years.
	now = now.Add(maxCacheTTL + time.Hour)
	if _, err := c.PodByUID(ctx, "u1"); err != nil {
		t.Fatal(err)
	}
	if n := atomic.LoadInt32(&hits); n != 2 {
		t.Fatalf("hits = %d after %v; a max-age that overflows time.Duration must not pin the "+
			"entry past maxCacheTTL", n, maxCacheTTL+time.Hour)
	}
}

// The mirror case: a max-age that wraps NEGATIVE disabled caching entirely, so
// every lookup went to the wire — the opposite of what the header asked for.
func TestNegativelyOverflowingMaxAgeStillCaches(t *testing.T) {
	var hits int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		atomic.AddInt32(&hits, 1)
		w.Header().Set("Cache-Control", "max-age=9300000000")
		w.Header().Set("ETag", `"v1"`)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"name":"web","uid":"u1"}`))
	}))
	t.Cleanup(srv.Close)

	c := New(Config{Base: srv.URL, Timeout: 5 * time.Second})
	ctx := context.Background()
	for i := range 3 {
		if _, err := c.PodByUID(ctx, "u1"); err != nil {
			t.Fatalf("lookup %d: %v", i, err)
		}
	}
	if n := atomic.LoadInt32(&hits); n != 1 {
		t.Fatalf("hits = %d, want 1: a max-age too large to represent must clamp, not disable caching", n)
	}
}
