package metaclient

// The cache HIT path is the inner loop of the agent's concurrent ingest
// enrichment: one Client is shared by every handler, and a push wave issues up
// to maxLookupsPerRequest lookups per handler. Serving a hit must therefore be
// a READ, not an exclusive acquisition of the one mutex every other handler
// needs.

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

// A hit must be servable while another goroutine holds the cache for READING.
// It used to take the exclusive lock — to write an idle stamp whose only
// consumer is a 5-minute eviction window — so every concurrent lookup queued
// behind every other one.
func TestCacheHitDoesNotTakeTheExclusiveLock(t *testing.T) {
	srv, hits := cachingServer(t, `"v1"`, `{"name":"web","uid":"u1"}`)
	c := New(Config{Base: srv.URL, Timeout: 5 * time.Second})
	ctx := context.Background()
	if _, err := c.PodByUID(ctx, "u1"); err != nil {
		t.Fatalf("populate: %v", err)
	}

	c.mu.RLock()
	defer c.mu.RUnlock()

	done := make(chan error, 1)
	go func() {
		_, err := c.PodByUID(ctx, "u1")
		done <- err
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("cache hit failed: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("a cache hit blocked behind a concurrent reader: the hit path must not take the write lock")
	}
	if n := atomic.LoadInt32(hits); n != 1 {
		t.Errorf("server hits = %d, want 1: the second lookup was served from the cache", n)
	}
}

// The idle stamp is still refreshed — coarsely, but well inside the eviction
// window, or a URL read on every push would be swept out from under itself.
func TestCacheHitRefreshesTheIdleStampWithinTheWindow(t *testing.T) {
	// Long-lived, so every read below is a HIT: the 304 path re-stamps under the
	// write lock it already takes, and would hide whether the hit path stamps at
	// all.
	var hits int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		atomic.AddInt32(&hits, 1)
		w.Header().Set("Cache-Control", "max-age=3600")
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"name":"web","uid":"u1"}`))
	}))
	t.Cleanup(srv.Close)
	c := New(Config{Base: srv.URL, Timeout: 5 * time.Second})
	now := time.Now()
	c.now = func() time.Time { return now }
	ctx := context.Background()
	if _, err := c.PodByUID(ctx, "u1"); err != nil {
		t.Fatalf("populate: %v", err)
	}

	// Read it steadily for well past the idle window, then sweep.
	for elapsed := time.Duration(0); elapsed < 3*cacheMaxIdle; elapsed += cacheMaxIdle / 4 {
		now = now.Add(cacheMaxIdle / 4)
		if _, err := c.PodByUID(ctx, "u1"); err != nil {
			t.Fatalf("read at %v: %v", elapsed, err)
		}
	}
	if n := atomic.LoadInt32(&hits); n != 1 {
		t.Fatalf("server hits = %d, want 1: the reads above must all be cache hits", n)
	}
	c.mu.Lock()
	c.lastSweep = time.Time{}
	c.evictLocked()
	n := len(c.cache)
	c.mu.Unlock()
	if n != 1 {
		t.Fatalf("cache entries = %d, want 1: a continuously-read entry was swept as idle", n)
	}
}

// BenchmarkCacheHitParallel reports what the hit path costs when the handlers
// that share one Client run concurrently — the shape that matters, since the
// contention is the point.
func BenchmarkCacheHitParallel(b *testing.B) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Cache-Control", "max-age=60")
		w.Header().Set("ETag", `"v1"`)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"name":"web","uid":"u1"}`))
	}))
	defer srv.Close()
	c := New(Config{Base: srv.URL, Timeout: 5 * time.Second})
	ctx := context.Background()
	if _, err := c.PodByUID(ctx, "u1"); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, err := c.PodByUID(ctx, "u1"); err != nil {
				b.Fatal(err)
			}
		}
	})
}
