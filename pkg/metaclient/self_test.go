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

// selfServer serves a pod body with the headers the service sends on
// /v1/self — `private, max-age` + ETag — honoring If-None-Match, and records
// the conditional headers it saw.
func selfServer(t *testing.T, body string) (*httptest.Server, *int32, func() []string) {
	t.Helper()
	var hits int32
	var mu sync.Mutex
	var conds []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/self" {
			t.Errorf("path = %q; want /v1/self", r.URL.Path)
		}
		atomic.AddInt32(&hits, 1)
		mu.Lock()
		conds = append(conds, r.Header.Get("If-None-Match"))
		mu.Unlock()
		w.Header().Set("Cache-Control", "private, max-age=10")
		w.Header().Set("ETag", `"v1"`)
		if r.Header.Get("If-None-Match") == `"v1"` {
			w.WriteHeader(http.StatusNotModified)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(srv.Close)
	return srv, &hits, func() []string {
		mu.Lock()
		defer mu.Unlock()
		return append([]string(nil), conds...)
	}
}

// A fresh entry is served locally: re-reading own metadata inside the TTL
// costs nothing at all.
func TestSelfServedFromCacheWithinTTL(t *testing.T) {
	srv, hits, _ := selfServer(t, `{"name":"kubescrape-agent-xyz","namespace":"monitoring","uid":"agent-uid"}`)
	c := New(Config{Base: srv.URL, Timeout: 5 * time.Second})

	ctx := context.Background()
	pod, err := c.Self(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if pod.Name != "kubescrape-agent-xyz" || pod.Namespace != "monitoring" {
		t.Fatalf("pod = %+v", pod)
	}
	if _, err := c.Self(ctx); err != nil {
		t.Fatal(err)
	}
	if n := atomic.LoadInt32(hits); n != 1 {
		t.Fatalf("server hits = %d; want 1 (the second read served from cache)", n)
	}
}

// Past the TTL the re-read is a CONDITIONAL GET: that is what makes polling
// for a relabelled pod or namespace cheap, and what keeps serving the value
// when nothing changed.
func TestSelfRevalidatesAfterTTL(t *testing.T) {
	srv, hits, conds := selfServer(t, `{"name":"kubescrape-agent-xyz","namespace":"monitoring","uid":"agent-uid"}`)
	c := New(Config{Base: srv.URL, Timeout: 5 * time.Second})
	now := time.Now()
	c.now = func() time.Time { return now }

	ctx := context.Background()
	if _, err := c.Self(ctx); err != nil { // populates (hit 1)
		t.Fatal(err)
	}
	now = now.Add(time.Minute) // entry is stale
	pod, err := c.Self(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if pod.Name != "kubescrape-agent-xyz" {
		t.Fatalf("pod = %+v after revalidation", pod)
	}
	if n := atomic.LoadInt32(hits); n != 2 {
		t.Fatalf("server hits = %d; want 2", n)
	}
	if got := conds(); len(got) != 2 || got[1] != `"v1"` {
		t.Fatalf("conditional headers = %q; the stale re-read must send If-None-Match", got)
	}
}

// A caller the service cannot attribute to a live pod gets an error, not a
// zero-valued pod that would stamp empty attributes on everything — and the
// failure is not cached, so the next poll retries for real.
func TestSelfUnresolved(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		http.Error(w, `{"error":"no live pod with peer IP"}`, http.StatusNotFound)
	}))
	defer srv.Close()

	c := New(Config{Base: srv.URL, Timeout: time.Second})
	for i := range 2 {
		if pod, err := c.Self(context.Background()); err == nil {
			t.Fatalf("attempt %d: Self returned %+v; want an error for an unattributable caller", i, pod)
		}
	}
	if n := hits.Load(); n != 2 {
		t.Fatalf("server hits = %d; want 2 (a 404 must not be cached)", n)
	}
}

// The client must never route through an environment proxy: a proxy hop
// REPLACES the connection's source address, which is the only thing /v1/self
// attributes the caller by, so a cluster-wide HTTP_PROXY would stamp the
// PROXY's pod on every agent's self-metrics while the resolved gauge reads 1.
// The server half of that argument is internal/server's
// TestASilentReOriginatingHopIsAnsweredWithTheHopsOwnIdentity, which defers
// the client half to here. It is asserted structurally because it cannot be
// asserted behaviourally against httptest: http.ProxyFromEnvironment never
// proxies a loopback address, and it reads the environment only once per
// process.
func TestClientNeverUsesAnEnvironmentProxy(t *testing.T) {
	if NewTransport().Proxy != nil {
		t.Fatal("NewTransport sets a Proxy; the metadata service must be dialled directly")
	}
	c := New(Config{Base: "http://kubescrape.monitoring:8080"})
	tr, ok := c.http.Transport.(*http.Transport)
	if !ok {
		t.Fatalf("default transport is %T; want *http.Transport", c.http.Transport)
	}
	if tr.Proxy != nil {
		t.Fatal("New's default transport sets a Proxy; the metadata service must be dialled directly")
	}
}
