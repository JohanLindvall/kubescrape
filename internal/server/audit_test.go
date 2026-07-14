package server

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/JohanLindvall/kubescrape/internal/services"
	"github.com/JohanLindvall/kubescrape/internal/store"
)

// audit_test.go: targeted tests from the 2026-07 audit.

// A client-supplied wait must only ever SHORTEN the server budget. Both the
// duration form and the plain-seconds form (including values that would
// overflow naive Duration arithmetic) clamp to MaxWait.
func TestWaitClampsToMaxWait(t *testing.T) {
	st := store.New(time.Minute)
	srv := testServer(t, st, closedChan()) // MaxWait 500ms

	for _, q := range []string{
		"wait=1h",                  // duration beyond max
		"wait=3600",                // plain seconds beyond max
		"wait=9223372036854775807", // would overflow secs*time.Second without the pre-clamp
	} {
		start := time.Now()
		getJSON(t, srv.URL+"/v1/containers/nope?"+q, http.StatusNotFound, nil)
		if elapsed := time.Since(start); elapsed > 3*time.Second {
			t.Fatalf("%s: waited %v; want clamp to the 500ms MaxWait", q, elapsed)
		}
	}
}

// Hostile/degenerate wait values: rejected or degraded, never a long block.
func TestWaitParameterEdgeCases(t *testing.T) {
	st := store.New(time.Minute)
	srv := testServer(t, st, closedChan())

	// Rejected outright.
	for _, q := range []string{
		"wait=-3600",                // negative seconds
		"wait=-1h",                  // negative duration
		"wait=1e18",                 // scientific notation is neither form
		"wait=99999999999999999999", // overflows int64 in both parsers
		"wait=%20",                  // whitespace
	} {
		getJSON(t, srv.URL+"/v1/containers/nope?"+q, http.StatusBadRequest, nil)
	}
	// wait=0 degrades to a non-blocking miss.
	start := time.Now()
	getJSON(t, srv.URL+"/v1/containers/nope?wait=0", http.StatusNotFound, nil)
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("wait=0 blocked for %v", elapsed)
	}
}

// The ETag must be stable across identical responses (map-key JSON ordering
// is deterministic in encoding/json); a changing ETag would defeat every
// metaclient revalidation.
func TestETagStableAcrossIdenticalResponses(t *testing.T) {
	st := store.New(time.Minute)
	addPod(st)
	srv := httptest.NewServer(New(Config{
		Store:    st,
		Services: services.NewIndex(),
		Resolver: stubResolver{},
		MaxWait:  500 * time.Millisecond,
		CacheTTL: 10 * time.Second,
		Ready:    closedChan(),
	}).Handler())
	t.Cleanup(srv.Close)

	get := func() string {
		resp, err := http.Get(srv.URL + "/v1/containers/cafe01")
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = resp.Body.Close() }()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("status %d", resp.StatusCode)
		}
		return resp.Header.Get("ETag")
	}
	first := get()
	if first == "" {
		t.Fatal("no ETag")
	}
	for i := 0; i < 5; i++ {
		if got := get(); got != first {
			t.Fatalf("ETag changed across identical responses: %q -> %q", first, got)
		}
	}
}

// CacheTTL 0 disables the whole conditional-request surface: no ETag header,
// and If-None-Match (even "*") is ignored — always a full 200.
func TestCacheTTLZeroDisables304(t *testing.T) {
	st := store.New(time.Minute)
	addPod(st)
	srv := testServer(t, st, closedChan()) // CacheTTL 0

	req, _ := http.NewRequest(http.MethodGet, srv.URL+"/v1/containers/cafe01", nil)
	req.Header.Set("If-None-Match", "*")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d; want 200 with caching disabled", resp.StatusCode)
	}
	if et := resp.Header.Get("ETag"); et != "" {
		t.Fatalf("ETag %q; want none with caching disabled", et)
	}
}

// If-None-Match may carry a LIST of entity tags, and weak comparison (W/) must
// match. Both work.
func TestIfNoneMatchMultipleETags(t *testing.T) {
	st := store.New(time.Minute)
	addPod(st)
	srv := cachingServer(t, st, 10*time.Second)

	etag := condGet(t, srv.URL+"/v1/containers/cafe01", "", http.StatusOK)
	for _, hdr := range []string{
		`"deadbeef", ` + etag,
		etag + `, "deadbeef"`,
		`"a", W/` + etag + `, "b"`,
		`*`,
	} {
		condGet(t, srv.URL+"/v1/containers/cafe01", hdr, http.StatusNotModified)
	}
	// A non-matching list is a full 200.
	condGet(t, srv.URL+"/v1/containers/cafe01", `"nope", W/"other"`, http.StatusOK)
}

// max-age has second granularity, so a sub-second -metadata-cache-ttl used to
// truncate to "max-age=0" — which tells the client to cache NOTHING (the
// opposite of a short cache) while the server still hashed an ETag per
// response. Any non-zero TTL must round up to at least a second; TTL 0 disables
// caching before the header is written (TestCacheTTLZeroDisables304).
func TestSubSecondCacheTTLRoundsUp(t *testing.T) {
	st := store.New(time.Minute)
	addPod(st)
	srv := cachingServer(t, st, 500*time.Millisecond)

	resp, err := http.Get(srv.URL + "/v1/containers/cafe01")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if got := resp.Header.Get("Cache-Control"); got != "max-age=1" {
		t.Fatalf("Cache-Control = %q; want max-age=1 (a sub-second TTL must not disable client caching)", got)
	}
}

func cachingServer(t *testing.T, st *store.Store, ttl time.Duration) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(New(Config{
		Store:    st,
		Services: services.NewIndex(),
		Resolver: stubResolver{},
		MaxWait:  500 * time.Millisecond,
		CacheTTL: ttl,
		Ready:    closedChan(),
	}).Handler())
	t.Cleanup(srv.Close)
	return srv
}

func condGet(t *testing.T, url, inm string, want int) string {
	t.Helper()
	req, _ := http.NewRequest(http.MethodGet, url, nil)
	if inm != "" {
		req.Header.Set("If-None-Match", inm)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != want {
		t.Fatalf("If-None-Match %q: status %d, want %d", inm, resp.StatusCode, want)
	}
	return resp.Header.Get("ETag")
}
