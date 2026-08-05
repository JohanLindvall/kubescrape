// Tests for the request handlers (handlers.go): wait budgets and the
// metadata response caching (Cache-Control/ETag) behavior.
package server

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/JohanLindvall/kubescrape/internal/services"
	"github.com/JohanLindvall/kubescrape/internal/store"
)

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
		"wait=-9223372037",          // negative overflow: wraps positive in the naive multiplication
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
	srv := cachingServer(t, st, 10*time.Second)

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

// A plain-integer ?wait= with a SUB-SECOND MaxWait must clamp to MaxWait, not
// truncate to zero whole seconds (which silently made the lookup non-blocking).
func TestWaitBudgetSubSecondMaxWait(t *testing.T) {
	s := New(Config{MaxWait: 700 * time.Millisecond})
	for _, tc := range []struct {
		q    string
		want time.Duration
	}{
		{"?wait=1", 700 * time.Millisecond},              // integer form clamps to MaxWait
		{"?wait=1s", 700 * time.Millisecond},             // duration form clamps identically
		{"?wait=200ms", 200 * time.Millisecond},          // shorter than MaxWait honored
		{"?wait=99999999999999", 700 * time.Millisecond}, // overflow-guard path
		{"", 700 * time.Millisecond},                     // default = MaxWait
	} {
		r := httptest.NewRequest("GET", "/v1/containers/x"+tc.q, nil)
		got, err := s.waitBudget(r)
		if err != nil {
			t.Fatalf("%q: %v", tc.q, err)
		}
		if got != tc.want {
			t.Fatalf("%q: budget = %v, want %v", tc.q, got, tc.want)
		}
	}
}

// /v1/pod-ips 200s must carry NO freshness lifetime even with a CacheTTL
// configured: the IP index exists for immediacy (IPs recycle), and a cached
// entry would let metaclient re-serve the OLD owner of a recycled IP for up to
// the TTL. It is spelled `no-store` rather than left blank — silence lets a
// shared cache store and heuristically freshen the response anyway. The
// pod-name/uid endpoints keep their cache headers.
func TestPodByIPUncached(t *testing.T) {
	st := store.New(time.Minute)
	addPod(st)
	srv := cachingServer(t, st, 10*time.Second)

	get := func(path string) http.Header {
		resp, err := http.Get(srv.URL + path)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = resp.Body.Close() }()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("%s: status = %d", path, resp.StatusCode)
		}
		return resp.Header
	}
	if h := get("/v1/pod-ips/10.1.2.3"); h.Get("Cache-Control") != "no-store" || h.Get("ETag") != "" {
		t.Fatalf("pod-ips cache headers = %q / %q; want no-store and no ETag",
			h.Get("Cache-Control"), h.Get("ETag"))
	}
	if h := get("/v1/pod-uids/pod-uid"); h.Get("Cache-Control") == "" {
		t.Fatal("pod-uids lost its cache headers")
	}
}

// The two routes whose answers must never be stored say so EXPLICITLY, on
// misses as well as hits. A response with no explicit freshness information is
// storable and heuristically freshenable by a shared cache (RFC 9111 4.2.2),
// and 404 is one of the statuses that applies to (RFC 9110 15.1) — so "we sent
// no Cache-Control" was never the same as "do not store this". What that cost:
// a cached /v1/pod-ips answer outlives the pod that took the recycled address,
// and a cached /v1/self answer — the one response that names its CALLER —
// hands the first asker's identity to the next.
func TestNoStoreOnCallerAndPodIPRoutes(t *testing.T) {
	st := store.New(time.Minute)
	addPod(st) // owns 10.1.2.3; nothing owns the loopback address the test client calls from
	srv := cachingServer(t, st, 10*time.Second)

	for _, tc := range []struct {
		path, want string
		status     int
	}{
		{"/v1/pod-ips/10.1.2.3", "no-store", http.StatusOK},
		{"/v1/pod-ips/10.9.9.9", "no-store", http.StatusNotFound},
		// The /v1/self 200 stays privately CACHEABLE on purpose (private +
		// max-age + ETag makes a self-attributes re-read a 304); only its 404,
		// which carries no lifetime at all, needs the explicit refusal.
		{"/v1/self", "private, no-store", http.StatusNotFound},
	} {
		resp, err := http.Get(srv.URL + tc.path)
		if err != nil {
			t.Fatal(err)
		}
		cc := resp.Header.Get("Cache-Control")
		code := resp.StatusCode
		_ = resp.Body.Close()
		if code != tc.status {
			t.Fatalf("%s: status = %d, want %d", tc.path, code, tc.status)
		}
		if cc != tc.want {
			t.Errorf("%s: Cache-Control = %q, want %q", tc.path, cc, tc.want)
		}
	}
}

// The node-targets route carries the writeCached treatment like the other
// metadata 200s: a stable ETag across identical requests (the target order
// must be deterministic — PodsOnNode iterates a map) and a 304 on
// If-None-Match, so agents stop re-downloading every pod document on the
// node each scrape cycle when nothing changed.
func TestNodeTargetsCachedWithStableETag(t *testing.T) {
	st := store.New(time.Minute)
	addPod(st)
	// MANY pods, each with several annotated ports. The property under test is
	// that the response is SORTED before hashing, because PodsOnNode iterates a
	// map and Go randomizes map order per range — with a single pod and a
	// single target the slice is trivially ordered and the test could not fail
	// however the handler behaved, which is what it did for its whole life.
	// Enough targets that an unsorted body would differ across requests with
	// overwhelming probability.
	for i := 0; i < 12; i++ {
		addTargetPod(st, i)
	}
	srv := cachingServer(t, st, 10*time.Second)

	url := srv.URL + "/v1/nodes/node1/targets"
	etag := condGet(t, url, "", http.StatusOK)
	if etag == "" {
		t.Fatal("no ETag on the targets response")
	}
	for i := 0; i < 20; i++ { // deterministic body → stable tag
		if e := condGet(t, url, "", http.StatusOK); e != etag {
			t.Fatalf("ETag changed across identical requests: %q vs %q "+
				"(the targets response must be sorted before hashing)", e, etag)
		}
	}
	condGet(t, url, etag, http.StatusNotModified)

	// With caching disabled (TTL 0) the route stays a plain 200, no ETag.
	plain := cachingServer(t, st, 0)
	if e := condGet(t, plain.URL+"/v1/nodes/node1/targets", "", http.StatusOK); e != "" {
		t.Fatalf("TTL 0 must not stamp an ETag, got %q", e)
	}
}
