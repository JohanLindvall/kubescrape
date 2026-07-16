package server

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/JohanLindvall/kubescrape/internal/services"
	"github.com/JohanLindvall/kubescrape/internal/store"
)

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

// /v1/pod-ips 200s must carry NO cache headers even with a CacheTTL configured:
// the IP index exists for immediacy (IPs recycle), and a cached entry would let
// metaclient re-serve the OLD owner of a recycled IP for up to the TTL. The
// pod-name/uid endpoints keep their cache headers.
func TestPodByIPUncached(t *testing.T) {
	st := store.New(time.Minute)
	addPod(st)
	srv := httptest.NewServer(New(Config{
		Store: st, Services: services.NewIndex(), Resolver: stubResolver{},
		MaxWait: 500 * time.Millisecond, CacheTTL: 10 * time.Second, Ready: closedChan(),
	}).Handler())
	t.Cleanup(srv.Close)

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
	if h := get("/v1/pod-ips/10.1.2.3"); h.Get("Cache-Control") != "" || h.Get("ETag") != "" {
		t.Fatalf("pod-ips carried cache headers: %q / %q", h.Get("Cache-Control"), h.Get("ETag"))
	}
	if h := get("/v1/pod-uids/pod-uid"); h.Get("Cache-Control") == "" {
		t.Fatal("pod-uids lost its cache headers")
	}
}
