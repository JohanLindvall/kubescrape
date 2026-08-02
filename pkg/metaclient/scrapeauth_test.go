package metaclient

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// The metadata endpoints are unauthenticated (they return object metadata the
// node already has); /v1/scrape-auth returns Secret VALUES and must carry the
// bearer token — and only it, so the token spreads no further than needed.
func TestScrapeAuthSendsBearerTokenOnlyThere(t *testing.T) {
	seen := map[string]string{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen[r.URL.Path] = r.Header.Get("Authorization")
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == "/v1/scrape-auth/ns/name/key" {
			_, _ = w.Write([]byte(`{"value":"s3cret"}`))
			return
		}
		_, _ = w.Write([]byte(`{"name":"p"}`))
	}))
	defer srv.Close()

	rotated := "tok-1"
	c := New(Config{Base: srv.URL, Timeout: 5 * time.Second,
		ScrapeAuthToken: func() string { return rotated }})

	if v, err := c.ScrapeAuth(context.Background(), "ns/name/key"); err != nil || v != "s3cret" {
		t.Fatalf("ScrapeAuth = %q, %v", v, err)
	}
	if got := seen["/v1/scrape-auth/ns/name/key"]; got != "Bearer tok-1" {
		t.Fatalf("Authorization = %q, want Bearer tok-1", got)
	}
	if _, err := c.PodByUID(context.Background(), "uid"); err != nil {
		t.Fatal(err)
	}
	if got := seen["/v1/pod-uids/uid"]; got != "" {
		t.Fatalf("metadata request carried Authorization %q — the token must not spread", got)
	}

	// The token is read per request, so a rotated file takes effect without a
	// restart.
	rotated = "tok-2"
	if _, err := c.ScrapeAuth(context.Background(), "ns/name/key"); err != nil {
		t.Fatal(err)
	}
	if got := seen["/v1/scrape-auth/ns/name/key"]; got != "Bearer tok-2" {
		t.Fatalf("Authorization after rotation = %q, want Bearer tok-2", got)
	}
}
