package server

// Tests for the bearer-token authentication guarding GET /v1/scrape-auth —
// the only route that serves Secret material (auth.go).

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"github.com/JohanLindvall/kubescrape/internal/servicemonitors"
	"github.com/JohanLindvall/kubescrape/internal/services"
	"github.com/JohanLindvall/kubescrape/internal/store"
)

// stubSecrets serves one fixed Secret value.
type stubSecrets struct{ value string }

func (s stubSecrets) Get(_ context.Context, _, _, _ string) (string, error) { return s.value, nil }

const testScrapeToken = "s3cr3t-shared-token"

// scrapeAuthServer builds a server with -scrape-auth-secrets enabled, one
// monitor referencing monitoring/tok/token, and the given expected token.
func scrapeAuthServer(t *testing.T, token string) *httptest.Server {
	t.Helper()
	monitors := servicemonitors.NewIndex()
	if err := monitors.Upsert(&unstructured.Unstructured{Object: map[string]any{
		"metadata": map[string]any{"namespace": "monitoring", "name": "web"},
		"spec": map[string]any{
			"selector": map[string]any{"matchLabels": map[string]any{"team": "obs"}},
			"endpoints": []any{map[string]any{
				"port":              "http-metrics",
				"bearerTokenSecret": map[string]any{"name": "tok", "key": "token"},
			}},
		},
	}}); err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(New(Config{
		Store:           store.New(time.Minute),
		Services:        services.NewIndex(),
		Monitors:        monitors,
		Resolver:        stubResolver{},
		MaxWait:         500 * time.Millisecond,
		Ready:           closedChan(),
		Secrets:         stubSecrets{value: "the-scrape-token"},
		ScrapeAuthToken: token,
	}).Handler())
	t.Cleanup(srv.Close)
	return srv
}

// get issues a GET with an optional Authorization header and returns the
// status and body.
func get(t *testing.T, url, authorization string) (int, string) {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	if authorization != "" {
		req.Header.Set("Authorization", authorization)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	buf := make([]byte, 4096)
	n, _ := resp.Body.Read(buf)
	return resp.StatusCode, string(buf[:n])
}

// Enabling -scrape-auth-secrets without a token file must STOP the process:
// an unauthenticated /v1/scrape-auth hands every Secret key a monitor names to
// anything that can reach the service.
func TestScrapeAuthSecretsRequireToken(t *testing.T) {
	if err := (Config{Secrets: stubSecrets{}}).Validate(); err == nil {
		t.Fatal("secrets enabled without a token must be a startup error")
	} else if !strings.Contains(err.Error(), "-scrape-auth-token-file") {
		t.Fatalf("error should name the missing flag: %v", err)
	}
	if err := (Config{Secrets: stubSecrets{}, ScrapeAuthToken: testScrapeToken}).Validate(); err != nil {
		t.Fatalf("secrets + token: %v", err)
	}
	if err := (Config{}).Validate(); err != nil {
		t.Fatalf("secrets disabled: %v", err)
	}
}

// The wire contract: `Authorization: Bearer <token>`. Anything else is a 401,
// and the response never carries the secret.
func TestScrapeAuthRequiresBearerToken(t *testing.T) {
	srv := scrapeAuthServer(t, testScrapeToken)
	url := srv.URL + "/v1/scrape-auth/monitoring/tok/token"

	for _, tc := range []struct{ name, header string }{
		{"absent", ""},
		{"empty bearer", "Bearer "},
		{"wrong token", "Bearer wrong-token"},
		{"token prefix", "Bearer " + testScrapeToken[:5]},
		{"token with suffix", "Bearer " + testScrapeToken + "x"},
		{"wrong scheme", "Basic " + testScrapeToken},
		{"raw token, no scheme", testScrapeToken},
	} {
		code, body := get(t, url, tc.header)
		if code != http.StatusUnauthorized {
			t.Errorf("%s: status %d, want 401", tc.name, code)
		}
		if strings.Contains(body, "the-scrape-token") {
			t.Errorf("%s: secret leaked in a rejected response: %s", tc.name, body)
		}
	}

	// The correct token gets the secret; the scheme is case-insensitive
	// (RFC 9110) and surrounding whitespace is tolerated.
	for _, header := range []string{
		"Bearer " + testScrapeToken,
		"bearer " + testScrapeToken,
		"BEARER  " + testScrapeToken + " ",
	} {
		code, body := get(t, url, header)
		if code != http.StatusOK {
			t.Fatalf("%q: status %d, want 200 (%s)", header, code, body)
		}
		if !strings.Contains(body, "the-scrape-token") {
			t.Fatalf("%q: body = %s", header, body)
		}
	}
}

// A Server built with Secrets but no token (an embedder bypassing Validate)
// must serve nothing rather than everything.
func TestScrapeAuthWithoutConfiguredTokenDeniesAll(t *testing.T) {
	srv := scrapeAuthServer(t, "")
	for _, header := range []string{"", "Bearer ", "Bearer anything"} {
		if code, _ := get(t, srv.URL+"/v1/scrape-auth/monitoring/tok/token", header); code != http.StatusUnauthorized {
			t.Fatalf("header %q: status %d, want 401", header, code)
		}
	}
}

// Authentication happens BEFORE the monitor-reference check, so an
// unauthenticated client cannot probe which secrets monitors reference
// (403 = referenced, 404 = not) — and a service without -scrape-auth-secrets
// keeps answering 404, which is what agents already distinguish.
func TestScrapeAuthOrderingAndDisabled(t *testing.T) {
	srv := scrapeAuthServer(t, testScrapeToken)
	// Unreferenced secret, no token: indistinguishable from a referenced one.
	if code, _ := get(t, srv.URL+"/v1/scrape-auth/other/nope/key", ""); code != http.StatusUnauthorized {
		t.Fatalf("unreferenced secret without token: status %d, want 401", code)
	}
	// With the token, the allowlist applies as before.
	if code, _ := get(t, srv.URL+"/v1/scrape-auth/other/nope/key", "Bearer "+testScrapeToken); code != http.StatusForbidden {
		t.Fatalf("unreferenced secret with token: status %d, want 403", code)
	}

	// Feature disabled entirely (Secrets nil): still a 404, not a 401.
	plain := testServer(t, store.New(time.Minute), closedChan())
	if code, _ := get(t, plain.URL+"/v1/scrape-auth/monitoring/tok/token", ""); code != http.StatusNotFound {
		t.Fatalf("disabled endpoint: status %d, want 404", code)
	}
}

// The other v1 routes stay OPEN: agents depend on them and they carry no
// secret material. A token requirement leaking onto them would break every
// agent's metadata lookups.
func TestOnlyScrapeAuthIsAuthenticated(t *testing.T) {
	st := store.New(time.Minute)
	addPod(st)
	srv := scrapeAuthServer(t, testScrapeToken)
	// Rebuild with a populated store so the metadata routes have data.
	srv.Close()
	monitors := servicemonitors.NewIndex()
	srv = httptest.NewServer(New(Config{
		Store: st, Services: services.NewIndex(), Monitors: monitors,
		Resolver: stubResolver{}, MaxWait: 500 * time.Millisecond, Ready: closedChan(),
		Secrets: stubSecrets{value: "x"}, ScrapeAuthToken: testScrapeToken,
	}).Handler())
	t.Cleanup(srv.Close)

	for _, path := range []string{
		"/v1/containers/cafe01",
		"/v1/pods/default/web-abc-xyz",
		"/v1/pod-uids/pod-uid",
		"/v1/pod-ips/10.1.2.3",
		"/v1/nodes/node1/targets",
		"/v1/nodes/node1/metadata",
		"/healthz",
		"/readyz",
	} {
		if code, _ := get(t, srv.URL+path, ""); code != http.StatusOK {
			t.Errorf("GET %s without a token = %d, want 200 (only /v1/scrape-auth is authenticated)", path, code)
		}
	}
}

// The token comparison must be constant time: the endpoint is reachable from
// every pod in the cluster, so a byte-at-a-time compare is a remotely timeable
// oracle for the shared token. Pinned by source inspection — a timing test
// would be flaky, and the property is a property of the code.
//
// The comparison itself now lives in internal/bearer, shared with the trace
// tier's internal listener (which had a byte-identical copy). This test follows
// it there: auth.go must delegate rather than open-code a compare, and the
// shared routine must still be the constant-time one.
func TestScrapeAuthUsesConstantTimeCompare(t *testing.T) {
	src, err := os.ReadFile("auth.go")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(src), "bearer.Authorized(") {
		t.Fatal("the scrape-auth handler must compare tokens through internal/bearer.Authorized")
	}
	if strings.Contains(string(src), "got == tok") {
		t.Fatal("string equality on a scrape-auth token leaks it to a timing attack")
	}
	shared, err := os.ReadFile("../bearer/bearer.go")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(shared), "subtle.ConstantTimeCompare([]byte(got), []byte(tok))") {
		t.Fatal("bearer.Authorized must compare with crypto/subtle.ConstantTimeCompare")
	}
	if strings.Contains(string(shared), "got == tok") {
		t.Fatal("string equality on a bearer token leaks it to a timing attack")
	}
}

// During a rotation grace window BOTH the current and previous token
// authorize; anything else still fails. This is what lets the token file
// rotate without agents and the service flipping in lockstep.
func TestScrapeAuthAcceptsRotationSet(t *testing.T) {
	tokens := []string{"new-token", "old-token"}
	s := New(Config{
		Secrets:          stubSecrets{},
		ScrapeAuthTokens: func() []string { return tokens },
	})
	req := func(tok string) bool {
		r := httptest.NewRequest("GET", "/v1/scrape-auth/ns/n/k", nil)
		if tok != "" {
			r.Header.Set("Authorization", "Bearer "+tok)
		}
		return s.authorizedForScrapeAuth(r)
	}
	if !req("new-token") || !req("old-token") {
		t.Fatal("both rotation-window tokens must authorize")
	}
	if req("wrong") || req("") {
		t.Fatal("non-window tokens must not authorize")
	}
	tokens = []string{"new-token"} // grace expired
	if req("old-token") {
		t.Fatal("the previous token must stop authorizing after the grace window")
	}
}
