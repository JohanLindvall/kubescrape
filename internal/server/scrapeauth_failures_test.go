package server

// GET /v1/scrape-auth is the only route that hard-fails on EXTERNAL state (an
// API-server read this process does not control), and every cause used to
// answer 404. These pin the classification: what the CLIENT got wrong is a
// 404, what the CLUSTER got wrong is a retryable 502, and a value that cannot
// survive JSON is refused rather than silently mangled.

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/JohanLindvall/kubescrape/internal/servicemonitors"
	"github.com/JohanLindvall/kubescrape/internal/services"
	"github.com/JohanLindvall/kubescrape/internal/store"
)

// erroringSecrets returns a fixed outcome for every read.
type erroringSecrets struct {
	value string
	err   error
}

func (s erroringSecrets) Get(_ context.Context, _, _, _ string) (string, error) {
	return s.value, s.err
}

func secretsServer(t *testing.T, sec SecretReader) *httptest.Server {
	t.Helper()
	monitors := servicemonitors.NewIndex()
	if err := monitors.Upsert(&unstructured.Unstructured{Object: map[string]any{
		"metadata": map[string]any{"namespace": "monitoring", "name": "web"},
		"spec": map[string]any{
			"selector": map[string]any{},
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
		Secrets:         sec,
		ScrapeAuthToken: testScrapeToken,
	}).Handler())
	t.Cleanup(srv.Close)
	return srv
}

// authGet issues an authenticated GET and returns the whole response.
func authGet(t *testing.T, url string) *http.Response {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+testScrapeToken)
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = res.Body.Close() })
	return res
}

func TestScrapeAuthClassifiesReadFailures(t *testing.T) {
	forbidden := apierrors.NewForbidden(
		schema.GroupResource{Resource: "secrets"}, "tok",
		errors.New("secrets is forbidden: User cannot get resource"))

	for _, tc := range []struct {
		name string
		err  error
		want int
	}{
		// The client named a key that does not exist: its 404.
		{"missing key", fmt.Errorf("%w: %q", ErrSecretKeyNotFound, "token"), http.StatusNotFound},
		// The Secret itself is gone: also the client's 404.
		{"missing secret", apierrors.NewNotFound(schema.GroupResource{Resource: "secrets"}, "tok"), http.StatusNotFound},
		// RBAC was never granted. This is the likeliest real failure — the
		// grant is added by hand — and the one that must NOT read as
		// "no such secret", nor land in metadata_requests_total's not_found.
		{"forbidden", forbidden, http.StatusBadGateway},
		// The API server could not be reached at all.
		{"timeout", apierrors.NewTimeoutError("etcd", 1), http.StatusBadGateway},
		{"opaque", errors.New("connection refused"), http.StatusBadGateway},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv := secretsServer(t, erroringSecrets{err: tc.err})
			res := authGet(t, srv.URL+"/v1/scrape-auth/monitoring/tok/token")
			if res.StatusCode != tc.want {
				t.Fatalf("status = %d, want %d", res.StatusCode, tc.want)
			}
			// A cluster-caused failure must advertise itself as retryable, or
			// a conformant client treats the response as final and gives up on
			// a credential that will exist a minute from now.
			ra := res.Header.Get("Retry-After")
			if tc.want == http.StatusBadGateway && ra == "" {
				t.Error("retryable failure carries no Retry-After")
			}
			if tc.want == http.StatusNotFound && ra != "" {
				t.Errorf("client-caused 404 carries Retry-After %q", ra)
			}
		})
	}
}

// Every exit of this handler must be uncacheable: a 404 is one of the statuses
// a cache may store and heuristically freshen, and a stored "secret not found"
// outlives the RBAC fix or the created key that resolved it — visible only as
// a permanently failing scrape.
func TestScrapeAuthNeverCacheable(t *testing.T) {
	keyMissing := fmt.Errorf("%w: %q", ErrSecretKeyNotFound, "token")
	for _, tc := range []struct {
		name string
		sec  SecretReader
		path string
		want int
	}{
		{"ok", erroringSecrets{value: "tok"}, "/v1/scrape-auth/monitoring/tok/token", http.StatusOK},
		{"not allowlisted", erroringSecrets{value: "tok"}, "/v1/scrape-auth/other/nope/key", http.StatusForbidden},
		{"missing key", erroringSecrets{err: keyMissing}, "/v1/scrape-auth/monitoring/tok/token", http.StatusNotFound},
		{"upstream", erroringSecrets{err: errors.New("boom")}, "/v1/scrape-auth/monitoring/tok/token", http.StatusBadGateway},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv := secretsServer(t, tc.sec)
			res := authGet(t, srv.URL+tc.path)
			if res.StatusCode != tc.want {
				t.Fatalf("status = %d, want %d", res.StatusCode, tc.want)
			}
			if cc := res.Header.Get("Cache-Control"); cc != "no-store" {
				t.Errorf("Cache-Control = %q, want no-store", cc)
			}
		})
	}
}

// encoding/json replaces invalid UTF-8 with U+FFFD and reports no error, so a
// credential built from raw bytes would reach the agent CORRUPTED behind a
// 200. Refuse it instead: a loud failure beats a credential that is wrong in a
// way only the far end can see.
func TestScrapeAuthRefusesNonUTF8Values(t *testing.T) {
	srv := secretsServer(t, erroringSecrets{value: string([]byte{0x01, 0xff, 0xfe, 0x41, 0x80})})
	res := authGet(t, srv.URL+"/v1/scrape-auth/monitoring/tok/token")
	if res.StatusCode != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want 422", res.StatusCode)
	}
}

// The Secret shapes that must keep working unchanged: ordinary credentials,
// including the high-bit-free base64 and PEM material real monitors use.
func TestScrapeAuthServesValidValues(t *testing.T) {
	const token = "eyJhbGciOi.J9-token_value"
	srv := secretsServer(t, erroringSecrets{value: token})
	res := authGet(t, srv.URL+"/v1/scrape-auth/monitoring/tok/token")
	if res.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", res.StatusCode)
	}
	buf := make([]byte, 1024)
	n, _ := res.Body.Read(buf)
	if body := string(buf[:n]); !strings.Contains(body, token) {
		t.Errorf("body %q does not carry the token", body)
	}
}
