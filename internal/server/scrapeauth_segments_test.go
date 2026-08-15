package server

// The /v1/scrape-auth allowlist is a flat "namespace/name/key" join checked
// against three separately-chosen URL path segments. Go's ServeMux unescapes
// %2F INSIDE a single wildcard segment, so those three segments can be re-cut
// by a caller unless both ends refuse the ambiguity: servicemonitors'
// secretRef.ref declines to mint an entry carrying a separator (its own test),
// and the handler declines to match one.

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"github.com/JohanLindvall/kubescrape/internal/servicemonitors"
	"github.com/JohanLindvall/kubescrape/internal/services"
	"github.com/JohanLindvall/kubescrape/internal/store"
)

// recordingSecrets is the pluggable SecretReader the allowlist protects. The
// SHIPPED reader goes through client-go, which rejects "tenant/victim" as an
// invalid namespace before sending anything — but Secrets is an exported
// interface on Config, that guarantee lives in a downstream library, and a
// reader over a lister keyed by "ns/name" (the obvious optimisation for a
// per-scrape-cycle path) would perform the real cross-namespace read. So the
// assertion is on the ARGUMENTS this handler passes, not on what some reader
// happens to do with them.
type recordingSecrets struct {
	mu    sync.Mutex
	calls [][3]string
}

func (r *recordingSecrets) Get(_ context.Context, namespace, name, key string) (string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, [3]string{namespace, name, key})
	return "the-scrape-token", nil
}

func (r *recordingSecrets) seen() [][3]string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.calls
}

func TestScrapeAuthRefusesReCutPathSegments(t *testing.T) {
	// A monitor whose namespace itself contains the separator is how the
	// allowlist entry "tenant/victim/creds/token" comes to exist at all in this
	// test; in a cluster it would come from a bearerTokenSecret named
	// "victim/creds" in namespace "tenant", which is a string the CRD does not
	// validate and any tenant can write.
	monitors := servicemonitors.NewIndex()
	if err := monitors.Upsert(&unstructured.Unstructured{Object: map[string]any{
		"metadata": map[string]any{"namespace": "tenant/victim", "name": "sm"},
		"spec": map[string]any{
			"selector": map[string]any{},
			"endpoints": []any{map[string]any{
				"port":              "http",
				"bearerTokenSecret": map[string]any{"name": "creds", "key": "token"},
			}},
		},
	}}); err != nil {
		t.Fatal(err)
	}
	if !monitors.AuthSecretRefs().Has("tenant/victim/creds/token") {
		t.Fatalf("test setup: the allowlist entry under attack does not exist: %v", monitors.AuthSecretRefs())
	}

	secrets := &recordingSecrets{}
	srv := httptest.NewServer(New(Config{
		Store:           store.New(time.Minute),
		Services:        services.NewIndex(),
		Monitors:        monitors,
		Resolver:        stubResolver{},
		MaxWait:         500 * time.Millisecond,
		Ready:           closedChan(),
		Secrets:         secrets,
		ScrapeAuthToken: testScrapeToken,
	}).Handler())
	t.Cleanup(srv.Close)

	// The namespace segment carries the escaped separator, so the three
	// segments the CALLER chose ("tenant%2Fvictim", "creds", "token") join to
	// the same allowlist key as the three the MONITOR named — while the
	// arguments reaching the reader are a different secret entirely.
	status, body := get(t, srv.URL+"/v1/scrape-auth/tenant%2Fvictim/creds/token",
		"Bearer "+testScrapeToken)
	if status != http.StatusBadRequest {
		t.Errorf("status = %d, want 400: %s", status, body)
	}
	if calls := secrets.seen(); len(calls) > 0 {
		t.Errorf("the reader was asked for %v; a re-cut path must never reach it", calls)
	}

	// The other two segments are validated the same way, and so are the path
	// traversal names the same validation refuses.
	for _, path := range []string{
		"/v1/scrape-auth/tenant/cre%2Fds/token",
		"/v1/scrape-auth/tenant/creds/to%2Fken",
		"/v1/scrape-auth/../creds/token",
		"/v1/scrape-auth/%2e%2e/creds/token",
	} {
		status, body := get(t, srv.URL+path, "Bearer "+testScrapeToken)
		if status == http.StatusOK {
			t.Errorf("GET %s = 200: %s", path, body)
		}
	}
	if calls := secrets.seen(); len(calls) > 0 {
		t.Errorf("the reader was asked for %v", calls)
	}

	// An ordinary ref still resolves — the validation must not refuse the
	// legitimate shape.
	if err := monitors.Upsert(&unstructured.Unstructured{Object: map[string]any{
		"metadata": map[string]any{"namespace": "monitoring", "name": "ok"},
		"spec": map[string]any{
			"selector": map[string]any{},
			"endpoints": []any{map[string]any{
				"port":              "http",
				"bearerTokenSecret": map[string]any{"name": "tok", "key": "token"},
			}},
		},
	}}); err != nil {
		t.Fatal(err)
	}
	if status, body := get(t, srv.URL+"/v1/scrape-auth/monitoring/tok/token",
		"Bearer "+testScrapeToken); status != http.StatusOK {
		t.Fatalf("a legitimate ref returned %d: %s", status, body)
	}
	if calls := secrets.seen(); len(calls) != 1 || calls[0] != [3]string{"monitoring", "tok", "token"} {
		t.Errorf("reader calls = %v, want exactly the legitimate one", calls)
	}
}
