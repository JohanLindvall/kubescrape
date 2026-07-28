// Tests for the /metrics bridge: kubescrape_* served exactly when the OTLP
// push is off.
package obs

import (
	"io"
	"net/http/httptest"
	"strings"
	"testing"
)

func scrapeBody(t *testing.T, internal bool) string {
	t.Helper()
	srv := httptest.NewServer(RuntimeHandler(internal))
	defer srv.Close()
	resp, err := srv.Client().Get(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// RuntimeHandler(true) serves the kubescrape_* Registry metrics beside the
// runtime collectors; RuntimeHandler(false) — the push-enabled mode — keeps
// /metrics runtime-only so the same series never ship twice.
func TestRuntimeHandlerInternalToggle(t *testing.T) {
	HTTPRequests.WithLabelValues("/bridge-test", "200").Inc()

	with := scrapeBody(t, true)
	if !strings.Contains(with, "kubescrape_http_requests_total") {
		t.Fatalf("internal=true body lacks kubescrape_* metrics:\n%.400s", with)
	}
	if !strings.Contains(with, "go_goroutines") {
		t.Fatal("internal=true body lacks runtime metrics")
	}
	without := scrapeBody(t, false)
	if strings.Contains(without, "kubescrape_") {
		t.Fatal("internal=false body must stay runtime-only")
	}
}
