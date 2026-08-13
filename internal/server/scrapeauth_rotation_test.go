package server

// The rotation of the shared /v1/scrape-auth token, end to end over the route
// an agent actually calls — not just over the accept set.
//
// TestScrapeAuthAcceptsRotationSet pins the half that was always there (the
// PREVIOUS token keeps working while a client catches up). This pins the other
// half: the agent and the service read the SAME Secret through two
// independently-cadenced readers, so about half the time it is the AGENT that
// re-reads first and presents a token the service has not read yet. There is no
// grace window for that direction — the service cannot accept a value it has
// never seen — so it was a hard 401 until the service's own re-read came round,
// up to a full minute later, on every agent whose copy led.

import (
	"log/slog"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"github.com/JohanLindvall/kubescrape/internal/bearer"
	"github.com/JohanLindvall/kubescrape/internal/servicemonitors"
	"github.com/JohanLindvall/kubescrape/internal/services"
	"github.com/JohanLindvall/kubescrape/internal/store"
)

// testClock is mutex-guarded because the handler reads it on the server's
// goroutine while the test advances it.
type testClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *testClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *testClock) advance(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

func TestScrapeAuthAcceptsATokenTheAgentRotatedToFirst(t *testing.T) {
	path := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(path, []byte("old-token"), 0o600); err != nil {
		t.Fatal(err)
	}
	clk := &testClock{t: time.Date(2026, 8, 12, 9, 0, 0, 0, time.UTC)}
	rt, err := bearer.NewRotating(path, slog.New(slog.DiscardHandler), bearer.WithClock(clk.now))
	if err != nil {
		t.Fatal(err)
	}

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
		Store:            store.New(time.Minute),
		Services:         services.NewIndex(),
		Monitors:         monitors,
		Resolver:         stubResolver{},
		MaxWait:          500 * time.Millisecond,
		Ready:            closedChan(),
		Secrets:          stubSecrets{value: "the-scrape-token"},
		ScrapeAuthTokens: rt.Tokens,
	}).Handler())
	defer srv.Close()
	url := srv.URL + "/v1/scrape-auth/monitoring/tok/token"

	if code, body := get(t, url, "Bearer old-token"); code != 200 {
		t.Fatalf("before the rotation: %d %s", code, body)
	}

	// A minute in, the service's own ticker re-reads (nothing has changed) —
	// which is what puts its cadence AHEAD of the agent's inside the minute.
	clk.advance(time.Minute)
	rt.Tokens()

	// The Secret is patched, and two seconds later an agent — whose own
	// per-minute re-read happened to fall in between — presents the new token.
	clk.advance(time.Second)
	if err := os.WriteFile(path, []byte("new-token"), 0o600); err != nil {
		t.Fatal(err)
	}
	clk.advance(time.Second)

	if code, body := get(t, url, "Bearer new-token"); code != 200 {
		t.Fatalf("GET /v1/scrape-auth with the rotated token = %d %q; an agent that re-read the "+
			"shared Secret before the service did must not be 401'd for it", code, body)
	}
	// The agents that have NOT re-read yet keep working for the grace window:
	// the two directions are covered by two different mechanisms and both have
	// to hold at once.
	if code, body := get(t, url, "Bearer old-token"); code != 200 {
		t.Fatalf("GET /v1/scrape-auth with the predecessor = %d %q; the grace window covers every "+
			"agent that has not re-read yet", code, body)
	}
	if code, _ := get(t, url, "Bearer neither-token"); code != 401 {
		t.Fatalf("an unrelated token = %d, want 401", code)
	}
}
