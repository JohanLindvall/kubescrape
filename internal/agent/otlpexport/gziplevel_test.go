package otlpexport

// The gzip level is a PER-DESTINATION config field, and New is called once per
// destination (per-signal x3 plus the default, plus one per route). Writing it
// into a process-global from inside New made the LAST client constructed set
// the level for every client in the process — inert only because no per-signal
// or per-route level exists yet to disagree.
//
// Now: the HTTP body path takes the level as a parameter (so it really is
// per-destination), and the gRPC codec — which grpc-go resolves by name from a
// process-global registry, with no per-call seam — REFUSES a second, differing
// level at construction instead of silently adopting it.

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/plog"
)

// withFreshGzipPin isolates a test from the process-wide gRPC codec pin (other
// tests in this package construct gRPC clients too).
func withFreshGzipPin(t *testing.T) {
	t.Helper()
	gzipPinMu.Lock()
	pinned, level := gzipPinned, gzipPinnedLevel
	gzipPinned, gzipPinnedLevel = false, 0
	gzipPinMu.Unlock()
	t.Cleanup(func() {
		gzipPinMu.Lock()
		gzipPinned, gzipPinnedLevel = pinned, level
		gzipPinMu.Unlock()
	})
}

func TestGRPCGzipLevelConflictRefusedAtConstruction(t *testing.T) {
	withFreshGzipPin(t)
	first, err := New(Config{Endpoint: "collector:4317", Protocol: "grpc", Insecure: true,
		Compression: "gzip", CompressionLevel: 1})
	if err != nil {
		t.Fatalf("first client: %v", err)
	}
	defer func() { _ = first.Close() }()

	// Same level: fine, this is every deployment today (CompressionLevel is a
	// TransportOnly field, so derived destinations inherit it).
	same, err := New(Config{Endpoint: "other:4317", Protocol: "grpc", Insecure: true,
		Compression: "gzip", CompressionLevel: 1})
	if err != nil {
		t.Fatalf("second client at the same level: %v", err)
	}
	defer func() { _ = same.Close() }()

	// A DIFFERENT level cannot be honoured for gRPC, so it must not be accepted.
	c, err := New(Config{Endpoint: "third:4317", Protocol: "grpc", Insecure: true,
		Compression: "gzip", CompressionLevel: 9})
	if err == nil {
		_ = c.Close()
		t.Fatal("a second gRPC destination asking for a different gzip level was accepted; the process-global codec would silently give it the first level")
	}
	if !strings.Contains(err.Error(), "9") || !strings.Contains(err.Error(), "1") {
		t.Errorf("the refusal must name both levels, got: %v", err)
	}
}

// A level set on one HTTP destination must not reach another. Constructed
// last-to-first on purpose: with a process-global set inside New, the level-1
// client's construction would have re-set the level for the level-9 one.
func TestHTTPGzipLevelIsPerDestination(t *testing.T) {
	sizes := map[string]int{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		sizes[strings.Split(strings.TrimPrefix(r.URL.Path, "/"), "/")[0]] = len(b)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	ld := plog.NewLogs()
	lrs := ld.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords()
	for i := 0; i < 400; i++ {
		lrs.AppendEmpty().Body().SetStr(strings.Repeat("compress me ", 20) + "unique-suffix")
	}

	newClient := func(who string, level int) *Client {
		t.Helper()
		c, err := New(Config{Endpoint: srv.URL + "/" + who, Protocol: "http",
			Compression: "gzip", CompressionLevel: level, Timeout: 5 * time.Second})
		if err != nil {
			t.Fatal(err)
		}
		return c
	}
	// The first path segment names the destination; the client appends /v1/logs.
	best := newClient("best", 9)
	fast := newClient("fast", 1)
	defer func() { _ = best.Close() }()
	defer func() { _ = fast.Close() }()

	for _, c := range []*Client{best, fast} {
		if err := c.ExportLogs(context.Background(), ld); err != nil {
			t.Fatal(err)
		}
	}
	if sizes["best"] == 0 || sizes["fast"] == 0 {
		t.Fatalf("both destinations must have received a body: %v", sizes)
	}
	if sizes["best"] >= sizes["fast"] {
		t.Errorf("level 9 produced %d bytes and level 1 produced %d: the level is not per-destination (a process-global would make them identical)",
			sizes["best"], sizes["fast"])
	}
}
