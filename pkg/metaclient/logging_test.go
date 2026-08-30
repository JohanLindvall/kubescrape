package metaclient

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

// syncBuf is written by the client on the caller's goroutine, but the cache
// summary test drives concurrent lookups.
type syncBuf struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (s *syncBuf) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.Write(p)
}

func (s *syncBuf) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.String()
}

func captureAt(level slog.Level) (*slog.Logger, *syncBuf) {
	buf := &syncBuf{}
	return slog.New(slog.NewTextHandler(buf, &slog.HandlerOptions{Level: level})), buf
}

// An unexpected status is a deployment fault the CALLER cannot diagnose: the
// agent paths that use this client hardest classify a failure into a counter
// and move on, so a wrong -metadata-endpoint, an unready replica or a 401 on
// /v1/scrape-auth produced a rising error counter and no line at all.
func TestUnexpectedStatusIsWarnedWithTheURL(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "upstream connect error", http.StatusServiceUnavailable)
	}))
	defer srv.Close()
	log, buf := captureAt(slog.LevelInfo)
	c := New(Config{Base: srv.URL, Timeout: time.Second, Log: log})

	_, err := c.PodByName(context.Background(), "ns1", "pod1")
	if err == nil {
		t.Fatal("want an error")
	}
	out := buf.String()
	if !strings.Contains(out, "level=WARN") || !strings.Contains(out, "status=503") {
		t.Errorf("want a WARN carrying the status:\n%s", out)
	}
	if !strings.Contains(out, "/v1/pods/ns1/pod1") {
		t.Errorf("want the URL on the line, or the operator cannot tell which endpoint answered:\n%s", out)
	}
	// ...and the typed error carries it too, for wherever a caller logs it.
	var se *StatusError
	if !errors.As(err, &se) || se.URL == "" {
		t.Fatalf("StatusError = %+v; the URL is what makes it diagnosable", se)
	}
	if !strings.Contains(se.Error(), "/v1/pods/ns1/pod1") {
		t.Errorf("Error() = %q, want the URL in the rendered message", se.Error())
	}
}

// A 404 is the metadata service's NORMAL answer (a container the kubelet has
// not posted yet, an IP that is not a pod's, a hostNetwork caller asking
// /v1/self) and must never produce a line: it happens per object per push.
func TestNotFoundIsNotLogged(t *testing.T) {
	srv := httptest.NewServer(http.NotFoundHandler())
	defer srv.Close()
	log, buf := captureAt(slog.LevelDebug)
	c := New(Config{Base: srv.URL, Timeout: time.Second, Log: log})

	for range 3 {
		if _, err := c.PodByIP(context.Background(), "10.0.0.5"); !IsNotFound(err) {
			t.Fatalf("want a 404, got %v", err)
		}
	}
	if out := buf.String(); strings.Contains(out, "level=WARN") {
		t.Errorf("a 404 must not warn — it is the expected answer:\n%s", out)
	}
}

// A repeatable condition must not become a flood: the client is called once
// per object per push on the concurrent ingest path.
func TestStatusWarningIsThrottled(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()
	log, buf := captureAt(slog.LevelInfo)
	c := New(Config{Base: srv.URL, Timeout: time.Second, Log: log})

	for range 25 {
		_, _ = c.PodByName(context.Background(), "ns1", "pod1")
	}
	if n := strings.Count(buf.String(), "level=WARN"); n != 1 {
		t.Errorf("WARN lines = %d for 25 failed lookups, want 1:\n%s", n, buf.String())
	}
}

// An undecodable 200 means this endpoint is not a kubescrape metadata service.
// The typed *DecodeError already named the URL, but only to whoever prints it.
func TestUndecodableBodyIsWarned(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		_, _ = w.Write([]byte("<html><body>404 not found (nginx)</body></html>"))
	}))
	defer srv.Close()
	log, buf := captureAt(slog.LevelInfo)
	c := New(Config{Base: srv.URL, Timeout: time.Second, Log: log})

	if _, err := c.Node(context.Background(), "node1"); err == nil {
		t.Fatal("want a decode error")
	}
	out := buf.String()
	for _, want := range []string{"level=WARN", "/v1/nodes/node1/metadata", "truncated=false", "bytes="} {
		if !strings.Contains(out, want) {
			t.Errorf("want %q on the decode warning:\n%s", want, out)
		}
	}
}

// The cache question an operator asks — "is the ETag path working, or is every
// lookup a full body?" — is a RATIO, so it is a periodic aggregate and never a
// line per request. At Info it costs nothing and prints nothing.
func TestCacheSummaryIsAggregatedAndDebugOnly(t *testing.T) {
	srv, _ := cachingServer(t, `"v1"`, `{"name":"pod1","namespace":"ns1"}`)

	log, buf := captureAt(slog.LevelDebug)
	c := New(Config{Base: srv.URL, Timeout: time.Second, Log: log})
	for range 20 {
		if _, err := c.PodByName(context.Background(), "ns1", "pod1"); err != nil {
			t.Fatal(err)
		}
	}
	out := buf.String()
	if n := strings.Count(out, "metadata cache"); n != 1 {
		t.Fatalf("cache summary lines = %d for 20 lookups, want exactly 1:\n%s", n, out)
	}
	if !strings.Contains(out, "hits=") || !strings.Contains(out, "entries=") {
		t.Errorf("the summary must carry the tallies and the size:\n%s", out)
	}

	quiet, qbuf := captureAt(slog.LevelInfo)
	qc := New(Config{Base: srv.URL, Timeout: time.Second, Log: quiet})
	for range 20 {
		_, _ = qc.PodByName(context.Background(), "ns1", "pod1")
	}
	if qbuf.String() != "" {
		t.Errorf("nothing is logged above Debug on a healthy client:\n%s", qbuf.String())
	}
}

// A first live run is exactly when a leaked credential reaches a log
// aggregator forever. /v1/scrape-auth is the one authenticated route AND the
// one that returns Secret VALUES, so both directions are asserted.
func TestScrapeAuthFailureLeaksNeitherTokenNorSecret(t *testing.T) {
	const token = "super-secret-bearer-token"
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		// A body carrying a secret VALUE, answered with a status that warns.
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"value":"the-secret-material"}`))
	}))
	defer srv.Close()
	log, buf := captureAt(slog.LevelDebug)
	c := New(Config{Base: srv.URL, Timeout: time.Second, Log: log,
		ScrapeAuthToken: func() string { return token }})

	if _, err := c.ScrapeAuth(context.Background(), "ns/name/key"); err == nil {
		t.Fatal("want an error")
	}
	out := buf.String()
	if !strings.Contains(out, "level=WARN") {
		t.Fatalf("the failure must still be visible:\n%s", out)
	}
	if strings.Contains(out, token) {
		t.Fatalf("THE BEARER TOKEN LEAKED INTO THE LOG:\n%s", out)
	}
	if strings.Contains(out, "the-secret-material") {
		t.Fatalf("THE RESPONSE BODY LEAKED INTO THE LOG:\n%s", out)
	}
	// The ref itself is fine and is what makes the line actionable.
	if !strings.Contains(out, "ns/name/key") {
		t.Errorf("the warning should name the ref it was resolving:\n%s", out)
	}
}

// The hard cap's ARBITRARY trim has a cost nothing else reports: an entry
// evicted this way takes its ETag with it, so that URL's next lookup is a full
// 200 instead of a 304. On a node whose live URL count sits above the cap that
// is permanent, and the only other symptom is load on the singleton service.
func TestHardCapEvictionIsWarned(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Cache-Control", "max-age=3600")
		w.Header().Set("ETag", `"x"`)
		_, _ = w.Write([]byte(`{"name":"p","namespace":"ns","uid":"u"}`))
	}))
	defer srv.Close()
	log, buf := captureAt(slog.LevelInfo)
	c := New(Config{Base: srv.URL, Timeout: time.Second, Log: log})

	for i := range maxCacheEntries + 100 {
		if _, err := c.PodByUID(context.Background(), fmt.Sprintf("uid-%d", i)); err != nil {
			t.Fatal(err)
		}
	}
	out := buf.String()
	if !strings.Contains(out, "hard cap") {
		t.Fatalf("want a WARN when the cap starts evicting live entries:\n%s", out)
	}
	if !strings.Contains(out, "re-fetches a full body") {
		t.Errorf("the line must name the consequence, which is the part an operator acts on:\n%s", out)
	}
	// Throttled: past the cap this happens on roughly every insert.
	if n := strings.Count(out, "hard cap"); n != 1 {
		t.Errorf("eviction warnings = %d, want 1:\n%s", n, out)
	}
}
