package promscrape

// The two bounded per-target caches evict silently by design: nothing is lost,
// the entry is rebuilt. That is precisely why the eviction has to be SAID —
// past the cap the symptom is a node paying a fresh TCP+TLS handshake, or a
// fresh regex compilation, per target per cycle, with every counter in this
// package flat.

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/JohanLindvall/kubescrape/pkg/kubemeta"
)

// lockProbeHandler renders through inner and, on every record, reports whether
// the mutex a caller must NOT be holding was in fact free. TryLock is exact
// here: the handler runs on the very goroutine that would be holding it, so a
// failed TryLock means that goroutine still owns the lock.
type lockProbeHandler struct {
	slog.Handler
	mu   *sync.Mutex
	held *bool
}

func (h lockProbeHandler) Handle(ctx context.Context, r slog.Record) error {
	if h.mu.TryLock() {
		h.mu.Unlock()
	} else {
		*h.held = true
	}
	return h.Handler.Handle(ctx, r)
}

// Reporting an eviction renders and writes a slog record and closes the
// victim's idle connections — I/O, done while every scrape goroutine on the
// node is waiting to look a client up. Both belong outside the critical
// section.
func TestTLSCacheEvictionIsReportedOutsideTheLock(t *testing.T) {
	var buf strings.Builder
	s := New(Config{Node: "n1", Interval: time.Hour, Timeout: time.Second})
	held := false
	s.log = slog.New(lockProbeHandler{
		Handler: slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}),
		mu:      &s.tlsMu, held: &held,
	})

	// Fill the cache to its cap. TLSServerName alone needs a per-target client
	// and needs no secret material, so each iteration mints a distinct key.
	for i := 0; i <= maxTLSClients; i++ {
		tgt := testTarget("https://x/metrics")
		tgt.TLSServerName = fmt.Sprintf("host-%d.example", i)
		if _, err := s.clientFor(context.Background(), tgt, time.Second); err != nil {
			t.Fatal(err)
		}
	}
	if held {
		t.Error("the eviction report ran while tlsMu was held: every scrape goroutine waits behind a slog write")
	}
	if got := len(s.tlsClients); got > maxTLSClients {
		t.Errorf("cache holds %d clients, want <= %d", got, maxTLSClients)
	}
	out := buf.String()
	for _, want := range []string{"bounded scrape cache", "cache=\"per-target TLS clients\"", "entries=" + strconv.Itoa(maxTLSClients)} {
		if !strings.Contains(out, want) {
			t.Errorf("the TLS cache eviction was not reported (%q missing):\n%s", want, out)
		}
	}
}

// The compiled-relabel cache had the cap and the evict-one policy but no report
// at all: a controller minting a templated regex per monitor makes every scrape
// recompile its chain, and the fleet's only symptom is CPU.
func TestRelabelCacheEvictionIsReported(t *testing.T) {
	srv := serveBody(t, "m 1\n")
	var buf strings.Builder
	tgt := testTarget(srv.URL)
	tgt.MetricRelabelings = []kubemeta.RelabelRule{
		{Action: "drop", SourceLabels: []string{"__name__"}, Regex: "the_one_that_evicts_.*"},
	}
	exp := &captureExporter{}
	s := New(Config{
		Node: "n1", Interval: time.Hour, Timeout: 5 * time.Second,
		Targets: staticTargets{tgt}, Exporter: exp, StartTime: time.Now(),
	})
	s.log = slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	// Fill the cache to its cap with chains this target does not use, so its
	// own insert is the one that has to evict.
	for i := 0; i < maxRelabelChains; i++ {
		if _, _, err := s.relabels.session([]kubemeta.RelabelRule{
			{Action: "drop", SourceLabels: []string{"__name__"}, Regex: fmt.Sprintf("filler_%d_.*", i)},
		}); err != nil {
			t.Fatal(err)
		}
	}
	s.cycle(context.Background())
	if exp.points() != 1 {
		t.Fatalf("points = %d, want 1 (the scrape must still work — an eviction costs work, not data)", exp.points())
	}
	out := buf.String()
	for _, want := range []string{"bounded scrape cache", "cache=\"compiled metricRelabelings chains\"", "entries=" + strconv.Itoa(maxRelabelChains)} {
		if !strings.Contains(out, want) {
			t.Errorf("the relabel cache eviction was not reported (%q missing):\n%s", want, out)
		}
	}
}

// The two caches thrash for different reasons and want different remedies. A
// single keyless throttle would let whichever fired first silence the other for
// the whole 30-minute window — and the report that never arrives is the one
// nobody knows to look for.
func TestTheTwoCacheEvictionGatesAreIndependent(t *testing.T) {
	var buf strings.Builder
	s := New(Config{Node: "n1", Interval: time.Hour, Timeout: time.Second})
	s.log = slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	s.warnCacheEviction(&s.tlsEvictWarn, "per-target TLS clients", maxTLSClients, "note")
	s.warnCacheEviction(&s.relabelEvictWarn, "compiled metricRelabelings chains", maxRelabelChains, "note")
	if n := strings.Count(buf.String(), "bounded scrape cache"); n != 2 {
		t.Fatalf("got %d eviction lines for two independent caches, want 2:\n%s", n, buf.String())
	}
	// Each gate still throttles its OWN condition.
	s.warnCacheEviction(&s.tlsEvictWarn, "per-target TLS clients", maxTLSClients, "note")
	if n := strings.Count(buf.String(), "bounded scrape cache"); n != 2 {
		t.Errorf("a repeat of the same condition was not throttled, got %d lines", n)
	}
}

// The resolved-secret cache holds bearer tokens, CA bundles, client
// certificates and client PRIVATE KEYS. Its TTL sweep used to sit INSIDE a
// cache-miss insert, so it could not reach the entry that matters — the ref
// that stopped being fetched, i.e. the monitor that was deleted or the pod that
// moved off this node — and if EVERY ref went away the sweep stopped running at
// all, leaving the whole map resident for the process lifetime. Nothing was
// ever SERVED stale, so this is hygiene: the material must not outlive its use.
func TestResolvedSecretsAreReleasedWhenNothingFetchesThemAgain(t *testing.T) {
	s := New(Config{
		Node: "n1", Interval: time.Hour, Timeout: time.Second,
		Targets: staticTargets{}, Exporter: &captureExporter{}, StartTime: time.Now(),
		Auth: &mapAuth{vals: map[string]string{"ns/creds/tls.key": "-----BEGIN PRIVATE KEY-----"}},
	})
	if _, err := s.authToken(context.Background(), "ns/creds/tls.key"); err != nil {
		t.Fatal(err)
	}
	if len(s.authCache) != 1 {
		t.Fatalf("cache holds %d entries, want 1", len(s.authCache))
	}
	// Age the entry past its usefulness. Nothing asks for this ref — or any
	// other — ever again; only the cycle still runs.
	s.authMu.Lock()
	e := s.authCache["ns/creds/tls.key"]
	e.fetched = e.fetched.Add(-2 * time.Minute)
	s.authCache["ns/creds/tls.key"] = e
	s.authMu.Unlock()

	s.cycle(context.Background())
	if got := len(s.authCache); got != 0 {
		t.Fatalf("cache still holds %d entries after a cycle: aged-out secret material outlives every ref that could sweep it", got)
	}
}

// And the cache is BOUNDED, unlike its two siblings: expiry alone bounds
// nothing under ref churn, since every entry minted inside one TTL window
// survives the sweep.
func TestResolvedSecretCacheIsBounded(t *testing.T) {
	vals := map[string]string{}
	for i := range maxAuthCacheEntries * 2 {
		vals["ns/creds/key"+strconv.Itoa(i)] = "secret"
	}
	s := New(Config{
		Node: "n1", Interval: time.Hour, Timeout: time.Second,
		Targets: staticTargets{}, Exporter: &captureExporter{}, StartTime: time.Now(),
		Auth: &mapAuth{vals: vals},
	})
	for i := range maxAuthCacheEntries * 2 {
		if _, err := s.authToken(context.Background(), "ns/creds/key"+strconv.Itoa(i)); err != nil {
			t.Fatal(err)
		}
	}
	if got := len(s.authCache); got > maxAuthCacheEntries {
		t.Fatalf("cache holds %d entries, want <= %d", got, maxAuthCacheEntries)
	}
}

// The per-target TLS client cache had the size cap as its ONLY removal path, so
// a cert-manager rotation (the key includes the resolved PEM, so every rotation
// mints a new entry) left the PREVIOUS client's private key resident until 64
// distinct materials had been seen — the process lifetime on a node with a
// handful of TLS targets. Retirement is by LAST USE, so a client scraped every
// cycle is never rebuilt on a timer.
func TestSupersededTLSClientsAreRetiredBeforeTheCap(t *testing.T) {
	s := New(Config{Node: "n1", Interval: time.Hour, Timeout: time.Second})
	old := testTarget("https://x/metrics")
	old.TLSServerName = "rotated.example"
	if _, err := s.clientFor(context.Background(), old, time.Second); err != nil {
		t.Fatal(err)
	}
	hot := testTarget("https://y/metrics")
	hot.TLSServerName = "hot.example"
	hotClient, err := s.clientFor(context.Background(), hot, time.Second)
	if err != nil {
		t.Fatal(err)
	}

	// Time passes; only the hot target keeps being scraped.
	s.tlsMu.Lock()
	for k, e := range s.tlsClients {
		e.used = e.used.Add(-2 * tlsClientIdleTTL)
		s.tlsClients[k] = e
	}
	s.tlsMu.Unlock()
	if _, err := s.clientFor(context.Background(), hot, time.Second); err != nil {
		t.Fatal(err)
	}

	// A third, distinct material: its insert is what runs the retirement.
	third := testTarget("https://z/metrics")
	third.TLSServerName = "third.example"
	if _, err := s.clientFor(context.Background(), third, time.Second); err != nil {
		t.Fatal(err)
	}
	if got := len(s.tlsClients); got != 2 {
		t.Fatalf("cache holds %d clients, want 2 — the superseded one must be retired well below the %d cap", got, maxTLSClients)
	}
	again, err := s.clientFor(context.Background(), hot, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if again != hotClient {
		t.Error("the still-used client was rebuilt: retirement must be by LAST USE, not by age")
	}
}
