package promscrape

// The two bounded per-target caches evict silently by design: nothing is lost,
// the entry is rebuilt. That is precisely why the eviction has to be SAID —
// past the cap the symptom is a node paying a fresh TCP+TLS handshake, or a
// fresh regex compilation, per target per cycle, with every counter in this
// package flat.

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/JohanLindvall/kubescrape/pkg/kubemeta"
	"github.com/JohanLindvall/kubescrape/pkg/metaclient"
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
		if _, err := s.clientFor(context.Background(), tgt); err != nil {
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
	for i := range maxRelabelChains {
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
// all, leaving the whole map resident for the process lifetime. An entry nothing
// asks for is never SERVED again, so this is hygiene: the material must not
// outlive its use.
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
	// other — ever again; only the cycle still runs. (The USE stamp is what
	// releases it: a value still being asked for is retained past its
	// freshness as authToken's fallback, one nothing asks for is not.)
	ageAuthEntry(s, "ns/creds/tls.key", authIdleRelease, authIdleRelease)

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
// distinct materials had been seen. Idle retirement then ran only inside
// clientFor's post-miss insert — the one path the ORDINARY rotation never takes
// again: after v1 -> v2 every cycle hits v2, nothing new is inserted, and v1
// stayed until some other distinct material arrived (typically the next
// rotation, weeks later). The cycle sweeps now. Retirement is by LAST USE, so a
// client scraped every cycle is never rebuilt on a timer.
func TestSupersededTLSClientsAreRetiredBeforeTheCap(t *testing.T) {
	auth := &mapAuth{vals: map[string]string{"ns/tls/ca.crt": otherCAPEM}}
	s := New(Config{
		Node: "n1", Interval: time.Hour, Timeout: time.Second,
		DisableTargets: true, Exporter: &captureExporter{}, StartTime: time.Now(),
		Auth: auth,
	})
	tgt := testTarget("https://x/metrics")
	tgt.TLSCA = "ns/tls/ca.crt"
	v1, err := s.clientFor(context.Background(), tgt)
	if err != nil {
		t.Fatal(err)
	}
	// The rotation: the same target, new material.
	auth.vals["ns/tls/ca.crt"] = rotatedCAPEM
	s.authMu.Lock()
	clear(s.authCache) // past the resolved-secret cache's freshness
	s.authMu.Unlock()
	v2, err := s.clientFor(context.Background(), tgt)
	if err != nil {
		t.Fatal(err)
	}
	if v1 == v2 {
		t.Fatal("the rotated material reused the old client")
	}

	// Time passes; only the rotated material is ever scraped again, and no
	// third material arrives.
	s.tlsMu.Lock()
	for k, e := range s.tlsClients {
		e.used = e.used.Add(-2 * tlsClientIdleTTL)
		s.tlsClients[k] = e
	}
	s.tlsMu.Unlock()
	if again, err := s.clientFor(context.Background(), tgt); err != nil || again != v2 {
		t.Fatalf("the still-used client was rebuilt (err %v): retirement must be by LAST USE, not by age", err)
	}
	s.cycle(context.Background())

	s.tlsMu.Lock()
	defer s.tlsMu.Unlock()
	if got := len(s.tlsClients); got != 1 {
		t.Fatalf("cache holds %d clients after a cycle, want 1 — the superseded client (and its key material) must be retired without waiting for a new insert", got)
	}
	for _, e := range s.tlsClients {
		if e.client != v2 {
			t.Error("the cycle retired the client still in use and kept the superseded one")
		}
	}
}

// A resolved secret whose refresh fails WITHOUT a verdict — the metadata
// service unreachable, or answering 502 because the API server is — keeps
// serving the value it last resolved: the credential is still valid, and
// failing every secret-ref target with reason=auth a minute into a control-plane
// outage was the alternative. internal/bearer's rule for a file-backed token.
func TestAuthTokenServesLastGoodOnUpstreamFailure(t *testing.T) {
	var buf strings.Builder
	auth := &switchAuth{val: "tok-1"}
	s := debugScraper(Config{Node: "n1", Interval: time.Hour, Timeout: time.Second, Auth: auth}, &buf)
	if got, err := s.authToken(context.Background(), "ns/creds/token"); err != nil || got != "tok-1" {
		t.Fatalf("authToken = %q, %v", got, err)
	}
	ageAuthEntry(s, "ns/creds/token", 2*authCacheTTL, 0) // stale, but still in use

	auth.set("", &metaclient.StatusError{Code: http.StatusBadGateway, Body: "upstream"})
	got, err := s.authToken(context.Background(), "ns/creds/token")
	if err != nil || got != "tok-1" {
		t.Fatalf("authToken = %q, %v; want the retained value while the service cannot answer", got, err)
	}
	auth.set("", errors.New("dial tcp: connection refused"))
	if got, err := s.authToken(context.Background(), "ns/creds/token"); err != nil || got != "tok-1" {
		t.Fatalf("authToken = %q, %v; a transport failure is no verdict either", got, err)
	}
	if n := strings.Count(buf.String(), "could not refresh a scrape credential"); n != 1 {
		t.Errorf("warned %d times, want 1 (throttled):\n%s", n, buf.String())
	}
	// The grace is bounded: past it the value is gone whatever the service says.
	ageAuthEntry(s, "ns/creds/token", authStaleGrace, 0)
	if _, err := s.authToken(context.Background(), "ns/creds/token"); err == nil {
		t.Error("a value fetched longer ago than authStaleGrace was still served")
	}
}

// A refresh the service ANSWERS with a refusal is a verdict: the secret was
// deleted, the monitor stopped referencing it, this agent's token was refused.
// Revocation must take effect at once, not after the grace.
func TestAuthTokenDropsOnDefinitiveRefusal(t *testing.T) {
	for _, code := range []int{http.StatusUnauthorized, http.StatusForbidden, http.StatusNotFound} {
		auth := &switchAuth{val: "tok-1"}
		s := New(Config{Node: "n1", Interval: time.Hour, Timeout: time.Second, Auth: auth})
		if _, err := s.authToken(context.Background(), "ns/creds/token"); err != nil {
			t.Fatal(err)
		}
		ageAuthEntry(s, "ns/creds/token", 2*authCacheTTL, 0)
		auth.set("", &metaclient.StatusError{Code: code})
		if got, err := s.authToken(context.Background(), "ns/creds/token"); err == nil {
			t.Errorf("status %d: served %q after a definitive refusal", code, got)
		}
		s.authMu.Lock()
		_, held := s.authCache["ns/creds/token"]
		s.authMu.Unlock()
		if held {
			t.Errorf("status %d: the refused value is still cached", code)
		}
	}
}

// The fallback must survive BETWEEN two scrapes of a target on a one-minute
// cadence — a monitor's `interval: 1m`, the commonest explicit one. Such a ref
// is asked for again ~60s after its last use, a little late or early with the
// tick, and the cycle's sweep runs before the scrape asks; an idle release of
// exactly authCacheTTL raced that scrape and discarded the retained value about
// half the time, so the grace silently did not apply to those targets.
func TestAuthFallbackSurvivesAOneMinuteCadence(t *testing.T) {
	var buf strings.Builder
	auth := &switchAuth{val: "tok-1"}
	s := debugScraper(Config{
		Node: "n1", Interval: time.Hour, Timeout: time.Second,
		DisableTargets: true, Exporter: &captureExporter{}, StartTime: time.Now(),
		Auth: auth,
	}, &buf)
	if _, err := s.authToken(context.Background(), "ns/creds/token"); err != nil {
		t.Fatal(err)
	}
	// One interval later, and a few seconds late: the next cycle sweeps first.
	ageAuthEntry(s, "ns/creds/token", 65*time.Second, 65*time.Second)
	s.cycle(context.Background())

	auth.set("", &metaclient.StatusError{Code: http.StatusBadGateway, Body: "upstream"})
	if got, err := s.authToken(context.Background(), "ns/creds/token"); err != nil || got != "tok-1" {
		t.Fatalf("authToken = %q, %v; a ref asked for once a minute lost its fallback to the idle sweep", got, err)
	}
}

// A refresh cut short by the SCRAPE's own context (shutdown, a spent budget)
// fails the scrape rather than serving the retained value: the value could not
// be used, and the stale-credential warning would blame the metadata service
// for a cancellation.
func TestAuthTokenDoesNotBridgeACancelledScrape(t *testing.T) {
	var buf strings.Builder
	auth := &switchAuth{val: "tok-1"}
	s := debugScraper(Config{Node: "n1", Interval: time.Hour, Timeout: time.Second, Auth: auth}, &buf)
	if _, err := s.authToken(context.Background(), "ns/creds/token"); err != nil {
		t.Fatal(err)
	}
	ageAuthEntry(s, "ns/creds/token", 2*authCacheTTL, 0)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	auth.set("", context.Canceled)
	if _, err := s.authToken(ctx, "ns/creds/token"); err == nil {
		t.Error("a cancelled scrape was handed the retained credential")
	}
	if strings.Contains(buf.String(), "could not refresh a scrape credential") {
		t.Errorf("a cancellation was reported as a metadata-service failure:\n%s", buf.String())
	}
}

// switchAuth resolves every ref to one value, or fails with one error, as set.
type switchAuth struct {
	mu  sync.Mutex
	val string
	err error
}

func (a *switchAuth) set(val string, err error) {
	a.mu.Lock()
	a.val, a.err = val, err
	a.mu.Unlock()
}

func (a *switchAuth) ScrapeAuth(context.Context, string) (string, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.val, a.err
}

// ageAuthEntry moves a resolved-secret entry's fetch and use stamps into the
// past by the given amounts.
func ageAuthEntry(s *Scraper, ref string, fetched, used time.Duration) {
	s.authMu.Lock()
	defer s.authMu.Unlock()
	e := s.authCache[ref]
	e.fetched = e.fetched.Add(-fetched)
	e.used = e.used.Add(-used)
	s.authCache[ref] = e
}
