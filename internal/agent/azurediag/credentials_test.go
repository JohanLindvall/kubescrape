package azurediag

// What a REJECTED or unreachable credential costs.
//
// Two properties, and they pull in opposite directions. A consumer rebuild is
// the recovery path for a credential the broker refused, so it must present a
// FRESH one — the cached Entra token lives ~1h and re-presenting it makes the
// rebuild a no-op for the rest of that hour. A token ENDPOINT outage is the
// other shape: there is nothing fresh to be had, so each new SASL session must
// not spend its own 10s round trip discovering that, nor its own log line.

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// erroringSource fails every poll, which is what makes Run close the consumer
// and rebuild it — the arm whose log line promises freshly read credentials.
type erroringSource struct{ err error }

func (s *erroringSource) poll(context.Context) ([][]byte, bool, error) { return nil, false, s.err }
func (s *erroringSource) commit(context.Context) error                 { return nil }
func (s *erroringSource) close()                                       {}

// A rebuild must drop the cached credential BEFORE the new consumer is opened,
// or the fatal-fetch line's "rebuilt with freshly read credentials" is false on
// the managed-identity path: the new client re-presents the very token the
// broker rejected, and an operator who fixes the role assignment sees no
// recovery until the token expires on its own.
func TestRebuildingTheConsumerDropsTheCachedCredential(t *testing.T) {
	var mu sync.Mutex
	var order []string
	opened := make(chan struct{}, 8)

	r := New(Config{
		Logger:       slog.New(slog.DiscardHandler),
		RetryBackoff: time.Millisecond,
		Kafka: KafkaConfig{Invalidate: func() {
			mu.Lock()
			order = append(order, "invalidate")
			mu.Unlock()
		}},
	})
	r.open = func() (source, error) {
		mu.Lock()
		order = append(order, "open")
		mu.Unlock()
		opened <- struct{}{}
		return &erroringSource{err: errors.New("event hubs fetch: SASL authentication failed")}, nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { r.Run(ctx); close(done) }()
	for range 2 { // the first open, then the rebuild
		select {
		case <-opened:
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for the consumer to be rebuilt")
		}
	}
	cancel()
	<-done

	mu.Lock()
	got := strings.Join(order[:3], ",")
	mu.Unlock()
	if got != "open,invalidate,open" {
		t.Fatalf("rebuild sequence = %q, want the cached credential dropped between the two opens", got)
	}
}

// The hook exists exactly where there is a cache to drop. The connection-string
// mechanism re-reads its file per SASL session, so it is already fresh and must
// not pretend to hold state.
func TestResolveWiresCredentialInvalidationOnlyWhereACacheExists(t *testing.T) {
	mi := KafkaConfig{Namespace: "myns.servicebus.windows.net"}
	if err := mi.Resolve(slog.New(slog.DiscardHandler)); err != nil {
		t.Fatal(err)
	}
	if mi.Invalidate == nil {
		t.Error("the managed-identity path caches one token for ~1h and exposes no way to drop it")
	}

	path := filepath.Join(t.TempDir(), "cs")
	if err := os.WriteFile(path, []byte("Endpoint=sb://myns.servicebus.windows.net/;SharedAccessKeyName=r;SharedAccessKey=k"), 0o600); err != nil {
		t.Fatal(err)
	}
	cs := KafkaConfig{ConnectionStringFile: path}
	if err := cs.Resolve(slog.New(slog.DiscardHandler)); err != nil {
		t.Fatal(err)
	}
	if cs.Invalidate != nil {
		t.Error("the connection-string path re-reads per session: it has nothing to invalidate")
	}
	// nil is not a trap for the caller.
	cs.invalidateCredentials()
}

// Resolve must wire Invalidate to the SAME token cache its Mechanism serves
// from. A non-nil hook is not enough: one bound to a second source would drop
// a cache nothing reads, and the rebuild would re-present the rejected token
// for the rest of its life. Driven end to end through the workload-identity
// env the AKS webhook sets, against a token endpoint that counts fetches.
func TestResolvedInvalidateDropsTheTokenTheMechanismServes(t *testing.T) {
	var (
		mu      sync.Mutex
		fetches int
	)
	entra := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		mu.Lock()
		fetches++
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"access_token":"tok","expires_in":3600}`)
	}))
	defer entra.Close()
	tokenFile := filepath.Join(t.TempDir(), "federated-token")
	if err := os.WriteFile(tokenFile, []byte("assertion"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("AZURE_FEDERATED_TOKEN_FILE", tokenFile)
	t.Setenv("AZURE_TENANT_ID", "tenant")
	t.Setenv("AZURE_CLIENT_ID", "client")
	t.Setenv("AZURE_AUTHORITY_HOST", entra.URL)

	k := KafkaConfig{Namespace: "myns.servicebus.windows.net"}
	if err := k.Resolve(slog.New(slog.DiscardHandler)); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	authenticate := func() {
		t.Helper()
		if _, _, err := k.Mechanism.Authenticate(ctx, k.Brokers[0]); err != nil {
			t.Fatalf("authenticating: %v", err)
		}
	}
	count := func() int {
		mu.Lock()
		defer mu.Unlock()
		return fetches
	}
	authenticate()
	authenticate()
	if n := count(); n != 1 {
		t.Fatalf("token fetches after two sessions = %d, want 1 (cached)", n)
	}
	k.invalidateCredentials()
	authenticate()
	if n := count(); n != 2 {
		t.Fatalf("token fetches after Invalidate = %d, want 2: the hook dropped a cache the mechanism does not read", n)
	}
}

func TestInvalidateForcesAFreshTokenAndClearsTheBackoff(t *testing.T) {
	now := time.Unix(1000, 0)
	calls := 0
	fail := false
	ts := &tokenSource{
		log: slog.New(slog.DiscardHandler),
		now: func() time.Time { return now },
		fetch: func(context.Context) (string, time.Duration, error) {
			calls++
			if fail {
				return "", 0, errors.New("imds: HTTP 400: identity not found")
			}
			return "tok-" + strings.Repeat("x", calls), time.Hour, nil
		},
	}
	first, err := ts.get(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ts.get(context.Background()); err != nil || calls != 1 {
		t.Fatalf("get: %v, fetches = %d, want the token cached", err, calls)
	}
	ts.invalidate()
	second, err := ts.get(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if second == first || calls != 2 {
		t.Fatalf("after invalidate: token %q (was %q), fetches = %d — a rebuild re-presented the rejected credential", second, first, calls)
	}

	// A failed fetch arms the back-off; invalidate must clear it too, or the
	// rebuild it exists to serve waits out a window it caused.
	fail = true
	now = now.Add(time.Hour - time.Minute) // inside the refresh margin
	if _, err := ts.get(context.Background()); err != nil {
		t.Fatal("a stale-but-valid token is still served")
	}
	if calls != 3 {
		t.Fatalf("fetches = %d, want the refresh attempted", calls)
	}
	fail = false
	ts.invalidate()
	if _, err := ts.get(context.Background()); err != nil || calls != 4 {
		t.Fatalf("after invalidate under back-off: %v, fetches = %d, want an immediate re-fetch", err, calls)
	}
}

// A token endpoint that is down is answered from the negative cache: get runs
// per SASL session, under a mutex, around a 10s-timeout round trip, so without
// one every new Kafka connection serialises behind a fresh failing fetch.
func TestFailedTokenFetchBacksOffInsteadOfRetryingPerSession(t *testing.T) {
	now := time.Unix(1000, 0)
	calls := 0
	want := errors.New("imds: connection refused")
	ts := &tokenSource{
		log: slog.New(slog.DiscardHandler),
		now: func() time.Time { return now },
		fetch: func(context.Context) (string, time.Duration, error) {
			calls++
			return "", 0, want
		},
	}
	if _, err := ts.get(context.Background()); !errors.Is(err, want) {
		t.Fatalf("first get: %v, want the fetch error", err)
	}
	for range 20 {
		now = now.Add(time.Second)
		if _, err := ts.get(context.Background()); !errors.Is(err, want) {
			t.Fatalf("get inside the back-off: %v, want the cached cause", err)
		}
	}
	if calls != 1 {
		t.Fatalf("%d token round trips inside one back-off window: every SASL session is paying for the outage", calls)
	}
	now = now.Add(tokenRetryBackoff)
	if _, err := ts.get(context.Background()); !errors.Is(err, want) {
		t.Fatal(err)
	}
	if calls != 2 {
		t.Fatalf("fetches = %d after the back-off passed, want the endpoint re-tried", calls)
	}
}

// The back-off must never reach past the cached token's expiry: after that
// there is nothing left to serve, so holding it would refuse sessions the
// endpoint might by then be able to answer.
func TestTokenBackoffNeverOutlivesTheTokenItProtects(t *testing.T) {
	now := time.Unix(1000, 0)
	calls := 0
	ts := &tokenSource{
		log: slog.New(slog.DiscardHandler),
		now: func() time.Time { return now },
		fetch: func(context.Context) (string, time.Duration, error) {
			calls++
			if calls == 1 {
				return "tok", time.Hour, nil
			}
			return "", 0, errors.New("endpoint down")
		},
	}
	if _, err := ts.get(context.Background()); err != nil {
		t.Fatal(err)
	}
	// Inside the refresh margin and 10s from expiry: the refresh fails and the
	// stale token is served, arming a back-off shorter than tokenRetryBackoff.
	now = now.Add(time.Hour - 10*time.Second)
	if tok, err := ts.get(context.Background()); err != nil || tok != "tok" {
		t.Fatalf("stale-serve: %q, %v", tok, err)
	}
	now = now.Add(11 * time.Second) // past expiry
	if _, err := ts.get(context.Background()); err == nil {
		t.Fatal("want an error once the token has expired")
	}
	if calls != 3 {
		t.Fatalf("fetches = %d: the back-off outlived the token, so an expired credential was never re-attempted", calls)
	}
}

// The failure lines are about a STATE — the endpoint is down — noticed once per
// SASL session for the length of the outage: one line per tokenWarnEvery, not
// one per failed fetch, and — the half this used to leave unpinned — AGAIN once
// that interval has passed, or a long outage says nothing after its first
// minute. The throttle reads the token source's own injected clock
// (logdedupe.Throttle.AllowAt); it read the wall clock, so stepping this clock
// through a ten-minute outage still produced exactly one line and the test
// expecting one passed whatever the cadence was.
func TestTokenFailureWarningsAreThrottled(t *testing.T) {
	log, dump := capturedLog()
	now := time.Unix(1000, 0)
	calls := 0
	ts := &tokenSource{
		log: log, what: "imds",
		now: func() time.Time { return now },
		fetch: func(context.Context) (string, time.Duration, error) {
			calls++
			return "", 0, errors.New("imds: connection refused")
		},
	}
	fail := func() {
		t.Helper()
		if _, err := ts.get(context.Background()); err == nil {
			t.Fatal("want an error")
		}
	}
	warns := func() int { return strings.Count(dump(), "level=WARN") }

	// Inside one tokenWarnEvery: two fetches (the back-off is shorter), one line.
	fail()
	now = now.Add(tokenRetryBackoff)
	fail()
	if calls != 2 || warns() != 1 {
		t.Fatalf("fetches = %d, warnings = %d inside one warn interval, want 2 and 1:\n%s", calls, warns(), dump())
	}
	// The interval passes: the outage is restated.
	now = now.Add(tokenWarnEvery - tokenRetryBackoff)
	fail()
	if warns() != 2 {
		t.Fatalf("warnings = %d once tokenWarnEvery elapsed, want 2 — a long outage must keep saying so:\n%s", warns(), dump())
	}
	now = now.Add(tokenRetryBackoff)
	fail()
	if warns() != 2 {
		t.Fatalf("warnings = %d inside the second interval, want still 2", warns())
	}
}

// The stale-serve line — the token endpoint is failing but the last good token
// still works — has its own throttle and the same cadence.
func TestTokenStaleServeWarningsAreThrottled(t *testing.T) {
	log, dump := capturedLog()
	now := time.Unix(1000, 0)
	calls := 0
	ts := &tokenSource{
		log: log, what: "imds",
		now: func() time.Time { return now },
		fetch: func(context.Context) (string, time.Duration, error) {
			calls++
			if calls == 1 {
				return "tok", time.Hour, nil
			}
			return "", 0, errors.New("imds: connection refused")
		},
	}
	serveStale := func() {
		t.Helper()
		if tok, err := ts.get(context.Background()); err != nil || tok != "tok" {
			t.Fatalf("want the stale token served, got %q, %v", tok, err)
		}
	}
	warns := func() int { return strings.Count(dump(), "level=WARN") }

	serveStale()                                           // the first acquisition
	now = now.Add(time.Hour - refreshMargin + time.Second) // inside the refresh margin
	serveStale()
	now = now.Add(tokenRetryBackoff)
	serveStale()
	if calls != 3 || warns() != 1 {
		t.Fatalf("fetches = %d, warnings = %d inside one warn interval, want 3 and 1:\n%s", calls, warns(), dump())
	}
	now = now.Add(tokenWarnEvery - tokenRetryBackoff)
	serveStale()
	if warns() != 2 {
		t.Fatalf("warnings = %d once tokenWarnEvery elapsed, want 2:\n%s", warns(), dump())
	}
}

// A recovered fetch says so: both failure lines are throttled and both say the
// pipeline is about to stop working, so a silent recovery reads as an outage
// nobody has noticed yet.
func TestTokenRecoveryIsReported(t *testing.T) {
	log, dump := capturedLog()
	now := time.Unix(1000, 0)
	calls := 0
	ts := &tokenSource{
		log: log, what: "workload identity",
		now: func() time.Time { return now },
		fetch: func(context.Context) (string, time.Duration, error) {
			calls++
			if calls == 2 {
				return "", 0, errors.New("endpoint down")
			}
			return "tok", time.Hour, nil
		},
	}
	if _, err := ts.get(context.Background()); err != nil { // first acquisition
		t.Fatal(err)
	}
	now = now.Add(time.Hour - time.Minute) // inside the margin: refresh fails
	if _, err := ts.get(context.Background()); err != nil {
		t.Fatal(err)
	}
	now = now.Add(tokenRetryBackoff + time.Second)
	if _, err := ts.get(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(dump(), "recovered") {
		t.Errorf("no recovery line after the token endpoint came back:\n%s", dump())
	}
}

// The failure that OPENS an outage always says so, on both lines, however soon
// after the previous outage's last line it lands. The two throttles alone
// fired once per tokenWarnEvery, so an outage opening inside that interval —
// seconds after a "recovered" line — was silent, and the log read as an
// endpoint that got better and stayed better.
func TestASecondTokenOutageWarnsAtOnce(t *testing.T) {
	for _, tc := range []struct {
		name string
		// ttl is what the successful fetches grant: inside the refresh margin
		// but unexpired a moment later puts the second failure on the
		// stale-serve line, already expired puts it on the no-token line.
		ttl, step time.Duration
		line      string
	}{
		{"stale", refreshMargin + 10*time.Second, 20 * time.Second, "serving the last good token"},
		{"no token", time.Second, 2 * time.Second, "the consumer cannot authenticate"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			log, dump := capturedLog()
			now := time.Unix(1000, 0)
			calls := 0
			ts := &tokenSource{
				log: log, what: "imds",
				now: func() time.Time { return now },
				fetch: func(context.Context) (string, time.Duration, error) {
					calls++
					if calls%2 == 1 {
						return "tok", tc.ttl, nil
					}
					return "", 0, errors.New("imds: connection refused")
				},
			}
			get := func() { _, _ = ts.get(context.Background()) }
			lines := func() int { return strings.Count(dump(), tc.line) }

			get() // 1: acquired
			now = now.Add(tc.step)
			get() // 2: the first outage opens
			if lines() != 1 {
				t.Fatalf("first outage logged %d %q lines, want 1:\n%s", lines(), tc.line, dump())
			}
			now = now.Add(tokenRetryBackoff)
			get() // 3: recovered
			if !strings.Contains(dump(), "recovered") {
				t.Fatalf("setup: no recovery line:\n%s", dump())
			}
			now = now.Add(tc.step)
			get() // 4: a second outage, well inside tokenWarnEvery of the first line
			if calls != 4 {
				t.Fatalf("setup: %d fetches, want 4", calls)
			}
			if lines() != 2 {
				t.Fatalf("second outage logged %d %q lines in total, want 2 — it opened %v after the first "+
					"outage's line and must announce itself:\n%s", lines(), tc.line, tokenRetryBackoff+tc.step, dump())
			}
		})
	}
}
