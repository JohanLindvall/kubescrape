package server

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/JohanLindvall/kubescrape/internal/services"
	"github.com/JohanLindvall/kubescrape/internal/store"
)

// A container lookup parked on the store's waiter when shutdown begins must
// receive an ANSWER — a retryable 503 with Retry-After — not a connection cut
// at the TCP level.
//
// Observed before the drain existed: with -wait-timeout=30s, a parked
// GET /v1/containers/<unknown>?wait=30s got "Empty reply from server" (no
// status, no body). srv.Shutdown gave up WAITING for the handler at its budget
// and returned — it closes no active connection — and the process exit that
// followed took the socket down; the process exited 0 with nothing logged.
func TestParkedLookupIsAnsweredWhenTheStoreDrains(t *testing.T) {
	st := store.New(time.Minute)
	srv := httptest.NewServer(newAPI(st, 30*time.Second).Handler())
	defer srv.Close()

	type result struct {
		code       int
		retryAfter string
		body       string
		err        error
	}
	done := make(chan result, 1)
	go func() {
		// A wait budget far longer than the test: only the drain can end this
		// request in time, so a pass cannot come from the budget expiring.
		resp, err := http.Get(srv.URL + "/v1/containers/neverappears?wait=30s")
		if err != nil {
			done <- result{err: err}
			return
		}
		defer func() { _ = resp.Body.Close() }()
		b, _ := io.ReadAll(resp.Body)
		done <- result{code: resp.StatusCode, retryAfter: resp.Header.Get("Retry-After"), body: string(b)}
	}()

	// Wait until the handler is actually parked in the store, so the drain
	// under test is the thing that releases it.
	deadline := time.Now().Add(5 * time.Second)
	for st.BlockedLookups() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("lookup never parked in the store")
		}
		time.Sleep(time.Millisecond)
	}

	st.Drain()

	select {
	case got := <-done:
		if got.err != nil {
			t.Fatalf("parked lookup got a transport error instead of a response: %v", got.err)
		}
		if got.code != http.StatusServiceUnavailable {
			t.Fatalf("parked lookup status = %d, want %d; body %q", got.code, http.StatusServiceUnavailable, got.body)
		}
		if got.retryAfter == "" {
			t.Error("503 without Retry-After: the client cannot tell a shed apart from a hard failure")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("parked lookup was not answered after the store drained")
	}
}

// The OTHER parking spot, and the one that is easy to forget: waitReady holds
// a container lookup for its whole wait budget while the INITIAL sync is
// outstanding. A SIGTERM there is the case most correlated with an API-server
// outage — the informers cannot sync, so no request ever reaches the store's
// waiter and draining only the store would leave every lookup parked until the
// process exit cut it without a status.
func TestDrainAnswersLookupsParkedOnTheInitialSync(t *testing.T) {
	st := store.New(time.Minute)
	addPod(st) // resolvable, so only the readiness gate can be parking this
	api := New(Config{
		Store:    st,
		Services: services.NewIndex(),
		Resolver: stubResolver{},
		MaxWait:  30 * time.Second,
		Ready:    make(chan struct{}), // never closed: the caches never sync
	})
	srv := httptest.NewServer(api.Handler())
	defer srv.Close()

	type result struct {
		code       int
		retryAfter string
		body       string
		err        error
	}
	done := make(chan result, 1)
	go func() {
		resp, err := http.Get(srv.URL + "/v1/containers/cafe01?wait=30s")
		if err != nil {
			done <- result{err: err}
			return
		}
		defer func() { _ = resp.Body.Close() }()
		b, _ := io.ReadAll(resp.Body)
		done <- result{code: resp.StatusCode, retryAfter: resp.Header.Get("Retry-After"), body: string(b)}
	}()

	deadline := time.Now().Add(5 * time.Second)
	for api.readyParked.Load() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("the lookup never parked on the readiness gate")
		}
		time.Sleep(time.Millisecond)
	}

	if released := api.Drain(); released < 1 {
		t.Errorf("Drain reported %d released, want the parked lookup counted", released)
	}

	select {
	case got := <-done:
		if got.err != nil {
			t.Fatalf("parked lookup got a transport error instead of a response: %v", got.err)
		}
		if got.code != http.StatusServiceUnavailable {
			t.Fatalf("parked lookup status = %d, want %d; body %q", got.code, http.StatusServiceUnavailable, got.body)
		}
		if got.retryAfter == "" {
			t.Error("503 without Retry-After: the next pod behind the Service can answer at once")
		}
		if !strings.Contains(got.body, "shutting down") {
			t.Errorf("body = %q, want it to say the server is shutting down rather than blaming the sync", got.body)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the lookup parked on the readiness gate was never answered")
	}

	// A later arrival is refused at once rather than parked for its budget.
	start := time.Now()
	resp, err := http.Get(srv.URL + "/v1/containers/cafe01?wait=30s")
	if err != nil {
		t.Fatalf("lookup after the drain: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("lookup after the drain: status %d, want %d", resp.StatusCode, http.StatusServiceUnavailable)
	}
	if d := time.Since(start); d > 5*time.Second {
		t.Fatalf("lookup after the drain took %v, want an immediate refusal", d)
	}
}

// Drain covers BOTH parking spots through one call — which is what main wires
// into the shutdown path, so the store half must not be reachable only via
// store.Drain.
func TestServerDrainReleasesTheStoreWaiterToo(t *testing.T) {
	st := store.New(time.Minute)
	api := newAPI(st, 30*time.Second) // Ready is already closed
	srv := httptest.NewServer(api.Handler())
	defer srv.Close()

	done := make(chan int, 1)
	go func() {
		resp, err := http.Get(srv.URL + "/v1/containers/neverappears?wait=30s")
		if err != nil {
			done <- 0
			return
		}
		defer func() { _ = resp.Body.Close() }()
		_, _ = io.ReadAll(resp.Body)
		done <- resp.StatusCode
	}()

	deadline := time.Now().Add(5 * time.Second)
	for st.BlockedLookups() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("lookup never parked in the store")
		}
		time.Sleep(time.Millisecond)
	}

	if released := api.Drain(); released != 1 {
		t.Errorf("Drain released %d, want the store's one waiter", released)
	}
	// Idempotent: a second call must not double-count or panic on the channel.
	if released := api.Drain(); released != 0 {
		t.Errorf("second Drain released %d, want 0", released)
	}

	select {
	case code := <-done:
		if code != http.StatusServiceUnavailable {
			t.Fatalf("parked lookup status = %d, want %d", code, http.StatusServiceUnavailable)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Server.Drain did not release the store's waiter")
	}
}

// The drain bounds WAITING, not answering: a request whose container is
// already indexed is still served normally while the server finishes its
// in-flight work.
func TestDrainStillServesKnownContainers(t *testing.T) {
	st := store.New(time.Minute)
	addPod(st)
	srv := httptest.NewServer(newAPI(st, time.Second).Handler())
	defer srv.Close()

	st.Drain()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"/v1/containers/cafe01", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("known container after drain: status %d, body %q", resp.StatusCode, b)
	}
}
