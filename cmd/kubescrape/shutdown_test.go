package main

import (
	"context"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"
)

// parkedServer starts an HTTP server whose handler blocks until release is
// closed, and returns its URL plus a func that waits for a request to be
// inside the handler.
func parkedServer(t *testing.T, release <-chan struct{}) (*http.Server, string, func()) {
	t.Helper()
	entered := make(chan struct{})
	var once bool
	mux := http.NewServeMux()
	mux.HandleFunc("/park", func(w http.ResponseWriter, _ *http.Request) {
		if !once {
			once = true
			close(entered)
		}
		<-release
		w.WriteHeader(http.StatusServiceUnavailable)
	})
	srv := &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = srv.Serve(ln) }()
	return srv, "http://" + ln.Addr().String(), func() {
		select {
		case <-entered:
		case <-time.After(5 * time.Second):
			t.Fatal("request never reached the handler")
		}
	}
}

// The drain must run BEFORE srv.Shutdown, not after it: Shutdown waits for
// in-flight handlers, so a release that happens afterwards is a release that
// happens too late — the handler is still parked when the budget runs out,
// Shutdown returns without closing anything, and the process exit that follows
// cuts the connection with no response at all.
//
// The ordering is what this asserts: the drain here is what lets the parked
// handler finish, so Shutdown returns cleanly inside the budget. Run after
// Shutdown, the budget would expire instead.
func TestShutdownHTTPDrainsBeforeShuttingDown(t *testing.T) {
	release := make(chan struct{})
	srv, url, waitEntered := parkedServer(t, release)

	go func() { _, _ = http.Get(url + "/park") }()
	waitEntered()

	log, h := newCapturingLogger()
	drained := 0
	err := shutdownHTTP(context.Background(), srv,
		func() int { drained = 1; close(release); return drained },
		func() int64 { return 0 },
		5*time.Second, log)
	if err != nil {
		t.Fatalf("shutdownHTTP: %v", err)
	}
	if drained != 1 {
		t.Fatal("the drain never ran")
	}
	if warns := h.at(slog.LevelWarn); len(warns) != 0 {
		t.Fatalf("clean shutdown warned: %v", warns)
	}
	if infos := h.at(slog.LevelInfo); len(infos) != 1 || !strings.Contains(infos[0], "released blocked container lookups") {
		t.Fatalf("infos = %v, want the released-lookups line", infos)
	}
}

// A shutdown that runs out of budget leaves requests in flight, and the
// process exit that follows cuts them without a response (Shutdown itself
// closes no active connection — TestShutdownDeadlineDoesNotCloseConnections
// pins that, because the WARN's wording depends on it). It used to happen
// silently — the observed run exited 0 at exactly the 10s step with four INFO
// lines and no mention of the request it dropped — so the deadline is now a
// WARN naming the budget and what was still in flight.
func TestShutdownHTTPWarnsWhenTheBudgetExpires(t *testing.T) {
	release := make(chan struct{})
	defer close(release)
	srv, url, waitEntered := parkedServer(t, release)

	go func() { _, _ = http.Get(url + "/park") }()
	waitEntered()

	log, h := newCapturingLogger()
	// Nothing to drain (this handler is not a store waiter), so the request
	// stays parked and the budget expires.
	err := shutdownHTTP(context.Background(), srv,
		func() int { return 0 },
		func() int64 { return 3 },
		100*time.Millisecond, log)
	// A missed deadline is not a startup-level failure: the process is exiting
	// either way, and returning an error here would mask the real runErr.
	if err != nil {
		t.Fatalf("shutdownHTTP returned %v, want nil for a missed deadline", err)
	}
	warns := h.at(slog.LevelWarn)
	if len(warns) != 1 || !strings.Contains(warns[0], "budget exceeded") {
		t.Fatalf("warns = %v, want one budget-exceeded line", warns)
	}
	if got := h.attr(slog.LevelWarn, "requestsInFlight"); got != "3" {
		t.Fatalf("requestsInFlight attr = %q, want 3: the count is what makes the warning actionable", got)
	}
	if got := h.attr(slog.LevelWarn, "budget"); got == "" {
		t.Error("the warning does not name the budget it exceeded")
	}
	// A drain that releases nothing must not log a line claiming it did.
	if infos := h.at(slog.LevelInfo); len(infos) != 0 {
		t.Fatalf("infos = %v, want none", infos)
	}
}

// What the missed deadline actually does, pinned — because the WARN, the
// store's Drain and four comments describe this sequence, and the obvious
// reading of Shutdown ("it closes the connections") is WRONG.
//
// http.Server.Shutdown closes the listeners and the IDLE connections, then
// WAITS for the active handlers; at its deadline it returns
// context.DeadlineExceeded and touches nothing (only srv.Close closes an active
// connection). The client stays attached and still gets the handler's response.
// The cut an operator sees comes later, from the PROCESS EXITING.
//
// If a future Go release changes this, the wording above it is what has to
// change with it — which is the whole reason this test exists.
func TestShutdownDeadlineDoesNotCloseConnections(t *testing.T) {
	release := make(chan struct{})
	srv, url, waitEntered := parkedServer(t, release)

	type result struct {
		code int
		err  error
	}
	done := make(chan result, 1)
	go func() {
		resp, err := http.Get(url + "/park")
		if err != nil {
			done <- result{err: err}
			return
		}
		defer func() { _ = resp.Body.Close() }()
		_, _ = io.ReadAll(resp.Body)
		done <- result{code: resp.StatusCode}
	}()
	waitEntered()

	log, _ := newCapturingLogger()
	if err := shutdownHTTP(context.Background(), srv,
		func() int { return 0 }, func() int64 { return 1 },
		100*time.Millisecond, log); err != nil {
		t.Fatalf("shutdownHTTP: %v", err)
	}

	// The deadline has passed. Nothing was closed: the request is still parked.
	select {
	case got := <-done:
		t.Fatalf("the client finished at the shutdown deadline (%+v); Shutdown is not supposed to close an active connection", got)
	case <-time.After(200 * time.Millisecond):
	}

	// And when the handler finally answers, the answer still arrives.
	close(release)
	select {
	case got := <-done:
		if got.err != nil {
			t.Fatalf("the client got a transport error after the shutdown deadline: %v", got.err)
		}
		if got.code != http.StatusServiceUnavailable {
			t.Fatalf("client status = %d, want %d (the handler's own response)", got.code, http.StatusServiceUnavailable)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the handler's response never reached the client")
	}
}
