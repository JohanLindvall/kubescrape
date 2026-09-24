package obs

import (
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"syscall"
	"testing"
	"time"
)

// listenerStarters are the two diagnostic listeners, each bound to addr.
func listenerStarters(addr string) map[string]func() (func(), error) {
	log := slog.New(slog.DiscardHandler)
	return map[string]func() (func(), error){
		"metrics": func() (func(), error) { return ServeMetrics(addr, true, log) },
		"pprof":   func() (func(), error) { return ServePprof(addr, log) },
	}
}

// A failed bind is RETURNED, never only logged from the serving goroutine: with
// -self-metrics-interval=0 the metrics port is the only delivery path for every
// kubescrape_* metric, and a caller that asked for a port and did not get one
// must be able to refuse to start rather than run on with it closed.
func TestListenersReturnTheirBindFailure(t *testing.T) {
	held, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = held.Close() }()
	addr := held.Addr().String()

	for name, start := range listenerStarters(addr) {
		stop, err := start()
		if err == nil {
			stop()
			t.Fatalf("%s: binding an occupied %s returned no error", name, addr)
		}
		if !strings.Contains(err.Error(), addr) {
			t.Errorf("%s: the bind error %q does not name the address", name, err)
		}
		stop() // the stopper returned beside an error must be safe to call
	}
}

// The success half: the listener is serving by the time the call returns (the
// bind is synchronous), and its stopper shuts it down and is idempotent, since
// both mains defer it and an error path may call it too.
func TestListenersServeUntilStopped(t *testing.T) {
	for name, path := range map[string]string{"metrics": "/metrics", "pprof": "/debug/pprof/"} {
		t.Run(name, func(t *testing.T) {
			addr := freeLoopbackAddr(t)
			stop, err := listenerStarters(addr)[name]()
			if err != nil {
				t.Fatal(err)
			}
			client := &http.Client{Timeout: 10 * time.Second}
			resp, err := client.Get("http://" + addr + path)
			if err != nil {
				stop()
				t.Fatalf("GET %s right after the call returned: %v", path, err)
			}
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				t.Errorf("GET %s = %d, want 200", path, resp.StatusCode)
			}
			client.CloseIdleConnections()

			stop()
			stop() // idempotent (sync.Once)
			if _, err := client.Get("http://" + addr + path); !errors.Is(err, syscall.ECONNREFUSED) {
				t.Errorf("GET %s after stop: err = %v, want connection refused", path, err)
			}
		})
	}
}

// freeLoopbackAddr returns a loopback address that was free a moment ago. The
// listeners take an address rather than a net.Listener, so a :0 bind would leave
// the test no way to learn the port it got.
func freeLoopbackAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	if err := ln.Close(); err != nil {
		t.Fatal(err)
	}
	return addr
}
