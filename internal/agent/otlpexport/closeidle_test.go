package otlpexport

// Client.Close on the OTLP/HTTP protocol.

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

// Close must release the HTTP arm's pooled sockets, not only the gRPC
// ClientConn. The Client owns its own http.Transport (16 idle connections per
// host, a 90s IdleConnTimeout) and every caller — BuildExporter's midway
// failure, PerSignal.Close — drops the Client entirely, so a connection this
// does not close can never be closed by anything.
func TestCloseReleasesPooledHTTPConnections(t *testing.T) {
	var mu sync.Mutex
	closed := 0
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	srv.Config.ConnState = func(_ net.Conn, s http.ConnState) {
		if s == http.StateClosed {
			mu.Lock()
			closed++
			mu.Unlock()
		}
	}
	srv.Start()
	defer srv.Close()

	c, err := New(Config{Endpoint: srv.URL, Protocol: "http", Compression: "none", Timeout: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.ExportLogs(context.Background(), testLogsPayload()); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	before := closed
	mu.Unlock()
	if before != 0 {
		t.Fatalf("the connection was closed before Close (%d); the pool is not being exercised", before)
	}

	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	// The server observes the close asynchronously.
	deadline := time.Now().Add(5 * time.Second)
	for {
		mu.Lock()
		got := closed
		mu.Unlock()
		if got > before {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("Close left the idle HTTP connection open: nothing holds the Client any more, so it can never be closed")
		}
		time.Sleep(5 * time.Millisecond)
	}
}
