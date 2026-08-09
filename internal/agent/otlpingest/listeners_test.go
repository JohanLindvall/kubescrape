package otlpingest

import (
	"context"
	"net"
	"net/http"
	"testing"
	"time"
)

// The Close that follows an expired shutdown grace. http.Server.Shutdown waits
// for active handlers but never interrupts them, and NewPushHTTPServer's
// ReadTimeout gives a request body 60 seconds of trickle — far past the grace.
// Before the Close, a handler still running when the grace expired kept its
// connection and could answer 200 into it while the rest of the process had
// already flushed its buffers; the tail-sampling buffer's post-Flush latch
// makes such an ack honest at that layer, and this is the listener-side half:
// past the grace the connection dies, the straggling sender sees a transport
// error, and its retry lands on the replacement pod (at-least-once).
//
// The grace is injected tiny (the production value is httpShutdownGrace);
// nothing here races a clock — the handler is released only after Run returns,
// so Shutdown deterministically times out and Close is deterministically what
// unblocks the client.
func TestHTTPCloseFollowsAnExpiredShutdownGrace(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := lis.Addr().String()
	_ = lis.Close()

	entered := make(chan struct{})
	release := make(chan struct{})
	defer close(release)
	h := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		close(entered)
		<-release // still "handling" long past any grace
		w.WriteHeader(http.StatusOK)
	})
	ready := make(chan struct{})
	l := Listeners{
		Name:          "test",
		HTTP:          NewPushHTTPServer(addr, h),
		Ready:         func() { close(ready) },
		shutdownGrace: 50 * time.Millisecond,
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runDone := make(chan error, 1)
	go func() { runDone <- l.Run(ctx) }()
	select {
	case <-ready:
	case err := <-runDone:
		t.Fatalf("Run returned before ready: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("listener never became ready")
	}

	// A request whose handler will still be running when the grace expires.
	reqErr := make(chan error, 1)
	go func() {
		resp, err := http.Get("http://" + addr + "/v1/traces")
		if err == nil {
			_ = resp.Body.Close()
		}
		reqErr <- err
	}()
	<-entered

	cancel() // shutdown: Shutdown waits the grace for the active handler, then Close
	select {
	case err := <-runDone:
		if err != nil {
			t.Fatalf("a ctx-cancelled shutdown must return nil, got %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return: without the Close a stuck handler holds the shutdown for its full 60s ReadTimeout window")
	}
	select {
	case err := <-reqErr:
		if err == nil {
			t.Fatal("the straggling request got a clean response after the listener force-closed; its sender must see a transport error so its retry lands on the replacement pod")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the straggler's connection was not closed by the expired grace")
	}
}
