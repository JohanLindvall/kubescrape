package otlpingest

import (
	"context"
	"net"
	"net/http"
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/plog/plogotlp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
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

// blockingLogsService parks every gRPC push until released.
type blockingLogsService struct {
	plogotlp.UnimplementedGRPCServer
	entered chan struct{}
	release chan struct{}
}

func (b *blockingLogsService) Export(ctx context.Context, _ plogotlp.ExportRequest) (plogotlp.ExportResponse, error) {
	b.entered <- struct{}{}
	select {
	case <-b.release:
	case <-ctx.Done():
	}
	return plogotlp.NewExportResponse(), nil
}

func freeAddr(t *testing.T) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := lis.Addr().String()
	_ = lis.Close()
	return addr
}

// The two listeners stop TOGETHER. gRPC's GracefulStop used to run to
// completion first — unbounded, for as long as one pushed RPC's forward took —
// so the HTTP listener went on ACCEPTING and acking pushes well into a shutdown
// whose final flushes no longer covered them (measured: an HTTP push acked 4.5 s
// after the shutdown began). And a gRPC RPC that outlives the grace is now
// force-stopped, as an HTTP handler already was, so Run returns on the grace
// rather than on the slowest forward.
func TestListenersStopBothDoorsTogetherUnderOneGrace(t *testing.T) {
	grpcAddr, httpAddr := freeAddr(t), freeAddr(t)
	svc := &blockingLogsService{entered: make(chan struct{}, 1), release: make(chan struct{})}
	defer close(svc.release)
	gs := grpc.NewServer()
	plogotlp.RegisterGRPCServer(gs, svc)
	ok := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })

	const grace = time.Second
	ready := make(chan struct{})
	l := Listeners{
		Name:          "test",
		GRPC:          gs,
		GRPCAddr:      grpcAddr,
		HTTP:          NewPushHTTPServer(httpAddr, ok),
		Ready:         func() { close(ready) },
		shutdownGrace: grace,
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
		t.Fatal("listeners never became ready")
	}

	conn, err := grpc.NewClient(grpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	pushErr := make(chan error, 1)
	go func() {
		ld := plog.NewLogs()
		ld.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()
		_, err := plogotlp.NewGRPCClient(conn).Export(context.Background(), plogotlp.NewExportRequestFromLogs(ld))
		pushErr <- err
	}()
	<-svc.entered // a gRPC push is now in its forward, and will stay there

	start := time.Now()
	cancel()
	time.Sleep(grace / 4) // well inside the grace, with the gRPC push still running
	client := &http.Client{Timeout: 2 * time.Second}
	if resp, err := client.Post("http://"+httpAddr+"/v1/logs", "application/x-protobuf", http.NoBody); err == nil {
		_ = resp.Body.Close()
		t.Errorf("the HTTP listener accepted a push %v into the shutdown (status %d) because a gRPC RPC was "+
			"still draining; both doors must close when the shutdown begins", time.Since(start), resp.StatusCode)
	}

	select {
	case err := <-runDone:
		if err != nil {
			t.Fatalf("a ctx-cancelled shutdown must return nil, got %v", err)
		}
		if took := time.Since(start); took > grace+2*time.Second {
			t.Errorf("Run took %v to return, past the %v grace", took, grace)
		}
	case <-time.After(grace + 5*time.Second):
		t.Fatal("Run did not return: a gRPC RPC outliving the grace held the shutdown open")
	}
	select {
	case err := <-pushErr:
		if err == nil {
			t.Error("the gRPC push force-stopped by the expired grace was answered OK; its sender must see a " +
				"transport error so its retry lands on the replacement pod")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the force-stopped gRPC push never returned to its sender")
	}
}
