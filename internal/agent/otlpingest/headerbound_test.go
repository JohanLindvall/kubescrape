package otlpingest

import (
	"bytes"
	"context"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/plog/plogotlp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

// TestIngestGRPCBoundsTheHeaderBlock pins the receive-side half of the
// unauthenticated-listener bound: grpc-go's server default lets a peer send a
// 16 MiB header block, which this process decodes per stream before any
// application code — the tap, the interceptor, the codec — runs. Without
// MaxHeaderListSizeOption a 256 KiB header sails through and the push is
// ACCEPTED; with it the sender is refused at the protocol level.
func TestIngestGRPCBoundsTheHeaderBlock(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := lis.Addr().String()
	_ = lis.Close()

	exp := &captureExporter{}
	srv := NewServer(ServerConfig{
		GRPCAddr: addr,
		Enricher: newEnricher(newMeta(), MetricsAuto),
		Exporter: exp,
	})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { _ = srv.Run(ctx); close(done) }()
	defer func() { cancel(); <-done }()

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()

	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	rl.Resource().Attributes().PutStr("container.id", "cafe01")
	rl.ScopeLogs().AppendEmpty().LogRecords().AppendEmpty().Body().SetStr("hi")
	client := plogotlp.NewGRPCClient(conn)

	// One ordinary push first: it waits for the listener AND for the client to
	// have seen the server's SETTINGS frame, which is where the advertised
	// bound arrives.
	var lastErr error
	for range 100 {
		if _, lastErr = client.Export(context.Background(), plogotlp.NewExportRequestFromLogs(ld)); lastErr == nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if lastErr != nil {
		t.Fatal(lastErr)
	}
	if len(exp.logs) != 1 {
		t.Fatalf("warm-up push exported %d batches, want 1", len(exp.logs))
	}

	// 256 KiB in one header value: far inside grpc-go's 16 MiB default, far
	// outside anything an OTLP sender writes.
	big := metadata.AppendToOutgoingContext(context.Background(), "x-junk", strings.Repeat("a", 256<<10))
	rpcCtx, rpcCancel := context.WithTimeout(big, 10*time.Second)
	defer rpcCancel()
	if _, err := client.Export(rpcCtx, plogotlp.NewExportRequestFromLogs(ld)); err == nil {
		t.Fatal("a 256 KiB header block was accepted: grpc-go decodes the whole block before the tap, the interceptor and the codec run, so this is unbounded buffering on an unauthenticated listener")
	}
	if len(exp.logs) != 1 {
		t.Fatalf("the refused push still reached the exporter: %d batches", len(exp.logs))
	}
}

// TestIngestHTTPBoundsTheHeaderBlock is the HTTP arm of the same bound.
// net/http's default MaxHeaderBytes is 1 MiB, and a handler parked in its body
// read keeps the parsed header (~3x its wire size) for the whole ReadTimeout,
// charged to no admission budget — so the HTTP arm of every OTLP receiver was
// ~16x looser than the gRPC arm beside it. It runs the REAL listener
// (NewServer + Run, i.e. NewPushHTTPServer), because a handler-level test would
// never see net/http's refusal: it happens before any handler runs.
func TestIngestHTTPBoundsTheHeaderBlock(t *testing.T) {
	if got := NewPushHTTPServer(":0", http.NotFoundHandler()).MaxHeaderBytes; got != maxHeaderListBytes {
		t.Fatalf("NewPushHTTPServer MaxHeaderBytes = %d, want maxHeaderListBytes (%d): both arms of one receiver share one header bound", got, maxHeaderListBytes)
	}

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := lis.Addr().String()
	_ = lis.Close()

	exp := &captureExporter{}
	srv := NewServer(ServerConfig{
		HTTPAddr: addr,
		Enricher: newEnricher(newMeta(), MetricsAuto),
		Exporter: exp,
	})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { _ = srv.Run(ctx); close(done) }()
	defer func() { cancel(); <-done }()

	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	rl.Resource().Attributes().PutStr("container.id", "cafe01")
	rl.ScopeLogs().AppendEmpty().LogRecords().AppendEmpty().Body().SetStr("hi")
	body, err := plogotlp.NewExportRequestFromLogs(ld).MarshalProto()
	if err != nil {
		t.Fatal(err)
	}
	push := func(pad int) (int, error) {
		req, err := http.NewRequest(http.MethodPost, "http://"+addr+"/v1/logs", bytes.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Content-Type", "application/x-protobuf")
		req.Header.Set("X-Junk", strings.Repeat("a", pad))
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return 0, err
		}
		_ = resp.Body.Close()
		return resp.StatusCode, nil
	}

	// An honest sender's fattest header set (a long bearer token, a tenant id)
	// is far inside the bound; this push also waits for the listener.
	var code int
	for range 100 {
		if code, err = push(16 << 10); err == nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if err != nil {
		t.Fatal(err)
	}
	if code != http.StatusOK || len(exp.logs) != 1 {
		t.Fatalf("a 16 KiB header push: status %d, %d batches exported; want 200 and 1", code, len(exp.logs))
	}

	// 128 KiB: past the bound, far inside net/http's 1 MiB default.
	code, err = push(128 << 10)
	if err != nil {
		t.Fatal(err)
	}
	if code != http.StatusRequestHeaderFieldsTooLarge {
		t.Fatalf("a 128 KiB header block: status %d, want 431 — net/http's 1 MiB default is back on an unauthenticated listener", code)
	}
	if len(exp.logs) != 1 {
		t.Fatalf("the refused push still reached the exporter: %d batches", len(exp.logs))
	}
}
