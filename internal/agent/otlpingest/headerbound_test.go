package otlpingest

import (
	"context"
	"net"
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
	for i := 0; i < 100; i++ {
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
