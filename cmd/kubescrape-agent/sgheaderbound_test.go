package main

// The header-block bound on the trace tier's INTERNAL receiver.
//
// The same receive-side gap as otlpingest's (grpc-go's 16 MiB server default),
// and the authentication does not close it: the credential arrives IN the
// header block, so grpc-go has already decoded the whole thing by the time the
// tap can read the token. Wired here because this receiver assembles its own
// grpc.Server, which is why otlpingest exports the option form.

import (
	"context"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

func TestServiceGraphInternalReceiverBoundsTheHeaderBlock(t *testing.T) {
	var consumed int
	rcv := &sgReceiver{
		grpcAddr: freeAddr(t),
		tokens:   func() []string { return []string{"s3cr3t"} },
		consume:  func(_ context.Context, td ptrace.Traces) error { consumed += td.SpanCount(); return nil },
		log:      slog.New(slog.DiscardHandler),
	}
	ready := make(chan struct{})
	rcv.ready = sync.OnceFunc(func() { close(ready) })
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errc := make(chan error, 1)
	go func() { errc <- rcv.Run(ctx) }()
	select {
	case <-ready:
	case err := <-errc:
		t.Fatalf("receiver stopped before it was ready: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("the receiver never became ready")
	}

	conn, err := grpc.NewClient(rcv.grpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	client := ptraceotlp.NewGRPCClient(conn)

	td := ptrace.NewTraces()
	td.ResourceSpans().AppendEmpty().ScopeSpans().AppendEmpty().Spans().AppendEmpty().SetName("ok")
	authed := metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer s3cr3t")

	// One ordinary push first: it is what makes the client see the server's
	// SETTINGS frame, which is where the advertised bound arrives.
	rpcCtx, rpcCancel := context.WithTimeout(authed, 10*time.Second)
	defer rpcCancel()
	if _, err := client.Export(rpcCtx, ptraceotlp.NewExportRequestFromTraces(td)); err != nil {
		t.Fatalf("ordinary push: %v", err)
	}
	if consumed != 1 {
		t.Fatalf("consumed %d spans, want 1", consumed)
	}

	// 256 KiB in one header value, sent by an AUTHENTICATED peer — the point
	// being that the token cannot have been checked yet.
	big := metadata.AppendToOutgoingContext(authed, "x-junk", strings.Repeat("a", 256<<10))
	bigCtx, bigCancel := context.WithTimeout(big, 10*time.Second)
	defer bigCancel()
	if _, err := client.Export(bigCtx, ptraceotlp.NewExportRequestFromTraces(td)); err == nil {
		t.Fatal("a 256 KiB header block was accepted: grpc-go decodes the block before the auth tap runs, so the credential bounds nothing here")
	}
	if consumed != 1 {
		t.Fatalf("the refused push still reached consume: %d spans", consumed)
	}
}
