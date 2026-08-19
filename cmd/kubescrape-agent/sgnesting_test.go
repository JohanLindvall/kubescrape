package main

// The wire-SHAPE guard on the trace tier's INTERNAL receiver.
//
// pdata's decoder recurses per nesting level with no limit of its own, so a
// small, legal, in-cap message buys an unbounded goroutine stack — and the
// decode happens before any interceptor, so only the codec can refuse it.
// otlpingest.Server wires the guard itself; this receiver assembles its own
// grpc.Server, which is why the option form exists and why the wiring needs a
// test of its own: the port is authenticated, but a token holder is a sibling
// shard, and a shard that OOMs takes every trace it was routed with it.

import (
	"context"
	"encoding/binary"
	"log/slog"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

// rawTraceProto carries pre-built protobuf bytes through pdata's codec
// unchanged: SizeProto/MarshalProto/UnmarshalProto are the interface the codec
// dispatches on, so a payload no encoder would produce can still be sent.
type rawTraceProto struct{ b []byte }

func (r *rawTraceProto) SizeProto() int              { return len(r.b) }
func (r *rawTraceProto) MarshalProto(dst []byte) int { return copy(dst, r.b) }
func (r *rawTraceProto) UnmarshalProto(b []byte) error {
	r.b = append([]byte(nil), b...)
	return nil
}

// protoField appends one length-delimited field.
func protoField(dst []byte, field int, payload []byte) []byte {
	dst = binary.AppendUvarint(dst, uint64(field)<<3|2)
	dst = binary.AppendUvarint(dst, uint64(len(payload)))
	return append(dst, payload...)
}

// nestedAnyValue builds AnyValue{array_value: {values: [AnyValue{...}]}} to the
// requested depth — the shape pdata recurses on, and a shape a real SDK could
// in principle emit, so the payload is one the decoder would happily accept.
func nestedAnyValue(levels int) []byte {
	v := []byte{1<<3 | 2, 0} // AnyValue{string_value: ""}
	for i := 0; i < levels; i++ {
		v = protoField(nil, 5, protoField(nil, 1, v)) // AnyValue{array_value{values}}
	}
	return v
}

// deepTraceRequest wraps a nested attribute value in a legal
// ExportTraceServiceRequest.
func deepTraceRequest(levels int) []byte {
	kv := protoField(protoField(nil, 1, []byte("deep")), 2, nestedAnyValue(levels))
	span := protoField(nil, 9, kv) // Span{attributes}
	ss := protoField(nil, 2, span) // ScopeSpans{spans}
	rs := protoField(nil, 2, ss)   // ResourceSpans{scope_spans}
	return protoField(nil, 1, rs)  // ExportTraceServiceRequest{resource_spans}
}

func TestServiceGraphInternalReceiverRefusesADeeplyNestedPush(t *testing.T) {
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

	// Authenticated, so the payload gets past the tap and reaches the decode —
	// which is the whole point: the credential does not bound the shape.
	rpcCtx, rpcCancel := context.WithTimeout(
		metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer s3cr3t"), 10*time.Second)
	defer rpcCancel()

	// 400 nesting levels is a couple of kilobytes — inside every byte bound
	// this listener has, which is exactly why the bound has to be on the SHAPE.
	deep := &rawTraceProto{b: deepTraceRequest(400)}
	err = conn.Invoke(rpcCtx, "/opentelemetry.proto.collector.trace.v1.TraceService/Export", deep, &rawTraceProto{})
	if err == nil {
		t.Fatal("a 400-level payload was decoded on the internal hop: pdata recurses per level, so this is unbounded goroutine stack for a kilobyte on the wire")
	}

	// And an ordinary push still lands: the guard must be a targeted refusal,
	// not a codec that broke the hop.
	td := ptrace.NewTraces()
	td.ResourceSpans().AppendEmpty().ScopeSpans().AppendEmpty().Spans().AppendEmpty().SetName("ok")
	if _, err := ptraceotlp.NewGRPCClient(conn).Export(rpcCtx, ptraceotlp.NewExportRequestFromTraces(td)); err != nil {
		t.Fatalf("ordinary push through the guarded codec: %v", err)
	}
	if consumed != 1 {
		t.Fatalf("consumed %d spans, want 1", consumed)
	}
}
