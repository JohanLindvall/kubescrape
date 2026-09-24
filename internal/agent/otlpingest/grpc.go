package otlpingest

// The gRPC arm of the ingest receiver: one Export wrapper per signal around the
// shared enrich-and-forward steps (server.go). Admission — the pre-decode tap,
// the decoded-structure claims and the unary interceptor — is grpcadmit.go's.

import (
	"context"
	"errors"

	"go.opentelemetry.io/collector/pdata/plog/plogotlp"
	"go.opentelemetry.io/collector/pdata/pmetric/pmetricotlp"
	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"
)

// grpcExport is the shared shape of the three gRPC Export wrappers: stamp the
// connection's peer address into ctx when the enricher's peer-IP fallback will
// read it (stampPeer), run the signal's enrich-and-forward step, and map a
// failure onto a status the sender's SDK retries correctly (grpcForwardCode)
// carrying the fixed text an unauthenticated sender is entitled to
// (forwardFailureText). The detail goes to the log (noteForwardFailure).
func (s *Server) grpcExport(ctx context.Context, signal string, forward func(ctx context.Context) error) error {
	pctx := ctx
	if s.stampPeer {
		pctx = grpcPeerCtx(ctx)
	}
	err := forward(pctx)
	if err == nil {
		return nil
	}
	// This receiver's OWN refusal keeps its words (receiveRefusal); only a
	// forward failure — whose text is the collector's — is redacted.
	if r, ok := errors.AsType[receiveRefusal](err); ok {
		return grpcForwardStatus(r.err)
	}
	return redactedForwardStatus(err, s.noteForwardFailure(signal, grpcPeerAddr(ctx), err))
}

type logsGRPC struct {
	plogotlp.UnimplementedGRPCServer
	s *Server
}

func (g *logsGRPC) Export(ctx context.Context, req plogotlp.ExportRequest) (plogotlp.ExportResponse, error) {
	// The decoded structure is already charged: by the codec, before the decode,
	// and held by limitUnary until this returns (decodedClaims).
	err := g.s.grpcExport(ctx, "logs", func(ctx context.Context) error { return g.s.forwardLogs(ctx, req.Logs()) })
	if err != nil {
		return plogotlp.ExportResponse{}, err
	}
	return plogotlp.NewExportResponse(), nil
}

type metricsGRPC struct {
	pmetricotlp.UnimplementedGRPCServer
	s *Server
}

func (g *metricsGRPC) Export(ctx context.Context, req pmetricotlp.ExportRequest) (pmetricotlp.ExportResponse, error) {
	// Charged by the codec like the logs arm — point-less metrics included,
	// since emptymetrics.go prunes them only after they are resident.
	err := g.s.grpcExport(ctx, "metrics", func(ctx context.Context) error { return g.s.forwardMetrics(ctx, req.Metrics()) })
	if err != nil {
		return pmetricotlp.ExportResponse{}, err
	}
	return pmetricotlp.NewExportResponse(), nil
}

type tracesGRPC struct {
	ptraceotlp.UnimplementedGRPCServer
	s *Server
}

func (g *tracesGRPC) Export(ctx context.Context, req ptraceotlp.ExportRequest) (ptraceotlp.ExportResponse, error) {
	// Charged by the codec like the logs arm — including for a payload
	// RejectTraces will refuse: the spans are already decoded and resident
	// when the guard runs.
	err := g.s.grpcExport(ctx, "traces", func(ctx context.Context) error { return g.s.forwardTraces(ctx, req.Traces()) })
	if err != nil {
		return ptraceotlp.ExportResponse{}, err
	}
	return ptraceotlp.NewExportResponse(), nil
}
