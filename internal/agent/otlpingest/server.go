package otlpingest

import (
	"context"
	"errors"
	"fmt"
	"io"
	"mime"

	"log/slog"
	"net"
	"net/http"
	"time"

	"github.com/klauspost/compress/gzip"

	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/plog/plogotlp"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/pmetric/pmetricotlp"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/status"

	"github.com/JohanLindvall/kubescrape/internal/agent/otlpexport"
)

// Exporter forwards enriched telemetry; implemented by otlpexport.Client.
type Exporter interface {
	ExportLogs(ctx context.Context, ld plog.Logs) error
	ExportMetrics(ctx context.Context, md pmetric.Metrics) error
}

// TracesExporter forwards traces; implemented by otlpexport.Client (and
// Buffered, which passes traces through unbuffered).
type TracesExporter interface {
	ExportTraces(ctx context.Context, td ptrace.Traces) error
}

// ServerConfig configures the ingest listeners. An empty address disables
// that transport; disabling both makes Run a no-op.
type ServerConfig struct {
	GRPCAddr string // default ":4317" when enabled
	HTTPAddr string // default ":4318" when enabled
	Enricher *Enricher
	Exporter Exporter
	// Traces accepts pushed traces on /v1/traces and the gRPC trace service,
	// enriching resources and passing them through. nil disables traces.
	Traces TracesExporter
	Logger *slog.Logger
}

// Server receives pushed OTLP over gRPC and/or HTTP, enriches it, and
// forwards it through the exporter.
type Server struct {
	cfg ServerConfig
	log *slog.Logger
}

// NewServer creates an ingest Server.
func NewServer(cfg ServerConfig) *Server {
	log := cfg.Logger
	if log == nil {
		log = slog.Default()
	}
	return &Server{cfg: cfg, log: log}
}

// Run serves until ctx is cancelled, then shuts both listeners down.
func (s *Server) Run(ctx context.Context) error {
	var grpcSrv *grpc.Server
	var httpSrv *http.Server
	errc := make(chan error, 2)
	started := 0

	if s.cfg.GRPCAddr != "" {
		lis, err := net.Listen("tcp", s.cfg.GRPCAddr)
		if err != nil {
			return fmt.Errorf("ingest gRPC listen %s: %w", s.cfg.GRPCAddr, err)
		}
		// Mirror the HTTP server's IdleTimeout: reap connections apps opened
		// and abandoned (default gRPC keeps them forever). Message size stays
		// at the 4 MiB gRPC default — the bound on pushed payloads.
		grpcSrv = grpc.NewServer(grpc.KeepaliveParams(keepalive.ServerParameters{
			MaxConnectionIdle: 120 * time.Second,
		}))
		plogotlp.RegisterGRPCServer(grpcSrv, &logsGRPC{s: s})
		pmetricotlp.RegisterGRPCServer(grpcSrv, &metricsGRPC{s: s})
		if s.cfg.Traces != nil {
			ptraceotlp.RegisterGRPCServer(grpcSrv, &tracesGRPC{s: s})
		}
		started++
		go func() { errc <- grpcSrv.Serve(lis) }()
		s.log.Info("otlp ingest gRPC listening", "addr", s.cfg.GRPCAddr)
	}

	if s.cfg.HTTPAddr != "" {
		mux := http.NewServeMux()
		mux.HandleFunc("POST /v1/logs", s.handleHTTPLogs)
		mux.HandleFunc("POST /v1/metrics", s.handleHTTPMetrics)
		if s.cfg.Traces != nil {
			mux.HandleFunc("POST /v1/traces", s.handleHTTPTraces)
		}
		// ReadHeaderTimeout kills Slowloris header trickling; ReadTimeout
		// bounds a trickled request body (the handlers read up to 16 MiB and
		// senders are node-local, so 60s is generous — it also caps handler
		// runtime via the whole-request read deadline, fine because forwarding
		// is bounded by the exporter's own much shorter timeout and a cut-off
		// surfaces as a retryable 503); IdleTimeout reaps parked keep-alives.
		// WriteTimeout is deliberately omitted: responses are tiny and its
		// clock would race a slow-but-legal body upload plus the forward.
		httpSrv = &http.Server{
			Addr:              s.cfg.HTTPAddr,
			Handler:           mux,
			ReadHeaderTimeout: 10 * time.Second,
			ReadTimeout:       60 * time.Second,
			IdleTimeout:       120 * time.Second,
		}
		started++
		go func() {
			if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				errc <- err
				return
			}
			errc <- nil
		}()
		s.log.Info("otlp ingest HTTP listening", "addr", s.cfg.HTTPAddr)
	}

	if started == 0 {
		return nil
	}

	// A runtime listener failure must propagate to the caller (main treats it
	// as fatal and exits non-zero); a ctx-cancelled shutdown returns nil.
	var runErr error
	select {
	case <-ctx.Done():
	case err := <-errc:
		if err != nil {
			s.log.Error("otlp ingest listener failed", "error", err)
			runErr = fmt.Errorf("ingest listener: %w", err)
		}
	}
	if grpcSrv != nil {
		grpcSrv.GracefulStop()
	}
	if httpSrv != nil {
		sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = httpSrv.Shutdown(sctx)
	}
	return runErr
}

// --- gRPC ---

type logsGRPC struct {
	plogotlp.UnimplementedGRPCServer
	s *Server
}

func (g *logsGRPC) Export(ctx context.Context, req plogotlp.ExportRequest) (plogotlp.ExportResponse, error) {
	ctx = grpcPeerCtx(ctx)
	ld := req.Logs()
	g.s.cfg.Enricher.EnrichLogs(ctx, ld)
	if err := g.s.cfg.Exporter.ExportLogs(ctx, ld); err != nil {
		return plogotlp.ExportResponse{}, grpcForwardStatus(err)
	}
	return plogotlp.NewExportResponse(), nil
}

type metricsGRPC struct {
	pmetricotlp.UnimplementedGRPCServer
	s *Server
}

func (g *metricsGRPC) Export(ctx context.Context, req pmetricotlp.ExportRequest) (pmetricotlp.ExportResponse, error) {
	ctx = grpcPeerCtx(ctx)
	md := g.s.cfg.Enricher.EnrichMetrics(ctx, req.Metrics())
	if err := g.s.cfg.Exporter.ExportMetrics(ctx, md); err != nil {
		return pmetricotlp.ExportResponse{}, grpcForwardStatus(err)
	}
	return pmetricotlp.NewExportResponse(), nil
}

// grpcForwardStatus maps a forwarding failure onto a gRPC status the sender's
// SDK retries correctly. A bare error would surface as codes.Unknown —
// NON-retryable per the OTLP spec — making senders permanently drop batches on
// transient conditions (a full disk buffer, an upstream 5xx). A status error
// from a gRPC upstream passes through unchanged.
func grpcForwardStatus(err error) error {
	if _, ok := status.FromError(err); ok {
		return err
	}
	// Permanence is classified by otlpexport.IsPermanent (the single source of
	// truth): only definitive upstream rejections become InvalidArgument (do
	// not retry). Everything else — spool.ErrFull back-pressure, upstream 5xx,
	// 401/403/404 windows, timeouts, unclassified failures — is Unavailable:
	// the receiver is a proxy, and the sender retrying is the safe default.
	if otlpexport.IsPermanent(err) {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	return status.Error(codes.Unavailable, err.Error())
}

type tracesGRPC struct {
	ptraceotlp.UnimplementedGRPCServer
	s *Server
}

func (g *tracesGRPC) Export(ctx context.Context, req ptraceotlp.ExportRequest) (ptraceotlp.ExportResponse, error) {
	ctx = grpcPeerCtx(ctx)
	td := req.Traces()
	g.s.cfg.Enricher.EnrichTraces(ctx, td)
	if err := g.s.cfg.Traces.ExportTraces(ctx, td); err != nil {
		return ptraceotlp.ExportResponse{}, grpcForwardStatus(err)
	}
	return ptraceotlp.NewExportResponse(), nil
}

// --- HTTP (OTLP/HTTP protobuf) ---

func (s *Server) handleHTTPLogs(w http.ResponseWriter, r *http.Request) {
	body, err := readBody(r)
	if err != nil {
		http.Error(w, err.Error(), bodyErrorStatus(err))
		return
	}
	req := plogotlp.NewExportRequest()
	if err := req.UnmarshalProto(body); err != nil {
		http.Error(w, "malformed OTLP logs payload", http.StatusBadRequest)
		return
	}
	ctx := withPeerIP(r.Context(), r.RemoteAddr)
	ld := req.Logs()
	s.cfg.Enricher.EnrichLogs(ctx, ld)
	if err := s.cfg.Exporter.ExportLogs(ctx, ld); err != nil {
		http.Error(w, err.Error(), httpForwardStatus(err))
		return
	}
	writeProto(w, plogotlp.NewExportResponse())
}

func (s *Server) handleHTTPMetrics(w http.ResponseWriter, r *http.Request) {
	body, err := readBody(r)
	if err != nil {
		http.Error(w, err.Error(), bodyErrorStatus(err))
		return
	}
	req := pmetricotlp.NewExportRequest()
	if err := req.UnmarshalProto(body); err != nil {
		http.Error(w, "malformed OTLP metrics payload", http.StatusBadRequest)
		return
	}
	ctx := withPeerIP(r.Context(), r.RemoteAddr)
	md := s.cfg.Enricher.EnrichMetrics(ctx, req.Metrics())
	if err := s.cfg.Exporter.ExportMetrics(ctx, md); err != nil {
		http.Error(w, err.Error(), httpForwardStatus(err))
		return
	}
	writeProto(w, pmetricResponse(pmetricotlp.NewExportResponse()))
}

func (s *Server) handleHTTPTraces(w http.ResponseWriter, r *http.Request) {
	body, err := readBody(r)
	if err != nil {
		http.Error(w, err.Error(), bodyErrorStatus(err))
		return
	}
	req := ptraceotlp.NewExportRequest()
	if err := req.UnmarshalProto(body); err != nil {
		http.Error(w, "malformed OTLP traces payload", http.StatusBadRequest)
		return
	}
	ctx := withPeerIP(r.Context(), r.RemoteAddr)
	td := req.Traces()
	s.cfg.Enricher.EnrichTraces(ctx, td)
	if err := s.cfg.Traces.ExportTraces(ctx, td); err != nil {
		http.Error(w, err.Error(), httpForwardStatus(err))
		return
	}
	writeProto(w, ptraceResponse(ptraceotlp.NewExportResponse()))
}

// ptraceResponse adapts the traces response to the marshaler interface.
func ptraceResponse(r ptraceotlp.ExportResponse) protoMarshaler { return r }

// httpForwardStatus maps a forwarding failure onto the HTTP status the sender
// retries correctly (the HTTP counterpart of grpcForwardStatus): a permanent
// upstream rejection is 400 (the sender must not retry the batch), everything
// else — spool.ErrFull back-pressure, upstream 5xx, timeouts — is 503
// (retryable).
func httpForwardStatus(err error) int {
	if otlpexport.IsPermanent(err) {
		return http.StatusBadRequest
	}
	return http.StatusServiceUnavailable
}

// bodyErrorStatus maps a readBody failure to its HTTP status.
func bodyErrorStatus(err error) int {
	switch {
	case errors.Is(err, errBodyTooLarge):
		return http.StatusRequestEntityTooLarge
	case errors.Is(err, errUnsupportedType):
		return http.StatusUnsupportedMediaType
	}
	return http.StatusBadRequest
}

const maxIngestBody = 16 << 20 // 16 MiB per request

// errBodyTooLarge maps to 413; truncating silently could ACK a payload whose
// tail was dropped.
var errBodyTooLarge = fmt.Errorf("request body exceeds %d bytes", maxIngestBody)

// errUnsupportedType maps to 415 (wrong media type, not a malformed request).
var errUnsupportedType = errors.New("unsupported Content-Type")

// cappedReader bounds the compressed request body. Reads past the cap fail
// with errBodyTooLarge so an oversized upload surfaces as 413 rather than as
// the gzip parse error its truncation would otherwise produce.
type cappedReader struct {
	r      io.Reader
	remain int64
}

func (c *cappedReader) Read(p []byte) (int, error) {
	if c.remain <= 0 {
		return 0, errBodyTooLarge
	}
	if int64(len(p)) > c.remain {
		p = p[:c.remain]
	}
	n, err := c.r.Read(p)
	c.remain -= int64(n)
	return n, err
}

func readBody(r *http.Request) ([]byte, error) {
	if ct := r.Header.Get("Content-Type"); ct != "" {
		// Parameterized types ("application/x-protobuf; charset=...") are fine;
		// only the media type itself must match.
		if mt, _, err := mime.ParseMediaType(ct); err != nil || mt != "application/x-protobuf" {
			return nil, fmt.Errorf("%w %q (want application/x-protobuf)", errUnsupportedType, ct)
		}
	}
	var src io.Reader = r.Body
	var capped *cappedReader
	switch enc := r.Header.Get("Content-Encoding"); enc {
	case "", "identity":
	case "gzip": // OTel SDKs commonly gzip OTLP/HTTP
		// Allow one byte over the cap so an exactly-at-cap compressed body is
		// not misreported as oversized; the decompressed cap below still holds.
		capped = &cappedReader{r: r.Body, remain: maxIngestBody + 1}
		zr, err := gzip.NewReader(capped)
		if err != nil {
			return nil, fmt.Errorf("gzip body: %w", err)
		}
		defer func() { _ = zr.Close() }()
		src = zr
	default:
		return nil, fmt.Errorf("unsupported Content-Encoding %q (want gzip or identity)", enc)
	}
	// The cap applies to the decompressed size too (zip-bomb guard). Read one
	// byte beyond it to distinguish at-cap from over-cap and reject the latter.
	body, err := io.ReadAll(io.LimitReader(src, maxIngestBody+1))
	if err != nil {
		if capped != nil && capped.remain <= 0 {
			// The compressed body hit the cap: the "gzip" failure is our own
			// truncation, not the sender's payload — report 413, not 400.
			return nil, errBodyTooLarge
		}
		return nil, err
	}
	if len(body) > maxIngestBody {
		return nil, errBodyTooLarge
	}
	return body, nil
}

type protoMarshaler interface{ MarshalProto() ([]byte, error) }

func writeProto(w http.ResponseWriter, m protoMarshaler) {
	b, err := m.MarshalProto()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/x-protobuf")
	_, _ = w.Write(b)
}

// pmetricResponse adapts the metrics response to the marshaler interface.
func pmetricResponse(r pmetricotlp.ExportResponse) protoMarshaler { return r }
