package otlpingest

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"time"

	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/plog/plogotlp"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/pmetric/pmetricotlp"
	"google.golang.org/grpc"
)

// Exporter forwards enriched telemetry; implemented by otlpexport.Client.
type Exporter interface {
	ExportLogs(ctx context.Context, ld plog.Logs) error
	ExportMetrics(ctx context.Context, md pmetric.Metrics) error
}

// ServerConfig configures the ingest listeners. An empty address disables
// that transport; disabling both makes Run a no-op.
type ServerConfig struct {
	GRPCAddr string // default ":4317" when enabled
	HTTPAddr string // default ":4318" when enabled
	Enricher *Enricher
	Exporter Exporter
	Logger   *slog.Logger
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
		grpcSrv = grpc.NewServer()
		plogotlp.RegisterGRPCServer(grpcSrv, &logsGRPC{s: s})
		pmetricotlp.RegisterGRPCServer(grpcSrv, &metricsGRPC{s: s})
		started++
		go func() { errc <- grpcSrv.Serve(lis) }()
		s.log.Info("otlp ingest gRPC listening", "addr", s.cfg.GRPCAddr)
	}

	if s.cfg.HTTPAddr != "" {
		mux := http.NewServeMux()
		mux.HandleFunc("POST /v1/logs", s.handleHTTPLogs)
		mux.HandleFunc("POST /v1/metrics", s.handleHTTPMetrics)
		httpSrv = &http.Server{Addr: s.cfg.HTTPAddr, Handler: mux, ReadHeaderTimeout: 10 * time.Second}
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

	select {
	case <-ctx.Done():
	case err := <-errc:
		if err != nil {
			s.log.Error("otlp ingest listener failed", "error", err)
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
	return nil
}

// --- gRPC ---

type logsGRPC struct {
	plogotlp.UnimplementedGRPCServer
	s *Server
}

func (g *logsGRPC) Export(ctx context.Context, req plogotlp.ExportRequest) (plogotlp.ExportResponse, error) {
	ld := req.Logs()
	g.s.cfg.Enricher.EnrichLogs(ctx, ld)
	if err := g.s.cfg.Exporter.ExportLogs(ctx, ld); err != nil {
		return plogotlp.ExportResponse{}, err
	}
	return plogotlp.NewExportResponse(), nil
}

type metricsGRPC struct {
	pmetricotlp.UnimplementedGRPCServer
	s *Server
}

func (g *metricsGRPC) Export(ctx context.Context, req pmetricotlp.ExportRequest) (pmetricotlp.ExportResponse, error) {
	md := g.s.cfg.Enricher.EnrichMetrics(ctx, req.Metrics())
	if err := g.s.cfg.Exporter.ExportMetrics(ctx, md); err != nil {
		return pmetricotlp.ExportResponse{}, err
	}
	return pmetricotlp.NewExportResponse(), nil
}

// --- HTTP (OTLP/HTTP protobuf) ---

func (s *Server) handleHTTPLogs(w http.ResponseWriter, r *http.Request) {
	body, err := readBody(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	req := plogotlp.NewExportRequest()
	if err := req.UnmarshalProto(body); err != nil {
		http.Error(w, "malformed OTLP logs payload", http.StatusBadRequest)
		return
	}
	ld := req.Logs()
	s.cfg.Enricher.EnrichLogs(r.Context(), ld)
	if err := s.cfg.Exporter.ExportLogs(r.Context(), ld); err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	writeProto(w, plogotlp.NewExportResponse())
}

func (s *Server) handleHTTPMetrics(w http.ResponseWriter, r *http.Request) {
	body, err := readBody(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	req := pmetricotlp.NewExportRequest()
	if err := req.UnmarshalProto(body); err != nil {
		http.Error(w, "malformed OTLP metrics payload", http.StatusBadRequest)
		return
	}
	md := s.cfg.Enricher.EnrichMetrics(r.Context(), req.Metrics())
	if err := s.cfg.Exporter.ExportMetrics(r.Context(), md); err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	writeProto(w, pmetricResponse(pmetricotlp.NewExportResponse()))
}

const maxIngestBody = 16 << 20 // 16 MiB per request

func readBody(r *http.Request) ([]byte, error) {
	if ct := r.Header.Get("Content-Type"); ct != "" && ct != "application/x-protobuf" {
		return nil, fmt.Errorf("unsupported Content-Type %q (want application/x-protobuf)", ct)
	}
	return io.ReadAll(io.LimitReader(r.Body, maxIngestBody))
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
