package obs

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/http/pprof"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// RuntimeHandler serves the Go runtime and process metrics (go_*, process_*)
// in Prometheus text format on a dedicated registry, for the dedicated metrics
// listener (-metrics-listen).
//
// These are deliberately NOT pushed over OTLP: the runtime's own numbers are
// operator-facing process diagnostics, and a Prometheus scrape is how they are
// consumed. kubescrape's own kubescrape_* metrics are the telemetry the service
// produces ABOUT the cluster and stay on the OTLP push (Registry) — they also
// cannot be served here, because Registry.snapshot() consumes the interval
// state the push path depends on, so scraping it would steal samples from the
// exporter.
func RuntimeHandler() http.Handler {
	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector())
	reg.MustRegister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	return promhttp.HandlerFor(reg, promhttp.HandlerOpts{})
}

// ServeMetrics starts a dedicated HTTP listener serving RuntimeHandler on
// /metrics, shutting down when ctx is done. A separate port from the health and
// debug surface: the scrape target is what a Prometheus/ServiceMonitor setup
// points at, while /debug and /readyz are operator-facing and often reachable
// only inside the cluster. An empty addr disables it.
func ServeMetrics(ctx context.Context, addr string, log *slog.Logger) func() {
	if addr == "" {
		return func() {}
	}
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", RuntimeHandler())
	srv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 10 * time.Second}
	go func() {
		log.Info("metrics endpoint started", "addr", addr, "path", "/metrics")
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("metrics endpoint failed", "addr", addr, "error", err)
		}
	}()
	return func() {
		sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(sctx)
	}
}

// ServePprof starts a dedicated HTTP listener serving net/http/pprof under
// /debug/pprof, shutting down when ctx is done. Its own port, separate from
// both the metrics endpoint and the health/debug surface: profiles expose
// goroutine stacks and heap contents, so the port that carries them is the one
// you firewall, bind to localhost, or leave unset. Empty addr disables it.
func ServePprof(ctx context.Context, addr string, log *slog.Logger) func() {
	if addr == "" {
		return func() {}
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /debug/pprof/", pprof.Index)
	mux.HandleFunc("GET /debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("GET /debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("GET /debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("GET /debug/pprof/trace", pprof.Trace)
	// No ReadHeaderTimeout bound on the profile handlers: /debug/pprof/profile
	// legitimately streams for its full ?seconds= duration.
	srv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 10 * time.Second}
	go func() {
		log.Info("pprof endpoint started", "addr", addr, "path", "/debug/pprof/")
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("pprof endpoint failed", "addr", addr, "error", err)
		}
	}()
	return func() {
		sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(sctx)
	}
}
