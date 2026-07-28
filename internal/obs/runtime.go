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

	"github.com/JohanLindvall/kubescrape/internal/metrics"
)

// RuntimeHandler serves Prometheus text metrics on a dedicated registry for
// the dedicated metrics listener (-metrics-listen): always the Go runtime and
// process metrics (go_*, process_*), plus — when internal is true — the
// kubescrape_* Registry metrics through the non-mutating Dump bridge.
//
// The runtime metrics are deliberately NOT pushed over OTLP: they are
// operator-facing process diagnostics, and a Prometheus scrape is how they are
// consumed. The kubescrape_* metrics default to the OTLP push (Registry.Run);
// the callers enable them here exactly when that push is off
// (-self-metrics-interval=0), so one knob selects the delivery modality and
// the two paths never double-deliver. Serving them is safe alongside a
// hypothetical concurrent push because Dump touches none of the interval
// state the exporter depends on.
func RuntimeHandler(internal bool) http.Handler {
	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector())
	reg.MustRegister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	if internal {
		reg.MustRegister(registryCollector{})
	}
	return promhttp.HandlerFor(reg, promhttp.HandlerOpts{})
}

// registryCollector bridges the OTLP-push Registry onto a Prometheus scrape as
// const metrics, via metrics.Registry.Dump (point-in-time, non-mutating). An
// unchecked collector: the label sets are data-driven, so there is nothing
// useful to Describe.
type registryCollector struct{}

func (registryCollector) Describe(chan<- *prometheus.Desc) {}

func (registryCollector) Collect(ch chan<- prometheus.Metric) {
	for _, s := range Registry.Dump() {
		for _, p := range s.Points {
			names := make([]string, 0, len(p.Labels))
			values := make([]string, 0, len(p.Labels))
			for _, kv := range p.Labels {
				names = append(names, kv[0])
				values = append(values, kv[1])
			}
			desc := prometheus.NewDesc(s.Name, s.Desc, names, nil)
			var m prometheus.Metric
			var err error
			switch s.Kind {
			case metrics.CounterType:
				m, err = prometheus.NewConstMetric(desc, prometheus.CounterValue, p.Value, values...)
			case metrics.GaugeType:
				m, err = prometheus.NewConstMetric(desc, prometheus.GaugeValue, p.Value, values...)
			case metrics.HistogramType:
				buckets := make(map[float64]uint64, len(s.Bounds))
				for i, b := range s.Bounds {
					buckets[b] = p.Buckets[i]
				}
				m, err = prometheus.NewConstHistogram(desc, p.Count, p.Sum, buckets, values...)
			default:
				continue
			}
			if err == nil {
				ch <- m
			}
		}
	}
}

// ServeMetrics starts a dedicated HTTP listener serving RuntimeHandler on
// /metrics, shutting down when ctx is done. internal additionally serves the
// kubescrape_* Registry metrics (callers pass "the OTLP self-metrics push is
// disabled"). A separate port from the health and debug surface: the scrape
// target is what a Prometheus/ServiceMonitor setup points at, while /debug and
// /readyz are operator-facing and often reachable only inside the cluster. An
// empty addr disables it.
func ServeMetrics(ctx context.Context, addr string, internal bool, log *slog.Logger) func() {
	if addr == "" {
		return func() {}
	}
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", RuntimeHandler(internal))
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
