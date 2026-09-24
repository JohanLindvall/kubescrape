package obs

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/http/pprof"
	"sync"
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
		reg.MustRegister(registryCollector{reg: Registry})
	}
	return promhttp.HandlerFor(reg, promhttp.HandlerOpts{})
}

// registryCollector bridges an OTLP-push Registry onto a Prometheus scrape as
// const metrics, via metrics.Registry.Dump (point-in-time, non-mutating). An
// unchecked collector: the label sets are data-driven, so there is nothing
// useful to Describe. reg is the registry it serves — always the process's
// Registry in production; a field rather than the global so a test can drive
// the refusal arm on a private registry instead of leaving a deliberately
// broken series registered on the one every test in this package shares.
type registryCollector struct{ reg *metrics.Registry }

func (registryCollector) Describe(chan<- *prometheus.Desc) {}

func (c registryCollector) Collect(ch chan<- prometheus.Metric) {
	for _, s := range c.reg.Dump() {
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
				// Dump only ever reports the three kinds above (metrics.kindType
				// refuses the rest), so this is a new kind that reached the
				// bridge without a case here — a point vanishing from the
				// exposition for a reason nothing else records.
				err = fmt.Errorf("no Prometheus mapping for series kind %q", s.Kind)
			}
			if err != nil {
				// NOT silently dropped. This is the /metrics exposition, which
				// with -self-metrics-interval=0 is the ONLY delivery path for
				// every kubescrape_* metric, so a discarded point is a series
				// simply absent from the response and every alert written over
				// it matching nothing — the same invisible shrinkage
				// metrics.Registry already counts one layer down, on the same
				// counter (kubescrape_self_metrics_points_skipped_total).
				//
				// What can refuse: NewDesc an invalid metric or label NAME
				// (every one here is code-defined kubescrape_*, so that is a
				// bug); NewConstMetric/NewConstHistogram a label-count
				// mismatch (also a bug) AND a label VALUE that is not valid
				// UTF-8. The last is reachable: most values are code-defined
				// too, but some are operator-supplied strings — a readiness
				// gate named after a flag value, and argv is not guaranteed
				// to be UTF-8 — and nothing below this bridge validates one
				// (metrics.truncLabelCut even keeps an already-invalid value's
				// bytes rather than dropping the label). Either way the point
				// is counted and named rather than silently absent.
				c.reg.NoteSkippedPoint(s.Name, err)
				continue
			}
			ch <- m
		}
	}
}

// ServeMetrics starts a dedicated HTTP listener serving RuntimeHandler on
// /metrics, shut down by the returned func, which the caller must call.
// It takes no context deliberately — see stopper. internal additionally serves the
// kubescrape_* Registry metrics (callers pass "the OTLP self-metrics push is
// disabled"). A separate port from the health and debug surface: the scrape
// target is what a Prometheus/ServiceMonitor setup points at, while /debug and
// /readyz are operator-facing and often reachable only inside the cluster. An
// empty addr disables it.
func ServeMetrics(addr string, internal bool, log *slog.Logger) (func(), error) {
	if addr == "" {
		return func() {}, nil
	}
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", RuntimeHandler(internal))
	// Bounded like the agent's debug server: this port is open to every pod
	// (the chart's NetworkPolicy leaves the scrape port unscoped by design),
	// and a client that opens a connection and then neither finishes its
	// request nor closes holds a file descriptor on a process whose tailer
	// needs one per log file. ReadHeaderTimeout alone bounds only the head.
	srv := &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       120 * time.Second,
	}
	return serve(srv, "metrics", "/metrics", log)
}

// serve binds srv.Addr and serves srv on it in the background, returning its
// stopper. The endpoint's name ("metrics", "pprof") and path only label the
// error and the lines.
//
// The bind is SYNCHRONOUS so a failure reaches the caller. It used to be logged
// from inside the goroutine and dropped: with -self-metrics-interval=0 — the
// documented way to choose the scrape modality over the OTLP push — the metrics
// port is the ONLY delivery path for every kubescrape_* metric, and the chart's
// prometheus.io annotations keep pointing at a port that never opened. A dead
// ingest listener is fatal for exactly this reason.
func serve(srv *http.Server, name, path string, log *slog.Logger) (func(), error) {
	ln, err := net.Listen("tcp", srv.Addr)
	if err != nil {
		return func() {}, fmt.Errorf("%s endpoint %s: %w", name, srv.Addr, err)
	}
	go func() {
		log.Info(name+" endpoint started", "addr", srv.Addr, "path", path)
		if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error(name+" endpoint failed", "addr", srv.Addr, "error", err)
		}
	}()
	return stopper(srv), nil
}

// ListenerShutdownTimeout bounds each ServeMetrics/ServePprof stopper's
// Shutdown. Exported because both binaries defer those stoppers AFTER their own
// shutdown sequence, so it is part of the termination-grace budget they are
// tested against.
const ListenerShutdownTimeout = 5 * time.Second

// stopper returns an idempotent shutdown func for srv.
//
// It deliberately does NOT arm the shutdown on ctx. Doing that is the obvious
// reading of the doc comments these two listeners have always carried
// ("shutting down when ctx is done"), and it is wrong: ctx is the PROCESS
// signal context, so a context.AfterFunc fires at the SIGTERM instant — t=0 of
// shutdown — and both diagnostic listeners would go dark for the whole
// termination grace, which is 60s on the agent. That is precisely the window in
// which an operator is scraping /metrics to watch the buffer drain, or
// attaching to pprof to find out why shutdown is hanging. The listeners must
// outlive the sequence they exist to observe.
//
// So the returned func is the ONLY shutdown, and both callers defer it — which
// is what actually keeps the listener from leaking. The doc comments say that
// now instead of promising ctx.
func stopper(srv *http.Server) func() {
	return sync.OnceFunc(func() {
		sctx, cancel := context.WithTimeout(context.Background(), ListenerShutdownTimeout)
		defer cancel()
		_ = srv.Shutdown(sctx)
	})
}

// ServePprof starts a dedicated HTTP listener serving net/http/pprof under
// /debug/pprof, shut down by the returned func, which the caller must call.
// It takes no context deliberately — see stopper.
//
// Its own port, separate from both the metrics endpoint and the
// health/debug surface: profiles expose goroutine stacks and heap contents, so
// the port that carries them is the one you firewall, bind to localhost, or
// leave unset. Empty addr disables it.
//
// The bind is synchronous and its failure returned, like ServeMetrics (serve).
// The consequence is milder here — this listener is opt-in, and an unset flag
// produces no log line at all while a bind failure produces exactly one Error,
// so the two states were already distinguishable — but a caller that asked for
// a port and did not get one should not have to read the log to find out.
func ServePprof(addr string, log *slog.Logger) (func(), error) {
	if addr == "" {
		return func() {}, nil
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /debug/pprof/", pprof.Index)
	mux.HandleFunc("GET /debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("GET /debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("GET /debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("GET /debug/pprof/trace", pprof.Trace)
	// No WriteTimeout here: /debug/pprof/profile?seconds=N streams for as long
	// as it was asked to, and a write deadline would cut a 30-second CPU
	// profile short. The read side and idle keep-alives are bounded as on the
	// metrics port — ReadTimeout covers reading the REQUEST, which a profile
	// GET finishes at once, so it neither cancels nor shortens a long profile.
	srv := &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		IdleTimeout:       120 * time.Second,
	}
	return serve(srv, "pprof", "/debug/pprof/", log)
}
