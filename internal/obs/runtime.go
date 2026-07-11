package obs

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// RuntimeHandler serves the Go runtime and process metrics (go_*, process_*)
// in Prometheus text format on a dedicated registry. kubescrape's own
// kubescrape_* metrics are NOT here — they are pushed over OTLP via Registry;
// this endpoint exists for debugging the process itself (goroutines, heap,
// GC, fds).
func RuntimeHandler() http.Handler {
	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector())
	reg.MustRegister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	return promhttp.HandlerFor(reg, promhttp.HandlerOpts{})
}
