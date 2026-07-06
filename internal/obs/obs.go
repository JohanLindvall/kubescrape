// Package obs holds the internal metrics of both binaries and the HTTP
// handler exposing them, so kubescrape's own health is observable like any
// other workload (scrape it via the usual prometheus.io annotations).
package obs

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Handler serves the internal metrics in Prometheus text format.
func Handler() http.Handler {
	return promhttp.Handler()
}

// Log pipeline (agent).
var (
	LogEntries = promauto.NewCounter(prometheus.CounterOpts{
		Name: "kubescrape_log_entries_total",
		Help: "Log entries exported.",
	})
	LogBytes = promauto.NewCounter(prometheus.CounterOpts{
		Name: "kubescrape_log_bytes_total",
		Help: "Raw log bytes read.",
	})
	LogExportFailures = promauto.NewCounter(prometheus.CounterOpts{
		Name: "kubescrape_log_export_failures_total",
		Help: "Log batch exports that failed after retries (files rewound).",
	})
	LogFiles = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "kubescrape_log_files",
		Help: "Log files currently tracked.",
	})
	LogRotations = promauto.NewCounter(prometheus.CounterOpts{
		Name: "kubescrape_log_rotations_total",
		Help: "Log file rotations and truncations handled.",
	})
)

// Scrape pipeline (agent).
var (
	Scrapes = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "kubescrape_scrapes_total",
		Help: "Scrapes by pipeline and outcome.",
	}, []string{"pipeline", "outcome"})
	ScrapeDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "kubescrape_scrape_duration_seconds",
		Help:    "Scrape duration by pipeline.",
		Buckets: prometheus.DefBuckets,
	}, []string{"pipeline"})
	ScrapeSamples = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "kubescrape_scrape_samples_total",
		Help: "Samples parsed by pipeline (before filtering).",
	}, []string{"pipeline"})
)

// OTLP exporter (agent).
var (
	Exports = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "kubescrape_export_requests_total",
		Help: "OTLP export attempts by signal and outcome.",
	}, []string{"signal", "outcome"})
)

// Metadata client (agent).
var (
	MetadataRequests = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "kubescrape_metadata_requests_total",
		Help: "Requests to the metadata service by outcome.",
	}, []string{"outcome"})
)

// Journald input (agent).
var (
	JournalEntries = promauto.NewCounter(prometheus.CounterOpts{
		Name: "kubescrape_journal_entries_total",
		Help: "Journal entries exported.",
	})
	JournalRestarts = promauto.NewCounter(prometheus.CounterOpts{
		Name: "kubescrape_journal_restarts_total",
		Help: "journalctl subprocess restarts.",
	})
)

// Events exporter (metadata service).
var (
	EventsExported = promauto.NewCounter(prometheus.CounterOpts{
		Name: "kubescrape_events_exported_total",
		Help: "Kubernetes events exported as OTLP logs.",
	})
)

// HTTP server (metadata service).
var (
	HTTPRequests = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "kubescrape_http_requests_total",
		Help: "Metadata API requests by pattern and status code.",
	}, []string{"pattern", "code"})
)

// RegisterStoreStats exposes store sizes as gauges.
func RegisterStoreStats(stats func() (pods, containers int)) {
	promauto.NewGaugeFunc(prometheus.GaugeOpts{
		Name: "kubescrape_store_pods",
		Help: "Pods currently in the store (including tombstones).",
	}, func() float64 { pods, _ := stats(); return float64(pods) })
	promauto.NewGaugeFunc(prometheus.GaugeOpts{
		Name: "kubescrape_store_containers",
		Help: "Container IDs currently indexed (including tombstones).",
	}, func() float64 { _, containers := stats(); return float64(containers) })
}
