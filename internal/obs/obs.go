// Package obs holds the internal (self-observability) metrics of both
// binaries. They are produced through internal/metrics' Registry and exported
// over OTLP alongside everything else — there is no Prometheus exposition.
// New failure paths should count into an existing metric here or add one.
package obs

import (
	"github.com/JohanLindvall/kubescrape/internal/metrics"
)

// Registry collects every metric below; the binaries export it periodically
// via metrics.Registry.Run with their own resource identity.
var Registry = metrics.NewRegistry()

// Log pipeline (agent).
var (
	LogEntries = Registry.Counter("kubescrape_log_entries_total",
		"Log entries exported.")
	LogBytes = Registry.Counter("kubescrape_log_bytes_total",
		"Raw log bytes read.")
	LogExportFailures = Registry.Counter("kubescrape_log_export_failures_total",
		"Log batch exports that failed after retries (files rewound).")
	LogFiles = Registry.Gauge("kubescrape_log_files",
		"Log files currently tracked.")
	LogRotations = Registry.Counter("kubescrape_log_rotations_total",
		"Log file rotations and truncations handled.")
	LogEnriched = Registry.CounterVec("kubescrape_log_enriched_total",
		"Log records by the enrichment strategy that matched (json, logfmt, pattern, none).", "format")
	LogLagBytes = Registry.Gauge("kubescrape_log_lag_bytes",
		"Largest per-file backlog: bytes on disk not yet exported and committed (per-file breakdown on /debug/tailer).")
	LogLagBytesTotal = Registry.Gauge("kubescrape_log_lag_bytes_sum",
		"Total backlog across tracked files: bytes on disk not yet exported and committed.")
	LogRateLimited = Registry.CounterVec("kubescrape_log_rate_limited_total",
		"Per-file line rate limit hits: lines discarded (action=drop) or reads paused (action=pause).", "action")
	LogRulesDropped = Registry.Counter("kubescrape_log_rules_dropped_total",
		"Log records dropped by the logs rules (including sampled-away lines).")
	BufferDropped = Registry.CounterVec("kubescrape_buffer_dropped_total",
		"Buffered batches dropped after a permanent collector rejection (bad payload, auth, unimplemented).", "signal")
	BufferRequeued = Registry.CounterVec("kubescrape_buffer_requeued_total",
		"Buffered batches moved to the back of the queue after repeated transient failures (keeps one stuck batch from blocking the signal).", "signal")
	BufferReadErrors = Registry.CounterVec("kubescrape_buffer_read_errors_total",
		"Disk-buffer read failures while draining (the head frame could not be read; lost=true means the segment was gone and its frames were skipped).", "signal", "lost")
	LogFifoDropped = Registry.Counter("kubescrape_log_fifo_orphans_total",
		"Stale per-line offset entries discarded because the multiline stage dropped over-limit lines it never emitted.")
)

// Scrape pipeline (agent).
var (
	Scrapes = Registry.CounterVec("kubescrape_scrapes_total",
		"Scrapes by pipeline and outcome.", "pipeline", "outcome")
	ScrapeDuration = Registry.HistogramVec("kubescrape_scrape_duration_seconds",
		"Scrape duration by pipeline.", nil, "pipeline")
	ScrapeSamples = Registry.CounterVec("kubescrape_scrape_samples_total",
		"Samples parsed by pipeline (before filtering).", "pipeline")
)

// OTLP exporter (agent).
var (
	Exports = Registry.CounterVec("kubescrape_export_requests_total",
		"OTLP export attempts by signal and outcome.", "signal", "outcome")
)

// Metadata client (agent).
var (
	MetadataRequests = Registry.CounterVec("kubescrape_metadata_requests_total",
		"Requests to the metadata service by outcome.", "outcome")
)

// Journald input (agent).
var (
	JournalEntries = Registry.Counter("kubescrape_journal_entries_total",
		"Journal entries exported.")
	JournalRestarts = Registry.Counter("kubescrape_journal_restarts_total",
		"Journal reader restarts.")
)

// Events exporter (metadata service).
var (
	EventsExported = Registry.Counter("kubescrape_events_exported_total",
		"Kubernetes events exported as OTLP logs.")
)

// OTLP ingest (agent).
var (
	Ingested = Registry.CounterVec("kubescrape_ingest_resources_total",
		"Distinct pushed identities (container id / pod uid, memoized per request) by enrichment outcome (enriched, unresolved, peer_ip).", "outcome")
	IngestDropped = Registry.CounterVec("kubescrape_ingest_dropped_batches_total",
		"Acknowledged ingest batches dropped: permanent collector rejection or the transient-retry limit exhausted.", "signal")
)

// Journald drops (agent).
var (
	JournalDropped = Registry.Counter("kubescrape_journal_dropped_batches_total",
		"Journal batches dropped after a permanent collector rejection (the cursor advances past them).")
)

// Events drops (metadata service).
var (
	EventsDropped = Registry.Counter("kubescrape_events_dropped_total",
		"Kubernetes events dropped because the export queue was full.")
)

// HTTP server (metadata service).
var (
	HTTPRequests = Registry.CounterVec("kubescrape_http_requests_total",
		"Metadata API requests by pattern and status code.", "pattern", "code")
)

// RegisterStoreStats exposes store sizes as gauges evaluated at export time.
func RegisterStoreStats(stats func() (pods, containers int)) {
	Registry.GaugeFunc("kubescrape_store_pods",
		"Pods currently in the store (including tombstones).",
		func() float64 { pods, _ := stats(); return float64(pods) })
	Registry.GaugeFunc("kubescrape_store_containers",
		"Container IDs currently indexed (including tombstones).",
		func() float64 { _, containers := stats(); return float64(containers) })
}
