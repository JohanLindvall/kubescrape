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
		"Raw log bytes read from live files and archives. Segment replays (re-reading a rotated file's owed range after a restart or rewind) are not re-counted.")
	LogExportFailures = Registry.Counter("kubescrape_log_export_failures_total",
		"Log batch exports that failed after retries (files rewound).")
	// LogPermanentDropped is the tailer's counterpart to the other producers'
	// permanent-rejection drops. Retrying a definitive rejection cannot
	// succeed, and because one sweep goroutine serves every file on the node,
	// retrying it forever stops ALL log shipping there — so the batch is
	// dropped and the offsets advance. That is real data loss and must be
	// alertable.
	//
	// With -buffer-dir the tailer's Export returns the ENQUEUE verdict, not
	// the collector's, so on that (documented, durable) configuration this
	// counter moves only for a batch larger than the whole buffer cap; the
	// collector's own permanent rejections land on
	// kubescrape_buffer_dropped_total{signal="logs"} instead, which is where
	// an alert for the buffered chain belongs.
	LogPermanentDropped = Registry.Counter("kubescrape_log_permanent_dropped_total",
		"Log records dropped after a definitive collector rejection (retrying could not succeed; offsets advanced so the pipeline survives).")
	LogFiles = Registry.Gauge("kubescrape_log_files",
		"Log files currently tracked.")
	LogRotations = Registry.Counter("kubescrape_log_rotations_total",
		"Log file rotations and truncations handled.")
	LogPrefixLost = Registry.Counter("kubescrape_log_prefix_lost_total",
		"Rotated-away log segments that could not be re-read (the file was deleted or compressed "+
			"before its lines were exported, and no open fd survived a restart). These lines are lost.")
	LogEnriched = Registry.CounterVec("kubescrape_log_enriched_total",
		"Log records by the enrichment strategy that matched (json, logfmt, pattern, none).", "format")
	LogLagBytes = Registry.Gauge("kubescrape_log_lag_bytes",
		"Largest per-file backlog: bytes on disk not yet exported and committed (per-file breakdown on /debug/tailer).")
	LogLagBytesTotal = Registry.Gauge("kubescrape_log_lag_total_bytes",
		"Total backlog across tracked files: bytes on disk not yet exported and committed.")
	LogRateLimited = Registry.CounterVec("kubescrape_log_rate_limited_total",
		"Per-file line rate limit hits: lines discarded (action=drop) or reads paused (action=pause).", "action")
	LogRulesDropped = Registry.Counter("kubescrape_log_rules_dropped_total",
		"Log records dropped by the logs rules (including sampled-away lines).")
	// MonitorFieldsIgnored counts ServiceMonitor/PodMonitor upserts carrying
	// endpoint fields kubescrape does not interpret — the metric form of the
	// startup warning, so a partially-applied CR is alertable and not just
	// visible in one pod's logs.
	MonitorFieldsIgnored = Registry.CounterVec("kubescrape_monitor_fields_ignored_total",
		"Monitor upserts whose endpoints set fields kubescrape does not interpret.", "kind")
	// MonitorParseErrors counts ServiceMonitor/PodMonitor upserts that failed
	// to parse. This is the SEVERE sibling of MonitorFieldsIgnored: a parse
	// failure removes the monitor from the index, so every target it
	// contributed disappears. It had only a log line until now, which left
	// the worse outcome unalertable while the milder one was counted.
	MonitorParseErrors = Registry.CounterVec("kubescrape_monitor_parse_errors_total",
		"Monitor upserts that failed to parse and were dropped from the index.", "kind")

	// BufferTruncated counts bytes the disk buffer lost to damage discovered
	// at OPEN (truncated tails, dropped or foreign segments — diskqueue's
	// open-time loss counters). A crash mid-append costs one torn record;
	// anything larger means corruption cost fsynced records.
	BufferTruncated = Registry.CounterVec("kubescrape_buffer_truncated_bytes_total",
		"Bytes the disk buffer lost to damage discovered at open (truncated, dropped or foreign segments).", "signal")

	BufferDropped = Registry.CounterVec("kubescrape_buffer_dropped_total",
		"Buffered batches dropped after a permanent collector rejection (bad payload, auth, unimplemented).", "signal")
	BufferRequeued = Registry.CounterVec("kubescrape_buffer_requeued_total",
		"Buffered batches moved to the back of the queue after repeated transient failures (keeps one stuck batch from blocking the signal).", "signal")
	BufferFull = Registry.CounterVec("kubescrape_buffer_full_total",
		"Batches the disk buffer refused: the undelivered backlog is at its cap, or one batch exceeds the whole cap. Back-pressure for logs (the tailer rewinds and re-reads), a lost batch for producers that cannot rewind (scrape, self-metrics, log-metrics).", "signal")
	// BufferEnqueueErrors counts write-side refusals that are NOT capacity:
	// a latched fsync failure, a closed queue, ENOSPC from segment
	// preallocation. For a producer that cannot rewind (scrape, self-metrics,
	// log-metrics) the batch is gone, and every other buffer metric stays flat
	// while it happens.
	BufferEnqueueErrors = Registry.CounterVec("kubescrape_buffer_enqueue_errors_total",
		"Batches the disk buffer refused for a reason other than capacity (I/O error, closed queue, no space left on device).", "signal")
	BufferReadErrors = Registry.CounterVec("kubescrape_buffer_read_errors_total",
		"Disk-buffer read failures while draining. lost=true is reported corruption the queue advanced past (its Stats carry the magnitude); lost=false left the queue in place for a retry.", "signal", "lost")
	LogFifoDropped = Registry.Counter("kubescrape_log_fifo_orphans_total",
		"Stale per-line offset entries discarded because the multiline stage dropped over-limit lines it never emitted.")
	PositionsCorrupt = Registry.Counter("kubescrape_positions_corrupt_total",
		"Positions files that failed to parse at startup (whatever decoded is kept; the affected inputs re-read "+
			"their window). Recurring bumps across restarts point at a failing disk, not a one-off crash.")
	LogUnresolvedLost = Registry.Counter("kubescrape_log_unresolved_lost_total",
		"Log files deleted before their metadata ever resolved (the metadata service was unreachable "+
			"or the container unknown for the file's whole life). Their content was never read and is lost.")
	LogOversizedDropped = Registry.Counter("kubescrape_log_oversized_dropped_total",
		"Unterminated lines discarded for exceeding the per-entry size bound (no newline within MaxEntryBytes+4096).")
	LogTornFinalLines = Registry.Counter("kubescrape_log_torn_final_lines_total",
		"Unterminated final lines of RENAMED-away files (the fragment can never complete and is dropped). In-place truncation destroys its unread tail unmeasurably — there is nothing left to count — so truncation losses do not appear here or anywhere.")
	LogScrubbed = Registry.CounterVec("kubescrape_log_scrubbed_total",
		"Log bodies redacted by a scrub pattern (one bump per pattern per record, not per match).", "pattern")
	LogArchiveErrors = Registry.Counter("kubescrape_log_archive_errors_total",
		"Compressed log files whose stream failed to decode mid-read (truncated gzip, trailing garbage). "+
			"What decoded before the error is delivered; the remainder is unrecoverable and the archive settles.")
)

// Scrape pipeline (agent).
var (
	Scrapes = Registry.CounterVec("kubescrape_scrapes_total",
		"Scrapes by pipeline and outcome.", "pipeline", "outcome")
	ScrapeDuration = Registry.HistogramVec("kubescrape_scrape_duration_seconds",
		"Scrape duration by pipeline.", nil, "pipeline")
	ScrapeSamples = Registry.CounterVec("kubescrape_scrape_samples_total",
		"Samples parsed by pipeline (before filtering).", "pipeline")
	ScrapeMalformed = Registry.CounterVec("kubescrape_scrape_malformed_total",
		"Exposition samples dropped as malformed by pipeline (unparseable lines, histogram buckets without le, summary rows without quantile).", "pipeline")
	ScrapeCollisions = Registry.Counter("kubescrape_scrape_name_collisions_total",
		"Data points dropped because their family name was already claimed by a metric of another shape in the same batch (a target redeclaring a family's TYPE mid-exposition).")
)

// OTLP exporter (agent).
var (
	Exports = Registry.CounterVec("kubescrape_export_requests_total",
		"OTLP export attempts by signal and outcome.", "signal", "outcome")
)

// OTLP ingest (agent).
var (
	// IngestRejected counts pushes refused for exceeding the in-flight bound.
	// They are retryable and the sender still holds the payload, but a
	// persistently non-zero rate means the node cannot keep up with what is
	// being pushed at it.
	IngestRejected = Registry.Counter("kubescrape_ingest_rejected_total",
		"Pushed OTLP requests refused because the concurrent in-flight bound was reached (retryable: 429 / ResourceExhausted).")
)

// Metadata client (agent).
var (
	MetadataRequests = Registry.CounterVec("kubescrape_metadata_requests_total",
		"Requests to the metadata service by outcome.", "outcome")
)

// SelfMetadataLookups counts the agent's own-pod lookups by outcome, SEPARATELY
// from kubescrape_metadata_requests_total.
//
// That counter is documented as the container-attribution health signal, and
// the self lookup retries forever: a fleet where /v1/self cannot resolve
// (hostNetwork, a NAT hop) would otherwise contribute a permanent stream of
// not_found to it and fire an alert about an attribution problem that does not
// exist. Separation is not achieved by this counter alone — the agent gives
// the self lookup its OWN metaclient.Client, without the Observe hook, since
// the hook is per-client and fires inside every fetch (main.go). Counting
// here and observing there would have double-counted the outcome into the
// very metric the split exists to keep clean.
var SelfMetadataLookups = Registry.CounterVec("kubescrape_self_metadata_lookups_total",
	"Own-pod metadata lookups for -self-attributes, by outcome.", "outcome")

// RegisterSelfMetadata exposes whether this process has resolved the pod it
// runs in, whose attributes it stamps on the metrics it generates about itself
// (-self-attributes). Both binaries register it whenever the lookup RUNS, and
// only then: a registered gauge means "this process is trying", so a 0 always
// means unresolved and never "the feature is off".
//
// Without it an unattributed process is invisible: the agent's failed lookups
// were indistinguishable from any other in kubescrape_metadata_requests_total,
// and the metadata service's own lookup touches no counter at all — so "my
// agents' own metrics carry no pod" is unalertable, and the symptom (a missing
// label on one job) is easy to read as a dashboard problem.
func RegisterSelfMetadata(resolved func() bool) {
	Registry.GaugeFunc("kubescrape_self_metadata_resolved",
		"1 when this process has resolved its own pod's metadata for -self-attributes, 0 while it has not.",
		func() float64 {
			if resolved() {
				return 1
			}
			return 0
		})
}

// Journald input (agent).
var (
	JournalEntries = Registry.Counter("kubescrape_journal_entries_total",
		"Journal entries exported.")
	JournalRestarts = Registry.Counter("kubescrape_journal_restarts_total",
		"Journal reader restarts.")
	JournalTruncated = Registry.Counter("kubescrape_journal_truncated_total",
		"Journal messages truncated at MaxEntryBytes (the record carries log.truncated).")
)

// OTLP ingest (agent).
var (
	Ingested = Registry.CounterVec("kubescrape_ingest_resources_total",
		"Distinct pushed identities (container id / pod uid, memoized per request) by enrichment outcome. enriched = an id resolved; peer_ip = no id, attributed by the connection's source address; peer_ip_rejected = that address resolved to the RECEIVER's own workload, so it was rewritten in flight (a proxy, a mesh sidecar, or an internal hop addressed to the application port) and nothing was attributed — anything above zero means peer-IP attribution cannot work on that path; unresolved = nothing identified the sender.", "outcome")
	Routed = Registry.CounterVec("kubescrape_routed_payload_parts_total",
		"Payload parts forwarded to a non-default routing destination.", "route", "signal")
	TransformErrors = Registry.CounterVec("kubescrape_transform_errors_total",
		"Transform program invocations that failed (the batch is NOT exported; the error propagates to the producer's retry path).", "signal")
	TransformReloads = Registry.CounterVec("kubescrape_transform_reloads_total",
		"Transforms-file reloads by outcome (applied, failed — a failed compile keeps the last good program).", "outcome")
	TraceSpansDropped = Registry.CounterVec("kubescrape_trace_spans_dropped_total",
		"Ingested spans dropped by the trace sampler (probability = the consistent trace-ID decision, rate = the spans/second cap).", "reason")
	SpanMetricsDropped = Registry.Counter("kubescrape_span_metrics_dropped_total",
		"Spans not aggregated into span metrics because the dimension-cardinality cap was reached.")
	SpanMetricsEvicted = Registry.Counter("kubescrape_span_metrics_evicted_total",
		"Span-metric series dropped at export because their dimensions went unobserved for traceMetrics.staleAfter (this is what frees cardinality-cap slots).")
)

// Service-graph edges (the -service-graph shard). Every one of these counts a
// place where the graph is INCOMPLETE, and each names the config bound that
// caused it, because all three bounds trade completeness for memory and an
// operator has to be able to see which one is binding: an edge missing from
// Grafana's Service Graph view looks exactly like a call that never happened.
var (
	ServiceGraphDropped = Registry.Counter("kubescrape_service_graph_dropped_total",
		"Edges not aggregated because the serviceGraph.maxCardinality series cap was reached (existing edges keep reporting; a new one is lost until eviction frees a slot).")
	ServiceGraphEvicted = Registry.Counter("kubescrape_service_graph_evicted_total",
		"Edge series dropped at export because they went unobserved for serviceGraph.staleAfter (this is what frees cardinality-cap slots).")
	// ServiceGraphStoreFull is the pairing store's back-pressure. It is bumped
	// per SPAN, not per edge: the span is never stored, so its partner will
	// later expire unpaired too, and both counters move for one lost request.
	ServiceGraphStoreFull = Registry.Counter("kubescrape_service_graph_store_full_total",
		"Spans dropped because the pairing store held serviceGraph.maxItems half-edges (the request cannot become an edge).")
	// ServiceGraphExpired is the OTHER shape of an incomplete graph, and a
	// non-zero baseline is normal: a client half whose server is uninstrumented
	// expires by design (and may then become a virtual-node edge). A RISING rate
	// means serviceGraph.wait is shorter than the real client-to-server span
	// delivery gap, or the two halves are landing on different shards.
	ServiceGraphExpired = Registry.Counter("kubescrape_service_graph_expired_total",
		"Half-edges that expired after serviceGraph.wait without their partner arriving.")
)

// RegisterServiceGraphStats publishes the pairing store's own numbers on the
// shard (-service-graph): what pairing ACHIEVED, plus the one leading
// indicator the counters above cannot give.
//
// The counters above all move only once the graph has already lost something.
// The pending gauge is the backlog against serviceGraph.maxItems — "the store
// is at 80% and climbing" — the same reason RegisterBufferStats exists beside
// the disk buffer's drop counters. Completed is what makes the rest readable:
// an expiry rate means nothing without the pairing rate it is a fraction of.
//
// Published through a registration function because the values are owned by
// servicegraph's store, which obs cannot reach — that package imports obs, so
// the dependency runs one way only. ServiceGraphStat mirrors
// servicegraph.Stats for exactly that reason.
//
// Stats.Dropped and Stats.Unpaired are deliberately NOT published here: the
// first is already kubescrape_service_graph_store_full_total, and the second is
// kubescrape_service_graph_expired_total minus the virtual-node counter below
// (every expiry goes exactly one of those two ways). A second series carrying a
// number two others already determine is one more thing to keep consistent.
func RegisterServiceGraphStats(stats func() ServiceGraphStat) {
	Registry.GaugeFunc("kubescrape_service_graph_pending_edges",
		"Half-edges currently awaiting their partner. serviceGraph.maxItems caps this; at the cap spans are refused (see kubescrape_service_graph_store_full_total), so the ratio against the cap is the leading indicator.",
		func() float64 { return float64(stats().Pending) })
	Registry.CounterFunc("kubescrape_service_graph_completed_total",
		"Edges completed by BOTH halves arriving within serviceGraph.wait. The denominator for the loss counters: an expiry or drop rate only means something against the rate of pairings that worked.",
		func() float64 { return float64(stats().Completed) })
	Registry.CounterFunc("kubescrape_service_graph_virtual_node_total",
		"Half-edges that expired unpaired but named their far side through serviceGraph.virtualNodePeerAttributes, and so still reached the graph (as a virtual-node edge). The remainder of kubescrape_service_graph_expired_total is the genuinely lost part — nothing named the missing side, so that request is on no edge at all.",
		func() float64 { return float64(stats().VirtualNode) })
	Registry.CounterFunc("kubescrape_service_graph_unkeyable_total",
		"Spans that could not be keyed for pairing: no trace id, or a client/producer span with no span id of its own. Never stored — every zero id shares one key space, so they would cross-pair unrelated requests into invented edges (a zero-id client span keys exactly where every ROOT SERVER span of its trace does). A moving rate means an SDK is emitting malformed spans.",
		func() float64 { return float64(stats().Unkeyable) })
}

// ServiceGraphStat is the pairing store's snapshot (servicegraph.Stats' shape).
type ServiceGraphStat struct {
	Pending     int
	Completed   uint64
	VirtualNode uint64
	Unkeyable   uint64
}

// RegisterServiceGraphResharder publishes the tier's INTERNAL hop: how a shard
// that received an application push distributed those spans to the shards that
// own their traces.
//
// These are the numbers only the resharder knows. The wire outcome of each hop
// lands in kubescrape_export_requests_total{signal="traces"} together with the
// ordinary collector exports, where a hop failure is indistinguishable from a
// collector failure — and the two mean very different things: one is a shard
// that cannot be reached, the other is a destination outside the cluster.
//
// A failed hop is NOT silent, unlike the best-effort side channel this replaced:
// the entry shard holds the only copy of a pushed span, so it refuses the
// application's push and the sender retries. SendsFailed therefore moves in step
// with the senders' own error rate.
func RegisterServiceGraphResharder(stats func() ServiceGraphReshardStat) {
	Registry.CounterFunc("kubescrape_service_graph_spans_forwarded_total",
		"Spans this shard handed to ANOTHER shard because that shard owns their trace, and which it accepted. Roughly (N-1)/N of everything pushed to this pod on an N-shard tier: the remainder is kubescrape_service_graph_spans_local_total. This is the tier's internal bandwidth.",
		func() float64 { return float64(stats().SpansForwarded) })
	Registry.CounterFunc("kubescrape_service_graph_spans_local_total",
		"Spans this shard already owned and kept in-process, taking no second hop. Its ratio against the forwarded count should track 1/N for an N-shard tier; a persistent skew means the ring is unbalanced or one shard is receiving most of the pushes.",
		func() float64 { return float64(stats().SpansLocal) })
	Registry.CounterFunc("kubescrape_service_graph_spans_unkeyed_total",
		"Pushed spans with no trace id. They cannot be hashed onto the ring, so they are kept locally and exported from here; they can never pair into an edge. A moving rate means an SDK is emitting malformed spans.",
		func() float64 { return float64(stats().SpansUnkeyed) })
	Registry.CounterFunc("kubescrape_service_graph_sends_failed_total",
		"Failed internal hops (one per owning shard per batch). Every one of them FAILS the application's push — the entry shard holds the only copy of those spans — so this moves together with the senders' export errors, and a rate that tracks one shard means that shard is down or its token is wrong.",
		func() float64 { return float64(stats().SendsFailed) })
	Registry.CounterFunc("kubescrape_service_graph_loops_blocked_total",
		"Spans in application pushes refused for carrying the internal forwarded marker: an internal hop addressed to the tier's APPLICATION port instead of its authenticated receiver, which without the refusal would re-enrich and re-shard every span on every hop until the network is the incident. Always zero in a correct deployment; anything else is a config error to fix now.",
		func() float64 { return float64(stats().LoopsBlocked) })
}

// ServiceGraphReshardStat is the resharder's snapshot
// (servicegraph.ReshardStats' shape; see RegisterServiceGraphStats on why obs
// declares its own).
type ServiceGraphReshardStat struct {
	SpansForwarded uint64
	SpansLocal     uint64
	SpansUnkeyed   uint64
	SendsFailed    uint64
	LoopsBlocked   uint64
}

// Journald drops (agent).
var (
	JournalDropped = Registry.Counter("kubescrape_journal_dropped_batches_total",
		"Journal batches dropped after a permanent collector rejection (the cursor advances past them).")
)

// Kubernetes events (the cluster-singleton events collector).
var (
	Leader = Registry.Gauge("kubescrape_leader",
		"1 while this replica holds the cluster-singleton lease, 0 otherwise; sum != 1 means split brain or nobody leading.")
	EventsObserved = Registry.CounterVec("kubescrape_events_observed_total",
		"Kubernetes events received from the watch, by event type (normal, warning, other — anything else the API server reports).", "type")
	EventsExported = Registry.Counter("kubescrape_events_exported_total",
		"Kubernetes event records exported (after the rules).")
	EventsDropped = Registry.Counter("kubescrape_events_dropped_total",
		"Kubernetes event batches dropped after a permanent collector rejection (the position advances past them).")
	EventWatchRestarts = Registry.Counter("kubescrape_event_watch_restarts_total",
		"Event watch restarts (a closed stream, an error, or an expired resourceVersion).")
	EventRelists = Registry.CounterVec("kubescrape_event_relists_total",
		"Event watches that fell back to a relist because the stored resourceVersion had aged out of the API server's watch window.", "stage")
	EventPositionErrors = Registry.CounterVec("kubescrape_event_position_errors_total",
		"Failures reading or writing the event position ConfigMap, by operation (load, save).", "operation")
)

// Azure diagnostics (the Event Hubs consumer in the cluster-singleton deployment).
var (
	AzureRecords = Registry.CounterVec("kubescrape_azure_records_total",
		"Azure diagnostic records decoded from Event Hubs messages, by kind (log, metric).", "kind")
	AzureDecodeErrors = Registry.Counter("kubescrape_azure_decode_errors_total",
		"Event Hubs messages or records that could not be decoded as Azure diagnostics JSON (skipped, committed past).")
	AzureExported = Registry.CounterVec("kubescrape_azure_exported_total",
		"Azure diagnostic records exported, by signal (logs, metrics).", "signal")
	AzureDropped = Registry.Counter("kubescrape_azure_dropped_total",
		"Azure diagnostic payloads dropped after a permanent collector rejection (the offsets advance past them).")
	AzureFetchErrors = Registry.Counter("kubescrape_azure_fetch_errors_total",
		"Kafka fetch errors from the Event Hubs consumer (retried; partial fetches are still processed).")
	AzureCommitErrors = Registry.Counter("kubescrape_azure_commit_errors_total",
		"Offset commit failures (the records were delivered; a redelivery produces at-least-once duplicates).")
	AzureTokenRefreshes = Registry.CounterVec("kubescrape_azure_token_refreshes_total",
		"Microsoft Entra token refreshes for the Event Hubs connection, by outcome (ok, error).", "outcome")
)

// HTTP server (metadata service).
var (
	HTTPRequests = Registry.CounterVec("kubescrape_http_requests_total",
		"Metadata API requests by pattern and status code.", "pattern", "code")
)

// Log-derived metrics (agent): observations the series store refused. The
// counters live in internal/metrics (which obs imports, so they cannot be
// declared here) and are surfaced as export-time gauges — cumulative since
// process start.
func init() {
	Registry.CounterFunc("kubescrape_log_metrics_dropped_capped_total",
		"Log-metric observations dropped since start because the metric's label-set cardinality cap was reached.",
		func() float64 { return float64(metrics.DroppedCapped()) })
	// Per metric as well: the cap frees slots only through idleness, so a burst
	// blinds ONE metric for up to maxAge + grace (24h by default) and an alert
	// has to be able to name it.
	//
	// It is a GAUGE carrying a since-start total, not a counter — the per-metric
	// label set is data-driven (a metric name appears only once it has dropped
	// something), which the func-gauge vec is what can express. The value is
	// still monotonic within a process, so `increase()`/`rate()` over it do NOT
	// see the reset at a restart the way they do for the aggregate counter
	// sibling: compare it against itself over a window, or alert on `> 0`, and
	// use kubescrape_log_metrics_dropped_capped_total for rates.
	Registry.GaugeFuncVec("kubescrape_log_metrics_dropped_capped_by_metric",
		"Log-metric observations dropped since start because that metric's cardinality cap was reached, by metric name. A gauge carrying a since-start total (not a counter): it does not mark restarts, so use kubescrape_log_metrics_dropped_capped_total for rates and this one to name the metric.",
		"metric", metrics.DroppedCappedByMetric)
	Registry.CounterFunc("kubescrape_log_metrics_dropped_collision_total",
		"Log-metric observations dropped since start because of a series hash collision.",
		func() float64 { return float64(metrics.DroppedCollision()) })
	Registry.CounterFunc("kubescrape_log_metrics_dropped_nan_total",
		"Log-metric observations dropped since start because the extracted value was NaN or +/-Inf (neither is representable as a sample).",
		func() float64 { return float64(metrics.DroppedNaN()) })
}

// RegisterStoreStats exposes store sizes as gauges evaluated at export time.
func RegisterStoreStats(stats func() (pods, containers int)) {
	Registry.GaugeFunc("kubescrape_store_pods",
		"Pods currently in the store (including tombstones).",
		func() float64 { pods, _ := stats(); return float64(pods) })
	Registry.GaugeFunc("kubescrape_store_containers",
		"Container IDs currently indexed (including tombstones).",
		func() float64 { _, containers := stats(); return float64(containers) })
}

// RegisterBufferStats exposes the disk buffer's per-signal backlog as gauges
// evaluated at export time.
//
// Every other buffer metric is a counter that only moves once data is ALREADY
// being dropped or refused (dropped/full/read_errors): by then the collector
// has been degrading for a while and the tailer is back-pressured. The backlog
// against its cap is the leading indicator — "the spool is at 70% and
// climbing" — and it was previously unobservable even though the spool had
// tracked the number all along.
func RegisterBufferStats(stats func() map[string]BufferStat) {
	Registry.GaugeFuncVec("kubescrape_buffer_backlog_bytes",
		"Undelivered bytes currently queued in the disk buffer, per signal (what -buffer-max-bytes caps).",
		"signal", func() map[string]float64 {
			out := map[string]float64{}
			for sig, st := range stats() {
				out[sig] = float64(st.Backlog)
			}
			return out
		})
	Registry.GaugeFuncVec("kubescrape_buffer_max_bytes",
		"Configured disk-buffer cap per signal (0 = uncapped); backlog/max is the utilisation to alert on.",
		"signal", func() map[string]float64 {
			out := map[string]float64{}
			for sig, st := range stats() {
				out[sig] = float64(st.Cap)
			}
			return out
		})
	Registry.GaugeFuncVec("kubescrape_buffer_segments",
		"Disk-buffer segment files on disk per signal. Physical footprint can exceed the backlog by up to one segment (a delivered but unreclaimed prefix).",
		"signal", func() map[string]float64 {
			out := map[string]float64{}
			for sig, st := range stats() {
				out[sig] = float64(st.Segments)
			}
			return out
		})
}

// BufferStat is one signal's disk-buffer occupancy.
type BufferStat struct {
	Backlog  int64
	Cap      int64
	Segments int
}
