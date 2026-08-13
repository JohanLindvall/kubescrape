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
		"Log entries exported. With -buffer-dir this counts acceptance into the disk buffer, not collector delivery — reconcile against kubescrape_buffer_dropped_records_total{signal=\"logs\"} for what was later dropped drain-side.")
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
	// kubescrape_buffer_dropped_batches_total{signal="logs"} (with
	// ..._records_total beside it for the magnitude) instead, which is where
	// an alert for the buffered chain belongs.
	LogPermanentDropped = Registry.Counter("kubescrape_log_permanent_dropped_total",
		"Log records dropped after a definitive collector rejection (retrying could not succeed; offsets advanced so the pipeline survives).")
	LogFiles = Registry.Gauge("kubescrape_log_files",
		"Log files currently tracked.")
	LogRotations = Registry.Counter("kubescrape_log_rotations_total",
		"Log file rotations and truncations handled.")
	LogPrefixLost = Registry.Counter("kubescrape_log_prefix_lost_total",
		"Log content given up on as unrecoverable: a rotated-away segment that could not be re-read "+
			"(the file was deleted or compressed before its lines were exported, and no open fd survived "+
			"a restart), a segment or vanished file whose reads kept erring without progress past the "+
			"stall budget, or a compressed archive replaced across a restart before its old stream fully "+
			"shipped. One count per given-up segment/file/archive; these lines are lost.")
	LogEnriched = Registry.CounterVec("kubescrape_log_enriched_total",
		"Log records by the enrichment strategy that matched (json, logfmt, pattern, none).", "format")
	LogEnrichTimeRejected = Registry.Counter("kubescrape_log_enrich_time_rejected_total",
		"Timestamps parsed from a log line that did NOT replace the producer's own (the CRI/journal/event time) "+
			"because the line's timestamp carried no zone. Such a stamp is a wall clock enrichment must read as UTC, "+
			"so a workload running with TZ set to anything else would misdate every record by that offset; the "+
			"accurate ingest time is kept instead. A timestamp that states its own offset (RFC3339, an epoch, any "+
			"zoned layout) always wins, however old it is, and a record with no producer timestamp at all takes the "+
			"parsed one either way.")
	// The two lag gauges were named the other way round: the unqualified
	// kubescrape_log_lag_bytes was the per-file MAXIMUM and the _total_bytes
	// suffix carried the sum. Both are gauges, and _total is reserved for
	// counters — so the name promised a counter, and the name WITHOUT the
	// qualifier was the one that was not the total.
	LogLagMaxBytes = Registry.Gauge("kubescrape_log_lag_max_bytes",
		"Largest per-file backlog: bytes on disk not yet exported and committed (per-file breakdown on /debug/tailer).")
	LogLagTotalBytes = Registry.Gauge("kubescrape_log_lag_bytes",
		"Total backlog across tracked files: bytes on disk not yet exported and committed.")
	LogRateLimited = Registry.CounterVec("kubescrape_log_rate_limited_total",
		"Per-file line rate limit hits: lines discarded (action=drop) or reads paused (action=pause).", "action")
	LogRulesDropped = Registry.Counter("kubescrape_log_rules_dropped_total",
		"Log records dropped by the logs rules (including sampled-away lines), whichever path carried the "+
			"record: the tailer, journald, Kubernetes events, Azure diagnostics, or an OTLP push into -ingest "+
			"(a dropped pushed record is still acked to its sender — it was delivered; the operator chose "+
			"to drop it). Counted ONCE PER RECORD, not once per attempt: the tailer rewinds and re-reads "+
			"the same bytes after a failed export, and counting every pass multiplied this by the number of "+
			"rewinds an outage spanned. Per RECORD rather than per DELIVERY is the exact claim — a record "+
			"withheld by a commit clamp and then rewound is DELIVERED twice and counted once, which is the "+
			"direction to be wrong in: under-claiming only inflates, while over-claiming would destroy "+
			"observations invisibly. The residual skew is a `sample` rule, whose verdict is a per-filter "+
			"counter rather than a function of the bytes — after a rewind the re-read samples a DIFFERENT set "+
			"of lines, and the drops are attributed to the pass that first put those bytes through the chain.")
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
	// MonitorNamespaceRefused counts monitors dropped because their namespace
	// is not in -monitor-namespaces. It is the ONE outcome on that code path
	// that had neither a metric nor a log line, which on a multi-tenant
	// cluster makes an admin's deliberate refusal look exactly like a selector
	// typo, a missing CRD, or a monitor that matches nothing.
	MonitorNamespaceRefused = Registry.CounterVec("kubescrape_monitor_namespace_refused_total",
		"Monitor upserts ignored because their namespace is not permitted by -monitor-namespaces (an informer re-delivery re-counts the same monitor, exactly like the sibling monitor_* counters).", "kind")
	// MonitorTargetShadowed counts monitor endpoints whose auth/TLS material
	// CONFLICTS with the monitor already holding the same URL on the same pod.
	// Two monitors resolving to one URL are served as ONE merged target that
	// honours both (a kubescrape target's exported identity has no monitor
	// component, so scraping twice would put two byte-identical series
	// identities in one payload): relabel chains concatenate, the finer
	// explicit cadence wins, one-sided auth/TLS is adopted, and a bare or
	// identical endpoint merges silently, uncounted. Auth/TLS material is the
	// one group a single scrape cannot honour twice — when both sides declare
	// it and it differs, the first monitor's is served and the other's counts
	// here. A nonzero rate means a scrape is running with a credential or TLS
	// config one of its CRs did not choose, and wants the two monitors
	// reconciled.
	//
	// Counted per served targets request, like every other decision on that
	// path: the RATE is the signal, not the absolute value.
	MonitorTargetShadowed = Registry.CounterVec("kubescrape_monitor_target_shadowed_total",
		"Monitor endpoints whose auth/TLS conflicts with the monitor already holding the same URL on that pod (the holder's is served; the rest of the endpoint's configuration still merges).", "kind")
	// ScrapeAuthFailures counts /v1/scrape-auth requests that reached the
	// Secret read and failed there, by CAUSE. The route is the only one that
	// hard-fails on external state, and every cause used to answer 404: an
	// RBAC denial (the likeliest real failure, since -scrape-auth-secrets
	// needs a grant added by hand) was indistinguishable from a typo in a
	// monitor's secret ref, and both landed in metadata_requests_total's
	// not_found stream, which is documented as the container-attribution
	// signal. `upstream` is the one to alert on.
	// The label is `reason`, NOT `kind`: `kind` is the monitor-kind dimension
	// on the three sibling monitor_* metrics (values servicemonitor/podmonitor),
	// and reusing the name for a failure cause would make one label mean two
	// unrelated things across metrics an operator reads together.
	ScrapeAuthFailures = Registry.CounterVec("kubescrape_scrape_auth_failures_total",
		"Failed /v1/scrape-auth Secret resolutions by cause (not_found = no such Secret or key; upstream = forbidden, timeout or unreachable API server; not_utf8 = value cannot be served as a JSON string).", "reason")
	// InformerWatchErrors counts the list/watch failures the reflector REPORTS
	// to its error handler — a strictly smaller set than "the watch is
	// unhealthy", and the difference is the point of this comment.
	//
	// client-go reaches the handler only when ListAndWatchWithContext RETURNS
	// an error (reflector.go RunWithContext calls watchErrorHandler on any
	// non-nil return). Two things do return one, and both land here: a refusal
	// the API server ANSWERS on the LIST — revoked RBAC, a deleted CRD, a watch
	// the server rejects — and a RELIST that fails for any reason at all
	// (`err = r.list(ctx); if err != nil { return err }`).
	//
	// What it does NOT do is fire RELIABLY while the API server is
	// UNREACHABLE, which is the shape an operator most wants to see. As long as
	// the watch REQUEST itself keeps failing retriably (connection refused,
	// 429 — isWatchErrorRetriable), the reflector backs off and `continue`s
	// inside watchWithResync: it never returns, so it never relists, so nothing
	// is reported. Measured across four outage shapes of up to five minutes: no
	// series at all, /readyz 200 throughout, every log line INFO — and in one
	// real outage the counter stepped only five seconds AFTER recovery. It may
	// equally step during the next relist an outage happens to trigger; that is
	// a signal you cannot alert on the absence of.
	//
	// The half it does cover is worth alerting on, because readiness LATCHES —
	// /readyz gates on the initial sync and is never re-evaluated, deliberately,
	// since an unready service loses its endpoints and cuts every agent off a
	// cache that is still serving useful data — so once the process is up, this
	// and the reachability probe are all that speak.
	//
	// For the unreachable half the signal is kubescrape_apiserver_reachable
	// (RegisterAPIServerProbe), which exists because no PASSIVE signal is
	// dependable:
	// the store gauges do not even freeze at plausible values. The tombstone
	// sweeper keeps running over a store nothing refills, so
	// kubescrape_store_pods DECAYS during an outage (measured: 89 -> 85 over
	// five minutes) — an alert on a FLAT gauge reads healthy exactly when it
	// should not.
	InformerWatchErrors = Registry.CounterVec("kubescrape_informer_watch_errors_total",
		"List/watch failures the informers REPORT, by resource: the refusals the API server ANSWERS (revoked RBAC, a deleted CRD, a rejected watch), plus any relist that fails. It does NOT reliably move while the API server is UNREACHABLE — client-go retries a refused watch internally and never relists, so this stayed flat through outages of up to five minutes — so alert on kubescrape_apiserver_reachable for that half.", "resource")

	// BufferTruncated counts bytes the disk buffer lost to damage discovered
	// at OPEN (truncated tails, dropped or foreign segments — diskqueue's
	// open-time loss counters). A crash mid-append costs one torn record;
	// anything larger means corruption cost fsynced records.
	BufferTruncated = Registry.CounterVec("kubescrape_buffer_truncated_bytes_total",
		"Bytes the disk buffer lost to damage discovered at open (truncated, dropped or foreign segments).", "signal")

	// Data loss is counted TWICE over: once per batch and once per record.
	//
	// A batch here is 1..1024 records, so a batch counter alone answers "did we
	// lose anything" and cannot answer "how much" — and this pair is the alert
	// the documented durable configuration points at (with -buffer-dir the
	// tailer sees the ENQUEUE verdict, so the collector's permanent rejections
	// surface here rather than on kubescrape_log_permanent_dropped_total, which
	// has always counted records). Every other producer's drop counter gets the
	// same treatment, and the ones that count batches now say so in the name.
	BufferDroppedBatches = Registry.CounterVec("kubescrape_buffer_dropped_batches_total",
		"Buffered batches dropped after a permanent collector rejection (bad payload, auth, unimplemented).", "signal")
	BufferDroppedRecords = Registry.CounterVec("kubescrape_buffer_dropped_records_total",
		"Records lost with those batches: log records, metric data points or spans, by signal. A batch whose payload no longer DECODES is counted in kubescrape_buffer_dropped_batches_total only — its record count is not recoverable — so this is a lower bound whenever kubescrape_buffer_read_errors_total is also moving.", "signal")
	BufferRequeued = Registry.CounterVec("kubescrape_buffer_requeued_total",
		"Buffered batches moved to the back of the queue after repeated transient failures (keeps one stuck batch from blocking the signal).", "signal")
	BufferFull = Registry.CounterVec("kubescrape_buffer_full_total",
		"Batches the disk buffer refused: the undelivered backlog is at its cap, or one batch exceeds the whole cap. Back-pressure for logs (the tailer rewinds and re-reads), a lost batch for producers that cannot rewind (scrape, self-metrics, log-metrics). Counted per refusal, not per batch: the tailer's in-flush retries can refuse one batch up to three times.", "signal")
	// BufferEnqueueErrors counts write-side refusals that are NOT capacity:
	// a latched fsync failure, a closed queue, ENOSPC from segment
	// preallocation. For a producer that cannot rewind (scrape, self-metrics,
	// log-metrics) the batch is gone, and every other buffer metric stays flat
	// while it happens.
	BufferEnqueueErrors = Registry.CounterVec("kubescrape_buffer_enqueue_errors_total",
		"Batches the disk buffer refused for a reason other than capacity (I/O error, closed queue, no space left on device).", "signal")
	BufferReadErrors = Registry.CounterVec("kubescrape_buffer_read_errors_total",
		"Disk-buffer read failures while draining. lost=true is reported corruption the queue advanced past (its Stats carry the magnitude); lost=false left the queue in place for a retry.", "signal", "lost")
	PositionsCorrupt = Registry.Counter("kubescrape_positions_corrupt_total",
		"Positions files that failed to parse at startup (whatever decoded is kept; the affected inputs re-read "+
			"their window). Recurring bumps across restarts point at a failing disk, not a one-off crash.")
	// PositionsSaveErrors is the write-side counterpart: offsets are silently
	// NOT being persisted, so a restart re-reads (or, with an empty store,
	// skips) per -logs-unknown-files while every other metric stays flat — the
	// same dark-node failure kubescrape_buffer_enqueue_errors_total exists for.
	PositionsSaveErrors = Registry.Counter("kubescrape_positions_save_errors_total",
		"Failed writes of the positions file (committed offsets and the journald cursor are not being persisted). Any sustained rate means a bad path, a read-only mount or a full disk.")
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
		"Compressed log files whose remainder was lost: the stream failed to decode mid-read (truncated gzip, "+
			"trailing garbage), or the file vanished with uncommitted data and no retained fd. What decoded "+
			"before the failure is delivered; the remainder is unrecoverable and the archive settles.")
	LogDrainErrors = Registry.CounterVec("kubescrape_log_drain_errors_total",
		"Reads that failed part-way through DRAINING a file incarnation that is going away (a rotated inode, a "+
			"compressed archive). The drain cannot be retried — the next sweep would fail identically while "+
			"holding the fd — so the unread remainder of that incarnation is unrecoverable and lost. Distinct "+
			"from a clean EOF, which is the drain succeeding.", "source")
	LogPodConfigInvalid = Registry.Counter("kubescrape_log_pod_config_invalid_total",
		"Files whose pod's kubescrape.io/logs annotation failed to PARSE and was ignored (counted at metadata "+
			"resolution, once per file). Logs keep flowing under the source defaults — the failure mode this "+
			"guards against is silent: an operator edits the annotation, nothing changes, and the only signal "+
			"was one Warn line on one node. The offending files and their parse errors are listed on the "+
			"agent's GET /debug/tailer as podConfigError.")
	LogPodAttrsRefused = Registry.CounterVec("kubescrape_log_pod_attrs_refused_total",
		"Resource-attribute keys a pod's kubescrape.io/logs annotation tried to set that name RESOLVED KUBERNETES "+
			"IDENTITY (namespace, pod, container, node) and were refused. The annotation is authoritative about the "+
			"workload's own description, never about which object — or which tenant — the records belong to: "+
			"k8s.namespace.name is the routing key, so honouring it let any pod redirect its logs into another "+
			"tenant. A nonzero rate is a workload attempting it, whether by mistake or not.", "key")
	// LogSegmentsStalled is the ONE state in the tailer where a file stops
	// collecting without losing anything and without any counter moving: a
	// rotated segment must be replayed before the live tail may be read (or the
	// joiner fuses fragments across the gap into records that never existed),
	// and a source that will not open — EACCES on the rotated file, EMFILE at
	// RLIMIT_NOFILE, EIO on a failing disk — leaves that gate closed. Lag grows,
	// the fd stays pinned, and the only other signal is a Warn repeating at
	// sweep cadence. A sustained nonzero value is the alert; under EMFILE it is
	// fleet-correlated. The gate is bounded, so a stall that outlives the bound
	// gives the segment up and shows on kubescrape_log_prefix_lost_total.
	LogSegmentsStalled = Registry.Gauge("kubescrape_log_segments_stalled",
		"Tracked files whose live tail is currently NOT being read because a rotated segment's replay cannot proceed.")
)

// Scrape pipeline (agent).
var (
	Scrapes = Registry.CounterVec("kubescrape_scrapes_total",
		"Scrapes by pipeline and outcome.", "pipeline", "outcome")
	ScrapeDuration = Registry.HistogramVec("kubescrape_scrape_duration_seconds",
		"Scrape duration by pipeline.", nil, "pipeline")
	ScrapeSamples = Registry.CounterVec("kubescrape_scrape_samples_total",
		"Samples parsed by pipeline (before filtering).", "pipeline")
	// The counterpart to that "before filtering". Filtering happens between
	// the parse and the conversion, so a keep rule that matches nothing — or a
	// ServiceMonitor metricRelabeling that drops everything — empties a
	// pipeline while kubescrape_scrapes_total reports success and
	// kubescrape_scrape_samples_total keeps climbing. Nothing moved when that
	// happened; scraped minus dropped is now the number that reaches the
	// collector.
	ScrapeSamplesDropped = Registry.CounterVec("kubescrape_scrape_samples_dropped_total",
		"Parsed samples discarded before conversion, by pipeline and by what discarded them: filter = the config's metrics keep/drop rules, relabel = a monitor's metricRelabelings.", "pipeline", "reason")
	SummaryUnresolved = Registry.CounterVec("kubescrape_summary_unresolved_total",
		"Objects in the kubelet's /stats/summary that the metadata service could not place, by the LEVEL of "+
			"the object: `pod` (no pod of that namespace and name, or one whose UID neither matches nor MIRRORS "+
			"the UID the kubelet reported — a static pod's kubelet-minted UID is proved against the mirror pod's "+
			"kubernetes.io/config.mirror or config.hash annotation, and a pod merely REUSING the name carries "+
			"neither) and `container` (the pod resolved, but the summary named a container it does not list). "+
			"Counted once per OBJECT per scrape, never per data point — a pod with four statistics is one "+
			"unplaceable pod. The statistics are still exported, carrying the identity the payload itself gave "+
			"them, so nothing is lost; what an unplaceable object loses is the JOIN, since a series with no pod "+
			"identity cannot line up with the cadvisor row for the same container. A steady low rate is ordinary "+
			"— a pod that ended between the kubelet building the summary and the lookup landing — while a "+
			"sustained or fleet-wide rate means the metadata service is not answering, and the ephemeral-storage "+
			"series it exists for are arriving unattributable.", "level")
	ScrapeMetaBudgetExhausted = Registry.CounterVec("kubescrape_scrape_metadata_budget_exhausted_total",
		"Scrapes that spent their whole per-scrape metadata allowance, by pipeline. The allowance is half the "+
			"scrape timeout; past it the remaining objects are NOT looked up at all, so they export under the "+
			"identity the payload itself carried and lose only the join. It exists because a metadata service "+
			"that HANGS — a partition or a dropping firewall, as opposed to one that refuses, which fails "+
			"instantly and is harmless — would otherwise consume the entire scrape budget and take the kubelet "+
			"pipelines down with it, discarding stats already parsed. So this counter is the difference between "+
			"degraded attribution and no data: a sustained rate means the metadata service is slow or "+
			"unreachable and this node's cadvisor and summary series are arriving unjoinable. It is the only "+
			"signal for the objects that were never asked about — kubescrape_metadata_requests_total cannot "+
			"move for a request that is never issued.", "pipeline")
	ScrapeMalformed = Registry.CounterVec("kubescrape_scrape_malformed_total",
		"Exposition samples dropped as malformed by pipeline (unparseable lines, histogram buckets without le, summary rows without quantile).", "pipeline")
	ScrapeExemplarsMalformed = Registry.CounterVec("kubescrape_scrape_exemplars_malformed_total",
		"Unparseable OpenMetrics exemplar suffixes by pipeline. NO data was lost: the samples carrying them were exported without the exemplar, which is why this is separate from kubescrape_scrape_malformed_total. Only ever nonzero where exemplar scraping is enabled.", "pipeline")
	ScrapeCollisions = Registry.Counter("kubescrape_scrape_name_collisions_total",
		"Data points dropped because their family name was already claimed by a metric of another shape in the same batch (a target redeclaring a family's TYPE mid-exposition).")
	// The attributable half of the collision above, for the one case the
	// protobuf exposition makes reachable without any TYPE redeclaration: a
	// HISTOGRAM family whose metrics carry different REPRESENTATIONS. One name
	// carries one OTLP type, so the family resolves to native and the other
	// representation's data is dropped here rather than silently in the
	// batcher's type guard.
	ScrapeHistogramMixed = Registry.CounterVec("kubescrape_scrape_histogram_mixed_total",
		"Protobuf histogram metrics dropped because their family resolved to another representation, by pipeline and by the representation that lost: classic = per-bucket rows, nhcb = custom-bucket native (schema -53, whose bounds this client_model cannot read at all).", "pipeline", "dropped")
)

// OTLP exporter (agent).
var (
	Exports = Registry.CounterVec("kubescrape_export_requests_total",
		"OTLP export attempts by signal and outcome.", "signal", "outcome")
)

// OTLP ingest (agent).
var (
	// IngestRejected counts pushes refused for exceeding one of the receiver's
	// admission bounds: the concurrently-processed count, or the raw payload
	// bytes both transports may buffer while reading and decoding. They are
	// retryable and the sender still holds the payload, but a persistently
	// non-zero rate means the node cannot keep up with what is being pushed at
	// it.
	IngestAdmissionRejected = Registry.Counter("kubescrape_ingest_admission_rejected_total",
		"Pushed RESOURCES the transforms file's ingest admission hook (ingest: admit(resource)) rejected — "+
			"removed before enrichment, push still acked. The hook is the operator's per-sender policy on "+
			"listeners nothing authenticates; a script error fails OPEN (the resource is admitted) and counts "+
			"into kubescrape_transform_errors_total{signal=\"ingest\"} instead.")
	IngestBodyRejected = Registry.CounterVec("kubescrape_ingest_body_rejected_total",
		"OTLP/HTTP request bodies refused at the receiver's door, before anything was decoded, by reason. FOUR "+
			"of the five describe a request that is WRONG, so the sender must change something before a retry "+
			"can work: too_large (413, over the receiver's cap in either the compressed or the decompressed "+
			"direction), media_type (415, a Content-Type that is not application/x-protobuf), content_encoding "+
			"(400, a Content-Encoding that is neither gzip nor identity) and malformed (400, a body that would "+
			"not decompress, or bytes that are not a valid OTLP payload). Each of those carries a throttled Warn "+
			"naming the peer, which on a listener nothing authenticates is the only way to tell a misconfigured "+
			"sender apart from a probe. `aborted` is the ODD ONE OUT and carries no Warn: the client went away "+
			"mid-upload (a killed pod, a rolled deployment, an SDK export timeout), so nothing was wrong with "+
			"the request and the retry is exactly what happens next — a rolling deployment would otherwise log "+
			"one accusation per evicted pod. It is answered 503, deliberately neither 400 nor 408. Also "+
			"deliberately SEPARATE from kubescrape_ingest_rejected_total, which is the receiver protecting "+
			"ITSELF (in-flight or byte-budget back-pressure) and is retryable as sent. Only the APPLICATION-"+
			"facing listeners feed this family: the trace tier runs its authenticated internal hop in the same "+
			"process, and folding sibling-shard traffic in would put bearer-authenticated pushes into the series "+
			"an operator reads as \"somebody out there is pushing wrong\" — a failed hop is already one "+
			"kubescrape_service_graph_sends_failed_total on the SENDING shard, where the peer is known.", "reason")
	IngestChainSkipped = Registry.CounterVec("kubescrape_ingest_log_chain_skipped_total",
		"Ingested log RECORDS or RESOURCES whose line-derived processing (body enrichment, log-metrics "+
			"observation) was skipped by an abuse bound — the data itself is still forwarded. Reasons: "+
			"body_too_large (one record whose body's text view exceeds 1 MiB; the tailer never feeds lines past "+
			"-logs-max-entry-bytes either, and attribute/severity-keyed rules still apply), resource_too_wide "+
			"(one resource with more than 64 attributes — the metric store retains a serialization of the whole "+
			"resource per series, so sender-chosen width is sender-chosen retained heap), resources_capped (a "+
			"resource past the first 256 of one push). The listeners are unauthenticated; a nonzero rate is a "+
			"sender worth finding.", "reason")
	IngestRejected = Registry.Counter("kubescrape_ingest_rejected_total",
		"Pushed OTLP requests refused because a receiver admission bound was reached — concurrent in-flight pushes or buffered payload bytes (retryable: 429 / ResourceExhausted).")
	IngestReserveExpired = Registry.Counter("kubescrape_ingest_reserve_expired_total",
		"gRPC pre-decode buffer reservations reclaimed because the peer sent no message inside the decode "+
			"window; the reclaim also cancels that stream (the sender sees Canceled, which OTLP lists as "+
			"retryable). Deliberately NOT kubescrape_ingest_rejected_total: nothing was refused and the budget "+
			"had room, so folding the two would let one headers-only prober, at zero cost in bytes, drive the "+
			"rate an operator scales the node on. A sustained rate here is one slow or probing sender on a "+
			"listener nothing authenticates.")
	IngestReservedStripped = Registry.CounterVec("kubescrape_ingest_reserved_stripped_total",
		"Attribute occurrences removed at first receipt because a sender shipped a key reserved for "+
			"kubescrape's own plumbing, by key — the namespace router's script marker (honored on a resource "+
			"before any routing glob, so a wire-supplied copy would steer the payload onto any configured "+
			"route and its tenant headers) or the transform engine's drop marker (whose presence-only prune "+
			"would delete the element and count it as an operator-intended transform drop whenever a script "+
			"for that signal is active). The data itself is still forwarded, minus the reserved key.", "key")
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

// SelfMetadataLookups' outcome label values, shared by the two binaries'
// resolvers so their series stay unionable (each binary re-typing the strings
// is how one grows a spelling the other's dashboards do not match).
const (
	SelfLookupSelf   = "self"    // resolved via GET /v1/self (the agent's first try)
	SelfLookupByName = "by_name" // resolved via the namespace/name fallback
	SelfLookupError  = "error"   // not resolved; the error is returned to the poller
)

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

// High-frequency cgroup sampler (agent, -cgroup-stats).
var (
	CgroupSamples = Registry.Counter("kubescrape_cgroup_samples_total",
		"Per-container cgroup sampling rounds. Divided by the number of containers and the elapsed time this is the effective sampling rate, which is what tells a stalled sampler (a slow export goroutine cannot stall it, but a wedged filesystem can) from a quiet node.")
	CgroupReadErrors = Registry.CounterVec("kubescrape_cgroup_read_errors_total",
		"Failed reads of a held cgroup file, by file. A container removed between two samples produces a burst of these until the next discovery pass drops it, so a small steady rate on a node with churn is normal; a rate proportional to the container count is not.", "file")
	CgroupOpenErrors = Registry.Counter("kubescrape_cgroup_open_errors_total",
		"Discovered container cgroups whose files could not be opened (the container went away between the directory listing and the open, or its controllers are not enabled). The container is retried on the next discovery pass.")
	CgroupCounterResets = Registry.Counter("kubescrape_cgroup_counter_resets_total",
		"Intervals discarded because cpu.stat's usage_usec went backwards — the cgroup was replaced under a path still being sampled. The reading becomes the new baseline; no rate is derived across the reset, since the unsigned difference would render an enormous positive burst that never happened.")
	CgroupContainersCapped = Registry.CounterVec("kubescrape_cgroup_containers_capped_total",
		"Discovered container cgroups refused because a cap was reached, by which cap. The two bound different resources and have different remedies: `tracked` is the FILE DESCRIPTOR cap (three are held open per sampled container so the per-second read path allocates nothing), and a container refused there is discovered and attributable but never sampled; `pending` is the MEMORY cap on the not-yet-attributed set, which holds no descriptors at all and is sized for every pod's sandbox cgroup plus a metadata outage's worth of real containers. A nonzero `tracked` rate above the kubelet's max-pods means either an unusual node or a discovery walking something it should not; a nonzero `pending` rate means the hierarchy holds thousands of cgroups nothing can place.", "cap")
	CgroupDiscoveryErrors = Registry.CounterVec("kubescrape_cgroup_discovery_errors_total",
		"Discovery passes that could not read part of the cgroup hierarchy, by scope. `root` is the whole pass failing to list -cgroup-stats-root: the root is unmounted or unreadable, NOTHING is discovered and the sampler is exporting nothing — the same condition the startup warning names. `subtree` is a directory below the root failing to list: the pass still returns what it found, but it is INCOMPLETE, so nothing is retired from it (an unreadable directory is indistinguishable from a container that went away) and a container that really did go away keeps its descriptors until a complete listing proves otherwise.", "scope")
	CgroupWindowsDropped = Registry.CounterVec("kubescrape_cgroup_windows_dropped_total",
		"Container windows measured and then discarded rather than exported, by reason. ALERT ON THE REASON, NOT THE FAMILY: two of the three are faults and the third is expected on an ordinary node. `unresolved` means the metadata service could not place the container when the payload was built, so the window had no resource to carry it (the samples are already reset; a window describes one bounded interval and is never carried forward, which would silently stretch the interval the numbers claim to cover). `export_failed` means the collector — or, with -buffer-dir, the spool — refused the payload. Those two are the pipeline's LOSS signal: a sustained rate is data an operator asked for and did not get. `too_short` is NOT a fault — it is a container that entered the sampled set and left the cgroup hierarchy before any window of its life could be described, which is the only evidence a node can produce that -cgroup-stats-discover-interval is missing short-lived containers (a container that starts AND exits between two discovery passes is never seen at all and leaves nothing to count). It fires on ~13% of containers at every lifetime up to the discovery interval, so any node running CronJobs or init containers sits permanently nonzero; the remedy is a shorter discovery interval, paid for in metadata-service lookups. A rate(...) > 0 alert over the whole family therefore pages on normal pod churn.", "reason")
	CgroupHeldWindows = Registry.CounterVec("kubescrape_cgroup_held_windows_total",
		"Signal windows that took fewer than two samples, by what was done about it. A single reading is not a distribution (its stddev is 0 by construction and its max and min are the same number — the average the cadvisor scrape already publishes), so `held` re-states the last distribution actually measured, which bridges a sampling gap of a second or two without the series alternating between real and degenerate points. The hold is BOUNDED: `expired` counts a signal reaching the bound and ceasing to be exported, which is what keeps a dead-but-still-listed container (a CRI-O conmon scope outliving its container, or a stale listing) from republishing its last measurement as fresh data forever.", "outcome")
	CgroupScanTruncated = Registry.Counter("kubescrape_cgroup_scan_truncated_total",
		"Discovery passes that hit the directory budget and stopped early, so the container set is INCOMPLETE. Never expected on a Kubernetes node; if it moves, -cgroup-stats-root is pointing at something far larger than a cgroup hierarchy.")
	CgroupUnresolved = Registry.CounterVec("kubescrape_cgroup_unresolved_total",
		"Identity lookups that did not place a container cgroup, by outcome. Such a cgroup is NOT exported: a resource with no service.name has no Prometheus job, joins none of the cadvisor series these gauges exist to explain, and would appear and disappear with a job attached as lookups succeeded and stopped succeeding. `pending` counts one DEFINITIVE miss at discovery — the metadata service answered and does not know this container id — where the verdict decides whether to spend three file descriptors on the cgroup; `unreachable` counts a lookup the service could not answer at all (transport failure, 5xx, timeout), which is deliberately kept apart because it says nothing about the cgroup and therefore never counts toward giving up on it; `abandoned` counts giving up on one after a grace period of definitive misses, after which it is retried far more slowly — a cadence an unreachable lookup drops it back out of, so the node resumes within a minute of the service returning. `export` counts a failure at the other seam, where the resource is rebuilt from current metadata to carry a window that has already been measured — so unlike the others it costs data, counted again as kubescrape_cgroup_windows_dropped_total{reason=\"unresolved\"}. A steady low `pending` rate is normal and expected (every pod's sandbox cgroup lives there permanently, its container id appearing in no pod's containerStatuses); any sustained `unreachable` or `export` rate means the metadata service is not answering.", "outcome")
	CgroupContainersRetired = Registry.Counter("kubescrape_cgroup_containers_retired_total",
		"Containers dropped from the sampled set because every read of all three of their cgroup files failed for several consecutive export windows: the cgroup is still listed but no longer readable. It is what a CRI-O container whose own scope was removed while its conmon scope lingers looks like, and what a stale directory listing looks like anywhere. Their descriptors are released and they stop being counted in kubescrape_cgroup_containers; without the retirement they held three file descriptors and issued three failing reads per sampling period for the life of the process. A sustained rate means containers are disappearing in a way discovery cannot see.")
)

// RegisterCgroupStats publishes the tracked-container gauge, registered exactly
// when the sampler RUNS — the RegisterSelfMetadata rule, for the same reason. A
// published 0 then always means "enabled and finding nothing", which is the
// whole failure this pipeline had to be built not to hide: an agent with no
// /sys/fs/cgroup mount starts cleanly, answers /readyz 200 and exports no
// container at all, and an absent metric family is indistinguishable from a
// feature nobody turned on.
func RegisterCgroupStats(containers, unresolved func() int) {
	Registry.GaugeFunc("kubescrape_cgroup_containers",
		"Container cgroups currently sampled by -cgroup-stats. Registered whenever the sampler runs, so 0 means it is running and finding nothing (typically a missing read-only /sys/fs/cgroup mount) rather than disabled.",
		func() float64 { return float64(containers()) })
	Registry.GaugeFunc("kubescrape_cgroup_unresolved_containers",
		"Container cgroups discovered by -cgroup-stats whose identity the metadata service has not placed, and which are therefore not exported. One per pod is EXPECTED and permanent (the sandbox/pause cgroup appears in no pod's containerStatuses); a value that tracks the container count while kubescrape_cgroup_containers sits near 0 means the metadata service is not answering.",
		func() float64 { return float64(unresolved()) })
}

// Journald input (agent).
var (
	JournalEntries = Registry.Counter("kubescrape_journal_entries_total",
		"Journal entries exported.")
	JournalRestarts = Registry.Counter("kubescrape_journal_restarts_total",
		"Journal reader restarts.")
	JournalTruncated = Registry.Counter("kubescrape_journal_truncated_total",
		"Journal messages truncated at MaxEntryBytes (the record carries log.truncated).")
	JournalExportFailures = Registry.Counter("kubescrape_journal_export_failures_total",
		"Journal batch exports that failed and are being retried IN PLACE. The batch is kept and never re-read, "+
			"so this counts ATTEMPTS — not lost entries, and not re-reads: a steady rate is a collector outage the "+
			"reader is riding out with its cursor uncommitted, and the loss counter is "+
			"kubescrape_journal_dropped_batches_total. Deliberately NOT kubescrape_log_export_failures_total, which "+
			"is the tailer's files-rewound counter and cannot apply here — journald rewinds no file, and the "+
			"singleton that reads a journal typically runs with -logs=false, so those increments landed on a "+
			"metric whose help described something that had not happened.")
)

// OTLP ingest (agent).
var (
	Ingested = Registry.CounterVec("kubescrape_ingest_resources_total",
		"Distinct pushed identities (container id / pod uid, memoized per request) by enrichment outcome. enriched = an id resolved; peer_ip = no id, attributed by the connection's source address; peer_ip_rejected = that address resolved to the RECEIVER's own workload, so it was rewritten in flight (a proxy, a mesh sidecar, or an internal hop addressed to the application port) and nothing was attributed — anything above zero means peer-IP attribution cannot work on that path; unresolved = nothing identified the sender; split_capped = the push exceeded what one payload may inflate into — either its distinct-object count (maxSplitGroups) or the bytes those per-object resource copies would cost (maxSplitCopyBytes) — so the remainder shares the sender's resource unenriched rather than costing one full resource copy each.", "outcome")
	ExportRejected = Registry.CounterVec("kubescrape_export_rejected_records_total",
		"Records the collector REJECTED inside a payload it otherwise accepted (OTLP partial_success), by signal. "+
			"The export succeeded, so every producer advanced its offset, cursor or position past them — these are "+
			"lost, permanently, and retrying cannot help (OTLP defines them as invalid rather than deferred). "+
			"Any nonzero rate means telemetry is being discarded downstream; the collector's own message is on the "+
			"accompanying warning.", "signal")

	// Export seam (agent): routing and transforms. (Grouped here with the
	// ingest metrics historically; the banner above does not cover them.)
	Routed = Registry.CounterVec("kubescrape_routed_payload_parts_total",
		"Payload parts forwarded to a non-default routing destination.", "route", "signal")
	TransformErrors = Registry.CounterVec("kubescrape_transform_errors_total",
		"Transform program invocations that failed (the batch is NOT exported; the error propagates to the producer's retry path).", "signal")
	// A transform drop is INTENDED loss, which is exactly why it needs a
	// counter: the intent lives in an operator-edited Starlark file that hot-
	// reloads, so a one-character edit can silently discard a node's whole log
	// stream with no error logged and every other metric green. That has
	// already happened once (see transform/hostobj.go).
	TransformDropped = Registry.CounterVec("kubescrape_transform_dropped_total",
		"Records a transform script called drop() on, by signal: log records, metric data points (a dropped metric counts all of its points) and spans. Counted when the batch's export is ACKED (a delivered forward, or a payload transformed to nothing and acked without a send), so a producer's transient retries never re-count. The one exception is the tailer's in-place seam, which counts at transform time: a rewound re-read after a failed export re-runs the (possibly hot-reloaded) script and re-counts.", "signal")
	TransformReloads = Registry.CounterVec("kubescrape_transform_reloads_total",
		"Transforms-file reloads by outcome (applied, failed — a failed compile keeps the last good program).", "outcome")

	// Trace tier (the -service-graph shard's sampler and span metrics).
	TraceSpansDropped = Registry.CounterVec("kubescrape_trace_spans_dropped_total",
		"Ingested spans dropped by the trace sampler (probability = the consistent trace-ID decision, rate = the spans/second cap).", "reason")
	SpanMetricsDropped = Registry.Counter("kubescrape_span_metrics_dropped_total",
		"Spans not aggregated into span metrics because the dimension-cardinality cap was reached.")
	SpanMetricsEvicted = Registry.Counter("kubescrape_span_metrics_evicted_total",
		"Span-metric series dropped at export because their dimensions went unobserved for traceMetrics.staleAfter (this is what frees cardinality-cap slots).")
)

// Service-graph edges (the -service-graph shard). Every one of these counts a
// place where the graph is INCOMPLETE, and each names its cause — a config
// bound for all but the unnamed-spans counter, because those bounds trade
// completeness for memory and an operator has to be able to see which one is
// binding: an edge missing from Grafana's Service Graph view looks exactly
// like a call that never happened.
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
	// ServiceGraphUnnamed is the one counter in this block with no config bound
	// behind it: the incompleteness is the SENDER's. A resource without
	// service.name gives the graph no label value to name a node with, and
	// pairing its spans anyway minted one anonymous ""-labeled vertex that
	// every unattributable sender in the cluster shared, accreting edges to
	// real services (Tempo skips such resources too).
	ServiceGraphUnnamed = Registry.Counter("kubescrape_service_graph_unnamed_spans_total",
		"Spans skipped by the service-graph processor because their resource carries no service.name — nothing to name a graph node with. The shipped tier enriches at entry and derives service.name for every attributable sender, so a sustained rate means unattributable senders whose spans can mint no edge (they still forward to the collector; only the graph skips them).")
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

// Tail sampling (the -service-graph shard's buffering layer). Two things need
// counting here that no other layer knows: WHICH rule decided a trace — the
// policy engine returns it precisely so the answer is not "some subset of the
// list" — and what the memory bounds cost when they bind, since every bound
// trades trace completeness for a smaller buffer.
var (
	TailSampleTraces = Registry.CounterVec("kubescrape_tail_sampling_traces_total",
		"Traces decided by the tail sampler, by verdict (keep, drop) and by the policy that decided. policy=\"none\" is the default drop — no policy had an opinion — which is what a policy list matching nothing looks like. Every configured policy gets its series at startup, so a policy that has never fired reads as zero rather than as absent. This counts DECISIONS: whether the kept trace then reached the collector is kubescrape_tail_sampling_spans_total.", "decision", "policy")
	TailSampleSpans = Registry.CounterVec("kubescrape_tail_sampling_spans_total",
		"Spans leaving the tail-sampling buffer, by fate: kept = the trace was sampled and the export was acked (with -buffer-dir, acked means SPOOLED — a decided keep is durable and a collector outage becomes a backlog); dropped = the trace was not sampled; lost = the final attempt to move the trace's spans out of this process failed and nothing here holds a copy any longer (the sender was acked when the spans were buffered). Under a routed or size-split export, earlier shares may already have been delivered or spooled, so lost is an UPPER BOUND on loss — it never under-counts. A moving lost rate is data loss, not back-pressure; without -buffer-dir a collector outage produces it directly, with one it means the spool itself refused the payload.", "outcome")
	TailSampleEarly = Registry.CounterVec("kubescrape_tail_sampling_early_decisions_total",
		"Traces decided BEFORE their decisionWait elapsed, by the bound that forced it (spans_per_trace, max_traces, max_spans) or shutdown (a graceful stop flushing the buffer, plus any straggler push decided on arrival after that flush). An early decision judges the spans present, so it degrades gracefully — a slow trace can be missed, a fast one is never invented — but a sustained rate against a bound means that bound is sized below the shard's span rate.", "reason")
	TailSampleLate = Registry.CounterVec("kubescrape_tail_sampling_late_spans_total",
		"Spans that arrived after their trace was decided and followed the cached verdict, by outcome (kept = forwarded immediately, dropped). A large kept+dropped share means decisionWait is shorter than the spread of a trace's arrival.", "outcome")
	TailSampleCacheEvicted = Registry.Counter("kubescrape_tail_sampling_cache_evictions_total",
		"Verdicts evicted from the decision cache by its SIZE cap while still LIVE (evicting an entry already past its TTL is the cache reclaiming space, not a signal, and is not counted). A span arriving for an evicted trace starts a fresh window AND re-charges the rateLimiting and composite budgets — the cache remembers a trace was charged for as long as it holds the entry at all, so eviction is the only thing that still double-charges. Raise tailSampling.decisionCacheSize if this moves.")
)

// RegisterTailSamplingStats publishes what the tail-sampling buffer is holding.
//
// This is the one number the counters above cannot give and the one an operator
// most needs: buffered spans are ACKED but not durable (see agent/tailbuffer's
// package doc), so this gauge is exactly what a hard kill of the shard would
// lose at that instant. It is also the leading indicator for the memory bounds,
// the same reason RegisterBufferStats exists beside the disk buffer's drop
// counters — the early-decision counter only moves once a bound has ALREADY
// bound.
func RegisterTailSamplingStats(stats func() TailSamplingStat) {
	Registry.GaugeFunc("kubescrape_tail_sampling_buffered_traces",
		"Traces currently assembling in the tail-sampling buffer, awaiting their decision. tailSampling.maxTraces caps it; at the cap the oldest is decided early (kubescrape_tail_sampling_early_decisions_total).",
		func() float64 { return float64(stats().Traces) })
	Registry.GaugeFunc("kubescrape_tail_sampling_buffered_spans",
		"Spans currently held in the tail-sampling buffer. These are acked to their senders but not yet decided and not durable anywhere (a DECIDED keep is spooled when -buffer-dir is set; an undecided one is not): this is what a hard kill of this pod would lose. tailSampling.maxSpans caps it, and is itself checked against the pod's memory limit at startup — the likeliest hard kill here is the OOM this buffer causes.",
		func() float64 { return float64(stats().Spans) })
}

// TailSamplingStat is the buffer's occupancy (tailbuffer.Stats' shape; see
// RegisterServiceGraphStats on why obs declares its own).
type TailSamplingStat struct {
	Traces int
	Spans  int
}

// Journald drops (agent).
var (
	JournalDropped = Registry.Counter("kubescrape_journal_dropped_batches_total",
		"Journal batches dropped after a permanent collector rejection (the cursor advances past them).")
	JournalDroppedRecords = Registry.Counter("kubescrape_journal_dropped_records_total",
		"Journal records lost with those batches. The magnitude of the loss: a batch is up to Config.BatchSize entries.")
)

// Kubernetes events (the cluster-singleton events collector).
var (
	Leader = Registry.Gauge("kubescrape_leader",
		"1 while this replica holds the cluster-singleton lease, 0 otherwise; sum != 1 means split brain or nobody leading.")
	EventsObserved = Registry.CounterVec("kubescrape_events_observed_total",
		"Kubernetes events received from the watch, by event type (normal, warning, other — anything else the API server reports).", "type")
	EventsExported = Registry.Counter("kubescrape_events_exported_total",
		"Kubernetes event records exported (after the rules).")
	EventsDropped = Registry.Counter("kubescrape_events_dropped_batches_total",
		"Kubernetes event batches dropped after a permanent collector rejection (the position advances past them).")
	EventsDroppedRecords = Registry.Counter("kubescrape_events_dropped_records_total",
		"Kubernetes event records lost with those batches (the magnitude of the loss the batch counter only signals).")
	EventsOverflowDropped = Registry.Counter("kubescrape_events_overflow_dropped_total",
		"Kubernetes events dropped UNEXPORTED because the retained batch hit its cap before anything could commit (a collector outage on a fresh install); the watch will not re-deliver them, so each is outright loss.")
	EventsExportFailures = Registry.Counter("kubescrape_events_export_failures_total",
		"Kubernetes event batch exports that failed transiently. The batch is KEPT and the watch stays open "+
			"(tryFlush), so this counts ATTEMPTS — one per flush, not lost events: a steady rate is a collector "+
			"outage the reader is riding out with its position uncommitted, and the loss counters are "+
			"kubescrape_events_dropped_batches_total (a permanent rejection) and "+
			"kubescrape_events_overflow_dropped_total (the retained batch reaching its cap). Deliberately NOT "+
			"kubescrape_log_export_failures_total, which is the tailer's files-rewound counter and cannot apply "+
			"here — this reader rewinds no file, and the singleton that collects events runs with -logs=false, so "+
			"those increments landed on a metric whose help described something that had not happened.")
	EventWatchRestarts = Registry.Counter("kubescrape_event_watch_restarts_total",
		"Event watch restarts (a closed stream, an error, or an expired resourceVersion).")
	EventRelists = Registry.CounterVec("kubescrape_event_relists_total",
		"Event watches that fell back to a relist because the stored resourceVersion had aged out of the API server's watch window.", "stage")
	EventGapDiscarded = Registry.CounterVec("kubescrape_event_gap_discarded_total",
		"Event watch expiries with no relist to fall back to (nothing exported yet and none armed): the next stream restarts at the CURRENT revision and whatever the dead watch never delivered is discarded — the events pipeline's one silent-loss arm, worth an alert wherever kubescrape_event_relists_total has one.", "stage")
	EventPositionErrors = Registry.CounterVec("kubescrape_event_position_errors_total",
		"Failures reading or writing the event position ConfigMap, by operation (load, save).", "operation")
)

// Azure diagnostics (the Event Hubs consumer in the cluster-singleton deployment).
var (
	// signal/plural, matching AzureExported and every other producer. This
	// counter used to spell the same dimension "kind" with singular values, so
	// the decoded and exported counts of one pipeline could not be joined or
	// even reliably grepped for.
	AzureRecords = Registry.CounterVec("kubescrape_azure_records_total",
		"Azure diagnostic records decoded from Event Hubs messages, by signal (logs, metrics).", "signal")
	AzureDecodeErrors = Registry.Counter("kubescrape_azure_decode_errors_total",
		"Event Hubs messages or records that could not be decoded as Azure diagnostics JSON (skipped, committed past).")
	AzureExported = Registry.CounterVec("kubescrape_azure_exported_total",
		"Azure diagnostic records exported, by signal (logs, metrics).", "signal")
	AzureDropped = Registry.Counter("kubescrape_azure_dropped_batches_total",
		"Azure diagnostic payloads dropped after a permanent collector rejection (the offsets advance past them).")
	AzureDroppedRecords = Registry.CounterVec("kubescrape_azure_dropped_records_total",
		"Azure diagnostic records (log records or metric data points) lost with those payloads, by signal.", "signal")
	AzureExportFailures = Registry.CounterVec("kubescrape_azure_export_failures_total",
		"Azure diagnostic payload exports that failed transiently and are being retried IN PLACE, by signal "+
			"(logs, metrics). The payload is kept and the Kafka offsets do not advance until the collector acks, "+
			"so this counts ATTEMPTS, not lost records; the loss counters are "+
			"kubescrape_azure_dropped_batches_total and kubescrape_azure_dropped_records_total. Deliberately NOT "+
			"kubescrape_log_export_failures_total, the tailer's files-rewound counter: this reader owns no file, "+
			"it runs in the singleton Deployment with -logs=false, and only its LOGS signal was ever counted "+
			"there — so a hub carrying platform metrics retried invisibly.", "signal")
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

func init() {
	// The build version reaches internal/metrics' own exported scopes through
	// here: it owns BuildVersion and imports that package, so the value is
	// pushed down rather than imported back.
	metrics.SetScopeVersion(BuildVersion())
}

// RegisterLogMetricsDrops exposes one log-metrics set's refused observations as
// export-time counters — cumulative since process start.
//
// The counts used to be PROCESS-GLOBAL atomics in internal/metrics, registered
// unconditionally here, purely because obs imports that package and the
// counters therefore could not be declared in obs. Registering a getter over
// the set is what dissolves that, and it is the pattern the store stats, the
// buffer stats and the self-metadata gauge already use. It also makes the
// family mean what it says: registered exactly when a log-metrics set EXISTS,
// so a published 0 is "configured and dropping nothing" rather than "the
// feature is off", and two sets in one process no longer merge their counts
// into one number.
func RegisterLogMetricsDrops(set *metrics.DynamicMetricSet) {
	if set == nil {
		return
	}
	// One family, labeled by metric name — sum() over the label is the
	// aggregate. There used to be two: an unlabeled
	// kubescrape_log_metrics_dropped_capped_total counter beside a
	// kubescrape_log_metrics_dropped_capped_by_metric GAUGE that carried a
	// monotonic since-start total and spelled its label name inside the metric
	// name. Both halves were wrong: a gauge does not mark the reset at a
	// restart, so rate()/increase() over it silently swallowed one, and the cap
	// frees slots only through idleness — a burst blinds ONE metric for up to
	// maxAge + grace (24h by default) and an alert has to be able to name it,
	// which the aggregate could not. A counter vec is both.
	//
	// The cost of the merge, stated plainly: the label set is data-driven (a
	// metric name appears only once it has dropped something), so with nothing
	// dropped the family is ABSENT rather than reading 0, where the old
	// unlabeled counter always published a zero.
	Registry.CounterFuncVec("kubescrape_log_metrics_dropped_capped_total",
		"Log-metric observations dropped because that metric's label-set cardinality cap was reached, by metric name. sum() over the label is the total. Absent until something is dropped: the label set is data-driven.",
		"metric", set.DroppedCappedByMetric)
	// kubescrape_log_metrics_dropped_collision_total was REMOVED here, and the
	// removal is the point rather than a tidy-up. It counted observations the
	// store refused because a second "check" hash disagreed on a primary-hash
	// hit — a guard that existed because the primary key was 64 bits. The key is
	// 128 bits now, which puts a collision at ~1.5e-31 per series map (10000
	// label sets, birthday bound), so the guard was removed with it and this
	// counter could never move again. A counter that is structurally incapable
	// of moving is worse than no counter: it reads as evidence of absence, and
	// an operator alerting on it would believe they were watching something.
	// See metrics/labels.go's strHash for the full argument.
	Registry.CounterFunc("kubescrape_log_metrics_dropped_nan_total",
		"Log-metric observations dropped since start because the extracted value was NaN or +/-Inf (neither is representable as a sample).",
		func() float64 { return float64(set.DroppedNaN()) })
	Registry.CounterFunc("kubescrape_log_metrics_dropped_undelivered_total",
		"Undelivered log-metric resources dropped because the re-offer buffer filled or the collector rejected them "+
			"PERMANENTLY. Taking a snapshot is DESTRUCTIVE (it seals aggregation windows, zeroes idled samples and "+
			"deletes expired ones), so a transiently failed export retains its samples for the next one; this counts "+
			"what a collector outage longer than that buffer could hold, plus definitively rejected chunks that "+
			"retrying could never deliver. These are genuinely lost observations — the ones the retention cannot save.",
		func() float64 { return float64(set.DroppedUndelivered()) })
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

// RegisterAPIServerProbe publishes the API-server reachability watchdog's two
// series: a 1/0 gauge for the last probe's verdict and a counter of failed
// probes.
//
// Registered exactly when the probe RUNS (-apiserver-probe-interval > 0), the
// way RegisterSelfMetadata is: a published 0 then always means UNREACHABLE and
// never "the probe is off", and an ABSENT family means nobody is looking —
// which is a distinct thing for an alert to select on.
//
// It exists because no passive signal reliably sees the commonest outage.
// kubescrape_informer_watch_errors_total stays flat while the API server is
// merely unreachable (client-go retries a refused watch internally and never
// relists — see that metric); /readyz latches at the initial sync by design;
// and kubescrape_store_pods DECAYS rather than freezing, because the tombstone
// sweeper keeps running over a store nothing refills.
//
// What the gauge measures is EXACTLY one thing: whether a NEW connection from
// this pod, with this ServiceAccount, reached the API server on the last probe.
// That is a proxy for "the cache can refill", not a reading of the cache
// itself, and it is wrong in both directions at the margins. False negative:
// an ESTABLISHED watch blackholed by a NetworkPolicy or a dead conntrack entry
// leaves the cache frozen while a fresh connection still succeeds and this
// reads 1 — the classic silent stall. False positive: one failed probe (a
// dropped packet, an API server mid-restart, a 10s timeout under load) reads 0
// without proving the informers missed anything. Alert on it SUSTAINED, and
// read the failure counter for the history of an outage the process survived.
func RegisterAPIServerProbe(reachable func() bool, failures func() int64) {
	Registry.GaugeFunc("kubescrape_apiserver_reachable",
		"1 when the last API-server probe opened a NEW connection and got an answer, 0 when it did not. A proxy for 'the informer caches can still refill', not a reading of the caches: a blackholed established watch can leave them frozen while this reads 1, and a single failed probe does not prove they stopped. Alert on it sustained. The service keeps serving its in-memory cache either way — readiness latches at the initial sync on purpose. Absent when -apiserver-probe-interval=0 (no probe runs); never 0 for that reason.",
		func() float64 {
			if reachable() {
				return 1
			}
			return 0
		})
	Registry.CounterFunc("kubescrape_apiserver_probe_failures_total",
		"API-server reachability probes that failed (a metadata-only LIST of one namespace, the same TCP/TLS/authn/authz path the informers use). Nonzero with the gauge back at 1 means the outage healed; a rising rate with the gauge at 0 is an outage in progress.",
		func() float64 { return float64(failures()) })
}

// RegisterWaiterStats exposes the container-lookup waiter state: how many
// lookups are blocked right now, and how many have been SHED by the cap.
//
// The shed is a load-shedding 503, and without its own series it is
// indistinguishable from the not-ready 503 in
// kubescrape_http_requests_total{pattern="/v1/containers",code="503"}. The cap
// binding means abuse or an extreme anomaly rather than ordinary fleet load —
// exactly the moment the operator needs the two told apart — and the gauge is
// what shows the pressure building before the cap is reached.
//
// The shutdown drain gets its OWN counter rather than a second reason on the
// shed one. Both answer the same retryable 503, but they mean opposite things
// to an operator: the cap binding is abuse or an anomaly worth paging on, while
// the drain is one line per rolling update — and blurring them would make every
// deploy fire the abuse alert.
//
// A hook rather than a counter the store bumps directly: internal/store has no
// obs dependency, the same way the buffer stats and the self-metadata gauge
// are wired.
func RegisterWaiterStats(blocked func() int, shed, drained func() int64) {
	Registry.GaugeFunc("kubescrape_container_lookups_blocked",
		"Container lookups currently parked, in EITHER of the two places one can park: waiting for a container "+
			"ID to appear in the store, or — only until the informer caches finish their initial sync — waiting "+
			"for the store to become readable at all. Both draw on the one -max-blocked-lookups budget, because "+
			"both hold a request on a route nothing authenticates; the second is transient by nature, but it is "+
			"exactly when an agent fleet arrives at once.",
		func() float64 { return float64(blocked()) })
	Registry.CounterFunc("kubescrape_container_lookups_shed_total",
		"Blocking container lookups refused because the store's concurrent-waiter cap was reached.",
		func() float64 { return float64(shed()) })
	Registry.CounterFunc("kubescrape_container_lookups_drained_total",
		"Blocking container lookups answered with a retryable 503 because the process began shutting down. Without the drain they stayed parked until the process exit killed the connection mid-wait (curl reports an empty reply); this counts the answers, and it is expected to be nonzero on a rolling update, unlike kubescrape_container_lookups_shed_total.",
		func() float64 { return float64(drained()) })
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
		"Undelivered bytes currently queued in the disk buffer, per signal (what -buffer-max-bytes caps). signal=\"traces\" exists only on the trace tier with tail sampling on — the one trace payload this agent owns rather than forwards.",
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
