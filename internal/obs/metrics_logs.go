package obs

import (
	"github.com/JohanLindvall/kubescrape/internal/metrics"
)

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
	// LogSweeps is the tailer's HEARTBEAT. One sweep goroutine serves every
	// file on the node, and the lag gauges below are published FROM it, so when
	// it wedges — an export retry loop against a dead collector, an open(2)
	// that blocks — they freeze at their last value and the node reads as
	// quiet. A rate of zero here is the one signal for that state.
	LogSweeps = Registry.Counter("kubescrape_log_sweeps_total",
		"Sweeps the log tailer completed: its heartbeat. The single sweep goroutine serves every log file on "+
			"the node and publishes the lag gauges itself, so when it wedges (an export retry loop against a "+
			"dead collector, an open that blocks) those gauges FREEZE at their last value rather than move — a "+
			"rate of zero here, with -logs on, is the signal that the tailer has stopped, where the lag gauges "+
			"would say nothing changed. Poll-driven at -logs-poll-interval (500ms) plus one per file event, so "+
			"the steady rate is a few per second.")
	LogFiles = Registry.Gauge("kubescrape_log_files",
		"Log files currently tracked.")
	// LogFilesUnresolved answers the operator's first question — "why is this
	// pod's log missing?" — for the half of the answers that live INSIDE the
	// tailer. A tracked file is never read before it can be attributed, so a
	// file whose container metadata will not resolve produces nothing at all
	// while every other log metric looks healthy: the file is tracked
	// (kubescrape_log_files counts it), no byte is read
	// (kubescrape_log_bytes_total does not move for it) and nothing is lost
	// (the data waits on disk). A value that stays above zero for longer than a
	// container's first seconds means the metadata service is unreachable or
	// the containers are unknown to it; the tailer names the waiting files in a
	// throttled warning and on GET /debug/tailer.
	LogFilesUnresolved = Registry.Gauge("kubescrape_log_files_unresolved",
		"Tracked log files whose metadata has not resolved yet, so nothing is read from them (their content waits on disk and is not lost).")
	// LogFilesSkipped is the OTHER half of that question: a file the discovery
	// pass saw and deliberately did not track. Every one of these was silent by
	// design, so an operator whose pod produced no logs had nothing to look at
	// — the file simply never appeared anywhere. Counted ONCE PER FILE per
	// reason (not once per discovery pass, which runs every couple of seconds),
	// so a rate is "files newly skipped", and a file that changes reason counts
	// again under the new one. The matching Debug line names the path.
	LogFilesSkipped = Registry.CounterVec("kubescrape_log_files_skipped_total",
		"Log files seen by discovery and deliberately not tracked, by reason: source_exclude (the source's exclude "+
			"globs), excluded_namespace (-logs-exclude-namespaces or the source's excludeNamespaces), "+
			"namespace_not_selected (the source's namespaces allowlist; another source may still claim the file), "+
			"unparseable_name (not a CRI <pod>_<namespace>_<container>-<id>.log name), too_old (the source's "+
			"ignoreOlder cutoff), non_regular (a FIFO/socket/device, never opened because the open would block the "+
			"sweep goroutine node-wide) and stat_error (the path was listed but could not be stat'd — an EACCES or "+
			"EIO here is a genuine collection failure, not a selection). Counted once per file per reason.", "reason")
	// LogMetadataBudgetExhausted is promscrape's ScrapeMetaBudgetExhausted one
	// pipeline over, and it exists for the same reason: a metadata lookup can
	// BLOCK server-side for the whole -metadata-wait, every file on the node is
	// resolved by the SINGLE sweep goroutine, and past the sweep's shared
	// resolve budget files are simply not reached. No request is issued, so
	// kubescrape_metadata_requests_total cannot move, and the files stay
	// unresolved with nothing at all to show for it. Counted ONCE PER SWEEP
	// (not once per unreached file) so a rate is comparable with the sweep
	// cadence.
	LogMetadataBudgetExhausted = Registry.Counter("kubescrape_log_metadata_budget_exhausted_total",
		"Tailer sweeps that ran out of their shared metadata-resolution budget, leaving files unresolved and unread until a later sweep (nothing is lost; a sustained rate means the metadata service is slow or unreachable).")
	LogRotations = Registry.Counter("kubescrape_log_rotations_total",
		"Log file rotations and truncations handled.")
	LogPrefixLost = Registry.Counter("kubescrape_log_prefix_lost_total",
		"Log content given up on as unrecoverable: a rotated-away segment that could not be re-read "+
			"(the file was deleted or compressed before its lines were exported, and no open fd survived "+
			"a restart) or that turned out SHORTER than its checkpointed range (its missing tail), a "+
			"segment or vanished file whose reads kept erring without progress past the stall budget, the "+
			"checkpointed segments of a file deleted before its metadata resolved (their lines can no "+
			"longer be attributed), a compressed archive rewritten or replaced — in-process or across a "+
			"restart — before its old stream fully shipped, or a log file found rewritten in place (same "+
			"inode, different head) when it was reopened after a restart, before its checkpointed remainder "+
			"was read. One count per given-up segment/file/archive, whatever its size; these lines are lost.")
	LogEnriched = Registry.CounterVec("kubescrape_log_enriched_total",
		"Log records by the enrichment strategy that matched (json, logfmt, pattern, none).", "format")
	LogEnrichTimeRejected = Registry.Counter("kubescrape_log_enrich_time_rejected_total",
		"Timestamps parsed from a log line that did NOT replace the producer's own (the CRI/journal/event time) "+
			"because the line's timestamp carried no zone. Such a stamp is a wall clock enrichment must read as UTC, "+
			"so a workload running with TZ set to anything else would misdate every record by that offset; the "+
			"accurate ingest time is kept instead. A timestamp that states its own offset (RFC3339, an epoch, any "+
			"zoned layout) always wins, however old it is, and a record with no producer timestamp at all takes the "+
			"parsed one either way. Also counted: a parsed time, zoned or not, that an OTLP timestamp cannot hold "+
			"(at or before 1970-01-01, or past 2262), which would otherwise wrap into a nonsense wire value; the "+
			"record keeps whatever timestamp it had.")
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
			"of lines, and the drops are attributed to the pass that first put those bytes through the chain. "+
			"The -ingest path holds the same line by a different mechanism, and its unit is per DELIVERY: the "+
			"tally is staged while the chain runs and applied only once the push is ACKED, so the retransmits "+
			"of a NACKed push cost nothing — but a push whose ack is LOST is redelivered and recounted, which "+
			"no receiver can dedupe.")
	// LogReadErrors counts sweeps that failed to read a tracked log file for a
	// reason other than "it is gone" (permission denied, EIO on a failing
	// disk, an SELinux denial). The WARN beside it is throttled per path and
	// SATURATES past a bounded number of distinct paths, so without a counter a
	// broad failure becomes invisible rather than merely quiet.
	LogReadErrors = Registry.Counter("kubescrape_log_read_errors_total",
		"Failed reads of a tracked log file (excluding the file being gone); the per-path warning is throttled, this is not.")
	LogUnresolvedLost = Registry.Counter("kubescrape_log_unresolved_lost_total",
		"Log files deleted before their metadata ever resolved (the metadata service was unreachable "+
			"or the container unknown for the file's whole life). Their content was never read and is lost.")
	LogOversizedDropped = Registry.Counter("kubescrape_log_oversized_dropped_total",
		"Unterminated lines discarded for exceeding the per-entry size bound (no newline within MaxEntryBytes+4096).")
	LogTornFinalLines = Registry.Counter("kubescrape_log_torn_final_lines_total",
		"Unterminated final lines of RENAMED-away files (the fragment can never complete and is dropped). In-place truncation destroys its unread tail unmeasurably — there is nothing left to count — so truncation losses do not appear here or anywhere.")
	// LogEntriesUnmapped is the tailer's LAST unsignalled give-up path. The
	// multiline stage emits a fully assembled entry and the tailer maps it back
	// onto the byte ranges its per-stream offset FIFO owes; if that FIFO is
	// empty the entry cannot be positioned, so it is discarded — and it used to
	// be discarded in total silence, the one drop in the package with neither a
	// counter nor a line while every sibling (torn final lines, oversized
	// lines, lost prefixes, permanent rejections) carries both.
	LogEntriesUnmapped = Registry.Counter("kubescrape_log_entries_unmapped_total",
		"Assembled multi-line log entries discarded because the file's per-stream offset FIFO held no byte "+
			"range to map them onto, so the entry could not be positioned or committed. This is UNREACHABLE "+
			"while the multiline library keeps its sum(Lines) == lines-consumed contract (pinned upstream by "+
			"FuzzCappedConservation): any nonzero value means that contract broke and joined records are being "+
			"dropped un-exported while their bytes' commit frontier moves past them — silent loss, and a bug.")
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
		"Files whose pod's kubescrape.io/logs annotation could not be honoured and was ignored: it failed to "+
			"PARSE, or the metadata service OMITTED it for size (a value over its per-annotation ceiling, named in "+
			"the pod's kubescrape.io/annotations-omitted note). Counted at metadata resolution, once per file. "+
			"Logs keep flowing under the source defaults — the failure mode this "+
			"guards against is silent: an operator edits the annotation, nothing changes, and the only signal "+
			"was one Warn line on one node. The offending files and their parse errors are listed on the "+
			"agent's GET /debug/tailer as podConfigError.")
	LogPodAttrsRefused = Registry.CounterVec("kubescrape_log_pod_attrs_refused_total",
		"Resource-attribute keys a pod's kubescrape.io/logs annotation tried to set and that were refused because "+
			"they are RESERVED: keys naming RESOLVED KUBERNETES IDENTITY (namespace, pod, container, node) and "+
			"kubescrape's own control-plane markers (the transform route marker kubescrape.route and the transform "+
			"drop marker). The annotation is authoritative about the workload's own description, never about "+
			"which object — or which tenant — the records belong to, nor about how the pipeline treats them: "+
			"k8s.namespace.name is the routing key and the route marker is honoured BEFORE the namespace globs, so "+
			"honouring either let any pod redirect its logs into another tenant. A nonzero rate is a workload "+
			"attempting it, whether by mistake or not.", "key")
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

// Journald input (agent).
var (
	JournalEntries = Registry.Counter("kubescrape_journal_entries_total",
		"Journal entries exported.")
	JournalRestarts = Registry.Counter("kubescrape_journal_restarts_total",
		"Journal reader restarts.")
	JournalTruncated = Registry.Counter("kubescrape_journal_truncated_total",
		"Journal messages truncated at MaxEntryBytes (the record carries log.truncated).")
	// JournalEntryDefects covers the read-side fallbacks that were silent: the
	// journal stores raw BYTES, so a message can be invalid UTF-8 and is
	// rewritten with U+FFFD before it can be exported, and an entry can carry
	// no realtime stamp at all, in which case the record is dated with the
	// agent's own clock. Both change the record the operator receives, and
	// neither is visible in it (a replaced byte looks like the producer wrote
	// it; a substituted timestamp looks authoritative). One CounterVec rather
	// than two families because they are the same class of event and an
	// operator alerts on the family, not on either value.
	//
	// Deliberately NOT here: an unknown PRIORITY, which the converter maps to
	// severity Unspecified. That one IS visible — in the exported record's own
	// severity field — so it needs no counter to be noticed.
	JournalEntryDefects = Registry.CounterVec("kubescrape_journal_entry_defects_total",
		"Journal entries the reader had to repair before exporting, by defect: invalid_utf8 (raw bytes the journal "+
			"stored that are not valid UTF-8; each is replaced with U+FFFD, so the exported body differs from what "+
			"the producer wrote) and no_timestamp (the entry carried no realtime stamp, so the record is dated with "+
			"the agent's clock at read time instead of the producer's). Each carries a throttled warning naming the "+
			"unit.", "defect")
	JournalExportFailures = Registry.Counter("kubescrape_journal_export_failures_total",
		"Journal batch exports that failed and are being retried IN PLACE. The batch is kept and never re-read, "+
			"so this counts ATTEMPTS — not lost entries, and not re-reads: a steady rate is a collector outage the "+
			"reader is riding out with its cursor uncommitted, and the loss counter is "+
			"kubescrape_journal_dropped_batches_total. Deliberately NOT kubescrape_log_export_failures_total, which "+
			"is the tailer's files-rewound counter and cannot apply here — journald rewinds no file, and the "+
			"singleton that reads a journal typically runs with -logs=false, so those increments landed on a "+
			"metric whose help described something that had not happened.")
	JournalDropped = Registry.Counter("kubescrape_journal_dropped_batches_total",
		"Journal batches dropped after a permanent collector rejection (the cursor advances past them).")
	JournalDroppedRecords = Registry.Counter("kubescrape_journal_dropped_records_total",
		"Journal records lost with those batches. The magnitude of the loss: a batch is up to Config.BatchSize entries.")
)

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
// feature is off". The counts themselves are per-set instance state
// (DroppedNaN() and its siblings), but the PUBLISHED family is not per set:
// a repeated registration reuses the one series of that name (the Registry's
// documented dedupe, metrics.Registry.byName), so two sets in one process
// publish the SUM of their counts. Production has one set per process.
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
		"Log-metric observations dropped since start because the value was NaN or +/-Inf (neither is representable as a sample): extracted by "+
			"a logMetrics rule's value/valueRegexp, or passed to a transform script's emit_metric. The Warn it raises names the metric and "+
			"says which of the two supplied the value.",
		func() float64 { return float64(set.DroppedNaN()) })
	Registry.CounterFunc("kubescrape_log_metrics_dropped_negative_total",
		"Log-metric observations dropped since start because the extracted value was NEGATIVE on a metric whose exported form is monotonic "+
			"(a counter, which renders as an OTLP Sum with IsMonotonic(true), or a summary, whose sum reaches Prometheus as the counter-typed "+
			"<name>_sum). Folding a decrease into either on an unchanged start timestamp is a counter reset this process never declared, which "+
			"rate()/increase() reads as one and adds the whole new value on top of everything already counted -- so it is refused instead. A "+
			"SEPARATE series from the NaN one because the remedy is separate: a signed quantity belongs on type: gauge or on a histogram whose "+
			"bounds cover it. Nonzero means a logMetrics rule's value/valueRegexp, or a transform script's emit_metric, is feeding a signed "+
			"quantity into the wrong metric type; the Warn it raises names the metric and says which of the two supplied the value.",
		func() float64 { return float64(set.DroppedNegative()) })
	Registry.CounterFunc("kubescrape_log_metrics_dropped_undelivered_total",
		"Undelivered log-metric SAMPLES (data points) dropped because the re-offer buffer filled or the collector "+
			"rejected their chunk PERMANENTLY. A live series needs no buffer: the store still holds it and the next "+
			"export re-reads its running value. Taking a snapshot is DESTRUCTIVE for the rest (it seals aggregation "+
			"windows, zeroes idled gauges and deletes expired series), so a transiently failed export retains THOSE "+
			"for the next one, dropping the oldest first past the bound; this counts what a collector outage longer "+
			"than that buffer could hold, plus every sample of a definitively rejected chunk, which retrying could "+
			"never deliver. These are genuinely lost observations — the ones the retention cannot save.",
		func() float64 { return float64(set.DroppedUndelivered()) })
}
