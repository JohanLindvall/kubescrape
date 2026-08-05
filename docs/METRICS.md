# Metrics

kubescrape's own metrics (`kubescrape_*`) are **pushed over OTLP** by
default: exported on `-self-metrics-interval` (default 1m) under the
exporting process's own resource identity — `service.name=kubescrape` plus
hostname for the metadata service, `service.name=kubescrape-agent` plus
`k8s.node.name` for the agent, both stamped with `service.version`.

With `-self-metrics-interval=0` the push is off and the same metrics are
served on the `-metrics-listen` port's `/metrics` instead, beside the Go
runtime and process collectors (`go_*`, `process_*`) that always live
there. One knob selects the delivery modality, so the same series never
ship over both paths.

Every ScopeMetrics/ScopeLogs kubescrape emits now carries an
instrumentation-scope NAME and VERSION (the build version, the same string
as `service.version`). Log-derived metrics — the `logMetrics` section's
output — shipped with an empty scope name until now; every other producer
already named one. **Wire-visible**: a translation that turns the scope into
labels (`otel_scope_name`, `otel_scope_version`) sees new label values, so
those streams split at this upgrade — and, because the version is part of
it, at every release boundary thereafter. Group on the metric name and its
own labels if you need continuity.

Cumulative points (every counter, summary and histogram here) now carry a
real `StartTimeUnixNano`: the moment that stream began accumulating, not the
export time. `StartTimeUnixNano == TimeUnixNano` is the OTLP encoding for a
point that just RESET, and stamping it on every push made cumulative-to-delta
consumers (Datadog, Dynatrace, AWS EMF) report the whole running total as
each interval's delta, while Google Cloud rejected the points outright.
Prometheus/Mimir ignore the field by default and are unaffected.

This file is generated from `internal/obs/obs.go`. Regenerate with
`go test ./internal/obs/ -run TestMetricsDocIsCurrent -update-metrics-doc`;
`TestDocumentedMetricsExist` additionally fails if prose anywhere in the repo
names a metric or a label that is not registered.

## Renamed in this release

All of these are **wire-visible**: dashboards and alert rules selecting the
old names match nothing, silently. Names here are written WITHOUT their
`kubescrape_` prefix, so that the doc-check which fails on prose naming an
unregistered metric stays strict; the table below carries the full names.

| Was | Is | Why |
|---|---|---|
| `log_lag_bytes` (per-file max) | `log_lag_max_bytes` | `_total` is reserved for counters and both are gauges, so the name promised a counter — and the name WITHOUT the qualifier was the one that was not the total. |
| `log_lag_total_bytes` (sum) | `log_lag_bytes` | see above |
| `buffer_dropped_total` | `buffer_dropped_batches_total` + `buffer_dropped_records_total` | a batch is 1..1024 records, so the counter operators are told to alert on could not size the loss |
| `events_dropped_total` | `events_dropped_batches_total` + `events_dropped_records_total` | same |
| `azure_dropped_total` | `azure_dropped_batches_total` + `azure_dropped_records_total` | same |
| (new) | `journal_dropped_records_total` | beside the existing `journal_dropped_batches_total` |
| `azure_records_total{kind="log"/"metric"}` | `azure_records_total{signal="logs"/"metrics"}` | every other producer dimensions by `signal` with plural values; the decoded and exported counts of one pipeline shared no label to join on |
| `log_metrics_dropped_capped_by_metric` (gauge) and the unlabeled `log_metrics_dropped_capped_total` (counter) | `log_metrics_dropped_capped_total{metric}` | the label name belonged in a label, not inside the metric name, and a gauge carrying a monotonic since-start total hides the reset at a restart. `sum()` over the label is the old aggregate — but the family is now ABSENT rather than 0 until something is dropped, the label set being data-driven. |

`log_permanent_dropped_total` is unchanged: it already counts RECORDS, and it
is the documented alert for the non-buffered tailer.

## All metrics

| Metric | Labels | Description |
|---|---|---|
| `kubescrape_azure_commit_errors_total` | — | Offset commit failures (the records were delivered; a redelivery produces at-least-once duplicates). |
| `kubescrape_azure_decode_errors_total` | — | Event Hubs messages or records that could not be decoded as Azure diagnostics JSON (skipped, committed past). |
| `kubescrape_azure_dropped_batches_total` | — | Azure diagnostic payloads dropped after a permanent collector rejection (the offsets advance past them). |
| `kubescrape_azure_dropped_records_total` | `signal` | Azure diagnostic records (log records or metric data points) lost with those payloads, by signal. |
| `kubescrape_azure_exported_total` | `signal` | Azure diagnostic records exported, by signal (logs, metrics). |
| `kubescrape_azure_fetch_errors_total` | — | Kafka fetch errors from the Event Hubs consumer (retried; partial fetches are still processed). |
| `kubescrape_azure_records_total` | `signal` | Azure diagnostic records decoded from Event Hubs messages, by signal (logs, metrics). |
| `kubescrape_azure_token_refreshes_total` | `outcome` | Microsoft Entra token refreshes for the Event Hubs connection, by outcome (ok, error). |
| `kubescrape_buffer_backlog_bytes` | `signal` | Undelivered bytes currently queued in the disk buffer, per signal (what -buffer-max-bytes caps). signal="traces" exists only on the trace tier with tail sampling on — the one trace payload this agent owns rather than forwards. |
| `kubescrape_buffer_dropped_batches_total` | `signal` | Buffered batches dropped after a permanent collector rejection (bad payload, auth, unimplemented). |
| `kubescrape_buffer_dropped_records_total` | `signal` | Records lost with those batches: log records, metric data points or spans, by signal. A batch whose payload no longer DECODES is counted in kubescrape_buffer_dropped_batches_total only — its record count is not recoverable — so this is a lower bound whenever kubescrape_buffer_read_errors_total is also moving. |
| `kubescrape_buffer_enqueue_errors_total` | `signal` | Batches the disk buffer refused for a reason other than capacity (I/O error, closed queue, no space left on device). |
| `kubescrape_buffer_full_total` | `signal` | Batches the disk buffer refused: the undelivered backlog is at its cap, or one batch exceeds the whole cap. Back-pressure for logs (the tailer rewinds and re-reads), a lost batch for producers that cannot rewind (scrape, self-metrics, log-metrics). |
| `kubescrape_buffer_max_bytes` | `signal` | Configured disk-buffer cap per signal (0 = uncapped); backlog/max is the utilisation to alert on. |
| `kubescrape_buffer_read_errors_total` | `signal`, `lost` | Disk-buffer read failures while draining. lost=true is reported corruption the queue advanced past (its Stats carry the magnitude); lost=false left the queue in place for a retry. |
| `kubescrape_buffer_requeued_total` | `signal` | Buffered batches moved to the back of the queue after repeated transient failures (keeps one stuck batch from blocking the signal). |
| `kubescrape_buffer_segments` | `signal` | Disk-buffer segment files on disk per signal. Physical footprint can exceed the backlog by up to one segment (a delivered but unreclaimed prefix). |
| `kubescrape_buffer_truncated_bytes_total` | `signal` | Bytes the disk buffer lost to damage discovered at open (truncated, dropped or foreign segments). |
| `kubescrape_container_lookups_blocked` | — | Container lookups currently blocked waiting for a container ID to appear. |
| `kubescrape_container_lookups_shed_total` | — | Blocking container lookups refused because the store's concurrent-waiter cap was reached. |
| `kubescrape_event_position_errors_total` | `operation` | Failures reading or writing the event position ConfigMap, by operation (load, save). |
| `kubescrape_event_relists_total` | `stage` | Event watches that fell back to a relist because the stored resourceVersion had aged out of the API server's watch window. |
| `kubescrape_event_watch_restarts_total` | — | Event watch restarts (a closed stream, an error, or an expired resourceVersion). |
| `kubescrape_events_dropped_batches_total` | — | Kubernetes event batches dropped after a permanent collector rejection (the position advances past them). |
| `kubescrape_events_dropped_records_total` | — | Kubernetes event records lost with those batches (the magnitude of the loss the batch counter only signals). |
| `kubescrape_events_exported_total` | — | Kubernetes event records exported (after the rules). |
| `kubescrape_events_observed_total` | `type` | Kubernetes events received from the watch, by event type (normal, warning, other — anything else the API server reports). |
| `kubescrape_export_rejected_records_total` | `signal` | Records the collector REJECTED inside a payload it otherwise accepted (OTLP partial_success), by signal. The export succeeded, so every producer advanced its offset, cursor or position past them — these are lost, permanently, and retrying cannot help (OTLP defines them as invalid rather than deferred). Any nonzero rate means telemetry is being discarded downstream; the collector's own message is on the accompanying warning. |
| `kubescrape_export_requests_total` | `signal`, `outcome` | OTLP export attempts by signal and outcome. |
| `kubescrape_http_requests_total` | `pattern`, `code` | Metadata API requests by pattern and status code. |
| `kubescrape_informer_watch_errors_total` | `resource` | List/watch failures reported by the informers, by resource. |
| `kubescrape_ingest_rejected_total` | — | Pushed OTLP requests refused because a receiver admission bound was reached — concurrent in-flight pushes or buffered payload bytes (retryable: 429 / ResourceExhausted). |
| `kubescrape_ingest_resources_total` | `outcome` | Distinct pushed identities (container id / pod uid, memoized per request) by enrichment outcome. enriched = an id resolved; peer_ip = no id, attributed by the connection's source address; peer_ip_rejected = that address resolved to the RECEIVER's own workload, so it was rewritten in flight (a proxy, a mesh sidecar, or an internal hop addressed to the application port) and nothing was attributed — anything above zero means peer-IP attribution cannot work on that path; unresolved = nothing identified the sender; split_capped = the push named more distinct objects than one payload may inflate into (maxSplitGroups), so the remainder shares the sender's resource unenriched rather than costing one full resource copy each. |
| `kubescrape_journal_dropped_batches_total` | — | Journal batches dropped after a permanent collector rejection (the cursor advances past them). |
| `kubescrape_journal_dropped_records_total` | — | Journal records lost with those batches. The magnitude of the loss: a batch is up to Config.BatchSize entries. |
| `kubescrape_journal_entries_total` | — | Journal entries exported. |
| `kubescrape_journal_restarts_total` | — | Journal reader restarts. |
| `kubescrape_journal_truncated_total` | — | Journal messages truncated at MaxEntryBytes (the record carries log.truncated). |
| `kubescrape_leader` | — | 1 while this replica holds the cluster-singleton lease, 0 otherwise; sum != 1 means split brain or nobody leading. |
| `kubescrape_log_archive_errors_total` | — | Compressed log files whose stream failed to decode mid-read (truncated gzip, trailing garbage). What decoded before the error is delivered; the remainder is unrecoverable and the archive settles. |
| `kubescrape_log_bytes_total` | — | Raw log bytes read from live files and archives. Segment replays (re-reading a rotated file's owed range after a restart or rewind) are not re-counted. |
| `kubescrape_log_drain_errors_total` | `source` | Reads that failed part-way through DRAINING a file incarnation that is going away (a rotated inode, a compressed archive). The drain cannot be retried — the next sweep would fail identically while holding the fd — so the unread remainder of that incarnation is unrecoverable and lost. Distinct from a clean EOF, which is the drain succeeding. |
| `kubescrape_log_enriched_total` | `format` | Log records by the enrichment strategy that matched (json, logfmt, pattern, none). |
| `kubescrape_log_entries_total` | — | Log entries exported. |
| `kubescrape_log_export_failures_total` | — | Log batch exports that failed after retries (files rewound). |
| `kubescrape_log_fifo_orphans_total` | — | Stale per-line offset entries discarded because the multiline stage dropped over-limit lines it never emitted. |
| `kubescrape_log_files` | — | Log files currently tracked. |
| `kubescrape_log_lag_bytes` | — | Total backlog across tracked files: bytes on disk not yet exported and committed. |
| `kubescrape_log_lag_max_bytes` | — | Largest per-file backlog: bytes on disk not yet exported and committed (per-file breakdown on /debug/tailer). |
| `kubescrape_log_metrics_dropped_capped_total` | `metric` | Log-metric observations dropped because that metric's label-set cardinality cap was reached, by metric name. sum() over the label is the total. Absent until something is dropped: the label set is data-driven. |
| `kubescrape_log_metrics_dropped_collision_total` | — | Log-metric observations dropped since start because of a series hash collision. |
| `kubescrape_log_metrics_dropped_nan_total` | — | Log-metric observations dropped since start because the extracted value was NaN or +/-Inf (neither is representable as a sample). |
| `kubescrape_log_metrics_dropped_undelivered_total` | — | Undelivered log-metric resources dropped because the re-offer buffer filled. Taking a snapshot is DESTRUCTIVE (it seals aggregation windows, zeroes idled samples and deletes expired ones), so a failed export retains its samples for the next one; this counts what a collector outage longer than that buffer could hold. These are genuinely lost observations — the only ones the retention cannot save. |
| `kubescrape_log_oversized_dropped_total` | — | Unterminated lines discarded for exceeding the per-entry size bound (no newline within MaxEntryBytes+4096). |
| `kubescrape_log_permanent_dropped_total` | — | Log records dropped after a definitive collector rejection (retrying could not succeed; offsets advanced so the pipeline survives). |
| `kubescrape_log_pod_attrs_refused_total` | `key` | Resource-attribute keys a pod's kubescrape.io/logs annotation tried to set that name RESOLVED KUBERNETES IDENTITY (namespace, pod, container, node) and were refused. The annotation is authoritative about the workload's own description, never about which object — or which tenant — the records belong to: k8s.namespace.name is the routing key, so honouring it let any pod redirect its logs into another tenant. A nonzero rate is a workload attempting it, whether by mistake or not. |
| `kubescrape_log_prefix_lost_total` | — | Rotated-away log segments that could not be re-read (the file was deleted or compressed before its lines were exported, and no open fd survived a restart). These lines are lost. |
| `kubescrape_log_rate_limited_total` | `action` | Per-file line rate limit hits: lines discarded (action=drop) or reads paused (action=pause). |
| `kubescrape_log_rotations_total` | — | Log file rotations and truncations handled. |
| `kubescrape_log_rules_dropped_total` | — | Log records dropped by the logs rules (including sampled-away lines). |
| `kubescrape_log_scrubbed_total` | `pattern` | Log bodies redacted by a scrub pattern (one bump per pattern per record, not per match). |
| `kubescrape_log_torn_final_lines_total` | — | Unterminated final lines of RENAMED-away files (the fragment can never complete and is dropped). In-place truncation destroys its unread tail unmeasurably — there is nothing left to count — so truncation losses do not appear here or anywhere. |
| `kubescrape_log_unresolved_lost_total` | — | Log files deleted before their metadata ever resolved (the metadata service was unreachable or the container unknown for the file's whole life). Their content was never read and is lost. |
| `kubescrape_metadata_requests_total` | `outcome` | Requests to the metadata service by outcome. |
| `kubescrape_monitor_fields_ignored_total` | `kind` | Monitor upserts whose endpoints set fields kubescrape does not interpret. |
| `kubescrape_monitor_namespace_refused_total` | `kind` | Monitor upserts ignored because their namespace is not permitted by -monitor-namespaces (an informer re-delivery re-counts the same monitor, exactly like the sibling monitor_* counters). |
| `kubescrape_monitor_parse_errors_total` | `kind` | Monitor upserts that failed to parse and were dropped from the index. |
| `kubescrape_positions_corrupt_total` | — | Positions files that failed to parse at startup (whatever decoded is kept; the affected inputs re-read their window). Recurring bumps across restarts point at a failing disk, not a one-off crash. |
| `kubescrape_routed_payload_parts_total` | `route`, `signal` | Payload parts forwarded to a non-default routing destination. |
| `kubescrape_scrape_auth_failures_total` | `reason` | Failed /v1/scrape-auth Secret resolutions by cause (not_found = no such Secret or key; upstream = forbidden, timeout or unreachable API server; not_utf8 = value cannot be served as a JSON string). |
| `kubescrape_scrape_duration_seconds` | `pipeline` | Scrape duration by pipeline. |
| `kubescrape_scrape_malformed_total` | `pipeline` | Exposition samples dropped as malformed by pipeline (unparseable lines, histogram buckets without le, summary rows without quantile). |
| `kubescrape_scrape_name_collisions_total` | — | Data points dropped because their family name was already claimed by a metric of another shape in the same batch (a target redeclaring a family's TYPE mid-exposition). |
| `kubescrape_scrape_samples_dropped_total` | `pipeline`, `reason` | Parsed samples discarded before conversion, by pipeline and by what discarded them: filter = the config's metrics keep/drop rules, relabel = a monitor's metricRelabelings. |
| `kubescrape_scrape_samples_total` | `pipeline` | Samples parsed by pipeline (before filtering). |
| `kubescrape_scrapes_total` | `pipeline`, `outcome` | Scrapes by pipeline and outcome. |
| `kubescrape_self_metadata_lookups_total` | `outcome` | Own-pod metadata lookups for -self-attributes, by outcome. |
| `kubescrape_self_metadata_resolved` | — | 1 when this process has resolved its own pod's metadata for -self-attributes, 0 while it has not. |
| `kubescrape_service_graph_completed_total` | — | Edges completed by BOTH halves arriving within serviceGraph.wait. The denominator for the loss counters: an expiry or drop rate only means something against the rate of pairings that worked. |
| `kubescrape_service_graph_dropped_total` | — | Edges not aggregated because the serviceGraph.maxCardinality series cap was reached (existing edges keep reporting; a new one is lost until eviction frees a slot). |
| `kubescrape_service_graph_evicted_total` | — | Edge series dropped at export because they went unobserved for serviceGraph.staleAfter (this is what frees cardinality-cap slots). |
| `kubescrape_service_graph_expired_total` | — | Half-edges that expired after serviceGraph.wait without their partner arriving. |
| `kubescrape_service_graph_loops_blocked_total` | — | Spans in application pushes refused for carrying the internal forwarded marker: an internal hop addressed to the tier's APPLICATION port instead of its authenticated receiver, which without the refusal would re-enrich and re-shard every span on every hop until the network is the incident. Always zero in a correct deployment; anything else is a config error to fix now. |
| `kubescrape_service_graph_pending_edges` | — | Half-edges currently awaiting their partner. serviceGraph.maxItems caps this; at the cap spans are refused (see kubescrape_service_graph_store_full_total), so the ratio against the cap is the leading indicator. |
| `kubescrape_service_graph_sends_failed_total` | — | Failed internal hops (one per owning shard per batch). Every one of them FAILS the application's push — the entry shard holds the only copy of those spans — so this moves together with the senders' export errors, and a rate that tracks one shard means that shard is down or its token is wrong. |
| `kubescrape_service_graph_spans_forwarded_total` | — | Spans this shard handed to ANOTHER shard because that shard owns their trace, and which it accepted. Roughly (N-1)/N of everything pushed to this pod on an N-shard tier: the remainder is kubescrape_service_graph_spans_local_total. This is the tier's internal bandwidth. |
| `kubescrape_service_graph_spans_local_total` | — | Spans this shard already owned and kept in-process, taking no second hop. Its ratio against the forwarded count should track 1/N for an N-shard tier; a persistent skew means the ring is unbalanced or one shard is receiving most of the pushes. |
| `kubescrape_service_graph_spans_unkeyed_total` | — | Pushed spans with no trace id. They cannot be hashed onto the ring, so they are kept locally and exported from here; they can never pair into an edge. A moving rate means an SDK is emitting malformed spans. |
| `kubescrape_service_graph_store_full_total` | — | Spans dropped because the pairing store held serviceGraph.maxItems half-edges (the request cannot become an edge). |
| `kubescrape_service_graph_unkeyable_total` | — | Spans that could not be keyed for pairing: no trace id, or a client/producer span with no span id of its own. Never stored — every zero id shares one key space, so they would cross-pair unrelated requests into invented edges (a zero-id client span keys exactly where every ROOT SERVER span of its trace does). A moving rate means an SDK is emitting malformed spans. |
| `kubescrape_service_graph_virtual_node_total` | — | Half-edges that expired unpaired but named their far side through serviceGraph.virtualNodePeerAttributes, and so still reached the graph (as a virtual-node edge). The remainder of kubescrape_service_graph_expired_total is the genuinely lost part — nothing named the missing side, so that request is on no edge at all. |
| `kubescrape_span_metrics_dropped_total` | — | Spans not aggregated into span metrics because the dimension-cardinality cap was reached. |
| `kubescrape_span_metrics_evicted_total` | — | Span-metric series dropped at export because their dimensions went unobserved for traceMetrics.staleAfter (this is what frees cardinality-cap slots). |
| `kubescrape_store_containers` | — | Container IDs currently indexed (including tombstones). |
| `kubescrape_store_pods` | — | Pods currently in the store (including tombstones). |
| `kubescrape_tail_sampling_buffered_spans` | — | Spans currently held in the tail-sampling buffer. These are acked to their senders but not yet decided and not durable anywhere (a DECIDED keep is spooled when -buffer-dir is set; an undecided one is not): this is what a hard kill of this pod would lose. tailSampling.maxSpans caps it, and is itself checked against the pod's memory limit at startup — the likeliest hard kill here is the OOM this buffer causes. |
| `kubescrape_tail_sampling_buffered_traces` | — | Traces currently assembling in the tail-sampling buffer, awaiting their decision. tailSampling.maxTraces caps it; at the cap the oldest is decided early (kubescrape_tail_sampling_early_decisions_total). |
| `kubescrape_tail_sampling_cache_evictions_total` | — | Verdicts evicted from the decision cache by its SIZE cap while still LIVE (evicting an entry already past its TTL is the cache reclaiming space, not a signal, and is not counted). A span arriving for an evicted trace starts a fresh window AND re-charges the rateLimiting and composite budgets — the cache remembers a trace was charged for as long as it holds the entry at all, so eviction is the only thing that still double-charges. Raise tailSampling.decisionCacheSize if this moves. |
| `kubescrape_tail_sampling_early_decisions_total` | `reason` | Traces decided BEFORE their decisionWait elapsed, by the bound that forced it (spans_per_trace, max_traces, max_spans) or shutdown (a graceful stop flushing the buffer). An early decision judges the spans present, so it degrades gracefully — a slow trace can be missed, a fast one is never invented — but a sustained rate against a bound means that bound is sized below the shard's span rate. |
| `kubescrape_tail_sampling_late_spans_total` | `outcome` | Spans that arrived after their trace was decided and followed the cached verdict, by outcome (kept = forwarded immediately, dropped). A large kept+dropped share means decisionWait is shorter than the spread of a trace's arrival. |
| `kubescrape_tail_sampling_spans_total` | `outcome` | Spans leaving the tail-sampling buffer, by fate: kept = the trace was sampled and the export was acked (with -buffer-dir, acked means SPOOLED — a decided keep is durable and a collector outage becomes a backlog); dropped = the trace was not sampled; lost = the trace was sampled but neither the collector nor the disk buffer took it, and nothing else holds a copy (the sender was acked when the spans were buffered). A moving lost rate is data loss, not back-pressure; without -buffer-dir a collector outage produces it directly, with one it means the spool itself refused the payload. |
| `kubescrape_tail_sampling_traces_total` | `decision`, `policy` | Traces decided by the tail sampler, by verdict (keep, drop) and by the policy that decided. policy="none" is the default drop — no policy had an opinion — which is what a policy list matching nothing looks like. Every configured policy gets its series at startup, so a policy that has never fired reads as zero rather than as absent. This counts DECISIONS: whether the kept trace then reached the collector is kubescrape_tail_sampling_spans_total. |
| `kubescrape_trace_spans_dropped_total` | `reason` | Ingested spans dropped by the trace sampler (probability = the consistent trace-ID decision, rate = the spans/second cap). |
| `kubescrape_transform_dropped_total` | `signal` | Records a transform script called drop() on, by signal: log records, metric data points (a dropped metric counts all of its points) and spans. |
| `kubescrape_transform_errors_total` | `signal` | Transform program invocations that failed (the batch is NOT exported; the error propagates to the producer's retry path). |
| `kubescrape_transform_reloads_total` | `outcome` | Transforms-file reloads by outcome (applied, failed — a failed compile keeps the last good program). |

106 metrics.
