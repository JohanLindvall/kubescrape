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

This file is generated from `internal/obs/obs.go`. Regenerate with
`go test ./internal/obs/ -run TestMetricsDocIsCurrent -update-metrics-doc`;
`TestDocumentedMetricsExist` additionally fails if prose anywhere in the repo
names a metric or a label that is not registered.

| Metric | Labels | Description |
|---|---|---|
| `kubescrape_azure_commit_errors_total` | — | Offset commit failures (the records were delivered; a redelivery produces at-least-once duplicates). |
| `kubescrape_azure_decode_errors_total` | — | Event Hubs messages or records that could not be decoded as Azure diagnostics JSON (skipped, committed past). |
| `kubescrape_azure_dropped_total` | — | Azure diagnostic payloads dropped after a permanent collector rejection (the offsets advance past them). |
| `kubescrape_azure_exported_total` | `signal` | Azure diagnostic records exported, by signal (logs, metrics). |
| `kubescrape_azure_fetch_errors_total` | — | Kafka fetch errors from the Event Hubs consumer (retried; partial fetches are still processed). |
| `kubescrape_azure_records_total` | `kind` | Azure diagnostic records decoded from Event Hubs messages, by kind (log, metric). |
| `kubescrape_azure_token_refreshes_total` | `outcome` | Microsoft Entra token refreshes for the Event Hubs connection, by outcome (ok, error). |
| `kubescrape_buffer_backlog_bytes` | `signal` | Undelivered bytes currently queued in the disk buffer, per signal (what -buffer-max-bytes caps). |
| `kubescrape_buffer_dropped_total` | `signal` | Buffered batches dropped after a permanent collector rejection (bad payload, auth, unimplemented). |
| `kubescrape_buffer_enqueue_errors_total` | `signal` | Batches the disk buffer refused for a reason other than capacity (I/O error, closed queue, no space left on device). |
| `kubescrape_buffer_full_total` | `signal` | Batches the disk buffer refused: the undelivered backlog is at its cap, or one batch exceeds the whole cap. Back-pressure for logs (the tailer rewinds and re-reads), a lost batch for producers that cannot rewind (scrape, self-metrics, log-metrics). |
| `kubescrape_buffer_max_bytes` | `signal` | Configured disk-buffer cap per signal (0 = uncapped); backlog/max is the utilisation to alert on. |
| `kubescrape_buffer_read_errors_total` | `signal`, `lost` | Disk-buffer read failures while draining. lost=true is reported corruption the queue advanced past (its Stats carry the magnitude); lost=false left the queue in place for a retry. |
| `kubescrape_buffer_requeued_total` | `signal` | Buffered batches moved to the back of the queue after repeated transient failures (keeps one stuck batch from blocking the signal). |
| `kubescrape_buffer_segments` | `signal` | Disk-buffer segment files on disk per signal. Physical footprint can exceed the backlog by up to one segment (a delivered but unreclaimed prefix). |
| `kubescrape_buffer_truncated_bytes_total` | `signal` | Bytes the disk buffer lost to damage discovered at open (truncated, dropped or foreign segments). |
| `kubescrape_event_position_errors_total` | `operation` | Failures reading or writing the event position ConfigMap, by operation (load, save). |
| `kubescrape_event_relists_total` | `stage` | Event watches that fell back to a relist because the stored resourceVersion had aged out of the API server's watch window. |
| `kubescrape_event_watch_restarts_total` | — | Event watch restarts (a closed stream, an error, or an expired resourceVersion). |
| `kubescrape_events_dropped_total` | — | Kubernetes event batches dropped after a permanent collector rejection (the position advances past them). |
| `kubescrape_events_exported_total` | — | Kubernetes event records exported (after the rules). |
| `kubescrape_events_observed_total` | `type` | Kubernetes events received from the watch, by event type (normal, warning, other — anything else the API server reports). |
| `kubescrape_export_requests_total` | `signal`, `outcome` | OTLP export attempts by signal and outcome. |
| `kubescrape_http_requests_total` | `pattern`, `code` | Metadata API requests by pattern and status code. |
| `kubescrape_ingest_rejected_total` | — | Pushed OTLP requests refused because the concurrent in-flight bound was reached (retryable: 429 / ResourceExhausted). |
| `kubescrape_ingest_resources_total` | `outcome` | Distinct pushed identities (container id / pod uid, memoized per request) by enrichment outcome (enriched, unresolved, peer_ip). |
| `kubescrape_journal_dropped_batches_total` | — | Journal batches dropped after a permanent collector rejection (the cursor advances past them). |
| `kubescrape_journal_entries_total` | — | Journal entries exported. |
| `kubescrape_journal_restarts_total` | — | Journal reader restarts. |
| `kubescrape_journal_truncated_total` | — | Journal messages truncated at MaxEntryBytes (the record carries log.truncated). |
| `kubescrape_leader` | — | 1 while this replica holds the cluster-singleton lease, 0 otherwise; sum != 1 means split brain or nobody leading. |
| `kubescrape_log_archive_errors_total` | — | Compressed log files whose stream failed to decode mid-read (truncated gzip, trailing garbage). What decoded before the error is delivered; the remainder is unrecoverable and the archive settles. |
| `kubescrape_log_bytes_total` | — | Raw log bytes read from live files and archives. Segment replays (re-reading a rotated file's owed range after a restart or rewind) are not re-counted. |
| `kubescrape_log_enriched_total` | `format` | Log records by the enrichment strategy that matched (json, logfmt, pattern, none). |
| `kubescrape_log_entries_total` | — | Log entries exported. |
| `kubescrape_log_export_failures_total` | — | Log batch exports that failed after retries (files rewound). |
| `kubescrape_log_fifo_orphans_total` | — | Stale per-line offset entries discarded because the multiline stage dropped over-limit lines it never emitted. |
| `kubescrape_log_files` | — | Log files currently tracked. |
| `kubescrape_log_lag_bytes` | — | Largest per-file backlog: bytes on disk not yet exported and committed (per-file breakdown on /debug/tailer). |
| `kubescrape_log_lag_total_bytes` | — | Total backlog across tracked files: bytes on disk not yet exported and committed. |
| `kubescrape_log_metrics_dropped_capped_by_metric` | `metric` | Log-metric observations dropped since start because that metric's cardinality cap was reached, by metric name. A gauge carrying a since-start total (not a counter): it does not mark restarts, so use kubescrape_log_metrics_dropped_capped_total for rates and this one to name the metric. |
| `kubescrape_log_metrics_dropped_capped_total` | — | Log-metric observations dropped since start because the metric's label-set cardinality cap was reached. |
| `kubescrape_log_metrics_dropped_collision_total` | — | Log-metric observations dropped since start because of a series hash collision. |
| `kubescrape_log_metrics_dropped_nan_total` | — | Log-metric observations dropped since start because the extracted value was NaN or +/-Inf (neither is representable as a sample). |
| `kubescrape_log_oversized_dropped_total` | — | Unterminated lines discarded for exceeding the per-entry size bound (no newline within MaxEntryBytes+4096). |
| `kubescrape_log_permanent_dropped_total` | — | Log records dropped after a definitive collector rejection (retrying could not succeed; offsets advanced so the pipeline survives). |
| `kubescrape_log_prefix_lost_total` | — | Rotated-away log segments that could not be re-read (the file was deleted or compressed before its lines were exported, and no open fd survived a restart). These lines are lost. |
| `kubescrape_log_rate_limited_total` | `action` | Per-file line rate limit hits: lines discarded (action=drop) or reads paused (action=pause). |
| `kubescrape_log_rotations_total` | — | Log file rotations and truncations handled. |
| `kubescrape_log_rules_dropped_total` | — | Log records dropped by the logs rules (including sampled-away lines). |
| `kubescrape_log_scrubbed_total` | `pattern` | Log bodies redacted by a scrub pattern (one bump per pattern per record, not per match). |
| `kubescrape_log_torn_final_lines_total` | — | Unterminated final lines of RENAMED-away files (the fragment can never complete and is dropped). In-place truncation destroys its unread tail unmeasurably — there is nothing left to count — so truncation losses do not appear here or anywhere. |
| `kubescrape_log_unresolved_lost_total` | — | Log files deleted before their metadata ever resolved (the metadata service was unreachable or the container unknown for the file's whole life). Their content was never read and is lost. |
| `kubescrape_metadata_requests_total` | `outcome` | Requests to the metadata service by outcome. |
| `kubescrape_monitor_fields_ignored_total` | `kind` | Monitor upserts whose endpoints set fields kubescrape does not interpret. |
| `kubescrape_monitor_parse_errors_total` | `kind` | Monitor upserts that failed to parse and were dropped from the index. |
| `kubescrape_positions_corrupt_total` | — | Positions files that failed to parse at startup (whatever decoded is kept; the affected inputs re-read their window). Recurring bumps across restarts point at a failing disk, not a one-off crash. |
| `kubescrape_routed_payload_parts_total` | `route`, `signal` | Payload parts forwarded to a non-default routing destination. |
| `kubescrape_scrape_duration_seconds` | `pipeline` | Scrape duration by pipeline. |
| `kubescrape_scrape_malformed_total` | `pipeline` | Exposition samples dropped as malformed by pipeline (unparseable lines, histogram buckets without le, summary rows without quantile). |
| `kubescrape_scrape_name_collisions_total` | — | Data points dropped because their family name was already claimed by a metric of another shape in the same batch (a target redeclaring a family's TYPE mid-exposition). |
| `kubescrape_scrape_samples_total` | `pipeline` | Samples parsed by pipeline (before filtering). |
| `kubescrape_scrapes_total` | `pipeline`, `outcome` | Scrapes by pipeline and outcome. |
| `kubescrape_self_metadata_lookups_total` | `outcome` | Own-pod metadata lookups for -self-attributes, by outcome. |
| `kubescrape_self_metadata_resolved` | — | 1 when this process has resolved its own pod's metadata for -self-attributes, 0 while it has not. |
| `kubescrape_span_metrics_dropped_total` | — | Spans not aggregated into span metrics because the dimension-cardinality cap was reached. |
| `kubescrape_span_metrics_evicted_total` | — | Span-metric series dropped at export because their dimensions went unobserved for traceMetrics.staleAfter (this is what frees cardinality-cap slots). |
| `kubescrape_store_containers` | — | Container IDs currently indexed (including tombstones). |
| `kubescrape_store_pods` | — | Pods currently in the store (including tombstones). |
| `kubescrape_trace_spans_dropped_total` | `reason` | Ingested spans dropped by the trace sampler (probability = the consistent trace-ID decision, rate = the spans/second cap). |
| `kubescrape_transform_errors_total` | `signal` | Transform program invocations that failed (the batch is NOT exported; the error propagates to the producer's retry path). |
| `kubescrape_transform_reloads_total` | `outcome` | Transforms-file reloads by outcome (applied, failed — a failed compile keeps the last good program). |

72 metrics.
