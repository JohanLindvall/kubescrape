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
| `kubescrape_buffer_backlog_bytes` | `signal` | Undelivered bytes currently queued in the disk buffer, per signal (what -buffer-max-bytes caps). |
| `kubescrape_buffer_dropped_total` | `signal` | Buffered batches dropped after a permanent collector rejection (bad payload, auth, unimplemented). |
| `kubescrape_buffer_full_total` | `signal` | Batches the disk buffer refused because the undelivered backlog is at its cap: back-pressure for logs (the tailer rewinds and re-reads), a lost batch for producers that cannot rewind (scrape, self-metrics, log-metrics). |
| `kubescrape_buffer_max_bytes` | `signal` | Configured disk-buffer cap per signal (0 = uncapped); backlog/max is the utilisation to alert on. |
| `kubescrape_buffer_read_errors_total` | `signal`, `lost` | Disk-buffer read failures while draining (the head frame could not be read; lost=true means the segment was gone and its frames were skipped). |
| `kubescrape_buffer_requeued_total` | `signal` | Buffered batches moved to the back of the queue after repeated transient failures (keeps one stuck batch from blocking the signal). |
| `kubescrape_buffer_segments` | `signal` | Disk-buffer segment files on disk per signal. Physical footprint can exceed the backlog by up to one segment (a delivered but unreclaimed prefix). |
| `kubescrape_buffer_truncated_bytes_total` | `signal` | Bytes discarded by truncating a damaged or torn disk-buffer segment tail at open. |
| `kubescrape_export_requests_total` | `signal`, `outcome` | OTLP export attempts by signal and outcome. |
| `kubescrape_http_requests_total` | `pattern`, `code` | Metadata API requests by pattern and status code. |
| `kubescrape_ingest_resources_total` | `outcome` | Distinct pushed identities (container id / pod uid, memoized per request) by enrichment outcome (enriched, unresolved, peer_ip). |
| `kubescrape_journal_dropped_batches_total` | — | Journal batches dropped after a permanent collector rejection (the cursor advances past them). |
| `kubescrape_journal_entries_total` | — | Journal entries exported. |
| `kubescrape_journal_restarts_total` | — | Journal reader restarts. |
| `kubescrape_journal_truncated_total` | — | Journal messages truncated at MaxEntryBytes (the record carries log.truncated). |
| `kubescrape_log_archive_errors_total` | — |  |
| `kubescrape_log_bytes_total` | — | Raw log bytes read. |
| `kubescrape_log_enriched_total` | `format` | Log records by the enrichment strategy that matched (json, logfmt, pattern, none). |
| `kubescrape_log_entries_total` | — | Log entries exported. |
| `kubescrape_log_export_failures_total` | — | Log batch exports that failed after retries (files rewound). |
| `kubescrape_log_fifo_orphans_total` | — | Stale per-line offset entries discarded because the multiline stage dropped over-limit lines it never emitted. |
| `kubescrape_log_files` | — | Log files currently tracked. |
| `kubescrape_log_lag_bytes` | — | Largest per-file backlog: bytes on disk not yet exported and committed (per-file breakdown on /debug/tailer). |
| `kubescrape_log_lag_total_bytes` | — | Total backlog across tracked files: bytes on disk not yet exported and committed. |
| `kubescrape_log_metrics_dropped_capped_total` | — | Log-metric observations dropped since start because the metric's label-set cardinality cap was reached. |
| `kubescrape_log_metrics_dropped_collision_total` | — | Log-metric observations dropped since start because of a series hash collision. |
| `kubescrape_log_metrics_dropped_nan_total` | — | Log-metric observations dropped since start because the extracted value was NaN. |
| `kubescrape_log_oversized_dropped_total` | — | Unterminated lines discarded for exceeding the per-entry size bound (no newline within MaxEntryBytes+4096). |
| `kubescrape_log_prefix_lost_total` | — |  |
| `kubescrape_log_rate_limited_total` | `action` | Per-file line rate limit hits: lines discarded (action=drop) or reads paused (action=pause). |
| `kubescrape_log_rotations_total` | — | Log file rotations and truncations handled. |
| `kubescrape_log_rules_dropped_total` | — | Log records dropped by the logs rules (including sampled-away lines). |
| `kubescrape_log_scrubbed_total` | `pattern` | Log bodies redacted by a scrub pattern (one bump per pattern per record, not per match). |
| `kubescrape_log_torn_final_lines_total` | — | Unterminated final lines of rotated-away files (the fragment can never complete and is dropped). |
| `kubescrape_log_unresolved_lost_total` | — |  |
| `kubescrape_metadata_requests_total` | `outcome` | Requests to the metadata service by outcome. |
| `kubescrape_monitor_fields_ignored_total` | `kind` | Monitor upserts whose endpoints set fields kubescrape does not interpret. |
| `kubescrape_monitor_parse_errors_total` | `kind` | Monitor upserts that failed to parse and were dropped from the index. |
| `kubescrape_positions_corrupt_total` | — |  |
| `kubescrape_routed_payload_parts_total` | `route`, `signal` | Payload parts forwarded to a non-default routing destination. |
| `kubescrape_scrape_duration_seconds` | `pipeline` | Scrape duration by pipeline. |
| `kubescrape_scrape_malformed_total` | `pipeline` | Exposition samples dropped as malformed by pipeline (unparseable lines, histogram buckets without le, summary rows without quantile). |
| `kubescrape_scrape_name_collisions_total` | — | Data points dropped because their family name was already claimed by a metric of another shape in the same batch (a target redeclaring a family's TYPE mid-exposition). |
| `kubescrape_scrape_samples_total` | `pipeline` | Samples parsed by pipeline (before filtering). |
| `kubescrape_scrapes_total` | `pipeline`, `outcome` | Scrapes by pipeline and outcome. |
| `kubescrape_span_metrics_dropped_total` | — | Spans not aggregated into span metrics because the dimension-cardinality cap was reached. |
| `kubescrape_span_metrics_evicted_total` | — | Span-metric series dropped at export because their dimensions went unobserved for traceMetrics.staleAfter (this is what frees cardinality-cap slots). |
| `kubescrape_store_containers` | — | Container IDs currently indexed (including tombstones). |
| `kubescrape_store_pods` | — | Pods currently in the store (including tombstones). |
| `kubescrape_trace_spans_dropped_total` | `reason` | Ingested spans dropped by the trace sampler (probability = the consistent trace-ID decision, rate = the spans/second cap). |
| `kubescrape_transform_errors_total` | `signal` | Transform program invocations that failed (the batch is NOT exported; the error propagates to the producer's retry path). |
| `kubescrape_transform_reloads_total` | `outcome` | Transforms-file reloads by outcome (applied, failed — a failed compile keeps the last good program). |

52 metrics.
