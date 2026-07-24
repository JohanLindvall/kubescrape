# Configuration reference

kubescrape consists of two binaries built from one image:

* **`kubescrape`** — the metadata service (Deployment). Watches the
  Kubernetes API and serves pod/container metadata and scrape targets over
  HTTP.
* **`kubescrape-agent`** — the per-node agent (DaemonSet). Tails container
  logs and scrapes Prometheus targets, exporting OTLP.

Everything is configured through flags plus one optional unified YAML file on
the agent (`-config`, with `resourceAttributes`, `logs`, `logAttributes`,
`logMetrics`, `logScrubbing`, `metrics`, `traceMetrics`, `traceSampling` and
`routing` sections) — plus, optionally, a separate hot-reloaded Starlark
transforms file (`-transforms-file`). The
[Helm chart](../charts/kubescrape) exposes all of it as values; the raw
manifests live in [deploy/](../deploy).

- [Metadata service](#metadata-service)
- [Agent: general](#agent-general)
- [Agent: OTLP export](#agent-otlp-export)
- [Agent: log collection](#agent-log-collection)
- [Unified config file (`-config`)](#unified-config-file)
- [Agent: log sources](#agent-log-sources)
- [Agent: journald](#agent-journald)
- [Agent: log attributes](#agent-log-attributes)
- [Agent: log metrics](#agent-log-metrics)
- [Agent: log scrubbing (PII)](#agent-log-scrubbing)
- [Agent: OTLP ingest](#agent-otlp-ingest)
- [Agent: trace sampling](#agent-trace-sampling)
- [Agent: metrics scraping](#agent-metrics-scraping)
- [Agent: kubelet scrapes (cadvisor, node)](#agent-kubelet-scrapes)
- [Agent: host metrics](#agent-host-metrics)
- [Agent: transforms (Starlark)](#agent-transforms-starlark)
- [Agent: routing](#agent-routing)
- [Resource attributes](#resource-attributes)
- [Metrics config](#metrics-config)
- [Scrape annotations](#scrape-annotations)
- [Helm values](#helm-values)
- [Complete example](#complete-example)

## Metadata service

```sh
kubescrape -listen :8080 -wait-timeout 5s -cache-ttl 5m -log-format json
```

| Flag | Default | Description |
|---|---|---|
| `-listen` | `:8080` | HTTP listen address |
| `-kubeconfig` | — | kubeconfig path; defaults to in-cluster config, then `$KUBECONFIG` / `~/.kube/config` |
| `-wait-timeout` | `5s` | default and maximum time a container lookup blocks waiting for metadata (`?wait=` can shorten per request, never lengthen) |
| `-cache-ttl` | `5m` | how long metadata of deleted pods and replaced container IDs stays resolvable (tombstones) |
| `-metadata-cache-ttl` | `10s` | `max-age` stamped on metadata responses (`Cache-Control` + `ETag`) so the agent's client caches lookups and revalidates with `If-None-Match`/304; 0 disables cache headers |
| `-resync` | `0` | informer resync period (0 = watch stream only) |
| `-servicemonitors` | `false` | serve targets for `monitoring.coreos.com/v1` ServiceMonitors selecting pod-backed Services — plus **PodMonitors** (endpoints name container ports) and **Probes** (`staticConfig` only, resolved through the prober Service's pods) when the cluster serves those CRDs. Endpoint `port`/`targetPort`/`path`/`scheme`, `tlsConfig.insecureSkipVerify`, `bearerTokenSecret` and the keep/drop subset of `metricRelabelings` are honored; other relabel actions and per-endpoint intervals are not. Self-disables with a warning when the CRD is absent |
| `-scrape-auth-secrets` | `false` | serve monitor endpoints' `bearerTokenSecret` values to agents on `GET /v1/scrape-auth/{ns}/{name}/{key}`. Opt-in: requires `secrets get` RBAC (commented out in the manifests) and ships tokens over the cluster-internal HTTP channel |
| `-events` | `false` | export Kubernetes events as OTLP log records (batched; history from the initial list is skipped; pod events carry full pod resource attributes) |
| `-leader-elect` | `false` | gate the events exporter behind a `coordination.k8s.io` Lease so exactly one replica exports — required when running more than one replica with `-events`. Every replica serves reads from its own informer caches regardless; non-leaders keep watching events so failover needs no warmup |
| `-leader-elect-namespace` | `monitoring` | namespace of the leader-election Lease |
| `-leader-elect-name` | `kubescrape-events` | name of the leader-election Lease |
| `-self-metrics-interval` | `1m` | export the service's own metrics over OTLP at this interval (0 disables) |
| `-otlp-*` | as the agent | used by `-events` and the self-metrics push: `-otlp-endpoint`, `-otlp-protocol`, `-otlp-compression`, `-otlp-compression-level`, `-otlp-insecure`, `-otlp-tls-ca-file`, `-otlp-tls-insecure-skip-verify`, `-otlp-bearer-token-file`, `-otlp-timeout` |
| `-log-level` | `info` | `debug`, `info`, `warn`, `error` |
| `-log-format` | `text` | `text` or `json` (client-go's klog is routed through the same handler) |

The service's own metrics (store sizes, HTTP requests per pattern/status,
exported events) are pushed over OTLP on `-self-metrics-interval` (default
1m, 0 disables) using the `-otlp-*` flags. `GET /metrics` serves only the Go
runtime and process metrics (`go_*`, `process_*`).

RBAC (cluster-wide `get`/`list`/`watch`): `pods`, `services`, `namespaces`,
`nodes`, `events`, `replicasets.apps`, `deployments.apps`, `jobs.batch`,
`cronjobs.batch`, `servicemonitors.monitoring.coreos.com` (plus
`podmonitors`/`probes` when those CRDs should be discovered). `-leader-elect`
additionally needs `get`/`create`/`update` on `leases.coordination.k8s.io`,
and `-scrape-auth-secrets` needs `secrets get` (commented out in the
manifests — enable deliberately) — see
[deploy/kubernetes.yaml](../deploy/kubernetes.yaml).

## Agent: general

| Flag | Default | Description |
|---|---|---|
| `-node-name` | `$NODE_NAME` | the node this agent runs on (set via the downward API) |
| `-listen` | `:8081` | serves `/healthz`, `/readyz`, `/debug/tailer` (per-file positions/lag), `/debug/targets` (last scrape cycle's per-target outcomes, failures first), `/debug/transforms` (active transform program hash) and `/metrics` (Go runtime/process metrics only — `kubescrape_*` metrics are OTLP-pushed); empty disables |
| `-self-metrics-interval` | `1m` | export the agent's own metrics over OTLP at this interval (0 disables); both binaries have this flag |
| `-metadata-endpoint` | `http://kubescrape.monitoring` | base URL of the metadata service |
| `-metadata-wait` | `5s` | server-side wait for not-yet-known containers (covers the gap between container start and the kubelet posting its status) |
| `-node-metadata-refresh` | `1m` | refresh interval for the node's labels/annotations used in attribute templates (0 disables) |
| `-log-level` / `-log-format` | `info` / `text` | as for the service |

Pipeline toggles (all default `true` except the opt-in `-journald` and
`-host-metrics`):

| Flag | Enables |
|---|---|
| `-logs` | container log tailing |
| `-metrics` | annotation-discovered pod/service targets |
| `-cadvisor` | `<kubelet-endpoint>/metrics/cadvisor` (needs `-kubelet-endpoint`) |
| `-node-metrics` | `<kubelet-endpoint>/metrics` (needs `-kubelet-endpoint`) |
| `-journald` | systemd journal tailing (default `false`, [below](#agent-journald)) |
| `-host-metrics` | node-level system metrics from /proc (default `false`, [below](#agent-host-metrics)) |

## Agent: OTLP export

| Flag | Default | Description |
|---|---|---|
| `-otlp-endpoint` | `otel-collector.monitoring:4317` | `host:port` for gRPC, base URL for HTTP |
| `-otlp-protocol` | `grpc` | `grpc` or `http` (OTLP/HTTP protobuf on `/v1/logs`, `/v1/metrics`) |
| `-otlp-compression` | `gzip` | payload compression on either transport (`gzip` via klauspost/compress, or `none`); telemetry compresses 5–10x |
| `-otlp-compression-level` | `0` | gzip level `1` (fastest, ~2-3x less CPU for ~10% larger payloads) to `9` (smallest); `0` = library default |
| `-otlp-insecure` | `true` | plaintext gRPC (for HTTP, choose via the URL scheme) |
| `-otlp-bearer-token-file` | — | sends `Authorization: Bearer <token>` on either transport; re-read every minute, so rotated tokens work |
| `-otlp-tls-ca-file` | — | PEM CA bundle for verifying the collector |
| `-otlp-tls-insecure-skip-verify` | `false` | skip certificate verification |
| `-otlp-timeout` | `15s` | per export attempt |
| `-otlp-retry-attempts` | `3` | tries per **metrics** export (logs retry via the tailer's rewind, see below) |
| `-otlp-retry-backoff` | `1s` | initial backoff, doubled per attempt |
| `-otlp-max-send-bytes` | `0` (≈3.75 MiB) | cap on one payload's encoded protobuf size; a larger payload is split into parts each within the cap before sending, so a non-chunking producer (journald, tailer) never gets a batch rejected wholesale for exceeding the collector's gRPC receive limit. Lower it if the collector's `max_recv_msg_size` is below 4 MiB; negative disables splitting |

Examples:

```sh
# In-cluster collector, plaintext gRPC (the default):
kubescrape-agent -otlp-endpoint=otel-collector.monitoring:4317

# SaaS backend over OTLP/HTTP with a bearer token from a mounted Secret:
kubescrape-agent \
  -otlp-endpoint=https://ingest.example.com:443 \
  -otlp-protocol=http \
  -otlp-insecure=false \
  -otlp-bearer-token-file=/etc/kubescrape/otlp-token/token
```

## Agent: log collection

| Flag | Default | Description |
|---|---|---|
| `-config` | — | single YAML file holding all sections: `resourceAttributes`, `logs`, `logAttributes`, `logMetrics`, `logScrubbing`, `metrics`, `traceMetrics`, `traceSampling`, `routing` ([below](#unified-config-file)) |
| `-log-dir` | `/var/log/containers` | containerd log directory; the default source when the `logs` section is unset |
| `-positions-file` | — | single JSON file persisting BOTH log offsets and the journald cursor across restarts (mount a hostPath); empty disables persistence |
| `-logs-watch` | `true` | fsnotify events trigger reads and discovery; polling remains the fallback |
| `-logs-poll-interval` | `500ms` | fallback sweep interval |
| `-logs-fingerprint-bytes` | `1024` | file-head hash length used with the inode as file identity (guards against inode reuse and in-place rewrites); negative = inode only |
| `-logs-batch-size` | `1024` | flush after this many entries |
| `-logs-flush-interval` | `2s` | flush at least this often |
| `-logs-max-entry-bytes` | `1MiB` | truncate assembled entries beyond this |
| `-logs-multiline` | `true` | join stack traces (Go, Java, Python, .NET, Ruby, Rust, PHP) via [multiline](https://github.com/JohanLindvall/multiline) |
| `-logs-multiline-timeout` | `1s` | flush incomplete multi-line groups after this long |
| `-logs-enrich` | `true` | parse per-line metadata via [enrich](https://github.com/JohanLindvall/enrich): a timestamp in the line replaces the CRI time, an explicit level sets the severity, trace/span IDs fill the OTLP trace fields, exception/template/source-context details become record attributes. JSON, logfmt and common plain-text formats are recognized; the body is never modified, and plain-text stack traces are not duplicated into `exception.stacktrace`. Hit rates: `kubescrape_log_enriched_total{format}` in the self-metrics |
| `-logs-file-attributes` | `false` | stamp `log.file.name` (basename) and `log.file.position` (record start offset) on every record, for each file source |
| `-buffer-dir` | — | directory for a disk-backed export buffer (logs **and** metrics); a collector outage spools here instead of pinning the tailer to old offsets / dropping metrics ([below](#disk-buffer)). Empty disables |
| `-buffer-max-bytes` | `1GiB` | per-signal cap on the undelivered on-disk backlog; producers back-pressure (the tailer rewinds) when full |
| `-logs-exclude-namespaces` | — | comma-separated namespaces not tailed — **always exclude the namespace of your collector** to avoid feedback loops |
| `-logs-rate-limit` | `0` | per-file line rate limit (lines/second, token bucket; 0 disables). An exhausted file is **paused** — reading stops until tokens refill, the backlog stays on disk, nothing is lost (rotation drains bypass the limiter) |
| `-logs-rate-burst` | `0` | token bucket size (0 = 2× the rate) |
| `-logs-rate-drop` | `false` | discard lines over the limit instead of pausing (lossy; counted in `kubescrape_log_rate_limited_total{action="drop"}`) |
| `-logs-unknown-files` | `auto` | where a file with no checkpoint entry starts at startup: `end` (skip as pre-existing history), `start` (read whole), `auto` (start when the checkpoint store already has entries — the file appeared while the agent was down; end on a first-ever run). `auto`/`start` mean adding a new log source ingests those files' existing content |
| `-logs-idle-close` | `0` | close a fully-caught-up file's fd after this much inactivity (`0` = never). Bounds steady-state fds at one per *active* file rather than one per tracked file — but the open fd is the only handle to a rotated-away or deleted file's remaining bytes, so enabling this **forfeits the zero-loss guarantee** for the tail of a dying file. Set it only where a node tracks thousands of log files |
| `-logs-metrics-interval` | `30s` | export interval for the `logMetrics` metrics ([below](#agent-log-metrics)) |
| `-logs-metrics-max-bytes` | `3MiB` | export log-derived metrics in chunks below this many bytes (0 = one payload) |
| `-logs-metrics-name-prefix` | — | prefix prepended to every log-derived metric name |

Delivery is at-least-once: offsets are committed only after the collector
acknowledged the batch and never past lines still buffered in the multiline
pipeline; on export failure the files rewind to the committed offset.
Rotation handling (rename, copytruncate — including same-size rewrites —
deletion) is automatic.

Backlog is observable per node — `kubescrape_log_lag_bytes` (largest per-file
backlog) and `kubescrape_log_lag_total_bytes` in the self-metrics — and per file on
`GET /debug/tailer` (path, container, read/committed offsets, lag,
rate-limited flag; refreshed ~10s, largest lag first).

### Disk buffer

Without `-buffer-dir`, durability is checkpoint-and-rewind: during a collector
outage the tailer stops advancing (the source files are the buffer) and scraped
metrics are dropped and re-scraped. A long outage can lose logs if the source
files rotate away first.

With `-buffer-dir` set, every export goes through a disk-backed write-ahead
buffer instead — separate on-disk FIFO spools for logs and metrics. A batch is
serialized, `fsync`'d, and acknowledged to the producer immediately (so the
tailer commits its offsets and source logs may rotate away), then a background
sender drains it to the collector with retries; a batch is removed only after
the collector accepts it. Delivery stays at-least-once and survives agent
restarts (a crash-torn tail is truncated on reopen). The undelivered backlog is
capped per signal by `-buffer-max-bytes`; when full, appends fail and the tailer
back-pressures (rewinds), so disk stays bounded.

Point `-buffer-dir` at a node-local persistent path (e.g. under the agent's
state hostPath) so the buffer survives pod restarts. Note that delivered-but-
not-yet-reclaimed records linger until their whole segment is retired, so
physical disk use can exceed the backlog cap by up to one segment (8 MiB).

## Agent: journald

Opt-in with `-journald`. The agent reads the systemd journal natively through
libsystemd (`github.com/coreos/go-systemd/v22/sdjournal`, cgo — the agent binary
is built with cgo and the image ships libsystemd) and exports the entries as
OTLP log records, one resource per systemd unit (`service.name` = the unit
without `.service`, `systemd.unit`, plus node attributes via the `journal` attrs
pipeline; syslog priorities map to OTLP severities; `syslog.identifier` and
`process.pid` become record attributes).

| Flag | Default | Description |
|---|---|---|
| `-journald-dir` | — | read a specific journal directory; empty opens the default system journal (set to `/run/log/journal` for volatile journals) |
| `-journald-units` | — | comma-separated units (matched on `_SYSTEMD_UNIT`); empty reads everything |
| `-journald-batch-size` | `1024` | flush after this many entries |
| `-journald-max-batch-bytes` | `1048576` | flush before a batch's summed message bytes exceed this |
| `-journald-flush-interval` | `2s` | flush at least this often |
| `-journald-enrich` | `true` | per-message enrichment as `-logs-enrich`; an explicit level in the message wins over the journal priority |

Delivery is at-least-once: the cursor is committed only after a successful
export; on export failure or a reader error, it restarts from the committed
cursor with backoff (re-reading anything in flight). The cursor is
persisted only through `-positions-file` (there is no standalone journald
cursor file); without it, every start begins at the journal tail.

## Unified config file

All of the agent's YAML configuration lives in one file, passed with `-config`.
Every section is optional and mirrors the shape of the standalone file it
replaces, so migrating means nesting the former file under its section key:

```yaml
resourceAttributes: {...}          # see Resource attributes
logs:          {sources: [...]}    # see Agent: log sources
logAttributes: {rules: [...]}      # see Agent: log attributes
logMetrics:    {metrics: [...]}    # see Agent: log metrics
logScrubbing:  {builtin: [...], rules: [...]}         # see Agent: log scrubbing
metrics:       {pipelines: {...}, splitters: [...]}   # see Metrics config
traceMetrics:  {dimensions: [...], buckets: [...]}    # see -ingest-span-metrics
traceSampling: {probability: 0.1, ...}                # see Agent: trace sampling
routing:       {routes: [...]}                        # see Agent: routing
```

The sections below document each in turn. (Starlark transforms deliberately
do **not** live here: they have their own file, `-transforms-file`, so they
can hot-reload without touching the rest of the config — see
[Agent: transforms](#agent-transforms-starlark).)

## Agent: log sources

By default the agent tails containerd container logs under `-log-dir`. The
`logs` section instead declares **sources** — arbitrary files selected by
globs, each either containerd (CRI parsing + pod metadata) or plain (static
resource attributes). All sources use the identical rotation, offset-checkpoint
and cross-rotation multi-line machinery.

```yaml
logs:
  sources:
    - name: containers          # keep tailing container logs
      include: ["/var/log/containers/*.log"]
      containerd: true
    - name: host                # plus arbitrary host logs
      include: ["/var/log/**/*.log"]     # ** matches any depth (doublestar)
      exclude: ["/var/log/containers/*.log", "/var/log/azure/*.log"]
      multiline: true           # optional per-source override
      attributes:               # resource attributes for these (non-containerd) files
        service.name: host-syslog
        log.source: host
```

Per source: `include`/`exclude` are doublestar globs (`**` supported);
`containerd` selects CRI handling (filename → container ID → metadata → CRI
format) versus plain files; `attributes` are static resource attributes stamped
on plain-file records (node attributes from the resource-attribute builder are
added too, and `service.name` defaults to the source `name`); `multiline`
overrides `-logs-multiline` for that source. A file is claimed by the first
source that matches it. Container logs keep working because the default
(no-config) behavior is exactly one containerd source over `-log-dir`.

Per-source option:

- `compressed` reads matched files as gzip, decompressing on the fly (files
  ending in `.gz` are detected automatically). Compressed files are treated as
  **archives** — read once to completion, not tailed — so, unlike plain
  tailing, pre-existing ones *are* ingested; scope `include` to avoid re-reading
  unwanted history. A partially-read archive resumes correctly across a restart.

Caveat: a blank line inside a plain file is dropped, so multi-line formats that
rely on a blank separator (Go panics) do not join for plain files;
indentation-based traces (Python, Java, .NET) join normally.

### Log rules (drop / keep / sample)

The `logs` section's `rules` list filters exported records: ordered
first-match-wins, no match keeps. Selectors use the **same DSL and key
resolution as the `logMetrics` `match`/`matchRegexp`** — keys resolve against
the record's attributes (line-derived + enriched) and the file's resource
attributes, with the line's own JSON/logfmt fields as fallback; `__line__`
matches the whole raw body and `__severity__` the enriched severity text
(lowercased) — so "drop debug logs" needs no per-app parsing config:

```yaml
logs:
  rules:
    - action: keep                       # exceptions go before the drop they pierce
      matchRegexp: ["__line__=(ERROR|FATAL)"]
    - action: drop
      match: ["__severity__=debug"]
    - action: drop                       # noisy access logs from one namespace
      match: ["k8s.namespace.name=ingress"]
      matchRegexp: ["__line__=GET /healthz"]
    - action: keep                       # keep 10% of a chatty matcher
      matchRegexp: ["__line__=cache (hit|miss)"]
      sample: 0.1                        # deterministic: every 10th matching line
```

Rules run **after** enrichment (so severity is matchable) and **after**
`logMetrics` (so metrics still count every line — e.g. count errors while
dropping them). Dropped records advance offsets exactly like exported ones and
are counted in `kubescrape_log_rules_dropped_total`. `sample` is only valid on
keep rules; a matching line beyond the sampled fraction is dropped. journald
records are not filtered.

### Per-workload log config (pod annotation)

A workload can declare its own log handling in the `kubescrape.io/logs` pod
annotation — one JSON object, no agent config change or restart needed:

```yaml
metadata:
  annotations:
    kubescrape.io/logs: |
      {"exclude": false, "multiline": true, "serviceName": "checkout",
       "attributes": {"team": "payments"},
       "rules": [{"action": "drop", "matchRegexp": ["level=debug"]}]}
```

| Key | Meaning |
|---|---|
| `exclude` | skip this pod's log files entirely (like an excluded namespace, but self-service) |
| `multiline` | override the source's stack-trace joining for this pod |
| `serviceName` | override the derived `service.name` resource attribute |
| `attributes` | extra resource attributes (overwriting — the workload is authoritative about itself) |
| `rules` | keep/drop/sample rules (same shape as `logs.rules`), evaluated **before** the global chain: a pod-rule drop is final, a pod-rule keep still passes through the global rules |

The annotation arrives through the metadata resolution every container log
file already performs and is parsed once per file. A malformed annotation is
warned about and ignored — it must never lose logs.

## Agent: log attributes

The `logAttributes` section lifts configured keys out of each structured log
line (JSON or logfmt) onto the exported record. Applies to both `-logs` and
`-journald`.

```yaml
logAttributes:
  rules:
    - key: tenant             # JSON/logfmt key; dotted keys descend into JSON
      attribute: tenant.id    # exported name (defaults to key)
      target: resource        # resource | scope | log (default log)
    - key: http.status_code   # nested JSON path a.b.c
      target: log
```

JSON is scanned once for all rule paths with the
[lightning](https://github.com/JohanLindvall/lightning) toolkit; logfmt uses
the [logfmt](https://github.com/JohanLindvall/logfmt) reader. Values keep their
JSON type (integers → int, fractional → double, booleans → bool). Because
resource and scope attributes decide an OTLP record's grouping, records whose
line-derived resource/scope attributes differ are split into separate
`ResourceLogs`/`ScopeLogs`.

## Agent: log metrics

The `logMetrics` section distills log lines into metrics exported over OTLP,
instead of (or alongside) shipping the lines. Only the configured metrics are
exported. Runtime knobs are the `-logs-metrics-interval`,
`-logs-metrics-max-bytes` and `-logs-metrics-name-prefix` flags.

```yaml
logMetrics:
  metrics:
    - name: http_requests_total
      type: counter                 # counter (default) | gauge | histogram | summary
      value: "1"                    # numeric field to observe, or "1" to count lines
      match: ["level=info"]         # exact selectors (key=value / key!=value)
      matchRegexp: ["msg=^request"] # regex selectors on the value
      labels:                       # → data-point attributes (label DSL, see below)
        - status=$http_status       # passthrough: label status = field http_status
        - class=$http_status(_xx)   # mask: 503 → 5xx (keep chars where pattern is _)
        - path=$path/[0-9]+/:id/    # regex replace: /pattern/replacement/
        - method                    # bare key: label method = field method
        - env=prod                  # literal value
      resourceLabels:               # → resource attributes (same DSL)
        - tenant=$tenant
      maxCardinality: 5000          # cap on unique label sets (unset = default 10000, also the hard cap)
      maxAge: 1h                    # expire idle series (default/cap 24h)
      labelPrefix: ""               # optional prefix on every label name
    - name: request_duration_seconds
      type: histogram
      value: duration_s
      buckets: [0.1, 0.5, 1, 5]
      match: ["msg=request completed"]
    - name: goroutine_panics_total  # __line__ = the whole raw line
      type: counter
      value: "1"
      matchRegexp: ["__line__=^panic:"]
    - name: slow_request_seconds_total
      type: counter
      valueRegexp: 'took ([0-9.]+)s' # capture the value from an unstructured line
      matchRegexp: ["__line__=slow request"]
    - name: open_connections
      type: gauge
      action: inc                   # set (default) | inc | dec | add | sub | min | max | avg | first | sum | count | stddev | range | delta
      match: ["event=connect"]
```

Value, selector and label keys resolve against the record's enriched and
resource attributes (k8s metadata) first, then straight from the log line's own
JSON/logfmt fields (dotted keys descend into nested JSON) — so a metric can read
any field of the line without a separate `logAttributes` rule. Additional knobs:

- **Resource vs data-point attributes** — the log line's own resource attributes
  (the pod's k8s identity, plus the derived `service.namespace` /
  `service.instance.id`) become the metric's OTLP **resource**, so metrics group
  per-pod like scraped metrics (Mimir `job`/`instance`/`target_info`). The
  metric's `labels` are **data-point** attributes. `resourceLabels` lifts a
  log-derived label onto the resource instead (same DSL as `labels`).
- **`__line__`** is a synthetic selector/label key holding the whole raw line,
  for filtering on line contents (e.g. `matchRegexp: ["__line__=^panic:"]`).
- **`valueRegexp`** extracts the observed value from the raw line via a regex
  capture group (group 1, or the whole match); mutually exclusive with `value`.
  A line that does not match is skipped.
- **`action`** (gauge only): `set` (default, last value wins), `inc`/`dec`
  (±1 per matching line, no value needed), `add`/`sub` (±the observed value),
  or a windowed aggregation over the values seen in a window: `min`, `max`,
  `avg`, `first`, `sum`, `count` (matching lines, no value needed), `stddev`
  (population standard deviation), `range` (max−min), `delta` (last−first). An
  aggregation emits its value on every export and keeps emitting it while no new
  value arrives; the first value after an export starts a fresh window (so `avg`
  is a per-window mean).

`histogram` exports cumulative OTLP histograms; `summary` carries a running
count and sum (no quantiles); `counter` emits a monotonic sum (with synthetic
zero baseline points). Rules sharing a `name` share one underlying series (and
must agree on type/action).

## Agent: log scrubbing

The `logScrubbing` section redacts sensitive values from log **bodies** on
the agent, so secrets never leave the node. It applies to the tailer,
journald and OTLP-ingest log paths, and runs **before** anything copies from
the body — enrichment, `logAttributes`, log metrics and the export itself
all see the redacted line.

```yaml
logScrubbing:
  builtin: [defaults, email]     # named built-ins; "defaults" expands to the
                                 # low-false-positive set
  rules:                         # user regexes, applied after the built-ins
    - name: session-id
      regexp: 'session=[0-9a-f]{32}'
      replacement: 'session=[REDACTED]'   # $1-style refs work; default [REDACTED]
```

Built-in patterns: `bearer`, `basic-auth`, `secret-kv` (api_key / secret /
password / token / access_key key-value pairs — the key and separator are
kept so the line stays readable), `aws-key`, `private-key` (all five =
`defaults`), plus the opt-in-by-name `email` and `credit-card` (they redact
legitimate content too often to be defaults). Every built-in carries a cheap
literal prefilter, so the no-match hot path is a few substring scans and
zero allocations. Redactions count into
`kubescrape_log_scrubbed_total{pattern}`. An unknown builtin name, an
invalid regex, or a config with no patterns at all fails startup — a
scrubber that silently skips a pattern is a compliance bug.

## Agent: OTLP ingest

Opt-in with `-ingest`: the agent receives OTLP that apps push to the node and
enriches each resource with k8s attributes deduced from a container ID or pod
UID already on the data, forwarding through the same exporter. Enrichment
never overwrites an attribute the sender set.

| Flag | Default | Description |
|---|---|---|
| `-ingest-grpc-endpoint` | `:4317` | OTLP/gRPC listen address; empty disables |
| `-ingest-http-endpoint` | `:4318` | OTLP/HTTP protobuf listen address (`/v1/logs`, `/v1/metrics`); gzip `Content-Encoding` accepted; empty disables |
| `-ingest-metrics-mode` | `auto` | `resource` (ID on the resource), `datapoint` (ID per point → split into per-object resources), or `auto` |
| `-ingest-logs-enrich` | `true` | parse pushed log bodies as `-logs-enrich`, filling only fields the sender left unset |
| `-ingest-traces` | `true` | accept pushed traces (gRPC + `/v1/traces`), enrich their resources the same way, and pass them through (traces bypass the disk buffer — the pushing sender owns retry) |
| `-ingest-span-metrics` | `false` | derive RED metrics from ingested spans (OTel spanmetrics conventions: `traces.span.metrics.calls`/`.size`/`.duration` with per-bucket trace-id exemplars), dimensioned by `service.name`/`span.name`/`span.kind`/`status.code` plus the `traceMetrics` config section's extra dimensions. Requires `-ingest-traces` |
| `-ingest-span-metrics-interval` | `1m` | export interval for the span metrics (exported under the agent's own resource identity; the described service is a data-point label) |
| `-ingest-peer-ip-fallback` | `false` | attribute telemetry whose resource carries **no** container id / pod uid to the pod owning the connection's peer IP (`GET /v1/pod-ips/{ip}`, live non-hostNetwork pods only). Opt-in: NAT can rewrite peer addresses, and hostNetwork senders share the node IP and never resolve. Counted as `kubescrape_ingest_resources_total{outcome="peer_ip"}` |
| `-ingest-batch-items` | `0` | coalesce pushed payloads per signal to this many items (log records / data points / spans) before forwarding, flushing partial batches after `-ingest-batch-timeout` (200ms) or before the encoded payload exceeds `-ingest-batch-bytes` (3 MiB). `0` forwards each request as received. Enqueueing acknowledges the sender (collector batch-processor semantics) — pair with `-buffer-dir` for at-least-once delivery of coalesced batches (note: traces are never disk-buffered, so batching trades the sender's own retry for best-effort delivery of acked spans); a full queue back-pressures senders with a retryable error |
| `-ingest-batch-bytes` | `3145728` | flush a coalescing batch before its encoded size would exceed this (keeps merged payloads under the collector's 4 MiB gRPC recv default) |
| `-ingest-container-id-keys` | `container.id,k8s.container.id` | attribute keys inspected for a container ID |
| `-ingest-pod-uid-keys` | `k8s.pod.uid` | attribute keys inspected for a pod UID |
| `-ingest-metadata-wait` | `0` | how long a lookup may block for a not-yet-known object |

A container ID resolves the exact container incarnation; a pod UID resolves
the pod. Outcomes count into `kubescrape_ingest_resources_total{outcome}`
(`enriched` / `unresolved` / `peer_ip`).

## Agent: trace sampling

The `traceSampling` section samples **ingested** spans before forwarding
(it needs `-ingest` and `-ingest-traces`):

```yaml
traceSampling:
  probability: 0.1        # keep this fraction of traces (unset/1 keeps all)
  keepErrors: true        # default: status-ERROR spans always kept
  keepSlowerThan: 2s      # spans at least this slow always kept (0 disables)
  maxSpansPerSecond: 500  # hard cap after sampling (0 = uncapped)
```

The probability decision is **consistent per trace ID** (a hash of the ID
against the threshold): all spans of a trace sample identically on this node
and on every other node running the same config — a node-local sampler still
yields whole traces — and a sender's retry re-samples identically.
`keepErrors` and `keepSlowerThan` bypass the probability decision but not
the rate cap (a cap that can be exceeded is not a cap). Dropped spans count
into `kubescrape_trace_spans_dropped_total{reason="probability"|"rate"}`; a
payload sampled down to nothing is acked without a send. The sampler sits
below the span-metrics tap, so the `-ingest-span-metrics` RED metrics are
derived ahead of sampling while only the sampled subset ships.

## Agent: metrics scraping

| Flag | Default | Description |
|---|---|---|
| `-scrape-interval` | `30s` | one cycle scrapes every target of this node |
| `-scrape-timeout` | `15s` | per target |
| `-scrape-concurrency` | `4` | concurrent target scrapes |
| `-metrics-batch-size` | `10000` | export chunk size in data points — a 100k-series target is exported in ten chunks and never held in memory |
| `-metrics-batch-bytes` | `3145728` | also flush a chunk once its estimated encoded size reaches this (`0` = size only). The collector's gRPC receive limit applies to the **decompressed** message (4 MiB by default), which a label-rich target (kube-state-metrics, Istio) can exceed well before the point limit — every export of that target would then fail, so this bound is what keeps a chunk deliverable |
| `-scrape-max-samples` | `0` | abort a single scrape beyond this many samples (0 = unlimited) |
| `-scrape-exemplars` | `false` | negotiate OpenMetrics and attach exemplars to counter and histogram points (`trace_id`/`span_id` map to OTLP trace/span fields) |
| `-scrape-health-metrics` | `true` | export synthetic `up`, `scrape_duration_seconds` and `scrape_samples_scraped` gauges per target after each cycle |
| `-scrape-native-histograms` | `false` | offer the Prometheus **protobuf exposition** to annotation/monitor targets and convert native histograms to OTLP **exponential histogram** points. A family carrying both native and classic data uses the native representation; custom-bucket histograms (schema −53) fall back to classic buckets; targets that ignore the Accept header keep serving text (the parse mode follows the response Content-Type), and splitter-backed targets stay on the text path |

Series filters and target splitters live in the `metrics` section of `-config`
([below](#metrics-config)).

Histograms and summaries are converted to proper OTLP Histogram/Summary
points (de-cumulated buckets, explicit bounds, quantiles); counters become
cumulative monotonic sums.

The last completed cycle's per-target outcomes — pipeline, URL, source,
monitor, up, error, duration, samples — are served on `GET /debug/targets`
(failures first, then by URL): the human-readable counterpart of the health
gauges. Targets derived from monitor endpoints may carry
`insecureSkipVerify`, a bearer-token secret reference (resolved via
`GET /v1/scrape-auth/...`, which requires `-scrape-auth-secrets` on the
metadata service) and keep/drop `metricRelabelings` applied per sample —
all honored automatically, no agent flags involved.

## Agent: kubelet scrapes

| Flag | Default | Description |
|---|---|---|
| `-kubelet-endpoint` | — | kubelet base URL, typically `https://$(NODE_IP):10250` with `NODE_IP` from the downward API; empty disables both kubelet scrapes |
| `-kubelet-token-file` | ServiceAccount token | bearer token towards the kubelet (needs `nodes/metrics get` RBAC) |
| `-kubelet-insecure-tls` | `true` | kubelet serving certificates are typically self-signed |
| `-cadvisor-rollups` | `true` | `false` drops the hierarchy aggregates (`/`, `/kubepods`, QoS/system slices) and pod-level rows of container-scoped families, keeping container-level series, `container_network_*` and `machine_*` |

cadvisor series are split into one OTLP resource per pod/container, keyed by
the cgroup path in the `id` label: the container ID resolves the exact
container incarnation through the metadata service; pod-scoped series (e.g.
`container_network_*`) resolve by namespace/name cross-checked against the
cgroup pod UID.

## Agent: host metrics

Opt-in with `-host-metrics`: node-level system metrics read straight from
`/proc` (via `prometheus/procfs`, node_exporter's own parser) and exported
over OTLP — replacing a separate node_exporter DaemonSet for the core set.
Metric **names are node_exporter-compatible** (`node_cpu_seconds_total`,
`node_memory_MemAvailable_bytes`, `node_load1/5/15`, `node_disk_*`,
`node_network_*`, `node_filesystem_*`), so existing dashboards and alerts
keep working. The resource is `service.name: node`, `k8s.node.name` and
`service.instance.id` = the node name — `job="node"` / `instance=<node>`
after the Mimir mapping.

| Flag | Default | Description |
|---|---|---|
| `-host-metrics` | `false` | enable the collector |
| `-host-metrics-interval` | `30s` | collection interval |
| `-host-proc` | `/proc` | proc filesystem to read — in-cluster, mount the **host's** `/proc` into the container (e.g. at `/host/proc`) and point this at it, or you measure the agent's own namespace |
| `-host-rootfs` | — | host root mount for filesystem usage metrics (statfs); empty **skips** the `node_filesystem_*` metrics |

## Agent: transforms (Starlark)

`-transforms-file` points at a separate YAML file holding one optional
[Starlark](https://github.com/google/starlark-go) script per signal, each
defining `transform(batch)`:

```yaml
logs: |
  def transform(batch):
      for r in batch:
          if r.attributes["level"] == "debug":
              r.drop()
          r.resource["env"] = "prod"
metrics: |
  def transform(batch):
      for m in batch:
          if m.name.startswith("go_"):
              m.drop()
traces: |
  def transform(batch):
      for s in batch:
          s.attributes["region"] = "eu"
```

* **Where it runs**: once per exported batch, at the exporter seam **above**
  the disk buffer — spooled bytes are final, and a reload never
  re-interprets a durable backlog. A transformed-to-empty batch is acked
  without a send.
* **Host objects are lazy** views over the OTLP data: log records expose
  `body`, `severity_text`, `severity_number`, `attributes`, `resource`,
  `drop()`; spans expose `name`, `status_code`, `attributes`, `resource`,
  `drop()`; metrics expose `name`, `resource`, `drop()`. Mutations are in
  place; a script pays only for the fields it touches (~1µs per touched
  record); dropped elements and emptied groups are pruned after the run.
* **Hot reload**: the file is watched (fsnotify on its directory — mount the
  ConfigMap **as a directory, not `subPath`**, or updates never arrive) with
  a 30s poll fallback. Reloads compile-then-commit: a broken edit keeps the
  last good program running (`kubescrape_transform_reloads_total{outcome}`),
  while a compile failure at startup is fatal. The active program's content
  hash is on `GET /debug/transforms`, so per-node convergence after a
  reload is checkable.
* **Safety**: Starlark is hermetic (no I/O, no imports, no clock) and every
  run is step-limited, so a pathological script errors out
  (`kubescrape_transform_errors_total{signal}`, the batch is not exported
  and the producer's usual retry applies) instead of wedging an export
  goroutine.

## Agent: routing

The `routing` section fans exported payloads out **by Kubernetes namespace**
to extra destinations or tenants; unmatched resources use the default chain:

```yaml
routing:
  routes:
    - name: team-a                      # required; labels metrics/logs
      namespaces: ["team-a-*"]          # required; path.Match globs on k8s.namespace.name
      headers: {X-Scope-OrgID: team-a}  # extra headers (header-only tenant routing)
    - name: audit
      namespaces: ["payments"]
      endpoint: audit-collector.security:4317   # overrides the default endpoint
```

First-matching route wins; a payload where everything matches the default is
forwarded untouched (no copy). Route clients inherit the main `-otlp-*`
settings (protocol, TLS, compression, retries) unless the route overrides
the endpoint. Delivery is at-least-once per destination: a failed
destination fails the whole export and the producer's retry re-splits
deterministically (already-succeeded destinations receive duplicates, which
OTLP consumers must tolerate anyway). Per-route destinations are **direct
(unbuffered) by design** — only the default chain keeps the `-buffer-dir`
durability; routes are for tenancy/fan-out, not for doubling the durability
machinery. Routed parts count into
`kubescrape_routed_payload_parts_total{route,signal}`.

## Resource attributes

The `resourceAttributes` section controls how resource attributes are built for
**all** exported data (logs and metrics). The built-in mapping also derives
`service.namespace` (= the k8s namespace) and `service.instance.id` (fallback
chain: `container.id`, pod-uid[/container], namespace/pod[/container], node) so
Prometheus/Mimir gets a unique `job` (`service.namespace/service.name`) and
`instance` — both omitted when a template sets them.

An `instancePrefix` (per pipeline, or per splitter rule) prepends `prefix-` to
`service.instance.id`. This keeps an exporter that *describes other objects*
(cadvisor, a kube-state-metrics splitter) from colliding with the described
pod's own self-scraped `target_info` — they share `service.name`/namespace, so
without a distinct instance they clash on `(job, instance)` with different
resource attributes. It defaults to `cadvisor` for the cadvisor pipeline and to
the describing target's `service.name` for splitter rules; set `""` to disable.
Precedence: explicit pipeline section > built-in default > top-level base. Quick
knobs also exist as flags:

* `-resource-attrs-static=cluster=prod,env=eu` — fixed attributes.
* `-resource-attrs-enable=<regex,...>` / `-resource-attrs-disable=<regex,...>`
  — anchored regexes on the attribute key; an attribute is exported when it
  matches the enable set (empty = all) and not the disable set (empty =
  none).

The config section:

```yaml
resourceAttributes:
  # Include the built-in mapping: k8s.namespace.name, k8s.pod.name,
  # k8s.pod.uid, k8s.node.name, k8s.pod.ip, owners (k8s.deployment.name, ...),
  # pod labels (k8s.pod.label.*), namespace labels, container.id,
  # container.image.name, service.name (workload owner). Default true.
  defaults: true

  # Fixed attributes on every resource (flag statics override these).
  static:
    k8s.cluster.name: prod-eu

  # Go templates evaluated per resource against {Node, Pod, Container,
  # Service}. Empty or failing templates (e.g. .Container on a pod-level
  # resource) omit the attribute.
  attributes:
    team: '{{ index .Pod.Labels "team" }}'
    container.image: '{{ with .Container }}{{ .Image }}{{ end }}'
    k8s.node.zone: '{{ with .Node }}{{ index .Labels "topology.kubernetes.io/zone" }}{{ end }}'
    service.name: >-
      {{ with .Pod }}{{ coalesce (index .Labels "gp/service-name")
      (index .Labels "app.kubernetes.io/name") .Name }}{{ end }}
    infra: '{{ with .Pod }}{{ if regexMatch "-system$" .Namespace }}yes{{ end }}{{ end }}'

  # Per-pipeline overrides (logs | targets | cadvisor | node | journal | ingest);
  # maps merge with the pipeline entry winning.
  pipelines:
    node:
      attributes:
        service.name: kubelet
    cadvisor:
      instancePrefix: cadvisor   # default; "" disables the collision prefix
```

Template context and functions:

| | |
|---|---|
| `.Pod` | full pod model: `.Name`, `.Namespace`, `.UID`, `.Labels`, `.Annotations`, `.Owners`, `.Containers`, … |
| `.Container` | the specific container: `.Name`, `.ID`, `.Image`, `.ImageID`, … (nil on pod/node-level resources) |
| `.Service` | the discovering Service on service-source targets |
| `.Node` | the agent node's `.Name`, `.Labels`, `.Annotations` (refreshed per `-node-metadata-refresh`) |
| `env` | `{{ env "CLUSTER" }}` |
| `coalesce` | first non-empty argument |
| `default` | `{{ default "unknown" $x }}` |
| `regexMatch` | `{{ if regexMatch "-system$" .Pod.Namespace }}…{{ end }}` |
| `regexReplace` | `{{ regexReplace ":.*$" "" .Container.Image }}` |

Order of application: defaults → static → templates → enable/disable filter.

## Metrics config

The `metrics` section (for scraped series, distinct from `logMetrics`) has two
subsections.

**`pipelines`** — ordered keep/drop rules per pipeline (`all` is prepended
to every pipeline; then `targets`, `cadvisor`, `node`). First matching rule
decides; no match keeps the series. Regexes are anchored; `labels` matchers
must all match (a missing label matches `""`). Filtering happens on the
scraped series names (`foo_bucket`, …) before histogram grouping.

```yaml
metrics:
  pipelines:
    all:
      - action: keep                # exceptions go before the drop they pierce
        metrics: 'envoy_requests_total'
      - action: drop
        metrics: '(envoy_|otelcol_|prometheus_|go_|process_).+'
    cadvisor:
      - action: keep
        metrics: 'container_network_.+'
        labels: {interface: eth0}
      - action: drop
        metrics: 'container_network_.+'
```

**`splitters`** — re-attribute targets whose series describe *other*
objects (kube-state-metrics style). Per matching target, rules are checked
in order per series (first `metrics` match wins); the `groupBy` labels move
into a per-object resource under the mapped attribute names, the remaining
labels stay on the data points, and unmatched series stay on the target's
own resource. With `enrich: true` the object resolves through the metadata
service (by `container.id` if mapped, else namespace+name, cross-checked
against a mapped `k8s.pod.uid`) and carries the full metadata set.
`datapointAttributes` (default `[k8s.node.name]`) lists resource attributes to
emit on the **data points** instead of the resource — the described object's
node is a property of the object, not the exporter's identity; set `[]` to keep
everything on the resource, or list more attributes to demote. `instancePrefix`
(default: the describing target's `service.name`) prefixes each split resource's
`service.instance.id` so the described object doesn't collide with its own
self-scraped `target_info`; set `""` to disable. `dropLabels` (anchored regex on
label names) omits matching labels from the data points (e.g. `label_.+` strips
the object's own Kubernetes labels off `kube_.+_labels` series once grouped).
`attributes` sets resource attributes **only where absent** — fallbacks for what
neither `groupBy` nor enrichment provided (e.g. `service.name: unknown`).
Several `groupBy` labels may map to the same attribute: labels apply in name
order and non-empty values overwrite, giving a deterministic coalesce (e.g.
`label_gp_service_name` beats `label_app_kubernetes_io_name` for
`service.name`).

```yaml
metrics:
  splitters:
    - match:                        # all set fields must match the target pod
        namespace: monitoring       # anchored regex
        podLabels:
          app.kubernetes.io/name: kube-state-metrics
      rules:
        - metrics: 'kube_.+_labels'     # ordered before kube_pod_.+ (first match wins)
          groupBy: {namespace: k8s.namespace.name}
          dropLabels: 'label_.+'        # the object's labels stay off the points
          attributes: {service.name: unknown}   # fallback, set only if absent
        - metrics: 'kube_pod_.+'
          groupBy:
            namespace: k8s.namespace.name
            pod: k8s.pod.name
            uid: k8s.pod.uid
            container: k8s.container.name
          enrich: true
          # instancePrefix: kube-state-metrics   # default: target's service.name
        - metrics: 'kube_.+'
          groupBy: {namespace: k8s.namespace.name}
```

## Scrape annotations

On pods, or on Services whose selector matches the pod (service ports
translate through `targetPort`; duplicates across both sources are reported
once, pod source wins):

| Annotation | Meaning |
|---|---|
| `prometheus.io/scrape` | `"true"` to generate targets |
| `prometheus.io/port` | comma-separated port numbers and/or names; absent = every declared port |
| `prometheus.io/path` | default `/metrics` |
| `prometheus.io/scheme` | `http` (default) or `https` |

With `-servicemonitors` on the metadata service, Prometheus-Operator CRDs
become additional target sources — scraping stays node-local throughout:

* **ServiceMonitors**: the monitor's `selector` picks Services by label
  (within `namespaceSelector`), each endpoint's `port` names a service port
  (or `targetPort` addresses the pod port directly), and `path` and `scheme`
  are honored.
* **PodMonitors** (watched when the cluster serves the CRD): the selector
  picks **pods** by label, and each `podMetricsEndpoints` entry's `port`
  names a **container** port (`targetPort` takes a number or container-port
  name).
* **Probes** (watched when served; `staticConfig` targets only — ingress
  targets are not interpreted): the prober URL must be the DNS form of a
  Service; the probe resolves through that Service's backing pods, so the
  agent on a prober pod's node scrapes
  `<prober>/probe?module=…&target=…` per static target.

On ServiceMonitor and PodMonitor endpoints, `tlsConfig.insecureSkipVerify`
and `bearerTokenSecret` are honored (tokens are fetched by agents through
`GET /v1/scrape-auth/{ns}/{name}/{key}`, served only when the service runs
`-scrape-auth-secrets`), and the **keep/drop subset** of
`metricRelabelings` (`action`, `sourceLabels`, `regex`; `__name__` = the
metric name) is applied per sample by the agent. Other relabel actions,
other authentication schemes and per-endpoint intervals are ignored.

## Helm values

The commonly-tuned flags above map to values (the rest are reachable via `agent.extraArgs`/`service.extraArgs`); `agent.config` is rendered verbatim into the
single mounted `-config` file (with a checksum annotation, so config changes
roll the DaemonSet). `agent.extraVolumes`/`agent.extraVolumeMounts` cover
the mounts the new opt-ins need: the host `/proc` for `-host-metrics` and
the dedicated transforms ConfigMap for `-transforms-file` (mounted as a
directory, not `subPath`). See
[charts/kubescrape/values.yaml](../charts/kubescrape/values.yaml) for the
full annotated list.

## Complete example

A production-shaped `values.yaml`:

```yaml
logFormat: json

service:
  replicas: 2
  cacheTTL: 10m
  podDisruptionBudget: {enabled: true, maxUnavailable: 1}

agent:
  kubeletEndpoint: "https://$(NODE_IP):10250"
  cadvisorRollups: false
  logsExcludeNamespaces: [monitoring]
  scrapeInterval: 30s
  scrapeExemplars: true

  otlp:
    endpoint: https://ingest.example.com:443
    protocol: http
    insecure: false
    bearerTokenSecret: {name: ingest-secrets, key: token}

  staticAttrs:
    k8s.cluster.name: prod-eu

  config:
    resourceAttributes:
      attributes:
        k8s.node.zone: '{{ with .Node }}{{ index .Labels "topology.kubernetes.io/zone" }}{{ end }}'
        service.name: >-
          {{ with .Pod }}{{ coalesce (index .Labels "app.kubernetes.io/name")
          (index .Labels "app") .Name }}{{ end }}

    logMetrics:
      metrics:
        - name: http_requests_total
          type: counter
          value: "1"
          match: ["level=info", "msg=request completed"]
          labels: [status=$http_status, class=$http_status(_xx)]

    metrics:
      pipelines:
        all:
          - action: drop
            metrics: '(go_|process_)generic_noise_.+'
        cadvisor:
          - action: keep
            metrics: 'container_network_.+'
            labels: {interface: eth0}
          - action: drop
            metrics: 'container_network_.+'
      splitters:
        - match:
            podLabels: {app.kubernetes.io/name: kube-state-metrics}
          rules:
            - metrics: 'kube_pod_.+'
              groupBy:
                namespace: k8s.namespace.name
                pod: k8s.pod.name
                uid: k8s.pod.uid
                container: k8s.container.name
              enrich: true
            - metrics: 'kube_.+'
              groupBy: {namespace: k8s.namespace.name}
```

```sh
helm install kubescrape charts/kubescrape -n monitoring -f values.yaml
```
