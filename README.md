# kubescrape

Two cooperating services:

* **kubescrape** — an HTTP service serving Kubernetes pod and container
  metadata — including the full ownership chain (ReplicaSet → Deployment,
  Job → CronJob, …) and namespace metadata — and deriving Prometheus scrape
  targets for pods from the conventional `prometheus.io/*` annotations (on
  pods or on Services selecting them).
* **kubescrape-agent** — a per-node DaemonSet that tails containerd container
  logs and scrapes the node's Prometheus targets, exporting both as OTLP to
  an [OpenTelemetry collector](https://github.com/open-telemetry/opentelemetry-collector-contrib),
  enriched with resource attributes fetched from the metadata service.

Full flag and config-file reference with examples:
[docs/CONFIGURATION.md](docs/CONFIGURATION.md).

## How it works

* On startup the service performs a single **LIST** of all pods and then keeps
  the view current with a **WATCH** event stream (client-go shared informers).
  There is no polling and no per-request API traffic.
* Owner chains, owner labels/annotations and namespace metadata are resolved
  from **metadata-only informers** (`PartialObjectMetadata`) for ReplicaSets,
  Deployments, Jobs, CronJobs and Namespaces, so full specs of those objects
  are never fetched or cached. `managedFields` are stripped before objects
  enter any cache.
* Services are watched so pods can also be discovered for scraping through
  the annotations of a Service that selects them.
* Every container runtime ID reported by a pod is indexed, including the
  previous incarnation of restarted containers (`lastState.terminated`).
* When a pod is deleted — or a container ID is replaced by a restart — its
  metadata stays resolvable for a configurable TTL (`-cache-ttl`), so
  short-lived pods can still be looked up shortly after they are gone.
* If a container ID is not (yet) known, the lookup **blocks** until the
  metadata arrives over the watch stream or the wait budget expires. This
  covers the gap between a container starting on a node and the kubelet
  posting its status to the API server.

## API

### `GET /v1/containers/{id}[?wait=2s]`

Metadata for a container by runtime ID. The ID may be bare
(`4fa6c3d0be…`) or prefixed (`containerd://4fa6c3d0be…`, `docker://…`,
`cri-o://…`), URL-escaped or not.

* Blocks up to the wait budget if the ID is unknown; `wait` (a Go duration or
  plain seconds) shortens the server default (`-wait-timeout`), and `wait=0`
  makes the lookup non-blocking.
* `404` if the ID is still unknown when the budget expires.
* `503` if the initial cache sync has not completed within the budget.

```json
{
  "containerId": "4fa6c3d0be...",
  "container": {
    "name": "app", "type": "container", "id": "4fa6c3d0be...",
    "runtimeId": "containerd://4fa6c3d0be...", "image": "nginx:1.27",
    "state": "running", "ready": true, "restartCount": 0,
    "ports": [{"name": "web", "port": 8080, "protocol": "TCP"}]
  },
  "pod": {
    "name": "web-5d9c8b-x7k2p", "namespace": "default", "uid": "…",
    "nodeName": "node-1", "podIP": "10.42.0.17", "hostIP": "192.168.1.10",
    "phase": "Running", "labels": {"app": "web"}, "annotations": {"…": "…"},
    "namespaceMetadata": {"uid": "…", "labels": {"kubernetes.io/metadata.name": "default"}},
    "owners": [
      {"apiVersion": "apps/v1", "kind": "ReplicaSet", "name": "web-5d9c8b", "uid": "…", "controller": true, "labels": {"…": "…"}},
      {"apiVersion": "apps/v1", "kind": "Deployment", "name": "web", "uid": "…", "controller": true, "labels": {"…": "…"}}
    ],
    "containers": [ … ]
  }
}
```

Owners carry their own labels and annotations for the kinds the service
watches (ReplicaSets, Deployments, Jobs, CronJobs); `namespaceMetadata` holds
the labels and annotations of the pod's namespace.

Pods served from the tombstone cache additionally carry `pod.deletedAt`.

### `GET /v1/nodes/{node}/targets`

Prometheus scrape targets for all live pods scheduled on `node`. Targets come
from three sources:

* **pod annotations** — the conventional annotations on the pod itself,
* **service annotations** — the same annotations on any Service whose
  selector matches the pod; service ports are translated to pod ports via
  their `targetPort` (named container port, explicit number, or the service
  port itself), and
* **ServiceMonitors** (opt-in, `-servicemonitors`) — Prometheus-Operator
  `monitoring.coreos.com/v1` ServiceMonitor resources whose selector matches
  a Service backing the pod. Endpoint `port` (service port name),
  `targetPort` (pod port number or container-port name), `path` and `scheme`
  are honored; per-endpoint authentication, relabelings and intervals are
  not. These targets carry `source: "servicemonitor"` and a
  `monitor: "<namespace>/<name>"` field. If the CRD is absent at startup the
  feature disables itself with a warning.

| Annotation             | Meaning                                                        |
|------------------------|----------------------------------------------------------------|
| `prometheus.io/scrape` | must be `"true"` for targets to be generated                   |
| `prometheus.io/port`   | comma-separated list of port numbers and/or port names (container-port names on pods, service-port names/numbers on services); if absent, every declared port becomes a target |
| `prometheus.io/path`   | metrics path, default `/metrics`                               |
| `prometheus.io/scheme` | `http` (default) or `https`                                    |

Pods without an IP or in phase `Succeeded`/`Failed` are excluded, and
duplicate endpoints reachable through both sources are reported once (pod
source wins). Each target embeds the complete pod metadata (including owners
and namespace metadata); service-derived targets also embed the service's
identity, labels and annotations:

```json
{
  "node": "node-1",
  "targets": [
    {
      "url": "http://10.42.0.17:9090/metrics",
      "scheme": "http", "address": "10.42.0.17:9090", "port": 9090, "path": "/metrics",
      "source": "pod",
      "pod": { … full pod metadata … }
    },
    {
      "url": "http://10.42.0.23:8080/svc-metrics",
      "scheme": "http", "address": "10.42.0.23:8080", "port": 8080, "path": "/svc-metrics",
      "source": "service",
      "service": {"name": "demo-svc", "namespace": "…", "uid": "…", "labels": {"…": "…"}, "annotations": {"…": "…"}},
      "pod": { … full pod metadata … }
    }
  ]
}
```

### `GET /v1/pods/{namespace}/{name}`

Full metadata for one pod looked up by name (the agent uses this to
attribute cadvisor series). Deleted pods stay resolvable until their
tombstone expires or a new pod with the same name replaces them.

### `GET /v1/pod-uids/{uid}`

Full metadata for one pod looked up by UID (the agent's OTLP-ingest enricher
uses this to attribute pushed telemetry that carries a `k8s.pod.uid`).
Tombstone-aware like the other pod lookups.

### `GET /v1/pod-ips/{ip}`

Full metadata for the **live** pod owning a pod IP (the agent's opt-in
peer-IP attribution for pushed OTLP). Unlike the other pod lookups this is
deliberately NOT tombstone-aware — pod IPs are recycled quickly, so a deleted
pod must never resolve — and hostNetwork pods (which share the node IP) are
not indexed.

### `GET /healthz`, `GET /readyz`, `GET /metrics`

Liveness is always `200`; readiness turns `200` once the initial informer
cache sync has completed. The service's own metrics (`kubescrape_store_pods`,
`kubescrape_store_containers`, `kubescrape_http_requests_total{pattern,code}`,
`kubescrape_events_exported_total`, …) are produced through the same internal
metrics machinery as everything else and **pushed over OTLP**
(`-self-metrics-interval`, default 1m; 0 disables). `/metrics` serves only
the Go runtime and process metrics (`go_*`, `process_*`) in Prometheus text
format, for debugging the process itself.

## Running

```sh
make build           # or: go build ./cmd/kubescrape
./bin/kubescrape -listen :8080 -wait-timeout 5s -cache-ttl 5m
```

| Flag            | Default | Description                                                              |
|-----------------|---------|--------------------------------------------------------------------------|
| `-listen`       | `:8080` | HTTP listen address                                                       |
| `-kubeconfig`   | —       | kubeconfig path; defaults to in-cluster config, then `$KUBECONFIG`/`~/.kube/config` |
| `-wait-timeout` | `5s`    | default and maximum time a container lookup blocks waiting for metadata  |
| `-cache-ttl`    | `5m`    | retention of metadata for deleted pods and replaced container IDs        |
| `-metadata-cache-ttl` | `10s` | `Cache-Control`/`ETag` max-age on metadata responses; agents cache lookups client-side (0 disables) |
| `-resync`       | `0`     | informer resync period (0 = watch stream only)                            |
| `-servicemonitors` | `false` | serve targets for ServiceMonitor CRDs (see above)                      |
| `-events`       | `false` | export Kubernetes events as OTLP log records                             |

With `-events` the service watches `corev1.Events` and exports them as OTLP
log records (batched, at-most-once): the event message becomes the body,
`reason`/`count`/`reportingComponent` become attributes, `type: Warning` maps
to severity Warn, and events about pods get the full pod resource attributes
(owners, labels) from the store — other objects get `k8s.object.*` plus the
well-known workload attribute for their kind. Events already in the informer's
initial list (history) are skipped. The OTLP connection shares the agent's
exporter flags: `-otlp-endpoint`, `-otlp-protocol`, `-otlp-insecure`,
`-otlp-tls-ca-file`, `-otlp-tls-insecure-skip-verify`,
`-otlp-bearer-token-file`, `-otlp-timeout`.

In-cluster it needs `get`/`list`/`watch` on `pods`, `services`, `namespaces`,
`events`, `replicasets.apps`, `deployments.apps`, `jobs.batch`,
`cronjobs.batch` and (optionally) `servicemonitors.monitoring.coreos.com`
cluster-wide — see [deploy/kubernetes.yaml](deploy/kubernetes.yaml).

`make image` builds a container image from the [Dockerfile](Dockerfile);
`make test` and `make vet` run the test suite and static checks.

## The node agent

`kubescrape-agent` ([deploy/agent.yaml](deploy/agent.yaml)) runs on every
node with only `/var/log` (read-only) and a small state directory mounted —
it needs no Kubernetes API access, only the metadata service and the
collector.

**Logs.** The agent tails `/var/log/containers` and runs each file through
the two-stage [JohanLindvall/multiline](https://github.com/JohanLindvall/multiline)
pipeline: the `cri` stage parses the CRI log format and rejoins partial-line
fragments, and the multiline stage joins application-level multi-line entries
such as stack traces (Go, Java, Python, .NET, Ruby, Rust, PHP). Reads and
discovery are event-driven (fsnotify, `-logs-watch`) with a polling fallback
(`-logs-poll-interval`). File identity is the inode plus a head fingerprint
(`-logs-fingerprint-bytes`), so checkpoints never mis-resume into a
different file after inode reuse or in-place rewrites; rename rotation
drains the old file to EOF before switching, truncation restarts at zero,
and removed files are drained before being dropped. A multi-line group (a CRI
partial-line run or a stack trace) that **straddles one or more rename
rotations is joined into one record** rather than split: the pipeline is
carried across each inode switch, and — for zero loss across a crash
mid-rotation — the rotated-away files are recorded in the checkpoint and
re-read in order on restart to reconstruct the group. (This spans only
rotations the agent observed; a rotation so fast the intermediate file is
never read loses that segment, as with any tailer.) It exports
OTLP log records with resource attributes (`k8s.pod.name`,
`k8s.deployment.name`, `container.id`, pod/namespace labels, …) resolved via
`GET /v1/containers/{id}` — the blocking wait covers containers whose
metadata has not reached the API server yet. Delivery is at-least-once:
batches (`-logs-batch-size` / `-logs-flush-interval`) are retried, file
offsets are committed only after a successful export, and committed offsets
are checkpointed to disk (`-checkpoint-file`) so restarts resume where they
left off. `-logs-pipelined-export` overlaps reading with delivery (one export
in flight; its commit/rewind applies before the next flush — the invariants
are unchanged). Per-file backlog is visible as `kubescrape_log_lag_bytes` and
on `GET /debug/tailer`; a per-file line **rate limit** (`-logs-rate-limit`,
pause or drop) keeps one runaway pod from consuming the pipeline. Set
`-logs-exclude-namespaces` to the observability namespace to avoid feeding
the collector its own output.

**Unified config file** (`-config`). All of the agent's YAML configuration
lives in one file, passed with `-config`. It has five optional sections, each
described below and each mirroring the shape of the standalone file it
replaces:

```yaml
resourceAttributes: {...}   # how exported resource attributes are built
logs:          {sources: [...], rules: [...]}   # what to tail; drop/keep/sample
logAttributes: {rules: [...]}     # lift line keys onto attributes
logMetrics:    {metrics: [...]}   # metrics derived from log lines
metrics:       {pipelines: {...}, splitters: [...]}   # scraped-series rules
```

**Log sources** (`logs` section). By default the agent tails container logs
under `-log-dir`. The `logs` section instead declares **sources** — files
selected by include/exclude globs (doublestar `**` supported), each either
*containerd* (CRI parsing + pod metadata, as above) or *plain* (arbitrary host
files with static resource attributes). Plain files use the **identical**
rotation, checkpoint and cross-rotation multi-line machinery; they just skip
CRI parsing and metadata resolution and take their resource attributes from the
source's `attributes` (plus node attributes, with `service.name` defaulting to
the source name):

```yaml
logs:
  sources:
    - name: containers
      include: ["/var/log/containers/*.log"]
      containerd: true
    - name: host
      include: ["/var/log/**/*.log"]
      exclude: ["/var/log/containers/*.log", "/var/log/azure/*.log"]
      attributes: {service.name: host-syslog}
```

A file is claimed by the first matching source; the default (no config) is one
containerd source over `-log-dir`, so container logs keep working unchanged.
Per source you can also set `compressed: true` (or a `.gz` name) to read gzip
archives — decompressed once to completion, resuming correctly across a
restart. `-logs-file-attributes` (opt-in) stamps `log.file.name` (basename) and
`log.file.position` (the byte offset of the record's start) on every record,
for each file source.

**Log rules** (`logs.rules`). Ordered first-match-wins keep/drop/sample rules
over every exported record — the cost lever: drop debug lines, health-check
noise or whole chatty matchers, or keep a deterministic sample of them. The
selector DSL and key resolution are shared with `logMetrics`
(`match`/`matchRegexp`, record + resource attributes, line JSON/logfmt fields,
`__line__` for the raw body) plus `__severity__` for the enriched severity —
so `action: drop, match: ["__severity__=debug"]` needs no per-app parsing.
Rules run after enrichment and after log metrics (metrics still count every
line — count errors while dropping them); dropped records advance offsets
like exported ones and count into `kubescrape_log_rules_dropped_total`.

**Log enrichment** (`-logs-enrich`, default true). Each exported line is run
through [JohanLindvall/enrich](https://github.com/JohanLindvall/enrich),
which recognizes JSON (Serilog/Pino/Envoy/Azure envelopes and common key
spellings), logfmt, and a table of plain-text formats (nginx, klog, redis,
syslog prefixes, Go/Java/Python/.NET stack traces). Whatever the line itself
carries is promoted into the OTLP record — a parsed timestamp replaces the
CRI write time, an explicit level sets the severity, trace/span IDs land in
the first-class trace fields (GUID-style request IDs included), and template
/ source-context / service / exception details become record attributes
(`log.template`, `log.source_context`, `exception.type`, …). The body is
never modified, and lines without recognizable metadata are exported
unchanged. Stack traces recognized in plain text are *not* duplicated into
`exception.stacktrace` — they already are the body; JSON-carried traces are,
since there the body is the raw JSON. Hit rates per strategy are exported as
`kubescrape_log_enriched_total{format="json|logfmt|pattern|none"}` on the
agent's self-metrics (pushed over OTLP).

**Log attributes from the line** (`logAttributes` section). Beyond the fixed
set enrich recognizes, this section lifts *arbitrary* keys out of a structured
line onto the record. Each rule names a JSON or logfmt `key` (dotted keys
descend into nested JSON), the `attribute` to set (defaults to the key), and a
`target` of `resource`, `scope`, or `log` (default):

```yaml
logAttributes:
  rules:
    - key: tenant             # {"tenant":"acme",...} or tenant=acme
      attribute: tenant.id
      target: resource        # groups records with different tenants into
                              # separate OTLP resources
    - key: http.status_code   # nested JSON path
      target: log
```

JSON is scanned once for all rules with the
[lightning](https://github.com/JohanLindvall/lightning) toolkit and logfmt
with the [logfmt](https://github.com/JohanLindvall/logfmt) reader — no
`encoding/json` in the hot path. Values keep their type (numbers → int/double,
booleans → bool). Because resource and scope attributes determine an OTLP
record's grouping, records whose line-derived resource/scope attributes differ
are split into distinct `ResourceLogs`/`ScopeLogs`. The same config applies to
journald messages.

**Log-derived metrics** (`logMetrics` section). Rather than shipping every line,
the agent can distill lines into metrics and export only those over OTLP. Each
entry declares a `counter` (default), `gauge`, `histogram` or `summary`; the
lines it applies to (`match` / `matchRegexp` selectors); the `value` to observe;
and the `labels` to carry. Values, label keys and selectors resolve against the
record's enriched attributes and resource attributes (k8s metadata) first, then
**straight from the log line's own JSON or logfmt fields** (dotted keys descend
into nested JSON) — so a metric can read any field of the line with no separate
`logAttributes` config. Series expire after `maxAge` of inactivity and are
capped at `maxCardinality` unique label combinations (hard cap 10000).

**Resource attributes.** The log line's own resource attributes (the pod's k8s
identity: namespace, pod, container, node, `service.name`, owners, and the
derived `service.namespace` / `service.instance.id`) become the metric's OTLP
**resource** — so log metrics group per-pod just like scraped metrics, giving
Mimir a proper `job`/`instance`/`target_info`. The metric's own `labels` stay on
the **data points**. To make a log-derived value a resource attribute instead,
list it under `resourceLabels` (same DSL as `labels`).

```yaml
logMetrics:
  metrics:
    - name: http_requests_total
      type: counter
      value: "1"                      # count matching lines
      match: ["level=info"]
      labels:                         # → data-point attributes
        - status=$http_status         # passthrough of the line's http_status
        - class=$http_status(_xx)     # 503 → 5xx (mask all but the first char)
        - method                      # bare key: label "method" = field "method"
      resourceLabels:                 # → resource attributes (alongside the pod's)
        - tenant=$tenant
    - name: request_duration_seconds
      type: histogram
      value: duration_s               # observe this numeric field
      buckets: [0.1, 0.5, 1, 5]
      match: ["msg=request completed"]
    - name: goroutine_panics_total
      type: counter
      value: "1"
      matchRegexp: ["__line__=^panic:"]  # __line__ matches the whole raw line
    - name: slow_request_seconds_total
      type: counter
      valueRegexp: 'took ([0-9.]+)s'  # capture a number out of an unstructured line
      matchRegexp: ["__line__=slow request"]
    - name: connections
      type: gauge
      action: inc                     # set (default)|inc|dec|add|sub|min|max|avg|first|sum|count|stddev|range|delta
      match: ["event=connect"]
```

Extras beyond the basics:

- **`resourceLabels`** lifts a log-derived label onto the resource instead of the
  data point (e.g. a `tenant` field). The pod's k8s resource attributes are
  always on the resource.
- **`__line__`** is a synthetic key holding the whole raw line, so
  `match`/`matchRegexp` (and labels) can filter on line contents directly.
- **`valueRegexp`** pulls the observed value out of an unstructured line via a
  regex capture group (mutually exclusive with `value`).
- **Gauge `action`** — `set` (default, last value wins), `inc`/`dec` (±1 per
  line), `add`/`sub` (±the value), or a windowed aggregation over the values seen
  in a window: `min`, `max`, `avg`, `first`, `sum`, `count` (matching lines),
  `stddev` (population), `range` (max−min), `delta` (last−first). An aggregation
  emits its value on every export and keeps emitting it while no new value
  arrives; the first value after an export starts a fresh window (so `avg` is a
  per-scrape-window mean, like the old avg-gauge).

Only these configured metrics are exported (no internal bookkeeping series).
The export interval, chunk size and an optional name prefix are runtime flags:
`-logs-metrics-interval` (default 30s), `-logs-metrics-max-bytes` and
`-logs-metrics-name-prefix`.

**Positions.** `-checkpoint-file` persists log offsets. `-positions-file`
persists BOTH log offsets and the journald cursor in a single JSON file (one
thing to mount), so a restart resumes every input from one place; it overrides
`-checkpoint-file` for logs and is the only way the journald cursor is
persisted (without it, journald begins at the tail each start).

**Disk buffer** (`-buffer-dir`, opt-in). By default the agent's durability is
checkpoint-and-rewind: on a collector outage the tailer stops advancing and the
source files *are* the buffer — simple, but a long outage risks loss if those
files rotate away, and scraped metrics are just dropped and re-scraped. Point
`-buffer-dir` at a (node-local, persistent) directory and every export instead
goes through a **disk-backed write-ahead buffer** — separate on-disk FIFO
spools for logs and metrics (`internal/agent/spool`). A batch is serialized,
`fsync`'d to disk, and acknowledged to the producer immediately (so the tailer
commits its offsets and the source logs may rotate away), then a background
sender drains the spool to the collector with retries; a batch is removed only
after the collector accepts it. Delivery stays at-least-once and **survives
agent restarts** (a torn tail from a crash is truncated on reopen). The
undelivered backlog is bounded per signal by `-buffer-max-bytes` (default 1
GiB); when full, `Append` fails and the tailer back-pressures by rewinding, so
disk use stays capped. This is the Fluent-Bit-style `filesystem` buffer: it
absorbs outages up to the cap instead of pinning to source files.

**Metrics.** Each `-scrape-interval` the agent fetches
`GET /v1/nodes/$NODE/targets` and scrapes every target concurrently
(bounded by `-scrape-concurrency`). The exposition body is **stream-parsed**
— constant memory per target regardless of size — and converted into OTLP
metric batches of at most `-metrics-batch-size` data points (default 10 000),
each exported and released before parsing continues, so a target exposing
100k+ series never resides in memory (measured: ~28 MB agent RSS while
continuously scraping a 100 000-series endpoint). Conversion is type-faithful:
counters become cumulative monotonic sums; histogram families
(`_bucket`/`_sum`/`_count`) are grouped per label set into proper OTLP
**Histogram** data points (de-cumulated bucket counts, explicit bounds);
summaries become OTLP **Summary** points with quantile values; gauges and
untyped series become gauges. Family grouping preserves the streaming
property — state is bounded by the largest single family, not the scrape.
With `-scrape-exemplars` the agent negotiates the OpenMetrics format and
attaches **exemplars** to counter and histogram points (`trace_id`/`span_id`
map to the OTLP trace/span fields, other exemplar labels become filtered
attributes). `-scrape-max-samples` can cap pathological targets. After each
scrape cycle the agent exports synthetic **health gauges** per target —
`up` (1/0), `scrape_duration_seconds` and `scrape_samples_scraped` — under
the target's own resource attributes (`-scrape-health-metrics`, default
true), so dead endpoints are visible exactly like with Prometheus.

**Kubelet metrics.** With `-kubelet-endpoint` (e.g.
`https://$(NODE_IP):10250`) the agent also scrapes, authenticated with its
ServiceAccount token (`nodes/metrics` RBAC, see
[deploy/agent.yaml](deploy/agent.yaml)):

* **cadvisor** (`/metrics/cadvisor`): per-container cgroup metrics, split
  into one OTLP resource per pod and container. The `id` label (the cgroup
  path, e.g. `/kubepods/burstable/pod<uid>/<containerID>`; both cgroupfs and
  systemd layouts) is the primary identity: the container ID resolves the
  **exact container incarnation** through `GET /v1/containers/{id}`, and the
  pod UID disambiguates same-name pod recreations. Pod-level series without
  a container cgroup — such as `container_network_*` — resolve by name via
  `GET /v1/pods/{namespace}/{name}`, cross-checked against the cgroup pod
  UID. Identity labels move into the resource attributes (owners, labels,
  namespace metadata included); the remaining labels stay on the data
  points, except that on pod/container-identified rows the redundant
  `id`/`name`/`image` labels are elided (the cgroup path and runtime name are
  already resolved into the resource identity, and on network rows they name
  the pause container — `image` is kept as `container.image.name` when
  metadata could not be resolved). Rollup rows keep `id`, their only
  distinguisher. `-cadvisor-rollups=false` drops the rollup aggregates — the
  cgroup hierarchy above pods (`/`, `/kubepods`, QoS and system slices) and
  pod-level rows of container-scoped families (the pod cgroup rolls its
  containers up) — while keeping container-level series, genuinely
  pod-scoped families (`container_network_*`) and `machine_*`.
* **node metrics** (`/metrics`): the kubelet's own metrics under a node-level
  resource (`k8s.node.name`, `service.name: kubelet`).

**journald** (opt-in, `-journald`). The agent reads the systemd journal
natively through libsystemd (`coreos/go-systemd/sdjournal`) and exports the
entries as OTLP log records, one resource per unit (`service.name` = unit
without `.service`, `systemd.unit`, plus the configured node attributes; syslog
priorities map to OTLP severities). Because it links libsystemd, the **agent
binary is built with cgo** (the metadata service stays fully static) and the
image ships libsystemd — no `journalctl` binary or subprocess. Delivery is
at-least-once: the cursor of the newest exported entry is persisted (via
`-positions-file`) only after a successful export, and on export failure or a
reader error it restarts from the committed cursor with backoff.
`-journald-units` restricts to specific units and `-journald-dir` reads a
non-default journal directory (e.g. `/run/log/journal`). `-journald-enrich`
(default true) applies the same per-line enrichment as `-logs-enrich`; an
explicit level found in the message wins over the journal priority.

**OTLP ingest** (opt-in `-ingest`). Applications on the node can push their
own OTLP to the local agent, which enriches it with Kubernetes attributes and
forwards it — closing the gap that otherwise needs a separate collector with
the k8sattributes processor. The agent listens for OTLP/gRPC
(`-ingest-grpc-endpoint`, default `:4317`) and OTLP/HTTP protobuf (gzip
bodies accepted)
(`-ingest-http-endpoint`, default `:4318`, on `/v1/logs`, `/v1/metrics` and
`/v1/traces`). Traces are enriched the same way and passed through
(`-ingest-traces`; they bypass the disk buffer — the pushing sender owns
retry).
For each pushed resource it finds a container ID (`container.id` /
`k8s.container.id`, keys configurable) or a pod UID (`k8s.pod.uid`), resolves
the metadata service (a container ID pins the exact incarnation), and merges
the k8s resource attributes **without overwriting anything the sender already
set**. Pushed log bodies additionally run the same line enrichment as the
tailer (`-ingest-logs-enrich`, filling only fields the sender left unset).
Metrics resolve per `-ingest-metrics-mode`: `resource` (the ID is a resource
attribute), `datapoint` (the ID is a per-point label; points are split into
one resource per object, as a kube-state-metrics-style stream needs), or
`auto` (resource when every resource carries an ID, else split). With
`-ingest-peer-ip-fallback` (opt-in), a resource carrying **no** ID at all is
attributed to the pod owning the connection's peer IP (live, non-hostNetwork
pods only, via `GET /v1/pod-ips/{ip}`) — so unmodified SDKs get k8s
attribution with zero sender configuration. `-ingest-batch-items` coalesces
pushed payloads per signal before forwarding (collector batch-processor
semantics; pair with `-buffer-dir` for at-least-once). Enrichment
outcomes count into `kubescrape_ingest_resources_total{outcome}` (including
`peer_ip`).

**Pipeline toggles.** Each pipeline is individually switchable: `-logs`,
`-metrics` (annotation-discovered targets), `-cadvisor` and `-node-metrics`
(all default true; the kubelet scrapes additionally require
`-kubelet-endpoint`), plus the opt-in `-journald` and `-ingest`.

**Self-observability.** The agent's own metrics — log entries/bytes/rotations
and export failures, enrichment hit rates per format, scrapes and scrape
duration/samples per pipeline, exports per signal and outcome, metadata
lookups, journal entries and reader restarts — are produced through the same
internal metrics machinery as everything else and **pushed over OTLP** on
`-self-metrics-interval` (default 1m, 0 disables) with the agent's own
resource identity (`service.name: kubescrape-agent`, `k8s.node.name`).
`-listen` (default `:8081`) serves `GET /healthz`, `GET /readyz`,
`GET /debug/tailer`, and `GET /metrics` with the Go runtime and process
metrics (`go_*`, `process_*`) only.

**Metric filtering and splitting** (`metrics` section). This section has two
subsections. `pipelines` holds ordered keep/drop rules per pipeline
(`all`, `targets`, `cadvisor`, `node`) — first match wins, no match keeps;
rules match the series name and label values with anchored regexes, so
"drop `container_network_*` except `interface=eth0`" is a keep rule followed
by a drop rule. `splitters` re-attribute targets whose series describe
*other* objects (kube-state-metrics style): per-target match + per-family
`groupBy` rules move identity labels into per-object resources, optionally
enriched through the metadata service (by `container.id` or namespace/name,
cross-checked against a mapped pod UID). Unmatched series stay on the
target's own resource. `datapointAttributes` (default `[k8s.node.name]`) lists
resource attributes to emit on the **data points** instead of the resource — a
described object's node is a property of the object, so it stays a queryable
series label rather than part of the resource identity / `target_info` (the
cmb-alloy placement); set it to `[]` to keep everything on the resource, or list
more attributes to demote. Regular (non-split) scrape/cadvisor/node resources
keep `k8s.node.name` as a resource attribute (the agent's node). Each split rule
also gets an `instancePrefix` (default: the describing target's `service.name`,
e.g. `kube-state-metrics`) prepended to `service.instance.id` so a described
object's series don't collide with its own self-scraped metrics (`""` disables
it), a `dropLabels` regex omitting matching label names from the data points
(e.g. `label_.+` on `kube_.+_labels` families), and set-if-absent `attributes`
fallbacks (e.g. `service.name: unknown` for label-derived resources).

**Resource attributes.** How resource attributes are built is configurable
and applies uniformly to log and metric resources. The built-in mapping also
derives, for Prometheus/Mimir, `service.namespace` = the k8s namespace and
`service.instance.id` (fallback chain: `container.id`, pod-uid[/container],
namespace/pod[/container], node) — so `job` = `service.namespace/service.name`
and `instance` are unique. Both are omitted when a template sets them. Pods
also carry `k8s.pod.ip` as a resource attribute (accessible in templates as
`.Pod.PodIP`; drop it via `-resource-attrs-disable` if unwanted).

An optional `instancePrefix` prepends `prefix-` to the derived
`service.instance.id`. It defaults to `cadvisor` for the cadvisor pipeline (and
to the describing target's `service.name` for splitter rules) so that
describing exporters — whose resources share the pod's `service.name`/namespace
— don't collide with the pod's own self-scraped `target_info`. Set it per
pipeline (or per splitter rule); `""` disables it. An explicit pipeline setting
wins over the built-in default, which wins over a top-level `instancePrefix`.

* `-resource-attrs-enable` / `-resource-attrs-disable` — comma-separated
  regexes matched against the full attribute key (anchored). An attribute is
  exported when it matches the enable set (empty = enable all) and does not
  match the disable set (empty = disable none), e.g.
  `-resource-attrs-disable='k8s\.pod\.label\..*,k8s\.namespace\.label\..*'`
  drops all label attributes.
* `-resource-attrs-static=cluster=prod,env=eu` — fixed attributes added to
  every exported resource.
* `resourceAttributes` section of `-config` — full control, including template
  attributes built from the node/pod/container/service metadata and
  per-pipeline overrides:

  ```yaml
  resourceAttributes:
    defaults: true            # include the built-in k8s.* mapping
    static:
      cluster: prod-eu
    attributes:               # Go templates over {Node, Pod, Container, Service}
      team: '{{ index .Pod.Labels "team" }}'
      container.image: '{{ with .Container }}{{ .Image }}{{ end }}'
      k8s.node.zone: '{{ with .Node }}{{ index .Labels "topology.kubernetes.io/zone" }}{{ end }}'
      service.name: '{{ with .Pod }}{{ coalesce (index .Labels "gp/service-name") (index .Labels "app.kubernetes.io/name") .Name }}{{ end }}'
    pipelines:                # overrides for logs|targets|cadvisor|node|journal|ingest
      node:
        attributes:
          service.name: kubelet
      cadvisor:
        instancePrefix: cadvisor   # default; "" to disable collision prefix
  ```

  Template functions beyond the built-ins: `env`, `coalesce`, `default`,
  `regexMatch`, `regexReplace`. `.Node` carries the node's labels and
  annotations, resolved through the metadata service and refreshed every
  `-node-metadata-refresh`. Template attributes that render empty or fail
  (e.g. `.Container` on a pod-level resource) are omitted. Order: defaults →
  static → templates → filter.

**Export.** `-otlp-protocol` selects gRPC or OTLP/HTTP;
`-otlp-bearer-token-file` (re-read periodically) authenticates either
transport; `-otlp-tls-ca-file`/`-otlp-tls-insecure-skip-verify` control TLS;
metric exports retry with `-otlp-retry-attempts`/`-otlp-retry-backoff`
(logs already retry through the tailer's rewind). Both binaries take
`-log-level` and `-log-format` (text/json), and the metadata service routes
client-go's klog output through the same handler.

## Helm chart

[charts/kubescrape](charts/kubescrape) deploys both components with every
flag exposed as a value, and renders the `agent.config` value verbatim into
the single mounted `-config` file:

```sh
helm install kubescrape charts/kubescrape -n monitoring -f my-values.yaml
```

Migrating from a Grafana Alloy setup? See
[docs/MIGRATING-FROM-ALLOY.md](docs/MIGRATING-FROM-ALLOY.md). For how
kubescrape compares to Alloy/Promtail, Vector, Fluent Bit and the OTel
Collector — features, delivery semantics and measured performance — see
[docs/COMPARISON.md](docs/COMPARISON.md).

For a local test pipeline, `hack/otel-collector.yaml` deploys a contrib
collector with a debug exporter; the agent's own internal metrics stay small.

## Local test cluster

`make cluster-up` creates a three-node [kind](https://kind.sigs.k8s.io/)
cluster (one control plane, two workers), downloading `kind` and `kubectl`
into `hack/bin` if they are not installed. It also deploys sample workloads
([hack/test-workloads.yaml](hack/test-workloads.yaml)): a Deployment with
`prometheus.io/*` annotations and a CronJob, so both endpoints and both
owner-chain shapes (ReplicaSet → Deployment, Job → CronJob) can be exercised:

```sh
make cluster-up
go run ./cmd/kubescrape     # picks up the kind kubeconfig context

node=$(kubectl -n kubescrape-demo get pods -o jsonpath='{.items[0].spec.nodeName}')
curl -s "localhost:8080/v1/nodes/$node/targets" | jq .

cid=$(kubectl -n kubescrape-demo get pods -o jsonpath='{.items[0].status.containerStatuses[0].containerID}')
curl -s "localhost:8080/v1/containers/${cid#containerd://}" | jq .
```

`make cluster-down` deletes the cluster again. Set `CLUSTER_NAME` to use a
different cluster name for both scripts.
