# Configuration reference

kubescrape consists of two binaries built from one image:

* **`kubescrape`** — the metadata service (Deployment). Watches the
  Kubernetes API and serves pod/container metadata and scrape targets over
  HTTP.
* **`kubescrape-agent`** — the per-node agent (DaemonSet). Tails container
  logs and scrapes Prometheus targets, exporting OTLP.

Everything is configured through flags plus two optional YAML files on the
agent (resource attributes, metrics filtering/splitting). The
[Helm chart](../charts/kubescrape) exposes all of it as values; the raw
manifests live in [deploy/](../deploy).

- [Metadata service](#metadata-service)
- [Agent: general](#agent-general)
- [Agent: OTLP export](#agent-otlp-export)
- [Agent: log collection](#agent-log-collection)
- [Agent: journald](#agent-journald)
- [Agent: log attributes](#agent-log-attributes)
- [Agent: OTLP ingest](#agent-otlp-ingest)
- [Agent: metrics scraping](#agent-metrics-scraping)
- [Agent: kubelet scrapes (cadvisor, node)](#agent-kubelet-scrapes)
- [Resource attributes (`-resource-attrs-config`)](#resource-attributes)
- [Metrics config (`-metrics-config`)](#metrics-config)
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
| `-servicemonitors` | `false` | serve targets for `monitoring.coreos.com/v1` ServiceMonitors selecting pod-backed Services (endpoint `port`/`targetPort`/`path`/`scheme`; no per-endpoint auth or relabelings). Self-disables with a warning when the CRD is absent |
| `-events` | `false` | export Kubernetes events as OTLP log records (batched; history from the initial list is skipped; pod events carry full pod resource attributes) |
| `-otlp-*` | as the agent | with `-events`: `-otlp-endpoint`, `-otlp-protocol`, `-otlp-insecure`, `-otlp-tls-ca-file`, `-otlp-tls-insecure-skip-verify`, `-otlp-bearer-token-file`, `-otlp-timeout` |
| `-log-level` | `info` | `debug`, `info`, `warn`, `error` |
| `-log-format` | `text` | `text` or `json` (client-go's klog is routed through the same handler) |

`GET /metrics` serves the service's internal metrics (store sizes, HTTP
requests per pattern/status, exported events).

RBAC (cluster-wide `get`/`list`/`watch`): `pods`, `services`, `namespaces`,
`nodes`, `events`, `replicasets.apps`, `deployments.apps`, `jobs.batch`,
`cronjobs.batch`, `servicemonitors.monitoring.coreos.com` — see
[deploy/kubernetes.yaml](../deploy/kubernetes.yaml).

## Agent: general

| Flag | Default | Description |
|---|---|---|
| `-node-name` | `$NODE_NAME` | the node this agent runs on (set via the downward API) |
| `-listen` | `:8081` | serves `/healthz`, `/readyz` and `/metrics` (the agent's internal metrics); empty disables |
| `-metadata-endpoint` | `http://kubescrape.monitoring` | base URL of the metadata service |
| `-metadata-wait` | `5s` | server-side wait for not-yet-known containers (covers the gap between container start and the kubelet posting its status) |
| `-node-metadata-refresh` | `1m` | refresh interval for the node's labels/annotations used in attribute templates (0 disables) |
| `-log-level` / `-log-format` | `info` / `text` | as for the service |

Pipeline toggles (all default `true`):

| Flag | Enables |
|---|---|
| `-logs` | container log tailing |
| `-metrics` | annotation-discovered pod/service targets |
| `-cadvisor` | `<kubelet-endpoint>/metrics/cadvisor` (needs `-kubelet-endpoint`) |
| `-node-metrics` | `<kubelet-endpoint>/metrics` (needs `-kubelet-endpoint`) |
| `-journald` | systemd journal tailing (default `false`, [below](#agent-journald)) |

## Agent: OTLP export

| Flag | Default | Description |
|---|---|---|
| `-otlp-endpoint` | `otel-collector.monitoring:4317` | `host:port` for gRPC, base URL for HTTP |
| `-otlp-protocol` | `grpc` | `grpc` or `http` (OTLP/HTTP protobuf on `/v1/logs`, `/v1/metrics`) |
| `-otlp-insecure` | `true` | plaintext gRPC (for HTTP, choose via the URL scheme) |
| `-otlp-bearer-token-file` | — | sends `Authorization: Bearer <token>` on either transport; re-read every minute, so rotated tokens work |
| `-otlp-tls-ca-file` | — | PEM CA bundle for verifying the collector |
| `-otlp-tls-insecure-skip-verify` | `false` | skip certificate verification |
| `-otlp-timeout` | `15s` | per export attempt |
| `-otlp-retry-attempts` | `3` | tries per **metrics** export (logs retry via the tailer's rewind, see below) |
| `-otlp-retry-backoff` | `1s` | initial backoff, doubled per attempt |

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
| `-log-dir` | `/var/log/containers` | directory of containerd log symlinks |
| `-checkpoint-file` | — | persists committed offsets across restarts (mount a hostPath); empty disables |
| `-positions-file` | — | single file holding BOTH log offsets and the journald cursor; overrides `-checkpoint-file` for logs and is the only way to persist the journald cursor |
| `-log-attributes-config` | — | YAML file lifting JSON/logfmt keys from the line onto attributes ([below](#agent-log-attributes)) |
| `-logs-watch` | `true` | fsnotify events trigger reads and discovery; polling remains the fallback |
| `-logs-poll-interval` | `500ms` | fallback sweep interval |
| `-logs-fingerprint-bytes` | `1024` | file-head hash length used with the inode as file identity (guards against inode reuse and in-place rewrites); negative = inode only |
| `-logs-batch-size` | `1024` | flush after this many entries |
| `-logs-flush-interval` | `2s` | flush at least this often |
| `-logs-max-entry-bytes` | `1MiB` | truncate assembled entries beyond this |
| `-logs-multiline` | `true` | join stack traces (Go, Java, Python, .NET, Ruby, Rust, PHP) via [multiline](https://github.com/JohanLindvall/multiline) |
| `-logs-multiline-timeout` | `1s` | flush incomplete multi-line groups after this long |
| `-logs-enrich` | `true` | parse per-line metadata via [enrich](https://github.com/JohanLindvall/enrich): a timestamp in the line replaces the CRI time, an explicit level sets the severity, trace/span IDs fill the OTLP trace fields, exception/template/source-context details become record attributes. JSON, logfmt and common plain-text formats are recognized; the body is never modified, and plain-text stack traces are not duplicated into `exception.stacktrace`. Hit rates: `kubescrape_log_enriched_total{format}` on `/metrics` |
| `-logs-exclude-namespaces` | — | comma-separated namespaces not tailed — **always exclude the namespace of your collector** to avoid feedback loops |

Delivery is at-least-once: offsets are committed only after the collector
acknowledged the batch and never past lines still buffered in the multiline
pipeline; on export failure the files rewind to the committed offset.
Rotation handling (rename, copytruncate — including same-size rewrites —
deletion) is automatic.

## Agent: journald

Opt-in with `-journald`. The agent runs `journalctl -f -o json` as a
subprocess and exports the entries as OTLP log records, one resource per
systemd unit (`service.name` = the unit without `.service`, `systemd.unit`,
plus node attributes via the `journal` attrs pipeline; syslog priorities map
to OTLP severities; `syslog.identifier` and `process.pid` become record
attributes).

| Flag | Default | Description |
|---|---|---|
| `-journald-path` | `journalctl` | the binary — the default distroless image does **not** contain it; use an image that does |
| `-journald-dir` | — | journal directory (`journalctl -D`); empty reads the system default |
| `-journald-units` | — | comma-separated units; empty reads everything |
| `-journald-batch-size` | `1024` | flush after this many entries |
| `-journald-flush-interval` | `2s` | flush at least this often |
| `-journald-enrich` | `true` | per-message enrichment as `-logs-enrich`; an explicit level in the message wins over the journal priority |

Delivery is at-least-once: the cursor is committed only after a successful
export; on export failure or subprocess death, journalctl restarts from the
committed cursor with backoff (re-reading anything in flight). The cursor is
persisted only through `-positions-file` (there is no standalone journald
cursor file); without it, every start begins at the journal tail.

## Agent: log attributes

`-log-attributes-config` points at a YAML file lifting configured keys out of
each structured log line (JSON or logfmt) onto the exported record. Applies to
both `-logs` and `-journald`.

```yaml
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

## Agent: OTLP ingest

Opt-in with `-ingest`: the agent receives OTLP that apps push to the node and
enriches each resource with k8s attributes deduced from a container ID or pod
UID already on the data, forwarding through the same exporter. Enrichment
never overwrites an attribute the sender set.

| Flag | Default | Description |
|---|---|---|
| `-ingest-grpc-endpoint` | `:4317` | OTLP/gRPC listen address; empty disables |
| `-ingest-http-endpoint` | `:4318` | OTLP/HTTP protobuf listen address (`/v1/logs`, `/v1/metrics`); empty disables |
| `-ingest-metrics-mode` | `auto` | `resource` (ID on the resource), `datapoint` (ID per point → split into per-object resources), or `auto` |
| `-ingest-logs-enrich` | `true` | parse pushed log bodies as `-logs-enrich`, filling only fields the sender left unset |
| `-ingest-container-id-keys` | `container.id,k8s.container.id` | attribute keys inspected for a container ID |
| `-ingest-pod-uid-keys` | `k8s.pod.uid` | attribute keys inspected for a pod UID |
| `-ingest-metadata-wait` | `0` | how long a lookup may block for a not-yet-known object |

A container ID resolves the exact container incarnation; a pod UID resolves
the pod. Outcomes count into `kubescrape_ingest_resources_total{outcome}`
(`enriched` / `unresolved`).

## Agent: metrics scraping

| Flag | Default | Description |
|---|---|---|
| `-scrape-interval` | `30s` | one cycle scrapes every target of this node |
| `-scrape-timeout` | `15s` | per target |
| `-scrape-concurrency` | `4` | concurrent target scrapes |
| `-metrics-batch-size` | `10000` | export chunk size in data points — a 100k-series target is exported in ten chunks and never held in memory |
| `-scrape-max-samples` | `0` | abort a single scrape beyond this many samples (0 = unlimited) |
| `-scrape-exemplars` | `false` | negotiate OpenMetrics and attach exemplars to counter and histogram points (`trace_id`/`span_id` map to OTLP trace/span fields) |
| `-scrape-health-metrics` | `true` | export synthetic `up`, `scrape_duration_seconds` and `scrape_samples_scraped` gauges per target after each cycle |
| `-metrics-config` | — | YAML file with series filters and target splitters ([below](#metrics-config)) |

Histograms and summaries are converted to proper OTLP Histogram/Summary
points (de-cumulated buckets, explicit bounds, quantiles); counters become
cumulative monotonic sums.

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

## Resource attributes

`-resource-attrs-config` points at a YAML file controlling how resource
attributes are built for **all** exported data (logs and metrics). Quick
knobs also exist as flags:

* `-resource-attrs-static=cluster=prod,env=eu` — fixed attributes.
* `-resource-attrs-enable=<regex,...>` / `-resource-attrs-disable=<regex,...>`
  — anchored regexes on the attribute key; an attribute is exported when it
  matches the enable set (empty = all) and not the disable set (empty =
  none).

The config file:

```yaml
# Include the built-in mapping: k8s.namespace.name, k8s.pod.name,
# k8s.pod.uid, k8s.node.name, owners (k8s.deployment.name, ...), pod labels
# (k8s.pod.label.*), namespace labels, container.id, container.image.name,
# service.name (top owner). Default true.
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

# Per-pipeline overrides (logs | targets | cadvisor | node | journal | ingest); maps
# merge with the pipeline entry winning.
pipelines:
  node:
    attributes:
      service.name: kubelet
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

`-metrics-config` points at a YAML file with two sections.

**`pipelines`** — ordered keep/drop rules per pipeline (`all` is prepended
to every pipeline; then `targets`, `cadvisor`, `node`). First matching rule
decides; no match keeps the series. Regexes are anchored; `labels` matchers
must all match (a missing label matches `""`). Filtering happens on the
scraped series names (`foo_bucket`, …) before histogram grouping.

```yaml
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

```yaml
splitters:
  - match:                        # all set fields must match the target pod
      namespace: monitoring       # anchored regex
      podLabels:
        app.kubernetes.io/name: kube-state-metrics
    rules:
      - metrics: 'kube_pod_.+'
        groupBy:
          namespace: k8s.namespace.name
          pod: k8s.pod.name
          uid: k8s.pod.uid
          container: k8s.container.name
          node: k8s.node.name
        enrich: true
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

With `-servicemonitors` on the metadata service, Prometheus-Operator
ServiceMonitors are a third target source: a monitor's `selector` picks
Services by label (within `namespaceSelector`), each endpoint's `port` names
a service port (or `targetPort` addresses the pod port directly), and `path`
and `scheme` are honored. Everything else on the endpoint (authentication,
relabelings, intervals) is ignored — scraping stays node-local and
unauthenticated.

## Helm values

Every flag above maps to a value; `agent.attrsConfig` and
`agent.metricsConfig` are rendered verbatim into the mounted config files
(with checksum annotations, so config changes roll the DaemonSet). See
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

  attrsConfig:
    attributes:
      k8s.node.zone: '{{ with .Node }}{{ index .Labels "topology.kubernetes.io/zone" }}{{ end }}'
      service.name: >-
        {{ with .Pod }}{{ coalesce (index .Labels "app.kubernetes.io/name")
        (index .Labels "app") .Name }}{{ end }}

  metricsConfig:
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
              node: k8s.node.name
            enrich: true
          - metrics: 'kube_.+'
            groupBy: {namespace: k8s.namespace.name}
```

```sh
helm install kubescrape charts/kubescrape -n monitoring -f values.yaml
```
