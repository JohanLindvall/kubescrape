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
| `-logs-watch` | `true` | fsnotify events trigger reads and discovery; polling remains the fallback |
| `-logs-poll-interval` | `500ms` | fallback sweep interval |
| `-logs-fingerprint-bytes` | `1024` | file-head hash length used with the inode as file identity (guards against inode reuse and in-place rewrites); negative = inode only |
| `-logs-batch-size` | `1024` | flush after this many entries |
| `-logs-flush-interval` | `2s` | flush at least this often |
| `-logs-max-entry-bytes` | `1MiB` | truncate assembled entries beyond this |
| `-logs-multiline` | `true` | join stack traces (Go, Java, Python, .NET, Ruby, Rust, PHP) via [multiline](https://github.com/JohanLindvall/multiline) |
| `-logs-multiline-timeout` | `1s` | flush incomplete multi-line groups after this long |
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
| `-journald-cursor-file` | — | persists the journal cursor across restarts; empty means every start begins at the tail |
| `-journald-batch-size` | `1024` | flush after this many entries |
| `-journald-flush-interval` | `2s` | flush at least this often |

Delivery is at-least-once: the cursor is committed only after a successful
export; on export failure or subprocess death, journalctl restarts from the
committed cursor with backoff (re-reading anything in flight).

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

# Per-pipeline overrides (logs | targets | cadvisor | node | journal); maps
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
