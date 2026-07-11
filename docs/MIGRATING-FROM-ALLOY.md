# Migrating from Grafana Alloy to kubescrape

This guide maps each Alloy component onto its kubescrape equivalent.
kubescrape is deployed with the [Helm chart](../charts/kubescrape); unlike
the Alloy configuration, nothing is hard-coded — every behavior below is a
flag or a config-file entry.

## Architecture differences

| | Alloy | kubescrape |
|---|---|---|
| Topology | 3–5 clustered Deployment replicas | metadata service (Deployment) + per-node agent (DaemonSet) |
| Target distribution | Alloy clustering | node-local by construction (each agent scrapes its node's pods) |
| Kubernetes access | every replica watches pods/nodes/services | only the metadata service watches; agents talk HTTP to it |
| Logs | not collected | collected (CRI + multiline joining, at-least-once, drop/keep/sample rules, per-file rate limit) |
| Delivery | batch processor + sending queue | logs: checkpointed at-least-once with rewind; metrics: bounded retries |

## Component mapping

### `scrape_prometheus_k8s_pods` (both instances)

Built in — no configuration. The two Alloy component instances exist only to
support two comma-separated ports via `port_regex`; kubescrape's
`prometheus.io/port` accepts any number of ports and named container ports.

### `scrape_prometheus_k8s_endpoints`

Built in: Services annotated `prometheus.io/scrape` select their pods via
label selectors, with `targetPort` translation. The discovery Services
(coredns, keda) work unchanged.

### `scrape_prometheus_k8s_nodes` (kubelet + cadvisor)

`agent.kubeletEndpoint: https://$(NODE_IP):10250` in the chart enables both;
disable individually with `agent.cadvisor` / `agent.nodeMetrics`. The
`service_name` arguments and node labels become attribute templates:

```yaml
agent:
  config:
    resourceAttributes:
      attributes:
        k8s.node.zone: '{{ with .Node }}{{ index .Labels "topology.kubernetes.io/zone" }}{{ end }}'
        k8s.node.type: '{{ with .Node }}{{ index .Labels "node.kubernetes.io/instance-type" }}{{ end }}'
        k8s.node.agentpool: '{{ with .Node }}{{ index .Labels "agentpool" }}{{ end }}'
        k8s.node.arch: '{{ with .Node }}{{ index .Labels "kubernetes.io/arch" }}{{ end }}'
      pipelines:
        node:
          attributes:
            service.name: aks-node
```

cadvisor attribution is *stronger* than the Alloy pipeline: series are keyed
by the cgroup path, the container ID resolves the exact incarnation, and the
pause-container/`drop_empty_cadvisor` special cases are built in
(`agent.cadvisorRollups: false` replaces the drop-aggregates filters). The
`cadvisor_network`/`groupbyattrs` label cleanup is built in too: on pod- and
container-identified rows the `id`/`name`/`image` labels are elided from the
data points (they duplicate the resolved resource identity; on network rows
they name the pause container), with `image` preserved as
`container.image.name` on resources the metadata service could not resolve.
Rollup rows keep `id` — there the cgroup path is the only distinguisher.
cadvisor resources get `service.instance.id` prefixed with `cadvisor-`
(cmb-alloy's `instance_prefix`, see below); Alloy's per-target
`up`/`scrape_duration_seconds` health series are exported by default
(`agent.scrapeHealthMetrics`, flag `-scrape-health-metrics`).

### `filter_metrics` + the node-scrape filter

`agent.config.metrics.pipelines`: ordered keep/drop rules, first match wins.
The keep-exception-then-drop shape translates directly:

```yaml
agent:
  config:
    metrics:
      pipelines:
        all:
          - action: keep
            metrics: 'envoy_(cluster_(upstream_(rq(_total|_xx|_completed)?|cx_total))|requests_total)'
          - action: drop
            metrics: '(envoy_|otelcol_|prometheus_|rest_client_|cortex_|csi_|grafana_|loki_|thanos_).+'
        node:
          - action: keep
            metrics: 'container_network_(receive|transmit)_bytes_total'
            labels: {interface: eth0}
          - action: drop
            metrics: 'container_network_.+|container_tasks_state|kubelet_runtime_operations_duration_seconds_bucket'
```

### `prometheus_to_otel` (kube-state-metrics, kubelet-stats regrouping)

The ~400 lines of `groupbyattrs`/`transform` OTTL become splitter rules.
Rules are first-match-wins per series, so order them like the Alloy filter
pipelines route: the `kube_.+_labels` rules must come *before* `kube_pod_.+`
(otherwise `kube_pod_labels` lands in the pod pipeline):

```yaml
agent:
  config:
    metrics:
      splitters:
        - match:
            podLabels: {app.kubernetes.io/name: kube-state-metrics}
          rules:
            - metrics: 'kube_node_labels'      # keeps its label_* points
              groupBy: {node: k8s.node.name}
            - metrics: 'kube_.+_labels'        # the kube_state_labels pipeline
              groupBy:
                namespace: k8s.namespace.name
                label_gp_service_name: service.name
                label_app_kubernetes_io_name: service.name
                label_software_product: platform.product.name
                label_app_kubernetes_io_part_of: platform.product.name
              dropLabels: 'label_.+'           # delete_matching_keys(^label_.+$)
              attributes:                      # set ... where attributes[...] == nil
                service.name: unknown
                platform.product.name: unknown
            - metrics: 'kube_pod_.+'           # the kube_state_pod pipeline
              groupBy:
                namespace: k8s.namespace.name
                pod: k8s.pod.name
                uid: k8s.pod.uid
                container: k8s.container.name
                container_id: container.id     # containerd:// prefix stripped
              enrich: true        # full metadata via the metadata service
            - metrics: 'kube_.+'               # the kube_state_rest pipeline
              groupBy: {namespace: k8s.namespace.name}
```

`enrich: true` replaces the `k8sattributes` association: pods resolve by
container ID or namespace/name (UID cross-checked), bringing owners, labels
and namespace metadata along. When several `groupBy` labels map to the same
attribute, labels are applied in name order and non-empty values overwrite —
`label_gp_service_name` sorts after `label_app_kubernetes_io_name`, so the
result is Alloy's `coalesce(gp_service_name, app_kubernetes_io_name)`.
`dropLabels` covers the `delete_matching_keys` datapoint cleanups and
`attributes` the `where … == nil` fallbacks. Two placement/identity nuances
of the Alloy pipeline are defaults here (both overridable per rule):

* `datapointAttributes` (default `[k8s.node.name]`) — the described object's
  node moves onto the data points, mirroring the `set_otel_attrs` transform
  that demotes `k8s.node.name` for kube-state-metrics only; regular
  scraped/cadvisor/node resources keep it as a resource attribute.
* `instancePrefix` (default: the describing target's `service.name`, i.e.
  `kube-state-metrics`) — cmb-alloy's `instance_prefix`, keeping split
  resources' `service.instance.id` from colliding with the described pods'
  own self-scraped `target_info`.

The kubelet-stats regrouping is another splitter matched on that pod, with
`groupBy: {node_name: k8s.node.name, pod_namespace: k8s.namespace.name,
pod_name: k8s.pod.name, container_name: k8s.container.name}` and
`enrich: true`.

### `otel_process_attrs` — Mimir identity (`set_otel_attrs`, `common`)

The whole identity derivation is built in: every resource gets
`service.namespace` (= the k8s namespace) and `service.instance.id`
(fallback chain `container.id` → pod-uid[/container] →
namespace/pod[/container] → node — the `common` transform's `Concat` chain),
neither overwritten if a template already set it. The `instance_prefix`
mechanism is the `instancePrefix` config (default `cadvisor` on the cadvisor
pipeline, the target's `service.name` on splitter rules, `""` disables;
top-level `resourceAttributes.instancePrefix` covers the
cluster-name-prefix rule for shared tenants). Placement nuances:

* `k8s.node.name` stays a resource attribute except on split (KSM-style)
  resources, where `datapointAttributes` demotes it — exactly the
  `set_otel_attrs` datapoint/resource split.
* `k8s.pod.ip` is a **resource** attribute here (a deliberate deviation:
  cmb-alloy demotes it to a datapoint attribute); drop it with
  `-resource-attrs-disable='k8s\.pod\.ip'` if your backend treats pod IPs
  as identity-breaking.

The `service.name` / `platform.product.name` label chains and
namespace-based defaults are templates:

```yaml
agent:
  staticAttrs:
    k8s.cluster.name: prod-eu        # replaces resourcedetection env
  config:
    resourceAttributes:
      attributes:
        service.name: >-
          {{ with .Pod }}{{ coalesce (index .Labels "gp/service-name")
          (index .Labels "app.kubernetes.io/name") (index .Labels "app")
          (index .Labels "name") (index .Labels "component")
          (index .Labels "k8s-app") (index .Labels "control-plane") .Name }}{{ end }}
        platform.product.name: >-
          {{ with .Pod }}{{ coalesce (index .Labels "gp/software-product")
          (index .Labels "software-product") (index .Labels "app.kubernetes.io/part-of") }}{{ end }}
        k8s.instance.name: '{{ with .Pod }}{{ index .Labels "app.kubernetes.io/instance" }}{{ end }}'
```

Namespace-based defaulting uses `regexMatch`:

```yaml
        platform.product.name: >-
          {{ with .Pod }}{{ if regexMatch "^tigera-operator$|-system$" .Namespace }}gp-infrastructure{{ end }}{{ end }}
```

Unwanted attributes are removed with `-resource-attrs-disable` (the
`delete_key` transforms), e.g. `agent.extraArgs:
["-resource-attrs-disable=k8s\\.pod\\.label\\..*"]`. `net.host.name`/
`net.host.port` never exist here, so their deletions have no equivalent.

### `output_otlp`

```yaml
agent:
  otlp:
    endpoint: https://ingest.example.com:443
    protocol: http
    insecure: false
    bearerTokenSecret: {name: alloy-secrets, key: MONITORING_INGEST_TOKEN}
    retryAttempts: 3
    retryBackoff: 1s
```

Batching is inherent (`-metrics-batch-size`, `-logs-batch-size`,
`-logs-flush-interval`), and payloads are gzip-compressed by default
(`-otlp-compression`, klauspost/compress — the counterpart of Alloy's
snappy/otlphttp gzip). By default there is no persistent sending queue:
metric scrapes retry with backoff and re-scrape next interval; log delivery is
at-least-once via checkpointed offsets. For Alloy's disk-buffered WAL, set
`agent.bufferDir` (flag `-buffer-dir`) to spool both logs and metrics to a
disk-backed buffer during a collector outage, bounded by `-buffer-max-bytes`.

### `output_debug_otlp` / the `debug_otlp_output` pod label

Expose the label as an attribute and filter in the collector:

```yaml
agent:
  config:
    resourceAttributes:
      attributes:
        debug_otlp_output: '{{ with .Pod }}{{ index .Labels "debug_otlp_output" }}{{ end }}'
```

with an `otelcol.processor.filter`/debug exporter pair (or a routing
connector) on the receiving collector, exactly as Alloy's
`output_debug_otlp` does.

### `discover_servicemonitors` / `prometheus.operator.servicemonitors`

`service.serviceMonitors: true` in the chart (flag `-servicemonitors` on the
metadata service). Monitors select Services by label within their
`namespaceSelector`; endpoint `port`/`targetPort`/`path`/`scheme` are
honored. Per-endpoint authentication, relabelings and interval overrides are
**not** interpreted — convert those monitors to annotated Services or
metrics-config rules.

### `loki.source.kubernetes_events`

`service.events.enabled: true` in the chart (flag `-events` on the metadata
service): Kubernetes events are exported as OTLP log records with
`k8s.event.*` attributes, and events about pods carry the full pod resource
attributes.

### `loki.source.journal`

`agent.journald.enabled: true` (flag `-journald`): the agent reads the systemd
journal natively through libsystemd (`coreos/go-systemd/sdjournal`, cgo — no
journalctl subprocess), with an at-least-once cursor checkpoint. The default
image ships libsystemd; the agent binary is built with cgo (the metadata
service stays static).

### `input_otlp` (apps pushing OTLP)

Built in — `agent.ingest.enabled: true` (flag `-ingest`) receives OTLP/gRPC
(`:4317`) and OTLP/HTTP (`:4318`) that apps push to the node, enriches each
resource with k8s attributes from a `container.id`/`k8s.pod.uid` on the data
(without overwriting sender-set values), and forwards it — replacing the
collector-with-k8sattributes-processor you'd otherwise keep as the OTLP
endpoint.

Traces are accepted and passed through with the same resource enrichment
(`-ingest-traces`), and `-ingest-batch-items` plays the role of
`otelcol.processor.batch` on the pushed path (coalesce per signal, timeout
flush).

Two association differences from `otelcol.processor.k8sattributes`:

* **Connection-IP association is opt-in** (`-ingest-peer-ip-fallback`,
  Alloy's `pod_association from = "connection"`): a resource with no
  container id / pod uid resolves via the pod owning the connection's peer
  IP (live, non-hostNetwork pods only). Prefer stamping the ID at the
  sender — it is immune to NAT and hostNetwork ambiguity — via the Downward
  API:

  ```yaml
  env:
    - name: POD_UID
      valueFrom: {fieldRef: {fieldPath: metadata.uid}}
    - name: OTEL_RESOURCE_ATTRIBUTES
      value: k8s.pod.uid=$(POD_UID)
  ```

* **No uid-suffixing of sender-set instances**: cmb-alloy appends
  `/<pod uid>` to a pushed `service.instance.id` to force uniqueness across
  replicas; kubescrape never rewrites sender-set attributes. If replicas
  report colliding instance ids, include the pod uid in the sender's
  `OTEL_RESOURCE_ATTRIBUTES` (as above — `service.instance.id=$(POD_UID)`).

## Not covered — keep a collector for these

* **`input_pyroscope` / `output_pyroscope`**: profiles are out of scope;
  push them directly to the backend.

## Rollout approach

1. Deploy kubescrape alongside Alloy with the OTLP output pointed at a
   staging tenant; compare series and attributes.
2. Move the metric filters over first (largest cost lever), then the
   splitters, then attribute parity.
3. Cut over the exporters, scale Alloy down, keep it available for
   rollback until a full retention period has passed.
