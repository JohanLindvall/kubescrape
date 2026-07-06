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
| Logs | not collected | collected (CRI + multiline joining, at-least-once) |
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
  attrsConfig:
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
(`agent.cadvisorRollups: false` replaces the drop-aggregates filters).

### `filter_metrics` + the node-scrape filter

`agent.metricsConfig.pipelines`: ordered keep/drop rules, first match wins.
The keep-exception-then-drop shape translates directly:

```yaml
agent:
  metricsConfig:
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

The ~400 lines of `groupbyattrs`/`transform` OTTL become splitter rules:

```yaml
agent:
  metricsConfig:
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
              container_id: container.id
              node: k8s.node.name
            enrich: true        # full metadata via the metadata service
          - metrics: 'kube_.+'
            groupBy: {namespace: k8s.namespace.name}
```

`enrich: true` replaces the `k8sattributes` association: pods resolve by
container ID or namespace/name (UID cross-checked), bringing owners, labels
and namespace metadata along.

### `otel_process_attrs` label chains and identity attributes

The `service.name` / `platform.product.name` fallback chains and
namespace-based defaults are templates:

```yaml
agent:
  staticAttrs:
    k8s.cluster.name: prod-eu        # replaces resourcedetection env
  attrsConfig:
    attributes:
      service.name: >-
        {{ with .Pod }}{{ coalesce (index .Labels "gp/service-name")
        (index .Labels "app.kubernetes.io/name") (index .Labels "app")
        (index .Labels "k8s-app") .Name }}{{ end }}
      platform.product.name: >-
        {{ with .Pod }}{{ coalesce (index .Labels "gp/software-product")
        (index .Labels "software-product") (index .Labels "app.kubernetes.io/part-of") }}{{ end }}
      service.namespace: '{{ with .Pod }}{{ .Namespace }}{{ end }}'
      service.instance.id: >-
        {{ with .Container }}{{ .ID }}{{ else }}{{ with .Pod }}{{ .UID }}{{ end }}{{ end }}
```

Namespace-based defaulting uses `regexMatch`:

```yaml
      platform.product.name: >-
        {{ with .Pod }}{{ if regexMatch "^tigera-operator$|-system$" .Namespace }}gp-infrastructure{{ end }}{{ end }}
```

Unwanted attributes are removed with `-resource-attrs-disable` (the
`delete_key` transforms), e.g. `agent.extraArgs:
["-resource-attrs-disable=k8s\\.pod\\.label\\..*"]`.

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
`-logs-flush-interval`). There is no persistent sending queue: metric
scrapes retry with backoff and re-scrape next interval; log delivery is
at-least-once via checkpointed offsets.

### `output_debug_otlp` / the `debug_otlp_output` pod label

Expose the label as an attribute and filter in the collector:

```yaml
agent:
  attrsConfig:
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

`agent.journald.enabled: true` (flag `-journald`): the agent tails the
systemd journal via a journalctl subprocess with an at-least-once cursor
checkpoint — but the default distroless image contains no journalctl, so
supply an image that does.

## Not covered — keep a collector for these

* **`input_otlp`** (apps pushing OTLP for enrichment/forwarding): kubescrape
  is pull/tail only. Keep an OpenTelemetry collector (with the
  k8sattributes processor) as the OTLP endpoint, or point apps directly at
  the backend.
* **`input_pyroscope` / `output_pyroscope`**: profiles are out of scope;
  push them directly to the backend.

## Rollout approach

1. Deploy kubescrape alongside Alloy with the OTLP output pointed at a
   staging tenant; compare series and attributes.
2. Move the metric filters over first (largest cost lever), then the
   splitters, then attribute parity.
3. Cut over the exporters, scale Alloy down, keep it available for
   rollback until a full retention period has passed.
