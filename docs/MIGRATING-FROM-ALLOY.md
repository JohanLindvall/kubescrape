# Migrating from Grafana Alloy to kubescrape

This guide maps each Alloy component onto its kubescrape equivalent.
kubescrape is deployed with the [Helm chart](../charts/kubescrape); unlike
the Alloy configuration, nothing is hard-coded — every behavior below is a
flag or a config-file entry. If this is also your first kubescrape install,
read [FIRST-RUN.md](FIRST-RUN.md) alongside it: the mapping tells you what to
configure, the runbook tells you what to watch while it comes up.

## Architecture differences

| | Alloy | kubescrape |
|---|---|---|
| Topology | 3–5 clustered Deployment replicas | metadata service (Deployment; every replica serves reads from its own informer caches, no coordination needed) + per-node agent (DaemonSet) |
| Target distribution | Alloy clustering | node-local by construction (each agent scrapes its node's pods) |
| Kubernetes access | every replica watches pods/nodes/services | only the metadata service watches; agents talk HTTP to it |
| Logs | not collected | collected (CRI + multiline joining, at-least-once, drop/keep/sample rules, per-file rate limit) |
| Delivery | batch processor + sending queue | logs: checkpointed at-least-once with rewind; metrics: bounded retries |

## Top-level blocks

The `logging` block maps to one flag on both binaries: `-log-level`
(debug/info/warn/error). There is no format choice — kubescrape logs logfmt,
always, and a `-log-format` in `extraArgs` is `flag provided but not defined`
plus exit 2 rather than an ignored setting (see
[CONFIGURATION.md#logging](CONFIGURATION.md#logging)). `livedebugging` maps to the agent's
`GET /debug/otlp` — the same live-inspection idea, but per HTTP session
instead of a process-wide toggle: resource-attribute glob filters and a
sample percentage ride the request, a built-in page lives at
`/debug/otlp/ui`, and an agent nobody is watching pays one atomic load per
export — but unlike a `livedebugging` block, reading it needs either a local
connection (`kubectl port-forward`) or the `-debug-token-file` token. Each
binary also serves a `/debug` homepage indexing its whole debug
surface (`/` redirects there).

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
disable individually with `agent.cadvisor` / `agent.nodeMetrics`. That one
value is right on IPv6 clusters too — `status.hostIP` expands to a bare IPv6
literal, which is not a parseable URL authority, and the agent brackets the
host itself; do not pre-bracket it, since `[$(NODE_IP)]` renders `[10.0.0.5]`
on IPv4 and Go rejects that as an invalid IP-literal. The
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
pipeline and `summary` on the `-kubelet-summary` one, the target's
`service.name` on splitter rules, `""` disables;
top-level `resourceAttributes.instancePrefix` covers the
cluster-name-prefix rule for shared tenants). Placement nuances:

* `k8s.node.name` stays a resource attribute except on split (KSM-style)
  resources, where `datapointAttributes` demotes it — exactly the
  `set_otel_attrs` datapoint/resource split.
* `k8s.pod.ip` is a **resource** attribute here (a deliberate deviation:
  cmb-alloy demotes it to a datapoint attribute); drop it with
  `resourceAttributes.disable: ['k8s\.pod\.ip']` if your backend treats pod IPs
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

Unwanted attributes are removed with `resourceAttributes.disable` (the
`delete_key` transforms) — a list of anchored regexes in `agent.config`,
e.g. `resourceAttributes: {disable: ['k8s\.pod\.ip']}`. `net.host.name`/
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

An Alloy pipeline shipping different signals to different backends (a
`loki.write` + `prometheus.remote_write` + `otelcol.exporter` fan-out) maps
onto the config file's `export` section: per-signal OTLP endpoint/protocol/
headers/auth overrides riding the same per-signal disk buffer, plus static
tenancy headers and an mTLS client certificate on the default chain — see
CONFIGURATION.md's "Per-signal destinations". Loki, Mimir and Tempo all
ingest OTLP natively, so no push-protocol or remote-write translation is
involved.

### `output_debug_otlp` / the `debug_otlp_output` pod label

Built in, and on-demand instead of always-on: every agent serves
`GET /debug/otlp` on its `-listen` port — a **streaming** dump of what it is
exporting (logs, metrics and traces, post-transform), one OTLP JSON payload
per line, with resource-attribute filters (`attr=key=value`, `*`/`?`
wildcards on both halves, repeatable and ANDed) and a sample percentage as
query parameters — plus a small built-in page at `/debug/otlp/ui` with
signal checkboxes, filter fields and a start/stop button:

```sh
kubectl port-forward ds/kubescrape-agent 8081 &
curl -sN 'localhost:8081/debug/otlp?signal=logs&attr=k8s.namespace.name=team-*&sample=10'
```

Nothing is duplicated to the collector and nothing costs anything until a
client attaches (one atomic load per export).

> **The data-bearing debug surfaces are not open.** `/debug/otlp`,
> `/debug/otlp/ui` and `/debug/tailer` stream or enumerate the node's whole
> telemetry feed, so they are served only to a **local** connection — which is
> exactly what `kubectl port-forward` produces, so the command above works
> unchanged — or to a request carrying the `-debug-token-file` bearer token.
> Set that flag if you want to read an agent from a central debug pod;
> refusals count `kubescrape_debug_refused_total{reason}` and the startup line
> reports the posture as `debugAccess=local-only` or `debugAccess=token`.

The `debug_otlp_output` pod
label becomes a filter value instead of pipeline wiring: expose it as an
attribute (template below) and stream with
`attr=debug_otlp_output=true`:

```yaml
agent:
  config:
    resourceAttributes:
      attributes:
        debug_otlp_output: '{{ with .Pod }}{{ index .Labels "debug_otlp_output" }}{{ end }}'
```

### `discover_servicemonitors` / `prometheus.operator.servicemonitors`

`service.serviceMonitors: true` in the chart (flag `-servicemonitors` on the
metadata service). Monitors select Services by label within their
`namespaceSelector`; endpoint `port`/`targetPort`/`path`/`scheme` are
honored. **PodMonitors** (endpoints naming container ports) are discovered
under the same flag whenever the cluster serves that CRD — covering
`prometheus.operator.podmonitors` too.

Per-endpoint `tlsConfig.insecureSkipVerify` and `bearerTokenSecret` **are**
interpreted (run the metadata service with `-scrape-auth-secrets` so agents
can fetch the tokens; it needs `secrets get` RBAC, commented in the
manifests, plus `-scrape-auth-token-file` — the bearer token agents present
on `/v1/scrape-auth`, which the chart generates and mounts on both sides when
`service.scrapeAuthSecrets` is set), and the keep/drop subset of
`metricRelabelings` is applied per
sample by the agent (the action is matched case-insensitively, so `Keep`/`Drop`
work as well as `keep`/`drop`). Per-endpoint `interval`/`scrapeTimeout`
overrides **are** interpreted — each target is scheduled on its own period —
so those monitors need no conversion. Relabel actions other than keep/drop, and
authentication schemes beyond `basicAuth`/`authorization`/`bearerTokenSecret`,
are still not interpreted: convert those monitors to annotated Services or
metrics-config rules — or, for relabel power the declarative rules lack, the
transforms file's `targets:` hook, which runs a script per fetched scrape
target per cycle (drop it, rewrite its path). Nothing is dropped silently —
an uninterpreted field is
logged once per monitor and counted in
`kubescrape_monitor_fields_ignored_total{kind}`.

### `loki.source.journal`

`agent.journald.enabled: true` (flag `-journald`): the agent reads the systemd
journal natively through libsystemd (`coreos/go-systemd/sdjournal`, cgo — no
journalctl subprocess), with an at-least-once cursor checkpoint. The default
image ships libsystemd; the agent binary is built with cgo (the metadata
service stays static).

### `input_otlp` (apps pushing OTLP)

Built in — `agent.ingest.enabled: true` (flag `-ingest`) receives OTLP/gRPC
(`:4317`) and OTLP/HTTP (`:4318`) LOGS AND METRICS that apps push to the node, enriches each
resource with k8s attributes from a `container.id`/`k8s.pod.uid` on the data,
and forwards it — replacing the collector-with-k8sattributes-processor you'd
otherwise keep as the OTLP endpoint. The merge leaves the sender authoritative
about *itself* — anything descriptive it set survives — with one deliberate
exception: the **resolved identity keys** (`k8s.namespace.name`,
`k8s.pod.name`, `k8s.pod.ip`, `k8s.node.name`, `k8s.container.name`,
`container.name`) are overwritten with what the metadata service says about
that exact resource, because `routing` keys tenancy on `k8s.namespace.name`
and the listeners authenticate nothing. The lookup keys themselves and the
sender's OTLP service triple are exempt. On a resource that does **not**
resolve there is nothing to correct with, so those same keys are stripped at
receipt instead ([OTLP ingest](CONFIGURATION.md#agent-otlp-ingest)).

Traces go to the separate trace tier (`serviceGraph.enabled: true`, flag
`-service-graph`) rather than to the node's agent: applications point their
exporters at `http://<release>-traces.<ns>.svc:4318`, and that tier enriches,
re-shards by trace id and derives service-graph edges and RED metrics from spans
it can see whole. Pushed payloads are forwarded as received — the role of
`otelcol.processor.batch` stays with your SDK's batch span/log processor or
the downstream collector, and every forward keeps the sender's own retry
semantics (nothing is acknowledged before it is handed on).

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
  replicas; kubescrape leaves a sender-set `service.instance.id` exactly as
  sent — it is one of the two keys both the identity overwrite and the
  receipt-time strip exempt, precisely because a sender names itself with
  them. If replicas
  report colliding instance ids, include the pod uid in the sender's
  `OTEL_RESOURCE_ATTRIBUTES` (as above — `service.instance.id=$(POD_UID)`).

One capacity difference: cmb-alloy raises the receiver's gRPC
`max_recv_msg_size` to 40 MiB, and kubescrape's default is gRPC's own 4 MiB.
Carry the raise over with `agent.ingest.grpcMaxRecvBytes: 41943040` (flag
`-ingest-grpc-max-recv-bytes`; it covers the trace tier's application ports
too, and both of the receiver's memory budgets scale with it — the raw-payload
one to four times the new cap, the decoded-structure one to eight, so size the
pod for it). The OTLP/HTTP body cap
stays 16 MiB. An over-cap push is refused, never truncated, and retrying the
same batch cannot succeed — the alternative to raising the cap is smaller
sender batches (an SDK batch-processor setting).

### `input_pure_otlp` (the unenriched listener pair on 14317/14318)

There is no second listener pair and no per-listener enrichment switch —
but the pure input's job is largely covered by enrichment being fill-if-absent
for everything a sender describes about itself: it fires only on a resource
carrying a `container.id`/`k8s.pod.uid` (connection-IP association stays
opt-in), so a payload from outside the cluster passes through with its
descriptive attributes untouched. What is *not* left alone is the sender's
Kubernetes identity claim, on every push these listeners accept: those keys
are stripped at first receipt (the identity strip —
[OTLP ingest](CONFIGURATION.md#agent-otlp-ingest)) and then re-derived for the
resources the receiver can resolve. So "already fully attributed" means
attributed by *this* cluster, and a sender that ships its own
`k8s.namespace.name`/`k8s.pod.name` from outside it loses them here — carry
that identity under keys of your own, or push to the downstream collector
directly. Residual differences: pushed
log bodies are still parsed for timestamp/severity/trace ids when `-enrich`
is on (a global switch shared with the tailer), configured `logAttributes`,
`logs.rules` and `logMetrics` apply to pushed log records as to tailed ones
(a `target: log` lift is written onto the record; a `target: resource` lift
still resolves for rule keys and metric labels but is not stamped, and a
`target: scope` lift is dropped outright — a pushed record already lives in the
sender's grouping, so there is no ScopeLogs of ours to carry it), and `datapoint`/`auto`
metrics mode may still split a payload whose data points carry IDs
(`-ingest-metrics-mode resource` disables that). One divergence to plan for:
cmb-alloy's `resourcedetection env` stamps `k8s.cluster.name` on every
payload, pure ones included, while kubescrape's static attributes ride the
enrichment merge — an unresolved resource passes with no cluster attribute.
Stamp it in such senders' `OTEL_RESOURCE_ATTRIBUTES`, or at the downstream
collector. A sender that must bypass everything should push to the
downstream collector directly.

### `prometheus.exporter.unix` (node_exporter)

Not covered: kubescrape does not collect host-level system metrics. Keep a
node_exporter DaemonSet and scrape it via `prometheus.io/*` annotations (or
a PodMonitor) like any other target.

### `otelcol.processor.transform` / OTTL statements

The declarative layers (metrics `pipelines` filters, splitters,
`logs.rules`, attribute templates) cover most OTTL uses. For the remainder,
`-transforms-file` runs **Starlark** scripts (`transform(batch)` per signal)
at the exporter seam: mutate or drop log records / metrics / spans (down to
individual data points), with
hot reload (compile-then-commit — a broken edit keeps the last good program)
and a step limit. The same file also carries `route()`/`emit_metric()` verbs
(the routing-connector and count-connector shapes) and four hooks at other
decision points — `ingest:` per-resource admission on the push listeners,
`targets:` per scrape target (the relabel-parity gap above), `sample:` as a
tail-sampling policy body, `parse:` per line of opted-in plain log sources —
see the transforms section of
[CONFIGURATION.md](CONFIGURATION.md#agent-transforms-starlark). It is
deliberately narrower than OTTL: batch- and hook-scoped,
lazy field access, no I/O.

### `otelcol.processor.probabilistic_sampler` / spanmetrics connector

The `traceSampling` config section samples ingested spans with
**consistent per-trace-ID decisions** (all agents keep the same traces given
the same config), `keepErrors`/`keepSlowerThan` guard rails and a
spans/second cap. `-ingest-span-metrics` is the spanmetrics-connector
counterpart (same `traces.span.metrics.*` naming), derived ahead of the
sampler so RED metrics keep full coverage. Cross-node **tail** sampling
(whole-trace decisions on buffered traces) is the `tailSampling` config
section on the `-service-graph` trace tier: the shard buffers a trace's spans
until `decisionWait` elapses and then exports or discards it whole. Note the
one delivery caveat, spelled out in that section — a pushed span is acked
BEFORE its trace is decided, so buffered-but-undecided spans are lost if a
shard is hard-killed (a graceful stop flushes them).

### Per-tenant exporters (`otelcol.exporter.* + X-Scope-OrgID`)

The `routing` config section fans exported payloads out by Kubernetes
namespace: per route a distinct endpoint and/or extra headers (e.g.
`X-Scope-OrgID`), first match wins, everything else stays on the default
chain. Route destinations are direct (unbuffered) — the default chain keeps
the `-buffer-dir` durability.

### Azure Event Hubs (`loki.source.azure_event_hubs`)

`loki.source.azure_event_hubs` maps to `-azure-diagnostics` in the same
singleton Deployment — with two differences in kubescrape's favour: metric
records are converted to **real OTLP gauge data points** rather than shipped
as log lines, and both signals land on the **ARM resource's own identity**
(`cloud.resource_id`, subscription, resource group, `service.name`). Auth
covers the same connection-string path plus managed identity (workload
identity/IMDS). See
[Agent: Azure diagnostics](CONFIGURATION.md#agent-azure-diagnostics).

### Kubernetes events (`loki.source.kubernetes_events`)

`loki.source.kubernetes_events` maps to `-events` — but note the shape
difference: it is a **cluster-singleton** (its own single-replica Deployment
of the agent binary, leader-elected), not part of the DaemonSet, and each
event lands on the *involved object's* resource attributes rather than as a
flat stream. See
[Agent: Kubernetes events](CONFIGURATION.md#agent-kubernetes-events).

## Not covered — keep a collector for these

* **`input_pyroscope` / `output_pyroscope`**: profiles are out of scope;
  push them directly to the backend.
* **`prometheus.operator.probes`**: blackbox probing has no node affinity and
  doesn't fit the node-local model; keep the prometheus-operator prober (or a
  collector's blackbox receiver) for Probe CRDs.

## Rollout approach

1. Deploy kubescrape alongside Alloy with the OTLP output pointed at a
   staging tenant; compare series and attributes.
2. Move the metric filters over first (largest cost lever), then the
   splitters, then attribute parity.
3. Cut over the exporters, scale Alloy down, keep it available for
   rollback until a full retention period has passed.
