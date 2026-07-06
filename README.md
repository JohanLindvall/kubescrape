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
from two sources:

* **pod annotations** — the conventional annotations on the pod itself, and
* **service annotations** — the same annotations on any Service whose
  selector matches the pod; service ports are translated to pod ports via
  their `targetPort` (named container port, explicit number, or the service
  port itself).

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

### `GET /healthz`, `GET /readyz`

Liveness is always `200`; readiness turns `200` once the initial informer
cache sync has completed.

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
| `-resync`       | `0`     | informer resync period (0 = watch stream only)                            |

In-cluster it needs `get`/`list`/`watch` on `pods`, `services`, `namespaces`,
`replicasets.apps`, `deployments.apps`, `jobs.batch` and `cronjobs.batch`
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
and removed files are drained before being dropped. It exports
OTLP log records with resource attributes (`k8s.pod.name`,
`k8s.deployment.name`, `container.id`, pod/namespace labels, …) resolved via
`GET /v1/containers/{id}` — the blocking wait covers containers whose
metadata has not reached the API server yet. Delivery is at-least-once:
batches (`-logs-batch-size` / `-logs-flush-interval`) are retried, file
offsets are committed only after a successful export, and committed offsets
are checkpointed to disk (`-checkpoint-file`) so restarts resume where they
left off. Set `-logs-exclude-namespaces` to the observability namespace to
avoid feeding the collector its own output.

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
attributes). `-scrape-max-samples` can cap pathological targets.

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
  points. `-cadvisor-rollups=false` drops the rollup aggregates — the
  cgroup hierarchy above pods (`/`, `/kubepods`, QoS and system slices) and
  pod-level rows of container-scoped families (the pod cgroup rolls its
  containers up) — while keeping container-level series, genuinely
  pod-scoped families (`container_network_*`) and `machine_*`.
* **node metrics** (`/metrics`): the kubelet's own metrics under a node-level
  resource (`k8s.node.name`, `service.name: kubelet`).

**Pipeline toggles.** Each pipeline is individually switchable: `-logs`,
`-metrics` (annotation-discovered targets), `-cadvisor` and `-node-metrics`
(all default true; the kubelet scrapes additionally require
`-kubelet-endpoint`).

**Metric filtering and splitting.** `-metrics-config` points at a YAML file
with two sections. `pipelines` holds ordered keep/drop rules per pipeline
(`all`, `targets`, `cadvisor`, `node`) — first match wins, no match keeps;
rules match the series name and label values with anchored regexes, so
"drop `container_network_*` except `interface=eth0`" is a keep rule followed
by a drop rule. `splitters` re-attribute targets whose series describe
*other* objects (kube-state-metrics style): per-target match + per-family
`groupBy` rules move identity labels into per-object resources, optionally
enriched through the metadata service (by `container.id` or namespace/name,
cross-checked against a mapped pod UID). Unmatched series stay on the
target's own resource.

**Resource attributes.** How resource attributes are built is configurable
and applies uniformly to log and metric resources:

* `-resource-attrs-enable` / `-resource-attrs-disable` — comma-separated
  regexes matched against the full attribute key (anchored). An attribute is
  exported when it matches the enable set (empty = enable all) and does not
  match the disable set (empty = disable none), e.g.
  `-resource-attrs-disable='k8s\.pod\.label\..*,k8s\.namespace\.label\..*'`
  drops all label attributes.
* `-resource-attrs-static=cluster=prod,env=eu` — fixed attributes added to
  every exported resource.
* `-resource-attrs-config=attrs.yaml` — full control, including template
  attributes built from the node/pod/container/service metadata and
  per-pipeline overrides:

  ```yaml
  defaults: true            # include the built-in k8s.* mapping
  static:
    cluster: prod-eu
  attributes:               # Go templates over {Node, Pod, Container, Service}
    team: '{{ index .Pod.Labels "team" }}'
    container.image: '{{ with .Container }}{{ .Image }}{{ end }}'
    k8s.node.zone: '{{ with .Node }}{{ index .Labels "topology.kubernetes.io/zone" }}{{ end }}'
    service.name: '{{ with .Pod }}{{ coalesce (index .Labels "gp/service-name") (index .Labels "app.kubernetes.io/name") .Name }}{{ end }}'
  pipelines:                # overrides for logs|targets|cadvisor|node
    node:
      attributes:
        service.name: kubelet
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
flag exposed as a value, and renders `agent.attrsConfig` /
`agent.metricsConfig` values directly into the mounted config files:

```sh
helm install kubescrape charts/kubescrape -n monitoring -f my-values.yaml
```

Migrating from a Grafana Alloy setup? See
[docs/MIGRATING-FROM-ALLOY.md](docs/MIGRATING-FROM-ALLOY.md).

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
