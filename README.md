[![CI](https://github.com/JohanLindvall/kubescrape/actions/workflows/ci.yml/badge.svg)](https://github.com/JohanLindvall/kubescrape/actions/workflows/ci.yml)

# kubescrape

Two cooperating services:

* **kubescrape** — an HTTP service serving Kubernetes pod and container
  metadata — including the full ownership chain (ReplicaSet → Deployment,
  Job → CronJob, StatefulSet, DaemonSet, …) and namespace metadata — and deriving Prometheus scrape
  targets for pods from the conventional `prometheus.io/*` annotations (on
  pods or on Services selecting them).
* **kubescrape-agent** — a per-node DaemonSet that tails containerd container
  logs and scrapes the node's Prometheus targets, exporting both as OTLP to
  an [OpenTelemetry collector](https://github.com/open-telemetry/opentelemetry-collector-contrib),
  enriched with resource attributes fetched from the metadata service.

Full flag and config-file reference with examples:
[docs/CONFIGURATION.md](docs/CONFIGURATION.md); the exhaustive generated
per-binary flag inventory: [docs/FLAGS.md](docs/FLAGS.md). Every
`kubescrape_*` metric, with its labels: [docs/METRICS.md](docs/METRICS.md).
The agent's `-config` YAML has a generated JSON Schema for editor/CI
validation ([docs/agent-config.schema.json](docs/agent-config.schema.json)),
and the chart ships a `values.schema.json`, so a typo'd value fails at
install time.

## How it works

* On startup the service performs a single **LIST** of all pods and then keeps
  the view current with a **WATCH** event stream (client-go shared informers).
  There is no polling and no per-request API traffic.
* Owner chains, owner labels/annotations and namespace metadata are resolved
  from **metadata-only informers** (`PartialObjectMetadata`) for ReplicaSets,
  Deployments, StatefulSets, DaemonSets, Jobs, CronJobs and Namespaces, so
  full specs of those objects are never fetched or cached. `managedFields` are stripped before objects
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

## Out of scope

Two things are **deliberately** not kubescrape's job — use the standard
component for each:

* **Host/node system metrics** (`/proc`, node_exporter territory): run a
  node_exporter DaemonSet and scrape it via `prometheus.io/*` annotations or a
  PodMonitor.
* **kube-state-metrics generation**: kubescrape does not produce KSM series —
  deploy kube-state-metrics itself and scrape it. The agent's metrics
  splitters then re-attribute its output into per-object resources; only the
  generation is out of scope.

**In scope — the systemd journal** (`-journald`). Node/system *logs* — the
kubelet, containerd and other systemd units — **are** collected, unlike the
host *metrics* above. The line is drawn at operational necessity, not at
"host vs. Kubernetes": when a node's Kubernetes plane misbehaves, those unit
logs are where you look, and on many distros they live only in the journal
(not as container logs). It costs the agent a cgo dependency on libsystemd
(hence a non-static binary on `distroless/base`), which is accepted for that
payoff; host metrics, well served by node_exporter, are not worth an
equivalent cost. That cost is also **opt-out**: the reader sits behind the
`journald` [build tag](#build-variants-optional-pipelines), which the default
build sets — drop it and the agent is cgo-free and static, at the price of the
node's unit logs.

## API

### `GET /v1/containers/{id}[?wait=2s]`

Metadata for a container by runtime ID. The ID may be bare
(`4fa6c3d0be…`) or prefixed (`containerd://4fa6c3d0be…`, `docker://…`,
`cri-o://…`), URL-escaped or not.

> An **unescaped** prefixed ID contains `//`, which Go's `http.ServeMux`
> collapses — the request is answered with a `307` to the cleaned path rather
> than directly. Redirect-following clients (and `metaclient`, which escapes
> the ID) never notice; `curl` needs `-L`, or strip the prefix
> (`${cid#containerd://}`) / escape it (`containerd%3A%2F%2F…`).

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
watches (ReplicaSets, Deployments, StatefulSets, DaemonSets, Jobs, CronJobs);
`namespaceMetadata` holds the labels and annotations of the pod's namespace.
StatefulSets and DaemonSets own their pods directly, so their labels land on
the pod's single owner entry.

`pod.ready` mirrors the PodReady condition (a Running pod may be failing
every probe). Pods marked for deletion carry `pod.deletionTimestamp` while
they drain — their phase stays `Running` for the whole grace period — and
pods served from the tombstone cache additionally carry `pod.deletedAt`.

### `GET /v1/nodes/{node}/targets`

Prometheus scrape targets for all live pods scheduled on `node`. Pods that
are finished (`Succeeded`/`Failed`), deleted, or **terminating**
(`deletionTimestamp` set — draining, but still `Running` and still listed by
the API for the whole grace period) never appear, exactly as Prometheus'
endpoints discovery drops terminating endpoints; they do stay resolvable
through the metadata endpoints, so their last logs remain attributable.
Targets come from four sources:

* **pod annotations** — the conventional annotations on the pod itself,
* **service annotations** — the same annotations on any Service whose
  selector matches the pod; service ports are translated to pod ports via
  their `targetPort` (named container port, explicit number, or the service
  port itself),
* **ServiceMonitors** (opt-in, `-servicemonitors`) — Prometheus-Operator
  `monitoring.coreos.com/v1` ServiceMonitor resources whose selector matches
  a Service backing the pod. Endpoint `port` (service port name),
  `targetPort` (pod port number or container-port name), `path` and `scheme`
  are honored, and so are `tlsConfig.insecureSkipVerify`,
  `basicAuth`/`authorization`/`bearerTokenSecret` and secret-backed
  `tlsConfig` `ca`/`cert`/`keySecret`/`serverName` (see `/v1/scrape-auth`
  below), per-endpoint `interval`/`scrapeTimeout`, and the keep/drop subset
  of `metricRelabelings` (applied per sample by the agent; other relabel
  actions are ignored). These targets carry
  `source: "servicemonitor"` and a `monitor: "<namespace>/<name>"` field. If
  the CRD is absent at startup the feature disables itself with a warning,
  and
* **PodMonitors** (same `-servicemonitors` gate, watched when the cluster
  serves the CRD) — PodMonitor endpoints select pods directly by label and
  name **container** ports (`source: "podmonitor"`), honoring the same
  tlsConfig/bearer-token/relabeling subset as ServiceMonitors.

| Annotation             | Meaning                                                        |
|------------------------|----------------------------------------------------------------|
| `prometheus.io/scrape` | must be `"true"` for targets to be generated                   |
| `prometheus.io/port`   | comma-separated list of port numbers and/or port names (container-port names on pods, service-port names/numbers on services); if absent, every declared port becomes a target. A NAME resolves to one port: a regular container's declaration wins over an init/sidecar or ephemeral one (so a native sidecar cannot take the app container's port name), and every path — this annotation, a Service's named `targetPort`, a monitor endpoint — resolves it the same way |
| `prometheus.io/path`   | metrics path, default `/metrics`                               |
| `prometheus.io/scheme` | `http` (default) or `https`                                    |

Pods without an IP or in phase `Succeeded`/`Failed` are excluded, and
duplicate endpoints reachable through both sources are reported once (pod
source wins; a monitor-derived target upgrades an annotation one wholesale).
Two **monitors** resolving to the same URL on one pod are served as **one
merged target** honouring both endpoint declarations — relabel chains
concatenate, the finer explicit cadence wins, one-sided auth/TLS is adopted
whole — with the contributing monitors listed in an additive `monitors`
field (absent on single-monitor targets, so existing consumers decode
unchanged). Each target embeds the complete pod metadata (including owners
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
not indexed. When the claiming pod is deleted while another live pod reports
the same IP, the survivor is promoted immediately.

### `GET /v1/self`

Full metadata for the pod the **caller** runs in, attributed by the
connection's source address — the agent uses it to stamp its own pod's
Kubernetes attributes on the metrics it generates about itself (see
`-self-attributes`), without a downward-API env var wired into every
deployment that runs the binary.

The address is taken from the connection and **never** from a header
(`X-Forwarded-For` is caller-controlled, and this endpoint hands out whatever
pod owns the address it is given). Resolution goes through the same live-only
pod-IP index as `/v1/pod-ips`, so a caller on hostNetwork — sharing the node
IP — or behind an address-rewriting hop gets a `404` rather than someone
else's identity. Responses carry `Cache-Control: private, max-age=<TTL>` +
`ETag`: the answer names its caller, so a per-client cache may keep it (and
revalidate with `If-None-Match`) while a shared one must not store it at all.

### `GET /v1/nodes/{node}/metadata`

The node's labels and annotations (the agent's `startNodeInfo` provider
refreshes from this on `-node-metadata-refresh` for `.Node` attribute
templates).

### `GET /debug` (homepage)

Small HTML index of the service's debug surface, with forms for the
parameterised routes — namespace/pod for the explain endpoint, node for the
target list, container-ID/UID/IP lookups. `GET /` redirects here, so a bare
`kubectl port-forward deploy/kubescrape 8080` plus a browser is the whole
workflow. Served even when the service is not ready (that is exactly when
someone reaches for it); the agents serve their own homepage on `-listen`.

### `GET /v1/explain/{namespace}/{pod}`

**Why is this pod (not) scraped?** — the decision chain, walked for one pod
and reported verdict by verdict: whether the pod is scrapeable (and if not,
why: no IP, terminating, finished), what the `prometheus.io/*` annotations
say with an **entry-by-entry port resolution** (a numeric port no container
declares gets a caveat; a port *name* nothing declares is called out as
resolving to nothing), the declared container ports for comparison, every
Service selecting the pod with the same per-port analysis (including a
`targetPort` name that matches no container port), every
ServiceMonitor/PodMonitor endpoint's verdict, and the final target list
exactly as `/v1/nodes/{node}/targets` would serve it — dedup and merges
included. Always answers 200 with a JSON document (a missing pod is
`"found": false` plus a hint), so `curl -s .../v1/explain/team-a/api-6f9c…-x2 | jq .`
is the whole workflow. Diagnostic and read-only: no counters move.

### `GET /v1/scrape-auth/{namespace}/{name}/{key}`

One key of a Secret referenced by a ServiceMonitor/PodMonitor
endpoint's `bearerTokenSecret` — agents resolve a target's `authSecret`
reference through this before scraping. Served **only** when the service
runs with `-scrape-auth-secrets` (404 otherwise): it needs `secrets get`
RBAC and ships secret material over the cluster-internal HTTP channel, so it
is deliberately opt-in.

This is the **one authenticated route**. Callers must present the shared
token from `-scrape-auth-token-file`:

```
GET /v1/scrape-auth/monitoring/prom-token/token
Authorization: Bearer <token>
```

Anything else is a `401` (`WWW-Authenticate: Bearer`), before the request
can even probe which secrets a monitor references. The token is compared in
constant time, and `-scrape-auth-secrets` **without** `-scrape-auth-token-file`
is a startup error — the service holds cluster-wide `secrets: get`, so an
unauthenticated endpoint here would hand every referenced Secret key to
anything that can open a connection to the service. The token file is re-read
about once a minute, and after a change the previous token stays accepted for
a 5-minute grace window — rotation is just updating the Secret; the service
and the agents (which re-read on the same cadence) converge with no restarts.
Every replica just reads the same file. The rest of the API stays
unauthenticated: it carries no secret material and agents poll it constantly.

### `GET /healthz`, `GET /readyz`

Liveness is always `200`; readiness turns `200` once the initial informer
cache sync has completed. The service's own metrics (`kubescrape_store_pods`,
`kubescrape_store_containers`, `kubescrape_http_requests_total{pattern,code}`,
…) are produced through the same internal
metrics machinery as everything else and **pushed over OTLP**
(`-self-metrics-interval`, default 1m; 0 disables) under a resource carrying
this pod's own Kubernetes attributes (`-self-attributes`, default on:
namespace, pod, uid, owner chain, labels — re-read from its own store,
since the pod name is the hostname and the namespace comes from the
ServiceAccount projection it already mounts, so no API call and no downward-API
env var is involved; `service.name`/`service.instance.id` are never
overwritten, `service.namespace` is newly derived) — together with the Go
cluster telemetry. The process's own Go runtime and process metrics
(`go_*`, `process_*`) are served as Prometheus text on the dedicated
`-metrics-listen` port instead — and with `-self-metrics-interval=0` that
port also serves the `kubescrape_*` internal metrics, replacing the OTLP
push with a scrape (one knob selects the modality, so the same series never
ship twice). There is no other Prometheus
format, for debugging the process itself.

## Reusable packages

Most of the code is `internal/`, but the pieces that stand on their own are
importable:

| Package | What it is |
|---|---|
| [`pkg/promparse`](pkg/promparse) | Streaming parser for the Prometheus text exposition format (classic + OpenMetrics). Never buffers more than a line, so a 100k-series endpoint parses in constant memory; pooled parsers keep the intern tables warm (a large scrape costs a handful of allocations). |
| [`pkg/kubemeta`](pkg/kubemeta) | The metadata model the service serves — the wire contract for its API — plus `NormalizeContainerID`. Pure stdlib; the Kubernetes-object conversion lives in [`pkg/kubemeta/kubeconvert`](pkg/kubemeta/kubeconvert) so clients don't compile `k8s.io/api`. |
| [`pkg/metaclient`](pkg/metaclient) | HTTP client for that API: blocking container-ID lookups, ETag/Cache-Control caching, no metrics dependency (set `Observe` to feed your own). |
| [`pkg/logattrs`](pkg/logattrs) | Lifts configured keys out of a JSON or logfmt log line onto an OTLP record, as resource, scope or log attributes. |
| [`pkg/otlpsplit`](pkg/otlpsplit) | Splits an over-cap OTLP payload (logs/metrics/traces) into parts each under a byte limit, preserving resource/scope grouping — the guarantee against wholesale gRPC-limit rejections. |
| [`pkg/cgroupid`](pkg/cgroupid) | Parses cgroup paths (cgroupfs and systemd layouts) into pod UID / container ID identity — the routing key for kubelet cadvisor series. |

They carry no `internal/` dependencies, so they are usable outside this module.

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
| `-servicemonitors` | `false` | serve targets for ServiceMonitor CRDs — plus PodMonitors when the cluster serves them (see above) |
| `-scrape-auth-secrets` | `false` | serve the Secret keys monitor endpoints reference (`bearerTokenSecret`, `basicAuth`, `authorization.credentials`, `tlsConfig` ca/cert/keySecret) on `/v1/scrape-auth`; only keys some monitor actually names are served (requires `secrets get` RBAC **and** `-scrape-auth-token-file`) |
| `-scrape-auth-token-file` | — | file holding the shared bearer token callers must present on `/v1/scrape-auth` (`Authorization: Bearer <token>`); mandatory with `-scrape-auth-secrets`; re-read per minute with a 5-minute grace for the previous token, so rotation needs no restarts |

The service's own metrics are pushed over OTLP (`-self-metrics-interval`);
the connection uses the agent's exporter flags: `-otlp-endpoint`,
`-otlp-protocol`, `-otlp-compression`, `-otlp-compression-level`,
`-otlp-insecure`, `-otlp-tls-ca-file`, `-otlp-tls-insecure-skip-verify`,
`-otlp-bearer-token-file`, `-otlp-timeout`.

The service can run **multiple replicas**: every replica serves reads from
its own informer caches, so no coordination between replicas is needed.

In-cluster it needs `get`/`list`/`watch` on `pods`, `services`, `namespaces`,
`nodes`, `replicasets.apps`, `deployments.apps`, `statefulsets.apps`,
`daemonsets.apps`, `jobs.batch`,
`cronjobs.batch` and (optionally) `servicemonitors.monitoring.coreos.com`
(plus `podmonitors` when that CRD should be discovered)
cluster-wide, and `secrets get` for `-scrape-auth-secrets` (commented out
in the manifests — enable deliberately) — see
[deploy/kubernetes.yaml](deploy/kubernetes.yaml).

Every listed resource is watched at startup and readiness waits for all of
those caches to sync, so a hand-maintained ClusterRole must be updated
**with** (or before) the image: a missing rule leaves `/readyz` failing
rather than degrading quietly. `statefulsets.apps` and `daemonsets.apps`
are the most recent additions.

`make image` builds a container image from the [Dockerfile](Dockerfile);
`make test` and `make vet` run the test suite and static checks. `make check`
runs the whole pre-merge story in one command — formatting, vet, lint, chart
lint (bootstrapping `helm` into `hack/bin` if needed), tests and the
build-tag guard — and `make e2e` runs the kind-based end-to-end smoke test
([hack/e2e.sh](hack/e2e.sh)): build and load the image, deploy the shipped
manifests plus a debug collector, and assert readiness, target discovery, a
container-ID lookup and telemetry arriving at the collector.

### Build variants (optional pipelines)

Two **agent** pipelines are behind Go build tags, and the Makefile carries the
default set — so `make build`, `make test` and `make image` produce exactly the
binaries and image they always have:

```sh
TAGS ?= journald,azure
```

| Build | Contains | Costs / saves |
|-------|----------|---------------|
| `make build` | both | today's binaries: agent is `CGO_ENABLED=1`, dynamically linked |
| `make build TAGS=azure` | Azure only | **no journald ⇒ no cgo**: the agent links statically and needs no libsystemd |
| `make build TAGS=journald` | journald only | **no franz-go**: 11 packages, ≈5 MB off the stripped binary |
| `make build TAGS=` | neither | both of the above |

`journald` is the only reason the agent needs cgo (it links libsystemd through
`coreos/go-systemd/sdjournal`); without that tag both binaries are static, which
is what [Dockerfile.static](Dockerfile.static) / `make image-static` uses to put
them on `distroless/static` instead of `distroless/base` plus seven copied `.so`
files. `azure` is the Event Hubs (Kafka) consumer, which only ever runs in the
single-replica Deployment yet ships in every DaemonSet image. `make verify-tags`
asserts both exclusions actually happen.

> **A bare `go build ./cmd/kubescrape-agent/` passes no tags and therefore
> builds an agent with NEITHER pipeline.** That is the price of a default that
> lives in the Makefile rather than in the source; build through `make`, or pass
> `-tags` yourself. Such a binary still *defines* `-journald` and
> `-azure-diagnostics` — the manifests pass them, and a missing flag would be
> `flag provided but not defined` + exit 2 — but enabling one is a startup error
> naming the tag, which `-check-config` reports too. Every build says which one
> it is on its first log line (`optionalPipelines=journald,azure`, or `(none)`).
> The `-config` file is unaffected: no section belongs to either pipeline, so one
> ConfigMap stays valid for every variant.

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
never read loses that segment, as with any tailer. In-place truncation is
different in kind: the writer destroys the unread tail, so that loss is
inherently unmeasurable — unlike every loss the agent itself decides on,
which is counted in a metric.) It exports
OTLP log records with resource attributes (`k8s.pod.name`,
`k8s.deployment.name`, `container.id`, pod/namespace labels, …) resolved via
`GET /v1/containers/{id}` — the blocking wait covers containers whose
metadata has not reached the API server yet. (A file whose metadata will not
resolve is retried with an exponential, jittered backoff — 2s doubling to 1m —
so one unresolvable file cannot monopolise the single sweep goroutine and a
metadata-service rollout does not recover into a synchronised fleet-wide
burst.) Delivery is at-least-once:
batches (`-logs-batch-size` / `-logs-flush-interval`) are retried, file
offsets are committed only after a successful export, and committed offsets
are checkpointed to disk (`-positions-file`) so restarts resume where they
left off. The one exception is a batch the collector rejects **definitively**
(a malformed payload, an unimplemented endpoint, a body over the receiver's
limit): retrying that cannot succeed, and because one goroutine sweeps every
file on the node, retrying it forever would stop all log shipping there — so
the batch is dropped and the offsets advance. That is real loss, and it is
counted in `kubescrape_log_permanent_dropped_total` (plus an `ERROR` log line):
**alert on any nonzero rate of it.** That counter counts RECORDS, so it also
says how much was lost. With `-buffer-dir` the tailer sees the enqueue verdict
rather than the collector's, so there the same loss surfaces as
`kubescrape_buffer_dropped_batches_total{signal="logs"}` — alert on that one —
with `kubescrape_buffer_dropped_records_total{signal="logs"}` beside it for the
magnitude (a batch is 1..1024 records; only the batch counter existed before,
so the size of the loss on the recommended durable path was unknowable).
Total backlog is visible as `kubescrape_log_lag_bytes`, the largest single
file's as `kubescrape_log_lag_max_bytes`, and per file
on `GET /debug/tailer`; a per-file line **rate limit** (`-logs-rate-limit`,
pause or drop) keeps one runaway pod from consuming the pipeline. Set
`-logs-exclude-namespaces` to the observability namespace to avoid feeding
the collector its own output. (The Helm chart does this for you: with
`agent.logsExcludeNamespaces` left at its `null` default it excludes the
release's own namespace plus, when an in-cluster Service is named as a LOGS
destination — `agent.otlp.endpoint`, or `agent.config.export.logs.endpoint` —
that Service's namespace. Only a logs destination counts: excluding a metrics
or traces endpoint's namespace would drop logs it never caused. An explicit
`[]` means exclude nothing.)

**Unified config file** (`-config`). All of the agent's YAML configuration
lives in one file, passed with `-config`. Every section is optional, each
described below and each mirroring the shape of the standalone file it
replaces:

```yaml
resourceAttributes: {...}   # how exported resource attributes are built
logs:          {sources: [...], rules: [...]}   # what to tail; drop/keep/sample
logAttributes: {rules: [...]}     # lift line keys onto attributes
logMetrics:    {metrics: [...]}   # metrics derived from log lines
logScrubbing:  {builtin: [...], rules: [...]}   # redact secrets/PII from bodies
metrics:       {pipelines: {...}, splitters: [...]}   # scraped-series rules
traceMetrics:  {dimensions: [...], buckets: [...], staleAfter: 15m}  # span-derived RED metrics (trace tier)
traceSampling: {probability: 0.1, ...}          # sample spans by trace ID (trace tier)
tailSampling:  {policies: [...], decisionWait: 5s}   # decide whole traces (trace tier)
serviceGraph:  {wait: 10s, dimensions: [...]}   # edge pairing and its bounds (trace tier)
serviceGraphShards: {endpoints: [...], self: ...}   # the ring, in its richer form (trace tier)
routing:       {routes: [...]}    # per-namespace fan-out / tenancy
export:        {headers: {...}, logs: {...}, metrics: {...}, traces: {...}}  # per-signal OTLP destinations, buffered-chain headers, mTLS
```

(Starlark transforms are the one exception: they live in their **own** file,
`-transforms-file`, so they can hot-reload independently — see below.)

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

**Per-workload log config** (pod annotation `kubescrape.io/logs`). A workload
can declare its own log handling — no agent config change, no restart — with
one JSON object in a pod annotation: `exclude` (opt the pod out of
collection entirely), `multiline` (override stack-trace joining for this
pod), `serviceName` (override the derived `service.name`), `attributes`
(extra resource attributes, overwriting — the workload is authoritative
about itself, with one carve-out: keys naming resolved Kubernetes identity
(namespace, pod, container, node) are refused and counted in
`kubescrape_log_pod_attrs_refused_total{key}`, because `k8s.namespace.name`
is the routing key and honouring it would let any pod redirect its logs into
another tenant), and `rules` (keep/drop/sample rules, same shape as
`logs.rules`, evaluated **before** the global chain: a pod-rule drop is
final, a pod-rule keep still passes through the global rules):

```yaml
metadata:
  annotations:
    kubescrape.io/logs: |
      {"multiline": true, "serviceName": "checkout",
       "attributes": {"team": "payments"},
       "rules": [{"action": "drop", "matchRegexp": ["level=debug"]}]}
```

The annotation arrives for free through the metadata resolution every
container log file already performs and is parsed once per file. A
malformed annotation must not lose logs: it is warned about and ignored.

**PII scrubbing** (`logScrubbing` section). Redacts sensitive values from log
bodies **on the agent**, before anything downstream copies from them
(enrichment, `logAttributes`, log metrics — and before export), so secrets
never leave the node. `builtin` enables named patterns — `defaults` expands
to the low-false-positive set (`bearer`, `basic-auth`, `secret-kv`,
`aws-key`, `private-key`, `url-userinfo`); `email` and `credit-card` are opt-in by name —
and `rules` adds user regexes (`name`, `regexp`, `replacement`, `$1`-style
group references; the default replacement is `[REDACTED]`):

```yaml
logScrubbing:
  builtin: [defaults, email]
  rules:
    - name: session-id
      regexp: 'session=[0-9a-f]{32}'
      replacement: 'session=[REDACTED]'
```

It applies to every log-producing pipeline alike — the tailer, journald,
Kubernetes events, Azure diagnostics and OTLP-ingested logs. Every
built-in pattern carries a cheap prefilter, so the no-match hot path costs a
scan or two and zero allocations. The key-value pattern's prefilter checks the
whole `keyword[suffix]["]:=` SHAPE rather than the bare keyword: admitting a
line the regex cannot match costs a full regex pass over the record, which for
a 1 MiB line was 100 ms of the single goroutine that tails every file on the
node. Redactions count into
`kubescrape_log_scrubbed_total{pattern}`; an unknown builtin name or invalid
regex fails startup (a scrubber that silently skips a pattern is a
compliance bug).

**Readiness** (`GET /readyz`). Liveness (`/healthz`) is always `200`;
readiness reports whether the agent can actually do its job and lists the
pending gates in the body (`not ready: metadata-service`). A DaemonSet rolling
update advances on this, so a new pod that cannot reach the metadata service —
and could therefore attribute nothing — halts the rollout instead of replacing
every node. Receiving pipelines gate on their own listeners being bound
(`otlp-ingest` for `-ingest`; the trace tier has both `service-graph-ingest`
for the application ports and `service-graph-receiver` for the internal
listener), since
a rollout that advanced past an unbound receiver would leave the applications
pushing into a void on every node it had already replaced.

**Config validation** (`-check-config`). Compiles every section of `-config`
and `-transforms-file` (templates, regexes, selectors, globs, durations) plus
the flags, prints a summary and exits — without opening listeners, log files,
the positions file, spools or the network. Run it in CI: a DaemonSet's bad
ConfigMap otherwise surfaces as a fleet-wide CrashLoop. A real start runs the
same validation, so the two cannot disagree.

Flag **values** are part of that. A value you passed that the process cannot
honour is an error naming the flag, the value and a usable one — a non-positive
`-scrape-timeout` (it *is* each request's context budget, so it would fail every
target and both kubelet scrapes with `context deadline exceeded`), or a
`-logs-rate-burst` below the one whole token a line costs (pause mode would stop
reading every file on the node). Both are refused rather than quietly replaced
by a working default, because a replacement is discovered mid-rollout if at all.
Only what you *passed*: a flag you left alone is always its default, and a bound
arriving programmatically still gets the constructor's safe substitute.

**Config unit tests** (`-test-config tests.yaml`). Goes one step further than
`-check-config`: named sample lines run through the compiled pipeline in the
tailer's order (scrub → logAttributes → enrich → logMetrics → `logs.rules` →
transforms) and assertions check the outcome — kept/dropped, the post-scrub
body, the enriched severity, lifted attributes, and which log-metrics
observed the line:

```yaml
tests:
  - name: health checks dropped
    line: 'GET /healthz 200 OK'
    expect: {kept: false}
  - name: secrets scrubbed, errors counted
    line: 'level=error auth="Bearer abc123"'
    expect:
      severity: error
      body: 'level=error auth="Bearer [REDACTED]"'
      metrics: [errors_total]
```

Exit status is non-zero on any failure, so a too-greedy drop or scrub regex
is caught in CI instead of by missing production logs. Like `-check-config`,
nothing is acquired.

**Log enrichment** (`-enrich`, default true — one switch covering every
log-producing pipeline: container logs, journald, Kubernetes events, Azure
diagnostics and pushed OTLP log bodies). Each exported line is run
through [JohanLindvall/enrich](https://github.com/JohanLindvall/enrich),
which recognizes JSON (Serilog/Pino/Envoy/Azure envelopes and common key
spellings), logfmt, and a table of plain-text formats (nginx, klog, redis,
syslog prefixes, Go/Java/Python/.NET stack traces). Whatever the line itself
carries is promoted into the OTLP record — a parsed timestamp replaces the
CRI write time when it carries a zone (a zone-less one is an ambiguous wall
clock, so the accurate ingest time is kept and
`kubescrape_log_enrich_time_rejected_total` counts it), an explicit level
sets the severity, trace/span IDs land in
the first-class trace fields (GUID-style request IDs included), and template
/ source-context / service / exception details become record attributes
(`log.template`, `log.source_context`, `exception.type`, …). The body is
never modified, and lines without recognizable metadata are exported
unchanged. Stack traces recognized in plain text are *not* duplicated into
`exception.stacktrace` — they already are the body; JSON-carried traces are,
since there the body is the raw JSON. Hit rates per strategy are exported as
`kubescrape_log_enriched_total{format="json|logfmt|pattern|none"}` on the
agent's self-metrics (pushed over OTLP).

The full set it can stamp is `log.template`, `log.template_hash`,
`log.source_context`, `log.service`, `log.service_version`, `log.product`,
`exception.type`, `exception.message`, `exception.stacktrace`, and — for lines
carrying an Azure resource ID — `cloud.resource_id`,
`azure.resource_group.id` (the resource group's own full ARM ID, *not* its
bare name) and `azure.event_category`. Only `log.iostream`, `log.file.name`,
`cloud.resource_id` and the three `exception.*` keys are OpenTelemetry
semantic-convention attributes; the rest have no registry counterpart as of
semconv v1.44.0 and are this project's own. On the Azure diagnostics pipeline
`cloud.resource_id` and `azure.resource_group.id` are dropped from the record
again, because there the *resource* already states the identity authoritatively
— see [Azure diagnostics](docs/CONFIGURATION.md#agent-azure-diagnostics).

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
capped at `maxCardinality` unique label combinations (hard cap 10000). A
histogram is one stored sample per label combination, carrying its whole
per-bucket distribution — but each bucket is still a slot in that sample and a
series in every export, so a configuration whose `maxCardinality` x buckets
would exceed 150000 bucket slots is rejected at startup.

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
      action: inc                     # set (default)|inc|dec|add|sub|min|max|avg|sum|count
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
  in a window: `min`, `max`, `avg`, `sum`, `count` (matching lines). An aggregation
  emits its value on every export and keeps emitting it while no new value
  arrives; the first value after an export starts a fresh window (so `avg` is a
  per-scrape-window mean, like the old avg-gauge). Derived statistics (stddev,
  range, …) are backend recording-rule territory.

Only these configured metrics are exported (no internal bookkeeping series).
The export interval, chunk size and an optional name prefix are runtime flags:
`-logs-metrics-interval` (default 30s), `-logs-metrics-max-bytes` and
`-logs-metrics-name-prefix`.

**Positions.** `-positions-file` persists BOTH log offsets and the journald
cursor in a single JSON file (one thing to mount), so a restart resumes every
input from one place (without it, offsets reset per `-logs-unknown-files` and
journald begins at the tail each start).

**Disk buffer** (`-buffer-dir`, opt-in). By default the agent's durability is
checkpoint-and-rewind: on a collector outage the tailer stops advancing and the
source files *are* the buffer — simple, but a long outage risks loss if those
files rotate away, and scraped metrics are just dropped and re-scraped. Point
`-buffer-dir` at a (node-local, persistent) directory and every export instead
goes through a **disk-backed write-ahead buffer** — one durable FIFO queue per
signal, backed by
[JohanLindvall/diskqueue](https://github.com/JohanLindvall/diskqueue). A batch
is serialized, `fsync`'d to disk, and acknowledged to the producer immediately
(so the tailer commits its offsets and the source logs may rotate away), then
a background sender drains the queue to the collector with retries; a batch is
removed only after the collector accepts it. Delivery stays at-least-once and
**survives agent restarts** (per-record checksums; a crash-torn tail costs at
most the torn record, and every corruption loss is reported, never silent).
The undelivered backlog is bounded per signal by `-buffer-max-bytes` (default
1 GiB); when full, enqueues fail and the tailer back-pressures by rewinding,
so disk use stays capped. A latched I/O failure (a failed fsync is not
retriable) is recovered by an automatic close-and-reopen; the affected batch
redelivers. This is the Fluent-Bit-style `filesystem` buffer: it
absorbs outages up to the cap instead of pinning to source files.

It buffers logs and metrics. **Traces pass through it**, deliberately: a
forwarded trace is still held by the application that pushed it, and its SDK's
retry is a better durability story than a queue that would ack that sender and
remove the only other copy. The single exception is a **tail-sampling decision**
on the trace tier — those spans were acked when they were *buffered* for the
decision window, so by the time the verdict ships nobody else holds them. Such a
payload marks itself as owned and is spooled like any log batch; a third queue
(`traces/`) is opened for it, and only where tail sampling actually runs.

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
true), so dead endpoints are visible exactly like with Prometheus. The
last cycle's per-target outcomes (up/error/duration/samples, failures
first) are also served on `GET /debug/targets`. Targets derived from
monitor endpoints may carry `insecureSkipVerify`, a bearer-token secret
reference (resolved through `GET /v1/scrape-auth/...`, which requires
`-scrape-auth-secrets` on the service) and keep/drop `metricRelabelings`
(applied per sample) — all honored by the agent.

**Native histograms** (`-scrape-native-histograms`, opt-in). The agent
offers the Prometheus **protobuf exposition** to annotation- and
monitor-discovered targets (the only format that carries native
histograms) and converts native histograms to OTLP **exponential
histogram** points; classic series in a protobuf response convert exactly
as in text. A family carrying both native and classic data uses the native
representation; custom-bucket histograms (schema −53) fall back to classic
buckets. Targets that ignore the Accept header keep serving text (the parse
mode always follows the response Content-Type). Splitter-backed targets
(kube-state-metrics style) take the protobuf path too — native points route
through the same groupBy/enrichment machinery as every other kind.

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

**High-frequency cgroup sampling** (opt-in, `-cgroup-stats`). cadvisor is
scraped once per `-scrape-interval`, so a container that spikes to 4 cores for
two seconds inside a 60-second window is reported as roughly 0.13 cores — the
average hides the burst that mattered. With `-cgroup-stats` the agent reads
each container's `cpu.stat`, `memory.current` and `memory.stat` **directly,
once a second**, and exports the *distribution* of each scrape window as ten
extra gauges beside the cadvisor series they annotate:
`container_cpu_usage_stddev` / `_max` / `_min` / `_mean` / `_samples` (in
**cores**, from the rate) and `container_memory_working_set_bytes_stddev` /
`_max` / `_min` / `_mean` / `_samples`. `_samples` is that window's reading
count: below 2 it marks the four statistics beside it as the previous window's,
re-stated. Measured on
a real burst workload, the sampler reported a max of 2.000 cores where
cadvisor's 30-second average read 0.394 — a 5.1x gap — and on memory it
bracketed a known 192 MiB allocation to +0.26% while cadvisor's single sample
per scrape missed the peak in two windows out of three. At 200 containers the
sampling costs **0.48% of one core (0.43–0.51%) and about +5.5 MiB of RSS
(+4.7 to +6.3)**, measured as the same binary over the same hierarchy with the
sampler off — size a DaemonSet with room above that range rather than to it.

The gauges carry the **same resource attributes as the cadvisor series for
the same container**, so they join in a query — they are built by the
`cadvisor` resource-attribute pipeline for exactly that reason, and tuning that
pipeline moves both. A container the metadata
service cannot resolve is not exported at all, which is also what keeps the
pod sandbox out. Requires the host's `/sys/fs/cgroup` mounted read-only — the
chart does it behind `agent.cgroupStats.enabled`, and
[deploy/agent.yaml](deploy/agent.yaml) carries the flag, mount and volume
commented out together. cgroup **v2 only**: a v1 node disables this pipeline
alone and logs an error, leaving every other pipeline running. See
[docs/CONFIGURATION.md](docs/CONFIGURATION.md#agent-high-frequency-cgroup-sampling).

**journald** (opt-in, `-journald`). The agent reads the systemd journal
natively through libsystemd (`coreos/go-systemd/sdjournal`) and exports the
entries as OTLP log records, one resource per unit (`service.name` = unit
without `.service`, `systemd.unit`, plus the configured node attributes; syslog
priorities map to OTLP severities). Records carry the instrumentation scope
`github.com/JohanLindvall/kubescrape/agent/journald` — earlier releases shipped
journal records with an empty scope name, so backends that label by it
(`otel_scope_name` in Loki/Elasticsearch) split every journal stream at the
upgrade boundary; group on the unit instead if you need continuity across it.
Because it links libsystemd, the **agent
binary is built with cgo** (the metadata service stays fully static) and the
image ships libsystemd — no `journalctl` binary or subprocess. This pipeline is
the *only* reason for either, so it is behind the `journald`
[build tag](#build-variants-optional-pipelines): `make build TAGS=azure` (or
`make image-static`) leaves it out and gives you a fully static agent on
`distroless/static`, and `-journald` on such a binary then refuses to start
rather than silently collecting nothing. Delivery is
at-least-once: the cursor of the newest exported entry is persisted (via
`-positions-file`) only after a successful export. A *reader* error restarts
from the committed cursor with backoff; an *export* failure retries the same
batch in place and never re-reads it, so a collector outage cannot multiply the
log-metric and rules counters by the number of retries it spans (the attempts
themselves are `kubescrape_journal_export_failures_total`).
`-journald-units` restricts to specific units and `-journald-dir` reads a
non-default journal directory. The host journal must be **mounted into the
container** (`/var/log/journal` and/or `/run/log/journal`) — the chart does
this behind `agent.journald.enabled`; without it the reader starts, reports
ready and collects nothing, which the agent now warns about at startup. `-enrich`
(default true) applies the same per-line enrichment here as to container logs; an
explicit level found in the message wins over the journal priority.

**Kubernetes events** (opt-in `-events`). Events are a **cluster-wide**
stream, not a per-node one, so this pipeline runs the agent binary as its own
single-replica Deployment ([deploy/events.yaml](deploy/events.yaml), or
`events.enabled=true` in the chart) with every per-node pipeline switched off
— deliberately **not** in the DaemonSet, which would put cluster-wide `events`
read plus lease/configmap write credentials on every node and cost one lease
poll per node per retry period. Exactly one replica reads at a time
(`coordination.k8s.io` **Lease** election, `-events-lease`), so a rolling
update or a `>1` replica count never double-ships. Each event becomes an OTLP
log record whose body is the message, with `k8s.event.reason`,
`k8s.event.action`, `k8s.event.type`, `k8s.event.count`, `k8s.event.name`,
`k8s.event.involved_object.*` and `k8s.event.reporting_component`/`_instance`
as record attributes, and whose **resource is the
involved object's own** — an event about a pod is resolved through the
metadata service (`GET /v1/pods/{ns}/{name}`, UID cross-checked so a recycled
name cannot mis-attribute) and carries the same `k8s.*`/`service.*` attributes
as that pod's container logs, which is what makes events and logs line up in a
query. Other kinds get `k8s.namespace.name` plus the kind's own name
attribute. Aggregated events (the API server's `count`/`lastTimestamp`
rollup) arrive as MODIFIED and are exported as fresh occurrences.

Delivery is at-least-once, with the same discipline as every other pipeline:
the position (the watch `resourceVersion` plus a timestamp watermark) is
persisted only **after** the collector acks, and it lives in a **ConfigMap**
(`-events-position-configmap`) rather than a node-local file, because the
leader moves — the successor of a killed pod resumes exactly where its
predecessor stopped. A written position is only ever a *lower bound* on what
was delivered, so a zombie writer can at worst cause a replay, never a gap. If
the API server's watch window has passed (`410 Gone`), the reader re-lists and
the watermark suppresses the events it already shipped. `-events-start`
chooses the cold-start behaviour when no position exists at all
(`auto`/`end`/`start`). The full agent chain applies: `logScrubbing`,
`logAttributes`, `-enrich`, `logMetrics`, `logs.rules`, Starlark transforms,
routing and the disk buffer.

**Azure diagnostics** (opt-in `-azure-diagnostics`). Azure resources stream
their diagnostic-settings output — resource **logs** and platform **metrics**
— to an Event Hubs namespace; the agent consumes it over the namespace's
Kafka endpoint (franz-go) and exports OTLP. It is cluster-scoped like
`-events` and runs in the **same singleton Deployment** (`azure.enabled` in
the chart), but needs no leader election: the Kafka **consumer group** is the
coordination, and its committed offsets are the resume position — shared
across restarts and replicas, committed only after the collector acks
(at-least-once; poison payloads are counted and skipped). Auth is either an
Event Hubs **connection string** (SASL PLAIN, mounted secret, re-read per
connection) or **managed identity** (SASL OAUTHBEARER — AKS workload
identity when its environment is present, else IMDS; no Azure SDK, the two
token exchanges are implemented directly). A connection string may be
namespace-scoped or **entity-scoped** (`…;EntityPath=myhub`, the shape copied
from a single event hub's shared access policies) — an `EntityPath` bounds
what the credential may reach, so it also selects the hub to consume, in
place of the `^insights-.*` pattern that would match nothing for it.
**Several hubs** cost one client per *credential*, not per hub: a namespace
policy (or managed identity) consumes any number over one connection, while
entity-scoped strings need one client each, so both the namespace and the
connection-string flags take lists. A namespace with several clients gives
each its own consumer group automatically, since Event Hubs' groups are
namespace-wide and a shared one lets the group leader starve members whose
hubs it cannot see. Log records keep the diagnostic
record's verbatim JSON as the body — the full chain applies (scrubbing,
`logAttributes`, `-enrich`, `logMetrics`, `logs.rules`, transforms, routing,
disk buffer) — while metric records become **real OTLP gauges**
(`azure.<metricname>.count/.total/.minimum/.maximum/.average` per timeGrain
window). Both land on the **ARM resource's own identity**:
`cloud.resource_id`, subscription, resource group, type and name, with
`service.name`/`service.namespace`/`service.instance.id` derived so Azure
resources sit beside Kubernetes workloads in the same backend. Because its
Kafka client (11 franz-go packages, ≈5 MB) would otherwise ride in every
DaemonSet image for a pipeline that only ever runs in that one Deployment, it
is behind the `azure` [build tag](#build-variants-optional-pipelines):
`make build TAGS=journald` leaves it out, and `-azure-diagnostics` on such a
binary refuses to start rather than doing nothing.

**OTLP ingest** (opt-in `-ingest`). Applications on the node can push their
own OTLP **logs and metrics** to the local agent, which enriches them with
Kubernetes attributes and forwards them — closing the gap that otherwise needs a
separate collector with the k8sattributes processor. The agent listens for
OTLP/gRPC (`-ingest-grpc-endpoint`, default `:4317`) and OTLP/HTTP protobuf
(gzip bodies accepted) (`-ingest-http-endpoint`, default `:4318`, on `/v1/logs`
and `/v1/metrics`). **Traces go to the trace tier instead** (below): pairing a
service-graph edge and sampling a trace as a unit both need every span of that
trace in one process, which a per-node receiver never has, so a sender pointed
here for traces gets an immediate Unimplemented / 404 rather than an ack for
spans that could never have become an edge.
For each pushed resource it finds a container ID (`container.id` /
`k8s.container.id`, keys configurable) or a pod UID (`k8s.pod.uid`), resolves
the metadata service (a container ID pins the exact incarnation), and merges
the k8s resource attributes **without overwriting anything the sender already
set**. Pushed log bodies additionally run the same line enrichment as the
tailer (`-enrich`, filling only fields the sender left unset) — and the rest
of the log chain reaches them too: `logScrubbing` redacts the bodies, and the
same compiled `logMetrics` and `logs.rules` that serve the tailer observe and
filter each ingested record (metrics before rules; a dropped record is still
acked, and an all-dropped payload acks without a send), so one config selects
identically however a line arrived. Line-derived processing is
sender-bounded (oversized bodies, over-wide or excess resources are skipped
and counted in `kubescrape_ingest_log_chain_skipped_total{reason}`, the data
still forwarded), and per-resource admission policy is the transforms file's
`ingest:` hook (rejections count into
`kubescrape_ingest_admission_rejected_total`).
Metrics resolve per `-ingest-metrics-mode`: `resource` (the ID is a resource
attribute), `datapoint` (the ID is a per-point label; points are split into
one resource per object, as a kube-state-metrics-style stream needs), or
`auto` (resource when every resource carries an ID, else split). With
`-ingest-peer-ip-fallback` (opt-in), a resource carrying **no** ID at all is
attributed to the pod owning the connection's source address (live,
non-hostNetwork pods only, via `GET /v1/pod-ips/{ip}`) — so unmodified SDKs get
k8s attribution with zero sender configuration. It is only correct while that
address still names the sender, which on this node-local listener it does; the
trace tier, whose senders are cluster-wide, adds a guard (below). Payloads are forwarded as
received (batch in the sender's SDK or the downstream collector); every
forward keeps the sender's own retry semantics. Enrichment
outcomes count into `kubescrape_ingest_resources_total{outcome}` (including
`peer_ip`). Concurrently-processed pushes are bounded across both transports by
`-ingest-max-in-flight` (default 32) — the listeners are unauthenticated and
node-local, and each in-flight request holds its body plus the inflated pdata on
the process that also tails the node's logs. Over the bound a sender is refused
**retryably**, never dropped: HTTP `429` with `Retry-After: 1`, gRPC
`ResourceExhausted` **with a `RetryInfo` detail** (without it, conformant
senders read the code as permanent and discard the batch). Because a body is
read (and a gRPC message decoded) *before* a slot is taken, a second bound —
64 MiB, or four times `-ingest-grpc-max-recv-bytes` when that flag lifts the
per-message cap past it — caps the raw payload bytes both transports may buffer
at once, refused the same retryable way. Refusals count into `kubescrape_ingest_rejected_total`.

**Trace sampling** (`traceSampling` section, on the trace tier). Received spans
can be sampled before export: `probability` keeps that fraction of **traces**
(the decision is a hash of the trace ID against a threshold, so it is
deterministic per trace — all spans of a trace sample identically on every
shard running the same config, and a sender's retry re-samples identically);
`keepErrors` (default true) always keeps status-ERROR spans and
`keepSlowerThan` always keeps spans at least that slow, regardless of the
probability decision; `maxSpansPerSecond` is a hard cap applied after
sampling (guard-rail keeps included — a cap that can be exceeded is not a
cap). Dropped spans count into
`kubescrape_trace_spans_dropped_total{reason="probability"|"rate"}`, and a
payload sampled down to nothing is acked without a send. The sampler sits
below the span-metrics tap and below edge pairing, so RED metrics
(`-ingest-span-metrics`) and the service graph are computed from 100% of spans
while only the sampled subset ships.

```yaml
traceSampling:
  probability: 0.1
  keepErrors: true          # default
  keepSlowerThan: 2s
  maxSpansPerSecond: 500
```

**Tail sampling** (`tailSampling` section, on the trace tier). Where
`traceSampling` judges each span from the span alone, this holds a trace's spans
for a **decision window** (`decisionWait`, 5s) and then judges the *whole trace*
against an ordered policy list — the OpenTelemetry Collector's
`tailsamplingprocessor` vocabulary (`statusCode`, `latency`, `stringAttribute`,
`numericAttribute`, `booleanAttribute`, `probabilistic`, `rateLimiting`, `and`,
`composite` — plus this repo's `script`, whose body is the transforms file's
`sample:` hook) in this repo's camelCase, **first match wins**, so the policy that
kept a trace is a metric label rather than "some subset of the list". It runs
below the head sampler and below both taps, so the graph and the RED metrics
still see 100% of spans; the two samplers **nest** rather than compound (both
hash the trace ID unsalted against the same threshold, so a 50% tail policy keeps
exactly what a head probability of 0.5 passed).

```yaml
tailSampling:
  decisionWait: 5s
  maxSpans: 200000          # the bound that sets the memory (~1 KiB/span)
  policies:
    - {name: errors, type: statusCode, statusCode: {statusCodes: [ERROR]}}
    - {name: slow, type: latency, latency: {threshold: 500ms}}
    - {name: baseline, type: probabilistic, probabilistic: {samplingPercentage: 5}}
```

**It is the one place in kubescrape that acks data it has not delivered**, and
that is worth reading twice: a pushed span must be acked *before* its trace is
decided, or the push would hold one of the receiver's in-flight slots for the
whole window and stall every sender. Buffered-but-undecided spans are therefore
**lost if the shard is hard-killed** (SIGKILL, OOM, node failure) — no sender
still holds them. The exposure is bounded by `decisionWait`, by `maxSpans`, and
by the graceful-shutdown flush (a SIGTERM or a rolling update decides everything
buffered and exports the keeps, so a normal restart loses nothing); and it is
visible before it is spent — `kubescrape_tail_sampling_buffered_spans` is exactly
what a hard kill would lose at that instant. At each memory bound the oldest
trace is **decided early** rather than evicted unjudged, counted in
`kubescrape_tail_sampling_early_decisions_total`.

Once a trace *is* decided, `-buffer-dir` makes the keep **durable**: a decided
trace is spooled to disk like logs and metrics, so a collector outage becomes a
backlog rather than `{outcome="lost"}`. That is an exception to the rule that
traces pass through the disk buffer unbuffered, and only this path gets it — a
plain forwarded trace is still held by the application that pushed it, and
spooling would ack a sender whose retry was the durability. Because a hard kill
here is most likely the OOM this buffer itself causes, `maxSpans` is checked
against the pod's memory limit at startup: an unset ceiling is lowered to fit,
and one that could only OOM is refused. See
[docs/CONFIGURATION.md](docs/CONFIGURATION.md#agent-tail-sampling) for the sizing
rule and the late-span cache.

### Traces: the trace tier

**Applications send their OTLP traces to the trace tier's Service**, not to
their node's agent:

```sh
OTEL_EXPORTER_OTLP_ENDPOINT=http://kubescrape-traces.monitoring.svc:4318
OTEL_EXPORTER_OTLP_TRACES_PROTOCOL=http/protobuf
# or :4317 with the default grpc protocol
```

The tier is opt-in and is the one pipeline that needs a **workload of its own**
([charts/kubescrape/templates/servicegraph.yaml](charts/kubescrape/templates/servicegraph.yaml),
`serviceGraph.enabled=true`, or
[deploy/servicegraph.yaml](deploy/servicegraph.yaml)). It exists because of one
fact: a service-graph edge — one series per caller→callee pair — needs *both*
halves of a request, the caller's CLIENT span and the callee's SERVER span, and
those are emitted by two pods that usually run on two different nodes. A
per-node aggregator holds half of every edge by construction and no amount of
aggregation completes it. Sampling a trace as a unit has the same shape.

So a push lands on an arbitrary shard (the Service round-robins), and that shard:

1. **enriches** it with Kubernetes attributes — the one moment the connection's
   source address still names the sender;
2. **re-shards** each span by **trace ID** onto a ring and hands it to the shard
   that owns that trace (Grafana Tempo's hash, FNV-1 32-bit);
3. and on the **owning** shard pairs the edge, derives the RED metrics, applies
   head sampling and exports.

Every span is therefore enriched once, counted once and exported once, on one
shard. The tier is a **StatefulSet** because the ring addresses shards by their
stable ordinal DNS names behind a *headless* Service — a load-balanced
destination for the internal hop would round-robin a trace's two halves onto two
owners, the exact failure the ring prevents.

**Two listeners, and the difference matters.** The application ports
(`-service-graph-ingest-grpc`/`-http`, `:4317`/`:4318`, behind the ClusterIP
Service) are **unauthenticated**: every instrumented pod in the cluster is a
sender, and requiring a credential from each of them is not a bargain most
fleets can make — scope them with a NetworkPolicy
(`serviceGraph.ingest.allowFrom`) instead. The internal port
(`-service-graph-listen`, `:4319`, behind the headless Service) is
**authenticated** with a shared bearer token (`-service-graph-token-file`,
re-read periodically; the binary refuses to open it without one), because what
arrives there is treated as final — already enriched, already routed — and an
unauthenticated one would let anything that can reach the pod put unattributed
spans straight into the collector. Which port a payload arrived on is what
decides its treatment, not anything the payload claims, so loop-freedom is
structural: application pushes are re-sharded exactly once, internal hops never.
An internal hop misaddressed to an application port is refused outright
(permanently, counted in
`kubescrape_service_graph_loops_blocked_total`) rather than re-enriched and
re-sharded on every pass.

**Peer-IP attribution has a guard here.** `-ingest-peer-ip-fallback` attributes a
resource with no `container.id`/`k8s.pod.uid` by the connection's source address.
A ClusterIP normally preserves it, but a mesh that terminates the connection, an
ingress, or any NAT hop replaces it — and on this tier the replacement usually
belongs to *kubescrape*. A resolution naming this tier's own workload is
therefore **refused and counted**
(`kubescrape_ingest_resources_total{outcome="peer_ip_rejected"}`) rather than
applied: an application's traces labelled with a kubescrape pod would render
perfectly and be wrong on every span. Senders that set a resource-level
`container.id` (every SDK's container detector does) are unaffected.

**The internal hop cannot shed.** The shard that received a push holds the only
copy of those spans, so a failed hop **fails the application's push** and the
sender's retry is the recovery. A partial fan-out that fails part-way delivers
at-least-once: the split is a pure function of the trace ID, so the retry
re-splits identically and the owners that already accepted see a duplicate — safe
because both taps count only after a *successful* export.

The emitted series are Grafana **Service Graph** compatible on purpose — that
view queries these exact names, so a better-reading name would render in
nothing: `traces_service_graph_request_total`,
`traces_service_graph_request_failed_total` and the two separate histograms
`traces_service_graph_request_server_seconds` /
`_request_client_seconds` (separate because the two sides are measured by
different processes with unsynchronised clocks; each carries **exemplars**,
one per latency bucket, pointing at the trace and at the span that measured
*that* side — `serviceGraph.exemplars: false` turns them off). Labels are `client`, `server`,
`connection_type` (empty, `messaging_system`, `database` or `virtual_node`),
`virtual_node` when a side was synthesized, plus any configured `dimensions`
as `client_<dim>`/`server_<dim>`. `wait` and `staleAfter` (Go durations —
`10s`, `15m`; `staleAfter: "0"` disables eviction), `maxItems` and
`maxCardinality` (config section `serviceGraph`) are the bounds that trade
completeness for memory; each has a counter that moves when it binds, because
a missing edge looks exactly like a call that never happened. **The limitation
is structural**: an uninstrumented callee emits no server span, so its calls
appear only as **virtual nodes** named from the client span's `peer.service` /
`db.name` / `db.system` (symmetrically, an expired server half carrying one of
those names its uninstrumented *caller*) — and a party naming none of them
yields no edge at all. See
[docs/CONFIGURATION.md](docs/CONFIGURATION.md#agent-service-graph).

**What this topology costs.** The tier is a hard, cluster-wide dependency for
traces: while it is down or unreachable every application's trace export fails,
and there is no per-node fallback. Each span crosses the network twice at full
fidelity — application → entry shard, then entry → owner for (N−1)/N of them —
which is real intra-cluster bandwidth. And the tier is shared: one noisy
service's spans occupy the same shards as everyone else's, bounded only by
`-ingest-max-in-flight` per pod. Those are the price of holding a whole trace in
one process, which is what an edge and a sampled trace both require.

**Pipeline toggles.** Each pipeline is individually switchable: `-logs`,
`-metrics` (annotation-discovered targets), `-cadvisor` and `-node-metrics`
(all default true; the kubelet scrapes additionally require
`-kubelet-endpoint`), plus the opt-in `-journald`, `-ingest`, `-events`,
`-azure-diagnostics` and `-service-graph` (the last is the trace tier's own
StatefulSet, not the DaemonSet).

**Self-observability.** The agent's own metrics — log entries/bytes/rotations
and export failures, enrichment hit rates per format, scrapes and scrape
duration/samples per pipeline, exports per signal and outcome, metadata
lookups, journal entries and reader restarts, events observed/exported and
watch restarts — are produced through the same
internal metrics machinery as everything else and **pushed over OTLP** on
`-self-metrics-interval` (default 1m, 0 disables) with the agent's own
resource identity (`service.name: kubescrape-agent`, `k8s.node.name`).

That identity is completed with the agent's **own pod's** Kubernetes
attributes (`-self-attributes`, default on): namespace, pod name and UID, pod
IP, the owner chain, pod and namespace labels, plus whatever the
`resourceAttributes` section's `self` pipeline adds (a `static` cluster name,
for instance) — resolved from the metadata service's `GET /v1/self`. The same
applies to the trace tier's span metrics and service-graph edges, which carry the same
identity. Attributes the agent already set win, so `service.name` stays
`kubescrape-agent` and `service.instance.id` stays the node (stable across
restarts, unlike a pod UID); `service.namespace` is newly derived from the
pod's namespace, which makes the agent's own job in a Prometheus backend
`<namespace>/kubescrape-agent`. The `self` pipeline can therefore only ADD
attributes — a template setting `service.name` there is deliberately
ineffective.

The pod is re-read on `-self-attributes-refresh` (default 1m), so a pod or
namespace **relabelled after startup** reaches the metrics that stamp it. That
poll is nearly free: `/v1/self` answers with `private, max-age` + `ETag`, so
the client serves a fresh entry locally and revalidates a stale one with
`If-None-Match` — a 304 whenever nothing changed. `private` is what makes
caching a caller-dependent response safe: one client belongs to one process,
which is the one pod the answer describes, and a shared cache is told not to
store it at all. Until the lookup first succeeds — or forever, for a caller the
service cannot attribute — the metrics ship with the bare identity: they are
how a metadata-service outage is diagnosed, so nothing waits on it.
`kubescrape_self_metadata_resolved` reports which of those you are in (it is
published exactly when the lookup runs, so `0` always means unresolved), and
`kubescrape_self_metadata_lookups_total{outcome}` counts the attempts —
`self` for the peer-address answer, `by_name` for the fallback below.

Where the peer address cannot identify the caller — a **hostNetwork** agent
shares the node IP, a NAT hop or proxy replaces the address, a dual-stack pod
may connect from the family `status.podIP` does not carry — the agent falls
back to a lookup by name (`$POD_NAME` or the hostname, in `$POD_NAMESPACE` or
the ServiceAccount projection's namespace), the same way the metadata service
resolves itself.

Two identity attributes are deliberately NOT left to the lookup:
`service.namespace` is derived at startup from the namespace the process
already knows, so the Prometheus job cannot change mid-series when the lookup
lands; and the agent's own metrics bypass the namespace router, so a route glob
covering the agent's namespace cannot move the fleet's own health signal off
the durable buffered chain (transforms still apply to them).
`-listen` (default `:8081`) serves `GET /debug` (a homepage linking every
debug surface the process serves), `GET /healthz`, `GET /readyz`,
`GET /debug/tailer` (per-file positions and lag, plus any pod's malformed
`kubescrape.io/logs` annotation as `podConfigError`), `GET /debug/targets`
(per-target last outcomes — up/error/duration/samples, merged across cycles
so long-interval targets keep their last result and undue ones show as
pending; failures first), `GET /debug/transforms` (the active transform
program's content hash, for checking per-node convergence after a reload),
and `GET /debug/otlp` — a **live stream** of what the agent is exporting
(logs, metrics and traces, post-transform) as OTLP JSON lines, filtered by
resource attributes (`attr=key=value`, `*`/`?` wildcards on both halves,
ANDed), by signal (`signal=logs|metrics|traces`) and downsampled
(`sample=10`), with a built-in page at
`/debug/otlp/ui`; it costs one atomic load per export until a client
attaches, and a slow client drops (counted on its own stream) rather than
ever back-pressuring delivery.

**Three separate listeners.** `-listen` carries health and the `/debug`
surface; `-metrics-listen` (default `:9090`) serves the Prometheus
`GET /metrics` endpoint with this process's Go runtime and process metrics
(`go_*`, `process_*`) — plus the `kubescrape_*` internal metrics when
`-self-metrics-interval=0` disables the OTLP push; `-pprof-listen` (empty by
default) serves
`net/http/pprof`. Profiles expose goroutine stacks and heap contents, so the
port carrying them is the one to firewall or bind to localhost — which is why
it is a port of its own.

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
`.Pod.PodIP`; drop it via `resourceAttributes.disable` if unwanted).

An optional `instancePrefix` prepends `prefix-` to the derived
`service.instance.id`. It defaults to `cadvisor` for the cadvisor pipeline (and
to the describing target's `service.name` for splitter rules) so that
describing exporters — whose resources share the pod's `service.name`/namespace
— don't collide with the pod's own self-scraped `target_info`. Set it per
pipeline (or per splitter rule); `""` disables it. An explicit pipeline setting
wins over the built-in default, which wins over a top-level `instancePrefix`.

* `resourceAttributes.enable` / `.disable` — anchored regex **lists** matched
  against the full attribute key. An attribute is exported when it matches the
  enable list (empty = enable all) and does not match the disable list (empty =
  disable none). Because they are lists, a pattern may contain a comma. Both
  are global (top level only): the filter is shared by every pipeline, so a
  pipeline section setting them is rejected rather than silently ignored.

  ```yaml
  resourceAttributes:
    static: {cluster: prod, env: eu}   # fixed attributes on every resource
    disable:
      - 'k8s\.pod\.label\..*'
      - 'k8s\.namespace\.label\..*'
  ```
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
    pipelines:                # overrides for logs|targets|cadvisor|node|journal|ingest|self
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

**Starlark transforms** (`-transforms-file`, opt-in). For the cases the
declarative rules don't cover, a separate YAML file holds one optional
[Starlark](https://github.com/google/starlark-go) script per signal
(`logs:`, `metrics:`, `traces:`), each defining `transform(batch)`. The
script runs once per exported **batch** at the exporter seam, above the disk
buffer — so spooled bytes are final and a reload never re-interprets a
durable backlog. Batch elements are **lazy host objects** over the OTLP
data: log records expose `body`, `severity_text`, `severity_number`,
`attributes`, `resource`, timestamps (writable), trace/span IDs and the
scope name, plus `drop()`; spans expose `name`, `kind`, `status_code`,
`status_message`, `duration_ms`, IDs, `attributes`, `resource`, `drop()`;
metrics expose `name`, `type`, `unit`, `description`, `resource`, `drop()`
and `datapoints` — per-point attributes, scalar `value` and per-point
`drop()`, so shedding one label set doesn't cost the whole metric.
Mutations are in place, a script pays only for the fields it
touches, and dropped elements (and emptied groups) are pruned after the run:

```yaml
logs: |
  def transform(batch):
      for r in batch:
          if r.attributes["level"] == "debug":
              r.drop()
          r.resource["env"] = "prod"
```

Every item also carries two verbs beyond mutation: `route("name")` sends its
payload to a named `routing` route ahead of the namespace globs (the marker
is reserved to scripts — a copy arriving on the wire is stripped and counted
at ingest receipt), and
`emit_metric(name, value, labels)` observes into a **declared** `logMetrics`
series (an undeclared name is a script error, as is a label named `le` on a
histogram — it is generated from the histogram's own buckets). Scripts are predeclared `re`
(RE2, with a bounded compile cache) and a 1/s-throttled `log()`. The same
file can define four **hooks**, all fail-open (an error means the hook did
nothing, counted and warned): `ingest:` `admit(resource)` runs per pushed
resource before enrichment — the operator's per-sender policy on the
unauthenticated listeners; `targets:` `target(t)` runs per scrape target per
cycle (drop or rewrite paths); `sample:` `decide(trace)` is the body of tail
sampling's `type: script` policy; and `parse:` `parse(line)` pre-parses
lines of plain log sources flagged `parseScript: true` (refused on
containerd sources, whose per-line budget is allocation-pinned). See
[docs/CONFIGURATION.md](docs/CONFIGURATION.md#agent-transforms-starlark) for
the full surface and its deliberate refusals.

The file is **hot-reloaded** (fsnotify on the directory — mount its
ConfigMap as a directory, not `subPath`, or updates never arrive — with a
30s poll fallback): reloads compile-then-commit, so a broken edit keeps the
last good program running (`kubescrape_transform_reloads_total{outcome}`),
while a compile failure at startup is fatal. Starlark is hermetic (no I/O,
no imports) and each run is step-limited, so a pathological script errors
out instead of wedging an export goroutine
(`kubescrape_transform_errors_total{signal}`; the failed export retries per
the producer's usual semantics). The active program's content hash is on
`GET /debug/transforms`.

**Routing** (`routing` section). Exported payloads can fan out **per
Kubernetes namespace** to extra destinations or tenants: each route matches
`k8s.namespace.name` against glob patterns (first matching route wins) and
forwards to its own OTLP client — a different `endpoint`, extra `headers`
(e.g. `X-Scope-OrgID` for per-tenant Mimir/Loki), or both; an endpoint-less
route inherits the whole `-otlp-*` base, while a route with its own endpoint
inherits transport, headers and (unless it sets `insecure` itself) the
base's plaintext-vs-TLS choice but **never the base credentials** — those
are per-route fields. Unmatched resources use the
default chain. Payloads are split per destination; a failed destination
fails the whole export, and the producer's retry re-splits
deterministically (destinations that already succeeded receive duplicates —
standard at-least-once). Note that per-route destinations are **direct
(unbuffered) by design** — only the default chain keeps the disk buffer;
routes are for tenancy/fan-out, not for doubling the durability machinery.
Routed parts count into `kubescrape_routed_payload_parts_total{route,signal}`.

```yaml
routing:
  routes:
    - name: team-a
      namespaces: ["team-a-*"]
      headers: {X-Scope-OrgID: team-a}
    - name: audit
      namespaces: ["payments"]
      endpoint: audit-collector.security:4317
```

**Export.** `-otlp-protocol` selects gRPC or OTLP/HTTP;
`-otlp-bearer-token-file` (re-read periodically) authenticates either
transport; `-otlp-tls-ca-file`/`-otlp-tls-insecure-skip-verify` control TLS;
metric exports retry with `-otlp-retry-attempts`/`-otlp-retry-backoff`
(logs already retry through the tailer's rewind). Both binaries take
`-log-level` and `-log-format` (text/json), and the metadata service routes
client-go's klog output through the same handler.

## Helm chart

[charts/kubescrape](charts/kubescrape) deploys both components, exposing the
commonly-tuned flags as values (everything else via `service.extraArgs` /
`agent.extraArgs` — `-monitor-namespaces`, for instance, has no value of its
own), and renders the `agent.config` value verbatim into the single mounted
`-config` file:

```sh
helm install kubescrape charts/kubescrape -n monitoring -f my-values.yaml
```

Values are validated against the chart's `values.schema.json` at
install/template time, so a misspelled key is an immediate error rather than
a silently-ignored setting.

Migrating from a Grafana Alloy setup? See
[docs/MIGRATING-FROM-ALLOY.md](docs/MIGRATING-FROM-ALLOY.md). For how
kubescrape compares to Alloy/Promtail, Vector, Fluent Bit, the OTel
Collector and the Elastic Beats — features, delivery semantics and measured
performance — see
[docs/COMPARISON.md](docs/COMPARISON.md).

For a local test pipeline, `hack/otel-collector.yaml` deploys a contrib
collector with a debug exporter; the agent's own internal metrics stay small.

## Local test cluster

`make cluster-up` creates a three-node [kind](https://kind.sigs.k8s.io/)
cluster (one control plane, two workers), downloading `kind` and `kubectl`
into `hack/bin` if they are not installed. It also deploys sample workloads
([hack/test-workloads.yaml](hack/test-workloads.yaml)): annotated and
Service-fronted Deployments, a CronJob, a StatefulSet and a DaemonSet, so
scrape discovery and every owner-chain shape (ReplicaSet → Deployment,
Job → CronJob, direct owners) can be exercised:

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
