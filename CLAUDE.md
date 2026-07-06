# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```sh
make build              # build to bin/kubescrape
make test               # go test ./...
go test ./internal/store/ -run TestWaitIsPerContainer -v   # single test
make vet fmt tidy
make image              # docker build (distroless, CGO_ENABLED=0)
```

`go test -race` does not work on the current dev machine (no C compiler); rely on the concurrency tests in `internal/store`.

### End-to-end verification (kind)

Changes to informer wiring, the store, or the HTTP API should be verified against a real cluster, not just unit tests:

```sh
make cluster-up         # 3-node kind cluster; downloads kind/kubectl into hack/bin; deploys hack/test-workloads.yaml
go run ./cmd/kubescrape # uses the kind kubeconfig context
make image && hack/bin/kind load docker-image ghcr.io/johanlindvall/kubescrape:latest --name kubescrape
kubectl apply -f deploy/kubernetes.yaml   # in-cluster deployment (namespace: monitoring)
make cluster-down
```

`hack/test-workloads.yaml` intentionally exercises every code path: a pod-annotated Deployment (ReplicaSet→Deployment chain, pod-source targets), a non-annotated Deployment behind an annotated Service with a named `targetPort` (service-source targets), and a CronJob (Job→CronJob chain).

## Architecture

Two binaries: `cmd/kubescrape` (metadata service) and `cmd/kubescrape-agent` (per-node DaemonSet: log tailer + Prometheus scraper exporting OTLP). Both live in one image; the DaemonSet overrides `command: ["/kubescrape-agent"]`.

### Metadata service

`cmd/kubescrape` serves Kubernetes pod/container metadata over HTTP. Everything is driven by one initial LIST + WATCH per resource (client-go informers); there is **no per-request API-server traffic**.

Data flow:

- **Pod informer** (full objects) → `internal/store.Store`: pods keyed by UID, with two derived indexes — normalized container-ID → entry, and nodeName → pods. `managedFields` are stripped by an informer transform before caching.
- **Service informer** (full objects) → `internal/services.Index`: selector-based matching of Services to pods, for service-annotation scrape discovery.
- **Metadata-only informers** (`PartialObjectMetadata`, cheap) for ReplicaSets, Deployments, Jobs, CronJobs, Namespaces → `internal/owners.Resolver`. Owner chains and namespace metadata are resolved **lazily at request time** from these caches, never stored on the pod record. Adding a new owner kind means extending `owners.AllGVRs` + `kindGVR` and the ClusterRole in `deploy/kubernetes.yaml`.
- `internal/server` composes the above per request; `internal/scrape` is pure functions deriving targets from `prometheus.io/*` annotations; `internal/kubemeta` holds the JSON model and the corev1.Pod → model conversion (`FromPod`).

### Invariants the API guarantees (user requirements — do not regress)

- **Container lookups block per container ID.** An unknown ID registers a waiter channel keyed by that exact ID; the pod-upsert path wakes only those waiters. Multiple concurrent requests for the same ID are all released. Waiting is bounded by `-wait-timeout` (client can shorten via `?wait=`, never lengthen). Requests may legitimately arrive ~1s before the kubelet posts the container ID to the API server — this wait covers that gap.
- **Deleted pods stay resolvable via the container endpoint for `-cache-ttl`** (tombstone: `expireAt` + `deletedAt` stamped, swept periodically). Container IDs replaced by restarts get the same TTL; `lastState.terminated` IDs are indexed while the pod lives.
- **Scrape targets never include deleted pods**: `DeletePod` removes the pod from the node index immediately, before tombstoning. `Succeeded`/`Failed` pods are excluded from targets but remain resolvable by container ID.
- **`prometheus.io/port` is a comma-separated list** (numbers and/or port names) on both pods and Services; service ports translate to pod ports via `targetPort`. Duplicate endpoints reachable via both pod and service annotations are deduped by URL (pod source wins).

### Concurrency model

One `sync.RWMutex` in `store.Store` (plus one in `services.Index`) guards everything; there are no goroutines besides HTTP handlers, the informer callbacks, and the sweeper (`Store.Run`). Stored `kubemeta.Pod` values are never mutated in place — upserts replace whole records — so returning shallow copies under RLock is safe. `FromPod` deep-copies everything it takes from informer objects; never retain or mutate an informer-owned object.

Store tests use an injectable clock (`s.now`); follow that pattern for any time-dependent behavior instead of sleeping.

### Node agent (`internal/agent/...`)

The agent talks only to the metadata service (`metaclient`) and an OTLP gRPC collector (`otlpexport`, built on `go.opentelemetry.io/collector/pdata`) — no Kubernetes API, no RBAC (except `nodes/metrics` for the kubelet scrapes). Resource-attribute building lives in `agent/attrs`: `attrs.Builder` (nil = defaults) runs defaults → static → config templates → `attrs.Filter`, driven by `-resource-attrs-config`/`-static`/`-enable`/`-disable`. It is invoked at every point a final resource is built — tailer metadata resolution, both single-resource batchers, and the cadvisor per-pod resources (which may pre-put identity attrs before Build). New resource-producing code paths must call `Attrs.Build`.

- **Tailer** (`agent/tailer`): single sweep goroutine over `/var/log/containers`. Log lines flow through the two-stage `github.com/JohanLindvall/multiline` pipeline, per file: `multiline/cri` parses the CRI format and rejoins P/F fragment runs (non-CRI lines pass through rather than being dropped), then `multiline` joins stack traces. Offset mapping relies on stage emission being synchronous inside `Add`/`Flush*`: `lastEnd[key]` is exact because the line completing a run is always the line currently being fed, and `Entry.Lines` (which counts even limit-dropped lines) pops the per-key FIFO of logical-line offset ranges. At-least-once invariants: exports happen inline in the sweep with retries; per-file offsets are committed only after a successful export and never past lines still buffered in either stage (`watermark` = min of `runStart` and FIFO heads); on failure files are rewound to the committed offset and the pipeline is discarded unemitted (the lines get re-read); committed offsets are checkpointed to disk. A file's metadata is resolved (with server-side wait) before its data is consumed — nothing is read until it can be attributed.
- **Scraper** (`agent/promscrape`): hand-written streaming parser for the Prometheus text format, classic + OpenMetrics (constant memory; do NOT replace it with `expfmt`, which buffers whole metric families — the 100k-series requirement is why it exists). The parser classifies samples into roles (`parser.go`); `convert.go` groups histogram/summary component series per family and label set into proper OTLP Histogram/Summary points — grouping state lives only for the current family, so memory stays bounded by the largest family, not the scrape. Exemplars are opt-in (`Config.Exemplars` / `-scrape-exemplars`): enabling them switches the Accept header to OpenMetrics; the parse mode is always detected from the response Content-Type (OM uses float-second timestamps, classic integer milliseconds). Conversion chunks at `BatchPoints` data points per export via the `chunker`/`sink` interfaces.
- **Kubelet scrapes** (`agent/cadvisor.go`, `agent/cgroup.go`): with `-kubelet-endpoint` the agent scrapes `/metrics/cadvisor` and `/metrics`, bearer-authenticated with the mounted ServiceAccount token (the agent's ServiceAccount exists ONLY for `nodes/metrics`). cadvisor routing (`cadvisorBatcher`) keys resources by the cgroup path in the `id` label (`cgroupIdentity` parses both cgroupfs and systemd layouts): container ID → exact incarnation via `/v1/containers/{id}` (non-blocking); pod-level series (e.g. `container_network_*`, which have NO container id) → `/v1/pods/{ns}/{name}` cross-checked against the cgroup pod UID; sandbox rows (`container="POD"`) fold into the pod resource (their cgroup names the pause container — never look that ID up). Lookups share a 1-minute TTL cache. `-cadvisor-rollups=false` drops above-pod-level cgroup series AND pod-level rows of container-scoped families; `podScopedFamily` (container_network_*) is the keep-list of families with no per-container breakdown. All four pipelines toggle independently: `-logs`, `-metrics`, `-cadvisor`, `-node-metrics`.
- Tailer tests are timing-based: use `startTailer` (guarantees file creation happens after the initial scan — files present at startup are skipped to their end) and `tl.retryBackoff` to keep retry tests fast.

### E2E in kind

`hack/otel-collector.yaml` deploys a contrib collector (debug exporter). Gotchas learned the hard way: with `verbosity: detailed` the collector's own stdout gets huge and kubelet rotates it within seconds (capture with `kubectl logs -f`, not `--since`); the agent must exclude the observability namespace (`-logs-exclude-namespaces`) or it feeds the collector its own output; after a collector rollout the agent's gRPC connection may keep delivering to the terminating pod for a while.
