# Configuration reference

kubescrape consists of two binaries built from one image:

* **`kubescrape`** — the metadata service (Deployment). Watches the
  Kubernetes API and serves pod/container metadata and scrape targets over
  HTTP.
* **`kubescrape-agent`** — the per-node agent (DaemonSet). Tails container
  logs and scrapes Prometheus targets, exporting OTLP.

Everything is configured through flags plus one optional unified YAML file on
the agent (`-config`, with `resourceAttributes`, `logs`, `logAttributes`,
`logMetrics`, `logScrubbing`, `metrics`, `traceMetrics`, `traceSampling`,
`tailSampling`, `serviceGraph`, `serviceGraphShards`, `routing` and `export`
sections) — plus, optionally, a separate hot-reloaded
Starlark transforms file (`-transforms-file`). The
[Helm chart](../charts/kubescrape) exposes all of it as values; the raw
manifests live in [deploy/](../deploy).

This document is the narrative reference; the exhaustive per-binary flag
inventory is [FLAGS.md](FLAGS.md), **generated** from the registered flag sets
— its defaults are authoritative because they cannot drift.

Two machine-readable schemas, both generated/enforced by tests:
[agent-config.schema.json](agent-config.schema.json) is a JSON Schema for the
`-config` YAML, generated from the very structs the file decodes into
(`additionalProperties: false` matches the strict decoder exactly) — point
your editor at it (`# yaml-language-server: $schema=…`) or validate in CI;
the chart carries a `values.schema.json`, so a typo'd Helm value fails
`helm install`/`template` instead of being silently ignored. Structural
only — `-check-config` remains the semantic validator (regexes, durations,
bounds, templates).

- [Build variants (optional pipelines)](#build-variants-optional-pipelines)
- [Metadata service](#metadata-service)
- [Agent: general](#agent-general)
- [Agent: OTLP export](#agent-otlp-export)
- [Agent: log collection](#agent-log-collection)
- [Unified config file (`-config`)](#unified-config-file)
- [Agent: log sources](#agent-log-sources)
- [Agent: journald](#agent-journald)
- [Agent: Kubernetes events](#agent-kubernetes-events)
- [Agent: Azure diagnostics](#agent-azure-diagnostics)
- [Agent: log attributes](#agent-log-attributes)
- [Agent: log metrics](#agent-log-metrics)
- [Agent: log scrubbing (PII)](#agent-log-scrubbing)
- [Agent: OTLP ingest](#agent-otlp-ingest)
- [Agent: span metrics](#agent-span-metrics)
- [Agent: trace sampling](#agent-trace-sampling)
- [Agent: tail sampling](#agent-tail-sampling)
- [Agent: metrics scraping](#agent-metrics-scraping)
- [Agent: kubelet scrapes (cadvisor, node)](#agent-kubelet-scrapes)
- [Agent: transforms (Starlark)](#agent-transforms-starlark)
- [Agent: routing](#agent-routing)
- [Resource attributes](#resource-attributes)
- [Metrics config](#metrics-config)
- [Scrape annotations](#scrape-annotations)
- [Helm values](#helm-values)
- [Complete example](#complete-example)

## Build variants (optional pipelines)

Two agent pipelines are compiled in through Go **build tags**, and the default
set lives in the Makefile — so a stock `make build` / `make image` contains both
and nothing about the shipped artifacts has changed:

```sh
TAGS ?= journald,azure          # Makefile; passed to build, test, vet, lint and image
```

| Build | journald | azure | Effect |
|---|---|---|---|
| `make build` | ✔ | ✔ | the default: agent is `CGO_ENABLED=1`, dynamically linked |
| `make build TAGS=azure` | — | ✔ | agent is **cgo-free and static**; no libsystemd needed |
| `make build TAGS=journald` | ✔ | — | **11 franz-go packages** (≈5 MB) gone |
| `make build TAGS=` | — | — | both |

* **`journald`** compiles in [the systemd journal reader](#agent-journald). It
  is the *only* reason the agent needs cgo — it links libsystemd through
  `coreos/go-systemd/sdjournal`, which is why the default image is
  `distroless/base` plus seven copied `.so` files. Without the tag the agent
  links statically, which is what `make image-static`
  ([Dockerfile.static](../Dockerfile.static)) puts on `distroless/static`.
* **`azure`** compiles in [the Azure diagnostics consumer](#agent-azure-diagnostics).
  Its Kafka client rides in every DaemonSet image for a pipeline that only ever
  runs in the one-replica singleton Deployment.

`make verify-tags` asserts the exclusions really happen (no cgo without
`journald`, no franz-go without `azure`).

> **A bare `go build ./cmd/kubescrape-agent/` passes no tags and builds an agent
> with NEITHER pipeline.** `make` is the supported path; otherwise pass `-tags`
> yourself. The same applies to `go test` and `go vet` — `make test` passes
> `$(TAGS)` so the real code, not the stubs, is what gets tested.

**The flags never disappear.** `-journald` and `-azure-diagnostics` are defined
in every build (the shipped manifests pass them, and a missing flag would make
the process exit 2 with `flag provided but not defined`). Enabling a pipeline
the binary does not contain is instead a **startup error naming the tag**:

```
-journald is set, but this kubescrape-agent was built WITHOUT the "journald"
build tag, so the systemd journal reader is not compiled into it: either drop
-journald, or use an agent built with the tag (`make build` / `make image`
default to TAGS=journald,azure and contain every pipeline; a bare `go build`
contains neither)
```

`-check-config` raises the same error, so a rollout fails the dry run rather
than the DaemonSet. Every start (and `-check-config`) also reports which binary
it is — `optionalPipelines=journald,azure`, or `(none)`.

**The `-config` file is not affected by any of this.** No section belongs to
either pipeline, so one ConfigMap stays decodable by every variant; only
*enabling* an absent pipeline fails.

## Metadata service

```sh
kubescrape -listen :8080 -wait-timeout 5s -cache-ttl 5m -log-format json
```

| Flag | Default | Description |
|---|---|---|
| `-listen` | `:8080` | HTTP listen address (the `/v1` API, `/healthz`, `/readyz`, and a `/debug` homepage with forms for the parameterised routes; `/` redirects there) |
| `-kubeconfig` | — | kubeconfig path; defaults to in-cluster config, then `$KUBECONFIG` / `~/.kube/config` |
| `-wait-timeout` | `5s` | default and maximum time a container lookup blocks waiting for metadata (`?wait=` can shorten per request, never lengthen) |
| `-cache-ttl` | `5m` | how long metadata of deleted pods and replaced container IDs stays resolvable (tombstones) |
| `-metadata-cache-ttl` | `10s` | `max-age` stamped on metadata responses (`Cache-Control` + `ETag`) so the agent's client caches lookups and revalidates with `If-None-Match`/304; 0 disables cache headers |
| `-resync` | `0` | informer resync period (0 = watch stream only) |
| `-apiserver-probe-interval` | `30s` | how often to check that the API server is still reachable **from this pod** (0 disables). This is the outage signal, and it exists because nothing else is one: readiness LATCHES once the initial sync completes (deliberately — an unready service is dropped from its Service endpoints, cutting every agent off a cache that is still serving useful data), and `kubescrape_informer_watch_errors_total` cannot be relied on to cover an unreachable server, because client-go treats a refused connection as retriable and retries the watch inside the reflector without ever returning an error to the handler the counter lives in — measured silent across four outage shapes up to five minutes. Each probe is one `limit=1` metadata list of namespaces (a resource the service already watches, so no extra RBAC) with no `resourceVersion`, so it fails when etcd does rather than being answered from the watch cache. It publishes `kubescrape_apiserver_reachable` (1/0) and `kubescrape_apiserver_probe_failures_total`, warns on the transition and re-warns on a throttle while the outage persists, and logs the recovery. **Alert on it sustained, not on a single failure**: the probe measures whether a NEW connection reaches the API server, which is neither proof that the informer caches are advancing (a blackholed established watch probes healthy) nor proof that they stopped (one failed probe may be a blip). Set to `0` to turn the steady-state list off entirely; reachable from the chart via `extraArgs` |
| `-servicemonitors` | `false` | serve targets for `monitoring.coreos.com/v1` ServiceMonitors selecting pod-backed Services — plus **PodMonitors** (endpoints name container ports) when the cluster serves that CRD. Endpoint `port`/`targetPort`/`path`/`scheme`, `interval`/`scrapeTimeout`, `basicAuth`, `authorization`, `bearerTokenSecret`, `tlsConfig` (`insecureSkipVerify`, secret-backed `ca`/`cert`/`keySecret`, `serverName`) and the keep/drop subset of `metricRelabelings` are honored. Anything else an endpoint or monitor sets — `oauth2`, proxy settings, target `relabelings`, non-keep/drop relabel actions, configMap- and file-backed TLS material, the monitor-level guard rails (`sampleLimit`, `scrapeProtocols`, …); the authoritative list is `endpointSpec.ignoredFields()` + `specLimits.ignored()` in `internal/servicemonitors` — is **reported**: logged once per monitor and counted in `kubescrape_monitor_fields_ignored_total{kind}`, so a partially-applied CR is never silent. Self-disables with a warning when the CRD is absent |
| `-monitor-namespaces` | — | comma-separated namespaces whose ServiceMonitors/PodMonitors are honoured (empty = all, the historical default). **This is the gate**: a monitor is an instruction to every agent to issue a GET, so without it anyone who can create a ServiceMonitor in their own namespace can point `selector: {}` + `namespaceSelector.any: true` at an arbitrary path cluster-wide. It applies at INDEXING time, so a refused monitor never widens the `-scrape-auth-secrets` allowlist either. Reachable from the chart via `extraArgs` |
| `-scrape-auth-secrets` | `false` | serve monitor endpoints' `bearerTokenSecret` values to agents on `GET /v1/scrape-auth/{ns}/{name}/{key}`. Opt-in: requires `secrets get` RBAC (commented out in the manifests), `-scrape-auth-token-file`, and ships tokens over the cluster-internal HTTP channel |
| `-scrape-auth-token-file` | — | file with the shared bearer token callers must present on `/v1/scrape-auth` as `Authorization: Bearer <token>`. **Mandatory** with `-scrape-auth-secrets` — that endpoint is the only one serving Secret material, so starting without a token is refused rather than leaving it open to every pod in the cluster. Compared in constant time. The file is re-read about once a minute, and after a change the PREVIOUS token stays accepted for a 5-minute grace window — so rotating the Secret is a non-event: agents (which re-read their copy on the same cadence) and the service converge with no restarts and no 401 storm |
| `-self-metrics-interval` | `1m` | export the service's own metrics over OTLP at this interval (0 disables) |
| `-self-attributes` | `true` | add THIS pod's Kubernetes resource attributes (namespace, pod, uid, owners, labels) to those exported metrics. No API traffic and no manifest wiring: the service reads its own store, its pod name is the hostname and its namespace comes from `$POD_NAMESPACE` or the ServiceAccount projection it already mounts. Re-read once a minute (a bare in-process store lookup), so a pod or namespace relabelled after startup is picked up. Fill-if-absent — `service.name` stays `kubescrape` and `service.instance.id` stays the hostname, but `service.namespace` becomes the pod's namespace, so the job reads `<namespace>/kubescrape`. A process that is not a pod of that name simply gets none, reported by `kubescrape_self_metadata_resolved`. The agent has the same flag (resolved differently — see its section) |
| `-otlp-*` | as the agent | used by the self-metrics push: `-otlp-endpoint`, `-otlp-protocol`, `-otlp-compression`, `-otlp-compression-level`, `-otlp-insecure`, `-otlp-tls-ca-file`, `-otlp-tls-insecure-skip-verify`, `-otlp-bearer-token-file`, `-otlp-timeout` |
| `-otlp-header` | — | static `key=value` header sent on every self-metrics export (HTTP header / gRPC metadata, e.g. `X-Scope-OrgID=tenant`); repeatable — repeatable rather than comma-separated so a value may contain commas |
| `-log-level` | `info` | `debug`, `info`, `warn`, `error` |
| `-log-format` | `text` | `text` or `json` (client-go's klog is routed through the same handler) |

The service's own metrics (store sizes, HTTP requests per pattern/status)
are pushed over OTLP on `-self-metrics-interval` (default
1m, 0 disables) using the `-otlp-*` flags.

The process's own Go runtime and process metrics (`go_*`, `process_*`) are
served instead as Prometheus text on a DEDICATED port, `-metrics-listen`
(default `:9090`, empty disables) — they are operator-facing process
diagnostics, consumed by a scrape rather than pushed. With
`-self-metrics-interval=0` that port additionally serves the `kubescrape_*`
internal metrics, replacing the OTLP push with a scrape: one knob selects the
delivery modality, so the same series never ship over both paths.
`-pprof-listen` (empty by
default) serves `net/http/pprof` on a third port; profiles expose goroutine
stacks and heap contents, so it is separate from both.

RBAC (cluster-wide `get`/`list`/`watch`): `pods`, `services`, `namespaces`,
`nodes`, `replicasets.apps`, `deployments.apps`, `statefulsets.apps`,
`daemonsets.apps`, `jobs.batch`,
`cronjobs.batch`, `servicemonitors.monitoring.coreos.com` (plus
`podmonitors` when that CRD should be discovered).
`-scrape-auth-secrets` needs `secrets get` (commented out in the
manifests — enable deliberately) — see
[deploy/kubernetes.yaml](../deploy/kubernetes.yaml).

## Agent: general

| Flag | Default | Description |
|---|---|---|
| `-node-name` | `$NODE_NAME` | the node this agent runs on (set via the downward API) |
| `-listen` | `:8081` | serves `/debug` (homepage linking the debug surfaces), `/healthz`, `/readyz`, `/debug/tailer` (per-file positions/lag, malformed pod annotations), `/debug/targets` (per-target last outcomes, failures first), `/debug/transforms` (active transform program hash), `/debug/otlp` (live stream of exported OTLP as JSON lines; `signal`/`attr`/`sample` query params, UI at `/debug/otlp/ui`); empty disables. NOT `/metrics` — the Prometheus endpoint lives on its own `-metrics-listen` port |
| `-self-metrics-interval` | `1m` | export the agent's own metrics over OTLP at this interval (0 disables); both binaries have this flag |
| `-metadata-endpoint` | `http://kubescrape.monitoring` | base URL of the metadata service |
| `-metadata-wait` | `5s` | server-side wait for not-yet-known containers (covers the gap between container start and the kubelet posting its status) |
| `-node-metadata-refresh` | `1m` | refresh interval for the node's labels/annotations used in attribute templates (0 disables) |
| `-self-attributes` | `true` | add THIS pod's Kubernetes resource attributes (namespace, pod, uid, pod IP, owners, labels, plus the `resourceAttributes` section's `self` pipeline) to the metrics the agent generates about itself — its self-metrics and span metrics. Resolved from the metadata service's `GET /v1/self`, which attributes the request by its connection's source address, falling back to a lookup by name (`$POD_NAME` or the hostname, in `$POD_NAMESPACE` or the ServiceAccount projection's namespace) when that cannot work — hostNetwork agents share the node IP, a NAT hop replaces the source address, and a dual-stack pod may connect from the family `status.podIP` does not carry. Fill-if-absent: `service.name` stays `kubescrape-agent` and `service.instance.id` stays the node, but `service.namespace` becomes the pod's namespace — so the agent's own job reads `<namespace>/kubescrape-agent`. A caller the service cannot attribute to a live pod (hostNetwork, an address-rewriting hop, no Kubernetes) simply gets no extra attributes; nothing waits on the lookup, and `kubescrape_self_metadata_resolved` reports whether it succeeded |
| `-self-attributes-refresh` | `1m` | how often `-self-attributes` re-reads this pod's own metadata, so a pod or namespace **relabelled after startup** reaches the metrics that stamp it. The poll is nearly free: `/v1/self` answers with `private, max-age=<-metadata-cache-ttl>` + `ETag`, so a fresh entry is served from the client's own cache and a stale one revalidates with `If-None-Match` — a 304 whenever nothing changed. (`private` is what makes caching a caller-dependent response safe: one client belongs to one process, which is the pod the answer describes; shared caches are told not to store it.) Retries before the first success start at 5s and back off to this. `0` disables the lookup entirely (and with it the `kubescrape_self_metadata_resolved` gauge, which is published exactly when the lookup runs — so a `0` reading always means unresolved, never "switched off"). The agent's own metrics bypass the namespace router regardless of this setting: they keep the durable default chain rather than following a route glob that happens to cover the agent's namespace |
| `-check-config` | `false` | compile every config section plus the flags, print a summary and exit — nothing acquired (CI / pre-rollout). Flag **values** are checked too: one you passed that the process cannot honour — a non-positive `-scrape-timeout`, a `-logs-rate-burst` below one whole token — is an error naming the flag, the value and a usable one, rather than a value quietly replaced by a working default on a fleet you are already rolling out to. Only what you passed: an untouched flag is always its default |
| `-test-config` | — | run the YAML test cases in this file through the compiled log pipeline (scrub → logAttributes → enrich → logMetrics → `logs.rules` → transforms) and exit non-zero on failure; like `-check-config`, nothing is acquired. See the README's "Config unit tests" for the file shape |
| `-log-level` / `-log-format` | `info` / `text` | as for the service |

Pipeline toggles (all default `true` except the opt-in `-journald`,
`-ingest`, `-events` and `-azure-diagnostics`):

| Flag | Enables |
|---|---|
| `-logs` | container log tailing |
| `-metrics` | annotation-discovered pod/service targets |
| `-cadvisor` | `<kubelet-endpoint>/metrics/cadvisor` (needs `-kubelet-endpoint`) |
| `-node-metrics` | `<kubelet-endpoint>/metrics` (needs `-kubelet-endpoint`) |
| `-journald` | systemd journal tailing (default `false`, [below](#agent-journald); needs the `journald` [build tag](#build-variants-optional-pipelines), which the default build sets) |
| `-events` | Kubernetes events as OTLP logs (default `false`; **cluster-singleton** — its own Deployment, not the DaemonSet, [below](#agent-kubernetes-events)) |
| `-azure-diagnostics` | Azure diagnostic-settings logs + metrics from Event Hubs (default `false`; **cluster-scoped** — same singleton Deployment as `-events`, [below](#agent-azure-diagnostics); needs the `azure` [build tag](#build-variants-optional-pipelines), which the default build sets) |

## Agent: OTLP export

| Flag | Default | Description |
|---|---|---|
| `-otlp-endpoint` | `otel-collector.monitoring:4317` | `host:port` for gRPC, base URL for HTTP |
| `-otlp-protocol` | `grpc` | `grpc` or `http` (OTLP/HTTP protobuf on `/v1/logs`, `/v1/metrics`) |
| `-otlp-compression` | `gzip` | payload compression on either transport (`gzip` via klauspost/compress, or `none`); telemetry compresses 5–10x |
| `-otlp-compression-level` | `0` | gzip level `1` (fastest, ~2-3x less CPU for ~10% larger payloads) to `9` (smallest); `0` = library default |
| `-otlp-insecure` | `true` | plaintext gRPC (for HTTP, choose via the URL scheme) |
| `-otlp-bearer-token-file` | — | sends `Authorization: Bearer <token>` on either transport; re-read every minute, so rotated tokens work |
| `-otlp-tls-ca-file` | — | PEM CA bundle for verifying the collector. TLS material on a plaintext destination (gRPC with `-otlp-insecure`, or an `http://` endpoint) is refused at startup rather than silently ignored — the failure it otherwise produces is telemetry shipped in cleartext, which nothing surfaces |
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

### Per-signal destinations (`export` section)

The flags configure ONE endpoint for every signal. The config file's `export`
section overlays per-signal destinations onto that base — which is what makes
**collectorless** deployment expressible: Mimir, Loki and Tempo all ingest
OTLP natively, but on three different hosts/paths. Each override rides the
existing per-signal disk-buffer queue, so durability is unchanged; the base
part adds what the flags cannot express — static headers (tenancy) on the
default **buffered** chain (the `routing` section's headers are deliberately
unbuffered) and an mTLS client certificate:

```yaml
export:
  headers: {X-Scope-OrgID: platform}      # every signal, buffered chain included
  clientCertFile: /etc/certs/client.crt   # mTLS towards the backends (both or neither)
  clientKeyFile: /etc/certs/client.key
  logs:
    endpoint: https://loki.example.com/otlp
    protocol: http
  metrics:
    endpoint: https://mimir.example.com/otlp
    protocol: http
    headers: {X-Scope-OrgID: metrics-tenant}  # merges over the base, override wins
  traces:
    endpoint: tempo.example.com:4317
    insecure: false   # gRPC TLS — required here: a client cert on a plaintext
                      # connection is refused at startup rather than silently unused
```

Empty/omitted fields inherit the flag base (`endpoint`, `protocol`,
`headers`, `bearerTokenFile`, `caFile`, `insecure`, `insecureSkipVerify`,
`compression`, `clientCertFile`/`clientKeyFile` are overridable per signal).
The flag endpoint remains the **fallback** for any signal without an
override; when all three are overridden it is unreachable and no client is
built for it, so a collectorless deployment need not point `-otlp-endpoint`
at anything real. TLS material (a CA bundle or client certificate) on a
destination that turns out to be plaintext is a startup error, never a
silent no-op.
`-check-config` validates the section's shape without touching any files.
OAuth2 and AWS SigV4 export auth are deliberately not implemented — each
drags a heavyweight SDK dependency; front such endpoints with a collector
hop or an authenticating proxy.

## Agent: log collection

| Flag | Default | Description |
|---|---|---|
| `-config` | — | single YAML file holding all sections: `resourceAttributes`, `logs`, `logAttributes`, `logMetrics`, `logScrubbing`, `metrics`, `traceMetrics`, `traceSampling`, `tailSampling`, `serviceGraph`, `serviceGraphShards`, `routing`, `export` ([below](#unified-config-file)) |
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
| `-enrich` | `true` | one switch for every log-producing pipeline (container logs, journald, Kubernetes events, Azure diagnostics, pushed OTLP log bodies): parse per-line metadata via [enrich](https://github.com/JohanLindvall/enrich): a timestamp in the line replaces the CRI time, an explicit level sets the severity, trace/span IDs fill the OTLP trace fields, exception/template/source-context details become record attributes. JSON, logfmt and common plain-text formats are recognized; the body is never modified, and plain-text stack traces are not duplicated into `exception.stacktrace`. Hit rates: `kubescrape_log_enriched_total{format}` in the self-metrics |
| `-logs-file-attributes` | `false` | stamp `log.file.name` (basename) and `log.file.position` (record start offset) on every record, for each file source |
| `-buffer-dir` | — | directory for a disk-backed export buffer (logs, metrics, and tail-sampled traces on the `-service-graph` tier); a collector outage spools here instead of pinning the tailer to old offsets / dropping metrics ([below](#disk-buffer)). Empty disables |
| `-buffer-max-bytes` | `1GiB` | per-signal cap on the undelivered on-disk backlog; producers back-pressure (the tailer rewinds) when full |
| `-logs-exclude-namespaces` | — | comma-separated namespaces not tailed — **always exclude the namespace of your collector** to avoid feedback loops |
| `-logs-rate-limit` | `0` | per-file line rate limit (lines/second, token bucket; 0 disables). An exhausted file is **paused** — reading stops until tokens refill, the backlog stays on disk, nothing is lost (rotation drains bypass the limiter) |
| `-logs-rate-burst` | `0` | token bucket size (`0` = 2× the rate). A line spends one **whole** token and the bucket never holds more than the burst, so a burst you pass below 1 could never admit a line — it is **refused at startup**, `-check-config` included. A bucket *derived* below 1 (a rate under 0.5 lines/s) is not refused: the rate you passed is delivered exactly, the tailer floors only the bucket at 1 (refill unchanged), and startup warns that it did |
| `-logs-rate-drop` | `false` | discard lines over the limit instead of pausing (lossy; counted in `kubescrape_log_rate_limited_total{action="drop"}`) |
| `-logs-unknown-files` | `auto` | where a file with no checkpoint entry starts at startup: `end` (skip as pre-existing history), `start` (read whole), `auto` (start when the checkpoint store already has entries — the file appeared while the agent was down; end on a first-ever run). `auto`/`start` mean adding a new log source ingests those files' existing content |
| `-logs-idle-close` | `0` | close a fully-caught-up file's fd after this much inactivity (`0` = never). Bounds steady-state fds at one per *active* file rather than one per tracked file — but the open fd is the only handle to a rotated-away or deleted file's remaining bytes, so enabling this **forfeits the zero-loss guarantee** for the tail of a dying file. Set it only where a node tracks thousands of log files |
| `-logs-metrics-interval` | `30s` | export interval for the `logMetrics` metrics ([below](#agent-log-metrics)) |
| `-logs-metrics-max-bytes` | `3145728` (3 MiB) | export log-derived metrics in chunks below this many bytes (0 = one payload). Integer bytes only — human-format sizes like `3MiB` are not parsed (and the chart's `agent.logsMetrics.maxBytes` refuses them at template time) |
| `-logs-metrics-name-prefix` | — | prefix prepended to every log-derived metric name |

Delivery is at-least-once: offsets are committed only after the collector
acknowledged the batch and never past lines still buffered in the multiline
pipeline; on a transient export failure the files rewind to the committed
offset. Rotation handling (rename, copytruncate — including same-size rewrites
— deletion) is automatic.

There is exactly one path where the tailer gives up on data. A batch the
collector rejects **definitively** — a malformed payload, an unimplemented
endpoint, a body over the receiver's limit — cannot be fixed by retrying, and
one sweep goroutine serves every file on the node, so retrying it forever would
freeze all log shipping there, pin the file descriptor against logrotate, and
lose the whole backlog when the file eventually rotates away. Instead the batch
is dropped, the offsets advance, and the loss is counted in
`kubescrape_log_permanent_dropped_total` alongside an `ERROR` log line.
**Any nonzero rate on that counter is dropped logs and is worth an alert**; it
is the only loss the tailer decides on itself (`kubescrape_log_export_failures_total`
counts rewinds, which lose nothing). With `-buffer-dir` set the tailer's export
returns the *enqueue* verdict rather than the collector's, so this counter then
moves only for a batch larger than the whole buffer cap and the collector's own
permanent rejections land on `kubescrape_buffer_dropped_batches_total{signal="logs"}`
instead — alert on that one for the buffered chain, and read
`kubescrape_buffer_dropped_records_total{signal="logs"}` beside it for the size
of the loss. Every drop counter in the agent now comes in that pair: the batch
counter says a loss happened, the records counter says how much, and a batch is
anywhere from one record to a thousand.

A file whose container metadata cannot be resolved is retried on an
exponential backoff (2s doubling to a 1m cap, each delay jittered up to +25%)
rather than on a flat interval: the lookup blocks server-side for the whole
`-metadata-wait` on the single sweep goroutine, so a short flat retry lets a
handful of unresolvable files starve every other file on the node, and an
unjittered one puts every agent in the fleet on the same schedule — turning a
metadata-service rollout into a synchronised recovery burst. Nothing is read
from a file until it resolves, so nothing is lost meanwhile.

Backlog is observable per node — `kubescrape_log_lag_bytes` (the total across
tracked files) and `kubescrape_log_lag_max_bytes` (the largest single file's) in
the self-metrics — and per file on
`GET /debug/tailer` (path, container, read/committed offsets, lag,
rate-limited flag, and — for a pod whose `kubescrape.io/logs` annotation
failed to parse — the error as `podConfigError`, with the aggregate on
`kubescrape_log_pod_config_invalid_total`; refreshed ~10s, largest lag
first).

### Disk buffer

Without `-buffer-dir`, durability is checkpoint-and-rewind: during a collector
outage the tailer stops advancing (the source files are the buffer) and scraped
metrics are dropped and re-scraped. A long outage can lose logs if the source
files rotate away first.

With `-buffer-dir` set, every export goes through a disk-backed write-ahead
buffer instead — one durable FIFO queue per signal, backed by
[JohanLindvall/diskqueue](https://github.com/JohanLindvall/diskqueue). A batch
is serialized, `fsync`'d, and acknowledged to the producer immediately (so the
tailer commits its offsets and source logs may rotate away), then a background
sender drains it to the collector with retries; a batch is removed only after
the collector accepts it. Delivery stays at-least-once and survives agent
restarts (per-record checksums; corruption degrades to reported loss, never a
wedged queue). The undelivered backlog is capped per signal by
`-buffer-max-bytes`; when full, enqueues fail and the tailer back-pressures
(rewinds), so disk stays bounded — a single batch larger than the whole cap is
refused outright. A latched I/O failure (diskqueue treats a failed fsync as
non-retriable) recovers by automatic close-and-reopen, at the cost of
redelivering the affected batch.

Point `-buffer-dir` at a node-local persistent path (e.g. under the agent's
state hostPath) so the buffer survives pod restarts. Note that delivered-but-
not-yet-reclaimed records linger until their whole segment is retired, so
physical disk use can exceed the backlog cap by up to one segment (8 MiB).

**Traces are not buffered — with one exception.** A forwarded trace is still
held by the application that pushed it, and its SDK's retry is a better
durability story than a queue that would ack that sender and remove the only
other copy; so `ExportTraces` passes straight through. The exception is a
**tail-sampling decision** on the trace tier
([below](#agent-tail-sampling)): those spans were acked when they were buffered
for the decision window, so nothing else holds them by the time a verdict ships.
Such a payload marks itself as owned and is spooled like any log batch, into a
third queue (`traces/`) that is opened only where tail sampling runs. Its
occupancy shows up as `kubescrape_buffer_backlog_bytes{signal="traces"}`,
alongside the `_max_bytes` and `_segments` gauges the other signals publish.

## Agent: journald

Opt-in with `-journald`, and **behind the `journald`
[build tag](#build-variants-optional-pipelines)** (set by default): this is the
only pipeline that makes the agent need cgo, so an agent built without the tag
is fully static and refuses `-journald` at startup rather than collecting
nothing. The agent reads the systemd journal natively through
libsystemd (`github.com/coreos/go-systemd/v22/sdjournal`, cgo — the agent binary
is built with cgo and the image ships libsystemd) and exports the entries as
OTLP log records, one resource per systemd unit (`service.name` = the unit
without `.service`, `systemd.unit`, plus node attributes via the `journal` attrs
pipeline; syslog priorities map to OTLP severities; `syslog.identifier`,
`process.pid` and `systemd.transport` — the journal's `_TRANSPORT`, which is
what separates `kernel`/`stdout`/`syslog` streams sharing a unit — become
record attributes).

> **Upgrade note.** Journal records now carry an instrumentation-scope name —
> `otel_scope_name=github.com/JohanLindvall/kubescrape/agent/journald` — where
> earlier releases shipped an empty one. Every other producer named its scope;
> journald was the outlier. This is **wire-visible**: backends that turn the
> scope into a label (Loki, Elasticsearch) see a new label value, so every
> journal stream splits at the upgrade boundary. Expect a discontinuity in
> dashboards and alerts that group on labels including `otel_scope_name`, and
> match on the unit (`service.name` / `systemd.unit`) instead if you need
> continuity across the upgrade.

> **Upgrade note.** Journal severities were one grade too low at the top of the
> syslog range: priority 0/1/2 (emerg/alert/crit) mapped to FATAL/ERROR3/ERROR2
> where the OpenTelemetry logs data model says FATAL3 (23) / FATAL2 (22) /
> FATAL (21). They now use the data model's numbers, which is also what the
> enrichment step (`-enrich`) has always applied — and since enrichment
> OVERWRITES the severity whenever it parses a level out of the message, the
> same journal entry previously reported a different severity number depending
> on whether its text happened to look like a log line to a parser. **Wire-
> visible**: alerts selecting `severity_number >= 21` (or 18/19) on journal
> streams need re-checking.
>
> Severity **text** is now lowercase across every log producer — journald,
> Kubernetes events and Azure diagnostics — matching what enrichment writes and
> what `logs.rules` already matched on (the rules lowercase before comparing, so
> no rule changes). Events and Azure diagnostics previously emitted `WARN` /
> `INFO` / `ERROR` / `FATAL` / `DEBUG`. **Wire-visible** for anything comparing
> `severity_text` case-sensitively.

| Flag | Default | Description |
|---|---|---|
| `-journald-dir` | — | read a specific journal directory; empty opens the default system journal, which already covers the volatile one. **What the pipeline actually needs is the host journal MOUNTED into the container** — `/var/log/journal` (persistent) and/or `/run/log/journal` (volatile). The chart does it behind `agent.journald.enabled`; a hand-rolled manifest must add it, or the reader starts, reports ready and collects nothing. The agent now WARNs at startup when the resolved journal holds no readable files |
| `-journald-units` | — | comma-separated units (matched on `_SYSTEMD_UNIT`); empty reads everything |
| `-journald-batch-size` | `1024` | flush after this many entries |
| `-journald-max-batch-bytes` | `1048576` | flush before a batch's summed message bytes exceed this |
| `-journald-flush-interval` | `2s` | flush at least this often |
| (`-enrich`) | `true` | per-message enrichment, same switch as container logs; an explicit level in the message wins over the journal priority |

Delivery is at-least-once: the cursor is committed only after a successful
export; on export failure or a reader error, it restarts from the committed
cursor with backoff (re-reading anything in flight). The cursor is
persisted only through `-positions-file` (there is no standalone journald
cursor file); without it, every start begins at the journal tail.

## Agent: Kubernetes events

Opt-in with `-events`. Kubernetes events are a **cluster-wide** stream, not a
per-node one, so this pipeline runs the agent binary as its own single-replica
Deployment with every per-node pipeline off — see
[deploy/events.yaml](../deploy/events.yaml) or `events.enabled=true` in the
chart:

```sh
kubescrape-agent -events \
  -logs=false -metrics=false -cadvisor=false -node-metrics=false \
  -metadata-endpoint=http://kubescrape.monitoring \
  -otlp-endpoint=otel-collector.monitoring:4317
```

It is deliberately **not** part of the DaemonSet: that would put cluster-wide
`events` read plus lease/ConfigMap write credentials on every node (the
DaemonSet's ServiceAccount holds only `nodes/metrics: get`), and every
non-leader would poll the election Lease once per retry period — an
API-server fan-out that grows with the node count. Exactly one replica reads
at a time (a `coordination.k8s.io` **Lease**), so a rolling update's overlap,
or `replicas > 1`, never double-ships.

| Flag | Default | Description |
|---|---|---|
| `-events` | `false` | enable the events reader |
| `-events-namespace` | — | watch one namespace; empty is cluster-wide |
| `-events-start` | `auto` | where a **cold** start begins (no stored position): `end` skips the backlog, `start` replays everything still within the API server's event TTL (typically 1h), `auto` resumes the stored position and otherwise behaves as `end` |
| `-events-batch-size` | `512` | flush after this many events, **clamped to the retained-batch cap** (16384). The startup backlog walk blocks the single reader goroutine and services no flush ticker, so its only trigger is this count — unclamped, a larger value made the trigger unreachable and shed the entire backlog into `kubescrape_events_overflow_dropped_total` before the watch even opened |
| `-events-flush-interval` | `2s` | flush at least this often |
| `-events-position-interval` | `10s` | how often the position is written to its ConfigMap. A write per event would be an API-server write per event, so this bounds how much is **replayed** after a hard kill (bounded duplicates, never loss); a graceful stop always writes a final position |
| `-events-position-configmap` | `kubescrape-events-position` | ConfigMap holding the resume position |
| `-events-lease` | `kubescrape-cluster-leader` | Lease coordinating the singleton |
| `-events-lease-namespace` | — | namespace for the Lease and the ConfigMap; empty uses this pod's own (`$POD_NAMESPACE` via the downward API, else the ServiceAccount projection) |
| `-kubeconfig` | — | kubeconfig for the watch; empty uses the in-cluster config (only read with `-events`) |

RBAC: `get`/`list`/`watch` on `events` (both `""` and `events.k8s.io`)
cluster-wide, plus `get`/`create`/`update` on `leases` and `configmaps` in
the reader's own namespace.

**Records.** The body is the event message, with `k8s.event.reason`,
`k8s.event.action`, `k8s.event.type`, `k8s.event.count`, `k8s.event.name`,
`k8s.event.uid`, `k8s.event.involved_object.*` and
`k8s.event.reporting_component`/`_instance` as record attributes; severity
comes from the event type (`Warning` → WARN, else INFO). The **resource is
the involved object's own**: an event about a pod is resolved through the
metadata service (`GET /v1/pods/{ns}/{name}`, with the UID cross-checked so a
recreated pod of the same name cannot lend its identity to an event about its
predecessor) and carries the same `k8s.*`/`service.*` attributes as that pod's
container logs, so events and logs line up in one query. Other kinds get
`k8s.namespace.name` plus the kind's own name attribute. Aggregated events
(the API server's `count`/`lastTimestamp` rollup) arrive as MODIFIED and are
exported as fresh occurrences. This reader's own node is never stamped on a
record — the node an event is about is the involved object's property.

**Delivery** is at-least-once, as everywhere else: the position (the watch
`resourceVersion` plus a timestamp watermark) is persisted only *after* the
collector acks, and it lives in a ConfigMap rather than a node-local file
because the leader moves — the successor of a killed pod resumes where its
predecessor stopped. A written position is only ever a *lower bound* on what
was delivered, so even a zombie writer can at worst cause a replay, never a
gap. When the API server's watch window has passed (`410 Gone`) the reader
re-lists and the watermark suppresses what it already shipped; resumption is
therefore exact only within that window (minutes) — beyond it, the replay
protection is the watermark.

The whole agent chain applies: `logScrubbing`, `logAttributes`, `-enrich`,
`logMetrics` (which observe every event, including dropped ones),
`logs.rules`, Starlark transforms, `routing` and the disk buffer.

## Agent: Azure diagnostics

Opt-in with `-azure-diagnostics`. Azure resources stream their **diagnostic
settings** output — resource logs and platform metrics — to an Event Hubs
namespace; the agent consumes it over the namespace's **Kafka endpoint**
(franz-go, port 9093) and exports OTLP. Like `-events` it is cluster-scoped
and belongs in the singleton Deployment (`azure.enabled=true` in the chart
renders it there), not the DaemonSet — but it needs **no leader election**:
the Kafka consumer group is the coordination (each partition is owned by
exactly one member), and its **committed offsets are the resume position**,
shared across restarts and replicas with no ConfigMap. `replicas > 1` simply
share partitions.

It is **behind the `azure` [build tag](#build-variants-optional-pipelines)**
(set by default): its Kafka client is 11 franz-go packages (≈5 MB) shipped in
every DaemonSet image for a pipeline only the singleton Deployment ever runs, so
`make build TAGS=journald` drops it — and `-azure-diagnostics` on such a binary
is a startup error, caught by `-check-config`.

| Flag | Default | Description |
|---|---|---|
| `-azure-diagnostics` | `false` | enable the consumer |
| `-azure-eventhub-namespace` | — | comma-separated namespace hosts (`myns.servicebus.windows.net`), one client each; may be omitted when the connection strings supply it, where at most one is allowed as an override |
| `-azure-eventhub-topics` | — | comma-separated hubs; empty consumes the hub an entity-scoped connection string's `EntityPath` names, else every hub matching `^insights-.*` (the names diagnostic settings create by default) |
| `-azure-eventhub-group` | `$Default` | consumer group holding the committed offsets |
| `-azure-eventhub-connection-string-file` | — | comma-separated files, one connection string each, namespace- or entity-scoped → **SASL PLAIN** (re-read per connection, so rotation needs no restart), **one client per file**; empty → **managed identity** |
| `-azure-client-id` | `$AZURE_CLIENT_ID` | user-assigned managed identity / workload identity client id |
| `-azure-tenant-id` | `$AZURE_TENANT_ID` | Entra tenant for workload identity |
| `-azure-start` | `end` | where a group with **no committed offsets** starts: `end` (skip the backlog) or `start` (replay everything the hubs retain) |
| `-azure-metric-prefix` | `azure.` | prefix for converted metric names |

**Auth.** Two paths. A **connection string** authenticates as SASL PLAIN
(user `$ConnectionString`, the string as the password). **Managed identity**
authenticates as SASL OAUTHBEARER with a Microsoft Entra token scoped to the
namespace: AKS **workload identity** when its environment is present (the
webhook-injected federated token file + `$AZURE_CLIENT_ID`/`$AZURE_TENANT_ID`
— in the chart, setting `azure.clientId` annotates the ServiceAccount and
labels the pod so the webhook injects them), else **IMDS** (system-assigned,
or user-assigned via `-azure-client-id`). Tokens are cached and refreshed
ahead of expiry; a token-endpoint blip serves the still-valid cached token.
Both protocols are implemented directly (two small HTTP exchanges) — no
Azure SDK dependency. On the Azure side the identity needs the **Azure Event
Hubs Data Receiver** role on the namespace (or hub); a connection string
needs a policy with the **Listen** claim.

**Connection-string scope.** Both shapes are accepted, and which one you paste
changes the topic default:

```
# namespace-scoped — copied from the NAMESPACE's shared access policies
Endpoint=sb://myns.servicebus.windows.net/;SharedAccessKeyName=Root;SharedAccessKey=<key>

# entity-scoped — copied from ONE event hub's shared access policies
Endpoint=sb://myns.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=<key>;EntityPath=azure
```

An `EntityPath` both names a hub and bounds what the credential may reach (a
SAS rule's scope is the resource it was created on), so with no explicit
`-azure-eventhub-topics` the consumer subscribes to exactly that hub. That is
not a convenience — the `^insights-.*` default would not work:

* it is a *pattern*, and your entity is rarely named `insights-...`, so it
  matches nothing and the consumer joins the group and then reads no topic at
  all, which looks exactly like a hub with no traffic; and
* being a pattern, it makes the client request metadata for the **whole
  namespace**. How an entity-scoped key is answered there is undocumented; the
  nearest precedent (entity-level *OAuth*,
  [azure-event-hubs-for-kafka#159](https://github.com/Azure/azure-event-hubs-for-kafka/issues/159))
  had the broker closing the connection. Treat namespace-wide metadata as
  unsupported for such a credential rather than as something to rely on.

An explicit topic list still wins; if it does not include the `EntityPath`
hub, startup warns, because every fetch will then fail authorization. If you
want one deployment to consume *several* hubs, use a **namespace-level**
policy with **Listen** — that is the shape the regex default is built for.

The string is sent as the SASL password **verbatim**, `EntityPath` included.
Do not strip it: SAS scope follows the rule the key was created on, so
stripping cannot widen access, while an entity-level rule *name* is only
unique within its entity. If an entity-scoped string is genuinely refused, the
fix is a namespace-level policy, not an edited string.

**Several hubs.** How many *clients* that takes depends on how many
*credentials* it takes, because a Kafka connection authenticates exactly once:

| you have | you get |
|---|---|
| one namespace-scoped string (or managed identity) + `-azure-eventhub-topics=a,b` | **one** client, both hubs |
| *N* entity-scoped strings (`-azure-eventhub-connection-string-file=a,b`) | ***N*** clients, one hub each |
| *N* namespaces (`-azure-eventhub-namespace=ns1,ns2`, managed identity) | ***N*** clients, sharing the topic list |

So prefer a namespace-level policy with **Listen** when you can: one client,
one group, one set of offsets. Entity-scoped credentials force one client per
hub — that is a property of SAS, not of this agent. Each client owns its
offsets and its `/readyz` gate; with more than one, the gate names what it
consumes (`azure-eventhub[myns.servicebus.windows.net/azure]`), so an
unreadable hub is identified rather than masked by a sibling that polled
first. Two entries resolving to the same namespace *and* topics are refused as
a duplicate.

**Consumer groups span the namespace,** which is why the group name is not
always the one you configured. Event Hubs' Kafka groups are autocreated and
distinct from the AMQP ones (there is no need for `$Default` to exist), but
they are namespace-wide names — so several clients sharing one there are one
*group*. That is correct when they share a subscription (members split
partitions) and a trap when they do not: the group **leader** computes the
assignment from the union of the members' subscriptions using *its own*
metadata, and an entity-scoped credential is not authorized for its siblings'
hubs, so it sees no partitions for them and assigns none — starving them in a
way that looks exactly like a hub with no traffic.

So when a namespace has more than one client, each gets its own group,
`<group>.<topics>` (e.g. `$Default.azure`), logged at startup; a namespace
with a single client — and every namespace when there are several, since their
groups cannot collide — keeps the name verbatim. (Two clients of the *same*
namespace and topics never arise: that is the duplicate refused above.)
**One consequence worth planning for:** growing a namespace from one client to
several renames the first one's group, and a group's committed offsets *are*
the resume position — so that transition restarts consumption per
`-azure-start` rather than resuming where it left off.

**Records.** Each Event Hubs message is the `{"records":[...]}` envelope;
every record is classified individually, so logs and metrics may share a
hub. A **log** record exports with the record's verbatim JSON as the body,
severity from `level`, and `azure.category`, `azure.operation.name`,
`azure.result.type`/`.description` (scrubbed), `azure.correlation.id` and
`azure.tenant.id` as record attributes. `azure.category` comes from the
envelope's own `category` field and is distinct from the `azure.event_category`
that `-enrich` may stamp from an `eventCategory` key inside the body — related
concepts, different source fields, so both are kept. A **metric** record — Azure's
pre-aggregated window statistics — becomes **real OTLP gauge data points**,
one per present aggregation: `azure.<metricname>.count`/`.total`/`.minimum`/
`.maximum`/`.average`, with `azure.metric.timegrain` on the data point
(gauges because each value describes one closed timeGrain window — the shape
the widely-deployed Prometheus Azure exporters use; `sum_over_time` and
friends recover longer windows).

**Resource identity.** The ARM resource the record describes becomes the
OTLP resource: `cloud.provider=azure`, `cloud.resource_id` (verbatim),
`cloud.account.id` (subscription), `azure.resource_group.name`,
`azure.resource.type`, `azure.resource.name`, `cloud.region` — with
`service.name` = the resource name, `service.namespace` = the resource
group and `service.instance.id` = the lowercased resource ID, so Mimir
job/instance identity works out of the box and two same-named resources in
different groups never merge.

**Delivery** is at-least-once: offsets are committed only after the
collector (or the disk buffer) acknowledges **both** signals of a poll. A
crash or rebalance before the commit replays the poll (duplicates, never
loss); a payload the collector permanently rejects — and any undecodable
message — is counted and committed past, so one poison batch cannot wedge a
partition. The log path runs the whole shared chain: `logScrubbing` (before
anything copies from the body), `logAttributes`, `-enrich`, `logMetrics`,
`logs.rules`, Starlark transforms, `routing` and the disk buffer.

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
traceMetrics:  {dimensions: [...], buckets: [...]}    # see Agent: span metrics
traceSampling: {probability: 0.1, ...}                # see Agent: trace sampling
tailSampling:  {policies: [...], decisionWait: 5s}    # see Agent: tail sampling
serviceGraph:  {wait: 10s, dimensions: [...]}         # see Agent: service graph
serviceGraphShards: {endpoints: [...], self: ...}     # see Agent: service graph
routing:       {routes: [...]}                        # see Agent: routing
export:        {headers: {...}, logs: {...}, ...}     # see Per-signal destinations
```

The two `serviceGraph*` sections are read only by the trace tier
(`-service-graph`); every other section is ignored by a role that does not run
the pipeline it configures.

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

Per-source options:

- `namespaces` / `excludeNamespaces` (containerd sources) restrict collection
  by the pod's namespace, as `path.Match` globs; the denylist wins. They are
  read from the **CRI filename at discovery**, so a non-matching file is never
  opened, tracked or read:

  ```yaml
  logs:
    sources:
      - name: containers
        include: ["/var/log/containers/*.log"]
        containerd: true
        namespaces: ["team-*", "prod"]      # empty = all
        excludeNamespaces: ["team-scratch"]
  ```

- `selector` (containerd sources) restricts collection to pods whose **labels**
  match every `key: value`. Labels are only known once metadata resolves, so
  this is applied there — the file is tracked, but no data is ever read from it:

  ```yaml
        selector: {logging: "true"}
  ```

  Prefer both over a `logs.rules` drop for cost control: a rule saves egress
  but only after paying the read, parse and enrich; these skip the work.

- `ignoreOlder` skips files whose **mtime** is older than a Go duration, so a
  source pointed at a directory of retained history reads only what is recent.
  Like the namespace filters it applies at discovery — an ignored file is never
  opened, tracked or read:

  ```yaml
        ignoreOlder: 24h                    # unset/0 = read every matched file
  ```

  A file already being tailed, or one carrying a stored offset, is **never**
  ignored however stale it looks: dropping it would abandon the bytes it has
  not shipped and re-ingest the whole file if it were appended to again.

- `compressed` reads matched files as gzip, decompressing on the fly (files
  ending in `.gz` are detected automatically). Compressed files are treated as
  **archives** — read once to completion, not tailed — so, unlike plain
  tailing, pre-existing ones *are* ingested; scope `include` to avoid re-reading
  unwanted history. A partially-read archive resumes correctly across a restart.

- `pathAttributes` (plain sources) derives **per-file** resource attributes
  from the file's path — a build agent's diagnostic tree encodes job identity
  in directory names, and nothing on the line carries it (Promtail's `regex`
  stage over `filename`). Rules are evaluated once per file at resolve time,
  in order; each rule's regexp matches unanchored against the full path, a
  non-matching rule contributes nothing, and captures win over the source's
  static `attributes`:

  ```yaml
  logs:
    sources:
      - name: azdo-diag
        include: ["/var/log/host/azdo-diag/**/*.log"]
        pathAttributes:
          # Named capture groups become attributes keyed by their own name…
          - regexp: '/azdo-diag/(?P<azdo_agent>[^/]+)/'
          # …or map captures explicitly (dotted keys, numbered groups):
          - regexp: '/buildlogs/(?P<tl>[^/]+)/(.+)\.log$'
            attributes:
              azdo.timeline.id: tl
              azdo.display.name: "2"
  ```

  A bad regexp, a reference to a capture group that does not exist, or a rule
  that can produce nothing fails startup (`-check-config` catches it), and the
  section is **refused on containerd sources** — their path encodes pod
  identity that already arrives as metadata.

- `parseScript: true` (plain sources) runs the transforms file's `parse:`
  [hook](#hooks) on every line of the source, before scrubbing and the rest
  of the chain — for exotic formats no declarative rule can read. Like
  `pathAttributes` it is **refused on containerd sources**, whose per-line
  budget is allocation-pinned; the hook costs one Starlark call per line on
  the sources that opt in, and only there.

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
(lowercased) — so "drop debug logs" needs no per-app parsing config.

Selector escaping: `matchRegexp` values are **RE2 patterns passed to the
engine verbatim** — backslash is the regex escape, so `\d` is a digit class
and `\\` a literal backslash, exactly as in any Go regex. `match` values are
compared literally; a literal backslash or double quote may be spelled `\\`
or `\"` (one left-to-right pass; any other character after a backslash is
taken verbatim). The label half of a selector must be non-empty, and the
negated form is spelled `label!=value`:

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
keep rules; a matching line beyond the sampled fraction is dropped.

The same chain applies to **journald** entries (as does `logMetrics`), with the
same ordering: metrics observe every entry, the rules run after enrichment so
`__severity__` sees the enriched severity, and a dropped entry still advances
the journal cursor.

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
      maxCardinality: 5000          # cap on unique label sets (unset = default 10000, also the hard cap).
                                    # A histogram is one stored sample per label set (its buckets ride
                                    # along), and maxCardinality x buckets > 150000 is refused at startup.
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
      action: inc                   # set (default) | inc | dec | add | sub | min | max | avg | sum | count
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
  `avg`, `sum`, `count` (matching lines, no value needed). An
  aggregation emits its value on every export and keeps emitting it while no new
  value arrives; the first value after an export starts a fresh window (so `avg`
  is a per-window mean). The action set is deliberately closed: statistics
  derivable from these (stddev, range, …) belong in backend recording rules,
  which re-aggregate freely across windows.

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
password / token / access_key key-value pairs), `aws-key`, `private-key`,
`url-userinfo` (the password half of a `scheme://user:password@host`
connection string, which reaches logs through dial-failure messages and
config dumps where no key=value shape exists) — all six = `defaults` — plus
the opt-in-by-name `email` and `credit-card` (they redact legitimate content
too often to be defaults).

`secret-kv` keeps the key and separator so the line stays readable, and
redacts a QUOTED value to its closing quote, so a passphrase containing
spaces or commas does not survive as a tail. That includes a quote ESCAPED
inside it (`{"password":"he said \"hi\" ok"}` goes whole) and the
escaped-quote spelling a payload stringified into a JSON field takes
(`{"msg":"password=\"hunter2\" tail"}`, which used to emit
`password=[REDACTED]"hunter2\"` — a line that CLAIMS a redaction and still
carries the secret). Two shapes are **not** fully redacted, both visible in
the output: an UNQUOTED value containing one of the value class' own
delimiters (`&`, `,`, `;`, `}`, `]`, `)`, whitespace or a quote) is redacted
only up to that delimiter — quote the value, which every structured logger
already does — and more than ONE level of escaping is not decoded.
`url-userinfo` redacts to the LAST `@` in the token, so a DSN password
containing one (`svc:p@ss@db-1`) goes whole. Every built-in carries a cheap
prefilter, so the no-match hot path is a scan or two and zero allocations —
and `secret-kv`'s checks the assignment SHAPE, not just the keyword, because a
line admitted to the regex pays for the whole record (100 ms for a 1 MiB line,
on the single goroutine that tails every file on the node). Redactions count into
`kubescrape_log_scrubbed_total{pattern}`. An unknown builtin name, an
invalid regex, or a config with no patterns at all fails startup — a
scrubber that silently skips a pattern is a compliance bug.

## Agent: OTLP ingest

Opt-in with `-ingest`: the agent receives OTLP **logs and metrics** that apps
push to the node and enriches each resource with k8s attributes deduced from a
container ID or pod UID already on the data, forwarding through the same
exporter. Enrichment never overwrites an attribute the sender set.

**Traces are received by the trace tier**, not here (see [Agent: service
graph](#agent-service-graph)): pairing a service-graph edge and sampling a trace
as a unit both need every span of that trace in one process, which a per-node
receiver never has. This listener does not register the OTLP trace service or
`/v1/traces` at all, so a sender pointed here for traces gets Unimplemented /
404 — a named destination error rather than an ack for spans nothing will pair.

| Flag | Default | Description |
|---|---|---|
| `-ingest-grpc-endpoint` | `:4317` | OTLP/gRPC listen address; empty disables |
| `-ingest-http-endpoint` | `:4318` | OTLP/HTTP protobuf listen address (`/v1/logs`, `/v1/metrics`); gzip `Content-Encoding` accepted; empty disables |
| `-ingest-metrics-mode` | `auto` | `resource` (ID on the resource), `datapoint` (ID per point → split into per-object resources), or `auto` |
| (`-enrich`) | `true` | parse pushed log bodies with the same switch, filling only fields the sender left unset |
| `-ingest-peer-ip-fallback` | `false` | attribute telemetry whose resource carries **no** container id / pod uid to the pod owning the connection's source address (`GET /v1/pod-ips/{ip}`, live non-hostNetwork pods only). Opt-in: a proxy, a mesh sidecar or any NAT hop rewrites that address, and hostNetwork senders share the node IP and never resolve. Counted as `kubescrape_ingest_resources_total{outcome="peer_ip"}`. Read by the trace tier too |
| `-ingest-container-id-keys` | `container.id,k8s.container.id` | attribute keys inspected for a container ID |
| `-ingest-pod-uid-keys` | `k8s.pod.uid` | attribute keys inspected for a pod UID |
| `-ingest-metadata-wait` | `0` | how long a lookup may block for a not-yet-known object. A push's attribution lookups may block at most 4× this in TOTAL (the remainder proceed without waiting), so a payload naming many distinct unknown IDs cannot stack waits into a long in-flight-slot hold |
| `-ingest-max-in-flight` | `0` (= 32) | bound on pushes processed concurrently **across both transports**. Over it, senders are refused *retryably* rather than admitted into memory the node does not have |
| `-ingest-grpc-max-recv-bytes` | `0` (= 4 MiB) | cap on **one decoded gRPC message** (the counterpart of a collector's `max_recv_msg_size`); an over-cap push is refused, not truncated. Applies to the trace tier's application ports too; the OTLP/HTTP body cap stays 16 MiB. Raising it is a per-push memory grant on an unauthenticated listener — the buffer budget below scales with it |

A container ID resolves the exact container incarnation; a pod UID resolves
the pod.

**The operator's log cost levers reach pushed logs too**: after enrichment,
ingested log records run the same compiled `logs.rules` chain and feed the
same `logMetrics` set as the tailer, journald, events and Azure producers —
one config, one behavior, however the line arrived. Metrics observe every
record (before the rules), rules select on the enriched severity
(`__severity__`), a dropped record is removed before forwarding and counted
(`kubescrape_log_rules_dropped_total`) while the push is still acked — the
sender delivered it; the operator chose to drop it. A payload filtered to
nothing is acked without a send. Bodies are scrubbed first (`logScrubbing`,
structured bodies included), then enriched (`-enrich`) from ONE bounded text
view per body that metrics and rules share — a structured (map/slice) body
enriches from its JSON rendering, the same text the tailer would have seen,
and `__severity__` falls back to the OTLP severity-number band when a sender
set only the number. Because the listeners are unauthenticated, the
line-derived half is bounded: bodies whose text view exceeds 1 MiB (the
tailer never feeds longer lines either), resources wider than 64 attributes,
and resources past the first 256 of one push skip enrichment/observation —
counted in `kubescrape_ingest_log_chain_skipped_total{reason}`, with the
data itself always still forwarded. Duplicate resource keys (legal OTLP, a
hostile-sender shape) are deduped last-wins before metric binding. Outcomes count into `kubescrape_ingest_resources_total{outcome}`
(`enriched` / `unresolved` / `peer_ip` / `peer_ip_rejected`).

Both listeners are unauthenticated and node-local, and every in-flight request
holds its body plus the inflated pdata — on the same process that tails the
node's logs — so the count is bounded. A push arriving over the bound is not
queued (that would turn back-pressure into latency the sender cannot see) and
not dropped: it is refused with an answer the sender can act on, keeping the
payload where the retry logic lives.

* **HTTP**: `429 Too Many Requests` with `Retry-After: 1`.
* **gRPC**: `ResourceExhausted` **with a `RetryInfo` detail** (`retry_delay: 1s`).
  The status code alone reads as permanent to conformant senders — OTLP makes
  `ResourceExhausted` retryable only when `RetryInfo` is attached, and both the
  OTel SDK and the Collector drop the batch without it. `grpc.MaxConcurrentStreams`
  is set to the same value.

The count bounds *processing*, not *buffering*: the HTTP handlers read the whole
body before taking a slot (holding one across a trickled 16 MiB upload would let
a few senders shed everyone else for a `ReadTimeout`), and gRPC decodes the
message before the interceptor runs. A second, fixed bound — 64 MiB of raw
payload across both transports — covers that window, refused the same retryable
way: an HTTP body is charged as it is read, in 64 KiB steps — a declared
`Content-Length` is never credited up front, since the declaration is the
sender's claim, not a fact — and a gRPC push reserves `MaxRecvMsgSize` from
the moment its headers arrive until its message is decoded. It is not a flag:
the operator knob is the count above, which bounds the far more expensive
resource, while this one only has to keep an unauthenticated listener from
buffering without limit.

The per-push size caps are **4 MiB per gRPC message**
(`-ingest-grpc-max-recv-bytes` raises it; the buffer budget scales in step so
a single legal push always fits) and a fixed **16 MiB per HTTP body**. An
over-cap push is refused (`ResourceExhausted` / `413 Content Too Large`),
never truncated — and unlike an over-the-count refusal, retrying the same
batch cannot succeed, so a sender that hits the gRPC cap must ship smaller
batches (an SDK batch-processor setting), switch to OTLP/HTTP, or have the
cap raised to what a collector's `max_recv_msg_size` used to grant it.

Refusals count into `kubescrape_ingest_rejected_total`. A persistently non-zero
rate means the node cannot keep up with what is being pushed at it — raise the
bound (memory permitting), speed up the collector's acks (a slow collector holds
every slot for `-otlp-timeout`), or push less at this node.

## Agent: span metrics

The `traceMetrics` section tunes the RED metrics `-ingest-span-metrics` derives
from the spans the trace tier receives, exported on `-ingest-span-metrics-interval`
(default `1m`). Both the flag and this section belong on
that tier (`serviceGraph.spanMetrics` in the chart); aggregation runs on the
shard that OWNS each trace, so a span is counted exactly once however many
shards its push passed through:

```yaml
traceMetrics:
  namePrefix: traces.span.metrics   # .calls / .size / .duration
  buckets: [0.005, 0.05, 0.5, 5]    # duration histogram bounds, SECONDS
  dimensions: [http.route]          # extra span-then-resource attribute labels
  maxCardinality: 20000             # cap on distinct dimension tuples
  exemplars: true                   # per-bucket trace-id exemplars
  staleAfter: 15m                   # evict series unobserved for this long
```

`staleAfter` is what keeps `maxCardinality` from becoming a one-way latch: a
burst of one-off span names would otherwise fill the cap permanently and no
new service on the node would ever be measured again. Series whose dimensions
go unobserved that long are dropped at export time — the standard staleness
signal for a cumulative counter — and count into
`kubescrape_span_metrics_evicted_total`; a series that comes back is
re-created with a fresh start timestamp (the OTLP spelling of a counter
reset). Eviction only ever drops values a **delivered** export already
carried, so an export interval longer than `staleAfter`, or a failed export,
never loses observations. `"0"` disables eviction; a NEGATIVE value is refused
by `-check-config` rather than clamped, because clamping it would land on that
same `"0"` and disable the eviction the field was being set to configure. It is
parsed by the same code as `serviceGraph.staleAfter`, so the two agree.

## Agent: service graph

The **trace tier**: the workload that receives the cluster's OTLP traces,
enriches them, and derives service-graph **edge** metrics — one series per
caller→callee pair, with both sides' latency and the error count — by pairing
each request's CLIENT span with its SERVER span. Opt-in, and unlike every other
pipeline it is its own workload rather than part of the DaemonSet.

**Why its own tier.** The two halves of one request are emitted by two pods that
usually run on two different nodes, so a per-node receiver holds half of every
edge and cannot complete one no matter how the numbers are added up afterwards.
(RED span metrics are the opposite shape — each span is aggregated
independently, so cumulative counters simply sum — but they live here too,
because this is where the spans are.) So an application pushes to the tier's
Service, the shard that receives the push enriches and re-shards each span by
**trace ID** onto a ring, and the shard that owns that trace sees every span of
it: pairing, RED metrics, head sampling and the export all run there. This is
Grafana Tempo's distributor → metrics-generator split collapsed into one
workload, with Tempo's hash (FNV-1 32-bit over the trace id).

**Where applications point their SDKs:**

```sh
OTEL_EXPORTER_OTLP_ENDPOINT=http://kubescrape-traces.monitoring.svc:4318
OTEL_EXPORTER_OTLP_TRACES_PROTOCOL=http/protobuf
# or :4317 with the default grpc protocol
```

| Flag | Default | Description |
|---|---|---|
| `-service-graph` | `false` | run the trace tier |
| `-service-graph-ingest` | `true` | accept application OTLP traces |
| `-service-graph-ingest-grpc` | `:4317` | application OTLP/gRPC listen address; empty disables |
| `-service-graph-ingest-http` | `:4318` | application OTLP/HTTP protobuf listen address (`/v1/traces`); gzip accepted; empty disables |
| `-service-graph-listen` | `:4319` | the **internal** receiver: spans a sibling shard re-sharded here. Never one of the application ports |
| `-service-graph-http-listen` | (empty) | internal OTLP/HTTP receiver, only needed with `serviceGraphShards.protocol: http` |
| `-service-graph-token-file` | (empty) | shared bearer token authenticating the **internal** hop; re-read periodically, so rotating the Secret needs no restart. **Without it the binary refuses to open that listener** |
| `-service-graph-shards` | `0` | number of shards in the tier; must equal the StatefulSet's replicas. 0 or 1 means no internal hop |
| `-service-graph-endpoint` | (empty) | the tier's governing **headless** Service (`<sts>.<ns>.svc:<port>`), from which each shard's stable per-pod name `<sts>-<ordinal>.<service>.<ns>.svc:<port>` is derived |
| `-service-graph-shard-name` | `$POD_NAME` | this shard's own name in the ring, so traces it already owns take no hop |
| `-service-graph-interval` | `1m` | export interval for the edge metrics |

### Two listeners, and why

The **application** ports are unauthenticated. Every instrumented pod in the
cluster is a sender, and requiring a credential from each of them is not a
bargain most fleets can make; a NetworkPolicy
(`serviceGraph.ingest.allowFrom` in the chart) is the lever that scopes them.
What that exposes, plainly: any pod that can reach those ports may push
arbitrary spans, naming any `service.name` it likes, at whatever volume it
chooses — the same bargain every unauthenticated in-cluster OTLP collector
makes.

The **internal** port is authenticated, because what arrives there is treated as
final: already enriched, already routed to its owner. An unauthenticated one
would let anything that can reach the pod put unattributed spans straight into
the collector.

Which port a payload arrived on is what decides its treatment — never anything
the payload claims — so the two rules that keep the topology sound are
structural rather than checks that could be wrong: **an application push is
re-sharded exactly once**, and **a re-sharded payload is never re-sharded
again** (the internal path contains no resharder and no enricher at all). As a
second line, a re-sharded payload carries the resource attribute
`kubescrape.service_graph.forwarded`, and an application port that sees it
refuses the push **permanently** and counts
`kubescrape_service_graph_loops_blocked_total`. That is the one misconfiguration
worth a runtime guard: pointing the internal hop at an application port instead
of `:4319` would otherwise re-enrich (against a peer that is now a kubescrape
pod) and re-shard every span on every pass, without bound. The owner strips the
attribute before anything else runs, so it never reaches the collector.

### Enrichment happens at first receipt

The entry shard enriches; the owner never does. This is not an optimisation but
the only correct placement: the connection's source address names the *sender*
exactly once, on the hop the sender itself opened. On the internal hop the peer
is a sibling shard, so enriching there would attribute an application's traces
to a kubescrape pod — confidently, plausibly, and wrong on every span.

`-ingest-peer-ip-fallback` (off by default) is what makes that address matter at
all: it attributes a resource carrying no `container.id`/`k8s.pod.uid` to the pod
owning the source address. A ClusterIP normally preserves the client IP
pod-to-pod, but a service mesh that terminates the connection, an ingress, or
any NAT hop replaces it. So the tier adds an explicit guard: a resolution that
names **this tier's own workload** — the same pod, or a sibling replica — is
refused and counted as
`kubescrape_ingest_resources_total{outcome="peer_ip_rejected"}`, with a
throttled log line naming the address and the pod it resolved to. Those
resources stay unenriched, which is a visible gap rather than a confident lie.
The identity comes from the self-metadata lookup the process already runs for
`-self-attributes`; before it lands nothing is refused, because inventing an
answer from a lookup that has not happened is the same mistake reversed. The
dependable alternative is for senders to set a resource-level `container.id`,
which every SDK's container detector does.

### The internal hop cannot shed

The shard that received a push holds the **only** copy of those spans, so the
hop is synchronous and failable: a failed send fails the application's push and
the sender's retry is the recovery. There is no queue and no durability here —
`-buffer-dir` is deliberately not in this path, because a replayed hop would
double-count the owner's cumulative edge and RED counters. The bound on
concurrent hops is the entry listener's `-ingest-max-in-flight` semaphore.

A fan-out that fails part-way is at-least-once: the split is a pure function of
the trace ID, so the sender's retry re-splits identically and the owners that
already accepted see the batch twice. That is safe because both taps count only
after a **successful** export, so a re-delivered batch that was never counted
costs nothing. Failures count into `kubescrape_service_graph_sends_failed_total`
and move in step with the senders' own error rate; the split itself is visible in
`kubescrape_service_graph_spans_forwarded_total` (handed to another shard) and
`..._spans_local_total` (already ours — roughly 1/N on an N-shard tier).

The endpoint for the hop names the **headless** Service, never a ClusterIP: a
load-balanced destination round-robins, which is exactly what the hop exists to
undo. For the same reason the shard count is part of the ring's *definition*
rather than a local tuning knob: two shards disagreeing about it route a
request's two halves to two different owners, silently, with both reporting
success. Scaling the tier moves ~1/N of the traces to a different owner and
loses only the half-edges in flight at that moment.

### The emitted series

**Grafana-Service-Graph-compatible on purpose** — Grafana's Service Graph view
queries these exact names and labels, so a better-reading name would render in
nothing:

| Metric | Type | Description |
|---|---|---|
| `traces_service_graph_request_total` | counter | requests on the edge |
| `traces_service_graph_request_failed_total` | counter | the subset where either side reported an error (emitted at zero too, so the error ratio is defined before the first failure) |
| `traces_service_graph_request_server_seconds` | histogram | duration as the **server** measured it |
| `traces_service_graph_request_client_seconds` | histogram | duration as the **client** measured it |

Labels: `client` and `server` (the two `service.name`s), `connection_type`
(empty for a plain service-to-service call, else `messaging_system`,
`database` or `virtual_node`), `virtual_node` (`client`/`server`, only when
that side was synthesized) and any configured `dimensions`, which appear
twice — `client_<dim>` and `server_<dim>` — resolved from whichever side
carried the attribute. The two durations are separate histograms, not one:
they are measured by different processes with unsynchronised clocks, and their
difference (network plus queue time) is the operator's to take, not ours to
assert.

Tuning is the `serviceGraph` section of the agent config, mounted on the tier:

```yaml
serviceGraph:
  wait: 10s              # how long a half-edge waits for its partner
  maxItems: 10000        # half-edges held at once
  maxCardinality: 20000  # distinct edge series
  staleAfter: 15m        # evict edges unobserved for this long ("0" disables)
  histogramBuckets: [0.1, 0.2, 0.4, 0.8, 1.6, 3.2, 6.4, 12.8]   # SECONDS
  exemplars: true        # default: trace-id exemplars on both histograms
  dimensions: [http.route]
  virtualNodePeerAttributes: [peer.service, db.name, db.system]  # default
```

`wait` and `staleAfter` are Go durations written as **strings** (`10s`, `15m`,
`"0"`), like `traceMetrics.staleAfter` and `traceSampling.keepSlowerThan`.
Empty takes the default; `staleAfter: "0"` disables eviction outright. A value
that does not parse is refused by `-check-config` with the field and the value
named, rather than silently taken as a default.

A `dimensions` key needs no counterpart anywhere: the spans reaching the pairing
store are the ones the sender pushed, whole, so adding a key starts resolving it
on the next export.

Every one of the three bounds trades completeness for memory, and each has a
counter that moves when it binds — an edge missing from the graph looks exactly
like a call that never happened, so none of them fails silently:

* `wait` — longer pairs more slow requests but holds more half-edges. A rising
  `kubescrape_service_graph_expired_total` means it is shorter than the real
  client-to-server delivery gap (or that the two halves are reaching different
  shards). A non-zero baseline is normal: an uninstrumented callee's client
  half expires by design.
* `maxItems` — the pairing store's cap. Over it, spans are dropped
  (`kubescrape_service_graph_store_full_total`, counted per span: the partner
  will expire unpaired too, so one lost request moves both counters).
* `maxCardinality` — the emitted-series cap. A new edge over it is dropped
  (`kubescrape_service_graph_dropped_total`); existing edges keep reporting,
  because these are cumulative series.
* `staleAfter` — what keeps `maxCardinality` from being a one-way latch: without
  it one burst of short-lived services blinds the graph permanently. Evicted
  series count into `kubescrape_service_graph_evicted_total`. `"0"` turns
  eviction off and keeps every edge for the process' life.

`exemplars` (default on) attaches one exemplar per latency bucket to each
duration histogram — the link from "this call is slow" to the trace showing
why. The trace id is the same on both halves by construction (it is half the
pairing key), so there is nothing to choose there; the **span** id is each
half's own, so the client-latency exemplar points at the span that measured the
client latency and the server's at the server's. Exemplars are cleared only
after a DELIVERED export, so a failed send keeps them for the retry, and a span
with no trace id gets none rather than a link that resolves to nothing.

The `serviceGraphShards` section covers what the flags do not: explicit
`endpoints` (a tier outside Kubernetes, or a test), `self`, `protocol`,
`headers`, `caFile`/`insecureSkipVerify` for the hop, and `tokensPerShard` (part
of the ring's definition — identical on every shard or nothing pairs). `port`
defaults to **4319**, the internal receiver's own default listen port. With
`protocol: http` the derived per-shard URL takes the `https://` scheme when TLS
is asked for in any form — `caFile`, `insecureSkipVerify: true` or an explicit
`insecure: false` — because for HTTP the scheme *is* the TLS decision; explicit
`endpoints` carry their own scheme instead.

**The honest limitation: an edge needs BOTH halves.** A callee that is not
instrumented produces no server span, so no true edge can form. Those calls
appear as **virtual nodes** instead — the far side is named from the first
matching `virtualNodePeerAttributes` key on the client span (`peer.service`,
`db.name`, `db.system` by default).

> **On current semconv, extend this list.** The default is Tempo's verbatim, and
> as of semconv v1.44.0 all three of its keys are deprecated upstream:
> `db.name` → `db.namespace` (v1.26.0), `db.system` → `db.system.name`
> (v1.30.0) and `peer.service` → `service.peer.name` (v1.39.0). The default
> stays as it is because it names the `server` label existing Tempo dashboards
> select on. The database pair matters least — `connection_type` is classified
> from both spellings either way — but a callee named *only* via
> `service.peer.name` mints **no virtual node at all**, so an SDK on current
> conventions loses the far side of every uninstrumented call. Set:
>
> ```yaml
> serviceGraph:
>   virtualNodePeerAttributes: [peer.service, service.peer.name, db.name, db.system, db.namespace, db.system.name]
> ```
>
> Listing both spellings is safe: the keys are tried in order and the first
> present one wins, so a span carrying the old key is unaffected.

It works in both directions: an expired
**server** half carrying such a key names its uninstrumented *caller* (an
ingress, a browser, an external client) and the edge is labelled
`virtual_node="client"`. A half naming none of those keys is counted unpaired
and mints no node at all — unlike Tempo, which synthesizes a literal `user`
client for an unpaired root server span. The edge carries
`connection_type="virtual_node"` and only the client-side duration. A client
that names nothing at all produces no edge whatsoever. The graph is therefore a
map of what is instrumented, not of what the cluster does; and spans that never
reach the tier (traces pushed straight to a collector) are outside it entirely.

**What the topology costs.** The tier is a hard, cluster-wide dependency for
traces: while it is down or unreachable every application's trace export fails,
with no per-node fallback. Each span crosses the network twice at full fidelity
(application → entry shard, then entry → owner for (N−1)/N of them). And the
tier is shared, so one noisy service's spans occupy the same shards as everyone
else's, bounded only by `-ingest-max-in-flight` per pod.

## Agent: trace sampling

The `traceSampling` section samples received spans before export. It runs on the
trace tier (it needs `-service-graph`):

```yaml
traceSampling:
  probability: 0.1        # keep this fraction of traces (unset/1 keeps all)
  keepErrors: true        # default: status-ERROR spans always kept
  keepSlowerThan: 2s      # spans at least this slow always kept (0 disables)
  maxSpansPerSecond: 500  # hard cap after sampling (0 = uncapped)
```

The probability decision is **consistent per trace ID** (a hash of the ID
against the threshold): all spans of a trace sample identically on every shard
running the same config, and a sender's retry re-samples identically.
`keepErrors` and `keepSlowerThan` bypass the probability decision but not
the rate cap (a cap that can be exceeded is not a cap). Dropped spans count
into `kubescrape_trace_spans_dropped_total{reason="probability"|"rate"}`; a
payload sampled down to nothing is acked without a send. The sampler sits below
both the span-metrics tap and edge pairing, so the `-ingest-span-metrics` RED
metrics and the service graph are derived from 100% of spans while only the
sampled subset ships.

## Agent: tail sampling

`traceSampling` above decides each span as it arrives, from the span alone.
`tailSampling` decides each **trace as a whole**, after holding its spans long
enough for them to arrive — which is what makes "keep every trace that contains
an error", "keep every trace slower than 500 ms" and "keep 5% of the rest"
expressible. It runs on the trace tier, where re-sharding by trace id has already
put every span of a trace in one process; it is off unless it has policies.

```yaml
tailSampling:
  decisionWait: 5s          # hold a trace this long from its FIRST span
  maxTraces: 100000         # traces held at once
  maxSpansPerTrace: 1000    # spans held for one trace
  maxSpans: 200000          # TOTAL spans held — the bound that sets the memory
  decisionCacheSize: 100000 # verdicts remembered for late spans
  decisionCacheTTL: 1m      # for how long
  policies:                 # ORDERED, first match wins
    - name: exclude-health-checks
      type: stringAttribute
      stringAttribute: {key: http.route, values: ["^/healthz$"], enabledRegexMatching: true, invertMatch: true}
    - name: errors
      type: statusCode
      statusCode: {statusCodes: [ERROR]}
    - name: slow
      type: latency
      latency: {threshold: 500ms, upper: 30s}
    - name: baseline
      type: probabilistic
      probabilistic: {samplingPercentage: 5}
```

Every duration is a Go duration **string** (`5s`, `1m`, `500ms`), like
`serviceGraph.wait` and `traceSampling.keepSlowerThan`.

### Policies

The policy set and semantics are the OpenTelemetry Collector's
`tailsamplingprocessor` — `alwaysSample`, `latency`, `statusCode`,
`stringAttribute`, `numericAttribute`, `booleanAttribute`, `probabilistic`,
`rateLimiting`, `and`, `composite` — plus one addition of this repo's,
`script`, whose body is the transforms file's `sample:` [hook](#hooks) and
which is a policy type like any other in the same list. They are spelled in this repo's camelCase, so a
Collector policy list is *transcribed* key for key rather than pasted
(`string_attribute` → `stringAttribute`, `threshold_ms: 500` →
`threshold: "500ms"`). A leftover snake_case key is a strict-decode failure, not
a silently ignored field.

Two deliberate differences from the Collector:

* **First match wins, in configured order** — rather than evaluating everything
  and OR-ing it. An OR has no attribution, and the deciding policy is a metric
  label (`kubescrape_tail_sampling_traces_total{policy}`): under an OR the answer
  to "why was this trace kept" is "some subset of them".
* **`invertMatch` vetoes on a match and abstains otherwise** — rather than
  sampling everything it does not match, which in the Collector makes an
  exclusion rule silently enable 100% sampling for everything else. Write
  exclusions first, sampling rules after. The Collector's reading is one line
  away: append a final `type: alwaysSample` policy.

A policy never errors: anything that can fail (regexes, durations, rate
allocations) fails at `-check-config`, and a policy that cannot form an opinion
at decision time (a missing attribute, a trace with no timestamps) abstains and
the next one decides.

### What it costs, and what a restart costs

**Spans are acked to their sender before their trace is decided.** They must be:
holding the push open for the decision window would pin one of the receiver's
in-flight slots for five seconds and stall every application in the cluster. The
consequence is the one departure in kubescrape from ack-after-delivery — spans
that are buffered but **not yet decided** are lost if the shard is hard-killed
(SIGKILL, OOM, node failure). No sender still holds them.

The exposure is bounded and visible:

* by **time** — at most `decisionWait` of received spans;
* by **size** — at most `maxSpans`, whatever the rate;
* by **shutdown** — a graceful stop (SIGTERM, a rolling update, a drain) decides
  every buffered trace immediately and exports the keeps before the exporter
  closes, so a normal restart loses nothing;
* and it is **observable before it is spent**:
  `kubescrape_tail_sampling_buffered_spans` and
  `kubescrape_tail_sampling_buffered_traces` are exactly what a hard kill would
  lose at that instant.

### Decided traces are durable with `-buffer-dir`

Once a trace has been **decided**, the ack-first argument no longer applies: its
sender was told the spans had landed seconds ago and holds nothing. So a decided
keep is **spooled to disk** when the tier runs with `-buffer-dir`, exactly like a
log or metric batch — a collector outage becomes a backlog that survives a
restart, not `{outcome="lost"}`.

This is a deliberate exception to the rule that the disk buffer passes traces
through unbuffered, and it is made per payload rather than per signal. The same
exporter also carries plain forwarded traces from the tier's application
listener, and those must keep passing through: the pushing SDK still holds them
and retries, and spooling would ack that sender for data that has not shipped.
A third spool directory (`traces/`) is opened only on a workload where tail
sampling actually runs; everywhere else nothing marks a trace payload and the
behaviour is unchanged.

Without `-buffer-dir`, a decided trace whose export fails is retried a few times
and then dropped and counted
(`kubescrape_tail_sampling_spans_total{outcome="lost"}`) — by then nobody else
holds it either. With it, that counter moves only if the spool itself refuses the
payload (full, or a failed fsync).

Either way there is one further exception: a span arriving for an
already-decided trace is forwarded on the receiving goroutine, and a failure
there *fails the push*, so the sender's retry recovers it (at the price of the
still-buffering spans in that payload being buffered twice — the same
at-least-once trade the re-shard hop makes).

### Memory

The sizing rule, in one line:

> **`maxSpans` × 1 KiB must fit in a quarter of the pod's memory limit.**

At a 1 GiB limit that is ~262 000 spans; the default `maxSpans: 200000` is about
200 MiB of spans. The per-span figure is measured: a minimal two-attribute span
is ~365 B, a realistic one ~1 KiB, plus one resource copy per pushed payload per
trace (~320 B with a bare `service.name`, ~1040 B with the attribute set the
tier's enricher stamps). The quarter leaves room for the pairing store, the span
metrics, the exporter and Go's heap slack.

**It is checked at startup**, against the container's cgroup memory limit (or the
host's RAM when the pod is uncapped):

* an unset `maxSpans` is **lowered** to what the limit affords, with a warning
  naming the arithmetic;
* an explicit `maxSpans` above the budget share is honoured with a warning;
* an explicit `maxSpans` whose spans alone would need the **whole** limit is
  **refused** — that config reaches its own ceiling only by being OOM-killed
  first, and an OOM loses every buffered span at once.

That refusal exists because the loss mode here is self-correlated: the likeliest
hard kill of this workload is the OOM its own buffer causes, so raising
`maxSpans` to avoid early decisions raises the odds of the event that loses
everything buffered. Early decisions are the relief valve; the OOM is the
failure.

The arithmetic to do before enabling it: a shard receiving *R* spans/second holds
`R × decisionWait` spans. At **50 000 spans/s** with a 5 s window that is
**250 000 spans (~250 MiB)** — above the default, so the ceiling would bind and
the oldest traces would be decided about a second early. If that exceeds the
pod's budget, the answer is **more shards** (the ring divides *R* by the shard
count), not a bigger number.

At every bound the **oldest** trace is *decided early* rather than evicted
unjudged — the policy engine treats a partial trace as a lower bound, so an early
decision degrades gracefully (a slow trace can be missed, a fast one is never
invented) where a blind eviction loses the trace including whatever it was about
to reveal. Each is counted separately with the bound that caused it:
`kubescrape_tail_sampling_early_decisions_total{reason}` —
`spans_per_trace`, `max_traces`, `max_spans` or `shutdown`.

### Late spans

A span whose trace has already been decided follows that verdict from the
decision cache — forwarded immediately on a keep, dropped on a drop
(`kubescrape_tail_sampling_late_spans_total{outcome}`). Past `decisionCacheTTL`
the verdict no longer applies and the span starts a **fresh window**, which may
decide that trace a second time; every policy but `rateLimiting` and `composite`
answers identically.

Those two spend a spans/second budget, and a re-decision must not spend it twice
— a trace's own stragglers would otherwise shrink the budget available to
genuinely new traces. So the cache entry has **two lifetimes**: the *verdict*
expires at `decisionCacheTTL` (a straggler later than that really is a new
trace), while the record that this trace was decided **at all** lives until the
entry is evicted by `decisionCacheSize`. A re-decision within that window checks
the budget instead of charging it.

Eviction under the size cap is therefore the one thing that still double-charges,
and it is exactly what `kubescrape_tail_sampling_cache_evictions_total` counts —
only for entries whose verdict was still live, since reclaiming an expired
tombstone is the cache working. If it moves, `decisionCacheSize` is too small for
the arrival pattern. Note that the cache now fills to its cap and stays there
(~100 B an entry, so the 100000 default is ~10 MB at full occupancy) rather than
also draining by age.

### Composition

Tail sampling is the **last** stage above the exporter:

```
pair the edge -> RED metrics -> head sample (traceSampling) -> tail sample -> export
```

Edge pairing and the span metrics count *requests*, so they see 100% of spans;
only the sampled subset ships. The two samplers **nest** rather than compound:
both hash the trace id with the same unsalted hash against the same threshold
arithmetic, so `probabilistic: {samplingPercentage: 50}` keeps exactly the traces
`traceSampling: {probability: 0.5}` already passed, instead of halving them
again.

**The supported combination**, spelled out, because only part of `traceSampling`
is safe below a tail sampler:

| `traceSampling` field | Below `tailSampling` |
|---|---|
| `probability` | **safe** — the two nest exactly (same unsalted trace-id hash, same threshold) |
| `maxSpansPerSecond` | **safe enough** — an overload valve; when it bites it truncates traces, and the tail sampler judges the truncation |
| `keepErrors` | **unsafe** — per **span** |
| `keepSlowerThan` | **unsafe** — per **span** |

The guard rails rescue individual spans of traces the probability already
dropped, so the tail sampler is handed a trace that is only its error (or slow)
spans and judges *that* as if it were the trace — latency reads a lower bound, an
inverted attribute exclusion can miss the span that would have vetoed — and may
then export it, which is a trace that never existed.

Setting both sections therefore emits a **startup warning** (and a
`-check-config` one) naming the offending fields. It is a warning rather than a
refusal because `keepErrors` defaults to *on*: refusing would reject
`traceSampling: {probability: 0.1}` next to any `tailSampling` section — the most
natural composition there is — and would make the same effective config legal or
illegal depending on whether the operator spelled the default out. The fix is one
line: set `keepErrors: false`, drop `keepSlowerThan`, and express the same intent
as tail policies (`statusCode: {statusCodes: [ERROR]}`, `latency`), which judge
whole traces and are strictly better at it.

## Agent: metrics scraping

| Flag | Default | Description |
|---|---|---|
| `-scrape-interval` | `30s` | one cycle scrapes every target of this node |
| `-scrape-timeout` | `15s` | per target. A non-positive value does not mean "no timeout": it *is* each request's context budget, so it would expire before the request went out and every target plus both kubelet scrapes would fail with `context deadline exceeded`. Passing one is **refused at startup**, `-check-config` included — there is no spelling of "unbounded" here |
| `-scrape-concurrency` | `4` | concurrent target scrapes |
| `-metrics-batch-size` | `10000` | export chunk size in data points — a 100k-series target is exported in ten chunks and never held in memory |
| `-metrics-batch-bytes` | `3145728` | also flush a chunk once its estimated encoded size reaches this (`0` = size only). The collector's gRPC receive limit applies to the **decompressed** message (4 MiB by default), which a label-rich target (kube-state-metrics, Istio) can exceed well before the point limit — every export of that target would then fail, so this bound is what keeps a chunk deliverable |
| `-scrape-max-samples` | `0` | abort a single scrape beyond this many samples (0 = unlimited) |
| `-scrape-exemplars` | `false` | negotiate OpenMetrics and attach exemplars to counter and histogram points (`trace_id`/`span_id` map to OTLP trace/span fields) |
| `-scrape-health-metrics` | `true` | export synthetic `up`, `scrape_duration_seconds` and `scrape_samples_scraped` gauges per target after each cycle |
| `-scrape-native-histograms` | `false` | offer the Prometheus **protobuf exposition** to annotation/monitor targets — splitter-backed ones included — and convert native histograms to OTLP **exponential histogram** points (a split rule routes native points through the same groupBy/enrichment machinery as every other kind). A family carrying both native and classic data uses the native representation; custom-bucket histograms (schema −53) fall back to classic buckets; targets that ignore the Accept header keep serving text (the parse mode follows the response Content-Type) |

Series filters and target splitters live in the `metrics` section of `-config`
([below](#metrics-config)).

Histograms and summaries are converted to proper OTLP Histogram/Summary
points (de-cumulated buckets, explicit bounds, quantiles); counters become
cumulative monotonic sums.

Per-target outcomes — pipeline, URL, source, monitor, up, error, duration,
samples, scrape time — are served on `GET /debug/targets` (failures first,
then pending, then by URL): the human-readable counterpart of the health
gauges. The snapshot **merges across cycles** rather than showing only the
last one: per-target cadences mean a cycle scrapes only what was due, so a
long-interval target keeps its LAST outcome (`scraped` says how old it is), a
discovered-but-never-scraped target appears as `"pending": true` instead of
not at all, and a target only disappears when a *successful* listing no
longer contains it. Targets derived from monitor endpoints may carry
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
          elif m.type == "sum":
              for p in m.datapoints:
                  p.attributes["pod_name"] = None    # strip a high-cardinality label
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
  `drop()`; metrics expose `name`, `type` (`gauge`/`sum`/`histogram`/
  `exponential_histogram`/`summary`), `unit`, `description`, `resource`,
  `datapoints` and `drop()`. Each data point exposes `attributes`, `value`
  (`None` for the bucketed kinds, whose value is a distribution) and `drop()`
  — that is where a metric's cardinality lives, so dropping one label or one
  point does not cost the whole metric. Mutations are in place; a script pays
  only for the fields it touches (~1µs per touched record); dropped elements
  and emptied groups are pruned after the run (a metric whose points are all
  dropped goes with them).
* **Attributes** are a dict-like view: `attrs["k"]` reads (missing keys are
  `None`), `attrs["k"] = v` writes, and `attrs["k"] = None` deletes.
* **Hot reload**: the file is watched (fsnotify on its directory — mount the
  ConfigMap **as a directory, not `subPath`**, or updates never arrive) with
  a 30s poll fallback. Reloads compile-then-commit: a broken edit keeps the
  last good program running (`kubescrape_transform_reloads_total{outcome}`),
  while a compile failure at startup is fatal. The active program's content
  hash is on `GET /debug/transforms`, so per-node convergence after a
  reload is checkable.
* **Safety**: Starlark is hermetic (no I/O, no imports, no clock) and every
  run is bounded four ways, so a pathological script errors out
  (`kubescrape_transform_errors_total{signal}`, the batch is not exported
  and the producer's usual retry applies) instead of wedging an export
  goroutine or killing the process. The bounds are a **step limit**
  (10,000,000 instructions, ~130 ms of pure looping), a **wall clock** (2s),
  **per-value caps** (1Mi elements per sequence, 16 MiB per string, 1Mi bits
  per integer) and a **cumulative allocation budget** (128 MiB per
  invocation). The last three exist because the step limit counts *bytecode
  instructions*, and a Go-implemented builtin does unbounded work for one
  step: `list(range(1<<26))` is eleven steps and a gigabyte, and
  `[0] * ((1<<30)-1)` is fifteen steps and seventeen. So the amplifying
  builtins are shadowed by bounded wrappers (`range`, `list`, `tuple`,
  `sorted`, `reversed`, `set`, `dict`, `enumerate`, `zip`, `str`, `repr`,
  `bytes`, plus `re.findall`/`re.replace`), and `*` and `+` — operators, with
  no builtin to shadow — are rewritten into guarded calls between parse and
  resolve. **All of it applies to the compile-time evaluation of module-level
  code too**, which is the path that mattered: a module-level allocation used
  to OOM-kill every agent in the fleet from one ConfigMap edit and then
  CrashLoop them permanently, because startup re-evaluates the module and
  dies again before the last-good-program machinery can help. A refusal is
  now an ordinary config error, which that machinery already handles.

### Builtins

Every script (batch transforms and hooks alike) compiles against a tiny
predeclared environment:

* **`re`** — RE2 with a bounded compiled-pattern cache:
  `re.match(pat, s)` (bool, unanchored — anchor with `^$` yourself),
  `re.find(pat, s)` (first match or `None`), `re.findall(pat, s)`,
  `re.groups(pat, s)` (`[whole, group1, …]` or `None`) and
  `re.replace(pat, repl, s)` (Go `$1`/`${name}` references). A bad pattern
  is a script error like any other.
* **`log(msg)`** — a throttled (1/s per script) line into the agent log, for
  debugging a predicate without flooding the agent's own stream.

### Extended fields and verbs

Beyond the fields above: log records also expose `time_unix_nano` and
`observed_time_unix_nano` (read/write), `trace_id`/`span_id` (read-only hex,
`None` when zero) and `scope_name`; spans also expose `kind`
(`server`/`client`/…), `duration_ms`, `status_message` and
`trace_id`/`span_id`. Two verbs exist on every item:

* **`r.route("name")`** — send this item's whole RESOURCE group to the named
  `routing` route instead of matching namespaces (a reserved attribute the
  router honors first and strips before anything is sent; an unknown name
  warns and falls to the default chain). Scripts could always steer routing
  by rewriting `k8s.namespace.name`; this is the sanctioned spelling. The
  marker is reserved to scripts: a copy arriving on the wire is stripped at
  first receipt on the application-facing ingest listeners — as is the
  transform drop marker, whose presence would otherwise read as an
  operator-intended drop — counted
  `kubescrape_ingest_reserved_stripped_total{key}`.
* **`r.emit_metric(name, value, labels={})`** — one observation into a
  metric **declared in `logMetrics`** (declaration is where the type,
  buckets and cardinality cap live), grouped under the item's resource. An
  undeclared name is a script error. Retries re-run scripts, so a transient
  export failure re-emits — the same at-least-once every producer's metrics
  already have.

### Hooks

Four more optional sections in the same hot-reloaded file put scripts at
other decision points. Each defines its own function, and each **fails
open** — a script error degrades to "the hook did nothing" (counted in
`kubescrape_transform_errors_total{signal}`, warned throttled), never to
data loss:

```yaml
ingest: |                  # per pushed RESOURCE, before enrichment
  def admit(resource):     # False removes it (counted in
      return resource["team"] != "banned"   # kubescrape_ingest_admission_rejected_total)
targets: |                 # per fetched scrape target, once per cycle
  def target(t):           # t.url/.path/.namespace/.pod/.labels/.source/.monitor
      if t.labels["scrape-tier"] == "none":
          t.drop()
sample: |                  # the tailSampling `type: script` policy body
  def decide(trace):       # True samples, False drops, None abstains
      for s in trace.spans:
          if s.attributes["retain"] == "always": return True
      return None
parse: |                   # per line of plain sources flagged parseScript
  def parse(line):         # None = leave the line alone
      if line.startswith("<log>"):
          return {"body": line[5:], "severity_text": "warn"}
```

The admission hook is the operator's **per-sender policy** on listeners
nothing authenticates — the honest mitigation for a sender minting resources
to latch a cardinality cap, which built-in bounds can only slow. The target
hook is full relabel-power (drop, rewrite `path`) without growing the
declarative config. The sample policy plugs into the `tailSampling` policy
list as `type: script` (refused at config time if this section is missing).
A later hot reload that REMOVES the section cannot be refused the same way —
the reload sees only the transforms file, never `tailSampling` — so the
policy abstains on every decision from then on (traces fall through to the
next policy, or to the default drop). That is not silent: each abstaining
decision counts `kubescrape_transform_errors_total{signal="sample"}` and a
throttled warning names the remedy (restore the section, or drop the
`type: script` policy).
The parse hook runs **only** on plain sources that opt in with
`parseScript: true` — one Starlark call per line on those sources alone.

### Deliberate refusals

Two hook points and two script capabilities were considered and refused; they
are recorded here so the
next person to want one finds the reasoning rather than an accident:

* **No per-line hook on the containerd tail path, and no per-sample hook in
  the Prometheus scraper.** Both paths are allocation-pinned (0 allocs/op,
  enforced by build-failing tests) and sized for 100k+ series and full-node
  log volume; a ~1µs Starlark call per line/sample is 5-50x the entire
  current per-item cost. Everything those hooks could do is expressible at
  the batch seam, in `logs.rules`/`metrics.pipelines`, or — for exotic plain
  files — in the opt-in per-source `parse` hook, which is per-source
  precisely so the containerd hot path never pays it.
* **No mutable cross-batch script state.** Producers re-run scripts on
  retry (that is why unmarked payloads are transformed on a copy), so
  persistent state would make every retry a correctness question; counters
  belong in `emit_metric`, which inherits the producers' documented
  at-least-once semantics instead of inventing new ones.
* **No I/O, network, or clock builtins.** Hermeticity is what makes a
  hot-reloaded, operator-edited script safe to run inside the export path;
  a script that could block on the network would hold the tailer's single
  sweep goroutine.
* **`+=`/`*=` on a target that contains a call.** Bounding the `+` and `*`
  operators (they are the only amplifiers with no builtin to shadow) rewrites
  `t += x` into `t = <guard>(t, x)`, so the target appears twice and is
  evaluated twice. That is free of consequence for a name, a field
  (`r.body += " tag"`) and an index (`r.attributes["n"] += 1`, `a[i] *= 2`) —
  every read a script can perform on a host object is a pure read — but not
  for `d[f()] += 1`, where `f()` would run twice. Assign the call's result to
  a name first: `k = f()`, then `d[k] += 1`. The refusal is a config error
  naming the call's position and that spelling. Everything else about
  augmented assignment is unchanged, including the in-place list extend:
  `d["l"] += [2]` extends the list the dict already holds, exactly as starlark
  does. One difference from plain starlark, reachable only when the right-hand
  side MUTATES a container the target reads its key from: `d[a[0]] += f()`
  where `f` writes `a[0]` stores under the NEW key, because the bounded form is
  exactly the hand-written `d[a[0]] = d[a[0]] + f()` rather than starlark's
  evaluate-the-address-once `+=`.
* **The three guard identifiers (dunder-prefixed `kubescrape` names ending in
  `add`, `iadd` and `mul`) are reserved as NAMES.** The rewritten operators
  call them, and a global, def, parameter or loop variable of such a name
  resolves before the predeclared guard and would quietly unbound every `*`
  and `+` in the file. Only as a name: the same spelling used as an attribute
  selector, as a keyword-argument label, as a string literal or as a dict key
  can neither bind nor shadow, and is left alone — a fatal compile error over
  an incidental spelling would CrashLoop a fleet for nothing.
* **`secret-kv` over-redacts a quoted keyword followed by `:` or `=`.** In
  RE2 that is indistinguishable from an assignment, so ordinary prose written
  that way loses its next token: `{"msg":"unknown field \"token\": ignoring"}`
  redacts `ignoring`. The damage is bounded to one token (the unquoted value
  class stops at the first delimiter), and a quoted keyword with NO assignment
  after it (`missing key \"api_key\" in config`) is untouched. Not refused:
  over-redaction is far safer than under-redaction in a compliance control.
  Pinned by `TestKnownOverRedactionsAreAccepted`, so a future change to the
  pattern re-opens the decision instead of silently widening it.

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
forwarded untouched (no copy). An endpoint-less route inherits the whole
merged `-otlp-*`/`export:` base (it IS the default destination, reached with
extra headers). A route naming its **own** endpoint inherits only the
transport settings (protocol, compression, retries, timeout, send-size cap),
the merged headers, `-otlp-tls-insecure-skip-verify` and — unless the route
sets its own `insecure` — the base's plaintext-vs-TLS choice; it does **not**
inherit the base credentials (`-otlp-bearer-token-file`, the mTLS client
pair, the CA bundle), because those authenticate this deployment to *its*
collector and a route is a different destination — set the route's own
`bearerTokenFile`/`clientCertFile`/`clientKeyFile`/`caFile` instead.
Delivery is at-least-once per destination: a failed
destination fails the whole export and the producer's retry re-splits
deterministically (already-succeeded destinations receive duplicates, which
OTLP consumers must tolerate anyway). Per-route destinations are **direct
(unbuffered) by design** — only the default chain keeps the `-buffer-dir`
durability; routes are for tenancy/fan-out, not for doubling the durability
machinery. Routed parts count into
`kubescrape_routed_payload_parts_total{route,signal}`.

**The agent's own self-metrics and span metrics BYPASS the router** (and the
`/debug/otlp` tap, which sits beside it) and always go to the default
(buffered) chain — with or without a `routing` section configured. `-self-attributes` stamps this pod's
`k8s.namespace.name` on those resources, so without the bypass a route globbing
the namespace the agent runs in would silently move the fleet's own health
signal onto an unbuffered per-tenant destination — filed under whatever
`X-Scope-OrgID` that route sets, or rejected by it outright, and dropped either
way (a failed self-metrics export is logged and discarded, not retried and not
spooled), and only from the moment the self-lookup resolved. Transforms still
apply to the bypassed chain
(it forks the same reloaded program), so the two chains can never run different
scripts. Nothing needs configuring for this; it just means a route matching your
`monitoring` namespace does not capture `kubescrape_*`.

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

* `resourceAttributes.static` — fixed attributes on every resource.
* `resourceAttributes.enable` / `.disable` — anchored regex lists selecting which attribute keys are exported (global; a pipeline section setting them is rejected).

The config section:

```yaml
resourceAttributes:
  # Include the built-in mapping: k8s.namespace.name, k8s.pod.name,
  # k8s.pod.uid, k8s.node.name, k8s.pod.ip, owners (k8s.deployment.name, ...),
  # pod labels (k8s.pod.label.*), namespace labels, container.id,
  # container.image.name, service.name (workload owner). Default true.
  defaults: true

  # Fixed attributes on every resource. This is the only home for them: the
  # former -resource-attrs-static flag is gone, and the chart's convenience
  # value agent.staticAttrs merges INTO this map (an explicit entry here wins).
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

  # Per-pipeline overrides (logs | targets | cadvisor | node | journal | ingest | self);
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

On the `self` pipeline — the metrics the agent generates about ITSELF
(self-metrics, span metrics), see `-self-attributes` — `.Pod` is the pod the
AGENT runs in and `.Container` is nil: which of the pod's containers this
process is cannot be known, and guessing would mislabel every self-metric.
What that pipeline produces is applied fill-if-absent, so it extends the
agent's own identity rather than replacing it.

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
| `prometheus.io/port` | comma-separated port numbers and/or names; absent = every declared port. A NAME resolves to exactly one port, and identically on every path (this annotation, a Service's named `targetPort`, a monitor endpoint): a **regular container's** declaration is preferred over an init/sidecar or ephemeral one — matching `podutil.FindPort` and the EndpointSlice controller, so a native sidecar sharing the app's port name cannot win — and a name only a sidecar declares still resolves, since Kubernetes has no answer for that case at all. `/v1/explain` reports which case applied |
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

On ServiceMonitor and PodMonitor endpoints, `tlsConfig.insecureSkipVerify`
and `bearerTokenSecret` are honored (tokens are fetched by agents through
`GET /v1/scrape-auth/{ns}/{name}/{key}`, served only when the service runs
`-scrape-auth-secrets`), and the **keep/drop subset** of
`metricRelabelings` (`action`, `sourceLabels`, `regex`; `__name__` = the
metric name — the action is matched case-insensitively, so the CRD-legal
`Keep`/`Drop` spellings work) is applied per sample by the agent.
Per-endpoint `interval`/`scrapeTimeout` **are** interpreted (each target is
scheduled on its own period — see the `-servicemonitors` row above), as are
`basicAuth` and `authorization`. Every other field an endpoint or monitor
sets that kubescrape does not interpret — relabel actions other than
keep/drop, other authentication schemes, proxy settings, the monitor-level
guard rails — is ignored **and reported**,
never silently: they are collected into `Endpoint.Ignored`, logged once per
monitor, and counted in `kubescrape_monitor_fields_ignored_total{kind}`.
The authoritative list is `endpointSpec.ignoredFields()` +
`specLimits.ignored()` in `internal/servicemonitors`; this document
deliberately does not enumerate it (hand-kept copies drifted).

## Helm values

The commonly-tuned flags above map to values (the rest are reachable via `agent.extraArgs`/`service.extraArgs`); `agent.config` is rendered verbatim into the
single mounted `-config` file (with a checksum annotation, so config changes
roll the DaemonSet). `agent.extraVolumes`/`agent.extraVolumeMounts` cover
the mount `-transforms-file` needs: its dedicated ConfigMap, mounted as a
directory (not `subPath`). See
[charts/kubescrape/values.yaml](../charts/kubescrape/values.yaml) for the
full annotated list.

`agent.logsExcludeNamespaces` is **null by default, and null is not empty**.
Left unset, the chart excludes this release's own namespace *plus*, when
`agent.otlp.endpoint` names an in-cluster Service, that Service's namespace —
the dot-separated label immediately *before* `svc`, falling back to the second
label for a bare `<svc>.<ns>` (e.g. `monitoring` for the default
`otel-collector.monitoring:4317`). The two rules coincide for
`<svc>.<ns>.svc…` and diverge for a StatefulSet's per-pod address:
`otel-collector-0.otel-collector.observability.svc.cluster.local` excludes
`observability`, not the service name `otel-collector`. Tailing either is a
feedback loop that amplifies exactly when the collector is already struggling,
and the two are not the same namespace whenever the default endpoint is kept in
a release installed somewhere else. In-cluster means `service.namespace` or
anything under `.svc` — that is what makes the label a namespace; an external
endpoint (`otel.grafana.net:443`) adds nothing, since reading its second label
as a namespace silently stopped tailing any workload that happened to run in a
namespace of that name. Set the value to a list to name the namespaces
yourself, or to an explicit `[]` to exclude nothing — which is the one thing
the old `[]` default could not express, since it was indistinguishable from
"unset".

`azure.enabled: true` rides in the same singleton Deployment as
`events.enabled` (either renders it): `azure.eventhub.*` maps to the
`-azure-eventhub-*` flags, `azure.eventhub.connectionStringSecret` mounts
the secret and passes the file flag, and `azure.clientId` wires workload
identity (ServiceAccount annotation + pod label).

`events.enabled: true` is the other value that wires more than a flag: it
renders a separate single-replica Deployment of the agent binary with its own
ServiceAccount and the events/lease/ConfigMap RBAC, rather than widening the
DaemonSet's. `events.replicas`, `events.start`, `events.leaseName`,
`events.positionConfigMap`, the batch/flush/position intervals, scheduling
(`nodeSelector`/`tolerations`/`priorityClassName`), `resources` and
`extraArgs` are values; the export, enrichment and `agent.config` settings
are shared with the DaemonSet.

`serviceGraph.enabled: true` renders the trace tier: a **StatefulSet** (stable
ordinal DNS names are what the ring addresses; a Deployment's pods have none)
plus two Services — the governing **headless** one, which the internal hop
addresses, and a ClusterIP `<release>-traces`, which is where applications point
their SDKs (`http://<release>-traces.<ns>.svc:4318`, or `:4317` for gRPC).

`serviceGraph.replicas` is both the StatefulSet's size and every shard's
`-service-graph-shards`, from the one value — scale it with `helm upgrade`, not
`kubectl scale`, or a shard addresses a width the tier does not have and the
pushes that hash there fail. `serviceGraph.tokenSecret.name` mounts the shared
bearer token and passes `-service-graph-token-file`; leaving it empty renders no
flag, and the binary then refuses to open its cluster-reachable internal
receiver. `serviceGraph.port` (default 4319) is that receiver.

`serviceGraph.ingest.*` covers the application-facing side:
`grpcEndpoint`/`httpEndpoint` (the ports on the ClusterIP Service),
`peerIpFallback`, and `allowFrom` — NetworkPolicy `from` selectors for the trace
ports, and for those **only**. **Empty `allowFrom` means any pod in the cluster
may push traces, with no credential.** The policy is three rules, so setting
this leaves the other two alone: health/readiness and the metrics port stay
reachable (the kubelet's probes come from the node, Prometheus from wherever it
runs) and the internal shard port stays open to this tier's own pods, which the
ring needs — scoping it along with the trace ports broke the ring at this
chart's own default `replicas: 2`. Health and metrics being unscoped also means
the tier's `-listen` port (which serves `/debug/otlp`) is reachable cluster-wide
unless you add a policy of your own.

`serviceGraph.spanMetrics` turns on the RED metrics derived from the spans this
tier receives; tuning is `agent.config.traceMetrics`, and edge-pairing tuning is
`agent.config.serviceGraph`. The tier mounts the same rendered ConfigMap as the
DaemonSet and simply ignores the sections that are not its.

`service.scrapeAuthSecrets: true` is one value that wires three things at
once, because they must not drift apart: the `-scrape-auth-secrets` flag, the
`secrets: get` ClusterRole rule, and the bearer token guarding
`/v1/scrape-auth`. The token is generated into a `<release>-scrape-auth`
Secret (re-read from the cluster on upgrade so it survives a `helm upgrade`),
mounted into both the service Deployment and the agent DaemonSet, and passed
as `-scrape-auth-token-file`. Point `service.scrapeAuthToken.existingSecret`
at your own Secret to manage it yourself, or set
`service.scrapeAuthToken.value` for a fixed token.

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
