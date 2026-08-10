# Flag reference

**Generated — do not edit the tables by hand.** Each table below is rendered
from the binary's actual registered flags (name, default, usage string) by
`flagsdoc_test.go` in the binary's `cmd/` package. When a flag is added,
renamed, or its default changes, the corresponding test fails until the doc is
regenerated:

```sh
go test ./cmd/kubescrape -run TestFlagsDocIsCurrent -update-flags-doc
go test ./cmd/kubescrape-agent -run TestFlagsDocIsCurrent -update-flags-doc
```

[CONFIGURATION.md](CONFIGURATION.md) is the narrative documentation — what the
flags mean together, and every config-file section. This file is the exhaustive
inventory; the defaults here are authoritative because they cannot drift.

Both binaries accept flags with one or two dashes (`-listen` / `--listen`).

## Metadata service (`kubescrape`)

<!-- BEGIN GENERATED: kubescrape flags -->

| Flag | Default | Description |
|---|---|---|
| `-apiserver-probe-interval` | `30s` | how often to probe API-server reachability with a metadata-only LIST of one namespace, publishing kubescrape_apiserver_reachable and kubescrape_apiserver_probe_failures_total (0 disables the probe, and then neither metric is published). It probes a NEW connection, so it reports reachability rather than whether the caches are advancing: readiness latches at the initial sync, and client-go retries a refused watch without relisting, so the watch-error counter is not a dependable substitute |
| `-cache-ttl` | `5m0s` | how long metadata of deleted pods and replaced container IDs stays resolvable |
| `-kubeconfig` | — | path to a kubeconfig; defaults to in-cluster config, then $KUBECONFIG/~/.kube/config |
| `-listen` | `:8080` | HTTP listen address |
| `-log-format` | `text` | log format: text or json |
| `-log-level` | `info` | log level: debug, info, warn, error |
| `-metadata-cache-ttl` | `10s` | max-age sent on metadata responses (Cache-Control + ETag) so agents cache lookups client-side; 0 disables the cache headers. The server-side ServiceMonitor->Service memo is exact (invalidated by index generation, not this TTL), so 0 no longer costs a cross-product rebuild per request |
| `-metrics-listen` | `:9090` | listen address for the Prometheus /metrics endpoint (Go runtime and process metrics; with -self-metrics-interval=0 also the kubescrape_* internal metrics, replacing the OTLP push with a scrape; empty disables). Separate from -listen so the API and the scrape target can be exposed independently |
| `-monitor-namespaces` | — | comma-separated namespaces whose ServiceMonitors/PodMonitors are honoured (empty = all; a monitor is an instruction to every agent to scrape, so restricting this to admin-owned namespaces is recommended on multi-tenant clusters) |
| `-otlp-bearer-token-file` | — | file with a bearer token sent on every export (re-read periodically) |
| `-otlp-compression-level` | `0` | gzip level 1 (fastest, ~2-3x less CPU for ~10% larger payloads) to 9 (smallest); 0 = library default |
| `-otlp-compression` | `gzip` | OTLP payload compression: gzip or none |
| `-otlp-endpoint` | `otel-collector.monitoring:4317` | OTLP endpoint for self-metrics: host:port for grpc, base URL for http |
| `-otlp-header` | — | static key=value header sent on every self-metrics export (HTTP header / gRPC metadata, e.g. X-Scope-OrgID=tenant); repeatable |
| `-otlp-insecure` | `true` | use a plaintext gRPC connection (for http, use an http:// endpoint) |
| `-otlp-protocol` | `grpc` | OTLP transport: grpc or http |
| `-otlp-timeout` | `15s` | per-export-attempt timeout |
| `-otlp-tls-ca-file` | — | PEM CA bundle for verifying the collector |
| `-otlp-tls-insecure-skip-verify` | `false` | skip TLS certificate verification towards the collector |
| `-pprof-listen` | — | listen address for net/http/pprof under /debug/pprof, on its own port (empty disables). Off by default and separate from -listen and -metrics-listen: profiles expose goroutine stacks and heap contents, so this is the port to firewall or bind to localhost |
| `-resync` | `0s` | informer resync period (0 disables periodic resync; the watch stream keeps the cache current) |
| `-scrape-auth-secrets` | `false` | serve the Secret keys ServiceMonitor/PodMonitor endpoints reference — bearerTokenSecret, basicAuth username/password, authorization credentials and tlsConfig ca/cert/keySecret (a CLIENT PRIVATE KEY) — to agents on /v1/scrape-auth. Only keys some indexed monitor actually names are served. Requires cluster-wide `secrets get` RBAC and -scrape-auth-token-file |
| `-scrape-auth-token-file` | — | file holding the shared bearer token that clients must present on /v1/scrape-auth (Authorization: Bearer <token>); REQUIRED with -scrape-auth-secrets |
| `-self-attributes` | `true` | add THIS pod's Kubernetes resource attributes (namespace, pod, uid, owners, labels) to the service's own exported metrics. Resolved from the service's OWN store — its pod name is the hostname, its namespace comes from $POD_NAMESPACE or the ServiceAccount projection — so it needs no API traffic and no extra manifest wiring. Attributes already set (service.name, service.instance.id) are never overwritten; a process that is not a pod of that name simply gets none |
| `-self-metrics-interval` | `1m0s` | export the service's own metrics over OTLP at this interval (0 disables) |
| `-servicemonitors` | `false` | serve targets for monitoring.coreos.com ServiceMonitors (pod-backed Services) and PodMonitors. Endpoint port/targetPort/path/scheme, per-endpoint interval/scrapeTimeout, basicAuth/authorization/bearerTokenSecret and secret-backed tlsConfig (needs -scrape-auth-secrets), and the keep/drop subset of metricRelabelings are interpreted; everything else is reported through kubescrape_monitor_fields_ignored_total and a startup warning |
| `-wait-timeout` | `5s` | default and maximum time a container lookup blocks waiting for metadata to appear (shorten per request with ?wait=) |

<!-- END GENERATED: kubescrape flags -->

## Node agent (`kubescrape-agent`)

The agent's optional pipelines (`-journald`, `-azure-diagnostics`) always
define their flags, whatever build tags produced the binary — enabling one on
a build that lacks the pipeline is a startup error naming the tag (see
[Build variants](CONFIGURATION.md#build-variants-optional-pipelines)).

<!-- BEGIN GENERATED: kubescrape-agent flags -->

| Flag | Default | Description |
|---|---|---|
| `-azure-client-id` | — | user-assigned managed identity / workload identity client id (default $AZURE_CLIENT_ID) |
| `-azure-diagnostics` | `false` | consume Azure diagnostic-settings output (resource logs AND platform metrics) from an Event Hubs namespace over its Kafka endpoint and export it as OTLP. Cluster-scoped: run it in the same singleton Deployment as -events, not in the DaemonSet |
| `-azure-eventhub-connection-string-file` | — | file holding an Event Hubs connection string (SASL PLAIN; re-read per connection, so rotation needs no restart); empty authenticates with managed identity (OAUTHBEARER via AKS workload identity when its env is present, else IMDS) |
| `-azure-eventhub-group` | `$Default` | Kafka consumer group; its committed offsets ARE the resume position, shared across restarts and replicas |
| `-azure-eventhub-namespace` | — | Event Hubs namespace host (myns.servicebus.windows.net); derived from the connection string's Endpoint when -azure-eventhub-connection-string-file is set |
| `-azure-eventhub-topics` | — | comma-separated event hubs to consume; empty consumes every hub matching ^insights-.* (the names diagnostic settings create by default) |
| `-azure-metric-prefix` | `azure.` | prefix for converted Azure metric names (<prefix><metricname>.<aggregation>) |
| `-azure-start` | `end` | where a consumer group with NO committed offsets starts: end (skip the backlog) or start (replay everything the hubs retain) |
| `-azure-tenant-id` | — | Microsoft Entra tenant for workload identity (default $AZURE_TENANT_ID) |
| `-buffer-dir` | — | directory for a disk-backed export buffer (logs, metrics, and tail-sampled traces on the -service-graph tier); a collector outage spools here instead of pinning the tailer to old offsets or dropping metrics (empty disables) |
| `-buffer-max-bytes` | `1073741824` | per-signal cap on the undelivered on-disk buffer; producers back-pressure (the tailer rewinds) when full |
| `-cadvisor-rollups` | `true` | include cadvisor rollup series: cgroups above pod level and pod-level rows of container-scoped families |
| `-cadvisor` | `true` | scrape <kubelet-endpoint>/metrics/cadvisor (per-container metrics) |
| `-check-config` | `false` | validate -config and -transforms-file (every section compiled: templates, regexes, selectors, globs) plus the flags, print a summary and exit — no listeners, log files, positions file, spools or network. For CI and pre-rollout checks: a DaemonSet's bad ConfigMap otherwise surfaces as a fleet-wide CrashLoop |
| `-config` | — | unified YAML config file; sections: resourceAttributes, logs, logAttributes, logMetrics, metrics, traceMetrics, routing, logScrubbing, serviceGraph, serviceGraphShards, traceSampling, tailSampling, export (docs/CONFIGURATION.md) |
| `-enrich` | `true` | parse per-line metadata (timestamp, severity, trace/span IDs, exception details) into the OTLP record fields via github.com/JohanLindvall/enrich, for container logs, journald, Kubernetes events, Azure diagnostics and pushed OTLP log bodies alike |
| `-events-batch-size` | `512` | flush events after this many, clamped to the retained-batch cap: the startup backlog walk blocks the reader goroutine and services no ticker, so a value above the cap would make the count trigger unreachable and shed the whole backlog |
| `-events-flush-interval` | `2s` | flush events at least this often |
| `-events-lease-namespace` | — | namespace for the Lease and position ConfigMap (default: this pod's own, via $POD_NAMESPACE or the ServiceAccount projection) |
| `-events-lease` | `kubescrape-cluster-leader` | Lease coordinating the cluster-singleton pipelines |
| `-events-namespace` | — | namespace to watch (empty = cluster-wide) |
| `-events-position-configmap` | `kubescrape-events-position` | ConfigMap holding the resume position. NOT a node-local file: the leader moves, so the successor must be able to read it |
| `-events-position-interval` | `10s` | how often the position is written to its ConfigMap. A write per event would be an API-server write per event, so this is the bound on how much is REPLAYED after a hard kill (bounded duplicates, never loss); a graceful stop always writes a final position |
| `-events-start` | `auto` | where a cold start begins: end (skip the backlog), start (replay everything still within the API server's event TTL), auto (resume the stored position, else end) |
| `-events` | `false` | watch Kubernetes Events and export them as OTLP logs, enriched with the involved object's identity. Cluster-singleton: exactly one replica runs it (leader election), so deploy it as its own Deployment with -logs=false -metrics=false -cadvisor=false -node-metrics=false, NOT in the DaemonSet |
| `-ingest-container-id-keys` | `container.id,k8s.container.id` | comma-separated attribute keys inspected for a container id |
| `-ingest-grpc-endpoint` | `:4317` | listen address for pushed OTLP/gRPC (empty disables) |
| `-ingest-grpc-max-recv-bytes` | `0` | cap on one decoded OTLP/gRPC message on the ingest listeners (and the trace tier's application ports); an over-cap push is refused, not truncated. 0 uses gRPC's own default (4 MiB); the OTLP/HTTP body cap stays 16 MiB |
| `-ingest-http-endpoint` | `:4318` | listen address for pushed OTLP/HTTP protobuf on /v1/logs and /v1/metrics (empty disables) |
| `-ingest-max-in-flight` | `0` | bound on concurrently-processed pushes across both ingest transports; over it senders get a retryable refusal (429 / ResourceExhausted with RetryInfo). 0 uses the built-in default (32) |
| `-ingest-metadata-wait` | `0s` | how long an ingest metadata lookup may block for not-yet-known objects |
| `-ingest-metrics-mode` | `auto` | how pushed metrics resolve their object: resource (id on the resource), datapoint (id on each point, split into per-object resources), or auto |
| `-ingest-peer-ip-fallback` | `false` | attribute pushed telemetry whose resource carries no container id / pod uid to the pod owning the connection's SOURCE address (hostNetwork senders never resolve). Only correct where that address still names the sender: a proxy, a mesh sidecar that terminates, or any NAT hop replaces it, and on the -service-graph tier a source address belonging to the tier's own workload is refused and counted (kubescrape_ingest_resources_total{outcome="peer_ip_rejected"}) rather than attributed |
| `-ingest-pod-uid-keys` | `k8s.pod.uid` | comma-separated attribute keys inspected for a pod uid |
| `-ingest-span-metrics-interval` | `1m0s` | export interval for span metrics |
| `-ingest-span-metrics` | `false` | derive RED (calls + duration histogram) metrics from received spans, dimensioned by service.name/span.name/span.kind/status.code; exported over OTLP (tune via the traceMetrics config section). Traces are received by the -service-graph tier, so this belongs on that workload |
| `-ingest` | `false` | receive pushed OTLP logs and metrics and enrich them with k8s attributes before forwarding. Traces go to the -service-graph tier instead: pairing an edge and (later) sampling a trace need every span of that trace in one process, which a per-node receiver can never have |
| `-journald-batch-size` | `1024` | flush journal entries after this many |
| `-journald-dir` | — | read a specific journal directory; empty opens the default system journal |
| `-journald-flush-interval` | `2s` | flush journal entries at least this often |
| `-journald-max-batch-bytes` | `1048576` | flush journal entries before a batch's summed message bytes exceed this |
| `-journald-units` | — | comma-separated systemd units to read (empty reads everything) |
| `-journald` | `false` | read the systemd journal natively via libsystemd/sdjournal (the image must provide libsystemd) |
| `-kubeconfig` | — | path to a kubeconfig for the events watch; defaults to in-cluster config (only used with -events) |
| `-kubelet-endpoint` | — | kubelet base URL, e.g. https://$(NODE_IP):10250 (empty disables the cadvisor and node-metrics scrapes) |
| `-kubelet-insecure-tls` | `true` | skip TLS verification for the kubelet (its serving certificate is typically self-signed) |
| `-kubelet-token-file` | `/var/run/secrets/kubernetes.io/serviceaccount/token` | bearer token file for the kubelet (re-read per scrape) |
| `-listen` | `:8081` | HTTP listen address for /healthz, /readyz, /debug/tailer and /debug/targets (empty disables) |
| `-log-dir` | `/var/log/containers` | directory of containerd log symlinks (the default source when the config's logs section is unset) |
| `-log-format` | `text` | log format: text or json |
| `-log-level` | `info` | log level: debug, info, warn, error |
| `-logs-batch-size` | `1024` | flush logs after this many entries |
| `-logs-exclude-namespaces` | — | comma-separated namespaces whose container logs are not tailed |
| `-logs-file-attributes` | `false` | stamp log.file.name and log.file.position (byte offset) on every log record, for each file source |
| `-logs-fingerprint-bytes` | `1024` | file-head hash length used with the inode as file identity (negative = inode only) |
| `-logs-flush-interval` | `2s` | flush logs at least this often |
| `-logs-idle-close` | `0s` | close the fd of a fully-caught-up file after this much inactivity (0 = never, the default). The open fd is the only way to drain a rotated-away or deleted file, so enabling this trades the zero-loss guarantee for bounded fd usage |
| `-logs-max-entry-bytes` | `1048576` | truncate assembled log entries beyond this size |
| `-logs-metrics-interval` | `30s` | export interval for log-derived metrics |
| `-logs-metrics-max-bytes` | `3145728` | export log-derived metrics in chunks below this many bytes (0 = one payload) |
| `-logs-metrics-name-prefix` | — | prefix prepended to every log-derived metric name |
| `-logs-multiline-timeout` | `1s` | flush incomplete multi-line groups after this long |
| `-logs-multiline` | `true` | join application-level multi-line entries (stack traces, ...) |
| `-logs-poll-interval` | `500ms` | fallback sweep interval for the log tailer |
| `-logs-rate-burst` | `0` | rate-limit token bucket size (0 = 2x -logs-rate-limit) |
| `-logs-rate-drop` | `false` | discard lines over -logs-rate-limit instead of pausing the file |
| `-logs-rate-limit` | `0` | per-file line rate limit in lines/second (0 disables); exhausted files pause until tokens refill |
| `-logs-unknown-files` | `auto` | where a file with no checkpoint entry starts at startup: end (skip as history), start (read whole), auto (start when the checkpoint store has entries — it appeared while the agent was down — else end) |
| `-logs-watch` | `true` | use file events (fsnotify) to trigger reads and discovery; polling remains the fallback |
| `-logs` | `true` | tail container logs |
| `-metadata-endpoint` | `http://kubescrape.monitoring` | base URL of the kubescrape metadata service |
| `-metadata-wait` | `5s` | how long the metadata service may block waiting for a new container |
| `-metrics-batch-bytes` | `3145728` | also flush a metrics chunk once its estimated encoded size reaches this many bytes (0 = only -metrics-batch-size). The collector's gRPC receive limit applies to the DECOMPRESSED message (4 MiB by default), and a label-rich target can exceed it well before the point limit — every export of that target would then fail |
| `-metrics-batch-size` | `10000` | export metrics in chunks of this many data points |
| `-metrics-listen` | `:9090` | listen address for the Prometheus /metrics endpoint (Go runtime and process metrics; with -self-metrics-interval=0 also the kubescrape_* internal metrics, replacing the OTLP push with a scrape; empty disables). Separate from -listen so the debug/health surface and the scrape target can be exposed independently |
| `-metrics` | `true` | scrape annotation-discovered pod/service targets |
| `-node-metadata-refresh` | `1m0s` | refresh interval for the node's labels/annotations used in attribute templates (0 disables the lookup) |
| `-node-metrics` | `true` | scrape <kubelet-endpoint>/metrics (kubelet/node metrics) |
| `-node-name` | — | name of the node this agent runs on (default $NODE_NAME) |
| `-otlp-bearer-token-file` | — | file with a bearer token sent on every export (re-read periodically) |
| `-otlp-compression-level` | `0` | gzip level 1 (fastest, ~2-3x less CPU for ~10% larger payloads) to 9 (smallest); 0 = library default |
| `-otlp-compression` | `gzip` | OTLP payload compression: gzip or none |
| `-otlp-endpoint` | `otel-collector.monitoring:4317` | OTLP endpoint: host:port for grpc, base URL for http |
| `-otlp-insecure` | `true` | use a plaintext gRPC connection (for http, use an http:// endpoint) |
| `-otlp-max-send-bytes` | `0` | cap on one exported payload's encoded protobuf size; a larger payload is split into parts before sending (0 = default ~3.75 MiB, under the 4 MiB gRPC limit; negative disables) |
| `-otlp-protocol` | `grpc` | OTLP transport: grpc or http |
| `-otlp-retry-attempts` | `3` | tries per metrics export (logs retry via the tailer's rewind) |
| `-otlp-retry-backoff` | `1s` | initial backoff between metric export retries, doubled per attempt |
| `-otlp-timeout` | `15s` | per-export-attempt timeout |
| `-otlp-tls-ca-file` | — | PEM CA bundle for verifying the collector |
| `-otlp-tls-insecure-skip-verify` | `false` | skip TLS certificate verification towards the collector |
| `-positions-file` | — | single file persisting BOTH log offsets and the journald cursor across restarts (empty disables persistence) |
| `-pprof-listen` | — | listen address for net/http/pprof under /debug/pprof, on its own port (empty disables). Off by default and separate from -listen and -metrics-listen: profiles expose goroutine stacks and heap contents, so this is the port to firewall or bind to localhost |
| `-scrape-auth-token-file` | — | bearer token file for the metadata service's /v1/scrape-auth endpoint (re-read periodically); required when the service runs -scrape-auth-secrets |
| `-scrape-concurrency` | `4` | concurrent target scrapes |
| `-scrape-exemplars` | `false` | negotiate OpenMetrics and attach exemplars to counter and histogram data points |
| `-scrape-health-metrics` | `true` | export synthetic up/scrape_duration_seconds/scrape_samples_scraped gauges per target |
| `-scrape-interval` | `30s` | Prometheus scrape interval |
| `-scrape-max-samples` | `0` | abort a single scrape beyond this many samples (0 = unlimited) |
| `-scrape-native-histograms` | `false` | offer the Prometheus protobuf exposition to scrape targets and convert native histograms to OTLP exponential histograms |
| `-scrape-timeout` | `15s` | per-target scrape timeout |
| `-self-attributes-refresh` | `1m0s` | how often to re-read this pod's own metadata, so an edited pod or namespace label reaches the metrics it stamps (0 disables the lookup entirely, as -node-metadata-refresh=0 does for the node's). Cheap by construction: GET /v1/self carries `private, max-age` + ETag, so the client serves a fresh entry locally and revalidates a stale one as a conditional GET — a 304 whenever nothing changed. Retries before the first success start at 5s and back off to this |
| `-self-attributes` | `true` | add THIS pod's Kubernetes resource attributes (namespace, pod, uid, owners, labels, plus the resourceAttributes section's static/template attributes for the `self` pipeline) to the metrics the agent generates about itself — its self-metrics and span metrics. Resolved from the metadata service's GET /v1/self, which attributes the request by its source address. Attributes the agent already set (service.name, service.instance.id, ...) are never overwritten; a caller the service cannot attribute to a live pod (hostNetwork, an address-rewriting hop) simply gets none. kubescrape_self_metadata_resolved reports whether it resolved |
| `-self-metrics-interval` | `1m0s` | export the agent's own metrics over OTLP at this interval (0 disables) |
| `-service-graph-endpoint` | — | the tier's governing HEADLESS Service, <statefulset>.<namespace>.svc:<port>; each shard's stable per-pod address <sts>-<ordinal>.<service>.<ns>.svc:<port> is derived from it for the internal hop. Never a ClusterIP: a load-balanced destination round-robins, which is exactly what the re-shard exists to undo. The config's serviceGraphShards section is the richer form (explicit endpoints, TLS, tokensPerShard) and WINS field by field where both are set |
| `-service-graph-http-listen` | — | listen address for the tier's internal OTLP/HTTP protobuf receiver on /v1/traces (empty disables). Only needed with serviceGraphShards.protocol: http; the default internal hop is gRPC |
| `-service-graph-ingest-grpc` | `:4317` | listen address for application OTLP/gRPC traces on the tier (empty disables). UNAUTHENTICATED by design: every instrumented pod in the cluster is a sender, and requiring a credential from each of them is not a bargain most fleets can make. Restrict it with a NetworkPolicy if the pod network is not trusted |
| `-service-graph-ingest-http` | `:4318` | listen address for application OTLP/HTTP protobuf traces on the tier, /v1/traces (empty disables) |
| `-service-graph-ingest` | `true` | accept application OTLP traces on the tier (the addresses below). Off leaves only the internal receiver, which is a tier nothing can push to |
| `-service-graph-interval` | `1m0s` | export interval for the tier's service-graph edge metrics |
| `-service-graph-listen` | `:4319` | listen address for the tier's INTERNAL OTLP/gRPC receiver: spans re-sharded by a sibling shard, behind the shared bearer token. Deliberately not the application ports below — an internal hop addressed to those would re-enrich and re-shard on every pass (empty disables) |
| `-service-graph-shard-name` | — | this shard's own name in the ring (default $POD_NAME, which for a StatefulSet pod is <sts>-<ordinal>). Spans this shard already owns are then handled in-process instead of being sent over the network to itself; a name that is not in the ring still works but doubles the tier's internal traffic, and is warned about at startup |
| `-service-graph-shards` | `0` | number of shards in the tier (0 or 1 = no internal hop, everything is owned locally). It MUST equal the StatefulSet's replica count and be identical on every shard: the count defines the ring, and two shards disagreeing about it route a request's two halves to two different owners, where the edge silently never forms |
| `-service-graph-token-file` | — | shared bearer token file for the tier's INTERNAL hop: the receiver accepts it (and refuses to start without it — that listener takes spans from every pod in the cluster and must not be reachable unauthenticated), the sending shard presents it. Re-read periodically, with the previous value accepted for a grace window, so rotating the Secret needs no restart and no lockstep flip. It does NOT gate the application-facing listeners, which are open by design |
| `-service-graph` | `false` | run the TRACE TIER: receive application OTLP traces, enrich them, re-shard each span by trace id onto the tier's ring, and on the owning shard pair each request's client and server halves into Grafana-Tempo-compatible edge metrics. Deploy it as its own StatefulSet (stable per-pod DNS names are what the ring addresses) with -logs=false -metrics=false -cadvisor=false -node-metrics=false -ingest=false, NOT in the DaemonSet: a request's two halves are emitted by pods on two different nodes, so per-node pairing cannot complete an edge. Tuned by the config's serviceGraph section; REQUIRES -service-graph-token-file |
| `-test-config` | — | run the YAML test cases in this file through the compiled log pipeline (scrub → logAttributes → enrich → logMetrics → logs.rules → transforms) and exit non-zero on failure — CI proof of what a rule/scrub/transform edit does to sample lines, with nothing acquired (like -check-config) |
| `-transforms-file` | — | Starlark transforms file applied to exported logs/metrics/traces at the exporter seam; hot-reloaded on change (mount its ConfigMap as a directory, not subPath). Empty disables |

<!-- END GENERATED: kubescrape-agent flags -->
