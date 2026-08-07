# kubescrape vs. similar tools

How kubescrape compares to the agents commonly deployed for the same job:
**Grafana Alloy** (and its predecessor **Promtail**, now EOL — see
[Migrating off Promtail](#migrating-off-promtail)), **Vector**,
**Fluent Bit**, the **OpenTelemetry Collector** (filelog + prometheus +
k8sattributes), **Telegraf** for the metrics path, and
**Prometheus/vmagent** for the scrape path. The Elastic Agent and the
Datadog Agent occupy the same DaemonSet slot; rather than full columns they
are referenced where they differ materially (fleet management, curated PII
rule libraries, UDS origin detection).

Scope disclaimer: kubescrape is deliberately narrower than most of these — a
Kubernetes-only, Linux-only, OTLP-only node agent plus a cluster metadata
service. The comparison is against the *overlapping* feature set: collecting
container logs and Prometheus metrics on Kubernetes nodes, attributing them
with Kubernetes metadata, and shipping them via OTLP. Comparator behavior is
described as of mid-2026; measured numbers are from this repository's
benchmarks (see [Performance](#performance)).

## Architecture

The structural difference from every comparator is **where the Kubernetes
API is watched**:

| | API-server load | Metadata timing |
|---|---|---|
| **kubescrape** | **one** watcher set per cluster (the metadata service); agents talk HTTP to it, with ETag caching | container lookups **block** briefly until the kubelet posts the status — no unattributed startup logs |
| Vector `kubernetes_logs` | one watcher per node | local cache; races pod startup |
| Fluent Bit `kubernetes` filter | API calls per node (local cache; `Use_Kubelet` since v1.9 serves pod metadata from the node-local kubelet, though namespace metadata still hits the API server) | cache misses race pod startup |
| OTel Collector `k8sattributes` | one watcher per collector instance (agent-mode deployments narrow it with a `spec.nodeName` field selector — many narrow watches, not many full-cluster ones) | local cache; races pod startup |
| Alloy / Promtail | one watcher set per instance/replica | discovery-time relabeling only |

The claim is scoped to **metadata and attribution**: on the scrape path the
OTel Target Allocator already centralizes discovery into one component, and
node-filtered watches shrink the per-node cost without removing it. What no
comparator centralizes is the attribution lookup itself — and kubescrape's
trade-off is that agents depend on the metadata service being reachable
(lookups block, then retry; log data is never lost — files are not consumed
until they can be attributed).

Also structural: the tailer *blocks per container ID* with tombstones for
deleted pods, so logs from containers that live seconds (CronJobs) are still
fully attributed — the cache-race gap that per-node watchers accept.

## Feature matrix

### Log collection

| | kubescrape | Alloy/Promtail† | Vector | Fluent Bit | OTel filelog |
|---|---|---|---|---|---|
| CRI parsing + partial-line rejoin | ✔ built-in | ✔ stage | ✔ | ✔ | ✔ operator |
| Multiline (stack traces) | ✔ 7 languages, zero config, literal-prefiltered | config per format | ✘ in `kubernetes_logs` (open requests; `reduce` transform as workaround) | config | config |
| Multiline **across rotations** | ✔ incl. across crashes | ✘ breaks at rotation | ✘ | ✘ | ✘ |
| Rotation: rename / copytruncate / inode reuse | ✔ (inode + head fingerprint) | rename + truncate-reset (copytruncate loss window) | ✔ | ✔ | ✔ |
| Auto enrichment (timestamp, severity, trace IDs, exceptions) | ✔ zero config ([enrich](https://github.com/JohanLindvall/enrich)) | config stages | config (VRL) | config parsers | config operators |
| Structured-field lifting (JSON/logfmt → attributes) | ✔ `logAttributes` | ✔ stages | ✔ VRL | ✔ | ✔ |
| Drop / keep / sample rules | ✔ `logs.rules` (shared selector DSL, `__severity__`) | ✔ stages | ✔ VRL | ✔ | ✔ OTTL |
| Per-file rate limiting | ✔ pause (lossless) or drop | ✔ limit stage (Promtail adds a node-global `limits_config` budget kubescrape lacks) | ✔ throttle | ~ throttle filter (drop-based, per tag — not per source file, no lossless pause) | ✘ |
| Log-derived metrics | ✔ counter/gauge/histogram/summary, windowed aggregations, **pushed OTLP with per-pod resources** | ✔ metrics stage (local exposition) | ✔ log_to_metric | ✔ | ✔ count + sum connectors |
| Arbitrary host files / gzip archives | ✔ / ✔ gzip, **resumable** mid-archive across restarts | ✔ / ✔ gz, bz2, z (since Loki 2.8; whole-file, no resume) | ✔ / ✘ | ✔ / ✘ | ✔ / ✔ |
| journald | ✔ native (libsystemd) | ✔ | ✔ | ✔ | ✔ |
| Kubernetes events as logs | ✔ cluster-singleton (leader election), **checkpointed** resume, events land on the involved pod's own resource | ✔ `loki.source.kubernetes_events` | ✘ | ✔ `in_kubernetes_events` | ✔ `k8s_events`/`k8sobjects` (no checkpoint) |
| Azure diagnostics from Event Hubs | ✔ logs **and metrics→real OTLP gauges**, ARM-resource identity, group-offset resume, connection-string + managed-identity auth | ~ `loki.source.azure_event_hubs` (logs only) | ~ generic `kafka` source | ~ generic `kafka` input | ✔ `azureeventhub` receiver (logs + metrics) |
| Secret/PII scrubbing | ✔ `logScrubbing` (curated built-ins + custom regexes, pre-enrichment) | ~ `loki.secretfilter` (curated Gitleaks rules + entropy detection; experimental) / config stages | VRL | config | ✔ redaction processor |
| Per-workload (annotation) log config | ✔ `kubescrape.io/logs` (exclude, multiline, rules, attributes) | ~ PodLogs CRD (GA: per-workload selection/relabeling; no multiline/rule overrides) | ~ `vector.dev/exclude` label + exclude-containers annotation | ~ `fluentbit.io/exclude`, parser annotations | ✘ |
| Body rewriting / templating | ✔ opt-in (Starlark transforms; the built-in pipeline never modifies bodies) | ✔ | ✔ VRL | ✔ | ✔ OTTL |
| General transform language | **Starlark** (per-batch, hot-reloaded, opt-in) | River/stages | **VRL** | Lua/WASM/SQL stream processor/processors | **OTTL** |
| Live debug tap of shipped data | ✔ `GET /debug/otlp`: streamed OTLP JSON, resource-attr glob filters + sample %, built-in UI, zero cost unattached | ✔ Alloy `livedebugging` UI (per component) | ✔ `vector tap` (glob on component IDs) | ✘ | ~ debug exporter (always-on config, whole feed, needs a config edit + restart) |
| "Why is this pod (not) scraped?" | ✔ `GET /v1/explain/{ns}/{pod}`: the whole decision chain, verdict by verdict | ~ targets pages show the post-relabel result, not the why | ✘ | ✘ | ~ targetallocator debug endpoints (jobs/targets, no per-pod verdicts) |

† Cells describe Alloy; Promtail differences are noted inline. Promtail
itself is EOL — see [Migrating off Promtail](#migrating-off-promtail).

One property of the table that no comparator matches uniformly: the whole
processing chain — scrubbing, enrichment, attribute lifting, log-derived
metrics, drop/keep/sample rules, Starlark transforms, routing, disk buffer —
applies identically to **every log source**: container files, plain host
files, gzip archives, journald, Kubernetes events and Azure Event Hubs
diagnostics. Comparators typically wire parsing and enrichment per source
type, so a rule that works on file tails has to be rebuilt (or is
unavailable) for their journald or events inputs.

### Metrics

| | kubescrape | Alloy/Prometheus/vmagent | OTel prometheus receiver |
|---|---|---|---|
| Annotation discovery (`prometheus.io/*`) | ✔ incl. Services with `targetPort` translation, comma port lists | via relabel config | via SD config |
| ServiceMonitors / PodMonitors | ✔ subset (port/targetPort/path/scheme, per-endpoint cadence, secret-backed auth/TLS, insecureSkipVerify, keep/drop metricRelabelings); no Probe/ScrapeConfig CRDs | ✔ full via Alloy (`prometheus.operator.*`, incl. Probe GA + ScrapeConfig experimental); Prometheus/vmagent only **with the operator deployed alongside** | ✔ via Target Allocator (embeds the operator libraries) |
| Relabeling | keep/drop/label rules + splitters (narrower, declarative) | ✔ full relabel_configs | ✔ |
| Native histograms | ✔ opt-in (protobuf exposition → OTLP exponential histograms) | ✔ | ✔ |
| KSM re-attribution (per-object resources + metadata enrichment) | ✔ **splitters** — unique | Alloy: ~400 lines of OTTL/groupbyattrs; Prometheus/vmagent: n/a (flat labels over remote write, no resource model) | manual OTTL |
| cadvisor/kubelet | ✔ exact-incarnation attribution via cgroup ID | label-based | label-based |
| Mimir job/instance identity conventions | ✔ built-in (`service.namespace`/`instance.id`, collision prefixes) | native over remote write (job/instance are scrape labels); manual transforms only when shipping OTLP | manual |
| Streaming constant-memory parse | ✔ (100k-series targets) | ~ vmagent opt-in (`-promscrape.streamParse`, default buffers whole responses); Prometheus agent memory scales with active series regardless | buffers families |
| Exemplars | ✔ opt-in | ✔ | ✔ |
| Remote-write output | ✘ (OTLP only) | ✔ | ✔ |

**Telegraf** deserves its own note on this path: its `prometheus` input's
`monitor_kubernetes_pods` is the other widely-deployed implementation of the
exact `prometheus.io/*` annotation convention kubescrape builds on. Around
that it brings what kubescrape deliberately lacks — enormous input/output
breadth (remote write included), temporal aggregation and downsampling at
the edge, arbitrary-format parsers (grok/CSV/XPath/Avro), external plugins
via `execd` — and lacks what kubescrape is for: it has no Kubernetes log
attribution story (tail/syslog without pod identity, no CRI/multiline/
rotation machinery), no ack-gated log delivery, and reads no
prometheus-operator CRDs. A metrics-first, InfluxDB- or
heterogeneous-input-shaped fleet is Telegraf's home turf; Kubernetes
logs+metrics over OTLP is kubescrape's.

### Other signals & delivery

| | kubescrape | Alloy | Vector | Fluent Bit | OTel Collector |
|---|---|---|---|---|---|
| OTLP ingest (push) with k8s enrichment | ✔ logs/metrics on the node agent, traces on the trace tier, peer-IP fallback (payloads forwarded as received — batch in the SDK or downstream) | ✔ | ~ OTLP source decodes, but no k8s enrichment of pushed data | ✔ | ✔ |
| Traces | a dedicated sharded tier: enrichment + re-sharding by trace id + RED span metrics + consistent probabilistic sampling (`traceSampling`) + **whole-trace tail sampling** (`tailSampling`, the Collector's policy vocabulary, first-match-wins attribution) + Grafana-Tempo-compatible service-graph edges (opt-in) | ✔ full | ~ pass-through (practical since v0.50); OSS has no sampling or span metrics | ✔ head **and** conditional tail sampling (v4 sampling processor: latency/status/attribute policies) | ✔ full (tail sampling etc.) |
| Multi-destination / tenant routing | ✔ per-signal destinations + tenant headers on the buffered default chain (`export` section: Mimir/Loki/Tempo's distinct OTLP endpoints, collectorless); plus per-namespace fan-out (`routing`; unbuffered by design) | ✔ | ✔ | ✔ | ✔ routing connector |
| Log delivery | **ack-gated at-least-once** + rewind; offsets never pass unacked data | positions synced on timer (loss/dup window) | ✔ e2e acks + disk buffers | offsets on read; `storage.type filesystem` persists read-but-undelivered chunks across restarts | checkpoints when the downstream consumer accepts (not backend ack); persistent sending queue bounds outage loss |
| Disk buffering | ✔ logs, metrics and tail-sampled traces (fsync'd frames, checksummed cursor, poison-batch handling); a *forwarded* trace passes through by design, since its sender still holds it | ✔ metrics WAL (GA, agent-mode); otelcol file-storage queues since v1.9; the *log* WAL never went GA | ✔ mature | ✔ filesystem storage | ✔ file storage ext |
| Compression | gzip (klauspost) | snappy/gzip | ✔ several | ✔ | ✔ several |
| Backpressure to source | ✔ rewind = files wait on disk | partial | ✔ | ✔ | partial |
| Inputs beyond k8s/journal (syslog, kafka, statsd, cloud…) | ✘ | ✔ | ✔✔ | ✔✔ | ✔✔ |
| Windows / macOS | ✘ Linux only | ✔ | ✔ | ✔ | ✔ |
| Remote config (OpAMP/fleet) | ✘ | ✔ | ✘ OSS (local live reload; remote-managed pipelines are a Datadog commercial offering) | ✔ | ✔ |

## Performance

Measured on the same machine (AMD Ryzen 7 8840HS, Go 1.25) with this repo's
committed benchmarks; comparator figures below the table are order-of-
magnitude from public benchmarks, not same-machine measurements.

**Log pipeline, per line** (`BenchmarkIngestLine` / `BenchmarkIngestFlush`):

| Stage | ns/line | allocs |
|---|---|---|
| CRI + multiline pipeline + offset ledger | ~210 | 0 |
| + OTLP record building + export | ~400 | 4 |
| + automatic enrichment (production shape) | ~780 | 4 |
| + log-metrics + drop rules | ~1,140 | 5 |

≈ **1.2M enriched lines/s/core**. Typical published figures for full parse+
transform pipelines: Vector ~200–400k events/s/core, Fluent Bit in the same
range, Promtail/Alloy usually lower once regex stages run. kubescrape reaches
its number *with* enrichment that comparators need per-app config for. The
known ceiling: one sweep goroutine per node (a single core) — pair with
`-buffer-dir` to decouple delivery latency from reading;
Vector/Fluent Bit parallelize across sources.

**Metrics pipeline** — a same-input, same-machine comparison against the
reference implementation (Prometheus `textparse` v0.313, 12k-sample
Kubernetes-shaped exposition):

| | Work | Throughput | Allocs |
|---|---|---|---|
| Prometheus `textparse` | parse + label materialization only | ~207 MB/s | 29k |
| **kubescrape** | parse + **filter + full OTLP conversion** | **284 MB/s** | 42k |

The full kubescrape pipeline outruns the reference parser doing strictly less
work (Prometheus still has relabeling + append ahead at that point). Parse
alone: 552 MB/s, 21 allocs per 10k-series scrape, constant memory.

**Log-derived metrics**: 229–270 ns/line, ≤1 alloc — µs-scale in the
comparators (Promtail metrics stage, Vector log_to_metric).

**Cluster-scoped pipelines** (the events/Azure singleton, same committed
benchmarks): an Azure Event Hubs diagnostics record decodes in ~1.4 µs at
~1 alloc (the envelope walk itself is 36 ns/record and 0 allocs, via
lightning's SIMD-backed `ArrayEach`) and runs the FULL log chain — scrub +
logAttributes + enrich + log-metrics + rules — in ~9.7 µs/record: ≈100k
records/s on the consumer goroutine, an order of magnitude above realistic
hub rates; metric records convert to OTLP gauge points at ~1.8 µs/record.
A Kubernetes event costs ~4 µs end-to-end — invisible at cluster event
rates (tens per second).

## Delivery semantics in one paragraph

kubescrape commits log offsets **only after the collector acknowledges the
batch**, never past lines still buffered in the multiline pipeline; failures
rewind and re-read. Multi-line groups survive rename rotations *and* crashes
mid-rotation (rotated-away files are recorded in the checkpoint and re-read
in order). The disk buffer fsyncs every frame, checksums its cursor, rolls
back partial writes (ENOSPC), and classifies permanent rejections so a poison
batch cannot wedge a signal. This is Vector-class delivery; it is strictly
stronger than Promtail (timer-synced positions) and Fluent Bit's default
(offsets committed on read — its filesystem storage narrows the window by
persisting read-but-undelivered chunks), and stronger in kind than the OTel
filelog receiver, which checkpoints when the downstream consumer accepts a
batch and bounds — but does not eliminate — outage loss with a persistent
sending queue: none of them gate the source offset on the *backend's*
acknowledgment.

## Migrating off Promtail

Promtail entered LTS in February 2025 and reached end of life on
March 2, 2026 — it receives no updates or security fixes and should be
treated as a migration source, not an alternative. For a Promtail estate
deciding where to land:

- **What transfers cleanly to kubescrape**: Loki 3.x ingests OTLP natively,
  so no push protocol is needed; CRI parsing, journald, per-file limits and
  drop/sample rules all have direct equivalents; delivery gets strictly
  stronger (ack-gated offsets vs timer-synced positions, cross-rotation
  multiline, copytruncate detection Promtail lacks).
- **What does not**: `relabel_configs` muscle memory and agent-side control
  of the Loki index label set (kubescrape's resource attributes map to
  structured metadata/labels backend-side); the `tenant` stage
  (kubescrape routes tenancy by namespace, not by log content);
  multi-client dual-write for migration diffing; syslog/cloud/kafka inputs;
  the node-global `limits_config` rate budget (kubescrape's limits are
  per-file); journal fields beyond
  unit/priority/identifier/pid/transport (the reader deliberately fetches
  only those, one cgo copy each; another field is a code change). Promtail's
  `regex` stage over `filename` maps to per-source `pathAttributes`
  (path-regex → resource attributes) plus static per-source `attributes` for
  per-subtree labelling.
- **Deeply invested in those**: Alloy is Grafana's supported migration
  target and the path of least resistance
  ([migration guide](MIGRATING-FROM-ALLOY.md) if you change your mind).

## Choosing

- **On Kubernetes, shipping OTLP, wanting zero-config attribution and strong
  delivery guarantees with minimal API-server load** — kubescrape is built
  for exactly this and is the smallest-config option.
- **Need syslog/kafka/cloud inputs, Windows, or remote-write** — use Vector,
  Fluent Bit, Alloy, or the OTel Collector; or run
  kubescrape for node collection in front of a central collector that does the
  rest. (Cross-node tail sampling is no longer on this list either: the
  `tailSampling` section on the trace tier buffers a trace's spans on its
  owning shard and decides it whole — see the trace-tier rows above.
  Service graphs are likewise off it: the trace tier
  shards spans by trace id onto a small StatefulSet, the way Tempo's
  metrics-generator does, and emits the `traces_service_graph_*` series
  Grafana's Service Graph view queries. Tail sampling still is — the routing
  that puts a whole trace on one shard is there, but nothing buffers a trace
  until it is complete.)
- **Metrics-first with InfluxDB or a zoo of non-Kubernetes inputs** —
  Telegraf (see the note under [Metrics](#metrics)).
- **Deeply invested in Prometheus relabel_configs / Loki** — Alloy is the
  path of least resistance ([migration guide](MIGRATING-FROM-ALLOY.md) if
  you change your mind).

## Honest gaps

The transform language (Starlark, per exported batch) is deliberately
narrower than VRL/OTTL and has none of their ecosystem; traces need their own
sharded tier, which is then a hard cluster-wide dependency for them (no
per-node fallback, and each span crosses the network twice at full fidelity),
its edges miss what is uninstrumented (an uninstrumented callee appears only as
a virtual node) and its ring membership is what config says rather than what is
alive; tail sampling exists on that tier but is the one place in kubescrape that
acks a span before delivering it, so a hard kill of a shard loses whatever is
buffered-but-undecided (a decided keep is spooled when the tier runs a disk
buffer); the
ServiceMonitor/PodMonitor subset interprets neither target `relabelings`
nor the Probe/ScrapeConfig CRDs; no input breadth; OTLP-only output;
single-core log ingestion per node; Linux/containerd focus; and years less
production soak time than any comparator — the invariants are tested
(race-tested suite, crash/rotation/power-loss cases) but the field mileage
is not.

Two things are **deliberately out of scope** — kubescrape does not try to
replace the standard component for each:

- **Host/node system metrics** (`/proc`, node_exporter territory): run a
  node_exporter DaemonSet and scrape it via `prometheus.io/*` annotations or a
  PodMonitor.
- **kube-state-metrics generation**: kubescrape does not produce KSM series —
  deploy kube-state-metrics itself and scrape it. kubescrape's metrics
  splitters then re-attribute its output into per-object resources; only the
  generation is out of scope, the split/enrich capability stays.

The systemd journal (`-journald`) is the deliberate **in-scope** exception to
the host/node line: node/system *logs* (kubelet, containerd, systemd units)
are collected even though host *metrics* are not. The distinction is
operational necessity — those unit logs are how you debug a node's Kubernetes
plane, and on many distros they exist only in the journal — and it is worth
the cgo dependency on libsystemd (a non-static agent binary) that host
metrics, already covered by node_exporter, are not.
