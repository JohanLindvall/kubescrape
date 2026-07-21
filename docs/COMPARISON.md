# kubescrape vs. similar tools

How kubescrape compares to the agents commonly deployed for the same job:
**Grafana Alloy** (and its predecessor **Promtail**), **Vector**,
**Fluent Bit**, the **OpenTelemetry Collector** (filelog + prometheus +
k8sattributes), and **Prometheus/vmagent** for the scrape path.

Scope disclaimer: kubescrape is deliberately narrower than most of these — a
Kubernetes-only, Linux-only, OTLP-only node agent plus a cluster metadata
service. The comparison is against the *overlapping* feature set: collecting
container logs and Prometheus metrics on Kubernetes nodes, attributing them
with Kubernetes metadata, and shipping them via OTLP. Comparator behavior is
described as of early 2026; measured numbers are from this repository's
benchmarks (see [Performance](#performance)).

## Architecture

The structural difference from every comparator is **where the Kubernetes
API is watched**:

| | API-server load | Metadata timing |
|---|---|---|
| **kubescrape** | **one** watcher set per cluster (the metadata service); agents talk HTTP to it, with ETag caching | container lookups **block** briefly until the kubelet posts the status — no unattributed startup logs |
| Vector `kubernetes_logs` | one watcher per node | local cache; races pod startup |
| Fluent Bit `kubernetes` filter | API calls per node (with local cache) | cache misses hit the API server |
| OTel Collector `k8sattributes` | one watcher per collector instance (per node for agent-mode) | local cache; races pod startup |
| Alloy / Promtail | one watcher set per instance/replica | discovery-time relabeling only |

On a 500-node cluster that is 500 pod-watch streams versus one. The
trade-off: kubescrape's agents depend on the metadata service being
reachable (lookups block, then retry; log data is never lost — files are not
consumed until they can be attributed).

Also structural: the tailer *blocks per container ID* with tombstones for
deleted pods, so logs from containers that live seconds (CronJobs) are still
fully attributed — the cache-race gap that per-node watchers accept.

## Feature matrix

### Log collection

| | kubescrape | Alloy/Promtail | Vector | Fluent Bit | OTel filelog |
|---|---|---|---|---|---|
| CRI parsing + partial-line rejoin | ✔ built-in | ✔ stage | ✔ | ✔ | ✔ operator |
| Multiline (stack traces) | ✔ 7 languages, zero config, literal-prefiltered | config per format | config (VRL) | config | config |
| Multiline **across rotations** | ✔ incl. across crashes | ✘ breaks at rotation | ✘ | ✘ | ✘ |
| Rotation: rename / copytruncate / inode reuse | ✔ (inode + head fingerprint) | rename only | ✔ | ✔ | ✔ |
| Auto enrichment (timestamp, severity, trace IDs, exceptions) | ✔ zero config ([enrich](https://github.com/JohanLindvall/enrich)) | config stages | config (VRL) | config parsers | config operators |
| Structured-field lifting (JSON/logfmt → attributes) | ✔ `logAttributes` | ✔ stages | ✔ VRL | ✔ | ✔ |
| Drop / keep / sample rules | ✔ `logs.rules` (shared selector DSL, `__severity__`) | ✔ stages | ✔ VRL | ✔ | ✔ OTTL |
| Per-file rate limiting | ✔ pause (lossless) or drop | ✔ limit stage | ✔ throttle | ✔ | ✘ |
| Log-derived metrics | ✔ counter/gauge/histogram/summary, windowed aggregations, **pushed OTLP with per-pod resources** | ✔ metrics stage (local exposition) | ✔ log_to_metric | ✔ | ✔ count connector |
| Arbitrary host files / gzip archives | ✔ sources + gzip | ✔ / ✘ | ✔ / ✘ | ✔ / ✘ | ✔ / ✔ |
| journald | ✔ native (libsystemd) | ✔ | ✔ | ✔ | ✔ |
| Body rewriting / templating | ✘ deliberate (body is never modified) | ✔ | ✔ VRL | ✔ | ✔ OTTL |
| General transform language | ✘ | River/stages | **VRL** | Lua/filters | **OTTL** |

### Metrics

| | kubescrape | Alloy/Prometheus/vmagent | OTel prometheus receiver |
|---|---|---|---|
| Annotation discovery (`prometheus.io/*`) | ✔ incl. Services with `targetPort` translation, comma port lists | via relabel config | via SD config |
| ServiceMonitors | ✔ subset (port/targetPort/path/scheme) | ✔ full | ✔ via TA |
| Relabeling | keep/drop/label rules + splitters (narrower, declarative) | ✔ full relabel_configs | ✔ |
| KSM re-attribution (per-object resources + metadata enrichment) | ✔ **splitters** — unique | ~400 lines of OTTL/groupbyattrs | manual OTTL |
| cadvisor/kubelet | ✔ exact-incarnation attribution via cgroup ID | label-based | label-based |
| Mimir job/instance identity conventions | ✔ built-in (`service.namespace`/`instance.id`, collision prefixes) | manual transforms | manual |
| Streaming constant-memory parse | ✔ (100k-series targets) | ✔ | buffers families |
| Exemplars | ✔ opt-in | ✔ | ✔ |
| Remote-write output | ✘ (OTLP only) | ✔ | ✔ |

### Other signals & delivery

| | kubescrape | Alloy | Vector | Fluent Bit | OTel Collector |
|---|---|---|---|---|---|
| OTLP ingest (push) with k8s enrichment | ✔ logs/metrics/traces, peer-IP fallback, batching | ✔ | ✔ | ✔ | ✔ |
| Traces | passthrough + enrichment only | ✔ full | ✔ | ✔ | ✔ full (sampling etc.) |
| K8s events | ✔ (service-side, series-aware) | ✔ | ✘ | ✔ | ✔ receiver |
| Log delivery | **ack-gated at-least-once** + rewind; offsets never pass unacked data | positions synced on timer (loss/dup window) | ✔ e2e acks + disk buffers | offsets on read (not ack) | checkpoints on read (not ack) |
| Disk buffering | ✔ both signals (fsync'd frames, checksummed cursor, poison-batch handling) | in-memory queue (WAL never GA) | ✔ mature | ✔ filesystem storage | ✔ file storage ext |
| Compression | gzip (klauspost) | snappy/gzip | ✔ several | ✔ | ✔ several |
| Backpressure to source | ✔ rewind = files wait on disk | partial | ✔ | ✔ | partial |
| Inputs beyond k8s/journal (syslog, kafka, statsd, cloud…) | ✘ | ✔ | ✔✔ | ✔✔ | ✔✔ |
| Windows / macOS | ✘ Linux only | ✔ | ✔ | ✔ | ✔ |
| Remote config (OpAMP/fleet) | ✘ | ✔ | ✘ | ✔ | ✔ |

## Performance

Measured on the same machine (AMD Ryzen 7 8840HS, Go 1.25) with this repo's
committed benchmarks; comparator figures below the table are order-of-
magnitude from public benchmarks, not same-machine measurements.

**Log pipeline, per line** (`BenchmarkIngestLine` / `BenchmarkIngestFlush`):

| Stage | ns/line | allocs |
|---|---|---|
| CRI + multiline pipeline + offset ledger | 486 | 2 |
| + OTLP record building + export | 733 | 6 |
| + automatic enrichment (production shape) | 1,948 | 7 |
| + log-metrics + drop rules | 2,387 | 9 |

≈ **500k enriched lines/s/core**. Typical published figures for full parse+
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

## Delivery semantics in one paragraph

kubescrape commits log offsets **only after the collector acknowledges the
batch**, never past lines still buffered in the multiline pipeline; failures
rewind and re-read. Multi-line groups survive rename rotations *and* crashes
mid-rotation (rotated-away files are recorded in the checkpoint and re-read
in order). The disk buffer fsyncs every frame, checksums its cursor, rolls
back partial writes (ENOSPC), and classifies permanent rejections so a poison
batch cannot wedge a signal. This is Vector-class delivery; it is strictly
stronger than Promtail (timer-synced positions), Fluent Bit and the OTel
filelog receiver (offsets committed on read, not on acknowledgment).

## Choosing

- **On Kubernetes, shipping OTLP, wanting zero-config attribution and strong
  delivery guarantees with minimal API-server load** — kubescrape is built
  for exactly this and is the smallest-config option.
- **Need syslog/kafka/cloud inputs, Windows, remote-write, a transform
  language, or trace processing** — use Vector, Fluent Bit, Alloy, or the
  OTel Collector; or run kubescrape for node collection in front of a
  central collector that does the rest.
- **Deeply invested in Prometheus relabel_configs / Loki** — Alloy is the
  path of least resistance ([migration guide](MIGRATING-FROM-ALLOY.md) if
  you change your mind).

## Honest gaps

No general transform language (body rewriting is a deliberate non-goal); no
trace processing beyond enrichment/passthrough; no input breadth; OTLP-only
output; single-core log ingestion per node; Linux/containerd focus; and years
less production soak time than any comparator — the invariants are tested
(race-tested suite, crash/rotation/power-loss cases) but the field mileage is
not.
