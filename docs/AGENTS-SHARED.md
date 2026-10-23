# Shared contracts

[Repository guidance](../AGENTS.md)

Prefer these shared implementations whenever several packages need the same
decision. Keep caller-specific limits and refusal policies at the caller.

## Primitives and package boundaries

| Concern | Implementation and contract |
|---|---|
| Mounted bearer tokens | `internal/bearer`: client `File`, receiver `Rotating`, constant-time `Authorized` |
| Optional config durations | `internal/config.Duration`: empty = default, negative = error, explicit zero = off unless `Positive` is required |
| Glob validation | `internal/config.Glob` validates with `path.Match` at parse time |
| Prometheus durations | `internal/promdur`: `Interval` is the shared parsed-and-positive rule |
| Shared CLI flags | `cli.RegisterOTLPFlags`, `RegisterObsFlags`, `otlpexport.ConfigFromFlags`, `OTLPFlags.SummaryAttrs` |
| Listener collisions | `cli.CheckListeners` compares normalized ports and equal/wildcard hosts |
| Repeated complaints | `internal/logdedupe`: `Throttle`, bounded per-key `Table`, failure-run `Outage` |
| Rune-safe clipping | `internal/clip`: `Runes`, `Marked`, `Ellipsis` |
| Regex compilation cost | `internal/regexcost.Insts` bounds the parse tree before compilation |
| Peer addresses and forwarding headers | `internal/peerip`: `From`, `Canonical`, `ForwardingHeader` |
| Shared log processing | `internal/agent/logchain`; see [Logs](AGENTS-LOGS.md) |
| HTTP receive and relay errors | `otlpingest.BodyReader`, `WriteBodyError`, `WriteProto`, `GRPCForwardStatus`, `HTTPForwardStatus` |
| Monitor secret references | `servicemonitors.Endpoint.secretRefs`, used for parsing and allowlisting |

`pkg/promparse`, `kubemeta`, `metaclient`, `logattrs`, `otlpsplit` and `cgroupid`
are public and must not import `internal/`; `pkg/boundary_test.go` enforces
this. `metaclient.New(Config)` receives all configuration at construction, has
no metrics dependency, exposes `Observe` and `NewTransport`, and reports an
undecodable response as `*DecodeError`. Kubernetes object conversion stays in
`pkg/kubemeta/kubeconvert`. Standalone config-file loaders belong in test files.
Keep `cli.KubeConfig` in `internal/cli/kubecfg` to preserve optional dependencies.

## Authentication, hashing and runtime limits

Bearer refresh claims use a flag while I/O runs with the mutex dropped, never
an expiring deadline that permits concurrent blocked reads. `File.Token` waits
at most `refreshWait`, including concurrent first use; nonwaiting gRPC auth
uses `Rotating.Cached`. Failed refreshes retain the last good value and report
an error counter plus throttled warning. `NewRotating` reads fatally before a
receiver can listen; `NewFile` does not read until requested. `Authorized`
compares every candidate without early exit and never accepts an empty token.
Azure issuer-expiry tokens and per-session connection strings are separate.

Identity hashing uses `haste/xxh3` at 128 bits: log-metric series, HTTP ETags
and stuck-buffer payloads must not silently merge. Dispersal uses 64-bit
`haste/rapidhash`: sampling, selector memoization and tailer body hashes.
`tracehash` shares unsalted thresholds and rate buckets between head and tail
sampling so probabilities nest across shards, retries and restarts. Changing
the hash changes the sampled population. The service-graph ring deliberately
uses Tempo's FNV-1.

Both mains call `cli.SetMemoryLimit` after startup logging. It sets the Go
soft memory limit to 90% of this container's own cgroup limit. An explicit
`GOMEMLIMIT`, including `off`, wins. Read the process's own path for both
cgroup namespace layouts; use the mount root only as the cgroup-v1 fallback.
Never use an ancestor's limit or host MemTotal for this runtime limit. An
uncapped container remains uncapped; a node hierarchy mounted over
`/sys/fs/cgroup` warns. Keep GOGC unchanged. Tailbuffer sizing uses the same
`CgroupMemoryLimit` reader but has its own MemTotal fallback.

`logdedupe.Table` suppresses new keys at saturation; it must not clear the
table. Existing keys can re-warn. `Outage` handles first failure, throttled
repeats and recovery, carrying `failures` and `outage`. The exporter retains
its own narrative because it also distinguishes error-class changes.

`clip` must not replace `metrics.truncLabelCut`, which preserves invalid bytes
for identity, or `cumagg.Retain`, which must copy retained data.

## Configuration and attributes

`cmd/kubescrape-agent/config.go` owns the single `-config` YAML document;
`sectionNames()` is the authoritative inventory. Add config surfaces there,
using their package's typed config, and compile them in `compileConfig`.
`-check-config` validates without listeners, informers or filesystem-dependent
credential reads. Durations in YAML-facing config structs are strings parsed
through the shared duration helpers.

`-test-config` runs sample lines through scrub → attribute lifting → enrichment
→ log metrics → rules → transforms, with the same `emit_metric` bridge as
runtime. Transforms have a separate hot-reloaded file; see
[Transport](AGENTS-TRANSPORT.md).

`attrs.Builder` applies defaults → static values → templates → global filter.
Every resource-producing path calls `Attrs.Build`, including prepopulated
splitter, ingest and log-metric resources. Pipeline names come from
`attrs.pipelineNames`; `enable`/`disable` are global lists and are invalid
inside a pipeline section. Node metadata comes from `startNodeInfo`.

`attrs.ServiceName` derives workload owner → pod name. `attrs.Identity` fills
unset service namespace and instance ID; instance fallback is container ID,
pod UID[/container], namespace/pod[/container], then node. Preserve an explicit
template value. `attrs.PrefixInstance` separates describing exporters from the
objects they describe. Precedence is explicit pipeline setting → built-in
default → base setting; cadvisor and summary have built-in prefixes.

Summary pod/container/volume resources use the cadvisor builder; only summary
node resources use the summary builder. Resource equality across related
pipelines is part of their join contract.

## Self-telemetry and logging

`internal/obs` registers self-metrics on `metrics.Registry`. Both binaries push
them over OTLP by default. The dedicated `-metrics-listen` endpoint always
serves Go/process metrics and serves `kubescrape_*` metrics only when
`-self-metrics-interval=0`. Keep health/debug, metrics and optional pprof
listeners separate.

`internal/selfmeta.Poll` resolves metadata asynchronously, backs off and keeps
last-good results. `Wrap` fills absent attributes at export time, including
final exports; static attributes still build with a nil pod. Set the process's
namespace at startup, not after the first resolution, so cumulative identity
does not change. Singletons use their pod as the instance; the per-node agent
uses its node. Leave `.Container` nil instead of guessing under sidecar
injection. Self-telemetry bypasses namespace routing and the debug tap.

The agent uses `/v1/self` without an HTTP proxy and falls back on 404 to a
namespace/name lookup for hostNetwork, SNAT and dual-stack. The metadata
service resolves locally after pod cache sync. Refreshes update labels; the
hostname verification is reported rather than used as a gate. Register the
resolution gauge only when lookup runs, and keep these lookups separate from
container-attribution request metrics.

Both binaries install `cli.NewLogfmtHandler` before parsing flags. Output is
logfmt only; there is no `-log-format`. Sanitize keys at every entry point,
including groups and `LogValuer` results, using the handler's implementation.
Keep values intact. `internal/cli/logfmt_test.go` round-trips the repository's
log shapes through its reader.

Use the key vocabulary in `internal/cli`'s package documentation: `error`,
`path`, `tokenFile`, `namespace`, `container`, `peer`, `elapsed`, and the other
existing keys. Do not introduce alternate spellings. Never log credentials.
Guard expensive Debug arguments with `log.Enabled`; hot paths use counters
and throttled aggregate logs, not per-item logging.

Levels: Error means a pipeline is dead and needs action; Warn means an
unexpected but handled condition; Info covers lifecycle; Debug explains
per-object decisions. Startup and `-check-config` share five effective-settings
lines: configuration, destinations, listeners, identity and limits. Readiness
warns after its grace period with named pending gates through `cli.WatchGates`.

Error-formatting helpers preserve vet checking and the cause:

```go
fmt.Errorf("%s: %w", where, fmt.Errorf(format, args...))
```

Forward `format, args...` verbatim to the inner call. Do not add panic recovery;
malformed inputs must be fixed at their parsing boundary.

## Helm configuration

Common flags are values; other flags use `extraArgs`. Shared flag blocks live
once in `_helpers.tpl`, included on their own line for manifest checking.
`agent.config` renders into one mounted ConfigMap with a checksum annotation.
Render integer byte values with `int64`, and validate string byte limits before
conversion through `kubescrape.logsMetricsMaxBytes`. A unit-bearing string must
fail instead of converting to zero. Quote comma-containing extraArgs values
inside YAML flow sequences.

`agent.logsExcludeNamespaces` defaults to null, which excludes the release's
namespace and the effective in-cluster **logs** destination's namespace.
`agent.config.export.logs.endpoint` replaces the flag base when set; metrics,
traces and routing destinations do not add exclusions. Explicit `[]` means
exclude nothing. Match only actual cluster DNS forms: two-label service names
or names with a `.svc` component, taking the namespace immediately before svc.
Do not infer Kubernetes namespaces from arbitrary public hostnames. Preserve
the export-logs-replaces-base rendering fixture and validate with the pinned
Helm and chart golden tests.
