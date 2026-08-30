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
— its defaults are authoritative because they cannot drift. Installing for the
first time? [FIRST-RUN.md](FIRST-RUN.md) is the runbook: what must exist
before you install, the smallest useful configuration, what to watch in the
first ten minutes, a symptom→cause table, and which counters are worth an
alert.

Two machine-readable schemas, both generated/enforced by tests:
[agent-config.schema.json](agent-config.schema.json) is a JSON Schema for the
`-config` YAML, generated from the very structs the file decodes into
(`additionalProperties: false` matches the strict decoder exactly) — point
your editor at it (`# yaml-language-server: $schema=…`) or validate in CI.
The RECURSIVE sections — `resourceAttributes.pipelines` and `tailSampling`'s
`and`/`composite` sub-policies — are expressed with `definitions`/`$ref` and
are as strict as the rest, rather than the open `{}` a self-referential struct
would otherwise render as;
the chart carries a `values.schema.json`, so a typo'd Helm value fails
`helm install`/`template` instead of being silently ignored. Structural
only — `-check-config` remains the semantic validator (regexes, durations,
bounds, templates).

- [Toolchain and build floor](#toolchain-and-build-floor)
- [Build variants (optional pipelines)](#build-variants-optional-pipelines)
- [Runtime memory and CPU (GOMEMLIMIT, GOMAXPROCS)](#runtime-memory-and-cpu-gomemlimit-gomaxprocs)
- [Logging](#logging)
- [Accepted security residuals](#accepted-security-residuals)
- [Metadata service](#metadata-service)
- [Agent: general](#agent-general)
- [Agent: OTLP export](#agent-otlp-export)
- [Agent: log collection](#agent-log-collection)
- [Unified config file (`-config`)](#unified-config-file)
- [Agent: log sources](#agent-log-sources)
- [Agent: journald](#agent-journald)
- [Agent: high-frequency cgroup sampling](#agent-high-frequency-cgroup-sampling)
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

## Toolchain and build floor

**Building kubescrape needs Go 1.26.6 or newer.** `go.mod` says `go 1.26.6`,
and nothing in the source needs that language version — it is a **security
floor**. `govulncheck` at the previous `go 1.26.3` reported ten reachable
standard-library advisories; six of them have no earlier fix, which is what
makes the floor `.6` rather than `.4`:

| Advisory | Package | Reached from |
|---|---|---|
| [GO-2026-6218](https://pkg.go.dev/vuln/GO-2026-6218) | `net/url` — quadratic path resolution | every `metaclient` fetch (`http.Client.Do`) |
| [GO-2026-6090](https://pkg.go.dev/vuln/GO-2026-6090) | `crypto/tls` — unbounded post-handshake messages | every TLS listener and dialer, including the unauthenticated ingest ports |
| [GO-2026-6089](https://pkg.go.dev/vuln/GO-2026-6089) | `net/http` — `ReadHeaderTimeout` not applied on the h2c check | every `http.Server` this repo starts |
| [GO-2026-5972](https://pkg.go.dev/vuln/GO-2026-5972) | `encoding/asn1` — recursion depth | `otlpexport`'s mTLS `LoadX509KeyPair` |
| [GO-2026-5026](https://pkg.go.dev/vuln/GO-2026-5026) | `x/net/idna` vendored into `net/http` — Punycode acceptance | `metaclient`, the scraper's per-target clients |
| [GO-2026-6091](https://pkg.go.dev/vuln/GO-2026-6091) | `html/template` | only `hack/nhexporter`, the e2e fixture — not a shipped binary |

The other four (`GO-2026-5856`, `GO-2026-5039`, `GO-2026-5038`,
`GO-2026-5037`) are fixed in 1.26.4/1.26.5 and are closed by the same bump.
`govulncheck -tags journald,azure,events ./...` is clean as of this writing; re-run it
rather than trusting the directive, and raise the floor by editing `go.mod` —
nothing else in the tree states it.

**The directive is enforced, and it fails loudly rather than silently.** Under
the default `GOTOOLCHAIN=auto` a machine holding an older Go downloads 1.26.6
and re-execs into it; with `GOTOOLCHAIN=local` the build is refused outright
(`go.mod requires go >= 1.26.6`). There is no configuration in which an older
toolchain quietly produces a binary carrying those ten. What the directive does
**not** govern is everything that is not Go — see
[Accepted security residuals](#accepted-security-residuals) for the floating
base-image tags, which are how a stale glibc or libsystemd can still reach a
shipped image whose Go binary is current.

## Build variants (optional pipelines)

Three agent pipelines are compiled in through Go **build tags**, and the default
set lives in the Makefile — so a stock `make build` / `make image` contains all
three and nothing about the shipped artifacts has changed:

```sh
TAGS ?= journald,azure,events   # Makefile; passed to build, test, vet, lint and image
```

| Build | journald | azure | events | Stripped agent |
|---|---|---|---|---|
| `make build` | ✔ | ✔ | ✔ | 59.1 MB — the default: agent is `CGO_ENABLED=1`, dynamically linked |
| `make build TAGS=azure,events` | — | ✔ | ✔ | 58.9 MB, and **cgo-free and static**; no libsystemd needed |
| `make build TAGS=journald,events` | ✔ | — | ✔ | 54.1 MB — **11 franz-go packages** (≈5 MB) gone |
| `make build TAGS=journald,azure` | ✔ | ✔ | — | **26.3 MB** — 926 → 470 dependency packages (`k8s.io/`+`sigs.k8s.io/` **412 → 8**), −31.4 MiB / −55.6% |
| `make build TAGS=` | — | — | — | 21.0 MB |

* **`journald`** compiles in [the systemd journal reader](#agent-journald). It
  is the *only* reason the agent needs cgo — it links libsystemd through
  `coreos/go-systemd/sdjournal`, which is why the default image is
  `distroless/base` plus seven copied `.so` files. Without the tag the agent
  links statically, which is what `make image-static`
  ([Dockerfile.static](../Dockerfile.static)) puts on `distroless/static`.
* **`azure`** compiles in [the Azure diagnostics consumer](#agent-azure-diagnostics).
  Its Kafka client rides in every DaemonSet image for a pipeline that only ever
  runs in the one-replica singleton Deployment.
* **`events`** compiles in [the Kubernetes events reader](#agent-kubernetes-events) and its
  leader election. This is the same argument as `azure` at six times the size:
  they are the *only* reason the agent links `k8s.io/client-go` — the binary is
  otherwise documented, correctly, as talking to no Kubernetes API — and that is
  412 packages and **half the stripped binary**, carried on every node for a
  pipeline that only ever runs in the singleton Deployment. `KubeConfig` lives in
  its own `internal/cli/kubecfg` package for the same reason: `internal/cli` is
  imported by *every* build, so a `clientcmd` import there would have pinned
  client-go into the very variant the tag exists to slim.

`make verify-tags` asserts the exclusions really happen (no cgo without
`journald`, no franz-go without `azure`, no `k8s.io/client-go` without `events`)
— which is what makes "the agent talks to no Kubernetes API" a property a build
can fail on rather than a claim in a document.

> **What the table does NOT say: the shipped image is unchanged.** `TAGS`
> defaults to all three, so `make image` builds exactly what it always did.
> The image also carries *both* binaries, and the metadata service links
> client-go legitimately, so dropping `events` takes the image's binary
> payload from **112.98 MB to 80.09 MB (−29.1%)** — a real saving on every
> node's pull, but not the −55.6% the agent column shows. Every figure in
> this section was re-measured 2026-08-29 on go1.26.6 with
> `-trimpath -ldflags="-s -w"` (`CGO_ENABLED=1` for the `journald` rows,
> `0` otherwise), two byte-identical builds per arm.

> **A bare `go build ./cmd/kubescrape-agent/` passes no tags and builds an agent
> with NONE of the three.** `make` is the supported path; otherwise pass `-tags`
> yourself. The same applies to `go test` and `go vet` — `make test` passes
> `$(TAGS)` so the real code, not the stubs, is what gets tested.

**The flags never disappear.** `-journald`, `-azure-diagnostics` and `-events`
are defined in every build (the shipped manifests pass them, and a missing flag would make
the process exit 2 with `flag provided but not defined`). Enabling a pipeline
the binary does not contain is instead a **startup error naming the tag**:

```
-journald is set, but this kubescrape-agent was built WITHOUT the "journald"
build tag, so the systemd journal reader is not compiled into it: either drop
-journald, or use an agent built with the tag (`make build` / `make image`
default to TAGS=journald,azure,events and contain every pipeline; a bare `go
build` contains none of them)
```

`-check-config` raises the same error, so a rollout fails the dry run rather
than the DaemonSet. Every start (and `-check-config`) also reports which binary
it is — `optionalPipelines=journald,azure,events`, or `(none)`.

**The `-config` file is not affected by any of this.** No section belongs to any
of the three, so one ConfigMap stays decodable by every variant; only *enabling*
an absent pipeline fails.

## Runtime memory and CPU (GOMEMLIMIT, GOMAXPROCS)

**Both binaries set a Go soft memory limit from their own container's cgroup
limit at startup. There is no flag; it is on, and the way to change it is the
container's `resources.limits.memory` or the `GOMEMLIMIT` environment
variable.** It has no flag and no chart value, so it appears in no table on
this page — which is exactly why it gets a section.

Go's garbage collector sizes the next collection at `GOGC` percent above the
*live* heap and knows nothing about the cgroup it runs in. The agent's scrape
cycle takes `heap_alloc` from about 9 MB to about 58 MB every `-scrape-interval`
and the heap goal follows it; nothing in that loop knows the DaemonSet ships
`limits.memory: 512Mi`. A cycle that needs more than the limit is therefore not
collected harder — it is **OOMKilled**, losing the tailer's unflushed batch and
every buffered span on whichever node the fat target happened to land on.

A soft limit makes the heap goal `min(GOGC goal, limit goal)`, so it can only
ever make the GC run *earlier*:

* **When it does not bind it costs nothing.** A workload whose peak sat far
  below the limit measured 19.70 GC cycles per GB allocated both with the limit
  and without it (10 interleaved rounds).
* **When it binds it costs GC and buys survival.** A burst at the boundary of a
  384 MiB cgroup was OOMKilled 3 times in 16 runs without the limit and 0 times
  in 16 with it, at 10.7 → 17.2 cycles per GB.

  > That pair is the campaign's single most operator-visible claim and it rests
  > on **one** harness run; it has not been independently reproduced. Treat the
  > direction as established and the exact counts as one measurement.

**What it reads.** 90% of *this container's own* cgroup memory limit —
`/sys/fs/cgroup/…/memory.max` (v2) or `memory.limit_in_bytes` (v1), named by
`/proc/self/cgroup`, with the mount root as the fallback for the cgroup v1
bind-mount layout. It never reads an **ancestor's**: with the kubelet's default
`--enforce-node-allocatable=pods`, `kubepods.slice` carries a limit of the
node's whole allocatable memory, so walking up and taking the minimum would hand
an *uncapped* container ~0.9× the **node's** RAM as its heap goal. The remaining
10% is for everything the Go runtime cannot see — the binary's mapped text, the
C heap of libsystemd under the `journald` tag, and anything else the kernel
charges to the cgroup.

**When it does nothing, by design.** An uncapped workload gets no limit and no
warning: the metadata service ships with **no** `limits.memory` on purpose (its
footprint scales with the cluster, and a number picked in a values file is a
number picked without knowing the cluster). If you want the insurance there,
set a memory limit or `GOMEMLIMIT` — and note that `GOMEMLIMIT` in the
environment always wins, checked through the runtime's own current value rather
than `os.Getenv`, so any spelling the runtime accepted takes precedence.

**`GOGC` is left alone.** A soft limit can only lower the heap goal, so it is
pure tail insurance; raising `GOGC` would trade memory for CPU, which is not a
trade this code can make on an operator's behalf.

**`GOMAXPROCS` needs nothing.** Go has derived it from a cgroup CPU limit since
1.25 and `go.mod` pins 1.26 (verified: `CPUQuota=50%` yields `GOMAXPROCS=2`), so
an operator adding `resources.limits.cpu` is already handled and
`automaxprocs` would be a dependency for a fixed bug.

**What you see.** One Info line per process, right after the build-identity
line:

```
level=INFO msg="Go soft memory limit set from the cgroup memory limit" limitBytes=482344960 cgroupLimitBytes=536870912 share=0.9 path=/sys/fs/cgroup/memory.max note="a heap excursion now costs GC instead of an OOMKill; set GOMEMLIMIT to override"
```

An already-set `GOMEMLIMIT` logs that it is being left alone. An uncapped
workload logs nothing at Info — it is a Debug line, because an uncapped
workload is a legitimate documented shape here and a fleet-wide warning about a
deliberate choice is noise on every start.

## Logging

Both binaries log **logfmt on stderr, always**. There is no format flag — a
`-log-format` existed once and was removed — so one parser reads every line
every component emits. Both binaries route every OTHER logger linked into
the process through the same handler rather than interleaving a second
format into the same stream: client-go's klog (leader election, reflector
backoffs, watch errors) and grpc-go's grpclog (the OTLP exporter's client,
the ingest listeners, the trace tier) — the latter matters because grpc's
default logger writes its connection failures straight to stderr in its own
format, at its default severity, needing no environment variable. Only grpc's
ERROR class (the one its own default logger prints) keeps a level the default
prints: its INFO chatter (channel and resolver state transitions) and its
WARNING class are mapped to `debug`, and its verbose `V(n)` sites are gated off
unless `-log-level=debug`, so the routing changes the format without changing
the volume. WARNING is at `debug` because part of that class is **peer-driven**
— grpc renders a rejected metadata header's name and value verbatim for any
header any client sends, on ingest listeners that are unauthenticated by design
— so at a printed level an unauthenticated sender would choose both the rate
and the size of lines in your log. Every grpc-rendered message is clipped into
the record whatever the level, for the same reason. `-log-level=debug` shows
the whole class, including the `addrConn.createTransport failed to connect`
line that says why nothing reaches the collector.

```
time=2026-08-29T11:23:14.731+02:00 level=INFO msg="effective listeners" listen=:8081 metricsListen=:9090 pprofListen=""
```

**Upgrade note:** the flag is *gone*, not ignored. Go's `flag` package uses
`ExitOnError`, so a leftover `-log-format=json` in `agent.extraArgs`,
`service.extraArgs` or a hand-edited manifest is
`flag provided but not defined: -log-format` and exit 2 — a CrashLoop on
exactly the deployments most likely to have set it. Grep your values files
before upgrading. The shipped chart and `deploy/` manifests no longer pass it.

### Levels

`-log-level` takes `debug`, `info`, `warn` or `error` (default `info`) on both
binaries. The levels mean specific things, and the meanings are what make the
default safe to run:

| Level | Means | Steady state |
|---|---|---|
| `error` | a pipeline is dead or data was lost, and a human must act | silent |
| `warn` | something unexpected that the code HANDLED — a fallback, a refusal, a truncation, a drop, a retry that eventually succeeded | silent |
| `info` | lifecycle an operator wants without asking: startup, effective configuration, listeners, readiness, leadership, shutdown | **quiet** — a few lines at startup and then nothing |
| `debug` | the per-object decisions that answer "why did it do THAT?": which file was skipped and why, which target was dropped by a hook, which cadvisor row could not be attributed, which attribute a template omitted | one line per object per cycle |

`info` staying quiet in steady state is a property, not an accident: every
condition that can PERSIST is throttled (see below), so a healthy agent
produces no log volume at all after startup. That is what makes a nonzero rate
of `level=WARN` a usable alerting signal on its own.

**Raising the level in production.** `debug` is per-object and per-cycle, so on
a busy node it is genuinely loud — a line per tracked log file per discovery
pass, a line per unresolved cadvisor row per scrape. It is safe (the hot paths
carry no per-item logging at any level; see below), but budget for the volume:
raise it on ONE pod, not the fleet. On the agent the level is a flag, so
raising it is a restart — `kubectl -n <ns> set env` will not do it. Two
alternatives that need no restart at all and usually answer the question
faster:

* the **debug endpoints** — `GET /debug` on either binary indexes what that
  process serves; `/v1/explain/{namespace}/{pod}` answers "why is this pod (not)
  scraped?" against the same code path that derives the targets;
  `/debug/tailer` and `/debug/targets` are the agent's per-file and per-target
  state; `/debug/otlp` streams what the agent is actually exporting.
* the **metrics** — most of what `debug` explains has a counter with a `reason`
  or `outcome` label carrying the same classification, and the counter is
  fleet-wide where the log line is one pod's.

### Reading the output

It is logfmt, so `key=value` pairs with Go-style quoting for values containing
spaces, `=` or quotes. Anything that reads logfmt reads it:
[`logfmt`](https://github.com/JohanLindvall/logfmt) (this repo's own parser —
the format guarantee is a round-trip test against it, not an assumption),
`lnav`, Grafana Loki's `| logfmt` stage, Vector's `parse_logfmt`, or
`hcat`/`humanlog` for reading it by eye. In a pinch, `grep` works, which is the
point of the vocabulary below.

Values round-trip exactly — spaces, `=`, embedded and escaped quotes,
newlines, backslashes, Windows paths, durations, empty strings, `<nil>`,
non-ASCII. KEYS are sanitized before they are written (an unsafe byte becomes
`_`), because a key holding a space or `=` would be quoted and then re-read as
two wrong pairs, silently. The one known non-round-trip is a control byte other
than `\n`/`\r`/`\t` inside a value, which is written as Go's `\x00` — byte for
byte what the reference logfmt encoder writes, and pinned by test as a known
property of the format.

### The key vocabulary

A logfmt line is only greppable if the same concept is spelled the same way
everywhere: `error=` must find every failure and `path=` every file. The
authoritative list is the package documentation of `internal/cli`; new log
calls take a key from it rather than inventing a synonym beside one. What an
operator needs to know is what to grep for:

| Key | Carries | Never spelled |
|---|---|---|
| `error` | the error | `err`, `cause`, `msg` |
| `path` / `dir` | a filesystem path / a directory | `file` |
| `url` / `endpoint` / `addr` | a URL requested / a configured destination / a LISTEN address of this process | |
| `namespace` / `pod` / `node` / `container` / `uid` / `id` | Kubernetes identity (`namespace` travels beside `pod`, never inside it) | `ns`, `podName` |
| `target` / `monitor` / `kind` | a scrape target / a ServiceMonitor or PodMonitor as `namespace/name` / the object kind, matching the metric label | |
| `signal` / `pipeline` / `route` / `unit` | `logs`/`metrics`/`traces` / a kubescrape pipeline name / a routing route / a systemd unit | |
| `reason` / `outcome` | a classification that **matches the metric label of the same name**, so a log line and a counter can be joined by eye | |
| `flag` / `note` | the flag an operator would change, spelled with its dash / a remediation hint | |
| `interval` / `timeout` / `backoff` / `wait` / `grace` / `budget` | durations, rendered as `15s` / `1m0s`, never as a bare number | |
| `attempts` / `bytes` / plural count nouns | quantities (`records`, `targets`, `entries`, `dropped`, …) | |
| `version` / `built` / `hash` | build identity / a content hash | |
| `tokenFile` / `key` | the PATH a credential is read from / a config or secret KEY NAME | |

Keys are lowercase single words where possible and lowerCamelCase for
multiword, never snake_case. `reason` and `outcome` matching the metric label
is the load-bearing one: `kubescrape_scrape_failures_total{reason}` and the
`scrape failed … reason=tls` line classify identically by construction, so the
counter tells you the rate and the line tells you which URL.

### What is deliberately NOT logged

* **Secrets.** No bearer tokens, passwords, connection strings, `Authorization`
  headers, secret VALUES or full request/response bodies — ever, at any level.
  What is logged instead is the REFERENCE: `tokenFile=` is a path,
  `key=` is a key name, a secret ref appears as `namespace/name/key`. A first
  live run is exactly when a leaked token reaches a log aggregator and stays
  there, so this is a boundary rather than a preference.
* **Per-item lines on the hot paths.** The tailer's per-line and per-flush
  path, the Prometheus parser's per-sample path, the ingest per-record chain,
  the per-span service-graph and span-metrics paths, log-metrics observation,
  scrubbing's no-match path and the tail sampler's decision path are all
  allocation-pinned by build-failing tests (`TestXxxAllocationBudget`), and a
  log call on any of them would be several times the entire per-item budget.
  What those paths produce instead is a **counter** — plus, at most, one
  throttled aggregate line from the sweep or flush that owns them ("N lines
  dropped in the last minute"), never one line per item.
* **The same condition, over and over.** A condition that PERSISTS — a target
  that keeps failing, a file that cannot be attributed, a monitor field that
  cannot be honoured — is noticed once per object per cycle per node, so an
  unthrottled line is a flood proportional to fleet size. Those go through
  `internal/logdedupe`, which is why a five-minute outage produces a first
  Error, a re-statement each minute carrying `failures=` and `outage=`, and one
  recovery Info — rather than one line every two seconds ending in a silence
  indistinguishable from a stopped pipeline. **The corollary for tooling:** a
  log-based assertion about a repeating condition must not use a fixed short
  window, because a legitimately throttled line can be absent from it while the
  condition is still true.

### What a healthy start looks like

Both binaries describe themselves at startup: a build-identity line, then five
`effective …` lines naming every pipeline, destination, listener, identity and
cap that this process will actually use. Credentials appear only as the paths
they are read from. This is real output from `kubescrape-agent -check-config`,
which emits the SAME lines from the SAME function as a real start — so a dry
run and a rollout cannot describe different agents:

```
level=INFO msg="kubescrape-agent starting" version=v1.2.3 built=2026-08-19T01:10:59Z optionalPipelines=journald,azure,events
level=INFO msg="effective configuration" role=node-agent sections=logs,logMetrics optionalPipelines=journald,azure,events pipelines="logs=on metrics=on cadvisor=on cgroupStats=off node=on summary=off journald=off ingest=off events=off azure=off serviceGraph=off" positionsFile=/var/lib/kubescrape/positions.json transformsFile="" enrich=true selfAttributes=true logLevel=info
level=INFO msg="effective destinations" metadataEndpoint=http://kubescrape.monitoring otlpEndpoint=otel-collector.monitoring:4317 otlpProtocol=grpc otlpCompression=gzip otlpInsecure=true otlpTLSSkipVerify=false otlpCAFile="" otlpBearerTokenFile="" kubeletEndpoint=https://10.0.0.5:10250 bufferDir="" bufferMaxBytes=1073741824
level=INFO msg="effective listeners" listen=:8081 metricsListen=:9090 pprofListen=""
level=INFO msg="effective identity" node=node-1 namespace=monitoring serviceName=kubescrape-agent instance=node-1 selfAttributesRefresh=1m0s selfMetricsInterval=1m0s
level=INFO msg="effective limits" scrapeInterval=30s scrapeTimeout=15s scrapeConcurrency=4 metadataWait=5s logsExcludeNamespaces=monitoring logsUnknownFiles=auto logsBatchSize=1024 logsFlushInterval=2s logsMaxEntryBytes=1048576 logsRateLimit=0 logsMetricsInterval=30s
```

`optionalPipelines` is the one that catches a build surprise: a bare
`go build` produces an agent with NEITHER optional pipeline, and
`-journald`/`-azure-diagnostics` on such a binary is a startup error naming
the tag and the rebuild rather than a flag that silently does nothing.

The metadata service emits the same five messages with its own keys
(`apiServer`, `kubeconfig`, `servicemonitors`, `monitorNamespaces`,
`waitTimeout`, `cacheTTL`, …), followed by `informer caches synced` with the
pod and container counts. Warnings on this path are worth reading rather than
scrolling past — they are the legal-but-surprising combinations, and each one
names the flag to change:

```
level=WARN msg="-kubelet-endpoint is empty, so the kubelet scrapes that depend on it are never scheduled: -cadvisor, -node-metrics. Nothing is attempted and nothing fails — no scrape counter moves and no error is logged — so the only symptom is the missing metrics. Set -kubelet-endpoint=https://$(NODE_IP):10250 (the shipped manifests and the chart do), or turn those flags off so the startup log describes what is actually collected."
```

### When a rollout will not advance

Both binaries publish `kubescrape_readiness_gate{gate}` — 1 when that startup
gate is satisfied, 0 while it is pending — and both warn after a 30-second
grace naming the gates still pending, re-warning every two minutes:

```
level=WARN msg="not ready: /readyz is 503, so a rolling update will not advance past this pod" gates=metadata-service waited=30s
level=WARN msg="not ready: informer caches have not synced, so /readyz is 503 and this replica has no Service endpoints" caches=pods,services waited=30s note="a cache that never syncs is usually a missing RBAC rule for that resource; the reflector retries it forever"
```

The metric exists because the probe body only reaches whoever can curl the pod,
and a replica stuck unready has no Service endpoints — i.e. nobody can. The
self-metrics push runs from startup regardless of readiness, so an unready
process still reports it. The agent's gates are the pipelines it actually
wired (`metadata-service`, `otlp-ingest`, `service-graph-receiver`,
`service-graph-ingest`, `azure-eventhub…`); the service's are one per informer
cache (`pods`, `services`, `replicasets`, `deployments`, `statefulsets`,
`daemonsets`, `jobs`, `cronjobs`, `namespaces`, `nodes`, plus
`servicemonitor`/`podmonitor` when watched). An ABSENT gate means that
pipeline is off, never that it is healthy.

## Accepted security residuals

An adversarial review of this repo's own security work found four things that
were argued about and **left in place**. They are written here, in the document
an operator reads, rather than only in the comment at the code that leaves
them — a residual nobody can find is indistinguishable from one nobody thought
about. Each says what it is, why the obvious closure was rejected, and what
lever you actually have.

**1. A stolen `container.id` still crosses a tenancy boundary, and no counter
moves when it does.** The application-facing OTLP ingest listeners (the
DaemonSet's `-ingest` ports and the trace tier's application ports) strip the
Kubernetes identity keys a sender declares *about itself* at first receipt, so
a pod cannot simply push `k8s.namespace.name: payments` and have
[routing](#agent-routing) — which keys tenancy on exactly that attribute —
deliver its records to the payments endpoint and `X-Scope-OrgID`. What the
strip cannot remove are the **lookup keys** (`-ingest-container-id-keys`,
`-ingest-pod-uid-keys`), because stripping what the attribution is *made of*
turns the receiver into one that resolves nothing. So the crossing survives one
hop over: the metadata service's `/v1/pods/{namespace}/{name}` is
unauthenticated by design and returns each container's id and the pod UID, and
the store's container index is cluster-wide rather than node-scoped, so a pod
that can reach both services reads a victim's `container.id`, pushes it while
declaring no namespace of its own, and the enricher **derives** the victim's
namespace — correctly, from a stolen input. Nothing is forged on the wire, so
`kubescrape_ingest_identity_stripped_total` does not move either.

It is documented rather than closed because the obvious closure fails on both
axes. Refusing to enrich an object resolved on another *node* would cost
legitimate attribution — the datapoint/split path exists precisely so a sender
can describe objects across the cluster, the trace tier receives from every node
by design, and a DaemonSet receiver behind a Service round-robins away from the
sender's own node — and it would not even close the hole, a co-located victim
being the common case on a cluster that spreads namespaces across nodes.
Narrowing the victim set while silently un-attributing honest senders is the
worse bargain. **What actually closes it** is a credential on one of the two
doors — the ingest listener, or the metadata routes that hand out the id — and
kubescrape has neither on offer: the application-facing receivers are
unauthenticated by design (every instrumented pod is a sender), and of the `/v1`
routes only `/v1/scrape-auth`, which serves Secret material, is
bearer-authenticated. **What you can do
now**: scope the receivers with a NetworkPolicy (`agent.ingest.allowFrom`,
`serviceGraph.ingest.allowFrom` — both empty by default, which means *any pod
may push*), and treat `routing`'s per-tenant endpoints as a convenience rather
than an isolation boundary. If tenants must not be able to write into each
other's streams, the split has to happen at a collector that authenticates its
senders.

**2. grpc-go's own warnings are peer-driven, which is why they are at `debug`.**
Routing grpc-go's logger into the process logger (which is what makes every line
logfmt) has to choose a level for grpc's `Warning` class, and part of that class
is transport-level and therefore driven by the *peer*: `Failed to decode
metadata header (%q, %q)` renders a header's **name and value verbatim** for any
header any client sends, before any application code runs, and
`Encountered http2.StreamError` is likewise one line per broken stream. Against
an unauthenticated ingest listener at a printed level, the sender would choose
both the *rate* (one line per attempt, with no honest throttle available — grpc
hands over an already-rendered string, so a `logdedupe` key would be the peer's
bytes and one shared gate would let header warnings suppress the collector line)
and the *size* of records in your log — grpc-go's default header list size is 16
MiB, which every kubescrape gRPC receiver now lowers to 64 KiB as a *receive*
bound, far above any real OTLP header set and far below a log flood — and a
`-bin` header that fails to decode is printed whatever it holds. So
the class maps to `debug`, which costs nothing relative to grpc's own default —
that logger sends only its `Error` class to stderr and discards `Warning`
entirely unless `GRPC_GO_LOG_SEVERITY_LEVEL` says otherwise — and every
grpc-rendered message is clipped into the record at every level, so the stream
stays a stream. **What this leaves**: the connection-failure line
(`addrConn.createTransport failed to connect to …`) is one `-log-level=debug`
away rather than on by default, and at that level a peer can still drive volume
(clipped, and only while an operator has deliberately turned it on). The
NetworkPolicies above (`agent.ingest.allowFrom`, `serviceGraph.ingest.allowFrom`
— both empty by default, which means *any pod may push*) are what remove the
peers entirely.

**3. The chart's `values.schema.json` cannot express a CONDITIONAL requirement,
and has no generator behind it.** Unlike
[agent-config.schema.json](agent-config.schema.json), which is generated from
the structs the file decodes into, the chart's schema is hand-maintained. What
it does catch is real — `additionalProperties: false` turns a typo'd value into
a refused render, and a key added to `values.yaml` but not to the schema fails
the chart's own default render, which `internal/chartcheck` runs. What has no
generator behind it is the schema's *content*: the types, the enums and — the
part that matters here — anything CONDITIONAL. Two consequences, both real:

* *Conditional requirements live in the templates instead.*
  `serviceGraph.enabled: true` requires `serviceGraph.tokenSecret.name` — the
  binary refuses to start without `-service-graph-token-file`, so a chart that
  rendered anyway shipped a StatefulSet that CrashLoopBackOff'd on every shard
  with the reason visible only in a pod log. That one is now a template
  `fail()`, which also protects subchart use, where a parent's schema does not
  apply. It is *not* in the schema, because that is not a shape the schema
  expresses today.
* *An unguarded one is still there.* `seccompProfile.type: Localhost` is
  accepted by the schema's enum and rendered verbatim, but Kubernetes requires
  `localhostProfile` alongside it — so that combination renders a pod the API
  server rejects. Set both, or leave the default `RuntimeDefault`.

The rule for anyone editing the chart: **a new value must be added in two
places**, `values.yaml` and `values.schema.json`. Forgetting the second is at
least loud — every render becomes `additional properties … not allowed`, and
`internal/chartcheck` fails — but a value whose declared TYPE is wrong or merely
loose is caught by nothing until a cluster rejects the object.

**4. The container base images float.** Both `Dockerfile` and
`Dockerfile.static` build `FROM golang:1.26-bookworm`, and the runtime is
`gcr.io/distroless/base-debian12` (`static-debian12` for the static image) —
tags, not digests. The **Go** half of the supply chain is nailed down anyway by
`go.mod`'s `go 1.26.6` (see [Toolchain and build floor](#toolchain-and-build-floor)):
an older toolchain either upgrades itself or refuses to build. The half that
floats is everything else, and it is not only the runtime base: the default
(journald) image COPIES `libsystemd` plus its six transitive `.so` files out of
the **build** stage, so a cached or mirrored `golang:1.26-bookworm` layer puts
that library into a shipped image whose Go binary is current, while glibc and
the CA bundle come from the equally floating distroless base. Two builds of the
same commit are therefore not guaranteed to produce the same image. Pin the bases by digest in your own build
if reproducibility or base-layer CVE tracking matters to you; `make image` is
deliberately a developer convenience, not a release pipeline.

**A fifth, smaller one is documented at its flag rather than here**: with
`-debug-token-file` set, the `/debug/otlp/ui` page cannot present the token from
its own `fetch`, so the UI is reachable through a port-forward or a
header-adding proxy but not by pasting a token into the page. A token is
deliberately **not** accepted as a query parameter — that writes a credential
into every access log between the browser and the pod.


## Metadata service

```sh
kubescrape -listen :8080 -wait-timeout 5s -cache-ttl 5m -log-level debug
```

| Flag | Default | Description |
|---|---|---|
| `-listen` | `:8080` | HTTP listen address (the `/v1` API, `/healthz`, `/readyz`, and a `/debug` homepage with forms for the parameterised routes; `/` redirects there) |
| `-kubeconfig` | — | kubeconfig path; defaults to in-cluster config, then `$KUBECONFIG` / `~/.kube/config` |
| `-wait-timeout` | `5s` | default and maximum time a container lookup blocks waiting for metadata (`?wait=` can shorten per request, never lengthen) |
| `-cache-ttl` | `5m` | how long metadata of deleted pods and replaced container IDs stays resolvable (tombstones) |
| `-max-blocked-lookups` | `512` | how many container lookups may be blocked on `-wait-timeout` at once; past it `/v1/containers` answers 503 + `Retry-After` (counted `kubescrape_container_lookups_shed_total`) instead of parking another handler. **It is a memory bound wearing a count**: a parked lookup is an HTTP handler — two goroutines, their stacks, the connection buffers and the parsed request — held for the whole wait, on a route that is unauthenticated by design. Measured against a real listener: about 30 KiB for an agent's actual poll and 46 KiB for the worst shape the server's header bound admits, against a 64 KiB budget apiece — so the default spends 32 MiB, a quarter of the 128Mi the chart requests for this pod. (The budget is the arithmetic rather than the measurement: a parked lookup retains one ordinary poll plus, at most, one copy of the admitted head.) The same cap covers BOTH places a lookup parks — the per-ID waiters and the readiness wait during the initial informer sync. Legitimate demand is normally at most one blocked lookup per NODE, since the agent's tailer resolves on a single sweep goroutine and the cadvisor path never waits; the exception is an agent run with `-ingest-metadata-wait` (default 0, which blocks not at all), whose ingest handlers wait one lookup at a time per push and can add up to its own `-ingest-max-in-flight` (default 32). So a fleet larger than the default needs this raised — together with the pod's memory, by n x 64 KiB, since a cluster that big has outgrown the request the default was derived against. **Upgrade note:** this default was 16384 and is now 512, a 32x reduction, and the same budget now also covers the readiness park. Nothing warns on upgrade, so a cluster that was relying on the old headroom will start seeing retryable 503s where it previously parked: watch `kubescrape_container_lookups_shed_total` after upgrading and set this flag explicitly if it moves. The old value permitted several times the pod's entire memory request in parked requests alone, i.e. it could cause the eviction the cap exists to prevent. Reachable from the chart via `extraArgs` |
| `-metadata-cache-ttl` | `10s` | `max-age` stamped on metadata responses (`Cache-Control` + `ETag`) so the agent's client caches lookups and revalidates with `If-None-Match`/304; 0 disables cache headers |
| `-resync` | `0` | informer resync period (0 = watch stream only) |
| `-apiserver-probe-interval` | `30s` | how often to check that the API server is still reachable **from this pod** (0 disables). This is the outage signal, and it exists because nothing else is one: readiness LATCHES once the initial sync completes (deliberately — an unready service is dropped from its Service endpoints, cutting every agent off a cache that is still serving useful data), and `kubescrape_informer_watch_errors_total` cannot be relied on to cover an unreachable server, because client-go treats a refused connection as retriable and retries the watch inside the reflector without ever returning an error to the handler the counter lives in — measured silent across four outage shapes up to five minutes. Each probe is one `limit=1` metadata list of namespaces (a resource the service already watches, so no extra RBAC) with no `resourceVersion`, so it fails when etcd does rather than being answered from the watch cache. It publishes `kubescrape_apiserver_reachable` (1/0) and `kubescrape_apiserver_probe_failures_total`, warns on the transition and re-warns on a throttle while the outage persists, and logs the recovery. **Alert on it sustained, not on a single failure**: the probe measures whether a NEW connection reaches the API server, which is neither proof that the informer caches are advancing (a blackholed established watch probes healthy) nor proof that they stopped (one failed probe may be a blip). Set to `0` to turn the steady-state list off entirely; reachable from the chart via `extraArgs` |
| `-servicemonitors` | `false` | serve targets for `monitoring.coreos.com/v1` ServiceMonitors selecting pod-backed Services — plus **PodMonitors** (endpoints name container ports) when the cluster serves that CRD. Endpoint `port`/`targetPort`/`path`/`scheme`, `interval`/`scrapeTimeout`, `basicAuth`, `authorization`, `bearerTokenSecret`, `tlsConfig` (`insecureSkipVerify`, secret-backed `ca`/`cert`/`keySecret`, `serverName`) and the keep/drop subset of `metricRelabelings` are honored. Anything else an endpoint or monitor sets — `oauth2`, proxy settings, target `relabelings`, non-keep/drop relabel actions, configMap- and file-backed TLS material, the monitor-level guard rails (`sampleLimit`, `scrapeProtocols`, …); the authoritative list is `endpointSpec.ignoredFields()` + `specLimits.ignored()` in `internal/servicemonitors` — is **reported**: logged once per monitor and counted in `kubescrape_monitor_fields_ignored_total{kind}`, so a partially-applied CR is never silent. Self-disables with a warning when the CRD is absent |
| `-monitor-namespaces` | — | comma-separated namespaces whose ServiceMonitors/PodMonitors are honoured (empty = all, the historical default). **This is the gate**: a monitor is an instruction to every agent to issue a GET, so without it anyone who can create a ServiceMonitor in their own namespace can point `selector: {}` + `namespaceSelector.any: true` at an arbitrary path cluster-wide. It applies at INDEXING time, so a refused monitor never widens the `-scrape-auth-secrets` allowlist either. Reachable from the chart via `extraArgs`, which must be QUOTED for a multi-namespace value: `extraArgs: ["-monitor-namespaces=monitoring,platform"]` — a plain scalar in a YAML flow sequence ends at the comma, and the resulting stray positional both is ignored and terminates `flag.Parse`, so every later `extraArgs` entry is silently never parsed |
| `-scrape-auth-secrets` | `false` | serve monitor endpoints' `bearerTokenSecret` values to agents on `GET /v1/scrape-auth/{ns}/{name}/{key}`. Opt-in: requires `secrets get` RBAC (commented out in the manifests), `-scrape-auth-token-file`, and ships tokens over the cluster-internal HTTP channel |
| `-scrape-auth-token-file` | — | file with the shared bearer token callers must present on `/v1/scrape-auth` as `Authorization: Bearer <token>`. **Mandatory** with `-scrape-auth-secrets` — that endpoint is the only one serving Secret material, so starting without a token is refused rather than leaving it open to every pod in the cluster. Compared in constant time. The two ends re-read on DIFFERENT cadences, deliberately, because only one direction has a grace window: this service's ACCEPT SET (`bearer.Rotating`) is re-read about once a SECOND at request time (`bearer.DefaultRefreshInterval`; `DefaultReadInterval`'s minute is only the idle ticker floor and the back-off after a FAILED read), while each agent re-reads the token it PRESENTS (`bearer.File`) about once a minute. So an agent whose projection updated before the service's costs one retryable 401 rather than a minute of them, and the opposite lag — an agent still presenting the old token — is what the 5-minute grace on the PREVIOUS token covers. Rotating the Secret is a non-event: both ends converge with no restarts and no 401 storm |
| `-self-metrics-interval` | `1m` | export the service's own metrics over OTLP at this interval (0 disables) |
| `-self-attributes` | `true` | add THIS pod's Kubernetes resource attributes (namespace, pod, uid, owners, labels) to those exported metrics. No API traffic and no manifest wiring: the service reads its own store, its pod name is the hostname and its namespace comes from `$POD_NAMESPACE` or the ServiceAccount projection it already mounts. Re-read once a minute (a bare in-process store lookup), so a pod or namespace relabelled after startup is picked up. Fill-if-absent — `service.name` stays `kubescrape` and `service.instance.id` stays the hostname, but `service.namespace` becomes the pod's namespace, so the job reads `<namespace>/kubescrape`. A process that is not a pod of that name simply gets none, reported by `kubescrape_self_metadata_resolved`. The agent has the same flag (resolved differently — see its section) |
| `-otlp-*` | as the agent | used by the self-metrics push: `-otlp-endpoint`, `-otlp-protocol`, `-otlp-compression`, `-otlp-compression-level`, `-otlp-insecure`, `-otlp-tls-ca-file`, `-otlp-tls-insecure-skip-verify`, `-otlp-bearer-token-file`, `-otlp-timeout` |
| `-otlp-header` | — | static `key=value` header sent on every self-metrics export (HTTP header / gRPC metadata, e.g. `X-Scope-OrgID=tenant`); repeatable — repeatable rather than comma-separated so a value may contain commas |
| `-check-config` | `false` | validate the flags, print the same startup summary a real start prints, and exit — nothing acquired: no listeners, no informers, no API-server traffic (CI / pre-rollout). The agent has had this since it grew a config file; the SERVICE half of an install was the one that could not be checked before it was rolled out, which is backwards — it is the singleton every agent in the fleet blocks on. `validateConfig` is ONE function that a real start calls too, so a dry run cannot pass a config the process then CrashLoops on. It refuses what is otherwise **silent at runtime**: a `-monitor-namespaces` entry that is a glob or not a namespace name (that flag is an EXACT list, unlike the agent's namespace globs, so such an entry indexes no monitor and says nothing), an empty `-listen` (net/http reads an empty address as `:http`, so the API binds port 80 while every probe, Service and agent addresses the configured port), an unparseable or duplicated listener address, a negative `-resync`/`-apiserver-probe-interval`/`-wait-timeout`/`-cache-ttl`/`-metadata-cache-ttl`, and `-scrape-auth-secrets` without `-scrape-auth-token-file`. It deliberately does NOT judge the ENVIRONMENT — whether the token file is readable and whether an API server answers are both false on the laptop a pre-flight runs from, and both are already fatal at a real start; off-cluster it prints `apiServer=(unresolved)` and still reaches its verdict. Pass it the flags you intend to deploy with, since the check is over those flags. It is a CLI dry run: never render it into a manifest or values file, where it would make the Deployment exit 0 immediately |
| `-log-level` | `info` | `debug`, `info`, `warn`, `error`. The output format is **logfmt, always** — there is no format flag, so one parser reads every line of every component (client-go's klog is routed through the same handler) |

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

`kubescrape_self_metrics_points_skipped_total` guards exactly that path: it
counts data points of this process's own metrics left out of the exposition
because their stored label set failed to parse back. It should never move —
these label sets come from code, not from data — and it exists because of where
the loss would land: a skipped point is simply ABSENT from the response an
operator is reading to diagnose everything else, so without the counter their
own telemetry would shrink invisibly. It covers the SCRAPE path only; the OTLP
push reads the same stored string but degrades the other way (it emits the point
with whatever labels parsed), so a nonzero value means the pushed copy of that
series is MISLABELLED rather than missing. Any nonzero value is a bug in the
label round-trip; a throttled warning beside it names the metric. Both binaries
publish it.

RBAC (cluster-wide `get`/`list`/`watch`): `pods`, `services`, `namespaces`,
`nodes`, `replicasets.apps`, `deployments.apps`, `statefulsets.apps`,
`daemonsets.apps`, `jobs.batch`,
`cronjobs.batch`, `servicemonitors.monitoring.coreos.com` (plus
`podmonitors` when that CRD should be discovered).
`-scrape-auth-secrets` needs `secrets get` (commented out in the
manifests — enable deliberately) — see
[deploy/kubernetes.yaml](../deploy/kubernetes.yaml).

Five counters make the service's own quiet failures visible; all five are the
kind that used to be diagnosable only from a client's symptoms, minutes later
and somewhere else:

* `kubescrape_container_lookup_timeouts_total` — a blocking lookup whose wait
  budget expired without the container ID appearing. A low rate is normal (the
  wait exists to cover the ~1s gap between a container starting and the kubelet
  posting its ID, and a rotated file's ID may never come back); a SUSTAINED rate
  means this replica's pod informer is not seeing the pods whose logs the agents
  are shipping. Read it beside `kubescrape_apiserver_reachable` and the pod
  informer's RBAC. Requests that never blocked (`?wait=0`) are not counted.
* `kubescrape_self_lookups_refused_total{reason}` — the `/v1/self` 404s split
  by cause: `no_pod` (the connection's source address owns no live pod —
  EXPECTED and permanent under hostNetwork and behind SNAT, where the by-name
  fallback takes over), `forwarded` (the request carried
  `Forwarded`/`X-Forwarded-For`/`X-Real-Ip`, so the connection's address is no
  longer evidence about the caller and the route refuses rather than guessing)
  and `unparseable_peer` (should not happen; warned).
* `kubescrape_index_name_reuse_total{kind}` — an object arrived under a
  namespace/name a DIFFERENT, still-live UID held. The guard keeps served data
  correct, but reaching it means a Delete was never delivered — a relist gap.
  Read beside `kubescrape_informer_watch_errors_total`.
* `kubescrape_pod_ip_contested_total` — the live/live pod-IP recycle race. The
  index keeps the later acquisition, which is right; this counts the window in
  which it could have been wrong. The cost lands on the agent's opt-in
  `-ingest-peer-ip-fallback`, where a resource stamped with the previous
  holder's identity is never revisited.
* `kubescrape_owner_resolve_failures_total{kind,reason}` — an owner-chain or
  namespace/node metadata read that did not yield the object. `internal/owners`
  had no signal at all until this existed, and the degradation it covers is
  invisible by construction: a failed read returns the bare owner reference, so
  the response is well-formed, nothing 500s, and only `service.name` quietly
  becomes the POD NAME instead of the workload's — changing half the Prometheus
  job of every series the fleet exports for that workload. **Read the reasons,
  not the total.** `not_found` is the only one that is ever normal (a pod
  tombstone outliving its deleted owner, a cache still filling), so a low steady
  rate is expected and a sustained one means the informer is not seeing objects
  the pods reference — read it beside `kubescrape_informer_watch_errors_total`
  and the ClusterRole. `lister_error` is the RBAC-shaped case and the one to
  alert on; `no_informer` and `wrong_type` are wiring bugs; `bad_api_version`
  and `uid_mismatch` are per-object oddities costing that one pod its owner's
  labels. All but `not_found` also carry a throttled warning naming the object.

And every refusal on `/v1/scrape-auth` now names its cause in
`kubescrape_scrape_auth_failures_total{reason}`: `not_found`, `upstream`,
`not_utf8`, plus `disabled` (this service does not run `-scrape-auth-secrets`),
`no_monitors` (`-servicemonitors` is off, so nothing can be allowlisted),
`unauthorized` (what a `-scrape-auth-token-file` mismatch between the agents and
this service looks like), `not_allowed` (the ref is not referenced by any
INDEXED monitor endpoint — the monitor failed to parse, was refused by
`-monitor-namespaces`, or the ref is a typo) and `bad_request`. Each logs once
per ref, or once per process for the flag-level ones, and never the token.
Every one of them becomes `up=0` on a target whose agent sees only a status
code.

## Agent: general

| Flag | Default | Description |
|---|---|---|
| `-node-name` | `$NODE_NAME` | the node this agent runs on (set via the downward API) |
| `-listen` | `:8081` | serves `/debug` (homepage linking the debug surfaces), `/healthz`, `/readyz`, `/debug/tailer` (per-file positions/lag, malformed pod annotations), `/debug/targets` (per-target last outcomes, failures first), `/debug/transforms` (active transform program hash), `/debug/otlp` (live stream of exported OTLP as JSON lines; `signal`/`attr`/`sample` query params — `attr` is capped at 16 ANDed globs per stream and 512 bytes per glob, since every filter is walked against every resource of every export on the exporting goroutine and a glob's cost is linear in its length — UI at `/debug/otlp/ui`); empty disables. NOT `/metrics` — the Prometheus endpoint lives on its own `-metrics-listen` port |
| `-debug-token-file` | — | file with a shared bearer token gating the **data-bearing** debug surfaces on `-listen`: `/debug/otlp`, `/debug/otlp/ui` and `/debug/tailer`. Those three stream (or enumerate) everything this process exports — on the DaemonSet that is every container log line on the node, from every namespace scheduled there, selectable by resource-attribute glob — and the port is reachable from every pod in the cluster, so they are **not** open. **Without this flag they are served only to a LOCAL connection**: `kubectl port-forward` (the kubelet dials `127.0.0.1` inside the pod, so port-forward *is* the loopback address, and reaching it already requires `pods/portforward` on the namespace), a container in the agent's own pod, or — on an agent deliberately put on `hostNetwork` — the node itself. Set the flag to read them from anywhere else with `Authorization: Bearer <token>`; it is re-read periodically with the previous value accepted for a grace window, exactly like `-scrape-auth-token-file`, so rotating the Secret needs no restart. A local connection must **also** carry a loopback `Host` header (`localhost`, `127.0.0.1`, `::1`, or none at all): every client that dialled the port directly sends one, and a page in a browser whose DNS name has been rebound to `127.0.0.1` — the classic attack on a local debug UI, which reaches an operator's own `kubectl port-forward` and, being same-origin, can read the answer — keeps sending its own name. A refusal names the flag and counts `kubescrape_debug_refused_total{reason}` (`no_token`, `unauthenticated`, `forwarded`, `host`). `/healthz`, `/readyz`, `/debug`, `/debug/targets` and `/debug/transforms` are never gated: probes must answer, and target/transform state is what the metadata service's `/v1` routes already serve unauthenticated. Residual: the `/debug/otlp/ui` page cannot present a token from its own `fetch`, so with a token configured the UI is reachable through port-forward or a header-adding proxy — a token is deliberately NOT accepted in a query parameter, which would write a credential into every access log |
| `-self-metrics-interval` | `1m` | export the agent's own metrics over OTLP at this interval (0 disables); both binaries have this flag |
| `-metadata-endpoint` | `http://kubescrape.monitoring` | base URL of the metadata service |
| `-metadata-wait` | `5s` | server-side wait for not-yet-known containers (covers the gap between container start and the kubelet posting its status) |
| `-node-metadata-refresh` | `1m` | refresh interval for the node's labels/annotations used in attribute templates (0 disables) |
| `-self-attributes` | `true` | add THIS pod's Kubernetes resource attributes (namespace, pod, uid, pod IP, owners, labels, plus the `resourceAttributes` section's `self` pipeline) to the metrics the agent generates about itself — its self-metrics and span metrics. Resolved from the metadata service's `GET /v1/self`, which attributes the request by its connection's source address, falling back to a lookup by name (`$POD_NAME` or the hostname, in `$POD_NAMESPACE` or the ServiceAccount projection's namespace) when that cannot work — hostNetwork agents share the node IP, a NAT hop replaces the source address, and a dual-stack pod may connect from the family `status.podIP` does not carry. Fill-if-absent: `service.name` stays `kubescrape-agent` and `service.instance.id` stays the node, but `service.namespace` becomes the pod's namespace — so the agent's own job reads `<namespace>/kubescrape-agent`. A caller the service cannot attribute to a live pod (hostNetwork, an address-rewriting hop, no Kubernetes) simply gets no extra attributes; nothing waits on the lookup, and `kubescrape_self_metadata_resolved` reports whether it succeeded |
| `-self-attributes-refresh` | `1m` | how often `-self-attributes` re-reads this pod's own metadata, so a pod or namespace **relabelled after startup** reaches the metrics that stamp it. The poll is nearly free: `/v1/self` answers with `private, max-age=<-metadata-cache-ttl>` + `ETag`, so a fresh entry is served from the client's own cache and a stale one revalidates with `If-None-Match` — a 304 whenever nothing changed. (`private` is what makes caching a caller-dependent response safe: one client belongs to one process, which is the pod the answer describes; shared caches are told not to store it.) Retries before the first success start at 5s and back off to this. `0` disables the lookup entirely (and with it the `kubescrape_self_metadata_resolved` gauge, which is published exactly when the lookup runs — so a `0` reading always means unresolved, never "switched off"). The agent's own metrics bypass the namespace router regardless of this setting: they keep the durable default chain rather than following a route glob that happens to cover the agent's namespace |
| `-check-config` | `false` | compile every config section plus the flags, print a summary and exit — nothing acquired (CI / pre-rollout). Flag **values** are checked too: one you passed that the process cannot honour — a non-positive `-scrape-timeout`, a `-logs-rate-burst` below one whole token — is an error naming the flag, the value and a usable one, rather than a value quietly replaced by a working default on a fleet you are already rolling out to. Only what you passed: an untouched flag is always its default |
| `-test-config` | — | run the YAML test cases in this file through the compiled log pipeline (scrub → logAttributes → enrich → logMetrics → `logs.rules` → transforms) and exit non-zero on failure; like `-check-config`, nothing is acquired. See the README's "Config unit tests" for the file shape |
| `-log-level` | `info` | as for the service (logfmt output, always) |

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

Every wire send is counted `kubescrape_export_requests_total{signal,outcome}`
and narrated once per TRANSITION rather than once per attempt: a Warn when the
destination goes bad (naming the endpoint, the class and the likeliest cause),
a re-warn every 5 minutes while it stays bad, an immediate re-warn if the class
CHANGES (a 401 becoming a connection refusal is a different incident), and one
Info when it recovers. An over-cap payload's extra parts count
`kubescrape_export_split_parts_total{signal}` — each is its own round trip,
auth build and gzip pass, so a sustained rate means a producer is batching past
`-otlp-max-send-bytes`.

Every mounted bearer-token file in either binary — this one, the kubelet's, the
`/v1/scrape-auth` shared token, the trace tier's internal hop — is re-read on a
timer and **keeps its last good value on a failed re-read**, by design, so a
broken Secret projection produces no immediate symptom anywhere.
`kubescrape_bearer_token_read_errors_total{role}` is therefore the only signal
it has: `client` is the token this process PRESENTS, `receiver` the set it
ACCEPTS. The receiver half has no local symptom at all — it surfaces minutes
later and elsewhere, as a fleet-wide 401 once the clients rotate past it. Alert
on it sustained past one rotation interval; the counter moves on every failure
while the log line is throttled, so the rate never depends on the throttle.

**Upgrade note:** `outcome` used to be `ok` or `error`; it is now `ok`,
`transient` or `permanent`. An alert selecting `outcome="error"` matches
nothing after this upgrade, silently. Use
`outcome=~"transient|permanent"` for the old meaning, and prefer
`outcome="permanent"` for the alert that means telemetry is being LOST — a
sustained `transient` rate means the destination is down and the payload is
being retried or spooled, which is a different page at a different hour.

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

A full spool and a spool that cannot be WRITTEN TO look the same from the
collector's side and are opposite problems, so they are counted apart and each
produces a throttled line naming the directory and the filesystem's own error:
`kubescrape_buffer_full_total` is the cap binding (the collector is behind),
while `kubescrape_buffer_enqueue_errors_total` climbing with no `_full_total`
movement means the DISK — ENOSPC on segment preallocation, a latched fsync
failure, a read-only remount. Those refusals used to return bare, so a full
disk made a node go dark with every buffer metric flat. Watch
`kubescrape_buffer_backlog_bytes / kubescrape_buffer_max_bytes` to see a
degrading collector before anything is lost; every other buffer counter moves
only once data is already being refused or dropped.

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
export. A READER error restarts from the committed cursor with backoff; an
EXPORT failure retries the same batch in place and never re-reads it — re-reading
rebuilds the batch, and rebuilding re-runs the per-record chain, which multiplied
the log-metric and rules counters by the number of retries an outage spanned.
The attempts are counted `kubescrape_journal_export_failures_total`. The cursor is
persisted only through `-positions-file` (there is no standalone journald
cursor file); without it, every start begins at the journal tail — which the
reader now says out loud at startup (`start=tail` or `start=cursor`), because
"everything already in the journal is never exported" is correct behaviour that
reads exactly like a broken pipeline.

Two read-side repairs change the exported record INVISIBLY and are therefore
counted, `kubescrape_journal_entry_defects_total{defect}`: `invalid_utf8` (the
journal stores raw bytes, so U+FFFD replacement makes the body differ from what
the producer wrote) and `no_timestamp` (the entry carried no realtime stamp, so
the record is dated with the agent's clock at read time). Each carries a
throttled warning naming the unit. An unknown PRIORITY is deliberately NOT
counted — it is visible in the record's own severity field. Over-cap messages
are a separate, already-visible event:
`kubescrape_journal_truncated_total`, plus `log.truncated` and
`log.original_length` on the record itself.

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

A Pod that does **not** resolve — the metadata service could not answer, or a
pod of that name exists with a different UID — is still exported, under the
identity the event itself carries, and counted
`kubescrape_events_unresolved_total{reason}` (`lookup` or `uid_mismatch`) with a
throttled Warn naming the pod. That is lost **correlation**, not lost data,
which is exactly why it needs its own counter: correlation is the point of this
pipeline, the events keep flowing when it fails, and every other counter stays
green. A sustained rate means the metadata service is not answering; a
`uid_mismatch` rate means pods are being recreated under the same name faster
than the events about them are delivered.

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

**Every discovery-time selection is reported**, which is what turns "logs are
missing from namespace X" from a guess into a query:
`kubescrape_log_files_skipped_total{reason}` counts a declined file ONCE per
file (never once per discovery pass — the decisions are re-taken every ~2s and
on every fsnotify event), with a Debug line naming the path. The reasons are
`source_exclude`, `excluded_namespace`, `namespace_not_selected`,
`unparseable_name`, `too_old`, `non_regular` and `stat_error` — the last being
a genuine collection failure (an EACCES or EIO on a listed path) rather than a
selection. A file a LATER source claims is not counted at all, so `namespaces`
routing stays silent.

Its sibling is `kubescrape_log_files_unresolved`, the one state in the tailer
where a file is tracked, nothing is read from it, and nothing is lost: its
container metadata has not resolved yet. It is counted by
`kubescrape_log_files`, moves no byte counter, and returns to 0 on resolve — so
any nonzero value is a CURRENT condition. After two minutes the agent warns
naming the oldest waiting file, re-stating every five minutes. If the sweep ran
out of its shared resolve budget it did not even ISSUE the lookups, which
`kubescrape_metadata_requests_total` therefore cannot show:
`kubescrape_log_metadata_budget_exhausted_total` (once per sweep) is the only
signal for that.

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
same ordering: metrics observe every entry, the rules run after enrichment, and
a dropped entry still advances the journal cursor.

> **Known limitation — `__severity__` on journald entries spans two
> vocabularies.** journald stamps the *syslog* word (priority 4 is `warning`,
> 3 `err`, 2 `crit`), while enrichment overwrites the severity text with its own
> word (`warn`, `error`, `fatal`) whenever it parses a level out of the **body**.
> So one priority reaches the rules under either spelling depending on whether
> that particular line happened to be parseable, and `drop __severity__=warn`
> applies to only the parseable half of priority 4. Verified on a live journal:
> with `__severity__=warning` a plain priority-4 line is dropped while one whose
> body says `level=warn` is delivered, and with `__severity__=warn` it is the
> other way round.
>
> **Spell around it with a regex** — `__severity__=~^(warn|warning)$` — which
> selects both halves. The exported `severityNumber` is unaffected and is the
> reliable thing to alert on (both spellings export 13).
>
> This is not fixed by canonicalising the key onto one vocabulary, and that is
> a deliberate decision rather than an oversight: canonicalising un-matches
> every other spelling silently, and the **negated** form inverts —
> `{action: drop, match: ["__severity__!=err"]}`, meaning "ship only priority-3
> entries", would stop matching on any record and drop the node's whole journal.
> Fixing it honestly means canonicalising *both* sides of the comparison, which
> the exact-selector DSL does not do and a regex selector could not do at all.

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
line (JSON or logfmt) onto the exported record. It applies to every log
producer — `-logs`, `-journald`, `-events`, `-azure-diagnostics` — and, for the
`target: log` half, to logs pushed to `-ingest` as well.

The ingest exception is deliberate rather than an omission. A producer applies
a `target: resource`/`scope` lift by GROUPING records into a `ResourceLogs` (or
a `ScopeLogs`) that carries the lifted value; an ingested record already lives
in the SENDER's grouping, so writing there would stamp one record's value onto
every other record beside it.

The two unwritten halves then part ways, and the difference is worth knowing
before you configure one. A `target: resource` lift still RESOLVES on the ingest
path — a `logs.rules` key or a `logMetrics` label naming it selects identically
whether the line was tailed or pushed, which is where the divergence was
actually visible — it is simply not written onto the shared resource. A
`target: scope` lift is DROPPED on the ingest path: it is neither written nor
resolvable, so on a pushed line it has no effect at all. (Scope lifts resolve
for rules and metric labels on NO path — no producer offers them to the
resolver either — so what ingest loses is the stamping a tailed line gets.)

```yaml
logAttributes:
  rules:
    - key: tenant             # JSON/logfmt key; dotted keys descend into JSON
      attribute: tenant.id    # exported name (defaults to key)
      target: resource        # resource | scope | log (default log)
    - key: http.status_code   # nested JSON path a.b.c
      target: log
```

`-check-config` warns when a rule lifts a line value into one of kubescrape's
own keys, and the warning follows where each key is HONOURED rather than
naming a fixed target. A `target: resource` lift of a resolved-identity
attribute (`k8s.namespace.name` and friends) is reported because the resource
is what keys tenancy routing and series identity — the same keys the
pod-annotation path refuses and the ingest receivers strip. The plumbing
markers split the other way: `kubescrape.route` is honoured on a RESOURCE (the
router reads it ahead of the namespace globs, so the line would choose its own
destination and that route's tenant headers), while the transform **drop
marker** (`transform.DropMarker`) is
honoured on the log RECORD — `logAttributes`' default target — where an active
`logs:` script's post-script prune deletes every record carrying it and charges
it to `kubescrape_transform_dropped_total{signal="logs"}` as an
operator-intended drop. On the other placement each is inert, and the warning
says so rather than asserting a consequence that does not exist there.

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
      maxCardinality: 5000          # cap on unique SERIES — (resource, label-set) pairs, not label sets
                                    # alone (unset = default 10000, also the hard cap). The log line's
                                    # resource attributes are part of a series' identity, and ONE set is
                                    # shared by every pod on the node, so a rule matching N pods divides
                                    # this pool among them. A histogram is one stored sample per series
                                    # (its buckets ride along), and maxCardinality x buckets > 150000 is
                                    # refused at startup.
      maxAge: 1h                    # expire idle series (default/cap 24h)
      labelPrefix: ""               # optional prefix on every label name
    - name: request_duration_seconds
      type: histogram
      value: duration_s
      buckets: [0.1, 0.5, 1, 5]      # a histogram may NOT set a label named `le`
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

> **A histogram may not set a label named `le`, through `labels` OR
> `resourceLabels`.** It is refused at startup, naming the rule AND which of the
> two lists carried it. `le` is the bucket-bound label a Prometheus consumer
> generates from the histogram's own buckets, so setting it as a dimension
> would split one distribution into a separate series per value — each still
> rendering a full set of buckets — and collide with the generated label
> downstream. Both lists, because both fold into the identical series key: a
> `resourceLabels` `le` splits the distribution exactly the same way, and being
> a resource attribute it is the one that most surely meets the generated label
> downstream, since promoting resource attributes onto data points is the whole
> reason to lift a label onto the resource. The same refusal applies to a
> `labelPrefix` that composes to `le` in either list, and to `emit_metric()` in
> a transform script (an error there, since a script's label names are only
> known at runtime). On any other metric type `le` is an ordinary label and
> stays legal.

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
exporter. Enrichment leaves the sender authoritative about *itself* — every
descriptive attribute it set survives — with one deliberate exception: the
**resolved identity keys** (`k8s.namespace.name`, `k8s.pod.name`,
`k8s.pod.ip`, `k8s.node.name`, `k8s.container.name`, `container.name`) are
overwritten with what this receiver just read from the API server for that
exact resource, because `routing` keys tenancy on `k8s.namespace.name` and
these listeners authenticate nothing. Exempt are the keys the lookup was made
*by* (`-ingest-container-id-keys` / `-ingest-pod-uid-keys` — a value the
resolution is a function of has no independent truth to correct it with) and
`service.namespace` / `service.instance.id`, which are the same two exemptions
the receipt-time identity strip carves out (below), so both seams say one
thing about what a sender owns; `service.name` is descriptive and reserved by
neither. A resource that describes a *different* object — the
`datapoint`/`auto` split path — is relabelled wholesale instead: there the
sender's identity names the sender, not the object it reports on.

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
| `-ingest-max-in-flight` | `0` (= 32) | bound on pushes processed concurrently **across both transports**. Over it, senders are refused *retryably* rather than queued. It bounds PROCESSING only — the raw-byte and decoded-structure budgets below are what bound memory, and neither moves when this is raised |
| `-ingest-grpc-max-recv-bytes` | `0` (= 4 MiB) | cap on **one decoded gRPC message** (the counterpart of a collector's `max_recv_msg_size`); an over-cap push is refused, not truncated. Applies to the trace tier's application ports too; the OTLP/HTTP body cap stays 16 MiB. Raising it is a per-push memory grant on an unauthenticated listener — BOTH byte budgets below scale with it (the raw one to 4x the new cap once that passes its 64 MiB floor, the decoded one to twice the raw) |

A container ID resolves the exact container incarnation; a pod UID resolves
the pod.

**The operator's log cost levers reach pushed logs too**: after enrichment,
ingested log records run the same compiled `logs.rules` chain, feed the same
`logMetrics` set, and take the same `logAttributes` lifts (the `target: log`
half — see that section for why a `target: resource` lift resolves but is not
written, and why a `target: scope` lift does neither) as the tailer, journald,
events and Azure producers —
one config, one behavior, however the line arrived. Metrics observe every
record (before the rules), rules select on the enriched severity
(`__severity__`), a dropped record is removed before forwarding and counted
(`kubescrape_log_rules_dropped_total`) while the push is still acked — the
sender delivered it; the operator chose to drop it. A payload filtered to
nothing is acked without a send.

The two side effects are counted against **different units**, and this is the
one path in kubescrape where they diverge. The drop tally is applied when the
push is *acked*, not when the chain runs: a transient forward failure is
answered retryably on purpose, so the sender resends the identical bytes and
the chain runs again, and counting inline multiplied
`kubescrape_log_rules_dropped_total` by the number of delivery attempts an
outage spanned. A log-derived metric **observation**, by contrast, is once per
*receive attempt* — a retransmitted push observes again. It cannot be deferred
the same way: the observations are lazy (value and labels resolve per rule,
inside the metric store, off the record and the line), and the record that
would have to be re-read after the export is not the same record — a script may
have mutated the payload in place, and a rules-dropped record is not in it at
all. So on the ingest path an SDK's retries inflate log-derived metrics and
`kubescrape_log_scrubbed_total`, but not the drop counter. Everywhere else in
the repo — the tailer, journald, events, Azure — delivery is at-least-once and
observation is once per delivery. Bodies are scrubbed first (`logScrubbing`,
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

There are **three** resources to bound and no one of them covers another, so
there are three bounds. The count above is the first: it bounds *processing*,
not *buffering*, because the HTTP handlers read the whole body before taking a
slot (holding one across a trickled 16 MiB upload would let a few senders shed
everyone else for a `ReadTimeout`) and gRPC decodes the message before the
interceptor runs.

The second is **64 MiB of RAW payload** across both transports — four
full-size HTTP bodies, and scaled up in step with `-ingest-grpc-max-recv-bytes`
so one legal message always fits — which covers exactly that window: an HTTP body is charged as it is read, in 64 KiB
steps — a declared `Content-Length` is never credited up front, since the
declaration is the sender's claim, not a fact — and a gRPC push reserves
`MaxRecvMsgSize` from the moment its headers arrive until its message is
decoded.

The third is a **DECODED-structure budget of twice that** (128 MiB), because a
count cannot bound a size and raw bytes do not bound what they inflate INTO. A
legal 15.99 MiB logs body of 578 000 minimal `ResourceLogs` compresses to about
40 KiB on the wire and inflates roughly 16x, to ~256 MiB of live heap; the raw
budget deliberately admits four full-size bodies, so the shape it is designed to
allow is ~1 GiB of heap on a pod the chart limits to 512Mi — an OOM-kill of the
node's whole agent bought with ~160 KiB of unauthenticated traffic, repeatable
into CrashLoopBackOff. The same 16 MiB carrying a realistic 60 000-record batch
inflates 1.12x, so the hazard is SHAPE, not size. The budget charges STRUCTURE
only — 512 B per resource, 256 B per scope, 256 B per record/point/span,
measured against pdata — and deliberately not the strings a decode copies out,
which are already bounded transitively (on HTTP the raw body is charged for the
same lifetime; on gRPC by the in-flight count times the per-message cap), and
charging them twice would shed honest senders.

None of the three is a flag: the operator knob is the count above, and the two
byte budgets scale with `-ingest-grpc-max-recv-bytes` because a receiver told to
accept bigger messages was told to hold more of everything. All three refuse the
same retryable way (`429` + `Retry-After`, or `ResourceExhausted` + `RetryInfo`)
and count into `kubescrape_ingest_rejected_total`. The decoded budget's cost is
worth stating plainly: a SINGLE push whose structure alone estimates past the
whole budget can never be admitted and is refused **every time** — roughly
130 000 resources or 500 000 records in one push, an order of magnitude past
what a batching SDK emits. That case logs a throttled warning naming the
estimate, because "every push from this sender is 429" is otherwise
indistinguishable from ordinary back-pressure; the sender must batch smaller.

The per-push size caps are **4 MiB per gRPC message**
(`-ingest-grpc-max-recv-bytes` raises it; both byte budgets scale in step, so
a single legal push always fits) and a fixed **16 MiB per HTTP body**. An
over-cap push is refused (`ResourceExhausted` / `413 Content Too Large`),
never truncated — and unlike an over-the-count refusal, retrying the same
batch cannot succeed, so a sender that hits the gRPC cap must ship smaller
batches (an SDK batch-processor setting), switch to OTLP/HTTP, or have the
cap raised to what a collector's `max_recv_msg_size` used to grant it.

**Which refusal lands in which counter**, since the response differs by bound.
All three ADMISSION bounds — the in-flight count, the raw byte budget and the
decoded budget — count into `kubescrape_ingest_rejected_total`, which is the
receiver protecting itself and is retryable as sent. The per-push SIZE caps are
a different event: an over-cap HTTP body is
`kubescrape_ingest_body_rejected_total{reason="too_large"}` (the request is
wrong, and no retry of those bytes can work), while an over-cap gRPC message is
refused by grpc-go before any kubescrape code runs and moves no counter here at
all — `-ingest-grpc-max-recv-bytes` and the sender's own batch size are the only
evidence of it.

The counter says a refusal happened; the LINE says which bound and which knob.
Each of the three names itself on a throttled warning carrying
`reason=in_flight|buffer_bytes|decoded_bytes` — the same three values, and the
two flags they point at are `-ingest-max-in-flight` (the count) and
`-ingest-grpc-max-recv-bytes` (both byte budgets scale from it).

A persistently non-zero `kubescrape_ingest_rejected_total` means the node cannot
keep up with what is being pushed at it, but there is no single knob for it:
raising `-ingest-max-in-flight` relieves ONLY the count, and does nothing
whatever for either byte budget (both scale with `-ingest-grpc-max-recv-bytes`
instead, and raising that is a per-push memory grant on an unauthenticated
listener). The bound-independent fixes are the better ones: speed up the
collector's acks (a slow collector holds every slot for `-otlp-timeout`), push
less at this node, or — for the decoded budget, whose throttled warning names
the sender's own estimate — make that sender batch smaller.

A body that **nests** deeper than 100 length-delimited protobuf levels is
refused before it is decoded at all — `400` on HTTP, gRPC `Internal`, counted
`kubescrape_ingest_body_rejected_total{reason="too_deep"}`, on both transports
including the gRPC codec. The decoder pdata generates recurses without a limit
of its own (an `AnyValue` holding an `ArrayValue` calls back into `AnyValue`),
so ~1.6 MiB of perfectly legal body — inside every cap above — peaks at 128 MiB
of goroutine stack for ONE decode, and several such bodies fit inside the byte
budget and the in-flight count at once; the outcome is an OOM-kill with no
counter moving, because nothing survives the decode to move one. 100 WIRE levels
is roughly 31 levels of nested attribute structure (an OTLP log body already sits
5 down, and each further map/array level costs three), an order of magnitude past
what any SDK emits, and it is also protobuf's own portability limit — a payload
deeper than that is already undecodable by a conformant consumer.

A push's **header block** is bounded at **64 KiB** on every kubescrape gRPC
receiver — the ingest listeners, the trace tier's application ports and its
internal shard port. grpc-go's own server default is 16 MiB, and the block is
HPACK-decoded before anything kubescrape wrote runs: before the byte-budget tap,
before the auth tap on the internal hop (the credential arrives *in* the block),
before the codec's nesting guard. It was therefore both unbounded buffering per
stream and, since grpc renders a header it cannot decode into its own log line,
the amplifier the `debug` mapping above discusses. 64 KiB is many times the
largest header set an OTLP sender writes — a bearer token, a tenant id, the h2
pseudo-headers — so no legitimate push is refused; a sender past it gets a clean
protocol-level refusal rather than a stream reset, because grpc-go advertises
the value in its `SETTINGS` frame and a conformant client checks against it
before writing.

**A sender's identity claim dies at the door.** The application-facing
receivers — the DaemonSet's `-ingest` listeners and the trace tier's
application ports — strip the Kubernetes identity keys a sender declares about
itself (`k8s.namespace.name`, `k8s.pod.name`, `k8s.pod.ip`, `k8s.node.name`,
`k8s.container.name`, `container.name`) at first receipt, counted
`kubescrape_ingest_identity_stripped_total{key}`. `routing` keys tenancy on
`k8s.namespace.name`, so on a listener with no credentials a pod declaring
someone else's namespace would have its records exported to that tenant's
endpoint under that tenant's headers; the siblings are exactly the keys a
resolved lookup overwrites anyway, so on a resource that resolves the strip
changes nothing, and on one that does not a claim nobody can check would name
someone else's pod on every series and every log-derived metric bound to the
resource. Enrichment cannot stand in for the strip, because a resource with no
resolvable ID has no correction available and there the declaration was the only
namespace there was.

What the strip does **not** close, said plainly so the counter is not mistaken
for a solved problem: the keys the attribution is *made by* (`container.id`,
`k8s.pod.uid`) are exempt, because the strip runs before enrichment and removing
them would leave a receiver that resolves nothing at all. The metadata service's
`/v1/pods/{ns}/{name}` is unauthenticated and hands out each container's ID, and
the container index is cluster-wide, so a sender that reads another pod's
container ID and declares no namespace of its own is *resolved* into that pod's
namespace and routed to its tenant. Nothing is forged on the wire there, so
nothing is stripped and `kubescrape_ingest_identity_stripped_total` does not
move. Refusing objects resolved on another node would not close it either — a
co-located victim is the common case — and would break the datapoint/split path
and the trace tier, both of which describe objects across the cluster by design.
Authenticating the ingest listeners or the metadata service is what closes it.

That counter is deliberately **not**
`kubescrape_ingest_reserved_stripped_total`, which stays for kubescrape's own
plumbing markers (the transform `route()` selector and the drop marker): every
conformant SDK names its own pod — a workload instrumented by the OpenTelemetry
Operator ships several of these keys on every push — so the identity strip is a
routine correction that a healthy cluster runs continuously and there is nothing
on it to alert on, while a *plumbing* marker arriving on the wire is a key only
kubescrape has a reason to set and any rate at all is worth finding. For the
same reason the identity strip logs at **debug**, throttled per key; the
plumbing strip warns.

Three things are deliberately NOT stripped. The sender's own OTLP service
triple — `service.name`, `service.namespace` and `service.instance.id` — is how
a sender names *itself*: `service.name`, the half that decides which workload's
series a sample joins, is sender-controlled by design, so a sender out to
masquerade as another workload need only declare that workload's
`service.name`, and deleting the two dimensions beside it stops nothing while
costing an honest sender its instance identity (half of the Prometheus/Tempo
job+instance pair). kubescrape reads none of the three. And this receiver's own
lookup keys (`-ingest-container-id-keys` / `-ingest-pod-uid-keys`) survive
because the strip runs BEFORE enrichment — stripping those would disable
attribution entirely.
The cost, so it is a decision and not a surprise: a sender this receiver
**cannot resolve** loses its own honestly-declared `k8s.pod.name`,
`k8s.namespace.name` and the rest of the Kubernetes half, because nothing at an
unauthenticated door can tell an honest declaration from a forgery. On the trace
tier that means an application whose spans carry k8s attributes but no
`container.id`/`k8s.pod.uid` (and no working peer-IP fallback) ships them
without those attributes — it keeps its service triple, so it is still named,
but it is no longer placed on a node or in a namespace.

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
| `-metrics-batch-bytes` | `3145728` | also flush a chunk once its estimated encoded size reaches this (`0` = the 3 MiB default; **negative** disables the byte bound, leaving only `-metrics-batch-size`). The collector's gRPC receive limit applies to the **decompressed** message (4 MiB by default), which a label-rich target (kube-state-metrics, Istio) can exceed well before the point limit — every export of that target would then fail, so this bound is what keeps a chunk deliverable |
| `-scrape-max-samples` | `0` | abort a single scrape beyond this many samples (0 = unlimited) |
| `-scrape-exemplars` | `false` | negotiate OpenMetrics and attach exemplars to counter and histogram points (`trace_id`/`span_id` map to OTLP trace/span fields) |
| `-scrape-health-metrics` | `true` | export synthetic `up`, `scrape_duration_seconds` and `scrape_samples_scraped` gauges per target after each cycle |
| `-scrape-native-histograms` | `false` | offer the Prometheus **protobuf exposition** to annotation/monitor targets — splitter-backed ones included — and convert native histograms to OTLP **exponential histogram** points (a split rule routes native points through the same groupBy/enrichment machinery as every other kind). A family carrying both native and classic data uses the native representation; a custom-bucket message (NHCB, schema −53) whose bounds live in `custom_values` is REFUSED and counted — `kubescrape_scrape_malformed_total` standalone, `kubescrape_scrape_histogram_mixed_total{dropped="nhcb"}` inside a native family — because client_model v0.6.2 (Prometheus' own pin) does not generate that field, so the bounds are unreachable; one that also carries classic `bucket` rows converts through the classic path; targets that ignore the Accept header keep serving text (the parse mode follows the response Content-Type) |

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

**When nothing is being scraped**, read `kubescrape_scrape_targets` first: the
targets this node was handed on the last successful fetch, after the transforms
file's `targets:` hook. **Absent** means annotation and monitor scraping is off
(`-metrics=false`); **0** means the metadata service answered and this node has
no targets — the agent warns naming the node with a remediation hint, re-states
it every 30 minutes while it stays true, and logs an Info when targets appear.
It does not count the kubelet pipelines, which are configured rather than
discovered.

**When everything is `up=0`**, read `kubescrape_scrape_failures_total{reason}`
before anything else. It is the breakdown of
`kubescrape_scrapes_total{outcome="error"}` (the two move from the same place
once per scrape, so their sums agree by construction), and its fourteen
reasons take completely different remedies — `dns`/`connect`/`tls`/`timeout` mean the
target was not reached, `unauthorized`/`status` mean it answered and refused,
`auth`/`relabel` mean the scrape never left this agent, and `export` means the
target is **innocent** and the collector is refusing the payload. The
accompanying `scrape failed` Warn carries the URL and, for
`unauthorized`/`auth`/`proto_refused`/`export`/`tls`, a `note=` naming the
remedy. [FIRST-RUN.md](FIRST-RUN.md#every-target-is-up0) has the full table.

At `-log-level=debug` the agent additionally answers, per object, "why is this
cadvisor series unlabelled" and "why does this pod have (or not have) a pause
resource".

## Agent: kubelet scrapes

| Flag | Default | Description |
|---|---|---|
| `-kubelet-endpoint` | — | kubelet base URL, typically `https://$(NODE_IP):10250` with `NODE_IP` from the downward API. On an IPv6 node that expands to a BARE IPv6 literal (`https://fd00:10::5:10250`), which `net/url` refuses since Go 1.26's strict-colon host parsing — the agent re-forms the authority with `net.JoinHostPort` (`https://[fd00:10::5]:10250`), so the one default serves both families; do NOT pre-bracket it in the manifest, which renders `[10.0.0.5]` on IPv4 and is rejected as an invalid IP-literal. `-check-config` parses the value and refuses one no request can be built from. Empty disables all THREE kubelet scrapes (cadvisor, node-metrics, stats-summary) |
| `-kubelet-token-file` | ServiceAccount token | bearer token towards the kubelet (needs `nodes/metrics get` RBAC) |
| `-kubelet-insecure-tls` | `true` | kubelet serving certificates are typically self-signed |
| `-cadvisor-rollups` | `true` | `false` drops the hierarchy aggregates (`/`, `/kubepods`, QoS/system slices) and pod-level rows of container-scoped families, keeping container-level series, `container_network_*` and `machine_*` |

**A metadata outage costs attribution, not data.** Every enriched kubelet
pipeline resolves the objects it describes through the metadata service, and
those lookups are bounded by a **per-scrape allowance of half the scrape
timeout**. Past it the remaining objects are not looked up at all: they export
under the identity the payload itself carried and lose only the join, and the
scrape ships.

The allowance exists because the two outage shapes are not alike. A metadata
service that **refuses** connections is harmless — lookups fail instantly, the
scrape actually gets *faster*, and attribution is all that is lost. One that
**hangs** — a partition, a dropping firewall, a blackholed address — used to
consume the entire scrape budget and take *both* kubelet pipelines down with it,
discarding stats already parsed: measured on a live cluster, 23 consecutive
cycles of `context deadline exceeded` on cadvisor and summary together, with
~3300 summary samples and ~6100 cadvisor samples parsed and then thrown away,
while the `/metrics` pipeline beside them — the one that resolves nothing — kept
succeeding. The allowance turns the hanging case into the refused case.

When it binds, `kubescrape_scrape_metadata_budget_exhausted_total{pipeline}`
counts the scrape (once per scrape, not per shed object) and a warning naming
the allowance and the timeout is logged at most once a minute. A sustained rate
means the metadata service is slow or unreachable and this node's series are
arriving unjoinable.

The RESULT of that — a resource built without an answer — is
`kubescrape_cadvisor_unresolved_total{level}` (`container` or `pod`), counted
once per resource per exported chunk, and `kubescrape_summary_unresolved_total`
for `/stats/summary`. The rows still export under whatever identity their own
labels carry, so this costs the owner chain, the pod labels and a real
`service.name` rather than the measurement. A steady low rate is ordinary — a
container started seconds ago has no id in the API server yet and resolves on a
later cycle — while a sustained or fleet-wide rate is the same condition the
budget counter reports, seen from the other end. At `-log-level=debug` the
agent names, per object, why a cadvisor row could not be attributed and why a
pod did or did not get a pause resource.

cadvisor series are split into one OTLP resource per pod/container, keyed by
the cgroup path in the `id` label: the container ID resolves the exact
container incarnation through the metadata service; pod-scoped series (e.g.
`container_network_*`) resolve by namespace/name cross-checked against the
cgroup pod UID.

### `/stats/summary` — ephemeral storage and volumes

| Flag | Default | Description |
|---|---|---|
| `-kubelet-summary` | `false` | scrape `<kubelet-endpoint>/stats/summary`, the kubelet's JSON stats report |

The kubelet's third endpoint is **JSON, not exposition**, and it is the only
source of one number an operator repeatedly wants: **per-pod ephemeral-storage
usage** — a pod's containers' writable layers, plus their logs, plus their
on-disk `emptyDir`s. That is the quantity the kubelet's eviction manager
measures a pod against, and the one `limits["ephemeral-storage"]` bounds.
Neither cadvisor nor kube-state-metrics reports it. Also only here: per-container
log bytes, **inodes-used** at every level (cadvisor has no metric for it
anywhere), and every volume the kubelet can measure — `emptyDir`, `configMap`,
`secret` and the projected token included — attributed to the pod that mounts
it.

**It is OFF by default because of RBAC, not cost.** The kubelet authorizes
`/stats/*` against the `nodes/stats` subresource, while `/metrics` and
`/metrics/cadvisor` go through `nodes/metrics` — so a binary that rolls ahead
of its ClusterRole 403s on every node in the fleet, every scrape interval, with
the ClusterRole looking correct. The shipped manifests and the chart grant
`nodes/stats` **unconditionally** for that reason, rather than behind the
value: enabling the scrape through `extraArgs` is a thing people do.

**What overlaps, stated plainly**, because `/metrics/cadvisor` is not silent
about filesystems — it carries `container_fs_usage_bytes`,
`container_fs_limit_bytes` and the inode pair, keyed by *device*:

* the eighteen `k8s.node.{filesystem,imagefs,containerfs}.*` restate cadvisor's
  root-cgroup rows, adding the nodefs/imagefs/containerfs **role** the eviction
  thresholds are written against, which a device name cannot give you. Note the
  two sides arrive under **different `job` and `instance`** — the summary node
  resource is `service.name=kubelet`, `service.instance.id=summary-<node>`, the
  cadvisor rollup resource `cadvisor-<node>` — so the overlap cannot be
  deduplicated, or even compared, in a single backend query without a join on
  `k8s.node.name`;
* `k8s.pod.process.count` is **not** a pre-summed `container_processes`, though
  it is tempting to read it as one. Measured on a live node, three different
  numbers exist for one pod: the pod-cgroup row reads 0, the sandbox row 1, and
  the app container its own count. This is the kubelet's own figure for the pod,
  and it survives `-cadvisor-rollups=false`, which deletes the pod-cgroup row;
* `k8s.container.ephemeral_storage.usage{fs.type="rootfs"}` **has no cadvisor
  counterpart on a modern node**, whatever the family names suggest. Measured on
  v1.33.1 + containerd, `container_fs_usage_bytes` is emitted only for `id="/"`,
  keyed by device, with `container=""` and `pod=""` — there are no per-container
  filesystem rows for it to disagree with. Treat the per-container figure as
  this endpoint's alone;
* on a PVC-backed volume the six `k8s.volume.*` restate `kubelet_volume_stats_*`
  from the `-node-metrics` scrape, adding the pod attribution those lack (they
  carry namespace and PVC only). Not reproducible on kind, whose default
  StorageClass yields hostPath PVs that the kubelet measures on *neither* side.

**A caveat on `k8s.volume.available` and `k8s.volume.capacity`.** For a volume
that lives on the node filesystem — `emptyDir`, `configMap`, `secret`, the
projected token — the kubelet reports the **node filesystem's** free and total
bytes, repeated identically for every such volume, because that genuinely is
the space the volume can grow into. Only `usage` and the inode triple describe
the volume itself. So a "volume percent full" alert built from
`usage / capacity` measures the node's disk, not the volume, and reads near-zero
for every one of them. Alert on `k8s.volume.usage` directly, or restrict the
ratio to PVC-backed volumes (those carrying
`k8s.persistentvolumeclaim.name`).

CPU, memory, network and swap are left to the cadvisor scrape entirely.

**Attribution is the point.** Every statistic lands on the resource for the
object it *describes* — a container stat on that container's resource, a pod
stat on the pod's, a volume stat on its **pod's** resource with
`k8s.volume.name` on the data point (a volume is a property of the pod, not an
identity of its own), a node stat on the node's. The resource is built by the
same code a cadvisor row goes through, so these series **join** cadvisor's for
the same `container.id`.

Static (mirror) pods resolve too, and the mechanism is worth knowing because
the obvious implementation is wrong. A kubelet mints a static pod's UID from
its manifest, so it matches no pod in the API server and a UID lookup misses
forever — which would leave `kube-apiserver`, `etcd`, `kube-scheduler` and
`kube-controller-manager` permanently unattributed, on the node whose disk is
most likely to fill. Falling back to namespace/name would fix those and break
something worse: a pod that merely **reuses** a name would lend its identity to
statistics about its predecessor. So the UID cross-check is *redirected* rather
than dropped — the kubelet stamps that same UID onto the mirror pod as
`kubernetes.io/config.hash`, which the mirror client copies to
`kubernetes.io/config.mirror`, and a by-name answer is accepted only when one
of those annotations equals the UID the kubelet reported. A name-reusing pod
carries neither and stays unresolved.

An object the metadata service cannot place is **still exported**, carrying the
identity the payload itself gave it; what it loses is the join. That is counted
by `kubescrape_summary_unresolved_total{level}`, once per object per scrape
rather than per data point.

Cost, measured on a synthetic 110-pod node with two containers and two measured
volumes each: **2550 data points per scrape** — 1320 volume series (just over
half, at six per measured volume), 880 container, 330 pod and 20 node. The lever
is a drop rule under `metrics.pipelines.summary`, which sees the data-point
attributes (`k8s.volume.name`, `fs.type`) as labels; the flag itself is all or
nothing.

A stat carries **its own timestamp**, not the scrape's, and volume statistics
are **older than the "roughly a minute" the kubelet's refresh period suggests**.
Measured over six consecutive scrapes on a 12-pod node (n=72 volume points, age
taken against the fetch instant): min 6s, median 56s, p90 107s, **max 167s**,
with 42% older than 60 seconds. The kubelet refreshes volume stats on its own
jittered cycle and the jitter compounds, so size a staleness alert against the
p90, not the period. Stamping scrape time instead would fabricate freshness; an
unrefreshed stat re-sends an identical (series, timestamp, value) triple, which
Prometheus accepts as idempotent.


## Agent: high-frequency cgroup sampling

| Flag | Default | Description |
|---|---|---|
| `-cgroup-stats` | `false` | sample container cgroups directly and export the distribution of each `-scrape-interval` window |
| `-cgroup-stats-interval` | `1s` | sampling period; must be well below `-scrape-interval` |
| `-cgroup-stats-discover-interval` | `15s` | how often the container set is re-read from the hierarchy; the blind spot for short-lived containers |
| `-cgroup-stats-root` | — | cgroup v2 mount point; empty autodetects `/sys/fs/cgroup` |

The cadvisor scrape above reads a cumulative counter once per
`-scrape-interval`, which yields exactly one number: the average over that
window. A container that pins 4 cores for 2 seconds inside a 60s window is
reported as ~0.13 cores. Shipping 1s raw series instead would restore the
signal at 30-60x the egress.

`-cgroup-stats` samples each container's own cgroup at
`-cgroup-stats-interval` and exports ten gauges per container per window,
named in cadvisor's Prometheus style so they sit beside the series they
explain:

| Metric | Unit |
|---|---|
| `container_cpu_usage_stddev` / `_max` / `_min` / `_mean` | cores (the *rate* of `cpu.stat`'s `usage_usec`, hence no `_seconds` in the name) |
| `container_cpu_usage_samples` | readings taken this window |
| `container_memory_working_set_bytes_stddev` / `_max` / `_min` / `_mean` | bytes (`memory.current − inactive_file`, cadvisor's own definition) |
| `container_memory_working_set_bytes_samples` | readings taken this window |

`_mean` and `_samples` exist because the other four are not interpretable
alone. cadvisor's CPU *counter* yields the window mean as a `rate()`, but its
`container_memory_working_set_bytes` is a gauge sampled once per scrape, so the
average working set was available nowhere; and `-cadvisor=false` (or a failing
kubelet scrape) removes the CPU one too. `_samples` is **this** window's own
reading count, so a value below 2 says the four statistics beside it are the
previous window's, re-stated (see the sparse-window hold below) — the one thing
the six-gauge set could not tell you about a single container.

The resource attributes are built by the same code that builds a cadvisor
row's, so the two join on `job`/`instance`. The standard deviation is the
population one over the window, computed with Welford's algorithm.

Requirements and refusals:

* **The host's `/sys/fs/cgroup` must be mounted read-only.** A container sees
  only its own cgroup otherwise. The chart mounts it behind
  `agent.cgroupStats.enabled`; `deploy/agent.yaml` carries the flag, the mount
  and the volume commented out together. Without pod cgroups the agent WARNS
  loudly at startup (and periodically after) and publishes
  `kubescrape_cgroup_containers` at 0 — registered exactly when the sampler
  runs, so 0 means "on and finding nothing", never "off".
* **cgroup v2 only**, because v1's `cpuacct.usage` is nanoseconds and reading
  it as microseconds would silently report usage 1000x low. A v1 node
  **disables this pipeline alone** and logs an ERROR naming v1 and the flag —
  every other pipeline keeps running, because the cgroup version is a
  property of the node and not an operator mistake, and one flag must not
  take the log pipeline down across a mixed fleet. An explicit
  `-cgroup-stats-root` that is not a cgroup v2 hierarchy IS fatal: that one
  is an operator mistake, and `-check-config` cannot catch it because the
  node's cgroup version is not knowable at check time.
* The layout below the mount point is **discovered**, so kind's
  `kubelet.slice` nesting, a stock systemd node's `kubepods.slice` and a
  cgroupfs-driver node's `kubepods` all work with `-cgroup-stats-root` unset.
* **Short-lived containers are missed, and the miss is not fully countable.**
  Discovery is the only way into the sampled set, so a container that starts
  and exits between two `-cgroup-stats-discover-interval` passes is never
  sampled — and its cgroup directory is created and removed inside the
  interval, so nothing is left to count it by. Measured capture at the 15s
  default, by container lifetime (6000 trials, following (L-2)/15): 0% at 2s,
  20% at 5s, 53% at 10s, 87% at 15s, 100% from 17s up.
  cadvisor's housekeeping has the same blind spot for the same reason. Lower
  the interval to catch init containers, CronJob pods and crashloops; the
  price is a directory walk plus one metadata lookup per still-unresolved
  cgroup per pass, which for the first three minutes of a pod's life includes
  its permanently unresolvable sandbox cgroup. The half that IS countable is
  `kubescrape_cgroup_windows_dropped_total{reason="too_short"}`: a container
  seen and then gone before it produced two samples of anything — a lower
  bound on what a shorter interval would recover.
* **A sparse window holds the last value, at most twice.** A CPU rate needs
  two readings and the working-set gauge needs two for a distribution, so a
  window that collected fewer re-emits that signal's previous distribution
  rather than a stddev over one sample or a gap — per signal, since CPU is
  structurally one reading behind memory. That hold is bounded to two
  consecutive windows (a window is a whole `-scrape-interval`, so two of them
  cover any real sampling hiccup several times over); past it the signal
  stops entirely, and a container's FIRST window emits nothing rather than
  inventing a value. Held windows are counted in
  `kubescrape_cgroup_held_windows_total`, which is how you tell a bridged gap
  from a live reading — the value itself carries no marker, deliberately,
  because a resource attribute would fork the derived job/instance and a
  data-point attribute would break the cadvisor join for exactly the windows
  that are held.
* **These gauges are built by the `cadvisor` resource-attribute pipeline**, not
  by one of their own. That is deliberate and load-bearing: the whole point of
  these series is to sit beside the cadvisor series for the same
  `container.id`, and a join works only while both resources carry a
  byte-identical attribute set — so they are built through the same
  `Scraper.FillContainerResource` the cadvisor batcher uses, and they read
  `resourceAttributes.pipelines.cadvisor` with it. The consequence to know
  before you edit that section: a template, an `enable`/`disable` entry or an
  `instancePrefix` set there moves BOTH, and setting one for the cgroup gauges
  alone is not expressible — it would break the join it exists to serve.
* A container that VANISHES flushes its final window and is then retired, so
  an OOM-killed container's burst is exported rather than discarded — that
  case is the reason the feature exists. That final flush is never a held
  value: the last datapoint before a kill is the one an operator zooms into,
  so it is real or it is absent.
* A container the metadata service cannot resolve is **not exported at all**.
  A cgroup path carries a pod UID and a container ID but never a pod NAME, so
  there is nothing to derive `service.name` from, and a series with no
  Prometheus `job` cannot join the cadvisor series these gauges exist to
  annotate. This is also what keeps the pod sandbox out: a pause container's
  ID appears in no pod's `containerStatuses`, so it never resolves and never
  ships. Unresolved cgroups are retried on a cadence that depends on **why**
  the lookup failed, which is the difference between a sandbox and an outage:
  a **404** is a definitive answer — a pause container will never become a
  workload container — so after a short grace it drops to one retry every ten
  minutes, roughly one lookup per pod per ten minutes for the whole sandbox
  set; a lookup the metadata service **could not answer** (unreachable, 5xx,
  timeout) abandons nothing, so a transient outage never costs a node its
  real containers, and it puts an already-abandoned cgroup back on a
  one-minute clock so sampling resumes within a minute of the service
  returning. Both are counted in `kubescrape_cgroup_unresolved_total`, whose
  `outcome` label separates them.
* A container that is still **listed but answers no read** for three
  consecutive windows is **retired**: its descriptors are released, a
  throttled warning is logged and `kubescrape_cgroup_containers_retired_total`
  is incremented. It is then held on the ten-minute clock rather than
  re-adopted on the next 15-second discovery pass, so a lingering CRI-O
  supervisor scope or a stale listing cannot pin three descriptors and three
  failing reads per second indefinitely. Consequently
  `kubescrape_cgroup_containers` counts what is being **sampled**, not what is
  tracked — a container that goes unreadable leaves the gauge after one window
  and rejoins it if a read succeeds again.
* `-check-config` prints `cgroupStats=requested`, never `on`: the dry run
  acquires nothing and cannot know the node's cgroup version, so it reports
  what was asked for. The startup log is authoritative — `cgroup sampler
  started` or `cgroup stats are not available on this node`.


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
  goroutine or killing the process. A batch script that errors also warns
  throttled, naming the signal and the failing script position
  (`script=logs.star:7:14`): without it the only line was the producer's own
  "exporting logs failed", which reads as a collector problem when the
  collector was never asked. The bounds are a **step limit**
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
  debugging a predicate without flooding the agent's own stream. The text
  arrives as `output=` on a `msg="transform script log"` line — it used to be
  `msg=`, which collided with slog's own key for the record's message — so
  `grep 'transform script log'` finds every one of them.

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
  undeclared name is a script error, and so is a label named `le` on a
  histogram — a script's label names are the only ones not known before the
  agent starts, so this is where that refusal has to live (see the callout
  under **Agent: log-derived metrics**). Retries re-run scripts, so a transient
  export failure re-emits — the same at-least-once every producer's metrics
  already have.

### Hooks

Four more optional sections in the same hot-reloaded file put scripts at
other decision points. Each defines its own function, and each **fails
open** — a script error degrades to "the hook did nothing" (counted in
`kubescrape_transform_errors_total{signal}`, warned throttled, the warning
naming the failing script position as `script=targets.star:3:9`), never to
data loss:

```yaml
ingest: |                  # per pushed RESOURCE, before enrichment
  def admit(resource):     # False removes it (counted in
      return resource["team"] != "banned"   # kubescrape_ingest_admission_rejected_total)
targets: |                 # per fetched scrape target, once per cycle
  def target(t):           # t.url/.path/.namespace/.pod/.labels/.source/.monitor
      if t.labels["scrape-tier"] == "none":
          t.drop()         # counted in kubescrape_transform_dropped_total{signal="targets"};
                           # the dropped URL is named at -log-level=debug
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
declarative config. A dropped target has no other symptom — it is never
fetched, so no `up` series falls to 0 — which is why the drop is counted. The sample policy plugs into the `tailSampling` policy
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
Returning anything OTHER than a dict or `None` leaves the line unparsed too,
but it is a script bug rather than a decision: it counts
`kubescrape_transform_errors_total{signal="parse"}` and warns throttled,
naming the type that came back.

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

`kubescrape_routed_failures_total{route,signal}` is the missing half of that:
parts a destination REFUSED. Without it a route that never worked was
indistinguishable from a route nothing matched, and those need opposite
responses. A route outage produces TWO throttled lines — the client's, naming
the endpoint and the likeliest remedy, and the router's, naming the route —
because the two layers know different halves of the identity. A third counter,
`kubescrape_routed_unknown_total`, catches a transform script calling
`route("name")` for a name no route defines: the payload falls back to the
default chain, so the effect is silent mis-tenanting rather than loss, and the
route the script asked for rides the throttled log line (the name is
script-chosen and unbounded, so it must not become a label).

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
resource attributes. It defaults to `cadvisor` for the cadvisor pipeline, to
`summary` for the `-kubelet-summary` one (`defaultInstancePrefix` in
`internal/agent/attrs/builder.go` holds exactly that pair; the summary entry
prevents the same clash one level up, its node resource carrying `service.name`
`kubelet` and the node's name exactly as the `-node-metrics` scrape's does — so
the summary node resource is `service.instance.id: summary-<node>` out of the
box, as documented under `-kubelet-summary`), and to the describing target's
`service.name` for splitter rules; set `""` to disable. Precedence: explicit
pipeline section > built-in default > top-level base — so a top-level
`instancePrefix: ""` does NOT strip the built-in `summary-`; only
`pipelines.summary.instancePrefix: ""` does. The two knobs, in one place (they
are config keys only — the former `-resource-attrs-static`/`-enable`/`-disable`
flags are gone, and the sole home for each is below):

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
  # resource) omit the attribute. A template that FAILS (as opposed to
  # rendering empty) says so once per template at -log-level=debug, naming
  # the attribute key and the error; value-independent mistakes are still
  # refused at startup, so what reaches that line is a template that does
  # not fit the context it was given. enable/disable likewise report each
  # key they remove, once per key, at debug.
  attributes:
    team: '{{ index .Pod.Labels "team" }}'
    container.image: '{{ with .Container }}{{ .Image }}{{ end }}'
    k8s.node.zone: '{{ with .Node }}{{ index .Labels "topology.kubernetes.io/zone" }}{{ end }}'
    service.name: >-
      {{ with .Pod }}{{ coalesce (index .Labels "gp/service-name")
      (index .Labels "app.kubernetes.io/name") .Name }}{{ end }}
    infra: '{{ with .Pod }}{{ if regexMatch "-system$" .Namespace }}yes{{ end }}{{ end }}'

  # Per-pipeline overrides (logs | targets | cadvisor | node | summary |
  # journal | ingest | self — pipelineNames in internal/agent/attrs/builder.go
  # is the authoritative list, and -check-config prints it on a typo);
  # maps merge with the pipeline entry winning. `summary` governs the
  # /stats/summary NODE resource only: its pod/container/volume resources are
  # the CADVISOR pipeline's, because a summary series is worth something only
  # while it joins the cadvisor series for the same object, and two resources
  # join only while they are byte-identical.
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
to every pipeline; then `targets`, `cadvisor`, `node`, `summary`). First matching rule
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

That chain is **bounded**, in two places, because it is tenant-authored and
somebody else pays for it: it is copied into every target the monitor
resolves to (and the whole node's targets are marshalled into one response
body per agent poll), and the agent walks it per SAMPLE. One endpoint may
declare 64 rules totalling 8 KiB of regex and source labels, and a single rule
may spend that whole 8 KiB (the one shape that is legitimately large is a
metric allowlist written as one `keep` rule with a long alternation); a
**served target** may carry 128 rules totalling 16 KiB once every monitor that
resolved to its URL has merged in (chains concatenate, so N monitors on one URL
would otherwise multiply). The rule-count and chain-byte ceilings keep the
PREFIX and refuse the rest — the excess filters nothing, which **fails open**
for the refused tail: a `keep` that does not survive the cut was an allowlist,
and without it the target exports what the allowlist excluded. A single rule
that alone exceeds the whole chain budget is the exception and **refuses the
endpoint** (no targets at all, like the string ceilings below), because it fits
no chain in any order and honouring the endpoint without it is precisely that
silent inversion. Neither is
reachable by an ordinary chain (kube-prometheus-stack's own monitors carry a
handful of rules; a metric allowlist is normally one keep rule with a long
alternation). The parse-time refusal is reported through
`Endpoint.Ignored`/`kubescrape_monitor_fields_ignored_total`, the merge-time
one through `kubescrape_monitor_relabel_chain_capped_total{kind}`, a
throttled warning naming the monitor and the URL, and the endpoint's note in
`GET /v1/explain/{ns}/{pod}` — a refused rule is invisible in the data, since
the series it would have dropped simply arrive.

A **third** ceiling bounds the merged target's `monitors` list at 32
contributors, against the cheaper version of the same attack: a contribution
costs a finer `interval` and nothing else, so N monitors carrying no
`metricRelabelings` at all — each `selector: {}` with `namespaceSelector.any`
— can collide on one URL without reaching either chain bound and without adding
a target for the per-pod ceiling to see (measured: 2,000 of them made a
one-pod node's targets document 124,793 bytes instead of 2,777). What that
ceiling refuses is **attribution only** — the endpoint's rules, cadence and
auth have already merged, so the scrape is unchanged and a monitor simply stops
being *named* — which is also why it needs
`kubescrape_monitor_contributors_capped_total{kind}` and a throttled warning
keyed by the URL: no consumer of the served document could otherwise tell.
`GET /v1/explain/{ns}/{pod}` gives the per-monitor answer. Real collisions are
two or three monitors; if this ever binds, reconcile the CRs.

A **fourth** ceiling holds the endpoint's own STRINGS — `path`,
`interval`/`scrapeTimeout`, `tlsConfig.serverName`, `authorization.type` and
the rendered `basicAuth`/`authorization`/`bearerTokenSecret`/`tlsConfig`
secret references — to a few hundred bytes each (a scrape path to 2 KiB),
because every one of them is copied into every target the endpoint resolves
to exactly as the relabel chain is: one endpoint with a 1 MiB `path` and no
`metricRelabelings` at all yields ONE target of 2,097,625 bytes, once per
matched pod, in a document re-derived and re-marshalled on every agent poll.
Like the oversized-single-rule case above and unlike the prefix-keeping
ceilings, this one **refuses the endpoint** — it yields no targets — rather
than keeping a prefix: a truncated path scrapes a URL the
monitor does not name, and a dropped one defaults to `/metrics`, which is
very often a URL the pod already has a target for, so the endpoint's relabel
rules and credentials would merge onto somebody else's scrape. The refusal
rides `Endpoint.Ignored` (hence `kubescrape_monitor_fields_ignored_total` and
the per-upsert warning) and names the offending fields — never their values —
in `GET /v1/explain/{ns}/{pod}`. `prometheus.io/path` is held to the same 2
KiB at the annotation door, with the same refusal and the same explain note.
The report the ignored-field machinery produces is bounded too (per endpoint,
and per monitor across endpoints), and the tenant-chosen halves it echoes
(`metricRelabelings.action=`, `.separator=`) are clipped: a bound on the
rules a monitor applies is not a bound on the report about the ones it
refuses.

A **fifth** ceiling is the backstop under all four: one pod's targets may total
**256 KiB**, whatever they carry. Each of the four above closes one multiplier
on the node-targets response, and the largest one on that path was never a
monitor field at all — it is the **pod's own annotations and labels**, which
every `ScrapeTarget` embeds by value and the document re-marshals once per
target. Measured with all four fully respected: one pod carrying a 200 KiB
annotation (the API server permits 256 KiB per object), `prometheus.io/scrape:
"true"` and a 16-entry `prometheus.io/port` yields 16 targets and a
3,283,798-byte body — ten such pods on a node is ~33 MB per `GET
/v1/nodes/{node}/targets`, re-derived and re-marshalled on every agent poll, in
the singleton the chart requests 128Mi for with no memory limit, and
`writeCached` must BUILD the body to hash its ETag so a 304 does not save it.
The charge is the whole target document measured once per pod, so this bounds
every per-target string there is — including the ones nobody has thought to
bound yet, which is why it is a backstop rather than a sixth field limit. The
pod's **first** target is unconditional: annotations are legitimate attribution
data, so the ceiling bounds the MULTIPLIER and never the workload, and a
fat-but-honest pod is still scraped. Refusals count into
`kubescrape_scrape_targets_capped_total` — the same counter the 16-target
ceiling moves, since either way an endpoint is not being scraped — and `GET
/v1/explain/{ns}/{pod}` tells the two apart, with `cappedTargetsBySize`,
`podDocumentBytes` and a wording naming the remedy (shrink the pod's
annotations, or split its ports across workloads — not "declare fewer ports",
which is the answer to the other ceiling). None of the four is redundant now:
the 16-target ceiling still bounds a SMALL pod's target count, the field
ceilings still give the sharper diagnostic and still bind on the unconditional
first target, and the merge-time chain and contributor bounds are the only thing
that can REFUSE on the merge arm. The merge arm is nonetheless **charged**: a
fold grows the held target by up to 16 KiB of merged chain and 32 contributor
names (~26 KiB, ~400 KiB across a pod's 16 targets — bounded and deterministic,
needing ≥32 monitors on one URL), which used to ride entirely outside a budget
whose claim is that it charges the whole target document. It now spends the
pod's budget, so the next NEW url is measured against what is really being
served; nothing is refused by the charge, because refusing a merge would drop
relabel rules a monitor asked for — changing what is *exported* in order to
bound a response.

A **sixth** pair of ceilings is one level down, on the **pod document itself**,
because the fifth cannot see it: the pod's first target is unconditional by
design, and that one document was unbounded. It carries the pod's own
annotations, its **namespace's** (copied per pod — `/v1/pods/{ns}/{name}` has to
serve a self-contained document, so de-duplicating it would be a response-format
change that helps only the node-targets route), and **one annotation set per
resolved `ownerReference`** — and Kubernetes bounds neither the
`ownerReferences` count nor what an owner may annotate, while one fat owner can
be named by every pod on the node. A tenant with edit rights in ONE namespace
creates 100 fat ReplicaSets and points every pod at all of them: measured, ~25
MB per pod document and ~125 MB in a five-pod node's `targets` response.

So annotations are bounded **at the source**, in `kubemeta.CopyMeta`, for every
object this API serves (pods, owners, namespaces/nodes, Services): a single
value over **8 KiB** and an object's whole set over **16 KiB**. Real values sit
far below both once the deploy-tool copies of the applied object are dropped —
the fattest in the field are an istio sidecar status or a CNI network-status at
~1–2 KiB. Two rules matter more than the numbers:

* An oversized value is **omitted whole, never truncated**. Annotations are
  load-bearing for attribution (`resourceAttributes` templates read them), and a
  silently shortened value is the worse failure — a template that renders half a
  value looks like it worked.
* `prometheus.io/*` and `kubescrape.io/*` are admitted **first** when the total
  budget binds, so an unrelated blob on the same object can never starve the
  annotations the derivation itself reads. (A pod whose *own* `prometheus.io/*`
  annotations exceed the budget still loses some — that is self-harm on one pod,
  not a lever on anyone else. So is a `prometheus.io/port` value over the 8 KiB
  per-value ceiling: it is refused like any other blob, and the pod then reads as
  un-annotated.)

The owner **count** is bounded beside it, at **8** chain entries
(`owners.MaxOwners`) — against a legitimate maximum of two, since a pod has one
controller reference and this resolver follows at most one parent
(ReplicaSet → Deployment, Job → CronJob); the handful of extra non-controller
references some operators add fits comfortably inside 4× that.

Every one of these refusals is **visible**, because a document that is short and
does not say so is worse than a big one:

* the object's own annotations carry `kubescrape.io/annotations-omitted`, naming
  the refused keys and the ceiling that refused them (a cluster-supplied copy of
  that key is stripped at the door, so only kubescrape can set it);
* `kubemeta.Pod.ownersOmitted` says how many `ownerReferences` were not
  described;
* `kubescrape_metadata_annotations_omitted_total{kind}` and
  `kubescrape_owner_resolve_failures_total{reason="owners_capped"}` count them,
  with a throttled warning naming the object;
* `GET /v1/explain/{ns}/{pod}` lifts both onto the head of its document
  (`annotationsOmitted`, `ownersOmitted`) — without them it would answer "why is
  this pod not scraped?" with `podAnnotated: false` for a pod whose annotation
  was refused at the source.

**What is still not bounded, said plainly: labels.** They are selection input —
Service and PodMonitor selectors match on them — so no filter can know which
label is load-bearing, and refusing one would silently change *what is scraped*
in order to bound a response. Each label is small by API-server validation (a
value of at most 63 bytes), but their count is bounded only by the object's ~1.5
MiB ceiling, so a pod, an owner or a namespace can still carry ~1 MiB of them.
That is what the fifth ceiling binds on today.

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
Left unset, the chart excludes this release's own namespace *plus* the namespace
of the one in-cluster **logs** destination — `agent.config.export.logs.endpoint`
when set, otherwise `agent.otlp.endpoint`, since a non-empty per-signal endpoint
REPLACES the base rather than adding to it (so with `export.logs` set, the flag
base receives no logs at all and excluding its namespace would stop collecting
logs it never caused) — read as
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

`agent.cgroupStats.enabled: true` renders more than a flag: the agent gets the
host's `/sys/fs/cgroup` bind-mounted read-only, because the sampler has nothing
to read without it. `interval` maps to `-cgroup-stats-interval` and `root` to
`-cgroup-stats-root`.

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
bearer token and passes `-service-graph-token-file`, and it is REQUIRED whenever
the tier is enabled: the internal receiver is reachable from every pod in the
cluster, so the binary refuses to start without the flag, and the chart now
refuses to RENDER without the value rather than installing a StatefulSet that
CrashLoopBackOffs on every shard. `serviceGraph.port` (default 4319) is that
receiver.

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
the tier's `-listen` port is reachable cluster-wide unless you add a policy of
your own — which is why `/debug/otlp` on it is not open: like the DaemonSet's,
that stream is served only to a local connection (`kubectl port-forward`) or to
a request carrying the `-debug-token-file` token. See that flag for what the
gate does and does not cover.

`serviceGraph.spanMetrics` turns on the RED metrics derived from the spans this
tier receives; tuning is `agent.config.traceMetrics`, and edge-pairing tuning is
`agent.config.serviceGraph`. The tier mounts the same rendered ConfigMap as the
DaemonSet and simply ignores the sections that are not its.

`agent.debug.tokenSecret.name` is how you reach the gated debug surfaces from
somewhere other than a `kubectl port-forward`. Name a Secret and the chart
mounts it and passes `-debug-token-file` on **every** workload running the agent
binary — the DaemonSet, the events/Azure singleton and the trace tier — because
they all serve the same routes; `key` (default `token`) is the key within it,
mounted at `/etc/kubescrape/debug/<key>`. Leave it empty (the default) and those
surfaces are local-only, which needs no configuration and is the safe posture:

```sh
kubectl -n monitoring create secret generic kubescrape-debug \
  --from-literal=token="$(openssl rand -hex 24)"
helm upgrade … --set agent.debug.tokenSecret.name=kubescrape-debug

# then, from anywhere:
curl -sN -H "Authorization: Bearer $TOKEN" http://<agent-pod-ip>:8081/debug/otlp?sample=10
```

`networkPolicy.enabled` (**true by default**) renders ingress policies that
restrict each pod to the ports the chart declares, so an undeclared listener
(pprof) is closed everywhere; on a cluster with no NetworkPolicy controller the
objects are inert, so the default costs nothing there. What a rule restricts
*who* to is a separate decision per port, and three values carry it:
`agent.ingest.allowFrom` (the DaemonSet's OTLP ingest ports),
`serviceGraph.ingest.allowFrom` (the tier's application trace ports) and
`agent.debug.allowFrom` (the `listen` port on every agent-binary workload). **All
three default to empty, which means any pod may connect** — the first two are
unauthenticated writers and are what to tighten first; the third is a reader that
is already gated in-process. Read `agent.debug.allowFrom`'s comment in
[values.yaml](../charts/kubescrape/values.yaml) before setting it: a
NetworkPolicy scopes a *port*, not a path, and `/healthz`+`/readyz` share that
port — narrowing it narrows the kubelet's probes too, and a failing readiness
probe stops a DaemonSet rolling update dead. The metadata service's API port has
no `allowFrom` value at all, deliberately: narrowing it wrongly makes every agent
on the cluster stop resolving, so that one is an edit to
`templates/service.yaml` you make consciously.

`seccompProfile` (default `type: RuntimeDefault`) is a pod-level profile applied
to all four workloads: PodSecurity's `restricted` profile rejects a pod that does
not set it, so without it three of the four fail admission on a restricted
namespace. The agent DaemonSet cannot satisfy `restricted` whatever this says —
it runs as UID 0 and mounts hostPath `/var/log` — so its namespace needs
`privileged`. If you set `type: Localhost`, set `localhostProfile` too; the
chart's schema cannot express that requirement (see
[Accepted security residuals](#accepted-security-residuals)).

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
logLevel: info

service:
  replicas: 2
  cacheTTL: 10m
  podDisruptionBudget: {enabled: true, maxUnavailable: 1}

agent:
  # Correct on IPv4 and IPv6 alike: on an IPv6 node this expands to a bare
  # IPv6 literal, which is not a parseable URL authority, and the agent
  # brackets the host itself. Do NOT pre-bracket it — [$(NODE_IP)] renders
  # [10.0.0.5] on IPv4, which Go rejects as an invalid IP-literal.
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
