# First run

This is the runbook for installing kubescrape into a cluster for the first
time: what must exist before you install, the smallest configuration worth
running, what to watch while it comes up, and what each way it can fail looks
like from outside.

It assumes you have **logs, metrics and these docs** and nothing else — no
shell on a broken pod, no dashboard anyone has built yet. Everything below is
therefore either a log line you can grep, a metric you can query, or an HTTP
endpoint you can `kubectl port-forward` to.

* [Pre-flight](#pre-flight)
* [The first install](#the-first-install)
* [The first ten minutes](#the-first-ten-minutes)
* [Symptom → cause](#symptom--cause)
* [What to alert on](#what-to-alert-on)
* [What NOT to alert on](#what-not-to-alert-on)

## Pre-flight

Six things must be true before the first `helm install`. Each of them fails in
a way that is diagnosable, but knowing them up front saves the diagnosis.

**1. A collector endpoint that exists.** kubescrape exports OTLP and stores
nothing itself. The default is `otel-collector.monitoring:4317` (plaintext
gRPC), which is a *guess* about your cluster and is wrong more often than it is
right. Point `agent.otlp.endpoint` and `service.otlp.endpoint` at a real one, or
deploy the sample collector in [`hack/otel-collector.yaml`](../hack/otel-collector.yaml)
first. An unreachable collector is not fatal — the agent keeps tailing and
back-pressures — but it makes every other signal harder to read.

**2. RBAC, applied with or before the image.** The metadata service needs
`get`/`list`/`watch` cluster-wide on `pods`, `services`, `namespaces`, `nodes`,
`replicasets.apps`, `deployments.apps`, `statefulsets.apps`, `daemonsets.apps`,
`jobs.batch`, `cronjobs.batch` — every one of them is watched at startup and
readiness waits for all of those caches, so a missing rule leaves `/readyz`
failing rather than degrading quietly. `servicemonitors` and `podmonitors` are
additionally needed for `-servicemonitors`, and `secrets get` for
`-scrape-auth-secrets`; the chart grants the monitor rules unconditionally and
the Secret rule only with the flag. The agent DaemonSet needs almost nothing:
`get` on `nodes/metrics` and `nodes/stats`, and no Kubernetes access at all
beyond that — the two cluster-scoped singletons are the exceptions (`-events`
reads events cluster-wide and writes a Lease and a ConfigMap for leader
election and its position; `-azure-diagnostics` touches no Kubernetes object at
all). The chart renders all of it; a hand-maintained ClusterRole must be updated
in the same change as the image.

**3. A kubelet endpoint, if you want cadvisor and node metrics.**
`-kubelet-endpoint` is EMPTY by default in the binary (the chart and
`deploy/agent.yaml` both set `https://$(NODE_IP):10250`). Empty means the
kubelet scrapes are never scheduled: nothing is attempted, nothing fails, no
counter moves — the only symptom is the missing metrics. The agent warns about
exactly this at startup, and `-check-config` prints the same warning without
starting anything.

**4. Pod Security, if your namespaces enforce it.** The agent runs as UID 0 and
mounts `/var/log` as a hostPath, so it can never satisfy the `restricted`
profile — its namespace needs `privileged`. The other three workloads (metadata
service, the events/Azure singleton, the trace tier) do satisfy `restricted`
and the chart sets `seccompProfile: RuntimeDefault` so they pass admission.

**5. The build variant you are actually running.** Three agent pipelines sit
behind Go build tags, and the *Makefile* carries the default
(`journald,azure,events`), not the constraint — so the shipped image has all
three, and a bare `go build` has NONE. `-journald`, `-azure-diagnostics` or
`-events` on a binary that lacks the pipeline is a startup error naming the tag
and the rebuild, never a flag that silently collects nothing. Every start prints
which binary it is: `optionalPipelines=journald,azure,events`, or `=(none)`.

**6. No leftover `-log-format`.** The flag was removed; both binaries log
logfmt and only logfmt. Go's `flag` package exits 2 on an undefined flag, so a
`-log-format=json` still sitting in `agent.extraArgs`, `service.extraArgs` or a
hand-edited manifest is a CrashLoop, not an ignored setting. Grep your values
files. See [Logging](CONFIGURATION.md#logging).

Then, before you install anything, dry-run BOTH binaries' configuration.
Each compiles what it has, prints the same startup summary a real start
prints, and acquires nothing — no listeners, no informers, no API-server
traffic:

```sh
kubescrape-agent -check-config -config=agent.yaml -node-name=preflight
kubescrape -check-config
```

Pass the service the flags you intend to deploy it with (`-servicemonitors`,
`-monitor-namespaces`, `-scrape-auth-secrets`, the listener addresses): the
check is over its flags, and it refuses the ones that are silent at runtime
— a `-monitor-namespaces` entry that is a glob or not a namespace name (the
flag is an exact list, unlike the agent's namespace globs, so such an entry
indexes no monitor and says nothing), an empty or duplicated listener
address, a negative duration, and `-scrape-auth-secrets` without
`-scrape-auth-token-file`.

What it deliberately does NOT judge is the ENVIRONMENT: whether the
scrape-auth token file is readable, and whether an API server is reachable
from the machine you run it on. Both are false on a laptop by construction,
and both are already fatal at a real start. Run off-cluster it will say
`apiServer=(unresolved)` and still reach its verdict.

## The first install

**Install this first.** The metadata service plus the agent's two default
pipelines — container logs and annotation-discovered Prometheus targets — plus
the kubelet scrapes. That is the whole product for most clusters, and
everything else is an addition to a system you can already see working:

```sh
helm install kubescrape charts/kubescrape -n monitoring --create-namespace \
  --set agent.otlp.endpoint=my-collector.observability:4317 \
  --set service.otlp.endpoint=my-collector.observability:4317
```

The chart's defaults already do four things worth knowing about, because each
of them is a first-run failure that has been made impossible rather than
documented:

* `agent.positionsFile` is set, so log offsets and the journal cursor survive a
  pod restart. Without it a restart re-reads per `-logs-unknown-files` and
  journald resumes at the journal *tail*, losing everything written while the
  process was down.
* `agent.logsExcludeNamespaces` defaults to `null`, which the template reads as
  "this release's namespace, plus the namespace an in-cluster **logs**
  destination names". Both are feedback loops that amplify exactly when the
  collector is already struggling. An explicit `[]` means exclude nothing.
* `agent.kubeletEndpoint` is set (see pre-flight 3).
* `metrics.scrapeAnnotations` stamps `prometheus.io/*` on both pods, so
  kubescrape's own discovery picks up its own runtime metrics with no extra
  configuration.

**Turn on second, once logs and metrics are arriving.** In roughly this order,
because each depends on the one before it being demonstrably healthy:

| Then | Why second, not first |
|---|---|
| `logScrubbing` | It is a compliance control on data you are already shipping; you want it before the volume grows, but after you can see what a line looks like. Every redaction counts `kubescrape_log_scrubbed_total{pattern}`. |
| `logs.rules` / `logs` source `namespaces` | Cost control. Source-level selection beats a `rules` drop, which pays read+parse+enrich first and saves only egress — but you need a week of `kubescrape_log_entries_total` to know what to cut. |
| `-buffer-dir` | Turns a collector outage from back-pressure into a bounded on-disk backlog. Point it at a node-local persistent path. Watch `kubescrape_buffer_backlog_bytes / kubescrape_buffer_max_bytes`. |
| `service.serviceMonitors` | Honouring monitors cluster-wide means any namespace owner can direct every agent's scrapes. Add `-monitor-namespaces` at the same time unless that is genuinely what you want. |

**Turn on third, deliberately, one at a time.** These are separate workloads or
separate cost profiles, and each has its own failure mode:
`agent.kubeletSummary` (per-pod ephemeral storage and volumes; needs
`nodes/stats`), `agent.journald.enabled` (node unit logs; cgo, and the
`journald` build tag), `agent.cgroupStats` (1s CPU/memory distributions; ~0.5%
of a core and ~5.5 MiB RSS at 200 containers), the `-events` singleton
(cluster-wide Kubernetes events; its own Deployment with leader election), and
`-service-graph` (the trace tier — its own StatefulSet, and a hard cluster-wide
dependency for traces once applications point at it).

## The first ten minutes

### Minute 0-1: does each process describe itself?

```sh
kubectl -n monitoring logs deploy/kubescrape | head -8
kubectl -n monitoring logs ds/kubescrape-agent | head -10
```

Both binaries emit a build-identity line and then five `effective …` Info lines
naming every pipeline, destination, listener, identity and cap they will
actually use. This is the single highest-value thing to read on a first run,
because the commonest first-run failure is a process doing exactly what it was
told rather than what was meant:

```
level=INFO msg="kubescrape-agent starting" version=v1.2.3 built=… optionalPipelines=journald,azure,events
level=INFO msg="Go soft memory limit set from the cgroup memory limit" limitBytes=482344960 cgroupLimitBytes=536870912 share=0.9 …
level=INFO msg="effective configuration" role=node-agent sections=logs pipelines="logs=on metrics=on cadvisor=on …"
level=INFO msg="effective destinations" metadataEndpoint=http://kubescrape.monitoring otlpEndpoint=… kubeletEndpoint=…
level=INFO msg="effective listeners" listen=:8081 debugAccess=local-only metricsListen=:9090 pprofListen=""
level=INFO msg="effective identity" node=node-1 namespace=monitoring serviceName=kubescrape-agent instance=node-1
level=INFO msg="effective limits" scrapeInterval=30s scrapeTimeout=15s logsExcludeNamespaces=monitoring …
```

The **second** of those is not something you configured (it sits next to the
build identity because it is applied before anything large is allocated). Both binaries derive
`GOMEMLIMIT` from **their own container's** cgroup memory limit (90% of it) at
startup, so a heap excursion costs GC instead of an OOMKill; a workload with no
`limits.memory` — which is how the metadata service ships, on purpose — gets no
limit and logs nothing at Info. `GOMEMLIMIT` in the environment overrides it,
and `GOMAXPROCS` needs nothing (Go has derived it from a cpu limit since 1.25).
See [Runtime memory and CPU](CONFIGURATION.md#runtime-memory-and-cpu-gomemlimit-gomaxprocs).

Read the `level=WARN` lines that follow them. They are the legal-but-surprising
combinations, each naming the flag to change — an empty `-kubelet-endpoint`, a
missing `-positions-file`, guard rails that compose oddly with tail sampling.
None of them stops the process; all of them mean something you probably did not
intend.

### Minute 1-2: did it become ready?

```
level=INFO msg="informer caches synced" pods=412 containers=1180 waited=1.9s      # service
level=INFO msg=ready waited=3s gates=metadata-service                              # agent
```

If those lines do not appear, after 30 seconds you get the negative instead,
naming what is pending and re-stating it every two minutes:

```
level=WARN msg="not ready: /readyz is 503, so a rolling update will not advance past this pod" gates=metadata-service waited=30s
level=WARN msg="not ready: informer caches have not synced, so /readyz is 503 and this replica has no Service endpoints" caches=pods,services waited=30s note="a cache that never syncs is usually a missing RBAC rule for that resource; the reflector retries it forever"
```

The metric is the same statement fleet-wide, and it is the one that works when
you cannot reach the pod (an unready replica has no Service endpoints, so
nobody can curl its probe — the self-metrics push runs from startup anyway):

```promql
kubescrape_readiness_gate == 0
```

An **absent** gate means that pipeline was never wired, i.e. it is off — never
that it is healthy.

### Minute 2-5: is data actually moving?

Four queries, in this order. Each answers a different question and the order
matters, because a failure in an earlier one explains the later ones.

```promql
# 1. Does the service have a cluster in it?
kubescrape_store_pods
kubescrape_store_containers

# 2. Did this node get any scrape targets?
kubescrape_scrape_targets

# 3. Are scrapes succeeding, and if not, why?
sum by (pipeline, outcome) (rate(kubescrape_scrapes_total[5m]))
sum by (pipeline, reason)  (rate(kubescrape_scrape_failures_total[5m]))

# 4. Is the collector accepting what we send?
sum by (signal, outcome) (rate(kubescrape_export_requests_total[5m]))
```

Then the two log-pipeline gauges, which are the tailer's whole health in two
numbers:

```promql
kubescrape_log_files              # tracked files
kubescrape_log_files_unresolved   # tracked but not yet attributable, so NOT being read
rate(kubescrape_log_entries_total[5m])
kubescrape_log_lag_max_bytes      # worst single file's backlog
```

`kubescrape_log_files_unresolved` is the one that is easy to miss: those files
are counted, cost nothing, lose nothing — and produce no records. It returns to
0 on resolve, so any nonzero value is a *current* condition. The agent also
warns about the oldest waiting file once it has been waiting two minutes.

### Minute 5-10: is it the RIGHT data?

Metrics tell you the rate; the debug endpoints tell you about one object. Port-
forward and start at `/debug` on either binary — it is a homepage that indexes
exactly what that process serves, so it cannot advertise a surface that is off.

```sh
kubectl -n monitoring port-forward ds/kubescrape-agent 8081:8081
# then browse http://localhost:8081/debug   ← as localhost, see below
```

| Endpoint | On | Reach for it when |
|---|---|---|
| `/debug` | both | you do not know what else this process exposes. Root redirects here. |
| `/v1/explain/{namespace}/{pod}` | service | "why is this pod (not) scraped?" It walks the same decision chain that derives the targets — scrapeable verdicts, per-port resolution, Service and monitor endpoint verdicts, the final dedup — so the explanation cannot drift from the derivation. Always 200 with a JSON body, `found:false` for a miss. |
| `/debug/targets` | agent | "which targets does THIS node have and what happened to each?" Last outcome per target, failures first, undue targets shown as pending. |
| `/debug/tailer` | agent | "which files is this node tailing, and how far behind?" Per-file positions and lag, largest first, plus any pod's malformed `kubescrape.io/logs` annotation. |
| `/debug/otlp` (+ `/ui`) | agent | "what is this agent actually EXPORTING?" A live stream of post-transform OTLP as JSON lines, filterable by resource attribute (`attr=k8s.namespace.name=team-*`), signal and sample percentage. This is what settles "the agent is not sending it" versus "the backend is not showing it". |
| `/debug/transforms` | agent | after editing the transforms file: the active program's content hash, so you can see which nodes have converged. |

`/debug/otlp` costs one atomic load per export while nobody is attached; each
attached stream renders every exported payload on the exporting goroutine, so
attach and detach are logged at Info and a session refused by the four-stream
cap at Warn. Detach when you are done.

**Three of those are gated, and a `curl` from the wrong place is refused.**
`/debug/otlp`, `/debug/otlp/ui` and `/debug/tailer` are the node's telemetry
feed and the list of files behind it, on a port every pod in the cluster can
reach — so by default they answer only a **local** connection. Two rules, and
both are satisfied by an ordinary port-forward:

* the connection must arrive on loopback. `kubectl port-forward` does exactly
  that (the kubelet dials `127.0.0.1` inside the pod), and reaching it already
  needs `pods/portforward` on the namespace. A `curl` from a debug pod at the
  agent's pod IP does not, and is refused `no_token`;
* the request's `Host` must be `localhost`, `127.0.0.1` or `::1`. So reach the
  forwarded port as `http://localhost:8081/…`, not through a hostname you
  pointed at it — a mismatch is refused `host`, which is the DNS-rebinding
  guard doing its job. A forwarding header (`X-Forwarded-For` and friends) on a
  local connection is refused `forwarded` for the same family of reason.

To read an agent from anywhere else, give the fleet a token —
`agent.debug.tokenSecret.name` in the chart, `-debug-token-file` on the flag —
and present it as `Authorization: Bearer <token>`; a valid token satisfies the
gate on its own, from any address, and skips both local checks. Rotating the
Secret needs no restart. Which posture a process is in is on its startup line
and in `-check-config` as `debugAccess=local-only` or `debugAccess=token`, and
every refusal counts `kubescrape_debug_refused_total{reason}` and logs a
throttled Warn naming the peer — so "my curl gets a 403" and "somebody is
probing this port" are the same signal read two ways.

`/healthz`, `/readyz`, `/debug`, `/debug/targets` and `/debug/transforms` are
**not** gated: probes must answer, and target/transform state is what the
metadata service's `/v1` routes already serve to anyone. The metadata service's
own `/debug` and `/v1/explain` are ungated for the same reason.

## Symptom → cause

Each row starts from what you can see without a shell, and names the signal
that distinguishes it from its neighbours.

### Nothing at all in the backend

| Signal | Cause | Fix |
|---|---|---|
| `kubescrape_export_requests_total` absent entirely, and no `kubescrape_*` metrics anywhere | The self-metrics push is going to the same unreachable collector, so the absence is circular. Read the pod logs, not the metrics. | Set `-self-metrics-interval=0` temporarily: `kubescrape_*` then rides the `-metrics-listen` port's `/metrics` instead, and you can scrape it directly. |
| `rate(kubescrape_export_requests_total{outcome="transient"})` > 0, `ok` at 0 | The collector is unreachable or back-pressuring. The payload is retried (or spooled with `-buffer-dir`); nothing is lost yet. | The throttled Warn names the endpoint and the likeliest cause. Check the endpoint, the port, TLS, and the collector's own logs. |
| `rate(kubescrape_export_requests_total{outcome="permanent"})` > 0 | The collector rejected THIS payload and no retry can help — a body limit, an unimplemented signal, a malformed request. **Telemetry is being lost.** | Usually the collector's `max_recv_msg_size` below our `-otlp-max-send-bytes`. Lower ours. |
| Exports are `ok` but nothing appears | The data went somewhere else. A `routing` route, a transform `route()` call, or a per-signal `export:` override. | `kubescrape_routed_payload_parts_total{route,signal}` shows what went where; `kubescrape_routed_unknown_total` catches a script routing to a name no route defines (it falls back to the default chain — silent mis-tenanting). |

### No scrape targets

| Signal | Cause | Fix |
|---|---|---|
| `kubescrape_scrape_targets` **absent** | Annotation and monitor scraping is off (`-metrics=false`). | Turn it on, or stop expecting targets. |
| `kubescrape_scrape_targets` **== 0** | The metadata service answered and this node genuinely has no targets: no pod here carries `prometheus.io/scrape`, no annotated Service selects one, and no monitor resolves here. | The agent warns naming the node, re-stating every 30 minutes, and logs Info when targets appear. Ask the service `/v1/explain/{ns}/{pod}` about a pod you expected to be scraped — it names the verdict at every step. |
| `kubescrape_store_pods` == 0 on the service | The pod informer has nothing. Almost always RBAC, or an API server the pod cannot reach. | `kubescrape_readiness_gate{gate="pods"}` and `kubescrape_apiserver_reachable`. |
| Targets exist but one pod's are truncated; `kubescrape_scrape_targets_capped_total` moves | One pod exceeded the per-pod target ceiling. Every target embeds the whole pod by value, so an unbounded count is an O(N²) response. | A throttled Warn names the pod and its workload; `/v1/explain` names the ports it refused. |
| ServiceMonitors are installed but nothing resolves; `kubescrape_monitors_rejected{kind}` > 0 | A monitor failed to parse, or `-monitor-namespaces` refused it. | The gauge is the state (still true now), `kubescrape_monitor_parse_errors_total` the event. `kubescrape_monitor_namespace_refused_total` is the gate. |

### Every target is `up=0`

Read `kubescrape_scrape_failures_total{reason}` **before anything else** — the
reasons take completely different remedies, and one of them means the target is
innocent:

```promql
topk(10, sum by (pipeline, reason) (rate(kubescrape_scrape_failures_total[5m])))
```

| `reason` | Means | Note on the log line |
|---|---|---|
| `dns`, `connect`, `tls`, `timeout` | The target could not be reached. | `tls` carries a note about scheme, `tlsConfig.ca`, `serverName` and `insecureSkipVerify`. |
| `unauthorized` | The target answered and refused the credential. For the kubelet pipelines this is the `nodes/metrics` ClusterRole rule; `/stats/summary` needs `nodes/stats` instead, and says so by name. | note names both cases. |
| `status` | Any other non-200. | |
| `auth` | The scrape never left this agent: a secret ref could not be resolved. | note: the service must run `-scrape-auth-secrets` and both sides must share `-scrape-auth-token-file`. Cross-check `kubescrape_scrape_auth_failures_total{reason}` on the service. |
| `relabel` | A monitor's `metricRelabelings` regex would not compile. The scrape fails deliberately rather than export what the rule asked to drop. | |
| `proto_refused` | The target served protobuf without `-scrape-native-histograms` having asked for it. | note: pass the flag, or fix the target to honour Accept. |
| `sample_limit`, `body` | The target answered wrong or too big. What was converted before the abort is still exported. | |
| `export` | **The target is fine.** The scrape succeeded and the COLLECTOR (or the spool) refused the payload. | note points at `kubescrape_export_requests_total`. |
| `canceled` | Shutdown. Not a fault. | |
| `other` | Unclassified. A rate on it means the list above needs a new value — worth reporting. | |

`kubescrape_scrapes_total{outcome}` and `kubescrape_scrape_failures_total` move
from the same place once per scrape, so their sums agree by construction: use
the first for the up/down ratio and the second for the cause.

### No logs from a namespace

| Signal | Cause | Fix |
|---|---|---|
| `kubescrape_log_files_skipped_total{reason="excluded_namespace"}` | `-logs-exclude-namespaces`, or a source's `excludeNamespaces`. On the chart's default this includes your release namespace and any in-cluster logs destination — the feedback-loop guard. | Set `agent.logsExcludeNamespaces: []` to exclude nothing, or list namespaces explicitly. |
| `…{reason="namespace_not_selected"}` | A source's `namespaces` ALLOWLIST did not match, and no later source claimed the file. | Routing, not prohibition — add a catch-all source or widen the list. |
| `…{reason="too_old"}` / `"non_regular"` / `"unparseable_name"` | `ignoreOlder` cut it; it is a FIFO or socket (never opened — the open would block the sweep goroutine node-wide); or the filename is not CRI-shaped. All three are selections working as configured, so they are counted and named at `-log-level=debug` only. | |
| `…{reason="stat_error"}`, with `msg="log file could not be stat'd and is not being collected"` | The path was listed but could not be `stat`'d. The one skip reason that is a FAILURE rather than a selection, so it is the one that WARNS at the default level — the counter says how many, the line names one path and the errno, which is what tells an unreadable mount from a failing disk. Throttled to one line per five minutes; a vanished path (`no such file or directory`) is a rename rotation caught mid-scan and is counted but never warned. | An `EACCES` is the log `hostPath` mounted with the wrong ownership or mode — the commonest first-run form. An `EIO` is the node. |
| `kubescrape_log_files_unresolved` > 0 | The files ARE tracked; their container metadata has not resolved, so nothing is read. Nothing is lost — it waits on disk. | The Warn names the oldest waiting file. Check the metadata service is reachable and `kubescrape_log_metadata_budget_exhausted_total`, which means the sweep ran out of resolve budget and never even ISSUED the requests. |
| Nothing skipped, nothing unresolved, still no records | A `logs.rules` drop, a per-pod `kubescrape.io/logs` annotation, or a transform script. | `kubescrape_log_rules_dropped_total`, `kubescrape_transform_dropped_total{signal="logs"}`, and `/debug/tailer` (which reports a pod's malformed annotation as `podConfigError`). |
| **Nothing is collected at all, on a fresh install** | With no stored positions, `-logs-unknown-files=auto` resolves to `end` — every EXISTING log file is skipped to its end, and only new lines are shipped. This is correct and is the most-reported non-bug. | The positions store says so explicitly at startup. Set `-logs-unknown-files=start` if you want the backlog. |

### The pod will not start, or will not stay up

| Signal | Cause | Fix |
|---|---|---|
| `flag provided but not defined: -log-format`, exit 2, CrashLoop | The flag was removed. Go's `flag` package exits rather than ignoring. | Remove it from `extraArgs` and any hand-edited manifest. |
| Exit 2 naming some other flag | A chart value rendered a flag the binary does not define. `internal/manifestcheck` prevents this for the shipped templates, so it means a hand-edited manifest or `extraArgs`. | |
| A startup error naming a build tag | `-journald` or `-azure-diagnostics` on a binary compiled without that pipeline. | Rebuild with the tag, or drop the flag. `-check-config` catches this before a rollout does. |
| Config error, exit non-zero, same message every time | A `-config` section did not compile. The refusal names the field and the value. | Reproduce locally with `-check-config`; it runs the same `validateConfig`. |
| A flag parsed as `0` where a size was meant | Helm's `int64` parses `"3MiB"` to 0, and 0 on `-logs-metrics-max-bytes` reads as ONE payload. The chart's helper fails the render on a non-digit value for exactly this. | Render integer byte values as integers. |
| OOMKilled: the **agent** | Usually the ingest or trace-tier paths, which buffer sender-controlled bytes. | `-ingest-max-in-flight` bounds processing; the raw and decoded byte budgets bound memory and scale from `-ingest-grpc-max-recv-bytes`. On the trace tier, `kubescrape_tail_sampling_buffered_spans` is exactly what a hard kill would lose, and `maxSpans` is sized against the cgroup limit at startup. Check the startup line for `Go soft memory limit set…`: if it is absent, the container has no `limits.memory`, and the GC has no ceiling to collect against. |
| OOMKilled: the **metadata service** | Its footprint scales with the SIZE OF THE CLUSTER — one record per pod plus the owner and service caches — which is why the chart sets no memory limit for it. | `kubescrape_store_pods × ~2 KiB` is the shape. Measure, then set a limit; do not copy the agent's. Setting one also turns on the soft memory limit, which this workload otherwise gets nothing from. |

### Attribution is wrong or missing

Data arrives but is not joined to the right object. These cost correlation
rather than data, so no loss counter moves:

| Signal | Cause |
|---|---|
| `kubescrape_cadvisor_unresolved_total{level}` | A cadvisor row could not be attributed. A low rate is ordinary (a just-started container whose id the kubelet has not posted yet); sustained means the metadata service is not answering, usually beside `kubescrape_scrape_metadata_budget_exhausted_total`. |
| `kubescrape_summary_unresolved_total{level}` | Same for `/stats/summary`. Static pods (kube-apiserver, etcd, scheduler) are the interesting case: the kubelet mints their UID from the manifest, and they resolve only through the mirror-pod cross-check. |
| `kubescrape_events_unresolved_total{reason}` | An event about a Pod that could not be resolved (`lookup`) or whose name now belongs to a different UID (`uid_mismatch`). The events still ship under the identity they carry — correlation with that pod's logs is what is lost. |
| `kubescrape_self_metadata_resolved` == 0 | This process could not find its own pod, so its self-metrics carry no pod attributes. On a hostNetwork agent the `/v1/self` lookup legitimately misses and falls back to a by-name lookup — see the note under [What NOT to alert on](#what-not-to-alert-on). |
| `kubescrape_metadata_requests_total{outcome="not_found"}` sustained | The agent is asking about containers the service does not have. Check the service's `kubescrape_store_containers` and `kubescrape_apiserver_reachable`. |

## What to alert on

Two classes, and the distinction is the whole point: **data loss** pages,
**degradation** does not.

### Data loss — any nonzero rate is worth acting on

| Alert | Why this threshold |
|---|---|
| `rate(kubescrape_log_permanent_dropped_total[5m]) > 0` | The tailer's data-loss signal. A permanently-rejected batch is dropped and its offsets advance, because rebuilding it forever would stop ALL log shipping on that node and lose the backlog anyway when the file rotates. It is the one loss path the tailer has, and it is deliberate. Note: with `-buffer-dir` the tailer sees the ENQUEUE verdict instead, so this moves only for a batch larger than the whole spool cap — the buffered chain's loss lands on the next row. |
| `rate(kubescrape_buffer_dropped_records_total[5m]) > 0` | The spool gave up on a payload: a permanent rejection, or a poison batch dropped after repeated accountable failures. `kubescrape_buffer_dropped_batches_total` says a loss happened; this one says how big it was. |
| `rate(kubescrape_export_requests_total{outcome="permanent"}[5m]) > 0` | The collector refused a payload and no retry can help. On the unbuffered chain this is loss; on the buffered one it is what the previous row is about to count. |
| `rate(kubescrape_log_prefix_lost_total[5m]) > 0` | A rotated log segment became unrecoverable — the rotated file was pruned before its owed bytes were read, or a replay stalled past its budget. Silent otherwise. |
| `rate(kubescrape_tail_sampling_spans_total{outcome="lost"}[5m]) > 0` | A decided KEEP could not be delivered and was dropped. With `-buffer-dir` and a traces spool this should be flat: a decided keep is marked owned and spooled. |
| `rate(kubescrape_log_unresolved_lost_total[5m]) > 0` | A log file was deleted before its metadata ever resolved. Nothing was read and nothing can be. |
| `rate(kubescrape_positions_save_errors_total[5m]) > 0` | Offsets are not being persisted, so the next restart re-reads or skips. Not loss yet; it is the condition under which the next restart becomes loss. |

### Degradation — alert on it SUSTAINED, with a window

| Alert | Window | Why |
|---|---|---|
| `kubescrape_readiness_gate == 0` | 5m (longer than your pod startup budget) | A gate stuck at 0 across the fleet IS a stalled rolling update, and the label says which subsystem. This is the alert that works when nobody can curl the probe. |
| `kubescrape_apiserver_reachable == 0` | 5m | A NEW connection is not reaching the API server. It does not prove the caches stopped advancing (a blackholed established watch probes healthy), which is why it is sustained-only. Do NOT try to alert on the store gauges instead: the sweeper keeps expiring tombstones, so `kubescrape_store_pods` DECAYS during an outage rather than freezing. |
| `rate(kubescrape_export_requests_total{outcome="transient"}[10m])` high with `ok` at 0 | 10m | The destination is down. Nothing is lost yet — that is what the next row is for. |
| `kubescrape_buffer_backlog_bytes / kubescrape_buffer_max_bytes > 0.8` | 15m | The only buffer signal that fires BEFORE anything is refused or dropped; every other one is a counter that moves after the fact. |
| `rate(kubescrape_buffer_enqueue_errors_total[5m]) > 0` with `kubescrape_buffer_full_total` flat | 5m | The DISK, not the collector: ENOSPC, a latched fsync failure, a read-only remount. A full spool and an unwritable spool are opposite problems. |
| `rate(kubescrape_bearer_token_read_errors_total[5m]) > 0` | one rotation interval | The ONLY signal for a broken Secret projection: both halves of the rotation keep working on their last good value by design. `role="receiver"` has no local symptom at all — it surfaces minutes later as a fleet-wide 401 on the clients. |
| `rate(kubescrape_scrape_auth_failures_total{reason="upstream"}[5m]) > 0` | 5m | The service cannot read the Secrets monitors reference. Every one of these becomes `up=0` on a target whose agent sees only a status code. |
| `rate(kubescrape_owner_resolve_failures_total{reason=~"lister_error\|no_informer\|wrong_type"}[5m]) > 0` | 5m | The service cannot read owner metadata, and NOTHING ELSE SAYS SO: a failed read still answers with the bare owner reference, so the response is well-formed while `service.name` falls back to the pod name and half the Prometheus job of every series that workload exports changes. These three reasons are a broken cache or a wiring bug (usually a missing ClusterRole rule) and each carries a throttled Warn naming the object. Exclude `not_found`, which is normal at a low rate — alert on THAT one only if it is sustained and fleet-wide. |
| `rate(kubescrape_container_lookup_timeouts_total[5m])` sustained | 15m | This replica's pod informer is not seeing the pods whose logs the agents ship. A low rate is normal (the ~1s kubelet gap). |
| `rate(kubescrape_index_name_reuse_total[5m])` sustained | 15m | Watches keep breaking. The guard keeps served data correct, but it is also the condition under which OTHER missed deletes — the ones no name index can catch — leave a deleted pod served as a live target. Read beside `kubescrape_informer_watch_errors_total`. |
| `kubescrape_transform_reloads_total{outcome="failed"}` increasing | any | A broken transforms edit. The last good program keeps running, so nothing breaks — but the file on disk and the program in memory have diverged, and `/debug/transforms` shows which nodes are on which. |
| `rate(kubescrape_ingest_rejected_total[5m])` sustained | 15m | The node cannot keep up with what is pushed at it. There is no single knob: the throttled line carries `reason=` with one of `in_flight`, `buffer_bytes` or `decoded_bytes`, and names the flag. |
| `rate(kubescrape_debug_refused_total{reason=~"no_token\|unauthenticated"}[5m])` sustained | 15m | Somebody is being refused the node's telemetry stream from off-pod. One burst is usually an operator whose `curl` skipped the port-forward; a steady rate, or one from pods that are not yours, is somebody reading for the log lines. The throttled Warn names the peer. `host` and `forwarded` are the same signal seen from a browser or through a relay. |

## What NOT to alert on

These move for expected reasons, and a naive alert on them teaches people to
ignore the alerts that matter:

* **`kubescrape_self_lookups_refused_total{reason="no_pod"}`** — permanently
  nonzero on a hostNetwork fleet (the agent shares the node's address, so it
  owns no pod IP) and on anything behind SNAT. Those fall back to a by-name
  lookup and are fine. The pair worth watching is this rate *together with*
  `kubescrape_self_metadata_resolved == 0`; either alone is normal.
* **`kubescrape_pod_ip_contested_total`** — a trickle is ordinary on a churning
  cluster. It counts a window in which the answer *could* have been wrong, not
  an answer that was.
* **`kubescrape_cadvisor_unresolved_total` at a low rate** — a container that
  started seconds ago has no id in the API server yet, and resolves on the next
  cycle. Alert on a sustained or fleet-wide rate instead.
* **`kubescrape_log_files_skipped_total`** — this is configuration WORKING. It
  counts every file your namespace filters, `ignoreOlder` cutoffs and exclude
  globs deliberately declined. Read it when logs are missing; do not page on it.
* **`kubescrape_log_rules_dropped_total`, `kubescrape_transform_dropped_total`,
  `kubescrape_trace_spans_dropped_total`, `kubescrape_log_scrubbed_total`** —
  all of these count the operator's own policy being applied. A *change* in the
  rate is interesting; a nonzero value is the feature.
* **`kubescrape_scrape_failures_total{reason="canceled"}`** — shutdown.
* **`kubescrape_journal_entry_defects_total`** — a repair, not a loss: the
  record still ships, with a replacement character or the agent's clock. Worth a
  dashboard panel, not a page.
* **`kubescrape_export_requests_total{outcome="error"}`** — this label value no
  longer exists. It was split into `transient` and `permanent`, so an alert
  copied from an older release matches nothing, silently. This is the one
  "non-alertable" entry that is a *mistake* rather than a judgement call: fix it
  to `outcome=~"transient|permanent"`, or better, to `outcome="permanent"`.
* **Anything derived from a metric that is ABSENT rather than 0.** Several
  families here are registered exactly where the thing they measure exists —
  `kubescrape_readiness_gate`, `kubescrape_cgroup_containers`,
  `kubescrape_monitors_rejected`, `kubescrape_scrape_targets`. That is
  deliberate: a published 0 always means "on and finding nothing", never "off".
  An alert written as `metric == 0` therefore says something real, and one
  written as `absent(metric)` says the pipeline is not enabled. Do not confuse
  them.

---

Next: [CONFIGURATION.md](CONFIGURATION.md) for every flag and config section in
narrative form, [FLAGS.md](FLAGS.md) for the generated per-binary inventory,
[METRICS.md](METRICS.md) for every metric with its labels and help text, and
[COMPARISON.md](COMPARISON.md) for how this compares to Alloy, Vector, Fluent
Bit, the OTel Collector and the Beats.
