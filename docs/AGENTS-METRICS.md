# Metrics collection

[Repository guidance](../AGENTS.md) · [Resource attributes](AGENTS-SHARED.md#configuration-and-attributes)

## Prometheus scraping and parsing

`internal/agent/promscrape` uses the streaming parser in `pkg/promparse` for
classic text and OpenMetrics. Preserve constant-memory parsing; do not replace
it with a parser that buffers whole families. Drop and count malformed samples
individually, including duplicate label names. Parse mode follows Content-Type;
exemplar export is opt-in.

`convert.go` holds histogram/summary state only for the current family. Bound
HELP/UNIT tracking by count and bytes, preserve their values, and charge
descriptors against batch size. Export chunks stop at the point **or byte**
bound; splitters can duplicate resource framing for each described object.
Recheck size within a flushing family. Parse failure may still export converted
cumulative samples, but no work may outlive the scrape budget.

Keep per-sample paths allocation-disciplined: capped name/value interning,
last-seen caches, pooled parsers/accumulators and memoized filter sessions.
Do not intern exemplar values. Avoid closures and formatting in the sample
loop. Parser, exemplar, filter and accumulator allocation-budget tests are
required checks for changes in these paths.

Native histograms are negotiated only under `-scrape-native-histograms`.
Reject unsolicited protobuf when disabled. Bound each delimited MetricFamily
both on the wire and by estimated decoded size; enforce time and metadata
budgets while parsing. Convert schema to scale and spans/deltas to bounded
dense exponential buckets. Refuse NHCB schema -53 while the protobuf model
lacks its custom bounds. Apply the same exemplar opt-in and label-size limits
on both wire formats. Quoted UTF-8 metric names use cold parser branches rather
than disrupting the common last-seen caches.

## Filters, splitting and target configuration

Metric filters are ordered first-match-wins rules applied before conversion.
`filterPipelineNames` is authoritative. Filtering can leave partial histogram
families, as with Prometheus relabeling. Monitor relabeling and target merging
follow [Metadata service](AGENTS-METADATA.md#monitors-and-credentials).

Splitters regroup KSM-style targets by configured identity labels, with
optional metadata enrichment. They require `Kubelet.Meta` even when kubelet
scrapes are disabled; the shared pod cache is mutex-protected for concurrent
scrapers. Coalesce multiple groupBy labels mapping to one attribute in sorted
label order, with nonempty values winning. Routing, lookup identity and final
attributes must read that same coalesced vector.

Split rules can drop labels, fill absent resource attributes and set an
instance prefix. `datapointAttributes` moves selected attributes from resource
to points; its default is node name and an empty list keeps everything on the
resource. All final resources use the common attribute builder.

Every target is scheduled. Only an explicit usable interval may shorten the
ticker; invalid intervals warn once and fall back. Clamp timeouts to the
target's own interval. Cache per-target TLS clients by credential material
within `maxTLSClients`, not by an unbounded stream of URLs.

## Kubelet and summary identity

Kubelet scrapes use the mounted ServiceAccount token through `internal/bearer`
and RBAC for `nodes/metrics` or `nodes/stats`. Normalize endpoints once in
`kubeletBase`; reconstruct IP literals with `net.JoinHostPort`, reading a bare
IPv6 authority as host:port first. Keep timeout within the interval and join
all scrapes started by a cycle.

cadvisor resources derive from `pkg/cgroupid` paths. Resolve vouched container
IDs through the container endpoint; cross-check pod identities by UID.
Sandbox rows, including unlabelled containerd pause scopes, fold into the pod
only with the row's own namespace/pod evidence. Never look up a pause ID, clear
its image, and retain its cgroup ID on points. A path containing a supervised
container ID is not proof that the row describes that container: preserve
`lookupContainerID`/`pathVouched` checks for conmon and similar processes.

An unattributable row keeps its own path resource and is counted; it cannot
name or merge into a pod resource. Remove redundant identity labels only from
positively identified rows. Rollup filtering drops above-pod and
container-family pod aggregates, retaining the `podScopedFamily` keep-list.
Related resources use the shared `fillIdentityResource`/`FillContainerResource`.

Metadata lookups consume at most half the scrape's elapsed-time budget so
conversion and export retain time during metadata outages. Count budget
exhaustion once per scrape, including objects never looked up. Health metrics
use their own bounded export context.

`/stats/summary` uses fixed-path JSON extraction. Missing fields remain absent,
not zero. Export gauges with each stat's timestamp; inode units are `{inode}`.
For static pods, accept a by-name mirror only when config.hash or config.mirror
matches the reported UID, and verify container incarnation. Otherwise keep it
unresolved and count it. Summary node identity uses the summary builder;
pod/container/volume identity uses the cadvisor builder for consistent joins.

## High-frequency cgroup sampling

`internal/agent/cgroupstats` samples cgroup-v2 CPU and memory directly and
exports window distributions. Use Welford population standard deviation,
CPU rates in cores, and working-set bytes. The sample count describes the
current window; sparse windows may restate previous values at most twice per
signal. A vanished container flushes its final window before retirement.

Rebuild identity on every export through `Scraper.FillContainerResource` so
it matches cadvisor after label changes. Never export unresolved containers or
sandbox resources. The cadvisor attribute pipeline governs these resources.
This aggregation belongs on the node because the high-frequency readings do
not reach the backend.

Mount the node hierarchy beside the container's own `/sys/fs/cgroup`, never
over it. A genuine cgroup-v1 hierarchy disables only this pipeline with an
error; missing/invalid hierarchy configuration remains fatal. An enabled
sampler finding no pod cgroups warns periodically and publishes zero
containers; absence of that gauge means disabled. Keep chart flag, root mount
and manifest examples aligned and preserve the allocation-free sweep budget.
