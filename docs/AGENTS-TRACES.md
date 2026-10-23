# Trace tier

[Repository guidance](../AGENTS.md) · [Transport and admission](AGENTS-TRANSPORT.md)

## Deployment, ownership and forwarding

Traces enter the dedicated `-service-graph` tier, not the per-node receiver.
Pairing and sampling require all spans of a trace on the same owner. Deploy a
StatefulSet with ordinal DNS, a headless internal Service with
publishNotReadyAddresses, and the application-facing `<release>-traces`
ClusterIP. Disable per-node pipelines, including `-ingest`. No Kubernetes API
RBAC or volumeClaimTemplate is needed for the in-memory tier.

`serviceGraph.replicas` controls both StatefulSet size and shard configuration;
scale through Helm, not `kubectl scale`. Empty `serviceGraph.ingest.allowFrom`
permits any pod to push. Application listeners are unauthenticated; the
internal hop is bearer-authenticated and receives already-enriched data.

Enrich only at first receipt. Reject peer-IP fallback to the tier's own
workload. The internal path has no enricher or resharder, keeping forwarding
acyclic; the application path refuses a `ForwardedMarker`, and the owner strips
it. Apply shared nesting/header/body guards on the internal listener too;
oversized gzip returns 413, not a permanent 400.

`Resharder` uses Tempo's FNV-1 ring keyed by shard name. A single-owner payload
passes without copying; spans without trace IDs stay local. Forward shares
concurrently, synchronously and with one attempt: a failed hop fails the
application push so its sender can retry. Do not queue or spool this hop.
Try a known failing shard first to limit duplicate accepted shares on retry;
never reassign a dead shard's arc.

The owner chain is:

```text
servicegraph tap → spanmetrics tap → head sampling → tail buffer → collector
```

Taps forward first and observe only success. They run only on the owning
shard and precede samplers, so sampling does not reduce request counts.

## Shared cumulative aggregation

`internal/agent/cumagg.Store[S]` owns admission, cardinality, locking, expiry,
render/send/delivery order and the observed → rendered → delivered state.
Evict stale series only after an acknowledged export carried their current
values. Recreated series get fresh start timestamps; `ClampStart` prevents
start times later than the render timestamp.

Use `ParseStaleAfter`/`ResolveStaleAfter`: empty means default, zero disables
expiry and negative values are errors. YAML duration fields are strings.
`Builtins` handles label collisions by dropping colliding custom dimensions.
Keep each aggregator's metric names, dimensions and refusal counters local.

Snapshot, eviction and delivery-marking walks release the series lock in
chunks. Build pdata outside the lock and take `renderMu` before the store
mutex. Mark only the series included in that render; clear exemplars only
after delivery. Preserve nonstalling render/mark tests and warm-path allocation
budgets. Retained labels use `cumagg.Retain`, which clips **and copies**, rather
than retaining an oversized sender buffer by substring.

## Span metrics and service graph

Span metrics use OTel connector names and proto enum spellings for kind/status.
Each span contributes independently to calls, size and duration, with optional
dimensions and bucket exemplars. The described service is a point dimension;
the exporting tier supplies the resource. Exemplar eligibility follows the
head sampler and requires a trace ID. Cardinality caps reject new tuples while
existing ones continue.

Service-graph metric names and two latency histograms remain Tempo-compatible.
Emit failed-request counters at zero before the first failure. Edge dimensions
are canonically ordered and borrowed; a retaining sink must copy them.
Pairing-store and series bounds count every refusal/expiry/eviction.

Scale expiry work with batch size, within the total sweep budget and per-lock
hold ceiling. Reject client/producer spans with zero span IDs. Resources with
no service name do not produce edges, but their spans still forward and
spanmetrics can count them. Database classification applies on the client
side and overrides messaging classification. Keep semantic-convention and
Tempo database/peer attribute support at their shared definitions.

Edges normally need both halves; missing peers can form virtual nodes from
configured peer/database attributes. Do not invent a literal user node.
Dimensions resolve span before resource. For HTTP shard transport, any TLS
configuration requires an HTTPS URL. Refuse a tier with both application
listeners disabled.

## Head and tail sampling

`tracehash` provides unsalted per-trace probability decisions, threshold
conversion and rate buckets. Head and tail probabilities nest instead of
compounding, and remain deterministic across retries, restarts and shards.
Head-sampling guardrails keep errors/slow spans; apply the spans/second cap
last. A fully sampled-out payload ACKs without sending.

`tailsample` is the policy engine; `tailbuffer` owns buffered traces and calls
`Evaluator.Decide`. The engine holds no traces and imports no `obs`.

**Undecided buffered spans have already been acknowledged to senders and are
lost on SIGKILL, OOM or node failure.** Keep this visible in operator guidance
and buffered-span/trace gauges. Graceful shutdown stops receivers, calls the
latching `Flush`, then closes the exporter. Late arrivals after the latch are
decided under their own unowned acknowledgement.

Size `maxSpans` against the real cgroup limit, with MemTotal fallback: use the
configured per-span estimate and quarter-memory budget, lower an unset default
without crossing `maxSpansPerTrace`, warn above the share and refuse a ceiling
requiring the entire limit. Keep environment sizing out of shape-only Validate.

Bounds decide early rather than drop blindly: per-trace span limits decide
that trace; global trace/span caps decide the oldest. Partial input is a lower
bound on the trace, and early decisions are counted. All-zero trace IDs are
never buffered or cached.

Late spans follow the cached keep/drop verdict. Verdict TTL and rate-budget
accounting have different lifetimes: retain the `ChargedMask` to the cache-size
bound so re-decisions peek paid buckets and charge only unpaid ones. Count
eviction of live verdicts. Decide in bounded chunks with no export under the
mutex; keep loss and failed-forward warning throttles separate.

Tail sampling is last, after both metric taps and head sampling. Emit the
configuration warning when per-span head guardrails can fragment a trace
before tail decisions. Supported whole-trace composition uses probability
and rate limits.

Decided keeps leave via `sendOwned`, carrying `otlpexport.Own(ctx)` so an
enabled disk buffer spools them. Without a spool, retry within the send
context and report final loss. A failed forward of an already-decided span
still held by its sender must fail that push. Preserve allocation-free late
drop/decision paths and initialize policy-label values at construction.
