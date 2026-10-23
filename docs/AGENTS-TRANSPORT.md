# OTLP transport, processing and admission

[Repository guidance](../AGENTS.md) · [Trace tier](AGENTS-TRACES.md)

## Export and payload ownership

`internal/agent/otlpexport` supports gRPC and OTLP/HTTP. Every wire attempt
passes through `Client.noteSend` for outcome counters and diagnostics.
Transient destination outages have a transition/repeat/recovery narrative;
permanent payload rejection has a separate throttled line. Do not merge those
states. Metrics have bounded client retries; log producers own their retries.
All backoff waits, including the initial delay, stop at `backoff.Cap`.

Measure exact protobuf size before sending. `pkg/otlpsplit` preserves
resource/scope grouping, splits metrics down to points for all five types,
and sends an oversized indivisible item alone. Nonempty input must yield
nonempty output. Stop on a failed part so the caller can retry the whole
payload at least once. Bound framing amplification through `splitPaysOff`;
when splitting cannot make useful room, send the remainder as one over-cap
part and report its reason.

Keep the klauspost gRPC gzip codec as the sole codec registered under gzip.
Pooled readers return exactly once on the first terminal read or Close;
reset released readers to an empty source. Writers reset to nil before pooling
so sinks are not retained. One-slot warm caches supplement sync.Pool without
retaining unbounded compressors. Pool-identity tests skip under the race
detector. HTTP gzip bodies use a no-op Close wrapper: only `httpPost` releases
their buffer after EOF, otherwise GC owns it. Never set `req.GetBody`.

`export` provides per-signal destinations over flag defaults. A destination
naming a different endpoint does not inherit the base collector's bearer,
CA or skip-verify credentials. Share that partition through
`Config.TransportOnly`/`OwnEndpoint` with routing. Explicit export-section
headers and client-certificate additions still apply to its own destinations;
the client pair never applies to a route. Transport plaintext preference may
inherit. A repeated base endpoint retains its credentials. Report dropped
credential settings at startup and dry run.

Refuse static OTLP framing headers and Authorization alongside a bearer file.
Standalone Authorization is allowed. BuildExporter returns a PerSignal mux;
Validate checks shape without reading credentials. OAuth2/SigV4 are outside
this exporter.

## Disk buffering and shutdown

`otlpexport.Buffered` uses one `diskqueue.Queue[[]byte]` per signal. Configure
MaxBytes as the capacity knob with MaxSegments disabled and bounded open
files. Reserve → send → commit only after ACK. Reserved bytes alias the reader
buffer: hash before any queue operation. Rewind/requeue a transient head and
recover latched I/O failures with guarded close/reopen of the same handle.
Count record-too-large separately from I/O, closed, ENOSPC and other enqueue
failures. Publish occupancy before data loss occurs.

Authentication failures and HTTP 404 remain transient, including grpc-go's
synthetic Unimplemented from a proxy's HTTP 404. Permanent rejection is an
explicit drop. Poison tracking is bounded by payload hash and resets on
restart. A failed cycle is accountable only when another batch succeeded
since that payload's preceding failed cycle **and** the final error is a
nonretryable collector response. Re-establish both facts every lap; a total
outage or transport failure must not spend the poison budget. Use the shared
RetryableStatus classification, including RetryInfo on ResourceExhausted.

Forwarded traces pass through because their SDK still owns retry. Only
`otlpexport.Own(ctx)` payloads, such as decided tail-sampling keeps, may spool.
The marker belongs to the call, not permanent exporter wiring. Open the trace
queue only when tail sampling is enabled on the tier. Capacity can exceed its
byte cap by one segment; keep that documented.

Shutdown flushes are detached and bounded with
`context.WithTimeout(context.WithoutCancel(ctx), ...)`, preserving context
markers. FinalExport uses its caller's context. Both mains use
`cli.ShutdownContext` and `cli.WaitFor`; cancellation must not remove signal
handling before the drain finishes. Pass lifetimes into pipeline start methods
rather than storing a context in `pipelines`. Stop receivers and flush tail
decisions before closing exporters.

## Routing and transforms

The export chain is producers → transforms → debug tap → namespace router →
default buffered chain or route clients. Self-telemetry bypasses routing and
the tap. Routes use first-match namespace globs with memoized decisions;
single-destination payloads pass without copying. Route clients are unbuffered.
A destination failure fails the export so its owner retries deterministically.

Producers that re-offer records use `route.Reoffer(ctx)`: send route shares
first and withhold the default buffered share on transient route failure.
Single-shot producers still try every share. The roster is `route/reoffer.go`.
Mixed failures return an opaque `partialFailure` with an Errors accessor and
flattened text; do not implement Unwrap, which would misclassify the aggregate.

Starlark transforms run once per batch above buffering, so queued bytes are
final. A runtime error fails export; an empty result ACKs without a send.
Keep execution hermetic, step-limited, wall-clock-bounded and predictively
size-bounded. Bound rendering and regex result size before allocating them;
check builtin entry deadlines. Regex compilation is bounded by length,
captures and `regexcost.Insts`, and caches by entries, bytes and instructions.
Compile misses outside the lock; cache hits stay lock-free.

Unmarked payloads are copied before script mutation because their producer may
retry the same object. `transform.Handoff` allows in-place work only when the
producer rebuilds on retry. `Consumed` permits settled-drop counting on a
failed single-shot send. Preserve the marker roster tests. The tailer applies
its transform once to the new batch and exports through Wrapper.Inner; count
drops at commit, not rewind. Removing all metric points also removes the shell.

Reload uses directory watching plus poll fallback, compile-then-commit through
an atomic pointer, and last-good behavior for broken edits. Report each distinct
broken file once and expose failing state/active hash. Reload waits must observe
cancellation. Shared forks see the same program and metric emitter.

Hooks are fail-open and count errors: ingest admission runs after receipt-time
stripping but before enrichment; target hooks copy cached targets before edits;
sample hooks implement tier-only tail policies; parse hooks run only on opted-in
plain sources before the log chain. Cross-check required sample hooks at compile
time. Per-line container hooks, per-sample scrape hooks, cross-batch mutable state
and I/O/clock builtins are deliberately unsupported.

`route()` and drop markers are internal plumbing. Strip **every occurrence**
of wire-supplied markers at application receipt using `removeAll`; the
authenticated internal trace hop is exempt. `emit_metric` writes only declared
dynamic series, with label/resource-width bounds.

## Receiver attribution and log processing

The per-node OTLP receiver accepts logs/metrics; the trace tier accepts traces.
Disabled signals return Unimplemented/404, never success. Ready fires only
after every enabled listener binds. Forward received payloads without
agent-side batching.

Enrichment resolves configured container-ID or pod-UID keys. Preserve the
sender's service triple and lookup keys, strip other self-asserted Kubernetes
identity at application receipt, and let resolved identity overwrite remaining
claims. Use `attrs.ReservedIdentityKeys`, `senderControlledIdentity` and
`resolvedWins` consistently. Keep identity stripping separate from the warned
plumbing-marker strip. Unauthenticated lookup-ID impersonation remains an
[accepted residual](CONFIGURATION.md#accepted-security-residuals); metadata
enrichment is not authentication.

Peer-IP fallback is valid only at first receipt and uses canonical live-pod
lookups. The trace tier rejects its own workload's peers. In data-point split
mode, remove sender identity and apply the described object's resolved identity;
kind-tag lookup tokens so a container and its pod can represent the same object.
Use EnrichMetrics' returned payload because points move. Bound split groups and
fold over-budget remainder into one identity-stripped overflow resource.

Pushed logs use the shared scrub/enrich/lift/metrics/rule primitives. Preserve
sender grouping: write log-target lifts in place; resource lifts can resolve
for rules/metrics but do not regroup records, and scope lifts are not written.
Metrics observe before rules; all-dropped pushes ACK. With no positional proof,
log metrics and scrubbing count per receive attempt while rule drops count
settled delivery. Preserve severity-number fallback and bounded text rendering
of structured values. Oversized/wide inputs skip counted optional work while
forwarding data and running rules. Deduplicate resource keys last-wins before
rule/metric enrichment. Drop and count empty metrics at receipt after admission.

## Independent receive budgets

Preserve the independent limits and their placement:

- In-flight processing refuses without waiting, with retryable HTTP 429 plus
  Retry-After or gRPC ResourceExhausted **with RetryInfo**. Keep gRPC concurrent
  streams bounded too.
- Raw buffering has a separate byte budget. Charge HTTP as bytes are read,
  pre-size from a bounded head rather than Content-Length, and acquire the
  processing slot after reading. Reserve gRPC receive size in the pre-decode
  tap, releasing exactly once on interceptor, stream completion or bounded
  decode-window expiry.
- Decoded-structure estimates are charged from wire bytes before decoding.
  Wire size alone does not bound pdata object amplification. Wire the codec,
  admission tap, interceptor and claim reaper as one unit so early failures
  release their reservations.
- Check nesting before recursive decoding, on HTTP and every gRPC listener.
  Excess depth is a permanent rejection. Bound decoded header lists on both
  transports before handlers/auth taps run.
- Bound attribution by total elapsed lookup time per push as well as count.

Use `BodyReader`/`WriteBodyError`/`WriteProto` and shared forward-status helpers
on every receiver. The body cap is a parameter; internal trace hops allow at
least MaxPushBytes because an oversized single span can travel alone. Keep
refusal reasons distinct, warnings throttled, and suggested remedies consistent
with which setting actually controls the active budget.

## Debug access

`debugMux` is the registration source. Gate `/debug/otlp`, its UI and
`/debug/tailer` with either a valid rotating bearer or a local connection.
The local arm rejects forwarding headers by **presence** using
`peerip.ForwardingHeader` and requires a loopback literal/localhost/empty Host
to prevent DNS rebinding. A valid token bypasses those local-only checks.
Keep health/readiness and nonsensitive target/transform state available.

The tap streams OTLP JSON with compiled scalar-only attribute globs, sample
percentage and signal selection. Bound filter count and bytes, subscriber
count, and queue **bytes**. Slow readers drop observations without blocking
delivery. Subscribe before sending headers; clear HTTP write timeout for
streaming. With no subscribers the fast path is one atomic load.
