# Logs and log-derived metrics

[Repository guidance](../AGENTS.md) · [Optional log producers](AGENTS-OPTIONAL.md)

## Shared record processing

`internal/agent/logchain` owns both the processing order and key resolver:
scrub → lift attributes → stamp → enrich → log metrics → keep/drop rules.
`Chain.Line` does the pre-grouping scrub/extract work; `Chain.Emit` handles the
remaining steps and returns whether a record survived. A dropped record still
advances its source position. `Producer.Dest` is needed only for a kept record;
other producers may group first and call `logchain.Prune` afterwards.

Resolve record attributes before resource attributes, except
`attrs.ReservedIdentity`, which must resolve from the resource first. Synthetic
`__severity__` belongs to rules only. Use `LowerSeverity` and `RecordSeverity`
instead of separate spelling/number mappings.

Only a producer with positional proof may set `Input.Observed`. It suppresses
every record-counting observation, including enrichment, scrubbing, metrics
and rule drops. The comment on `Input.Observed` and `Config.CountsRecords`
define that set. Tailer byte ranges and event observations can provide proof;
ingest requests cannot. Underclaiming can recount; overclaiming loses data
silently. Preserve allocation-pinned producer interfaces and bind caches.

`logenrich.Apply` lets parsed values replace producer defaults. Ingest uses
`ApplyBodyText` because explicit sender fields remain authoritative. A parsed
timestamp replaces a producer timestamp only with its own timezone; without
a producer timestamp, a zone-less value may be used. IDs must be valid,
nonzero trace/span IDs. Keep pattern-derived stack traces in the body without
duplicating them as `exception.stacktrace`; JSON-carried traces may remain.
Enrichment does not modify the body.

## Tailer delivery and positions

The tailer has one sweep goroutine; exports and retries are synchronous.
Metadata resolves before consuming container data, with bounded, jittered
backoff. Count unresolved loss and exhausted metadata budgets separately.
fsnotify drives discovery with polling fallback; retain the resolved target
directory even if adding a watch fails.

CRI parsing rejoins partial fragments before application multiline joining.
Stage emission inside Add/Flush is synchronous. `Entry.Lines` accounts for
every consumed line and pops offset-range FIFOs; unmatched emissions are
dropped with a counter and throttled warning. Bound never-ending groups by
FIFO size and CRI partial runs by consumed bytes.

Offsets commit only after successful export and never beyond a buffered
stream's watermark. A transient failure rewinds to committed positions and
discards un-emitted pipeline state. Permanent rejection drops the batch,
advances positions through `advanceBatch`, and reports loss. With disk
buffering the export verdict is enqueue success; collector-side losses are
reported by the buffer.

The positions store is the sole persistence layer: one atomic, mutex-protected
JSON document holds file positions and the journal cursor. Consult saved
positions on every scan until applied or excluded by a successful listing.
Only successful discovery may prune checkpoints or finish a source's startup.
For files without a checkpoint, `-logs-unknown-files=auto` starts at the end
when the store is empty and at the beginning when it has entries; a corrupt
store causes replay. Persist discoveries and segment records promptly.

## Rotation, retries and observation

- File identity is inode plus head fingerprint. Verify before reads after
  modification, extend fingerprints only while their existing prefix matches,
  and treat truncation/rewrite as a new incarnation.
- Rename rotation drains the old file and immediately opens its replacement.
  Keep the multiline pipeline across observed renames. Truncation resets it.
  Apply the gone-file grace before finalizing a deletion; final shutdown skips
  that wait. Only observed rotations can be joined.
- Every offset is `pos{seg, off}`. A segment's end is the last fed line boundary,
  never merely the read position. Commit against the entry's ending segment;
  retire a segment only after its range commits or its loss is reported.
- Incomplete drains create open-ended segments and resume within sweep budgets.
  A drain cap limits one call, not total recoverable data. Do not mark a gone
  drain complete while work remains. Live-tail reads wait while replay is owed.
- Checkpoint incomplete segments and discard progress. Reconstruct them on
  restart through inode/fingerprint matching; count and retire missing prefixes.
  Bound retained descriptors and stall retries. Discarding pipeline rebuilds
  clear their feed/discard state; a completed rename does not.
- Idle descriptors close only when fed data has committed. A reopened file
  must be re-identified, including a rotation while its descriptor was closed.
- Remember exported high positions when another stream clamps the watermark,
  but discard those highs on rewind or a new incarnation. They must never
  commit bytes from a replacement that has not been exported.
- `file.observed` keys observation proof by segment-qualified range **and body
  hash**, within a cap. Record proof after export outcome. Retries may repeat
  delivery but must not recount log-derived observations with valid proof.

Per-file rate limits pause or explicitly drop. Rotation drains bypass them;
blank lines and oversized-line terminators spend no record token. Every restart
path clears pause/discard state correctly. Report torn final lines and
oversized discarded entries. Tailer status includes lag and unresolved files;
the sweep counter is its heartbeat because gauges freeze if the loop stalls.

Keep the line path allocation-free: parse CRI headers once, cache stream keys
and pointers, advance FIFO heads instead of reslicing, and reuse pending carry
buffers. Release oversized idle buffers. `TestPrefilterEnabled`,
`TestIngestLineAllocationBudget`, `TestIngestFlushAllocationBudget` and
`TestPendingBufferIsReusedAcrossChunks` protect these contracts.

## Sources, annotations and scrubbing

Sources use ordered doublestar include/exclude globs. Container sources derive
identity from CRI filenames; plain sources use node/static/path attributes and
their source name. Namespace exclusions are prohibitions: a later catch-all
must not reclaim an excluded file. An allowlist miss may route to a later
source. Reject nonregular files; never ignore an already-tracked/checkpointed
file merely because it is old. Failed discovery keeps the previous verdicts.

Refresh a file's rendered resource when node attributes change. Per-pod
`kubescrape.io/logs` config is parsed at resolution: explicit exclusion stops
reading; workload service/attributes apply after the builder; workload rules
run before global rules. A workload keep still passes global rules. Malformed
or omitted annotation values warn and are ignored, rather than losing logs.

Compressed source offsets are in decompressed bytes. Keep the archive open
across EOF and retries until every fed record commits, including an unfinished
final fragment. Detect replacement by fingerprint; quarantine non-gzip inputs
and count lost archives. Plain files skip CRI parsing but share rotation and
offset machinery; blank lines are discarded.

`logscrub.defaultSet` is authoritative; its documented enumeration is in
[AGENTS.md](../AGENTS.md). Scrub bodies before any lifting/enrichment/metrics
can copy sensitive values. Unknown pattern names and invalid regexes fail
startup. A builtin prefilter must admit **every** regex match and stay linear
on hostile large inputs; preserve the prefilter equivalence/fuzz tests and
zero-allocation no-match budget.

Credential parsing decodes one level of escaping, distinguishes plain from
escaped opening quotes, and redacts URL userinfo through the last `@` in a
token. Keep whole-quoted-credential, known-partial and accepted-overredaction
tests. Scrubbing is per record; PEM fragments split across records cannot be
fully recognized as a key.

`pkg/logattrs` scans JSON once with `GetPaths` and logfmt with the shared reader.
Unescape only quoted values through `LogfmtValueView`; `key= value` represents
an empty value. Resource/scope lifts participate in grouping; log lifts stay
on records. Preserve numeric representation: int64 integer tokens, whole
floats as integers only within ±2^53, otherwise doubles; booleans stay typed.

## Log-derived metrics

`internal/metrics.Registry` is unbounded, nonexpiring self-telemetry with
code-defined labels. `DynamicMetricSet` is capped, expiring telemetry with
data-defined labels. They share storage primitives, not global configuration
or refusal counters. Register dynamic-drop metrics only when that set exists.

`internal/logline` owns selectors, filters, `KeyIndex`/`Fields` extraction and
the synthetic line key. Bind a resource once per flush and observe per record.
Keep the gauge action set closed; derivable statistics belong in backend
recording rules. Both observation doors use `series.refuse`: reject NaN/Inf on
all kinds and negatives on monotonic counters/summaries, with distinct reasons.

Emit unexported observations before idle handling or expiry. Only gauges zero
while idle. Failed exports retain removed final samples and rearm live series.
Clamp cumulative start times to construction. Histograms are one sample per
label set; reject caller-supplied `le` at compile, direct-emission and Registry
doors. Clone bucket counts on snapshot/Dump and enforce cardinality × bucket
bounds at configuration time.

Hash the same identity that is rendered: drop empty values, apply label
truncation, and resolve duplicate keys last-wins. XOR folding requires a
uniqueness proof; fall back to materializing identity when it cannot be proved.
Use separate resource and label domains, full 128-bit comparisons, and chains
for low-half bucket collisions. `series.count`, not map length, is cardinality.
`TestResourceIdentityIsWhatItRenders` and its fuzz test define equivalence.
Preserve pooled contexts, memoized selectors, single-pass extraction and the
dynamic-metric allocation budgets. Rendering remains OTLP-only.
