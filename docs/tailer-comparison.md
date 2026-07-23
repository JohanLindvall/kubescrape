# The tailer vs. Filebeat, Promtail, Vector, Fluent Bit

Why is `internal/agent/tailer` so complicated? Short answer: it implements the
strongest delivery contract in this space without buying it with a buffering
layer, and it supports two things almost nobody else does. This document
compares the design against the major log shippers and names honestly which
parts of the complexity are essential and which are self-inflicted.

Sizes as of the segment refactor: ~3,300 production lines, split by concern
(`tailer.go`, `ledger.go`, `discover.go`, `pipeline.go`, `read.go`,
`rotate.go`, `archive.go`, `flush.go`, `checkpoint.go`, `sources.go`,
`status.go`) and roughly one and a half times that in tests, grouped the
same way.
The historical sections below describe the PRE-refactor design they analyzed;
"If it should ever get simpler" records what has since been done.

## The guarantee matrix

Each competitor weakens at least one leg that this tailer keeps:

| | kubescrape | Filebeat | Promtail | Vector | Fluent Bit |
|---|---|---|---|---|---|
| Offsets committed only after collector ack | **yes** | yes (via queue) | **no** — positions advance on read, send is decoupled | with acks enabled | only with filesystem storage |
| Multiline joining coupled to offset safety | **yes** (watermark) | partial (per-harvester) | **no** — buffered groups can be lost on restart | mostly | loose (in-memory buffer) |
| Multiline group joins **across** a rotation | **yes** | no | no | no | no |
| copytruncate handled | **yes** (fingerprint re-verify) | docs say "don't use copytruncate" | poorly | via checksums | partially |
| Compressed archives, resumable | **yes** (decompressed offsets) | no | no | no | no |
| Retry buffer | **the disk itself** (rewind + re-read) | memory queue | memory queue (lossy) | memory/disk buffers | chunks + storage |
| Rate limiting without loss | yes (pause + salvage) | no | drop-based | throttle transform | no |

## Per-tool notes

**Promtail** is the simplest of the group precisely because it does not hold
positions back for unsent data: read a line, advance the position, hand the
entry to a buffered client channel. Crash with data in flight and it is gone —
an accepted design cost. Delete that one requirement from this tailer and the
watermark, the ledger FIFOs, the segment accounting, rewind, and the
withheld-commit machinery all evaporate. That is the single biggest fork in
the road.

**Filebeat** does have the ack-coupled contract — but it buys it with an
architecture this tailer deliberately avoids: a harvester goroutine per file →
spooler → memory queue with ack callbacks → registrar. The offset-accounting
complexity does not disappear there; it is *relocated* into the queue/ack
plumbing and smeared across libbeat, plus a notorious config surface that
outsources the hard decisions to the operator (`close_inactive`,
`close_renamed`, `close_removed`, `close_eof`, `clean_inactive`,
`clean_removed`, `ignore_older`, …— each one a data-loss-vs-fd-leak trade
resolved here in code instead). Filebeat also keeps sent-but-unacked events
*in memory* until acked; the rewind design re-reads from disk instead, which
is why the offset machinery here is richer and the memory profile flatter.

**Vector** is the closest architectural cousin — its `file-source` crate has
the same fingerprinting-first design, checksum identity, and checkpoint files,
and with end-to-end acknowledgements enabled it approaches this contract. It
is also, not coincidentally, thousands of lines of Rust plus a separate
buffer/ack subsystem. Nobody achieves this contract cheaply.

**Fluent Bit**'s `in_tail` tracks offsets in SQLite and advances them on
ingestion into its chunk buffer; at-least-once holds only with filesystem
storage enabled, and the multiline engine's in-memory buffers sit outside the
offset accounting entirely (a crash loses buffered groups).

## Why this code *looks* worse than it is

1. **Concentration.** ~3,150 lines do what Filebeat spreads across harvester +
   input manager + spooler + queue + registrar — well over 10k lines before
   counting libbeat. No single Filebeat file looks scary; the complexity hides
   in the seams between abstractions. The single-sweep-goroutine design means
   the ledger/watermark/segment accounting *is* the ack plumbing, all in one
   place. Better
   for correctness (the audit rounds could trace every interleaving), worse
   for first impressions.

2. **The cross-rotation multiline join.** The most exotic feature — at the
   time of this analysis: carried prefixes, gen bumps, re-anchoring, retained
   fds with caps, checkpoint `Pending` lists, `findRotated` restart recovery.
   No competitor attempts it; they all break the group at the rotation
   boundary and ship two half-records. This one guarantee accounted for
   roughly a third of the package's intricacy. The cleaner decomposition (a
   logical-stream aggregator over a byte-durability layer) has since been
   DONE — see item 4 below: segment-qualified positions deleted the
   gen/reanchor protocol outright. This remains the honest "self-inflicted"
   portion — a real feature, but a rare event, and the deep audit found most
   of its historical bugs lived exactly here.

3. **Adversarial hardening for cases others document away.** Filebeat's answer
   to copytruncate is "don't". Promtail's answer to buffered-multiline loss is
   silence. Their issue trackers are full of precisely the bugs the audit
   rounds fixed here: inode-reuse duplicates, registry growth, torn records
   after in-place rewrites, fd exhaustion during outages. The
   `restartAt`/fingerprint-extension/quarantine code is the shape correctness
   takes when those are *not* accepted as known limitations.

The 5,607 test lines against 3,150 production lines is an unusually strong
ratio, and interleaving-exact tests are themselves a design constraint: the
synchronous sweep exists partly so tests can drive exact orderings, which is
why the audits could *prove* properties competitors cannot.

## If it should ever get simpler

In descending order of lines saved per guarantee lost:

1. **Drop the cross-rotation join** — accept a split group at rotation like
   everyone else. Post-refactor this means the segment list, feedSegments,
   findRotated and the fd caps: still the biggest lever, though the segment
   model has already removed the trickiest invariants (gen/reanchor).
2. **Decouple send from commit, promtail-style** — positions advance on read.
   Removes watermarks, rewind, segment commits, and withheld-highs entirely.
   Cost:
   at-least-once becomes best-effort across crashes. Not recommended — it is
   the product's core promise.
3. **Drop pipelined export** — an opt-in perf mode whose settle/gen interplay
   is real complexity, for overlap `-buffer-dir` can provide instead.
   Expanded below. **Done** (merged, PR #3).
4. **Do the deferred decomposition** — same behavior, better factoring.
   Expanded below. **Done** (merged, PR #3): offsets are
   segment-qualified `pos{seg, off}` values, rotations close the tail into
   per-file `segment` records with individual commit progress, and the
   rotation-generation protocol (`gen`), the buffered-offset rewrite
   (`reanchor`), and the all-or-nothing carried release (`carriedDone`) are
   deleted — the checkpoint format was already a segment list, so no
   migration was needed.

## Expanded: dropping pipelined export

*(Written as the proposal; the removal has since landed. Kept as the record
of what the mode was and why it went.)*

**What it was.** `Config.PipelinedExport` overlapped reading with delivery: flush
hands the payload plus its commit information (the `inflight` struct —
offsets, unclamped highs, per-file generations, carried-release marks) to one
worker goroutine and keeps sweeping. At most one export is outstanding; its
result is applied at the next flush, by `pollInflight` in housekeeping (a
failure must rewind even when no new lines arrive), or by `settle(f)` when
rotation machinery is about to touch a file the export references.

**What it actually cost.** The feature's price was not the 158 lines of
`pipelined.go` — it was a *protocol* the rest of the tailer had to observe:

- Every path that mutates file state (rename rotation, truncation,
  copytruncate, archive replacement, gone-file drain, drop, shutdown) must
  call `settle(f)` *first*, so an in-flight failure rewinds before the state
  it would rewind into is destroyed. Forgetting one call site is silent data
  loss; the gen-check in `commitBatch`/`failBatch` is only a backstop.
- The commit ceiling must be frozen at *build* time, not apply time: by the
  time a pipelined result lands, lines that were buffered at build may sit in
  the next, unexported batch, and the live watermark no longer protects them.
  This build/apply split is the single most non-obvious invariant in the
  package.
- Drains must force-synchronous export (`flushDuringDrain` nils the channel):
  a handed-off export would still be in flight when `reopen` bumps the
  generation or `release` closes fds, and its later failure would skip the
  rewind.
- The audit record is the argument made concrete: two real bugs traced to
  exactly this interplay — the settle-triggered rewind invalidating the
  truncation check's `readPos` (a torn record and a skipped replacement
  prefix), and an earlier commit-held-by-build-watermark bug. Four test files
  exist solely to pin the mode.

**What it buys, and the substitute.** Overlap matters when export latency is
high: inline export blocks the sweep for the round trip (times three retries
with backoff against a struggling collector). `-buffer-dir` provides the same
decoupling one layer down — the tailer's "export" becomes an fsync'd append to
a local spool (sub-millisecond), and the buffered exporter owns retries,
backoff, and at-least-once across restarts. Back-pressure composes correctly:
a full spool returns `ErrFull`, the tailer rewinds, same as an export failure.

**The honest niche.** The substitution is not free: the spool costs one
fsync per append and disk bandwidth, and on diskless or read-only-rootfs nodes
there is nowhere to put it. Pipelined export exists for exactly that corner —
overlap without local durability. If that corner is not in the deployment
matrix, the mode duplicates the buffer's capability at a high invariant cost,
and removing it deleted `pipelined.go`, every `settle` call site, the
build-time-freeze subtlety, and four test files, while keeping the
withheld-commit re-offer (which the synchronous path needs too). The gen
machinery survived this removal but fell to the decomposition below —
segment-qualified positions made it unnecessary.

## Expanded: the deferred decomposition

*(Written as the proposal; the decomposition has since landed as described,
except that no positions-format migration was needed — the checkpoint format
already was a segment list.)*

**The entanglement.** `tailer.go` fused three concerns that change for
different reasons:

1. *Byte acquisition and durability* — fd lifecycle, rotation identity
   (inode + head fingerprint), copytruncate detection, byte offsets,
   checkpoints, rewind.
2. *Logical-stream assembly* — CRI parsing, P/F fragment runs, stack-trace
   joining, and the ledger/FIFO machinery mapping logical entries back to the
   byte ranges they came from.
3. *Delivery* — batching, flush, export, commit.

The fusion point is that pipelines are keyed **per file**, while the thing
being assembled is a **per-container stream** that merely happens to be
materialized as a *sequence* of files. Every rotation therefore forces the
assembly state across a file boundary, and that is precisely the machinery the
audits kept finding bugs in: carried prefixes, offset re-anchoring, generation
bumps, the live-path vs restart-path asymmetry (`feedCarriedPrefix` vs
`findRotated`).

**The proposed shape.** Three layers with the stream, not the file, as the
stable identity:

- **Segment layer** (durability): tracks each stream's files as an ordered
  segment list — today's `carried` list promoted from exception-path to
  primary model, with the current file as the tail segment. Owns discovery,
  identity, fd caps, archives, and per-segment committed offsets. Emits
  ordered byte chunks tagged `(stream, segment, range)`. Checkpoint = per
  stream, a list of `(segment identity, committed offset)` — the shape
  `positions.Prefix` already has.
- **Assembly layer**: one CRI+multiline pipeline per *stream*, fed chunks in
  order. Rotation is invisible here — the joiner never knows a file boundary
  happened. Entries carry `(segment, range)` provenance; the watermark is the
  per-stream minimum unemitted `(segment, offset)`.
- **Delivery layer**: batches entries, exports, commits per-(stream, segment)
  offsets; a segment is releasable (fd closed, checkpoint entry dropped) when
  its whole range is committed.

**What structurally disappears.** `gen` exists to disambiguate offsets in the
old inode's space from the new one's after a rotation; with offsets *paired
with their segment* the ambiguity cannot be expressed, so the whole
gen-stamping/gen-checking protocol goes. `reanchor` (re-basing buffered
offsets onto a new origin) goes for the same reason. The carried-prefix
recovery paths merge with normal operation: a crash-restart re-reads
uncommitted segment ranges through the *same* code that live operation uses,
eliminating the live/restart asymmetry where several audit bugs lived.
Invariants become type-structural — an offset cannot be applied to the wrong
inode because it does not exist apart from its segment — instead of
protocol-discipline ("remember to bump gen before...").

**What is conserved, and the new risks.** Total essential complexity does not
shrink: segment identity, fingerprint extension, fd budgets, copytruncate
detection, archive decompression, and the entry-to-byte-range ledger all
remain — the ledger becomes the assembly layer's provenance tracking rather
than vanishing. Two risks need explicit guarding: the layer boundary must not
introduce per-line allocations (chunks handed off as slice views, provenance
pooled — the pinned `BenchmarkIngestLine` 0 allocs/op is the tripwire), and
the cross-layer release rule (a segment may only close once the assembler has
drained it) replaces today's watermark check and needs the same rigor.

**Migration sketch**, each step landable green against the existing test
suite (which is the asset that makes this tractable at all — the
interleaving-exact tests pin the behavior being preserved):

1. Introduce the segment list inside the current code — rename `carried` to
   segments, model the current file as the tail segment. No behavior change.
2. Key pipelines by container/stream instead of by file.
3. Move checkpoints to per-stream segment lists (a versioned positions-format
   migration; the spool already sets the versioning pattern).
4. Delete `gen`/`reanchor` once every offset is segment-qualified.

**Cost/benefit.** Net line count likely similar or slightly higher; per-file
complexity much lower; the win is correctness-by-construction for the class of
bug the audits kept finding. The right trigger: the next bug that lands in the
carry/gen machinery, or before any new feature that touches rotation.

## Verdict

For the contract it delivers, the implementation is *compact*, not bloated —
the complication is essential complexity that competitors either relocate into
queue abstractions, dilute into config knobs, or decline by weakening
guarantees. The one place paying for something rare is the cross-rotation
join; that is a product judgment, not an engineering flaw, and it is already
flagged as the candidate for restructuring if it ever stops earning its keep.
