# The tailer vs. Filebeat, Promtail, Vector, Fluent Bit

Why is `internal/agent/tailer` so complicated? Short answer: it implements the
strongest delivery contract in this space without buying it with a buffering
layer, and it supports two things almost nobody else does. This document
compares the design against the major log shippers and names honestly which
parts of the complexity are essential and which are self-inflicted.

Sizes as of the deep-audit round: ~3,150 production lines (`tailer.go` 2,723 +
`sources.go` + `pipelined.go` + `status.go`), 5,607 test lines across 25 test
files.

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
watermark, the ledger FIFOs, the gen-checking, rewind, and the withheld-commit
machinery all evaporate. That is the single biggest fork in the road.

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
   the ledger/watermark/gen *is* the ack plumbing, all in one place. Better
   for correctness (the audit rounds could trace every interleaving), worse
   for first impressions.

2. **The cross-rotation multiline join.** The most exotic feature — carried
   prefixes, gen bumps, re-anchoring, retained fds with caps, checkpoint
   `Pending` lists, `findRotated` restart recovery. No competitor attempts it;
   they all break the group at the rotation boundary and ship two
   half-records. This one guarantee accounts for roughly a third of the
   package's intricacy, and CLAUDE.md records that the cleaner decomposition
   (a logical-stream aggregator over a byte-durability layer) is known and
   deferred. This is the honest "self-inflicted" portion — a real feature, but
   a rare event, and the deep audit found most of its historical bugs lived
   exactly here.

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
   everyone else. Removes the carried/gen/reanchor/findRotated/fd-cap
   machinery: several hundred lines and the trickiest invariants. The biggest
   lever by far.
2. **Decouple send from commit, promtail-style** — positions advance on read.
   Removes watermarks, rewind, gens, and withheld-highs entirely. Cost:
   at-least-once becomes best-effort across crashes. Not recommended — it is
   the product's core promise.
3. **Drop pipelined export** — an opt-in perf mode whose settle/gen interplay
   is real complexity, for overlap `-buffer-dir` can provide instead.
4. **Do the deferred decomposition** — same behavior, better factoring: a
   per-container logical-stream aggregator over a byte-offset durability
   layer. Does not reduce essential complexity, but splits the 2,700-line file
   along its actual seam.

## Verdict

For the contract it delivers, the implementation is *compact*, not bloated —
the complication is essential complexity that competitors either relocate into
queue abstractions, dilute into config knobs, or decline by weakening
guarantees. The one place paying for something rare is the cross-rotation
join; that is a product judgment, not an engineering flaw, and it is already
flagged as the candidate for restructuring if it ever stops earning its keep.
