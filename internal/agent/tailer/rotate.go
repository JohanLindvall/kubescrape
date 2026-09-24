package tailer

// Rotation and rewind: draining a rotated-away inode, closing the tail into a
// segment (carrying a straddling multi-line group across the boundary), the
// hop save, and rewinding a file to its committed offset after a failed
// export. Replaying the recorded segments is replay.go's; draining and
// releasing vanished files is gone.go's.

import (
	"context"
	"errors"
	"io"

	"github.com/JohanLindvall/kubescrape/internal/obs"
)

// drainFile reads the (rotated-away or removed) file to EOF so no bytes
// written between our last read and the rotation are lost. Bounded to keep a
// still-active writer from pinning the sweep.
func (t *Tailer) drainFile(ctx context.Context, f *file) bool {
	if f.f == nil {
		return true
	}
	return t.drainReader(ctx, f, f.f, "file")
}

// drainReader reads r to EOF into f, consuming and flushing as it goes, so a
// rotated-away or removed file's uncommitted tail is not lost when its fd drops.
// Whatever is left in the source once the fd closes is unreachable, so a byte
// budget here would mean permanent loss (a backlog over the budget is realistic
// — kubelet rotates at 10MiB, rate-limit pause mode accumulates arbitrary
// backlogs); the cap is only a circuit breaker against a source that outruns the
// drain forever (a writer holding the rotated fd open, or a gzip bomb).
//
// It reports false when the drain did NOT reach the end of the source, which is
// two cases and both mean "the caller must not treat this incarnation as fully
// drained":
//
//   - a mid-drain flush FAILED and rewound this file: the rewind seeks the very
//     fd being drained back to the committed offset, so continuing would re-read
//     the same bytes into a batch whose export just failed — a hot loop burning
//     export attempts on the single sweep goroutine until the cap. The caller
//     must abort and retry the whole drain on a later sweep instead (sweep
//     cadence is the backoff; nothing is lost — the fd stays held and the
//     offsets are rewound).
//   - the cap fired. This used to return the same `true` as EOF, and that made
//     the circuit breaker a SILENT DATA LOSS on the one caller that reads the
//     verdict: handleRotation then completed the rotation as if the old inode
//     were exhausted, and everything past the 1 GiB mark became unreachable
//     forever — no obs.LogPrefixLost, no obs.LogDrainErrors, nothing but one log
//     line (the sibling read-error arm below was fixed for exactly this class).
//     A 4 GiB backlog — an agent down for an hour, or a source outrunning the
//     per-sweep budget — is ordinary, not pathological. Reporting the drain
//     unfinished routes it into the machinery that already exists for
//     "un-drained rename": reopen records the old incarnation as an OPEN-ENDED
//     segment and feedSegments replays it across sweeps under MaxBytesPerSweep,
//     which is exactly what readFile's own segment gate does when it rotates a
//     file it may not read yet. The circuit breaker keeps its real job — one
//     drain call never holds the single sweep goroutine for more than drainCap
//     bytes — and stops being a loss.
//
// The cost of that choice, said out loud because it is what the cap was written
// to prevent: while the replay is unfinished the LIVE TAIL is not read (readFile
// gates on it, or the joiner would fuse lines across the gap), so a writer that
// genuinely outruns the drain forever now gates the new incarnation instead of
// abandoning the old one. That is the right trade — in that scenario the writer
// is still writing to the ROTATED inode (it never reopened), so the data being
// followed is the data being produced — and it is bounded from below by
// chargeStall only for a replay making NO progress. The alternative on offer was
// discarding gigabytes with every loss counter flat.
func (t *Tailer) drainReader(ctx context.Context, f *file, r io.Reader, what string) bool {
	drainCap := t.drainCap
	if drainCap <= 0 {
		// New always sets it; a zero here would read as "drain nothing", which
		// is the one behaviour this function must never have.
		drainCap = defaultDrainCap
	}
	buf := t.scratch()
	if len(f.pending) > 0 {
		// A rate-limit-paused file may hold already-read unconsumed lines; they
		// would be discarded with pending when the fd drops.
		t.consume(ctx, f, true)
	}
	var drained int64
	for drained < drainCap {
		n, err := r.Read(buf)
		if n > 0 {
			drained += int64(n)
			// Drain mode: the rate limit is bypassed (pausing would lose the
			// remainder when the fd is dropped) and consume does not flush —
			// this is the drain's own flush point, paired with the rewind check
			// the drain has to make.
			t.ingestChunk(ctx, f, buf[:n], true)
			if t.maybeFlush(ctx, f) {
				return false // flush failed and rewound the drained fd
			}
		}
		if err != nil {
			// EOF is the drain succeeding. Anything else is the drain ENDING
			// EARLY on bytes that exist and cannot be read — an EIO on the
			// rotated inode, a corrupt gzip member — and the caller is about to
			// record this incarnation as fully fed, retiring its segment and
			// advancing past a remainder nobody read. That is a data loss, and
			// returning the same `true` as EOF made it an invisible one.
			//
			// It still returns true: the read cannot be retried into success
			// (the next sweep would fail identically while holding the fd, a
			// hot loop on the single sweep goroutine), so the honest handling
			// is to give up on the remainder and SAY SO. false is reserved for
			// "a flush failed and rewound this fd", which is genuinely
			// retryable.
			if !errors.Is(err, io.EOF) {
				obs.LogDrainErrors.WithLabelValues(what).Inc()
				// The gone path's stall accounting keys on this: a drain that
				// ERRORED without commit progress is what can leave goneEnd
				// unreachable forever (chargeGoneStall).
				f.drainErred = true
				t.log.Error("reading failed mid-drain; the unread remainder of this file is unrecoverable",
					"path", f.path, "source", what, "drained", drained, "error", err)
			}
			return true
		}
	}
	// Not an Error any more: the remainder is owed, not lost. It stays visible
	// (a source that outruns a 1 GiB drain is worth an operator's attention —
	// it is how a deep backlog or a writer that never reopened after logrotate
	// announces itself) but no loss counter moves, because nothing was given
	// up on: the caller records the un-drained remainder as a segment.
	t.log.Warn("source still yielding after draining the per-drain cap; the remainder is replayed as a segment",
		"path", f.path, "source", what, "drained", drained)
	return false
}

// reopen switches to the file now at the path and resets the byte position so
// the next sweep reads the new inode from offset 0. The file is marked dirty
// so an event-driven loop picks it up immediately.
//
// On a rename rotation (renamed) with an uncommitted range, the old inode is
// recorded as a segment on f.segments (with the fd where the budget allows)
// so a crash or rewind before its lines export can re-read the owed range.
// If a multi-line group still straddles the boundary — data remains buffered
// in the pipeline after the old inode was drained — the pipeline is carried
// across instead of flushed, so the group joins the pre- and post-rotation
// lines into one record: the buffered items keep their (old-segment)
// positions untouched, and the fresh tail id issued below makes the new
// inode's bytes unambiguous.
//
// Otherwise (truncation, copytruncate, or a rename with nothing buffered) the
// pipeline is flushed and reset as before — carrying makes no sense when the
// content was replaced.
//
// drained reports whether the pre-rotation drain of the old inode completed.
// A rename rotation whose drain ABORTED (mid-drain flush failure) is still
// completed rather than abandoned — see the aborted branch below.
func (t *Tailer) reopen(ctx context.Context, f *file, renamed, drained bool) {
	obs.LogRotations.Inc()
	// Complete lines sitting in pending (a rate-limit PAUSE leaves them there)
	// were read from the pre-rotation content and are deliverable regardless of
	// what happened to the file on disk since. Feed them now, bypassing the
	// limiter, before the pipeline is carried or discarded — clearing them
	// below would convert pause mode's "no loss" into loss. Only a trailing
	// unterminated fragment legitimately dies with the clear (its terminator
	// no longer exists anywhere).
	if len(f.pending) > 0 {
		t.consume(ctx, f, true)
		if n := len(f.pending); n > 0 && renamed && drained && !f.discarding {
			// A trailing unterminated fragment of a FULLY DRAINED, RENAMED-away
			// inode can never complete (the old file is not followed once the
			// drain reached its end); it dies with the reset below on every
			// path — live, rewind re-feed, and crash-restart (replaySegment
			// feeds only terminated lines) — so at minimum the loss is visible.
			//
			// The two gates say "these bytes are not a lost line":
			//
			// `drained` — an UNFINISHED drain (a mid-drain flush failure, or
			// the per-drain cap) does not abandon the old inode: the aborted
			// arm below records it as an OPEN-ENDED segment whose owed range
			// starts at committed, so the replay re-reads this fragment. It
			// completes it when the line continues in the file, and counts it
			// (replaySegment's open-ended completion) when the file ends there.
			// Counting it here too reported a loss for a line that may then be
			// delivered — a false positive on the tailer's own loss signal,
			// which is worse than useless because it is indistinguishable from
			// the real thing.
			//
			// !f.discarding is the same gate drainGone's structurally
			// identical block applies: while discarding, these bytes are the
			// TAIL of an oversized line whose prefix consume already dropped
			// and already counted into obs.LogOversizedDropped. Counting them
			// here too moves two loss counters for ONE physical line, and the
			// Warn — the only human-readable report the oversize path produces
			// at all — would name the residual fragment (a few KiB) for a drop
			// of megabytes and blame a rotation that destroyed nothing which
			// could ever have become a record.
			obs.LogTornFinalLines.Inc()
			t.log.Warn("unterminated final line lost at rotation", "path", f.path, "bytes", n)
		}
	}
	// The rotated-away inode's fd is handed to the segment that records it
	// (and closed below if none does): it is the only handle that survives
	// the runtime deleting the rotated file.
	old := f.f
	f.f = nil
	defer func() {
		if old != nil {
			_ = old.Close()
		}
	}()
	// Retaining an fd per segment is unbounded otherwise: an outage spanning
	// many rotations would exhaust RLIMIT_NOFILE and — worse — pin every
	// rotated inode's disk space, filling the node's log volume precisely
	// while the collector is down. Cap the fds; the segments themselves are
	// kept (a rotated file that still exists is recoverable by name via
	// findRotated). The fds are held for the OLDEST segments on purpose: the
	// runtime prunes its rotation backlog oldest-first, so those are the ones
	// for which the fd is the only remaining handle.
	keep := func(sg *segment) *segment {
		if f.retainedFds() >= maxCarriedFds {
			return sg // over budget: leave old to the deferred Close
		}
		sg.fd, old = old, nil
		return sg
	}
	// The segment's owed range ends at the last FED line boundary, not at
	// readPos: trailing bytes that never entered the pipeline — a torn final
	// fragment (counted above), a blank line, a rate-DROPPED or oversized-
	// discarded line — can never produce a committing entry, and a `to`
	// covering them pinned the segment below retirement forever (fd + gone
	// file + checkpoint entry leaked, one per rotation for a writer ending
	// with a blank line).
	fedEnd := f.fedEnd()
	// Whether every PRE-EXISTING segment's owed lines are live (pipeline or
	// batch) — captured before this rotation appends its own hop. With no
	// prior segments the answer is vacuously yes (segmentsFed is only
	// meaningful while segments exist).
	//
	// An ABORTED drain (mid-drain flush failure) breaks that: failBatch rewound
	// the fd and discarded the pipeline, so the old inode's owed lines are NOT
	// live and feedSegments must replay them. Forcing wasFed false is what
	// arms that replay.
	aborted := renamed && !drained
	// prevFed is the captured value BEFORE the abort overrides it, and nPrev
	// how many segments it speaks for (this rotation only ever APPENDS, and
	// neither stopPipeline nor newPipeline below touches the list).
	prevFed, nPrev := f.segmentsFed || len(f.segments) == 0, len(f.segments)
	wasFed := prevFed && !aborted
	hopAdded := false
	if aborted {
		// Record the un-drained inode as an OPEN-ENDED segment (to = -1) and
		// carry on with the rotation.
		//
		// Abandoning it instead — staying on the old inode with no fd, no
		// segment and no record of the new file at the path — looked safe
		// because the rotation would be retried next sweep. It is not: the
		// abort can only clear once an export succeeds, so the window is the
		// whole export outage, not one sweep. A SECOND rotation inside it finds
		// nothing to drain, records no segment, and the entire intermediate
		// incarnation becomes unreachable — up to the runtime's rotation size
		// per rotation, silently, with every loss counter flat.
		//
		// `to = -1` is the shape discover.go already synthesises for a
		// rotation-while-down: saveCheckpoints persists it, and replaySegment
		// reads to EOF under its own budget and pins `to` then. The normal
		// fedEnd > committed test cannot be used here because the rewind purged
		// the pipeline, so nothing would be recorded at all.
		//
		// fedTo starts at the FED boundary, which is the one thing that
		// differs between the two ways a drain can end unfinished. A flush
		// failure purged the pipeline and rewound, so fedEnd() is floored back
		// to committed and this is a no-op — the replay re-reads from
		// committed, as it must. The CAP purged nothing: [committed, fedEnd)
		// is in the unflushed batch, so resuming at committed would deliver
		// every one of those records twice for no reason. A later rewind
		// zeroes fedTo (purgeSegmentFeeds) and the replay falls back to committed,
		// which is exactly the contract fedTo already carries.
		f.segments = append(f.segments, keep(&segment{
			id: f.tail, inode: f.inode, fp: f.fp, committed: f.committed,
			fedTo: fedEnd, to: -1, fed: false,
		}))
		hopAdded = true
	} else if renamed && fedEnd > f.committed {
		// Close the tail into a segment: its uncommitted range [committed,
		// fedEnd) is owed. If a group is still buffered the pipeline is
		// carried below and the segment's items keep their (old-segment)
		// positions unchanged; either way, if the export of the drained
		// entries fails (or the process crashes) the rotated-away file is the
		// only copy, and the segment record is what lets feedSegments re-read
		// it. It retires in commitBatch once its whole range commits.
		f.segments = append(f.segments, keep(&segment{
			// fed: `to` IS the last fed line boundary, so this segment's whole
			// owed range is already in the pipeline or the batch.
			id: f.tail, inode: f.inode, fp: f.fp, committed: f.committed, to: fedEnd, fed: true,
		}))
		hopAdded = true
	}
	// segmentsFed asserts EVERY recorded segment's owed lines are live (in
	// the pipeline or the batch). A mid-drain export failure rewinds and sets
	// it false — the older segments' re-fed lines were just purged from the
	// batch — and this rotation must not overclaim them back to "fed": doing
	// so silently stranded an older rotation's lines until a restart (or
	// forever without a positions store). The new hop's own lines ARE live
	// (the drain re-read them after any rewind), so preserving the captured
	// value is exact.
	// carry: a group straddles the rotation, so the pipeline is carried into
	// the new inode. Buffered items keep their segment-qualified positions —
	// no re-basing, no generation — and the fresh tail id below makes the new
	// inode's bytes unambiguous. (Only when the older segments are fed: with
	// unfed segments owed, the buffered fragments would sit in the pipeline
	// AHEAD of the older lines feedSegments must replay first — flush the
	// group split instead of joining it out of order.)
	_, buffered := f.watermark()
	carry := renamed && buffered && wasFed
	if !carry {
		// DRAIN the stages into the batch, then rebuild them WITHOUT the purge
		// newPipeline performs. That purge (purgeSegmentFeeds) is written for a
		// REWIND, whose lines were discarded unemitted, so the replay must
		// start over from `committed`: it zeroes every segment's fedTo and
		// discard frontier and marks it unfed. stopPipeline does the opposite —
		// it EMITS the buffered lines into the batch — so a budget-cut
		// replay's already-fed prefix is still live, and re-reading it
		// delivered every one of those records twice until the batch flushed;
		// the discard frontier (skipTo/discarding) describes DISK content (an
		// oversized line's already-dropped prefix), which the drain does not
		// invalidate either. This used to call newPipeline and snapshot-and-
		// restore the frontiers around it.
		//
		// The segment list is NOT reset here: earlier segments' lines are
		// still uncommitted, and a second rotation (or a truncation) during a
		// collector outage does not make them recoverable any other way.
		// Segments retire individually in commitBatch.
		t.stopPipeline(ctx, f)
		t.rebuildPipeline(f)
	}
	// A rotation purges nothing: entries built from fed segments are still in
	// the unflushed batch, and re-feeding them would duplicate every one of
	// those records on a plain truncation. So the fed state is the captured
	// wasFed — for the file, and identically for EVERY segment, the one
	// recorded above included (born `fed: true`, it ends unfed whenever wasFed
	// is false). That per-segment answer is exactly what the purge-then-
	// restore this replaced left behind, and the fed flags are load-bearing:
	// proposeCandidates reads them for traversal claims.
	f.segmentsFed = wasFed
	for _, sg := range f.segments {
		sg.fed = wasFed
	}
	if prevFed && aborted {
		// The abort forced wasFed false so the NEW open-ended segment is
		// replayed, but it says nothing about the OLDER segments: when the
		// drain stopped at the per-drain CAP nothing was purged, and their
		// lines are still live in the unflushed batch. (A flush failure
		// rewinds first, which clears segmentsFed, so prevFed is false there
		// and this arm does not run.) Restoring `fed` alone is not enough:
		// feedSegments gates on the file-level flag, and replaySegment resumes
		// from max(committed, fedTo, skipTo) — a rotation-recorded segment's
		// fedTo is 0, so the replay re-fed every one of those lines and each
		// was delivered twice. fedTo = to makes their replay a no-op; a later
		// rewind still zeroes both through purgeSegmentFeeds, so the purge
		// semantics are unchanged.
		for _, sg := range f.segments[:nPrev] {
			sg.fed = true
			if sg.to >= 0 {
				sg.fedTo = max(sg.fedTo, sg.to)
			}
		}
	}
	// A new incarnation (beginIncarnation: fresh tail id, offsets at zero, and
	// any goneEnd from an earlier one no longer describes this file — see the
	// resurrect path in sweep). The identity is cleared for the next
	// ensureOpen to adopt; the withheld highs are KEPT, because they name the
	// segment recorded above.
	f.beginIncarnation(0)
	f.inode, f.fp = 0, fingerprint{}
	// The next ensureOpen's watchTarget re-derives the symlink target and
	// switches watches acquire-before-release, so no eager unwatch here — an
	// unwatched hole between reopen and that sweep would lose a second
	// rotation happening inside one poll interval.
	f.dirty = true
	if hopAdded {
		t.noteHop(f)
	}
}

// noteHop records that f just gained a rotation hop — a segment naming a
// rotated-away inode — and enforces the one invariant its persistence needs.
//
// The hop must reach disk long before the 10s checkpoint cadence: a crash in
// that window leaves the on-disk checkpoint with no record of the rotated
// inode, and the tail is then lost outright rather than merely re-read.
//
// What actually has to hold is narrower than "persist every hop synchronously",
// and this is the whole reason a save per SWEEP still closes the window:
// initFile reconstructs the ONE hop a stale checkpoint implies — it sees the
// path naming a different incarnation than the stored identity and synthesizes
// an open-ended segment for it — so a single unpersisted hop is recoverable
// from the previous save. It is the SECOND hop of the same file that has no
// route back, because nothing on disk names the intermediate inode. So the
// invariant is "one file never carries two unsaved hops", enforced here, and
// the sweep's closing save (keyed on hopsUnsaved) bounds the exposure of the
// first one to the rest of that sweep.
//
// The cost this buys back is not marginal: a save marshals the WHOLE positions
// document and fsyncs it twice (file, then directory) — ~25ms and ~3.4MB of
// garbage at 5000 files — on the single sweep goroutine, and it ran once per
// hop. A storm in which 50 files rotate in one sweep paid 50 of them, precisely
// during the event the immediate save exists to survive.
//
// Call it only once f's in-memory state is CONSISTENT — the new identity
// adopted, `committed` reset, the old incarnation's segment appended: the
// forced save writes exactly what it sees. reopen and ensureOpen are the two
// callers, and they used to carry two copies of this block, one of which ran
// BEFORE its reset and persisted the new inode paired with the old offset.
func (t *Tailer) noteHop(f *file) {
	if !t.checkpointing() {
		return
	}
	if f.hopUnsaved {
		t.saveCheckpoints()
	}
	f.hopUnsaved, t.hopsUnsaved = true, true
}

// rewind seeks a file back to its committed offset so unexported data is
// read again. Pipeline state is discarded without emitting: the buffered
// lines sit after the committed offset and will be re-read and re-fed.
func (t *Tailer) rewind(f *file) {
	// Bump BEFORE any state changes so a loop that flushed mid-pass can see
	// that its own read position was purged under it (see replaySegment).
	f.rewindGen++
	// Drop the withheld highs. Every one of them names bytes ABOVE `committed`
	// — that is what "withheld" means — so the rewind re-reads them and the
	// next flush re-proposes them; keeping the map buys nothing and costs the
	// one thing this package must never do. A rewind reuses the TAIL ID (no
	// newTail, unlike every replacement path, because the file is unchanged),
	// so a stale high stays live against it: if the writer then replaces the
	// content in place (logrotate copytruncate, an app reopening with O_TRUNC)
	// while readPos is back at `committed`, nothing detects it — the pre-read
	// fingerprint re-verify is skipped at readPos 0, and neither truncation arm
	// of handleRotation can fire on a from-zero read — and the stale high
	// commits the REPLACEMENT past bytes it never exported. Measured: a 587-byte
	// high applied to a 112-byte replacement, committed=587, 9 of 11 lines never
	// exported and the restart resuming mid-line into a torn body, with every
	// loss counter flat. read.go's `replaced` arm makes the same clear for the
	// same reason; this is the path that reuses the id rather than retiring it.
	f.exportedHighs = nil
	if f.compressed {
		// gzip is not seekable: drop the reader so openArchive re-decompresses
		// from the committed offset next sweep. The fd is RETAINED (the archive
		// may be unlinked before the retry — see closeArchiveReader).
		// archiveDone must reset with it: the rewound range needs re-reading
		// even though the file is unchanged.
		t.closeArchiveReader(f)
		f.archiveDone = false
		f.archiveEOF = false // the tail is owed again; see the release gate
		f.restartAt(f.committed)
		t.newPipeline(f)
		return
	}
	// The pipeline reset below must happen even with no fd open: reopen leaves
	// f.f nil and marks the segments fed (their lines are live in the
	// pipeline). Returning early here would discard those lines with the
	// batch while leaving segmentsFed set, so feedSegments would never
	// re-read them — the rotated tail would be lost on the first failed export.
	// purgeSegmentFeeds (via newPipeline) is what clears segmentsFed and re-arms it.
	if f.f != nil {
		if _, err := f.f.Seek(f.committed, 0); err != nil {
			_ = f.f.Close()
			f.f = nil // the next ensureOpen reopens and re-verifies identity
		}
	}
	f.restartAt(f.committed)
	t.newPipeline(f)
}
