package tailer

// Rotation, rewind and recovery: closing the tail into segments, replaying
// incomplete segments (live and after a restart), draining vanished files,
// and releasing settled ones.

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"slices"
	"time"

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
// It reports false when a mid-drain flush FAILED and rewound this file: the
// rewind seeks the very fd
// being drained back to the committed offset, so continuing would re-read the
// same bytes into a batch whose export just failed — a hot loop burning
// export attempts on the single sweep goroutine until the 1 GiB cap. The
// caller must abort and retry the whole drain on a later sweep instead
// (sweep cadence is the backoff; nothing is lost — the fd stays held and the
// offsets are rewound).
func (t *Tailer) drainReader(ctx context.Context, f *file, r io.Reader, what string) bool {
	const drainCap = 1 << 30
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
	t.log.Error("source still yielding after draining 1GiB, abandoning remainder", "path", f.path, "source", what)
	return true
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
		if n := len(f.pending); n > 0 && renamed {
			// A trailing unterminated fragment of a RENAMED-away inode can
			// never complete (the old file is not followed after the drain);
			// it dies with the reset below on every path — live, rewind
			// re-feed, and crash-restart (replaySegment feeds only terminated
			// lines) — so at minimum the loss is visible.
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
	wasFed := (f.segmentsFed || len(f.segments) == 0) && !aborted
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
		f.segments = append(f.segments, keep(&segment{
			id: f.tail, inode: f.inode, fp: f.fp, committed: f.committed, to: -1, fed: false,
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
	if _, buffered := f.watermark(); renamed && buffered && wasFed {
		// A group straddles the rotation: carry the pipeline into the new
		// inode. Buffered items keep their segment-qualified positions — no
		// re-basing, no generation — and the fresh tail id below makes the
		// new inode's bytes unambiguous. (Only when the older segments are
		// fed: with unfed segments owed, the buffered fragments would sit in
		// the pipeline AHEAD of the older lines feedSegments must replay
		// first — flush the group split instead of joining it out of order.)
	} else {
		// ledger.reset zeroes every segment's fedTo because it is written for
		// the PURGE case (rewind): the lines it names were discarded unemitted,
		// so the replay must start over from `committed`. stopPipeline does the
		// opposite — it DRAINS, emitting the buffered lines into the batch — so
		// a budget-cut replay's already-fed prefix is still live and re-reading
		// it delivers every one of those records twice, until the batch flushes.
		// Carry the frontier across the rebuild — the discard frontier
		// (skipTo/discarding) with it: it describes DISK content (an oversized
		// line's already-dropped prefix), which the drain does not invalidate,
		// and dropping it merely re-reads and re-discards the same bytes.
		//
		// Unconditionally, unlike the `fed` re-stamp below: the drain preserves
		// the lines whatever wasFed says, and where wasFed is false because a
		// rewind already purged, the value carried is the zero that purge left.
		type frontier struct {
			fedTo, skipTo int64
			discarding    bool
		}
		frontiers := make([]frontier, len(f.segments))
		for i, sg := range f.segments {
			frontiers[i] = frontier{sg.fedTo, sg.skipTo, sg.discarding}
		}
		t.stopPipeline(ctx, f)
		t.newPipeline(f)
		// The segment list is NOT reset here: earlier segments' lines are
		// still uncommitted, and a second rotation (or a truncation) during a
		// collector outage does not make them recoverable any other way.
		// Segments retire individually in commitBatch. Neither call above
		// touches the list, so the indexes still line up.
		for i, sg := range f.segments {
			sg.fedTo, sg.skipTo, sg.discarding = frontiers[i].fedTo, frontiers[i].skipTo, frontiers[i].discarding
		}
	}
	// newPipeline's reset cleared segmentsFed; that reset exists for REWINDS
	// (where the batch was purged). A rotation purges nothing: entries built
	// from fed segments are still in the unflushed batch, and re-feeding them
	// would duplicate every one of those records on a plain truncation.
	f.segmentsFed = wasFed
	if wasFed {
		// Same restoration, per segment: a rotation purges nothing, so the
		// lines that were live before it still are.
		for _, sg := range f.segments {
			sg.fed = true
		}
	}
	f.newTail()
	// A new incarnation: any goneEnd from an earlier one no longer describes
	// this file (see the resurrect path in sweep).
	f.goneEnd = 0
	f.inode = 0
	f.fp = fingerprint{}
	f.committed = 0
	f.restartAt(0)
	// The next ensureOpen's watchTarget re-derives the symlink target and
	// switches watches acquire-before-release, so no eager unwatch here — an
	// unwatched hole between reopen and that sweep would lose a second
	// rotation happening inside one poll interval.
	f.dirty = true
	if hopAdded && t.checkpointing() {
		// The hop must reach disk long before the 10s checkpoint cadence: a
		// crash in that window leaves the on-disk checkpoint with no record of
		// the rotated inode, and the tail is then lost outright rather than
		// merely re-read.
		//
		// What actually has to hold is narrower than "persist every hop
		// synchronously", and this is the whole reason a save per SWEEP still
		// closes the window: initFile reconstructs the ONE hop a stale
		// checkpoint implies — it sees the path naming a different incarnation
		// than the stored identity and synthesizes an open-ended segment for it
		// — so a single unpersisted hop is recoverable from the previous save.
		// It is the SECOND hop of the same file that has no route back, because
		// nothing on disk names the intermediate inode. So the invariant is "one
		// file never carries two unsaved hops", enforced here, and the sweep's
		// closing save (below, keyed on hopsUnsaved) bounds the exposure of the
		// first one to the rest of that sweep.
		//
		// The cost this buys back is not marginal: a save marshals the WHOLE
		// positions document and fsyncs it twice (file, then directory) —
		// ~25ms and ~3.4MB of garbage at 5000 files — on the single sweep
		// goroutine, and it ran once per hop. A storm in which 50 files rotate
		// in one sweep paid 50 of them, precisely during the event the
		// immediate save exists to survive.
		if f.hopUnsaved {
			t.saveCheckpoints()
		}
		f.hopUnsaved, t.hopsUnsaved = true, true
	}
}

// feedSegments re-reads the incomplete segments' owed ranges and feeds them,
// oldest first, into the fresh pipeline so a straddling group reconstructs
// before the new inode's continuation is consumed. Each segment's lines are
// fed UNDER ITS OWN id (l.feeding), so their items and entries carry the
// segment-qualified positions that route their commits back to the segment's
// record. A segment whose rotated file can no longer be found (already
// deleted/compressed by the runtime) is skipped and counted — it is genuinely
// gone from disk.
func (t *Tailer) feedSegments(ctx context.Context, f *file) {
	if len(f.segments) == 0 || f.segmentsFed {
		return
	}
	// Iterate a SNAPSHOT: replaySegment retires the segment it is replaying
	// when the source is unrecoverable (openSegmentSource's findRotated miss,
	// or nothing recoverable was fed), and retire compacts f.segments with
	// slices.DeleteFunc — which NILS the vacated tail of the backing array.
	// Ranging over the live slice would hand a nil *segment to a later
	// iteration and panic on sg.id, killing the tailer's single sweep
	// goroutine and with it log collection for the whole node. Only the
	// segment being replayed is ever retired, so the snapshot needs no
	// membership re-check.
	allDone := true
	for _, sg := range slices.Clone(f.segments) {
		f.feeding = sg.id
		gen, progressBefore := f.rewindGen, max(sg.fedTo, sg.skipTo)
		if t.replaySegment(ctx, f, sg) {
			sg.stalledSince = time.Time{}
			continue
		}
		allDone = false
		t.chargeStall(f, sg, gen, progressBefore)
		// Stop the pass at the FIRST unfinished segment: segments are
		// oldest-first, so feeding a later one's lines now would put them
		// into the pipeline AHEAD of this one's still-owed remainder —
		// the same out-of-order feed the segmentsFed gate exists to
		// prevent, one level down. (A rewind mid-replay purged the
		// pipeline outright; continuing was equally wrong there.) A segment
		// just given up on is gone from the list, so the next sweep starts at
		// what is now the head.
		break
	}
	f.feeding = 0
	// Marked fed only AFTER the pass, and only when every segment finished.
	// Setting it up front stranded a segment permanently on any transient
	// failure — a non-ENOENT open error, a Seek failure, a read error —
	// because nothing would replay it again, which is the opposite of
	// replaySegment's own "left untouched for a retry": the fd stayed pinned,
	// settledGone never fired, and the lines were never counted lost either.
	// A replay that ran out of its per-sweep byte budget is unfinished for the
	// same reason, and resumes next sweep from wherever its commits reached.
	f.segmentsFed = allDone
}

// chargeStall bounds how long the LIVE TAIL may stay gated behind one segment
// that is making no progress.
//
// readFile refuses to read the tail while a replay is unfinished, and
// openSegmentSource deliberately does not retire a segment whose file is still
// there but will not open — EACCES on a rotated file, EMFILE at RLIMIT_NOFILE,
// EIO on a failing disk. Those are transient by CLASS and frequently permanent
// in fact, and while one persists this file collects nothing at all: it is the
// tailer's only silent stop, since obs.LogPrefixLost covers the permanent
// give-up and a Warn at sweep cadence (~2/s) is the sole other signal. Past the
// bound the segment is given up on exactly as an unrecoverable one is —
// counted, logged, retired — which is also what releases the gate.
//
// A pass that FED anything, one that DISCARDED anything (an oversized line
// advancing only the skipTo frontier is still advancing — stalling it out
// would retire the segment and lose the readable remainder past the line),
// and one whose pipeline a rewind purged under it, all count as progress: a
// budget-cut replay is advancing, and a failed export re-owes the range
// without the gate being the thing that is stuck.
func (t *Tailer) chargeStall(f *file, sg *segment, gen int, progressBefore int64) {
	if f.rewindGen != gen || max(sg.fedTo, sg.skipTo) > progressBefore {
		sg.stalledSince = time.Time{}
		return
	}
	now := time.Now()
	if sg.stalledSince.IsZero() {
		sg.stalledSince = now
		return
	}
	stalled := now.Sub(sg.stalledSince)
	if stalled < t.segmentStallLimit {
		return
	}
	obs.LogPrefixLost.Inc()
	t.log.Error("a rotated segment's source has been unreadable for too long; giving up on its lines so the file resumes collecting",
		"path", f.path, "inode", sg.inode, "stalled", stalled)
	f.retire(sg)
}

// chargeGoneStall bounds how long a vanished file may stay pinned behind a
// drain that can no longer reach goneEnd.
//
// drainGone's `max` keeps goneEnd rewind-proof on purpose — a transient short
// read must not settle the file early and silently lose the [error, goneEnd)
// bytes an earlier drain proved exist. But when the fd's readable boundary
// REGRESSES for good (spreading bad sectors on the unlinked inode, a corrupt
// committed prefix failing every re-decompression), commit can never reach
// goneEnd again: settledGone stays false and the entry, the fd and the
// checkpoint line are pinned forever, with obs.LogDrainErrors and an Error at
// sweep cadence as the only signal. That is the segment replay's stall wedge
// one path over, so the same budget applies: a cycle whose drain ENDED IN A
// READ ERROR without commit progress charges the stall; progress, a rewind (a
// failed export re-owes the range without the drain being what is stuck —
// chargeStall's rule) or a cycle that ends at EOF resets it. Past the limit
// the remainder is given up on exactly as a stalled segment is — counted
// obs.LogPrefixLost, logged — and the caller releases the entry through the
// normal gone cleanup (the next successful-listing save prunes its checkpoint
// line). It reports whether the file was given up on.
func (t *Tailer) chargeGoneStall(f *file, gen int, progressBefore int64) bool {
	if f.rewindGen != gen || !f.drainErred || f.committed > progressBefore || f.committed >= f.goneEnd {
		f.goneStalledSince = time.Time{}
		return false
	}
	now := time.Now()
	if f.goneStalledSince.IsZero() {
		f.goneStalledSince = now
		return false
	}
	stalled := now.Sub(f.goneStalledSince)
	if stalled < t.segmentStallLimit {
		return false
	}
	obs.LogPrefixLost.Inc()
	t.log.Error("a vanished file's drain has been erring without progress for too long; giving up on its unread remainder",
		"path", f.path, "committed", f.committed, "goneEnd", f.goneEnd, "stalled", stalled)
	return true
}

// openSegmentSource resolves the readable handle for a segment's replay: the
// retained fd first (it reaches the inode even after the runtime has deleted
// or compressed the rotated file, which findRotated — resolving by NAME —
// cannot; only a restart, where no fd survives, falls back to the path). A
// segment whose source is genuinely gone is counted (obs.LogPrefixLost) AND
// retired — an unrecoverable segment kept on the list can never reach its
// `to` and would wedge retirement (fd budget, settledGone, the checkpoint)
// forever.
// retired reports whether the failure was PERMANENT (the segment was given up
// on and removed); false means transient and the segment stays on the list for
// another sweep.
func (t *Tailer) openSegmentSource(f *file, p *segment) (fh *os.File, path string, closeFh func(), ok, retired bool) {
	if p.fd != nil {
		return p.fd, f.path, func() {}, true, false
	}
	path, found := t.findRotated(f, p)
	if !found {
		obs.LogPrefixLost.Inc()
		t.log.Warn("rotated segment source not found; its lines are lost",
			"path", f.path, "inode", p.inode)
		f.retire(p)
		return nil, "", nil, false, true // retired: nothing to open, nothing owed
	}
	opened, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) { // pruned between findRotated and open
			obs.LogPrefixLost.Inc()
			f.retire(p)
			t.log.Warn("opening rotated segment", "path", path, "error", err)
			return nil, "", nil, false, true
		}
		// EACCES, EMFILE, EIO: the file is still there and may open next
		// sweep. The segment stays on the list — and the caller must report
		// the pass UNFINISHED, or segmentsFed strands it until an unrelated
		// rewind or a restart re-arms the replay, pinning an fd and a
		// checkpoint Pending entry that never clears.
		t.log.Warn("opening rotated segment", "path", path, "error", err)
		return nil, "", nil, false, false
	}
	return opened, path, func() { _ = opened.Close() }, true, false
}

// replaySegment re-reads one segment's owed [committed,to) range and feeds
// its lines into the pipeline under the segment's own id. It reports whether
// the range was finished this sweep; an unfinished one must be revisited.
func (t *Tailer) replaySegment(ctx context.Context, f *file, p *segment) bool {
	fh, path, closeFh, ok, retired := t.openSegmentSource(f, p)
	if !ok {
		// Finished only when the segment was given up on for good; a
		// transient open failure has to be retried.
		return retired
	}
	defer closeFh()
	// Resume at the FEED frontier, not the commit frontier: a budget-cut pass
	// left its lines in the pipeline, and re-reading them feeds duplicates
	// into groups that still buffer the originals. The DISCARD frontier
	// (skipTo) counts too: an oversized line's already-discarded prefix can
	// never produce a committing entry, and re-reading it re-discarded the
	// same bytes every pass — for a line whose newline is out of one pass's
	// reach, forever (see the segment field doc).
	from := max(p.committed, p.fedTo, p.skipTo)
	if _, err := fh.Seek(from, 0); err != nil {
		t.log.Warn("seeking rotated segment", "path", path, "error", err)
		return false // transient: retry next sweep
	}

	remaining := p.to - from
	if p.to < 0 {
		// Open-ended (a rotation that happened while the agent was DOWN: the
		// checkpoint knows the identity and the committed offset but not
		// where the rotated file ended). Read to EOF and pin `to` so the
		// segment can retire.
		remaining = 1 << 62
	}
	var carry []byte
	cur := from
	// fed is the last FED line boundary — the only offset commits can reach.
	// Deliberately NOT `from`: from may sit at skipTo, mid-discard, and the
	// open-ended completion below pins `to` from fed — a `to` inside a
	// discarded run is an offset no entry commits, wedging the segment.
	fed := max(p.committed, p.fedTo)
	var lastErr error
	// An over-cap line's remainder, dropped to its newline. Resumed from the
	// segment: a pass that ends mid-discard persists the state, or the next
	// pass would feed the oversized line's remainder as a fresh record.
	discarding := p.discarding
	buf := t.scratch()
	// Bounded like every other read loop. The open-ended case (a rotation that
	// happened while the agent was down) reads to EOF, so a large rotated
	// remainder built one enormous batch — ~100k entries against a BatchSize of
	// 1024 before the first size check — and starved the single sweep goroutine
	// for its whole duration. The budget stops the pass; the segment keeps its
	// committed progress and resumes on the next sweep.
	budget := int64(t.cfg.MaxBytesPerSweep)
	// A pass never stops MID-LINE without persisting where it stopped. `carry`
	// is a per-pass local, and the oversize escape fires at MaxEntryBytes+4096
	// — 1 MiB + 4 KiB against a 1 MiB default budget — so a single line at or
	// above the budget could never reach either the escape or a newline within
	// one pass: nothing was fed, `committed` could not advance, segmentsFed
	// stayed false, and the same megabyte was re-read every sweep forever,
	// pinning an fd and starving the sweep goroutine. Once the budget is spent
	// the loop keeps reading until that line progresses — either whole (fed,
	// fedTo advances) or by the discarded chunk the oversize escape just
	// dropped (skipTo advances) — and stops there, so the next pass resumes
	// past it instead of re-reading it.
	//
	// overrunFrom is the frontier the escape armed at, and it is what ENDS the
	// overrun. Re-deriving the escape from `len(carry) > 0` instead re-armed it
	// after every read whose 64 KiB boundary did not happen to fall on a
	// newline — essentially every read — so a pass that ran out of budget
	// mid-line went on to read the WHOLE owed range (up to a rotated kubelet
	// log's 10 MiB) in one go, with a synchronous export per BatchSize, on the
	// single sweep goroutine that serves every file on the node. The budget
	// bounded nothing in exactly the open-ended rotation-while-down case it was
	// written for.
	overrun := false
	var overrunFrom int64
	for remaining > 0 && (budget > 0 || overrun) {
		want := remaining
		if !overrun {
			want = min(want, budget)
		}
		n, rerr := fh.Read(buf[:min(int64(len(buf)), want)])
		if n > 0 {
			remaining -= int64(n)
			budget -= int64(n)
			carry = append(carry, buf[:n]...)
			for {
				i := bytes.IndexByte(carry, '\n')
				if i < 0 {
					// Bound the carried incomplete line exactly as consume
					// does: a checkpointed segment containing an oversized
					// line (whose live read was capped and discarded) must
					// not be slurped whole into memory on replay. The
					// remainder up to its newline is part of the same line.
					if len(carry) > t.cfg.MaxEntryBytes+oversizeSlack {
						cur += int64(len(carry))
						carry = carry[:0]
						// Counted once per LINE, exactly like consume's live
						// path: a line longer than the cap is discarded in as
						// many slabs as it has, and each pass of a replay that
						// resumes mid-discard would add another.
						if !discarding {
							obs.LogOversizedDropped.Inc()
						}
						discarding = true
						// Persist the discard progress on the segment BEFORE the
						// flush below (like fedTo): a failed flush rewinds and
						// ledger.reset zeroes it, and stamping afterwards would
						// resurrect a frontier the purge invalidated.
						p.skipTo, p.discarding = cur, true
					}
					break
				}
				line := carry[:i]
				start := cur
				cur += int64(i + 1)
				carry = carry[i+1:]
				if discarding {
					discarding = false // the newline ends the dropped line
					p.skipTo, p.discarding = cur, false
					continue
				}
				if len(line) > 0 {
					t.feedLine(ctx, f, string(line), start, cur)
					fed = cur
				}
			}
			// Advance the feed frontier BEFORE the flush below: a failed
			// flush rewinds and resets it (ledger.reset), and stamping it
			// afterwards would resurrect a frontier the purge invalidated.
			p.fedTo = fed
		}
		if rerr != nil {
			lastErr = rerr
			break
		}
		// Spent the budget mid-line: keep going until that line progresses,
		// then stop (see overrunFrom above).
		switch {
		case overrun:
			if max(fed, p.skipTo) > overrunFrom {
				overrun = false
			}
		case budget <= 0 && len(carry) > 0:
			overrun, overrunFrom = true, max(fed, p.skipTo)
		}
		// Ship what has accumulated rather than holding a whole rotated file
		// in one payload (which the collector would likely reject anyway).
		if t.maybeFlush(ctx, f) {
			// The flush FAILED and rewound the file: the pipeline was purged,
			// so every line this pass already fed is gone unemitted. Reading
			// on from the unrewound fd would leave that prefix owed while the
			// later lines' commits advanced `committed` past it — commitBatch
			// takes a max, not a contiguous frontier — and the segment would
			// eventually retire with the prefix never exported. Abandon the
			// pass; reporting it unfinished leaves segmentsFed false, so the
			// next sweep replays from what actually committed. drainReader has
			// carried the same guard all along.
			return false
		}
	}
	if budget <= 0 && remaining > 0 {
		// Out of budget with the range unfinished. p.committed is NOT advanced
		// here — it is commit progress, moved by commitBatch once the entries
		// actually export, so that a failed export still re-reads them. The
		// caller leaves segmentsFed false and the next sweep continues from
		// wherever the commits reached (re-feeding at most the uncommitted
		// prefix, which is the same at-least-once trade every other path
		// makes).
		return false
	}
	// The transient-error check comes FIRST, ahead of the open-ended
	// completion below. It used to come after, so a non-EOF failure part-way
	// through an OPEN-ENDED replay (to < 0, the rotation-while-down case) was
	// read as "reached EOF": the segment was pinned at whatever had been fed so
	// far — or retired outright when nothing had — and its unread remainder
	// became unrecoverable, with no obs.LogPrefixLost and no warning. That is
	// silent loss in the recovery path that exists precisely because nothing
	// else can recover those bytes.
	if lastErr != nil && !errors.Is(lastErr, io.EOF) {
		// A transient read error (EIO on a failing disk, a truncated NFS
		// handle): the range is still owed, so report the pass unfinished
		// rather than letting segmentsFed strand it.
		t.log.Warn("reading rotated segment", "path", path, "error", lastErr)
		return false
	}
	if p.to < 0 {
		// The open-ended replay reached EOF: pin the range so entry commits
		// can retire the segment. Only FED bytes count (a trailing fragment,
		// blank line or discarded oversize run can never produce a committing
		// entry).
		if fed > p.committed {
			p.to = fed
			p.fed = true // the whole pinned range is now live
		} else {
			f.retire(p) // nothing recoverable was fed
		}
		return true
	}
	if remaining > 0 && errors.Is(lastErr, io.EOF) {
		// The source ended before the owed range did: the rotated file was
		// truncated or shortened while the agent was down, or identity
		// matching landed on a shorter file. The missing tail is
		// unrecoverable — count it and clamp `to` to the fed boundary so the
		// segment retires through the normal commit path instead of wedging
		// forever below an offset no commit can ever reach (fd, checkpoint
		// Pending entry and the commit frontier all pinned). A transient read error
		// (lastErr not EOF) leaves the segment untouched for a retry.
		obs.LogPrefixLost.Inc()
		t.log.Warn("rotated segment shorter than its checkpointed range; missing tail lost",
			"path", path, "committed", p.committed, "to", p.to, "fed", fed)
		if fed > p.committed {
			p.to = fed
		} else {
			f.retire(p) // nothing recoverable at all
		}
	}
	// The owed range is covered: its lines are live, so an entry traversing
	// this segment genuinely reaches `to` and may claim it (see segment.fed).
	p.fed = true
	return true
}

// findRotated locates the rotated-away file matching p's identity in the log's
// resolved target directory (where the runtime keeps rotated files).
func (t *Tailer) findRotated(f *file, p *segment) (string, bool) {
	dir := f.targetDir
	if dir == "" {
		if target, err := filepath.EvalSymlinks(f.path); err == nil {
			dir = filepath.Dir(target)
		}
	}
	if dir == "" {
		return "", false
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return "", false
	}
	for _, de := range entries {
		full := filepath.Join(dir, de.Name())
		st, err := os.Stat(full)
		// The regularity check covers inode reuse by a non-file: opening a
		// FIFO to fingerprint it would block the sweep goroutine forever.
		if err != nil || !st.Mode().IsRegular() || inodeOf(st) != p.inode {
			continue
		}
		fh, err := os.Open(full)
		if err != nil {
			continue
		}
		match := p.fp.matches(fh)
		_ = fh.Close()
		if match {
			return full, true
		}
	}
	return "", false
}

// drainGone reads whatever the vanished file still holds into the batch. The
// fd stays OPEN: it is the only handle to the now-unlinked inode, so it must
// outlive a failed export — release only once the offsets commit.
//
// It takes the sweep's ctx: exports here used context.Background(), so the
// shutdown budget did not cover the final sweep's gone-file drain — a stuck
// collector could hold shutdown past it and cost the final saveCheckpoints.
func (t *Tailer) drainGone(ctx context.Context, f *file) {
	// Re-armed per cycle: chargeGoneStall reads it right after this cycle's
	// flush, and a verdict left over from an earlier cycle (or a rotation
	// drain) must not charge a cycle whose drain never ran.
	f.drainErred = false
	if !f.resolved {
		// Nothing was ever read (nothing is read before it can be attributed),
		// and with the file gone nothing can be: the content is lost. Make the
		// loss visible — a metadata-service outage overlapping pod deletions
		// silently eating final logs is exactly what an operator must see.
		// Count ONCE: drainGone re-runs every sweep until settledGone, and a
		// gone file can never resolve (the gone check precedes metadata
		// resolution), so re-counting here spammed the metric and the log ~2/s
		// per file forever.
		if f.unresolvedLost {
			return
		}
		f.unresolvedLost = true
		obs.LogUnresolvedLost.Inc()
		t.log.Warn("file deleted before its metadata resolved; content lost",
			"path", f.path, "containerID", f.containerID)
		// Checkpointed segments restored by initFile (created before metadata
		// could resolve) hold fds and checkpoint entries for content that is
		// now unattributable. Retire them as lost prefixes — without this
		// settledGone sees the segments and holds the file forever.
		for len(f.segments) > 0 {
			obs.LogPrefixLost.Inc()
			f.retire(f.segments[0])
		}
		return
	}
	if f.excluded {
		// The workload opted out: nothing was ever read (the sweep never
		// reads an excluded file) and nothing may be exported now that the
		// path is gone — feeding restored segments here would ship exactly
		// the backlog the exclusion refuses. dropExcludedBacklog retired them
		// at resolve time; the guard holds regardless of that ordering, as
		// the same intent-not-loss (no counter). goneEnd stays untouched, so
		// settledGone releases the entry on this sweep.
		t.dropExcludedBacklog(f)
		return
	}
	// Incomplete segments are OLDER than the current inode's remainder and
	// must enter the pipeline first. readFile normally feeds them, but a gone
	// file is never read again — without this, the prefixes' unexported lines
	// would be closed forever by release() once everything else settles (a pod
	// deleted during a collector outage after a rotation).
	t.feedSegments(ctx, f)
	if len(f.segments) > 0 && !f.segmentsFed {
		// Unfinished replay: draining the gone inode now would feed its
		// (newer) lines ahead of the segments' still-owed remainder — the
		// same out-of-order fuse readFile gates against. The fd is held and
		// drainGone re-runs every sweep until settledGone, so the drain
		// merely waits its turn.
		return
	}
	if f.compressed {
		// A large archive is read incrementally across sweeps; a deletion
		// mid-read leaves the rest readable from the open fd.
		t.drainArchive(ctx, f)
	} else {
		// An aborted drain (mid-drain flush failure) is fine here: drainGone
		// runs every sweep until settledGone, which stays false while the
		// rewound range is uncommitted.
		_ = t.drainFile(ctx, f)
	}
	if len(f.pending) > 0 {
		// An unterminated final line (a process killed mid-write) can never be
		// completed — the file is gone — and settledGone's pending check would
		// otherwise hold the fd and the files-map entry forever. Feed it
		// directly, ending at the REAL EOF: a synthetic terminator advanced
		// readPos past the file's true size, so every offset downstream named
		// a byte the file never had — a resurrected (listing-race) file of
		// unchanged size read as truncated and re-ingested whole, and a
		// checkpointed offset one past EOF did the same across a restart.
		// (consume already ran, so pending holds no newline: it IS one
		// unterminated line.)
		if f.discarding {
			// The tail of an oversized discarded line: not a record, so no
			// entry will ever commit its bytes. Record the boundary the
			// frontier may cross to (file.skipEnd) like every other never-fed
			// line, so the flush of this drain's own entries carries the
			// checkpoint over it.
			f.discarding = false
			f.skipEnd = f.readPos
		} else {
			t.feedLine(ctx, f, string(f.pending), f.lineStart, f.readPos)
		}
		f.pending = f.pending[:0]
		f.lineStart = f.readPos
	}
	t.stopPipeline(ctx, f)
	// The settle target is the FED boundary, not readPos: trailing consumed-
	// but-never-fed bytes (a blank final line, a rate-DROPPED or oversized-
	// discarded tail) can never produce a committing entry, and a goneEnd
	// covering them held the fd and the files-map entry forever (max keeps it
	// rewind-proof — a failed export must not lower an already-drained end).
	f.goneEnd = max(f.goneEnd, f.fedEnd())
}

// release closes the file's handles and watches. After this the inode is
// unreachable, so it must not be called while data read from it is still
// uncommitted.
func (t *Tailer) release(f *file) {
	if f.compressed {
		t.closeArchive(f)
	} else if f.f != nil {
		_ = f.f.Close()
		f.f = nil
	}
	f.closeSegments() // the file is going: its rotated inodes' fds go with it
	t.unwatchTarget(f)
}

// settledGone reports whether everything the vanished file held has been
// committed, so the file (and its unlinked inode) can be let go. It compares
// against the drained EOF, not readPos: a failed export rewinds readPos back
// to committed, which would otherwise look settled while the data is still
// unexported and reachable only through our fd.
func (t *Tailer) settledGone(f *file) bool {
	if len(f.segments) > 0 {
		// Incomplete segments still hold unexported lines whose only handles
		// are the retained fds release() would close; commitBatch retires
		// each segment once its range exports.
		return false
	}
	if _, buffered := f.watermark(); buffered {
		return false
	}
	return f.committed >= f.goneEnd && len(f.pending) == 0
}

// rewind seeks a file back to its committed offset so unexported data is
// read again. Pipeline state is discarded without emitting: the buffered
// lines sit after the committed offset and will be re-read and re-fed.
func (t *Tailer) rewind(f *file) {
	// Bump BEFORE any state changes so a loop that flushed mid-pass can see
	// that its own read position was purged under it (see replaySegment).
	f.rewindGen++
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
	// ledger.reset (via newPipeline) is what clears segmentsFed and re-arms it.
	if f.f != nil {
		if _, err := f.f.Seek(f.committed, 0); err != nil {
			_ = f.f.Close()
			f.f = nil // the next ensureOpen reopens and re-verifies identity
		}
	}
	f.restartAt(f.committed)
	t.newPipeline(f)
}
