package tailer

// Vanished files: draining what the unlinked inode still holds behind our fd,
// exporting it once per sweep, and releasing the file only once its offsets
// commit (or its drain has stalled for good).

import (
	"context"
	"time"

	"github.com/JohanLindvall/kubescrape/internal/obs"
)

// goneDrain is one vanished file drained this sweep, with the progress
// snapshot chargeGoneStall weighs the cycle against.
type goneDrain struct {
	path            string
	f               *file
	gen             int
	committedBefore int64
}

// settleGone exports what this sweep's gone drains read and releases each
// vanished file whose offsets have committed (or whose drain has stalled for
// good).
//
// ONE flush for all of them, never one per file. The flush used to sit inside
// the sweep's gone branch, so K pods deleted during a collector outage cost K
// full exportWithRetry cycles — three attempts and 1s+2s of backoff each, or
// three export timeouts apiece against a blackholed collector — on the single
// sweep goroutine, every sweep, since each failure rewinds the gone fd and the
// next sweep re-drains and re-fails. No other file was read and no rotation
// observed for that whole time, which widens exactly the "intermediate inode
// never opened" loss window the outage is already provoking. The per-cycle
// stall accounting is unchanged by batching them: a failed flush rewinds every
// file in its batch, so each one's rewindGen moves exactly as it did, and
// drainReader's BatchSize-bounded maybeFlush still caps the batch in between.
func (t *Tailer) settleGone(ctx context.Context) {
	if len(t.goneDrains) == 0 {
		return
	}
	t.flush(ctx)
	for _, g := range t.goneDrains {
		if t.settledGone(g.f) || t.chargeGoneStall(g.f, g.gen, g.committedBefore) {
			t.release(g.f)
			delete(t.files, g.path)
		}
	}
	clear(t.goneDrains)
	t.goneDrains = t.goneDrains[:0]
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
		// Nothing was ever read, so there is nothing to drain: the entry may
		// settle on this sweep.
		f.goneDrained = true
		if f.unresolvedLost {
			return
		}
		f.unresolvedLost = true
		obs.LogUnresolvedLost.Inc()
		t.log.Warn("file deleted before its metadata resolved; content lost",
			"path", f.path, "id", f.containerID)
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
		f.goneDrained = true
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
	var drained bool
	if f.compressed {
		// A large archive is read incrementally across sweeps; a deletion
		// mid-read leaves the rest readable from the open fd.
		drained = t.drainArchive(ctx, f)
	} else {
		drained = t.drainFile(ctx, f)
	}
	if !drained {
		// The drain did not reach the end of the inode — a mid-drain flush
		// failure rewound it, the per-drain cap fired on a deep backlog, or an
		// archive would not reopen. Stop the cycle here rather than falling
		// through to the settle accounting below: goneEnd is what releases the
		// file, and stamping it from a fed boundary the drain never reached
		// would settle the entry — closing the ONLY handle to the unlinked
		// inode — with its remainder unread and no counter moving. drainGone
		// re-runs every sweep until settledGone, so the next one continues
		// from where this fd stopped.
		//
		// Returning is not enough on its own, and it used to be all this did:
		// on a FIRST cycle goneEnd is still zero, so "committed >= goneEnd"
		// held after the rewind and settledGone released the fd anyway — every
		// line past the rewind lost, silently, for a pod deleted during a
		// collector outage (TestGoneFileMidDrainExportFailureDoesNotSettle).
		// goneDrained is what tells the gate that goneEnd means nothing yet; it
		// is stamped below, only by a cycle that reached the end.
		//
		// Not feeding pending here is deliberate too: with bytes still owed,
		// pending is a fragment with more of its line to come, not the
		// unterminated final line the block below exists for.
		return
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
			t.feedLine(ctx, f, string(f.pending), f.lineStart, f.readPos, time.Now())
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
	f.goneDrained = true
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
	stalled, spent := t.stallSpent(&f.goneStalledSince,
		f.rewindGen != gen || !f.drainErred || f.committed > progressBefore || (f.goneDrained && f.committed >= f.goneEnd))
	if !spent {
		return false
	}
	obs.LogPrefixLost.Inc()
	t.log.Error("a vanished file's drain has been erring without progress for too long; giving up on its unread remainder",
		"path", f.path, "committed", f.committed, "goneEnd", f.goneEnd, "stalled", stalled)
	return true
}

// settledGone reports whether everything the vanished file held has been
// committed, so the file (and its unlinked inode) can be let go. It compares
// against the drained EOF, not readPos: a failed export rewinds readPos back
// to committed, which would otherwise look settled while the data is still
// unexported and reachable only through our fd.
func (t *Tailer) settledGone(f *file) bool {
	if !f.goneDrained {
		// No drain has reached the inode's end yet, so goneEnd describes
		// nothing: the fd is the only route to what is still owed.
		return false
	}
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

// resurrect withdraws a gone verdict for a file whose path is proven alive
// again — a listing (or a stat in the gone branch) racing a rename+recreate
// rotation, or a rotated-away name taken by a new file. The three fields are
// ONE decision and must be cleared together, which is why both callers
// (scanDir's claimPath and sweep's gone branch) come through here: they were
// written twice and the discovery half already disagreed, leaving goneEnd set.
//
//   - goneEnd pinned the PREVIOUS incarnation's EOF. Left set, every later
//     completion check compares a fresh (usually shorter) stream's committed
//     offset against a stale, larger one: settledGone can never fire, so the
//     fd, the files-map entry and its checkpoint line are pinned for the
//     process lifetime with drainGone+flush re-running every sweep — or, if
//     the next deletion finds no handle to re-read from, drainArchive's
//     no-handle arm reports a lost remainder (obs.LogArchiveErrors plus a WARN
//     quoting committed from one stream and owedTo from another) for a file
//     whose every record was in fact delivered. A false loss alarm is
//     indistinguishable from the real one the same counter reports.
//   - The stall clock dies with the gone verdict: left set, a later gone
//     episode's first errored cycle reads the stale stamp as an already-spent
//     budget and gives up on sight (chargeGoneStall).
//
// What happens to the bytes is NOT decided here: readFile's rotation detection
// (or, for an archive, openArchive's identity check) owns the live path again.
func (f *file) resurrect() {
	f.gone = false
	f.goneEnd = 0
	f.goneDrained = false
	f.goneStalledSince = time.Time{}
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
