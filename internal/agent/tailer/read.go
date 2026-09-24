package tailer

// The incremental read path for live (non-archive) files: the per-sweep read
// loop, rotation classification, and open/identity verification. Metadata
// resolution is resolve.go's; the fingerprint itself is fingerprint.go's.

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"syscall"

	"github.com/JohanLindvall/kubescrape/internal/obs"
)

// swept reports whether every sweep runs this file through readFile's own
// os.Stat of the path — which is what lets discovery skip its stat (claimPath)
// and still notice a vanished path, one sweep sooner than the 2s discovery
// cadence rather than later.
//
// The three conditions are exactly readFile's: an OPEN fd means the file is
// resolved (nothing is read before it can be attributed) and not
// annotation-excluded (those are never opened), so the sweep reaches readFile;
// a live file always ends at the path stat, either the segment gate's or the
// post-read one. A compressed file takes readArchive, which short-circuits on
// archiveDone without statting, and a file already marked gone is handled ahead
// of readFile and needs a stat to be resurrected at all.
func (f *file) swept() bool { return f.f != nil && !f.compressed && !f.gone }

// readFile ingests up to MaxBytesPerSweep appended bytes and detects
// rotation.
func (t *Tailer) readFile(ctx context.Context, f *file) error {
	if f.compressed {
		return t.readArchive(ctx, f)
	}
	// An idle-closed file stays closed until the path shows evidence of
	// activity: the poll sweep runs every file through here each
	// PollInterval, and an unconditional ensureOpen reopened the fd (plus a
	// fingerprint read and a seek) only for closeIdleFiles to re-close it on
	// its own, coarser cadence — idle fds were open ~98% of steady state,
	// defeating -logs-idle-close. One stat of the path decides: the same
	// inode at the same size and mtime the close verified is still idle
	// (skip; a vanished path still errors into the gone handling, exactly as
	// ensureOpen's open would have). ANY deviation falls through to
	// ensureOpen, whose identity check tells an append (same inode: resume at
	// committed) from a replacement at the path (rotation while closed:
	// the replaced arm records the old incarnation as an open-ended segment
	// and recovers its remainder via findRotated) — the gate must never
	// bypass that arm, so it only ever skips the no-change case.
	if f.f == nil && f.idleClosed {
		st, err := os.Stat(f.path)
		if err != nil {
			return err
		}
		if inodeOf(st) == f.inode && st.Size() == f.readPos && st.ModTime().Equal(f.lastMod) {
			return nil
		}
	}
	if err := t.ensureOpen(f); err != nil {
		return err
	}
	// A group straddled a rename rotation and the pipeline was since discarded
	// (rewind or restart): re-read the rotated-away prefix before the new inode
	// so the group reconstructs.
	t.feedSegments(ctx, f)
	if len(f.segments) > 0 && !f.segmentsFed {
		// The replay is unfinished (per-sweep budget, a rewind, a transient
		// segment error): reading the tail now would feed lines NEWER than
		// the segments' still-owed remainder into the same pipeline keys,
		// and the joiner fuses fragments across that gap into records that
		// never existed in any file. Rotation of the tail is still detected
		// — the held fd must not go stale — but a renamed-away tail is
		// recorded UN-DRAINED as an open-ended segment (reopen's aborted
		// arm): draining would feed its lines out of order too, and the
		// segment machinery replays them in sequence once their turn comes.
		//
		// That is handleRotation with draining refused and nothing read this
		// pass (read = 0, readTo = readPos), which reduces its three arms to
		// exactly this gate's decisions — rename recorded un-drained,
		// truncation or same-size copytruncate restarting the tail (the
		// segments are unaffected: they live on their own inodes) — and names
		// the arm at Debug like every other rotation. The gate used to carry
		// its own copy of the classifier, without the reason lines.
		st, err := os.Stat(f.path)
		if err != nil {
			return err
		}
		t.handleRotation(ctx, f, st, 0, f.readPos, false)
		f.lastMod = st.ModTime()
		return nil
	}

	// Copytruncate whose replacement content is LONGER than our read offset:
	// the post-read check below cannot see it (bytes come back from the stale
	// offset, so read > 0 and its `read == 0` guard never fires) and we would
	// resume mid-way into the new file, silently skipping its prefix — and
	// splitting a line. The head fingerprint is the only witness, so re-verify
	// it here, BEFORE consuming anything, whenever the file changed on disk
	// since our last read.
	//
	// The stat is an fstat on the fd WE hold, never a stat of the path. Every
	// condition below only ever ACTS on our own inode — the branch it guards is
	// an in-place rewrite — so the path-stat's inode-equality test was
	// tautological here, and a rename rotation is the post-read stat's job
	// either way. Through the /var/log/containers symlink a path stat costs
	// ~6.9µs against ~2.4µs for the fstat, once per tracked file per sweep, and
	// the poll ticker sweeps every file.
	if f.readPos > 0 {
		if st, err := f.f.Stat(); err == nil &&
			st.Size() >= f.readPos &&
			!st.ModTime().Equal(f.lastMod) && !f.fp.matches(f.f) {
			// Named like handleRotation's arms: this is the same copytruncate,
			// caught before the read instead of after it, and the reason line
			// is the only thing that tells it from a clean rename.
			t.log.Debug("log file rotated", "path", f.path, "reason", "copytruncate",
				"inode", f.inode, "bytes", st.Size(), "readPos", f.readPos)
			t.reopen(ctx, f, false, true)
			f.lastMod = st.ModTime()
			if err := t.ensureOpen(f); err != nil {
				return err
			}
		}
	}

	// A paused (rate-limited) file first retries its retained pending bytes;
	// reading resumes only once they drain.
	if f.limited {
		if t.consume(ctx, f, false) {
			return nil // a batch flush failed and rewound; retry from committed
		}
	}
	budget := t.cfg.MaxBytesPerSweep
	buf := t.scratch()
	read := 0
	// Where this pass started reading: readFrom+read is how far into the file
	// the pass got, which a mid-read rewind hides from f.readPos (below).
	readFrom := f.readPos
	rewound := false
	for budget > 0 && !f.limited {
		limit := min(len(buf), budget)
		n, err := f.f.Read(buf[:limit])
		if n > 0 {
			budget -= n
			read += n
			if t.ingestChunk(ctx, f, buf[:n], false) {
				// A batch flush inside consume failed and rewound this file:
				// the fd is back at the committed offset and the pipeline is
				// purged, so reading on would re-feed the same bytes into a
				// batch whose export just failed. Stop reading, but still fall
				// through to the rotation check below — the fd must not be left
				// pointing at an inode the path no longer names, or a second
				// rotation inside the export outage loses the incarnation
				// between them.
				rewound = true
				break
			}
		}
		if err != nil {
			if !errors.Is(err, io.EOF) {
				return err
			}
			break
		}
	}

	if f.f == nil {
		// The only way to reach this with no fd is the break above: a mid-read
		// flush failed AND its rewind's Seek failed, so rewind dropped the
		// handle. The file then has no live identity to compare a rotation
		// against, and handleRotation's reopen would CLEAR the STORED
		// inode+fingerprint — after which the next ensureOpen can no longer
		// recognise a replacement at the path, and the rotated-away
		// incarnation's uncommitted range is lost uncounted. Leave the decision
		// to that ensureOpen, whose replaced arm records it as an open-ended
		// segment and recovers it through findRotated.
		return nil
	}

	// A first read on a file opened at size 0 (a fresh container log) leaves
	// fp.Len == 0, which matches ANYTHING — extend as soon as content exists,
	// not only on the checkpoint cadence (which never runs without a store).
	if read > 0 {
		t.extendFingerprint(f)
	}

	// Rotation/truncation detection.
	st, err := os.Stat(f.path)
	if err != nil {
		return err
	}
	// The truncation decision is taken against how far this pass READ, not
	// against f.readPos: a mid-read flush failure has already rewound readPos
	// to `committed`, and an in-place rewrite landing inside that failing
	// export with a size in [committed, readFrom+read) is then invisible to a
	// `size < readPos` test (and to the copytruncate arm, which needs read ==
	// 0). An unrewritten file is never smaller than bytes already read from it,
	// so the high-water is safe to compare against.
	t.handleRotation(ctx, f, st, read, readFrom+int64(read), true)
	if !rewound {
		// A rewound pass leaves lastMod UNSTAMPED. Stamping it consumed the
		// mtime change of a rewrite the size test above cannot see (same head
		// size or larger than what was read), and the next sweep's pre-read
		// fingerprint re-verify — the only other witness — is gated on the
		// mtime having moved, so the tailer resumed at `committed` mid-way into
		// the replacement: its prefix lost with no counter moving, and a torn
		// record exported whenever the line lengths differ. Unstamped, the next
		// pass re-verifies the head; an unchanged file costs one fingerprint
		// read, and only after an export failure.
		f.lastMod = st.ModTime()
	}
	return nil
}

// handleRotation classifies what happened to the file on disk since the last
// read — rename rotation (new inode at the path), in-place truncation, or a
// same-size copytruncate only the fingerprint can witness — and runs the
// matching recovery (no-op when the identity is unchanged).
//
// It used to report whether the rotation had been handled, so the caller could
// leave lastMod unstamped and retry. That contract is gone: an aborted drain no
// longer abandons the rotation (see the rename case below — reopen records the
// un-drained inode as an open-ended segment and opens the new incarnation
// either way), so there was nothing left to retry, the function had a single
// `return true`, and the caller's abort branch was unreachable code described
// by a comment that contradicted it. Returning nothing is the honest signature.
//
// readTo is the file offset the caller's read pass reached. It equals
// f.readPos unless a mid-read flush failure rewound the file, and the
// truncation arm must compare against the larger of the two (see readFile).
//
// drain is false only from readFile's unfinished-replay gate: with a segment
// replay still owed, a renamed-away tail must not be drained (its lines would
// enter the pipeline ahead of the segments' remainder), so the rename arm
// records it UN-DRAINED — reopen's aborted arm, an open-ended segment the
// replay reaches in its turn. The two truncation arms never drain either way.
func (t *Tailer) handleRotation(ctx context.Context, f *file, st os.FileInfo, read int, readTo int64, drain bool) {
	// Which ARM was taken, and on what evidence: kubescrape_log_rotations_total
	// counts all three together, so during an incident ("did logrotate
	// copytruncate under us and eat a window, or was this a clean rename?")
	// the counter cannot answer the question that decides whether data was
	// lost — a rename preserves the old inode's remainder as a segment, an
	// in-place truncation destroys it unmeasurably.
	//
	// The report is emitted from INSIDE the classifying switch rather than from
	// a reporting switch of its own, and that is load-bearing rather than tidy:
	// the third case's guard preads -logs-fingerprint-bytes off the head and
	// FNV-hashes them, so a second switch re-deriving the same predicate paid
	// that read twice per sweep per file — and slog evaluates its arguments
	// eagerly, so wrapping the report in an Enabled guard would have removed
	// the second read only for as long as nobody runs at Debug. Every argument
	// below is a field read, so the lines themselves cost nothing at Info.
	switch {
	case inodeOf(st) != f.inode:
		t.log.Debug("log file rotated", "path", f.path, "reason", "rename",
			"inode", f.inode, "newInode", inodeOf(st), "committed", f.committed, "readPos", f.readPos)
		// Rename rotation: the path names a new file. Drain what the old
		// writer appended after our last read (unless drain refuses it: an
		// unfinished segment replay), then switch — carrying a straddling
		// multi-line group across the boundary. An aborted or refused drain
		// (mid-drain flush failure rewound this fd) does NOT abandon the
		// rotation: reopen records the un-drained inode as an OPEN-ENDED
		// segment so feedSegments replays its remainder later, and the new
		// incarnation is opened below either way.
		//
		// Staying on the old inode instead — no fd, no segment, no record of
		// the file now at the path — lost the NEXT incarnation whole whenever a
		// second rotation landed inside the window, silently and uncounted. The
		// window is not one sweep: the abort only clears once an export
		// succeeds, so it is the entire export outage, which is exactly when
		// rotations pile up.
		drained := drain && t.drainFile(ctx, f)
		t.reopen(ctx, f, true, drained)
		// Open the NEW incarnation now, not on the next sweep. reopen clears
		// f.f/f.inode/f.fp, so until the file is opened again it has no fd and
		// no identity: a SECOND rotation inside that window finds nothing to
		// drain and records no segment, and the whole inode is unreachable
		// forever — silently and uncounted. The window is a full sweep, and it
		// widens to seconds whenever the sweep goroutine is blocked in a
		// failing export, which is exactly when rotations pile up. The two
		// truncation arms below and readFile's pre-read copytruncate
		// re-verify re-open for the same reason.
		if err := t.ensureOpen(f); err != nil {
			t.log.Debug("opening rotated-in file", "path", f.path, "error", err)
		}
	case st.Size() < max(f.readPos, readTo):
		t.log.Debug("log file rotated", "path", f.path, "reason", "truncated",
			"inode", f.inode, "bytes", st.Size(), "readPos", max(f.readPos, readTo))
		// In-place truncation: the unread tail is gone; restart at zero.
		// (Draining would read the replacement content mid-stream.)
		t.reopen(ctx, f, false, true)
		t.reopenTruncated(f)
	case read == 0 && !st.ModTime().Equal(f.lastMod) && !f.fp.matches(f.f):
		t.log.Debug("log file rotated", "path", f.path, "reason", "copytruncate",
			"inode", f.inode, "bytes", st.Size(), "readPos", f.readPos)
		// The file changed without yielding new bytes past our offset and
		// its head no longer matches: truncated and rewritten to a size at
		// or beyond our position (same-size copytruncate). Restart.
		t.reopen(ctx, f, false, true)
		t.reopenTruncated(f)
	}
}

// reopenTruncated opens the truncated file's new content right after a
// truncation arm's reopen, for the rename arm's reason: reopen clears
// f.f/f.inode/f.fp, and identityChanged needs a recorded inode, so a rename
// rotation landing before the next sweep would find nothing to drain and record
// no segment — everything the truncated inode held since would be lost with no
// counter and no log line.
func (t *Tailer) reopenTruncated(f *file) {
	if err := t.ensureOpen(f); err != nil {
		t.log.Debug("opening truncated file", "path", f.path, "error", err)
	}
}

// openRegular opens path read-only, refusing anything but a regular file.
// Discovery (claimPath) already skips non-regular files, but that guards the
// DISCOVERY stat only: a tracked path can be REPLACED by one (a FIFO taking a
// rotated log's name), and open(2) O_RDONLY on a writer-less FIFO blocks
// forever — on the single sweep goroutine, that is log collection stopping
// node-wide with /readyz still green and no counter moving. O_NONBLOCK makes
// the open itself non-blocking (a FIFO's read end opens immediately) and the
// fstat refuses the impostor; on a regular file the flag is inert — Linux
// reads never return EAGAIN there — so it is left set.
func openRegular(path string) (*os.File, os.FileInfo, error) {
	fh, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NONBLOCK, 0)
	if err != nil {
		return nil, nil, err
	}
	st, err := fh.Stat()
	if err != nil {
		_ = fh.Close()
		return nil, nil, err
	}
	if !st.Mode().IsRegular() {
		_ = fh.Close()
		return nil, nil, fmt.Errorf("%s: not a regular file", path)
	}
	return fh, st, nil
}

// ensureOpen opens the file at the committed offset on first use. The
// offset is only honored when the file's identity (inode and fingerprint)
// still matches; otherwise the path names a different file and reading
// starts from the top.
func (t *Tailer) ensureOpen(f *file) error {
	if f.f != nil {
		return nil
	}
	fh, st, err := openRegular(f.path)
	if err != nil {
		return err
	}
	inode := inodeOf(st)
	start := f.committed
	// A DIFFERENT file now lives at this path: -logs-idle-close released the
	// fd and the runtime rotated a replacement in while we held none.
	replaced := f.identityChanged(inode, fh)
	// The old incarnation's identity and progress, captured before the
	// assignments below adopt the new file's — the replaced arm records them.
	oldInode, oldFp, oldCommitted := f.inode, f.fp, f.committed
	// The SAME inode with a head that no longer matches is not a rename
	// rotation: the file was rewritten IN PLACE (copytruncate, a writer
	// reopening with O_TRUNC) while no fd was held. There is no rotated copy
	// under this inode to recover anything from — findRotated resolves a
	// segment by its inode, and the only file carrying it is the live one,
	// whose head no longer matches — so recording a segment only ever produced
	// a certain findRotated miss, i.e. a "rotated segment source not found"
	// loss report for a file that may have been fully caught up.
	inPlace := replaced && oldInode == inode
	if replaced {
		start = 0
	}
	// The file was TRUNCATED IN PLACE below our committed offset while we held
	// no fd: the same physical event as handleRotation's truncated arm, reached
	// through the other door (that one needs an fd and a read; this one is what
	// a restart before the first open, an -logs-idle-close release, or a rewind
	// whose Seek failed and dropped the handle sees). It restarts the file at
	// zero, so it is a NEW INCARNATION and takes the canonical reset below —
	// this arm used to do neither, reusing the tail id and keeping the withheld
	// highs live against it.
	truncated := !replaced && start > st.Size()
	if truncated {
		start = 0
	}
	// An idle close is allowed while an oversized line's discard window is
	// still OPEN (closeIdleFiles: `committed` sits at the line's start, readPos
	// far past it, pending empty — a writer that stalled mid-line must not pin
	// the fd forever). Reopening at `committed` and restarting would then
	// re-read the already-dropped prefix: the one physical line counted into
	// kubescrape_log_oversized_dropped_total twice, or counted dropped AND
	// exported as a truncated record when the re-read happens to meet its
	// newline before crossing the cap again. The identity was just re-verified
	// (same inode, same head, not shrunk below what was read), so
	// [committed, readPos) is the same dropped prefix: resume the window where
	// it stood instead.
	resumeDiscard := f.idleClosed && !replaced && !truncated && f.discarding &&
		len(f.pending) == 0 && f.readPos <= st.Size()
	seekTo := start
	if resumeDiscard {
		seekTo = f.readPos
	}
	if _, err := fh.Seek(seekTo, 0); err != nil {
		_ = fh.Close()
		return err
	}
	fp, err := computeFingerprint(fh, min(int64(t.cfg.FingerprintBytes), st.Size()))
	if err != nil {
		_ = fh.Close()
		return err
	}
	// Which door this reopen came through, before the flag is cleared: see the
	// in-place arm below.
	wasIdleClosed := f.idleClosed
	f.f = fh
	f.inode = inode
	f.fp = fp
	f.idleClosed = false // open again: the idle-close stat gate no longer applies
	hop := false
	switch {
	case inPlace && !f.compressed:
		// Counted and named like handleRotation's copytruncate arm, which is
		// the same physical event seen with an fd held: a rotation, and no
		// segment — the in-place rewrite destroyed whatever was unread.
		obs.LogRotations.Inc()
		t.log.Debug("log file rotated", "path", f.path, "reason", "copytruncate",
			"inode", inode, "bytes", st.Size(), "committed", oldCommitted)
		if !wasIdleClosed {
			// Through the RESTART door (and a rewind whose Seek dropped the
			// handle) a same-inode head mismatch is also exactly what a rename
			// rotation, a prune and INODE REUSE while the agent was down look
			// like — a genuine loss of the checkpointed remainder, which the
			// segment's findRotated miss used to count. Keep counting it, under
			// a line that names what was actually observed. Not on the
			// idle-close door: that file was fully caught up when its fd was
			// released, and inode reuse would need the rotated inode freed and
			// reused within one stat-gated sweep.
			obs.LogPrefixLost.Inc()
			t.log.Warn("log file was rewritten in place while no descriptor was held; "+
				"whatever it held past the committed offset is lost",
				"path", f.path, "inode", inode, "committed", oldCommitted)
		}
	case replaced && !f.compressed:
		// The OLD incarnation rotated away while we held no fd — between
		// -logs-idle-close releasing it and this reopen, or between a
		// restart's initFile (whose stat still saw the old inode) and the
		// file's first open, a window that widens to MINUTES when metadata
		// resolution is backing off. Its [committed, EOF) was never read and
		// the rotated file is the only copy: record it as an open-ended
		// segment (to = -1, the shape discover.go synthesizes for a
		// rotation-while-down) so feedSegments recovers it via findRotated —
		// or counts obs.LogPrefixLost and retires it if the runtime already
		// pruned the file. Previously the remainder was discarded silently,
		// with every loss counter flat. Not for archives: their offsets are
		// in decompressed space and archiveReplaced owns that decision.
		//
		// Counted like every sibling rotation arm (reopen, the truncated arm
		// below): kubescrape_log_rotations_total says "rotations and
		// truncations handled", and this one is handled. It cannot double-count
		// a rotation reopen already counted — reopen zeroes f.inode, and
		// identityChanged needs a recorded one.
		obs.LogRotations.Inc()
		// The one rotation shape no counter distinguishes: it happened
		// while this process held NO fd, so nothing observed it and the
		// only evidence is the identity mismatch found here. Whether its
		// remainder is recovered is decided later by feedSegments (and
		// counted obs.LogPrefixLost if it is not), so this line is what
		// says the recovery was even attempted, and from where.
		t.log.Debug("log file was replaced while no descriptor was held; recording its unread remainder for replay",
			"path", f.path, "inode", oldInode, "newInode", inode, "committed", oldCommitted)
		f.segments = append(f.segments, &segment{
			id: f.tail, inode: oldInode, fp: oldFp, committed: oldCommitted, to: -1, fed: false,
		})
		hop = true
	}
	if truncated {
		// Counted and named like every other rotation. This arm discarded a
		// whole committed prefix in SILENCE — kubescrape_log_rotations_total,
		// kubescrape_log_prefix_lost_total and every log line flat — leaving an
		// operator watching a file restart from zero with nothing anywhere that
		// says why. Nothing is LOST (the discarded prefix [size, committed) had
		// already exported), so no loss counter moves, exactly as in
		// handleRotation's truncated arm.
		obs.LogRotations.Inc()
		t.log.Debug("log file rotated", "path", f.path, "reason", "truncated",
			"inode", inode, "bytes", st.Size(), "committed", oldCommitted)
	}
	switch {
	case replaced || truncated:
		// A new incarnation, so take the canonical path rather than resetting
		// byte positions inline. Keeping the OLD tail id attributed the new
		// inode's bytes to the previous incarnation's segment, and a withheld
		// exportedHigh from that incarnation was later re-offered and applied
		// here — advancing `committed` past bytes this file never read. The
		// inline reset also skipped restartAt, so a rate-limit pause or an
		// oversized-line discard window survived into a file that has nothing
		// to do with them. (start is 0 on both arms.)
		f.exportedHighs = nil
		f.beginIncarnation(start)
		t.newPipeline(f) // fresh stages; its purge re-arms the segment replay
	case !resumeDiscard:
		f.restartAt(start)
		f.committed = start
	}
	if hop {
		// Only now: the forced save inside noteHop writes exactly what it sees,
		// and before the reset above that was the NEW inode paired with the OLD
		// incarnation's committed offset — a crash before the sweep's closing
		// save then resumed the replacement mid-file, skipping its prefix and
		// exporting a torn fragment.
		t.noteHop(f)
	}
	t.watchTarget(f)
	return nil
}
