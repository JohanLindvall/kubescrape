package tailer

// Rotation, rewind and recovery: closing the tail into segments, replaying
// incomplete segments (live and after a restart), draining vanished files,
// and releasing settled ones.

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"

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
// drainReader reads r to EOF into the pipeline. It reports false when a
// mid-drain flush FAILED and rewound this file: the rewind seeks the very fd
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
			// Bypass the rate limit: pausing a drain would lose the remainder
			// when the fd is dropped.
			t.ingestChunk(ctx, f, buf[:n], true)
			before := f.readPos
			t.flushDuringDrain(ctx)
			if f.readPos < before {
				return false // flush failed and rewound the drained fd
			}
		}
		if err != nil {
			return true
		}
	}
	t.log.Error("source still yielding after draining 1GiB, abandoning remainder", "path", f.path, "source", what)
	return true
}

// flushDuringDrain keeps a large drain from accumulating everything into one
// batch (and one OTLP payload, likely over the collector's receive limit) and
// from starving the sweep for the drain's whole duration.
func (t *Tailer) flushDuringDrain(ctx context.Context) {
	if len(t.batch) >= t.cfg.BatchSize {
		t.flush(ctx)
	}
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
func (t *Tailer) reopen(ctx context.Context, f *file, renamed bool) {
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
			// re-feed, and crash-restart (feedPrefix feeds only terminated
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
	fedEnd := f.committed
	for _, st := range f.streams {
		if st.lastEnd.seg == f.tail && st.lastEnd.off > fedEnd {
			fedEnd = st.lastEnd.off
		}
	}
	// Whether every PRE-EXISTING segment's owed lines are live (pipeline or
	// batch) — captured before this rotation appends its own hop. With no
	// prior segments the answer is vacuously yes (segmentsFed is only
	// meaningful while segments exist).
	wasFed := f.segmentsFed || len(f.segments) == 0
	hopAdded := false
	if renamed && fedEnd > f.committed {
		// Close the tail into a segment: its uncommitted range [committed,
		// fedEnd) is owed. If a group is still buffered the pipeline is
		// carried below and the segment's items keep their (old-segment)
		// positions unchanged; either way, if the export of the drained
		// entries fails (or the process crashes) the rotated-away file is the
		// only copy, and the segment record is what lets feedSegments re-read
		// it. It retires in commitBatch once its whole range commits.
		f.segments = append(f.segments, keep(&segment{
			id: f.tail, inode: f.inode, fp: f.fp, committed: f.committed, to: fedEnd,
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
		t.stopPipeline(ctx, f)
		t.newPipeline(f)
		// The segment list is NOT reset here: earlier segments' lines are
		// still uncommitted, and a second rotation (or a truncation) during a
		// collector outage does not make them recoverable any other way.
		// Segments retire individually in commitBatch.
	}
	// newPipeline's reset cleared segmentsFed; that reset exists for REWINDS
	// (where the batch was purged). A rotation purges nothing: entries built
	// from fed segments are still in the unflushed batch, and re-feeding them
	// would duplicate every one of those records on a plain truncation.
	f.segmentsFed = wasFed
	f.newTail()
	f.inode = 0
	f.fp = fingerprint{}
	f.committed = 0
	f.restartAt(0)
	// The next ensureOpen's watchTarget re-derives the symlink target and
	// switches watches acquire-before-release, so no eager unwatch here — an
	// unwatched hole between reopen and that sweep would lose a second
	// rotation happening inside one poll interval.
	f.dirty = true
	if hopAdded {
		// Persist the hop NOW, not on the 10s checkpoint cadence: a crash in
		// the window would leave the on-disk checkpoint with no Pending entry,
		// and the restart path has no other route back to the rotated inode —
		// the tail would be lost outright, not merely re-read. (Discovery
		// already persists immediately for the same reason.)
		t.saveCheckpoints()
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
	f.segmentsFed = true
	for _, sg := range f.segments {
		f.feeding = sg.id
		t.replaySegment(ctx, f, sg)
	}
	f.feeding = 0
}

// openSegmentSource resolves the readable handle for a segment's replay: the
// retained fd first (it reaches the inode even after the runtime has deleted
// or compressed the rotated file, which findRotated — resolving by NAME —
// cannot; only a restart, where no fd survives, falls back to the path). A
// segment whose source is genuinely gone is counted (obs.LogPrefixLost) AND
// retired — an unrecoverable segment kept on the list can never reach its
// `to` and would wedge retirement (fd budget, settledGone, the checkpoint)
// forever.
func (t *Tailer) openSegmentSource(f *file, p *segment) (fh *os.File, path string, closeFh func(), ok bool) {
	if p.fd != nil {
		return p.fd, f.path, func() {}, true
	}
	path, found := t.findRotated(f, p)
	if !found {
		obs.LogPrefixLost.Inc()
		t.log.Warn("rotated segment source not found; its lines are lost",
			"path", f.path, "inode", p.inode)
		f.retire(p)
		return nil, "", nil, false
	}
	opened, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) { // pruned between findRotated and open
			obs.LogPrefixLost.Inc()
			f.retire(p)
		}
		t.log.Warn("opening rotated segment", "path", path, "error", err)
		return nil, "", nil, false
	}
	return opened, path, func() { _ = opened.Close() }, true
}

// replaySegment re-reads one segment's owed [committed,to) range and feeds
// its lines into the pipeline under the segment's own id.
func (t *Tailer) replaySegment(ctx context.Context, f *file, p *segment) {
	fh, path, closeFh, ok := t.openSegmentSource(f, p)
	if !ok {
		return
	}
	defer closeFh()
	if _, err := fh.Seek(p.committed, 0); err != nil {
		t.log.Warn("seeking rotated segment", "path", path, "error", err)
		return
	}

	remaining := p.to - p.committed
	if p.to < 0 {
		// Open-ended (a rotation that happened while the agent was DOWN: the
		// checkpoint knows the identity and the committed offset but not
		// where the rotated file ended). Read to EOF and pin `to` so the
		// segment can retire.
		remaining = 1 << 62
	}
	var carry []byte
	cur := p.committed
	discarding := false // an over-cap line's remainder, dropped to its newline
	buf := t.scratch()
	for remaining > 0 {
		n, rerr := fh.Read(buf[:min(int64(len(buf)), remaining)])
		if n > 0 {
			remaining -= int64(n)
			carry = append(carry, buf[:n]...)
			for {
				i := bytes.IndexByte(carry, '\n')
				if i < 0 {
					// Bound the carried incomplete line exactly as consume
					// does: a checkpointed segment containing an oversized
					// line (whose live read was capped and discarded) must
					// not be slurped whole into memory on replay. The
					// remainder up to its newline is part of the same line.
					if len(carry) > t.cfg.MaxEntryBytes+4096 {
						cur += int64(len(carry))
						carry = carry[:0]
						discarding = true
						obs.LogOversizedDropped.Inc()
					}
					break
				}
				line := carry[:i]
				start := cur
				cur += int64(i + 1)
				carry = carry[i+1:]
				if discarding {
					discarding = false // the newline ends the dropped line
					continue
				}
				if len(line) > 0 {
					t.feedLine(ctx, f, string(line), start, cur)
				}
			}
		}
		if rerr != nil {
			break
		}
	}
	if p.to < 0 {
		// The open-ended replay reached EOF: pin the range so entry commits
		// can retire the segment. Only FED bytes count (a trailing fragment
		// or blank line can never produce a committing entry) — cur is the
		// last fed line boundary at this point.
		if cur > p.committed {
			p.to = cur
		} else {
			f.retire(p) // nothing recoverable was fed
		}
	}
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
		if err != nil || inodeOf(st) != p.inode {
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
func (t *Tailer) drainGone(f *file) {
	if !f.resolved {
		// Nothing was ever read (nothing is read before it can be attributed),
		// and with the file gone nothing can be: the content is lost. Make the
		// loss visible — a metadata-service outage overlapping pod deletions
		// silently eating final logs is exactly what an operator must see.
		obs.LogUnresolvedLost.Inc()
		t.log.Warn("file deleted before its metadata resolved; content lost",
			"path", f.path, "containerID", f.containerID)
		return
	}
	// Incomplete segments are OLDER than the current inode's remainder and
	// must enter the pipeline first. readFile normally feeds them, but a gone
	// file is never read again — without this, the prefixes' unexported lines
	// would be closed forever by release() once everything else settles (a pod
	// deleted during a collector outage after a rotation).
	t.feedSegments(context.Background(), f)
	if f.compressed {
		// A large archive is read incrementally across sweeps; a deletion
		// mid-read leaves the rest readable from the open fd.
		t.drainArchive(context.Background(), f)
	} else {
		// An aborted drain (mid-drain flush failure) is fine here: drainGone
		// runs every sweep until settledGone, which stays false while the
		// rewound range is uncommitted.
		_ = t.drainFile(context.Background(), f)
	}
	if len(f.pending) > 0 {
		// An unterminated final line (a process killed mid-write) can never be
		// completed — the file is gone. Without flushing it here, settledGone's
		// pending check would hold the fd and the files-map entry forever and
		// the tail would never be delivered. The synthetic terminator advances
		// readPos with it so the commit reaches goneEnd.
		f.pending = append(f.pending, '\n')
		f.readPos++
		t.consume(context.Background(), f, true)
	}
	t.stopPipeline(context.Background(), f)
	f.goneEnd = max(f.goneEnd, f.readPos) // the inode's true end, rewind-proof
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

// --- checkpoints ---
