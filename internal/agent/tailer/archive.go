package tailer

// The one-shot compressed (gzip) archive path: bounded incremental reads
// in decompressed-offset space, in-place-rewrite detection, and fd
// retention for uncommitted archives.

import (
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/JohanLindvall/kubescrape/internal/obs"
)

// readArchive reads a gzip archive: bounded decompressed bytes per sweep, fed
// through the same pipeline (offsets are decompressed positions). gzip is not
// seekable, so on resume openArchive re-decompresses from the start and
// discards the committed prefix; at EOF the pipeline is drained and the file
// is done (committed == readPos means a restart discards everything).
func (t *Tailer) readArchive(ctx context.Context, f *file) error {
	if t.archiveReplaced(f) {
		// Content replaced under us: the old bytes are gone from the inode, so
		// (exactly as with a plain-file truncation) nothing about them is
		// recoverable and joining across the boundary is meaningless. Discard
		// the pipeline and restart the file from zero.
		obs.LogRotations.Inc()
		// The restart abandons whatever of the old stream had not been read
		// (and, after a rewind, what had been read and re-owed): decompressed
		// offsets mean nothing in the new content, and the inode no longer
		// holds the bytes. archiveEOF is the in-process witness that there was
		// nothing left — only a rewind clears it, and a stream read to EOF has
		// every byte fed, in the batch and on its way out. Reaching here
		// WITHOUT it is a real loss and must be counted like the sibling arm in
		// openArchive: LogRotations moves for every ordinary rotation of every
		// file on the node, so on its own it left an operator no way to learn
		// the lines had existed.
		if !f.archiveEOF {
			obs.LogPrefixLost.Inc()
			t.log.Warn("archive rewritten in place with its content not fully shipped; the old stream's remainder is lost",
				"path", f.path, "committed", f.committed, "read", f.readPos)
		}
		// As in reopen: pause-retained complete pending lines were read from
		// the OLD stream and are deliverable; feed them before the pipeline is
		// discarded.
		if len(f.pending) > 0 {
			t.consume(ctx, f, true)
		}
		t.stopPipeline(ctx, f)
		t.closeArchive(f)
		f.committed = 0
		f.inode, f.fp = 0, fingerprint{}
		f.archiveDone, f.archiveEOF = false, false
		// A fresh tail id makes batched entries with OLD-stream positions
		// resolve to nothing at commit: the replaced content is gone and its
		// id is dead.
		f.newTail()
		f.restartAt(0)
		t.newPipeline(f)
	}
	// A paused (rate-limited) file first retries its retained pending bytes —
	// and must do so BEFORE the archiveDone short-circuit below: an archive
	// can hit EOF with its tail still pending (tokens exhausted mid-consume),
	// and the done path would otherwise return early on every sweep, wedging
	// those lines forever while the tokens sit refilled and unconsulted.
	if f.limited {
		if t.consume(ctx, f, false) {
			// A batch flush failed and rewound: rewind dropped the gzip reader
			// (f.gz is nil) and re-armed the range, so the next sweep
			// re-decompresses from the committed offset.
			return nil
		}
	}
	if f.gz == nil {
		// An fd retained for recovery (see closeArchiveReader) can go once its
		// data has been exported; otherwise every consumed archive would leak
		// one. Gated on archiveEOF, not on offsets alone: a rewind leaves
		// readPos == committed, which would look "delivered" while the data is
		// in fact still owed. The offset compared is fedEnd, never readPos:
		// trailing decompressed bytes that never feed AND never will — a final
		// blank line, a rate-DROPPED or oversize-DISCARDED tail — can never
		// produce a committing entry, and a readPos gate pinned one fd per
		// such archive for the file's life. An UNTERMINATED final fragment is
		// the deliberate exception (the pending gate): deletion turns it into
		// a record (drainGone feeds it), and releasing the fd made that record
		// unrecoverable — the gone drain's flush fails transiently, the rewind
		// discards the pipeline, and with the inode unlinked and no fd there
		// is nothing left to re-read, so the tail was lost with no counter
		// moving while settledGone pinned the entry forever. One held fd per
		// unterminated-tail archive is the price of that record. Runs whether
		// or not the archive is marked done — when the path was replaced under
		// us archiveDone was deliberately left false, and this is what lets
		// the next openArchive see the replacement.
		if f.f != nil && f.archiveEOF && f.committed >= f.fedEnd() && len(f.pending) == 0 {
			t.closeArchive(f)
		}
		if f.archiveEOF && f.f != nil {
			// Read to EOF with the batch not yet committed (the retained fd is
			// the proof — the gate above releases it once everything commits).
			// The data is fed and merely awaiting its flush: reopening here
			// re-decompressed from `committed` and RE-FED lines already
			// sitting in the batch, once per sweep until the flush interval
			// landed — several duplicate copies delivered in one export. Only
			// a rewind (which clears archiveEOF: the range is owed again)
			// re-arms reading.
			return nil
		}
		// A fully-consumed archive would otherwise be reopened and
		// re-decompressed end-to-end on every poll sweep, forever; skip while
		// the compressed file itself is unchanged.
		if f.archiveDone {
			st, err := os.Stat(f.path)
			if err != nil || (st.Size() == f.archiveSize && st.ModTime().Equal(f.archiveMod)) {
				return nil // unchanged (or momentarily unstattable): stay done
			}
			// Changed on disk: re-open and let openArchive's inode+fingerprint
			// identity check decide whether committed must reset — a bare
			// append or touch must not trigger a full duplicate re-ingest.
			f.archiveDone = false
		}
		if err := t.openArchive(f); err != nil {
			return err
		}
	}
	budget := t.cfg.MaxBytesPerSweep
	buf := t.scratch()
	for budget > 0 && !f.limited {
		n, err := f.gz.Read(buf[:min(len(buf), budget)])
		if n > 0 {
			budget -= n
			if t.ingestChunk(ctx, f, buf[:n], false) {
				// A batch flush inside consume failed and rewound the archive:
				// rewind CLOSES the gzip reader, so the loop's next f.gz.Read
				// would dereference nil and take the sweep goroutine — and with
				// it log collection for the whole node — down. The retained fd
				// re-decompresses from committed on the next sweep.
				return nil
			}
		}
		if err != nil {
			if errors.Is(err, io.EOF) {
				t.finishArchive(ctx, f)
				return nil
			}
			// A corrupt stream (truncated gzip tail, trailing garbage) is
			// unrecoverable: the retained reader would return the same error
			// every sweep forever — fd and reader held, nothing progressing,
			// nothing reported. Deliver what decoded, count the loss, and
			// settle under the current identity (finishArchive), so a later
			// rewrite of the file is still detected and re-read.
			obs.LogArchiveErrors.Inc()
			t.log.Warn("archive decode failed; remaining content lost",
				"path", f.path, "offset", f.readPos, "error", err)
			t.finishArchive(ctx, f)
			return nil
		}
	}
	return nil // hit the sweep budget; continue next sweep with f.gz retained
}

// finishArchive settles an archive whose decompressed stream just hit EOF:
// drain any buffered multi-line group, release or retain the fd depending on
// whether everything committed, and mark the archive consumed under its
// current identity.
func (t *Tailer) finishArchive(ctx context.Context, f *file) {
	t.stopPipeline(ctx, f) // drain a trailing multi-line group
	f.archiveEOF = true
	if f.committed >= f.readPos {
		t.closeArchive(f)
	} else {
		// Uncommitted data: hold the fd (see closeArchiveReader).
		t.closeArchiveReader(f)
	}
	// Record the consumed archive's identity so idle sweeps skip it instead
	// of re-decompressing it from scratch — but ONLY if the path still names
	// the inode we just read. When we finished a RETAINED fd (its data was
	// uncommitted) and a replacement archive has since taken the path,
	// stamping the replacement's size/mtime here would mark IT consumed and
	// its lines would never be read. Leaving archiveDone false makes the next
	// sweep open the path fresh, where the identity check resets committed.
	if st, statErr := os.Stat(f.path); statErr == nil && inodeOf(st) == f.inode {
		f.archiveDone = true
		f.archiveSize = st.Size()
		f.archiveMod = st.ModTime()
	}
}

// gzipAt opens a gzip reader over fh positioned past the first `skip`
// decompressed bytes (gzip is not seekable, so positioning is decode-and-
// discard). A header error is wrapped with the path; a mid-stream discard
// error is returned as-is — the two openArchive branches shared this block
// verbatim.
func gzipAt(fh *os.File, path string, skip int64) (*gzip.Reader, error) {
	gz, err := gzip.NewReader(fh)
	if err != nil {
		return nil, fmt.Errorf("gzip %s: %w", path, err)
	}
	if skip > 0 {
		if _, err := io.CopyN(io.Discard, gz, skip); err != nil && !errors.Is(err, io.EOF) {
			_ = gz.Close()
			return nil, err
		}
	}
	return gz, nil
}

// isGzipHeaderErr reports whether err is the wrapped not-a-gzip-file open
// error (vs a mid-stream read failure).
func isGzipHeaderErr(err error) bool {
	return errors.Is(err, gzip.ErrHeader)
}

// openArchive opens the gzip file and positions it at the committed offset by
// discarding that many decompressed bytes.
func (t *Tailer) openArchive(f *file) error {
	// A retained fd (uncommitted data, reader closed at EOF or by rewind) IS the
	// archive — reuse it rather than re-opening by path: the file may already be
	// unlinked, and that fd is then the only handle to the inode. gzip is not
	// seekable, so rewind the fd itself and re-decompress from the start.
	if f.f != nil {
		if _, err := f.f.Seek(0, 0); err != nil {
			return err
		}
		gz, err := gzipAt(f.f, f.path, f.committed)
		if err != nil {
			return err
		}
		f.gz = gz
		// restartAt also clears `limited`: the wiped pending is re-read from
		// committed and re-metered by allowLine. Leaving the flag set with
		// pending empty wedges the file forever (nothing else clears it).
		f.restartAt(f.committed)
		if st, err := os.Stat(f.path); err == nil {
			f.archiveSize, f.archiveMod = st.Size(), st.ModTime()
		}
		return nil
	}
	// openRegular, not os.Open: an open(2) on a FIFO that took the archive's
	// name would block the sweep goroutine forever (see openRegular).
	fh, st, err := openRegular(f.path)
	if err != nil {
		return err
	}
	inode := inodeOf(st)
	// A replaced file (different inode or head fingerprint) restarts at zero.
	if f.identityChanged(inode, fh) {
		// The reset below abandons the old stream's [committed, end)
		// remainder — its offsets are decompressed positions that mean
		// nothing in the replacement — so it is a loss unless everything had
		// shipped. In-process that is knowable: a consumed archive still
		// carries archiveEOF (only a rewind, which retains the fd and routes
		// the reopen through the reuse branch above, clears it before this
		// open), so reaching here with it set means every fed byte committed.
		// Across a restart it is not knowable — exactly like the plain path's
		// rotation-while-down synthesis, whose findRotated miss counts
		// obs.LogPrefixLost without knowing whether the vanished remainder
		// was empty. Count it the same way; first discovery (no recorded
		// identity) never takes this arm.
		if !f.archiveEOF {
			obs.LogPrefixLost.Inc()
			t.log.Warn("archive replaced with its content not fully shipped; the old stream's remainder is lost",
				"path", f.path, "committed", f.committed)
		}
		f.committed = 0
		// The old identity is spent with the reset. Kept, it re-fired this
		// arm — and the count — on the NEXT replacement whenever this open
		// fails below (the non-gzip quarantine returns before the new
		// identity is adopted).
		f.inode, f.fp = 0, fingerprint{}
	}
	gz, err := gzipAt(fh, f.path, f.committed)
	if err != nil {
		_ = fh.Close()
		if isGzipHeaderErr(err) {
			// Quarantine under the file's current identity: a stray non-gzip
			// file matched by a compressed source would otherwise be re-opened
			// and warn-logged EVERY sweep forever. archiveDone's stat
			// short-circuit skips it until the content changes, which clears
			// the mark and retries (a rewritten-valid archive recovers on its
			// own).
			f.archiveDone = true
			f.archiveSize, f.archiveMod = st.Size(), st.ModTime()
		}
		return err
	}
	if fp, err := computeFingerprint(fh, min(int64(t.cfg.FingerprintBytes), st.Size())); err == nil {
		f.fp = fp
	}
	f.f = fh
	f.gz = gz
	f.inode = inode
	f.restartAt(f.committed)
	f.archiveEOF = false // a retained EOF mark must not outlive a fresh open
	f.archiveSize, f.archiveMod = st.Size(), st.ModTime()
	t.watchTarget(f)
	return nil
}

// archiveReplaced reports whether the archive we hold open has been REWRITTEN
// IN PLACE (same inode, new content: `gzip -c > x.gz`, os.WriteFile). The
// compressed path has no equivalent of the plain path's rotation detection, and
// its offsets are DECOMPRESSED positions — so without this, a rewrite makes the
// reader either resume at a stale offset in the new stream (skipping its prefix
// and splitting a line) or trip over a corrupt stream mid-member.
//
// It is deliberately keyed on the fingerprint of the fd WE hold, not of the
// path: when the old archive was unlinked and a different file took its name,
// our fd still sees the original, intact content — that data is still owed and
// must not be discarded (openArchive keeps reading it; the EOF path declines to
// mark the replacement consumed).
func (t *Tailer) archiveReplaced(f *file) bool {
	if f.f == nil {
		return false
	}
	st, err := os.Stat(f.path)
	if err != nil {
		return false // vanished: the gone path drains it from the fd
	}
	if st.Size() == f.archiveSize && st.ModTime().Equal(f.archiveMod) {
		return false // untouched since we opened it
	}
	return !f.fp.matches(f.f)
}

// drainArchive finishes reading a mid-read archive from its still-open fd
// (the file may already be unlinked) so its remainder is not lost when the
// file drops. It reports whether the drain reached the end of what the fd can
// yield — drainFile's contract, and read by drainGone the same way: a cycle
// that stopped early (a mid-drain flush failure, the per-drain cap, a reopen
// that failed) must not settle the file.
func (t *Tailer) drainArchive(ctx context.Context, f *file) bool {
	if f.gz == nil {
		// Reader closed at EOF or by a rewind, but the data is not committed and
		// the fd is still held: re-decompress the uncommitted suffix from it.
		// At EOF there is nothing left to drain — everything is fed and merely
		// awaiting its flush, and re-decompressing here re-fed lines already
		// in the batch (only a rewind, which clears archiveEOF, re-owes them).
		if f.f == nil {
			// No handle: nothing can be re-read. Normally nothing is owed
			// either — the release gate keeps the fd while anything fed is
			// uncommitted or an unterminated fragment sits in pending — but if
			// an owed range survives here anyway (a handle lost some other
			// way), returning silently every sweep is a wedge: goneEnd stays
			// above committed, settledGone never fires, and the entry (files
			// map, checkpoint, gone-sweep) is pinned forever with no counter
			// moving. The range is unrecoverable, so count it and settle,
			// like the corrupt-stream arm: reported loss, never a wedged
			// file. Pending bytes are deliverable from memory; drainGone
			// feeds them right after this.
			if f.goneEnd > f.committed {
				obs.LogArchiveErrors.Inc()
				t.log.Warn("archive gone with uncommitted data and no retained fd; remainder lost",
					"path", f.path, "committed", f.committed, "owedTo", f.goneEnd)
				f.goneEnd = f.committed
			}
			return true // nothing left that a drain could reach: settle
		}
		if f.archiveEOF || f.committed >= f.readPos && f.archiveDone {
			return true // everything fed; only its flush is pending
		}
		if err := t.openArchive(f); err != nil {
			// A failed reopen is a drain that ended in an error without
			// reading anything: a corrupt committed prefix fails the CopyN
			// discard identically every sweep, which is the same
			// unreachable-goneEnd wedge chargeGoneStall bounds.
			f.drainErred = true
			t.log.Warn("re-opening archive to drain", "path", f.path, "error", err)
			return false
		}
	}
	// An aborted drain (mid-drain flush failure) retries on a later sweep:
	// the gone-file loop calls drainGone every sweep until settledGone.
	return t.drainReader(ctx, f, f.gz, "archive")
}

// closeArchive releases the archive's readers.
func (t *Tailer) closeArchive(f *file) {
	t.closeArchiveReader(f)
	if f.f != nil {
		_ = f.f.Close()
		f.f = nil
	}
}

// closeArchiveReader drops the gzip reader but KEEPS the underlying fd. The fd
// is the only handle to an unlinked archive, so it must outlive the reader
// whenever data read from the file is still uncommitted: an export can fail and
// the runtime can prune the .gz before the retry, and openArchive/drainArchive
// then re-decompress the uncommitted suffix straight from this fd.
func (t *Tailer) closeArchiveReader(f *file) {
	if f.gz != nil {
		_ = f.gz.Close()
		f.gz = nil
	}
}
