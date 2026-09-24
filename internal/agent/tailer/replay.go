package tailer

// Segment replay: re-reading the owed ranges of rotated-away incarnations
// (after a rewind, and after a restart) under their own segment ids, and the
// stall budget that keeps an unreadable one from gating the live tail forever.

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
	stalled, spent := t.stallSpent(&sg.stalledSince,
		f.rewindGen != gen || max(sg.fedTo, sg.skipTo) > progressBefore)
	if !spent {
		return
	}
	obs.LogPrefixLost.Inc()
	t.log.Error("a rotated segment's source has been unreadable for too long; giving up on its lines so the file resumes collecting",
		"path", f.path, "inode", sg.inode, "stalled", stalled,
		"committed", sg.committed, "to", sg.to)
	f.retire(sg)
}

// stallSpent advances one stall clock by a pass and reports whether its budget
// (segmentStallLimit) is spent. reset is the caller's "this pass does not
// charge the stall" — progress, a rewind that purged the pipeline under the
// pass, a clean end — and zeroes the clock. Otherwise the first charging pass
// arms it and a later one returns how long it has been stalled, spent once that
// reaches the limit.
//
// ONE state machine for both clocks — chargeStall's per-segment one and
// chargeGoneStall's per-file one — because the gone path's doc promises "the
// same budget applies", and two hand-written copies were the only thing
// keeping that true. The clock is read only on a charging pass, as before.
func (t *Tailer) stallSpent(since *time.Time, reset bool) (stalled time.Duration, spent bool) {
	if reset {
		*since = time.Time{}
		return 0, false
	}
	now := time.Now()
	if since.IsZero() {
		*since = now
		return 0, false
	}
	stalled = now.Sub(*since)
	return stalled, stalled >= t.segmentStallLimit
}

// openSegmentSource resolves the readable handle for a segment's replay: the
// retained fd first (it reaches the inode even after the runtime has deleted
// or compressed the rotated file, which findRotated — resolving by NAME —
// cannot; only a restart, where no fd survives, falls back to the path). A
// segment whose source is genuinely gone is counted (obs.LogPrefixLost) AND
// retired — an unrecoverable segment kept on the list can never reach its
// `to` and would wedge retirement (fd budget, settledGone, the checkpoint)
// forever.
//
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
		// The owed range is the MAGNITUDE of the loss, which
		// kubescrape_log_prefix_lost_total (one count per given-up segment)
		// cannot carry: a segment owing 40 bytes and one owing 40 MiB are the
		// same increment. `to` is -1 for an open-ended segment (a rotation the
		// agent was down for), where the end is genuinely unknown — the line
		// says -1 rather than inventing a number.
		t.log.Warn("rotated segment source not found; its lines are lost",
			"path", f.path, "inode", p.inode, "committed", p.committed, "to", p.to)
		f.retire(p)
		return nil, "", nil, false, true // retired: nothing to open, nothing owed
	}
	opened, err := os.Open(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) { // pruned between findRotated and open
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
	// carryBase pins carry's whole array across reads (appendCompact): carry
	// is consumed by re-slicing, and appending to it directly reallocated a
	// chunk-sized array per read — a pass allocated ~2.2x the bytes it
	// replayed. Pass-local, so a buffer an oversized line grew is not kept.
	var carry, carryBase []byte
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
	// One clock read per read chunk, exactly as consume does — see the note
	// there for why f.lastFed does not need a per-line reading.
	var fedAt time.Time
	for remaining > 0 && (budget > 0 || overrun) {
		want := remaining
		if !overrun {
			want = min(want, budget)
		}
		n, rerr := fh.Read(buf[:min(int64(len(buf)), want)])
		if n > 0 {
			fedAt = time.Now()
			remaining -= int64(n)
			budget -= int64(n)
			carry = appendCompact(carryBase, carry, buf[:n])
			carryBase = carry[:0:cap(carry)]
			for {
				i := bytes.IndexByte(carry, '\n')
				if i < 0 {
					// Bound the carried incomplete line exactly as consume
					// does: a checkpointed segment containing an oversized
					// line (whose live read was capped and discarded) must
					// not be slurped whole into memory on replay. The
					// remainder up to its newline is part of the same line.
					if t.overCap(len(carry)) {
						cur += int64(len(carry))
						carry = carry[:0]
						// Counted once per LINE, exactly like consume's live
						// path: a line longer than the cap is discarded in as
						// many slabs as it has, and each pass of a replay that
						// resumes mid-discard would add another.
						if !discarding {
							f.noteOversized()
						}
						discarding = true
						// Persist the discard progress on the segment BEFORE the
						// flush below (like fedTo): a failed flush rewinds and
						// purgeSegmentFeeds zeroes it, and stamping afterwards would
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
					t.feedLine(ctx, f, string(line), start, cur, fedAt)
					fed = cur
				}
			}
			// Advance the feed frontier BEFORE the flush below: a failed
			// flush rewinds and resets it (purgeSegmentFeeds), and stamping it
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
		if len(carry) > 0 && !discarding {
			// The rotated file ENDS in an unterminated line, and pinning `to`
			// at the fed boundary means nothing reads past it again: the
			// fragment is lost exactly as the fd-held rename drain's torn final
			// line is (reopen), and must be counted like it. Every open-ended
			// door — reopen's unfinished drain, ensureOpen's replaced arm, the
			// rotation-while-down synthesis — routes the rotated file's end
			// through here, and all of them lost it uncounted. The
			// `!discarding` gate is reopen's and drainGone's: while discarding,
			// these bytes are the tail of an oversized line
			// obs.LogOversizedDropped already counted. This arm runs once per
			// segment (it pins `to` or retires), so the count cannot repeat.
			obs.LogTornFinalLines.Inc()
			t.log.Warn("unterminated final line lost at rotation", "path", path, "bytes", len(carry))
		}
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
