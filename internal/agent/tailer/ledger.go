package tailer

// The byte-offset durability accounting: segment-qualified positions
// (pos), the per-file segment list, per-stream offset FIFOs and watermarks.
// This is the layer that decides how far the checkpoint may safely advance.
// (The file struct it is embedded in lives in file.go, the content
// fingerprint in fingerprint.go, the observation dedup in observed.go.)

import (
	"os"
	"slices"
	"time"
)

// pos is a byte position qualified by the segment (file incarnation) it lives
// in: seg is a per-file monotonic id (the live file is the tail segment; each
// rename rotation closes the tail into a recorded segment and starts a new
// one). Qualifying every buffered/emitted offset with its segment is what
// makes cross-rotation offsets unambiguous BY CONSTRUCTION — the old design
// disambiguated them with a rotation generation stamped on entries and a
// rewrite of buffered offsets at the rotation instant (reanchor), both of
// which this type replaces.
type pos struct {
	seg int
	off int64
}

// less orders positions: segment ids are monotonic, so lexicographic order is
// stream order.
func (p pos) less(q pos) bool { return p.seg < q.seg || (p.seg == q.seg && p.off < q.off) }

// logItem is one buffered logical line's offset range.
type logItem struct {
	start, end pos
}

// ledger is the byte-offset durability accounting for one file's two-stage
// pipeline: it decides how far the checkpoint may safely advance and how a
// multi-line group buffered across a rename rotation survives a crash. It is
// embedded in file (fields/methods are used unqualified as f.state(), f.tail,
// f.watermark(), ...).
//
// # Offsets within one inode
//
// A physical line spans [start, end) bytes. Each pipeline key
// ("<containerID>/<stream>") owns one streamState: lastEnd is the end of the
// newest physical line fed; runStart is the start of the oldest physical line
// not yet emitted by stage 1 (the CRI P/F rejoiner); fifo holds the [start,end)
// ranges of the logical lines currently buffered in stage 2 (the trace joiner).
// The set of keys per file is fixed (stdout/stderr, or one plain/passthrough
// key), so the states live in a small slice and the per-line paths reach them
// through pointers cached on the file — no map operations per line.
// The multiline package hands the emitter only the *first* line's payload, so
// an emitted group's end offset is recovered by popping Entry.Lines items off
// its fifo and taking the last one's end. watermark() is the lowest offset
// still buffered anywhere; the checkpoint must never advance past it, or a
// crash would skip un-exported lines.
//
// # Across a rename rotation (multi-line join + crash safety)
//
// When a group straddles a rename rotation the pipeline is carried into the
// new inode instead of being flushed (see reopen). Every buffered/emitted
// offset is a pos — qualified by its segment — so pre-rotation lines commit
// to THEIR segment's record and can never advance the new tail's checkpoint;
// there is nothing to re-base and no generation to check.
//
// segments lists the rotated-away incarnations (oldest first, one per hop)
// whose bytes are not yet fully committed; it is checkpointed. On restart or
// after a rewind (segmentsFed == false) the incomplete ranges are re-read
// from the rotated files before the new inode, reconstructing a straddling
// group with no loss. A segment leaves the list once its whole range commits.
type ledger struct {
	streams []*streamState

	// segSeq issues per-file monotonic segment ids; tail is the live file's.
	// A truncation-style restart (content destroyed, nothing recoverable)
	// starts a new tail WITHOUT recording the old segment: batch entries
	// still naming the dead id simply resolve to nothing at commit.
	segSeq int
	tail   int
	// segments are the closed, incompletely-committed incarnations.
	segments    []*segment
	segmentsFed bool
	// feeding is the segment id lines are currently being fed under: 0 (the
	// normal case) means the tail; feedSegments sets it while re-reading an
	// old segment so its items/entries carry THAT segment's id.
	feeding int
}

// curSeg is the segment id for bytes being fed right now.
func (l *ledger) curSeg() int {
	if l.feeding != 0 {
		return l.feeding
	}
	return l.tail
}

// fedEnd returns the last FED line boundary of the current tail incarnation
// (max streamState.lastEnd at the tail, floored at committed): the highest
// offset a committing entry can ever reach. Bytes past it — a torn final
// fragment, a blank line, rate-DROPPED or oversize-discarded lines — never
// entered the pipeline and can never commit, so completion conditions
// (segment `to`, goneEnd) must compare against this, never raw readPos.
func (f *file) fedEnd() int64 {
	end := f.committed
	for _, st := range f.streams {
		if st.lastEnd.seg == f.tail && st.lastEnd.off > end {
			end = st.lastEnd.off
		}
	}
	return end
}

// absorbSkipped advances `committed` across bytes the read CONSUMED but never
// FED (file.skipEnd), which no entry can ever commit for it.
//
// The guard is `fedEnd() == committed`, i.e. no fed line at the tail ends above
// the commit frontier. That single test covers everything that could be lost by
// jumping the frontier: a line still buffered in either stage, and a line
// already emitted into the unflushed batch, both keep a lastEnd above committed
// (a line's bytes are only committed once its entry exports), so neither can be
// live when it holds. Old segments are untouched — they carry their own
// committed frontier and their lines never land in the tail's lastEnd.
func (f *file) absorbSkipped() {
	if f.skipEnd > f.committed && f.fedEnd() == f.committed {
		f.committed = f.skipEnd
	}
}

// streamState is the offset accounting for one pipeline key. stream is the
// precomputed streamOf(key), stamped on emitted entries. hasRun marks a
// pending stage-1 run (presence, not just a zero offset).
type streamState struct {
	key      string
	stream   string
	lastEnd  pos
	runStart pos
	hasRun   bool
	// runBytes counts the bytes consumed by the CURRENT stage-1 fragment run
	// (a byte count, not an offset, so a run carried across a rename rotation
	// keeps accumulating where segment-qualified offsets could not subtract).
	// feedLine bounds it: past double the retention cap the run force-closes
	// (the never-completing-run wedge — see the bound in feedLine).
	runBytes int64

	// fifo holds the buffered logical lines; the live ones are fifo[fifoHead:].
	// Consumption advances fifoHead rather than re-slicing fifo, so the backing
	// array is reused: re-slicing walked the array forward until its capacity
	// ran out, and since the steady state is one line pushed and one popped, it
	// then reallocated a one-element array for EVERY subsequent line — the
	// tailer's whole per-line allocation. Popping the last live item recycles
	// the array instead (see pop).
	fifo     []logItem
	fifoHead int
}

// live are the buffered items still awaiting emission.
func (st *streamState) live() []logItem { return st.fifo[st.fifoHead:] }

// push appends one logical line's offset range.
func (st *streamState) push(it logItem) { st.fifo = append(st.fifo, it) }

// pop discards the first n live items. Once the fifo drains it resets to the
// base of the backing array, so the steady state never allocates. A partially
// drained fifo keeps its head offset; it is bounded by the buffered group.
func (st *streamState) pop(n int) {
	st.fifoHead += n
	if st.fifoHead >= len(st.fifo) {
		st.fifo = st.fifo[:0]
		st.fifoHead = 0
	}
}

// state returns the key's stream state, creating it on first use. The slice
// holds at most a few entries, and the compares hit the pointer-equality fast
// path, so this stays cheaper than a map — but per-line code should use the
// pointers cached on the file instead.
func (l *ledger) state(key string) *streamState {
	for _, st := range l.streams {
		if st.key == key {
			return st
		}
	}
	st := &streamState{key: key, stream: streamOf(key)}
	l.streams = append(l.streams, st)
	return st
}

// resetStreams clears the per-stream offset states for a fresh pipeline
// incarnation. Callers must re-derive any cached state pointers afterwards
// (rebuildPipeline does).
func (l *ledger) resetStreams() { l.streams = nil }

// purgeSegmentFeeds records that the pipeline's lines were DISCARDED unemitted
// (a rewind), so the recorded segments' owed lines are live no longer: it
// leaves the segment list untouched (segments persist across a carried
// rotation) and sets segmentsFed false so incomplete segments are re-read
// before the new inode. newPipeline runs it; reopen's drained rebuild does not.
func (l *ledger) purgeSegmentFeeds() {
	l.segmentsFed = false
	// The purge takes the segments' lines with it: whatever was live is not
	// any more, so no traversal claim over them is sound until they are
	// re-fed (fedTo included — it names lines that were in the purged
	// pipeline). skipTo/discarding go with fedTo: a re-replay from committed
	// must re-feed the purged lines BELOW the discard frontier, so resuming at
	// skipTo would lose them — the discard window is re-derived from the
	// re-read instead (see the field doc for why the pair resets together).
	for _, sg := range l.segments {
		sg.fed = false
		sg.fedTo = 0
		sg.skipTo, sg.discarding = 0, false
	}
}

// newTail starts a fresh tail segment (l.tail is its id).
func (l *ledger) newTail() {
	l.segSeq++
	l.tail = l.segSeq
}

// segmentByID resolves a recorded (non-tail) segment; nil for the tail, for
// dead ids (truncated-away incarnations), and after the segment completed.
func (l *ledger) segmentByID(id int) *segment {
	for _, s := range l.segments {
		if s.id == id {
			return s
		}
	}
	return nil
}

// committedIn resolves the commit frontier of segment id seg: the file's own
// `committed` for the tail, the segment record's for a recorded one, and
// ok=false for a dead id (retired, or truncated away), which has nothing left to
// commit against. The READ half of the tail-vs-recorded branch; advanceBatch's
// candidate loop keeps its own, because it needs the *segment to advance and
// retire.
func (f *file) committedIn(seg int) (int64, bool) {
	if seg == f.tail {
		return f.committed, true
	}
	if s := f.segmentByID(seg); s != nil {
		return s.committed, true
	}
	return 0, false
}

// segment is a rotated-away file incarnation whose byte range is not yet
// fully committed, held (with its fd where the budget allows) until every
// byte up to `to` commits.
type segment struct {
	id    int
	inode uint64
	fp    fingerprint
	// fed reports whether this segment's owed range is LIVE — its lines are in
	// the pipeline or the unflushed batch, so an entry that traverses the
	// segment genuinely covers it through `to`.
	//
	// Per-SEGMENT, because the file-level segmentsFed is only true after the
	// WHOLE replay pass finishes: every mid-pass flush (maybeFlush) sees
	// it false, and proposeCandidates is evaluated once per entry at flush
	// time, so gating on it DROPPED the traversal claim instead of deferring
	// it — and after the pass f.feeding is 0, so no later entry can start in
	// that segment and re-offer it. The segment then never retired: its
	// checkpoint entry was rewritten forever, settledGone never fired (a
	// deleted file's fd and map entry pinned for the process lifetime), and a
	// restart replayed the prefix without its continuation, freezing the
	// commit frontier.
	//
	// A rotation-recorded segment is fed at birth: `to` is the last FED line
	// boundary at rotation time, so its bytes are already in the pipeline. A
	// checkpoint-restored one is not, until feedSegments re-reads its range.
	fed bool
	// committed is the commit progress within the segment: [committed, to) is
	// the range still owed (re-read on restart or after a rewind). It starts
	// at the tail's committed offset when the rotation closes the segment and
	// advances as the segment's entries export; the segment retires once it
	// reaches to.
	committed, to int64
	// fedTo is the replay's FEED progress: lines up to it are already in the
	// pipeline from an earlier, budget-cut pass of THIS pipeline incarnation.
	// A resumed pass starts at max(committed, fedTo, skipTo) — resuming at
	// committed alone re-fed lines the pipeline still buffers, and a re-fed P
	// fragment APPENDS to its own still-open run (a duplicated fragment inside
	// one joined record, not an at-least-once duplicate record). Never
	// checkpointed: it describes pipeline state, so a purge
	// (purgeSegmentFeeds) zeroes it and the replay re-reads from committed.
	fedTo int64
	// skipTo is the replay's DISCARD progress: the already-discarded prefix of
	// an oversized line ends here, so a resumed pass skips past it (a discarded
	// run can never produce a committing entry, so skipping it is safe). It is
	// SEPARATE from fedTo because fedTo is a committable line boundary — the
	// open-ended replay pins `to` from it, and a `to` at the end of a discarded
	// run is an offset no entry can ever commit, wedging the segment below
	// retirement. Without skipTo the discard progress was pass-local: a line
	// whose newline is out of one pass's reach was re-read from fedTo and
	// re-discarded identically every sweep — the segment pinned at one offset
	// until the stall limit retired it and lost its readable remainder.
	// discarding mirrors the live path's file.discarding: skipTo sits MID-LINE,
	// so the remainder up to the line's eventual newline is part of the same
	// oversized line, not a record — resuming without the flag would feed that
	// remainder as a fresh record.
	//
	// Neither is checkpointed (positions.Prefix carries commit progress only —
	// a restart re-reads from committed and re-derives the discard window from
	// the same bytes, which suffices), and purgeSegmentFeeds zeroes BOTH together
	// with fedTo: a purge re-replays from committed, which re-derives them, and
	// zeroing one without the other would either feed the oversized line's tail
	// as a record (skipTo kept, discarding cleared) or swallow legitimate lines
	// up to the next newline (discarding kept, skipTo zeroed).
	skipTo     int64
	discarding bool
	// stalledSince is when this segment's replay last made no progress at all
	// (see chargeStall); zero while it is advancing. It bounds how long the
	// LIVE TAIL may stay gated behind a source that will not open.
	stalledSince time.Time
	// fd is the rotated inode's still-open handle, kept while the segment is
	// incomplete: the runtime prunes rotated files on its own schedule (a
	// bounded rotation count), and once it does, findRotated cannot resolve
	// the segment by name — but the fd still reaches the unlinked inode. nil
	// after a restart, where findRotated is the only route.
	fd *os.File
}

// maxCarriedFds bounds the rotated-inode fds held for recovery across an
// outage (see reopen).
const maxCarriedFds = 4

// retainedFds counts the segments still holding an open fd.
func (f *file) retainedFds() int {
	n := 0
	for _, s := range f.segments {
		if s.fd != nil {
			n++
		}
	}
	return n
}

// retire closes one completed segment's fd and removes it from the list.
// Only legitimate once its whole range is committed (or the file is being
// dropped) — the fd is the last handle to an inode the runtime may already
// have unlinked.
func (f *file) retire(s *segment) {
	if s.fd != nil {
		_ = s.fd.Close()
		s.fd = nil
	}
	f.segments = slices.DeleteFunc(f.segments, func(x *segment) bool { return x == s })
}

// closeSegments releases every segment unconditionally (drop/release paths).
func (f *file) closeSegments() {
	for _, s := range f.segments {
		if s.fd != nil {
			_ = s.fd.Close()
			s.fd = nil
		}
	}
	f.segments = nil
}

// watermark returns the lowest position still buffered in the pipeline;
// committed offsets must not advance past it (per segment: a candidate in a
// segment NEWER than the watermark's commits nothing, one in the SAME segment
// clamps to the watermark offset, and OLDER segments are unconstrained).
func (l *ledger) watermark() (pos, bool) {
	var wm pos
	found := false
	lower := func(v pos) {
		if !found || v.less(wm) {
			wm, found = v, true
		}
	}
	for _, st := range l.streams {
		if st.hasRun {
			lower(st.runStart)
		}
		if live := st.live(); len(live) > 0 {
			lower(live[0].start)
		}
	}
	return wm, found
}
