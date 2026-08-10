package tailer

// The byte-offset durability accounting: segment-qualified positions
// (pos), the per-file segment list, per-stream offset FIFOs, watermarks,
// and content fingerprints. This is the layer that decides how far the
// checkpoint may safely advance.

import (
	"compress/gzip"
	"errors"
	"hash/fnv"
	"io"
	"os"
	"slices"
	"syscall"
	"time"

	"github.com/JohanLindvall/kubescrape/internal/logline"
	"github.com/JohanLindvall/multiline"
	"github.com/JohanLindvall/multiline/cri"
	"go.opentelemetry.io/collector/pdata/pcommon"
)

// file is the tailer's state for one tracked log file under the watched
// directory (`/var/log/containers/<pod>_<ns>_<container>-<id>.log`), i.e. the
// current on-disk log of a single container instance. The Tailer holds one
// per path in its files map. It is owned entirely by the single Run goroutine
// and never shared.
//
// It is a streaming cursor, not a buffer of the container's history: reads
// advance readPos, whole physical lines are handed to the two-stage pipeline,
// and emitted entries are appended to the batch and forgotten. Only the
// unfinished tail (pending) and the pipeline's in-flight groups are retained
// between sweeps.
//
// A file's lifetime is one inode. Rotation, truncation, or a same-size
// copytruncate is detected against inode+fp and handled by reopen, which
// resets the byte offsets and rebuilds the pipeline — so a container whose log
// rotates is represented by a succession of file values, each with offsets
// local to its own inode. Offsets are therefore only meaningful within one
// file, which is why the pipeline and its accounting live here rather than
// globally.
//
// Metadata (resource) is resolved lazily from the container ID before any of
// the file's data is consumed, so every emitted record can be attributed.
//
// State invariant: lineStart + len(pending) == readPos, where lineStart is the
// file offset of pending[0] (the first byte not yet consumed as a line).
type file struct {
	path string
	// source is the configured source this file belongs to; it selects
	// containerd (CRI + metadata) vs plain handling. The rotation, offset and
	// multi-line machinery below is identical for both.
	source      *compiledSource
	containerID string // set for containerd files only
	// compressed reads the file as a gzip archive (read once to completion via
	// readArchive, offsets in decompressed space) rather than tailing it.
	compressed bool
	gz         *gzip.Reader
	// goneEnd is the EOF offset of a vanished file, captured when it is
	// drained. committed and readPos both rewind on a failed export, so they
	// cannot tell whether the drained bytes were ever exported; this can.
	goneEnd int64
	// archiveDone marks a compressed file read to completion; size/mod pin
	// the on-disk identity so sweeps skip it until the file changes.
	archiveDone bool
	// archiveEOF: the archive has been read to EOF in this pass, so readPos is
	// its true end. Distinguishes "delivered" from the post-rewind state, where
	// readPos == committed == 0 makes any offset comparison trivially true —
	// closing the fd there would drop an unlinked archive's only handle.
	archiveEOF  bool
	archiveSize int64
	archiveMod  time.Time
	inode       uint64
	// fp is the identity fingerprint: a hash of the first fp.Len bytes.
	// Together with the inode it prevents a checkpoint from resuming into a
	// different file (inode reuse, replaced content).
	fp fingerprint
	// targetDir is the watched directory of the symlink target.
	targetDir string
	// dirty marks files with pending fsnotify write events.
	dirty bool
	// lastMod is the modtime observed by the previous sweep, used to detect
	// same-size in-place rewrites.
	lastMod time.Time
	// lastLineTime is the newest LOG timestamp fed from this file, and lastFed
	// is the wall-clock instant it was fed at. The multi-line age-out compares
	// its cutoff against a buffered group's own log timestamp, so the cutoff
	// has to be in the same clock while lines are arriving — otherwise any lag
	// above MultilineTimeout tears every group (see sweep). lastFed is what
	// distinguishes "behind" from "idle": an abandoned group in a quiet file
	// must still age out, and only the wall clock can say that.
	lastLineTime time.Time
	lastFed      time.Time

	f         *os.File
	readPos   int64 // fd position
	lineStart int64 // offset of the first byte not yet consumed as a line
	committed int64 // offset covered by successful exports / checkpoint
	// skipEnd is the end offset of the newest line consume took WITHOUT feeding
	// it: a rate-DROPPED line, a blank line, or an oversized one whose discard
	// window just closed. `committed` only ever advances to an EXPORTED entry's
	// end, and an entry's end derives from a fed line's boundary, so a TRAILING
	// run of skipped lines — the normal shape for a rate-limited or finished
	// container — is otherwise permanently uncommittable: the checkpoint freezes
	// at the first of them, kubescrape_log_lag_bytes counts deliberately
	// discarded bytes forever, and a restart re-reads and RE-DELIVERS lines this
	// process already dropped. absorbSkipped is what lets committed cross them.
	//
	// Always a real line BOUNDARY, never the mid-line frontier an unfinished
	// oversize discard leaves behind (consume stamps it when the discard's
	// newline arrives, not when its prefix is dropped), so a restart resuming
	// here can never land inside a line. Bound to the byte-consumption state, so
	// restartAt clears it with pending and `discarding`.
	skipEnd int64
	pending []byte // incomplete physical line carried between sweeps
	// pendingBase pins the ONE backing array behind pending. consume advances
	// pending by RE-SLICING, so its base pointer walks forward and its spare
	// capacity drains to zero; appendPending moves the remainder back to the
	// front of this array before the next chunk is appended. Without it every
	// read allocated a fresh chunk-sized (64 KiB) array — garbage proportional
	// to the log volume read.
	pendingBase []byte

	// Two-stage pipeline: criStage rejoins CRI fragments into logical lines
	// (stage-1 data is the line's segment-qualified start position, captured
	// at feed time — emission may happen after a carried rotation has moved
	// the tail id), traces joins stack traces (nil when Multiline is off;
	// data is the first line's timestamp).
	criStage *cri.Aggregator[pos]
	traces   *multiline.Aggregator[time.Time]
	// ledger tracks which byte offsets are safe to checkpoint and how a group
	// buffered across a rotation is recovered.
	ledger

	resource    pcommon.Resource // resolved metadata, valid when resolved
	resolved    bool
	nextMetaTry time.Time
	// metaBackoff is the current retry interval for this file's metadata
	// lookup; it doubles per failure and resets on success.
	metaBackoff time.Duration
	gone        bool
	// unresolvedLost: the gone-before-resolve loss was already counted and
	// its checkpointed segments retired — drainGone must not re-count on the
	// sweeps between the drain and the release.
	unresolvedLost bool
	// drainErred: the most recent drain attempt over this file ended in a
	// read error (drainReader's non-EOF arm, or drainArchive failing to
	// reopen) rather than at EOF. Re-armed by every drainGone cycle;
	// chargeGoneStall is the only consumer.
	drainErred bool
	// goneStalledSince is when a gone file's drain last ERRORED with no
	// commit progress (chargeGoneStall); zero while advancing. It bounds how
	// long a goneEnd no read can reach again may pin the fd, the files-map
	// entry and the checkpoint line.
	goneStalledSince time.Time
	// Per-pod annotation config (podconfig.go), stamped at resolve time:
	// excluded skips the file entirely; multiline overrides the source's
	// stack-trace joining; podRules run before the global rules.
	excluded bool
	// podConfigErr is the parse error of a malformed kubescrape.io/logs
	// annotation (the annotation is then ignored — a malformed one must not
	// lose logs). Kept for /debug/tailer: the Warn line is on one node,
	// while the operator who edited the annotation is on another.
	podConfigErr string
	multiline    *bool
	podRules     *logline.LineFilter

	// Per-file line rate limiting (Config.RateLimit): a token bucket refilled
	// by elapsed time. limited marks a paused file (tokens exhausted, reading
	// suspended until they refill); drop mode discards lines instead.
	tokens     float64
	lastRefill time.Time
	limited    bool
	// rewindGen counts rewinds of this file. A loop that flushes mid-pass
	// snapshots it and stops if it moved: a failed flush purges the pipeline
	// under the loop, so everything fed since the snapshot is gone unemitted.
	rewindGen int
	// exportedHighs are the exported-entry end positions whose COMMIT was
	// withheld by the build-time watermark clamp — another stream's group was
	// still buffered. The next flush touching the file re-offers them: the
	// bytes are delivered, only the checkpoint lags, and without the re-offer
	// `committed` freezes below readPos forever (the high entry belongs to an
	// earlier batch that no later candidate set sees). Dead segment ids
	// (truncated away) resolve to nothing and are dropped harmlessly.
	//
	// Keyed BY SEGMENT, because withholding is per segment: the clamp deletes
	// or lowers each segment's candidate independently. Collapsing them to one
	// max discarded every older segment's delivered-but-withheld high the
	// moment a newer segment had one — pos.less orders by segment id first —
	// so that segment's `committed` stuck below its `to` for good and it could
	// never retire: its fd stayed held and its checkpoint Prefix entry was
	// rewritten on every save, forever.
	exportedHighs map[int]int64
	// hopUnsaved: a rename rotation recorded a segment for this file that no
	// save has persisted yet. A second hop while it is set forces the save
	// (reopen), because the intermediate inode is the one a restart has no
	// route back to; the sweep's closing save clears it.
	hopUnsaved bool
	// discarding marks the remainder of an oversized unterminated line: the
	// accumulated prefix was dropped (see consume), and everything up to the
	// line's eventual newline is part of the same line, not a record.
	discarding bool
	// idleClosed: the fd was released by closeIdleFiles with the file fully
	// caught up. readFile then gates the reopen on a cheap stat of the path
	// showing evidence of activity (size/mtime moved, or a different inode at
	// the path) — without the gate, every poll sweep's ensureOpen reopened the
	// fd (and re-read the fingerprint) just for closeIdleFiles to re-close it
	// on its own cadence, leaving idle fds open ~98% of steady state. Cleared
	// by any successful open (ensureOpen), so it is only ever consulted for a
	// close that THIS mechanism performed.
	idleClosed bool

	// keyStdout/keyStderr are the precomputed pipeline keys
	// ("<containerID>/<stream>") — feedLine runs per physical line and must
	// not rebuild them. stStdout/stStderr/stPlain are the matching cached
	// ledger states (stPlain doubles as the containerd passthrough key's
	// state); they are re-derived by newPipeline after every reset.
	keyStdout, keyStderr string
	stStdout, stStderr   *streamState
	stPlain              *streamState
}

// stateFor resolves a pipeline key handed back by an aggregator callback to
// its stream state. The keys are the fixed per-file set, so the common cases
// are single string compares (usually pointer-equal).
func (f *file) stateFor(key string) *streamState {
	switch key {
	case f.keyStdout:
		return f.stStdout
	case f.keyStderr:
		return f.stStderr
	}
	return f.state(key)
}

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

// reset clears the per-stream offset states for a fresh pipeline incarnation.
// It leaves the segment list untouched (segments persist across a carried
// rotation); segmentsFed goes false so incomplete segments are re-read before
// the new inode. Callers must re-derive any cached state pointers afterwards.
func (l *ledger) reset() {
	l.segmentsFed = false
	// The purge takes the segments' lines with it: whatever was live is not
	// any more, so no traversal claim over them is sound until they are
	// re-fed (fedTo included — it names lines that were in the purged
	// pipeline). reopen restores fed for a rotation, which purges nothing.
	// skipTo/discarding go with fedTo: a re-replay from committed must re-feed
	// the purged lines BELOW the discard frontier, so resuming at skipTo would
	// lose them — the discard window is re-derived from the re-read instead
	// (see the field doc for why the pair resets together).
	for _, sg := range l.segments {
		sg.fed = false
		sg.fedTo = 0
		sg.skipTo, sg.discarding = 0, false
	}
	l.streams = nil
}

// newTail starts a fresh tail segment and returns its id.
func (l *ledger) newTail() int {
	l.segSeq++
	l.tail = l.segSeq
	return l.tail
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
	// checkpointed: it describes pipeline state, so a purge (ledger.reset)
	// zeroes it and the replay re-reads from committed.
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
	// the same bytes, which suffices), and ledger.reset zeroes BOTH together
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

// fingerprint identifies file content: an FNV-1a hash of the first Len
// bytes. A zero Len means "not recorded" and matches anything.
type fingerprint struct {
	Len  int64
	Hash uint64
}

// computeFingerprint hashes the first n bytes of f (independent of the read
// offset).
func computeFingerprint(f io.ReaderAt, n int64) (fingerprint, error) {
	if n <= 0 {
		return fingerprint{}, nil
	}
	buf := make([]byte, n)
	read, err := f.ReadAt(buf, 0)
	if err != nil && !errors.Is(err, io.EOF) {
		return fingerprint{}, err
	}
	if int64(read) < n {
		buf = buf[:read]
	}
	h := fnv.New64a()
	_, _ = h.Write(buf)
	return fingerprint{Len: int64(len(buf)), Hash: h.Sum64()}, nil
}

// identityChanged reports whether the file previously had a recorded identity
// and the inode or head content at hand no longer matches it — the path (or
// the held fd) now names a DIFFERENT incarnation. Shared by ensureOpen and
// openArchive, whose replaced-file decisions must agree.
func (f *file) identityChanged(inode uint64, r io.ReaderAt) bool {
	return f.inode != 0 && (f.inode != inode || !f.fp.matches(r))
}

// matches reports whether the file still begins with the fingerprinted
// content.
func (fp fingerprint) matches(f io.ReaderAt) bool {
	if fp.Len == 0 {
		return true
	}
	cur, err := computeFingerprint(f, fp.Len)
	return err == nil && cur == fp
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

type entry struct {
	file      *file
	time      time.Time
	stream    string
	body      string
	truncated bool
	// match names the multiline pattern that produced a joined entry ("" for
	// plain single lines).
	match string
	// start is the segment-qualified position of the entry's first byte
	// (start.off is exposed as log.file.position); end is the position just
	// past the physical line that completed it. Committing end marks the
	// entry's bytes exported — against end.seg's record, so a pre-rotation
	// entry can never advance the new tail's checkpoint.
	start pos
	end   pos
}

// restartAt resets the byte-consumption state to off: read/line positions,
// the pending buffer, and the flags whose lifetime is bound to pending (a
// rate-limit pause, an oversized-line discard window and the skipped-bytes
// frontier all die with it — the bytes are re-read and re-evaluated from off).
// Every restart/rewind path shares this ONE helper deliberately: the
// archiveReplaced restart once drifted from reopen by omitting two of these
// resets, each a real bug.
func (f *file) restartAt(off int64) {
	f.readPos = off
	f.lineStart = off
	// Rewind to the front of the carry buffer, not to the current (re-sliced)
	// window: the whole array is free again.
	f.pending = f.pendingBase[:0]
	f.limited = false
	f.discarding = false
	// A frontier from the discarded pass names bytes off no longer covers (and
	// on a rotation, an offset in a different incarnation entirely): kept, it
	// would let absorbSkipped jump `committed` over lines the re-read is about
	// to feed.
	f.skipEnd = 0
}

func inodeOf(st os.FileInfo) uint64 {
	if sys, ok := st.Sys().(*syscall.Stat_t); ok {
		return sys.Ino
	}
	return 0
}
