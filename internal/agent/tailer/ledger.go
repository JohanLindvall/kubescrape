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

	f         *os.File
	readPos   int64  // fd position
	lineStart int64  // offset of the first byte not yet consumed as a line
	committed int64  // offset covered by successful exports / checkpoint
	pending   []byte // incomplete physical line carried between sweeps

	// Two-stage pipeline: criStage rejoins CRI fragments into logical lines
	// (stage-1 data is the physical start offset), traces joins stack traces
	// (nil when Multiline is off; data is the first line's timestamp).
	criStage *cri.Aggregator[int64]
	traces   *multiline.Aggregator[time.Time]
	// ledger tracks which byte offsets are safe to checkpoint and how a group
	// buffered across a rotation is recovered.
	ledger

	resource    pcommon.Resource // resolved metadata, valid when resolved
	resolved    bool
	nextMetaTry time.Time
	gone        bool

	// Per-file line rate limiting (Config.RateLimit): a token bucket refilled
	// by elapsed time. limited marks a paused file (tokens exhausted, reading
	// suspended until they refill); drop mode discards lines instead.
	tokens     float64
	lastRefill time.Time
	limited    bool
	// exportedHigh is the highest exported-entry end position whose COMMIT was
	// withheld by the build-time watermark clamp — another stream's group was
	// still buffered. The next flush touching the file re-offers it: the bytes
	// are delivered, only the checkpoint lags, and without the re-offer
	// `committed` freezes below readPos forever (the high entry belongs to an
	// earlier batch that no later candidate set sees). A dead segment id here
	// (truncated away) resolves to nothing and is dropped harmlessly.
	exportedHigh pos
	// discarding marks the remainder of an oversized unterminated line: the
	// accumulated prefix was dropped (see consume), and everything up to the
	// line's eventual newline is part of the same line, not a record.
	discarding bool

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

// logItem is one buffered logical line's offset range; when carries the
// line's timestamp (CRI-parsed, or the feed time for plain files) so stale
// items can be recognized (see the fifo pop).
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

type logItem struct {
	start, end pos
	when       time.Time
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

// streamState is the offset accounting for one pipeline key. stream is the
// precomputed streamOf(key), stamped on emitted entries. hasRun marks a
// pending stage-1 run (presence, not just a zero offset).
type streamState struct {
	key      string
	stream   string
	lastEnd  pos
	runStart pos
	hasRun   bool
	// A multi-fragment run closed by its F line is not emitted until the
	// stage sees the NEXT line for the key (or a flush), by which point
	// feedLine has already advanced lastEnd past it. closed pins the run's
	// own boundaries for that deferred emission; hasRun stays true meanwhile
	// so the watermark keeps covering it. nextStart/hasNext hold the
	// triggering line's registration, installed by the emission callback.
	closed      bool
	closedStart pos
	closedEnd   pos
	nextStart   pos
	hasNext     bool

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
	// committed is the commit progress within the segment: [committed, to) is
	// the range still owed (re-read on restart or after a rewind). It starts
	// at the tail's committed offset when the rotation closes the segment and
	// advances as the segment's entries export; the segment retires once it
	// reaches to.
	committed, to int64
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
// rate-limit pause and an oversized-line discard window both die with it —
// the bytes are re-read and re-evaluated from off). Every restart/rewind
// path shares this ONE helper deliberately: the archiveReplaced restart once
// drifted from reopen by omitting two of these resets, each a real bug.
func (f *file) restartAt(off int64) {
	f.readPos = off
	f.lineStart = off
	f.pending = f.pending[:0]
	f.limited = false
	f.discarding = false
}

func inodeOf(st os.FileInfo) uint64 {
	if sys, ok := st.Sys().(*syscall.Stat_t); ok {
		return sys.Ino
	}
	return 0
}
