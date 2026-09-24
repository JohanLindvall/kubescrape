package tailer

// The per-path tailing state: the file struct the whole package threads
// through, the entry it emits, and the byte-consumption resets every restart
// path shares (restartAt, beginIncarnation).

import (
	"compress/gzip"
	"os"
	"syscall"
	"time"

	"github.com/JohanLindvall/kubescrape/internal/agent/attrs"
	"github.com/JohanLindvall/kubescrape/internal/logline"
	"github.com/JohanLindvall/kubescrape/pkg/kubemeta"
	"github.com/JohanLindvall/multiline"
	"github.com/JohanLindvall/multiline/cri"
	"go.opentelemetry.io/collector/pdata/pcommon"
)

// file is the tailer's state for one tracked PATH matched by a configured
// source: for a containerd source the CRI log of one container instance
// (`/var/log/containers/<pod>_<ns>_<container>-<id>.log`), for a plain source
// any matched file, for a compressed source a gzip archive read once. The
// Tailer holds one per path in its files map (created only by claimPath) for
// as long as the path is tracked. It is owned entirely by the single Run
// goroutine and never shared.
//
// It is a streaming cursor, not a buffer of the container's history: reads
// advance readPos, whole physical lines are handed to the two-stage pipeline,
// and emitted entries are appended to the batch and forgotten. Only the
// unfinished tail (pending) and the pipeline's in-flight groups are retained
// between sweeps.
//
// ONE file value outlives every rotation of its path. Each on-disk incarnation
// (inode) is a SEGMENT with a per-file monotonic id; the live one is the tail
// (ledger.tail), and every buffered or emitted offset is a segment-qualified
// pos, so offsets from different incarnations can never be confused. Rotation,
// truncation and a same-size copytruncate are detected against inode+fp and
// handled by reopen, in place: a RENAME rotation closes the old tail into
// file.segments when it still owes uncommitted bytes, CARRIES the pipeline
// when a multi-line group straddles the boundary, and issues a fresh tail id;
// a truncation or copytruncate flushes the pipeline and restarts the tail at
// zero under a fresh id WITHOUT recording a segment (the old content is gone).
// The scalar offsets below (readPos, lineStart, committed, skipEnd, goneEnd)
// are always the TAIL incarnation's; an older segment's progress lives on its
// segment record. That per-path, per-incarnation accounting is why the
// pipeline and the ledger live here rather than globally.
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
	// goneDrained: a gone-file drain has reached the end of what its fd can
	// yield at least once, so goneEnd describes the inode and settledGone may
	// compare against it. Zero on the first cycle and zeroed by resurrect; a
	// cycle whose drain stops early (a mid-drain export failure rewound the
	// fd, the per-drain cap fired, an archive would not reopen) leaves it
	// unset, and the settle gate reads "committed >= goneEnd" as "nothing owed"
	// only once it is set — on a first cycle goneEnd is still 0, so without
	// this the rewound file settled and released the ONLY handle to the
	// unlinked inode with every line past the rewind still behind it.
	goneDrained bool
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
	// targetDir is the RESOLVED directory of the symlink target, cached the
	// moment it is known (findRotated reads it to locate a rotated segment's
	// file by name, long after the live symlink may be gone).
	targetDir string
	// watchedDir is the directory this file currently holds a watch REFERENCE
	// on. Normally == targetDir; they diverge whenever watcher.Add fails
	// (fs.inotify.max_user_watches exhausted, the directory racing away), which
	// must not cost the cache above — see watchTarget.
	watchedDir string
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

	resource pcommon.Resource // resolved metadata, valid when resolved
	resolved bool
	// discovered stamps when this file was first tracked. Nothing is read
	// before a file resolves, so a file that never resolves produces no
	// records, moves no counter and loses nothing — the one failure mode in
	// this package with no symptom at all. publishStatus warns about files that
	// have been waiting since longer ago than unresolvedWarnAfter, which is
	// what tells "the metadata service is down" from "this container started a
	// second ago".
	discovered time.Time
	// oversized counts unterminated lines this file had discarded for
	// exceeding MaxEntryBytes (the aggregate is
	// kubescrape_log_oversized_dropped_total, which cannot say WHICH file).
	// Bumped together with the counter by noteOversized, on the first
	// over-cap slab of a line, live or replayed — not per read chunk and not
	// per line, so it is off the per-line path.
	oversized int
	// meta is the container metadata the resource was built from, retained for
	// containerd files so buildResource can re-render without a second lookup
	// when the NODE metadata changes. The maps inside are metaclient's, shared
	// under its treat-as-immutable contract — this is a struct per tracked
	// file, not a copy of the pod.
	meta *kubemeta.ContainerMetadata
	// nodeInfo is the node metadata the resource was built from, compared by
	// POINTER against what the provider currently yields (refreshNodeAttrs).
	// selfmeta.Poll allocates a fresh value per successful resolve, so pointer
	// inequality is exactly "something new has been produced" — which a value
	// comparison could not express anyway (NodeInfo carries maps).
	nodeInfo    *attrs.NodeInfo
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
	// podService/podAttrs are the pod annotation's RESOURCE overrides, already
	// vetted against reservedAttr. They are kept rather than applied once and
	// forgotten because buildResource re-renders the resource whenever the node
	// metadata changes, and the workload's overrides must survive that;
	// re-parsing the annotation there would re-count obs.LogPodAttrsRefused and
	// re-warn once per refresh, forever.
	podService string
	podAttrs   map[string]string

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
	// observed holds the IDENTITY of every entry that has already been through
	// the per-record chain and whose bytes a rewind can still bring back. An
	// entry whose identity is in here is rebuilt normally but carries
	// logchain.Input.Observed, so the chain skips its counting half.
	//
	// It exists because a failed export REWINDS the file and the next sweep
	// re-reads the same bytes: delivery is at-least-once by design, but a
	// user-configured counter or histogram is cumulative, so re-observing
	// multiplied it by the number of rewinds a collector outage spanned — the
	// metric lying hardest exactly while an operator reads it to diagnose that
	// outage. (Same class as TransformDropped moving to ack time, and as
	// tailbuffer deferring its late tallies to the carrying push's ack.)
	//
	// PER ENTRY, never a byte RANGE. A range asserts something about bytes that
	// may never have gone through the chain at all: consume SKIPS lines without
	// feeding them (a rate-DROPPED line, a blank one, an oversized one's
	// discard window), and a skipped line sits BELOW the frontier the admitted
	// lines around it establish. The rate limiter is the demonstrated case —
	// its verdict is a function of the token bucket, not of the bytes, so the
	// pass after a rewind legitimately ADMITS a line the pass before dropped.
	// Under a range frontier that line shipped with its observation suppressed:
	// silent loss, which is strictly worse than the over-count this fixes (an
	// inflated counter during an outage is visibly weird; a missing one is
	// indistinguishable from no traffic). Measured on that sequence: a range
	// frontier counted 2 observations for 3 delivered records, per-entry
	// identities count 3, and no suppression at all counts 5. The test is
	// TestRateDroppedLineAdmittedLaterIsStillObserved.
	observed map[obsKey]struct{}
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

// beginIncarnation starts a NEW incarnation of the path at offset off: a fresh
// tail id, `committed` and the byte-consumption state (restartAt) at off, and
// the gone verdict's settle state cleared — a goneEnd pinned the previous
// incarnation's EOF and describes nothing about this one.
//
// Every door where the path turns out to name different content goes through
// here — reopen, ensureOpen's replaced/truncated arm, readArchive's in-place
// rewrite — for restartAt's reason one level up: these resets were written
// inline at each door, and the archive restart once drifted from reopen by
// omitting two of them. What it deliberately leaves to the caller:
//
//   - exportedHighs. reopen's are keyed by the OLD tail id, which is exactly
//     the id of the segment reopen just recorded, and they are the only thing
//     that can commit that segment's delivered-but-withheld lines (it is fed,
//     so nothing re-feeds them): clearing them here would pin the segment
//     until a restart. The doors that must drop them (ensureOpen; rewind,
//     which keeps its tail id) do so themselves.
//   - the pipeline and the identity (inode/fp): reopen may CARRY the pipeline
//     across a rename, and ensureOpen adopts the new identity rather than
//     clearing it.
func (f *file) beginIncarnation(off int64) {
	f.newTail()
	f.committed = off
	f.goneEnd, f.goneDrained = 0, false
	f.restartAt(off)
}

func inodeOf(st os.FileInfo) uint64 {
	if sys, ok := st.Sys().(*syscall.Stat_t); ok {
		return sys.Ino
	}
	return 0
}
