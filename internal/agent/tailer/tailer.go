// Package tailer tails log files selected by configurable sources (see
// sources.go) and exports the entries as OTLP logs. The default source is
// containerd container logs under /var/log/containers, whose resource
// attributes are fetched from the kubescrape metadata service; plain sources
// tail arbitrary files with static resource attributes. Both use the same
// rotation, offset and multi-line machinery.
//
// Log lines flow through the two-stage github.com/JohanLindvall/multiline
// pipeline: the cri stage parses the CRI format and rejoins partial-line
// fragments (containerd sources only), and the multiline stage joins
// application-level multi-line entries such as stack traces.
//
// Design: a single sweep goroutine reads all files (bounded bytes per file
// per sweep), feeds the pipeline, and batches emitted entries. Sweeps are
// triggered by fsnotify events (writes on the log directories, symlink
// creation/removal) with a polling ticker as fallback; polling alone remains
// available with Watch off. Export happens inline in the sweep with retries;
// file offsets are only committed (and checkpointed) after a successful
// export — never past lines still buffered in the pipeline — and on failure
// the files are rewound to the committed offsets so the data is re-read:
// at-least-once delivery with no unbounded in-memory queue.
//
// Rotation handling: a file's identity is its inode plus a fingerprint (hash
// of the first FingerprintBytes), so checkpoints never mis-resume into a
// different file that reuses an inode. On rename rotation the old fd is
// drained to EOF before switching to the new file; in-place truncation
// restarts at offset zero; removed files are drained before being dropped.
package tailer

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/JohanLindvall/multiline"
	"github.com/JohanLindvall/multiline/cri"
	"github.com/fsnotify/fsnotify"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"

	"github.com/JohanLindvall/kubescrape/internal/agent/attrs"
	"github.com/JohanLindvall/kubescrape/internal/agent/logattrs"
	"github.com/JohanLindvall/kubescrape/internal/agent/logenrich"
	"github.com/JohanLindvall/kubescrape/internal/agent/metaclient"
	"github.com/JohanLindvall/kubescrape/internal/agent/positions"
	"github.com/JohanLindvall/kubescrape/internal/kubemeta"
	"github.com/JohanLindvall/kubescrape/internal/metrics"
	"github.com/JohanLindvall/kubescrape/internal/obs"
)

// LogExporter sends one OTLP logs payload.
type LogExporter interface {
	ExportLogs(ctx context.Context, ld plog.Logs) error
}

// MetadataSource resolves container metadata; implemented by
// metaclient.Client.
type MetadataSource interface {
	Container(ctx context.Context, id string, wait time.Duration) (*kubemeta.ContainerMetadata, error)
}

// Config configures the tailer.
type Config struct {
	// Dir is the containerd log directory used to build the default source
	// when Sources is empty (/var/log/containers).
	Dir string
	// Sources selects which files to tail and how (containerd vs plain). Empty
	// means a single containerd source over Dir.
	Sources        []Source
	CheckpointFile string // "" disables the standalone checkpoint file
	// Positions, when set, persists offsets to the shared positions store
	// instead of CheckpointFile (which is then ignored).
	Positions *positions.Store
	// Watch uses fsnotify events to trigger reads and discovery; the poll
	// ticker remains as a fallback for missed events.
	Watch bool
	// FingerprintBytes is the length of the file-head hash used (with the
	// inode) as the file identity for checkpoint resumption and rewrite
	// detection; 0 means the 1024-byte default, negative relies on the
	// inode alone.
	FingerprintBytes int
	PollInterval     time.Duration
	FlushInterval    time.Duration
	BatchSize        int // flush after this many entries
	MaxEntryBytes    int // cap on one assembled log entry
	MaxBytesPerSweep int // per file; keeps one chatty container from starving others
	// Multiline joins application-level multi-line entries (stack traces,
	// ...); CRI partial-line rejoining is always on.
	Multiline bool
	// Enrich parses metadata out of each line (timestamp, severity,
	// trace/span IDs, exception details, ...) into the record's OTLP fields
	// and attributes.
	Enrich bool
	// FileAttributes stamps log.file.name (the file's basename) and
	// log.file.position (the byte offset just past the record) on every emitted
	// record, for any file source. Opt-in.
	FileAttributes bool
	// LogAttrs lifts configured keys out of structured lines onto the record
	// as resource/scope/log attributes (nil = none).
	LogAttrs *logattrs.Extractor
	// LogMetrics derives configured metrics from each exported log record
	// (nil = none). Its keys resolve against the record's attributes and the
	// file's resolved resource attributes.
	LogMetrics *metrics.DynamicMetricSet
	// MultilineTimeout flushes buffered fragment runs and multi-line groups
	// that have not completed within this duration.
	MultilineTimeout time.Duration
	// ExcludeNamespaces lists namespaces whose container logs are not
	// tailed (e.g. the observability namespace itself, to avoid feedback
	// loops through the collector's own output).
	ExcludeNamespaces []string
	// RateLimit caps each file at this many lines per second (token bucket,
	// 0 disables). By default an exhausted file is PAUSED — reading stops
	// until tokens refill, leaving the backlog on disk (no loss; a rotation
	// drain bypasses the limiter). RateDrop discards excess lines instead.
	RateLimit float64
	// RateBurst is the token bucket size (default 2x RateLimit).
	RateBurst float64
	// RateDrop discards lines over the limit instead of pausing the file.
	RateDrop bool
	// Rules filters exported records (ordered keep/drop/sample, nil = keep
	// all). Evaluated after enrichment — severity is matchable via the
	// synthetic __severity__ key — and after LogMetrics, so metrics still see
	// every line. Dropped records advance offsets like exported ones.
	Rules *metrics.LineFilter
	// PipelinedExport overlaps reading with export delivery: one export may
	// be in flight while the sweep keeps reading; its result (commit or
	// rewind) is applied before the next flush. At-least-once semantics are
	// unchanged. Off by default (exports happen inline in the sweep).
	PipelinedExport bool
	// Attrs builds the exported resource attributes (nil = defaults).
	Attrs *attrs.Builder
	// NodeInfo supplies the agent node's metadata for attribute templates
	// (nil = omitted; the pod's nodeName still fills k8s.node.name).
	NodeInfo     func() *attrs.NodeInfo
	MetadataWait time.Duration
	Metadata     MetadataSource
	Exporter     LogExporter
	Logger       *slog.Logger
}

// Tailer tails all container logs in a directory. All methods run on the
// single Run goroutine.
type Tailer struct {
	cfg            Config
	log            *slog.Logger
	sources        []*compiledSource
	scanDirs       map[string]struct{} // fixed base dirs of all include globs, watched for new files
	files          map[string]*file    // by path
	batch          []entry
	readBuf        []byte // reusable read scratch (single sweep goroutine)
	warnedListing  bool   // a glob-failure warning was already emitted
	lastFlush      time.Time
	lastCheckpoint time.Time
	retryBackoff   time.Duration // initial export retry backoff

	// status is the published per-file snapshot for /debug/tailer (written by
	// the sweep goroutine in publishStatus, read from HTTP handlers).
	status      atomic.Pointer[[]FileStatus]
	lastStatus  time.Time
	statusEvery time.Duration // snapshot cadence (10s; tests shorten it)

	// Pipelined export (Config.PipelinedExport): the worker channel and the
	// single outstanding export, owned by the sweep goroutine (see
	// pipelined.go). exportCh == nil means inline (synchronous) export.
	exportCh chan *inflight
	inflight *inflight

	// Event-driven mode (nil watcher = pure polling).
	watcher   *fsnotify.Watcher
	watchRefs map[string]int // watched target directories, refcounted
	// byTargetDir indexes files by their watched target directory so an
	// fsnotify event marks only that directory's files dirty instead of
	// scanning the whole files map per event.
	byTargetDir map[string]map[*file]struct{}
}

// scratch returns the shared read buffer. The sweep goroutine owns all reads,
// so one buffer serves every file — the previous per-file-per-sweep make was
// files x 128KiB/s of steady-state garbage on idle directories.
func (t *Tailer) scratch() []byte {
	if t.readBuf == nil {
		t.readBuf = make([]byte, 64*1024)
	}
	return t.readBuf
}

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
	// archiveDone marks a compressed file read to completion; size/mod pin
	// the on-disk identity so sweeps skip it until the file changes.
	archiveDone bool
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
type logItem struct {
	start, end int64
	when       time.Time
}

// ledger is the byte-offset durability accounting for one file's two-stage
// pipeline: it decides how far the checkpoint may safely advance and how a
// multi-line group buffered across a rename rotation survives a crash. It is
// embedded in file (fields/methods are used unqualified as f.state(), f.gen,
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
// When a group straddles a rename rotation the pipeline is carried into the new
// inode instead of being flushed (see reopen). Two problems follow, solved by
// gen and carried:
//
//   - gen (rotation generation) stamps every emitted entry; flush commits only
//     offsets of entries whose gen == the file's current gen. This keeps the
//     pre-rotation lines already drained from the old inode — which carry
//     old-inode offsets — from advancing the new inode's checkpoint. reanchor()
//     zeroes the offsets still buffered from the old inode at the moment of the
//     switch, so watermark reflects the new inode's origin.
//   - carried lists the rotated-away files (oldest first, one per hop) whose
//     tails are buffered but not yet exported; it is checkpointed. On restart
//     or after a rewind (carriedFed == false) those tails are re-read from the
//     rotated files before the new inode, reconstructing the group with no
//     loss. carried clears once the group exports (watermark shows nothing
//     buffered).
type ledger struct {
	streams []*streamState

	gen        int
	carried    []rotatedPrefix
	carriedFed bool
}

// streamState is the offset accounting for one pipeline key. stream is the
// precomputed streamOf(key), stamped on emitted entries. hasRun marks a
// pending stage-1 run (presence, not just a zero offset).
type streamState struct {
	key      string
	stream   string
	lastEnd  int64
	runStart int64
	hasRun   bool
	// A multi-fragment run closed by its F line is not emitted until the
	// stage sees the NEXT line for the key (or a flush), by which point
	// feedLine has already advanced lastEnd past it. closed pins the run's
	// own boundaries for that deferred emission; hasRun stays true meanwhile
	// so the watermark keeps covering it. nextStart/hasNext hold the
	// triggering line's registration, installed by the emission callback.
	closed      bool
	closedStart int64
	closedEnd   int64
	nextStart   int64
	hasNext     bool
	fifo        []logItem
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

// reset clears the per-inode offset states for a fresh pipeline incarnation. It
// leaves gen and carried untouched (they persist across a carried rotation);
// carriedFed goes false so any carried tails are re-read before the new inode.
// Callers must re-derive any cached state pointers afterwards.
func (l *ledger) reset() {
	l.carriedFed = false
	l.streams = nil
}

// reanchor resets the offsets still buffered in the pipeline to the new inode's
// origin, so watermark holds the new inode's checkpoint at 0 until the carried
// group completes and the (new-inode) offset of its final line becomes the
// commit point.
func (l *ledger) reanchor() {
	for _, st := range l.streams {
		st.lastEnd = 0
		st.runStart = 0
		st.closedStart = 0
		st.closedEnd = 0
		st.nextStart = 0
		for i := range st.fifo {
			// Zero the offsets only: when must survive, or the fifo's
			// orphan detection would mistake reanchored live items for
			// dropped lines.
			st.fifo[i].start, st.fifo[i].end = 0, 0
		}
	}
}

// rotatedPrefix is the unexported tail of a rotated-away file, held until the
// straddling multi-line group completes and exports.
type rotatedPrefix struct {
	inode    uint64
	fp       fingerprint
	from, to int64
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

// watermark returns the lowest offset still buffered in the pipeline;
// committed offsets must not advance past it.
func (l *ledger) watermark() (int64, bool) {
	wm := int64(-1)
	lower := func(v int64) {
		if wm < 0 || v < wm {
			wm = v
		}
	}
	for _, st := range l.streams {
		if st.hasRun {
			lower(st.runStart)
		}
		if len(st.fifo) > 0 {
			lower(st.fifo[0].start)
		}
	}
	return wm, wm >= 0
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
	// start is the file offset of the first byte of the entry (exposed as
	// log.file.position); offset is the offset just after the physical line
	// that completed it, and committing offset marks the entry as exported.
	start  int64
	offset int64
	// gen is the file's rotation generation when the entry was emitted. Only
	// entries of the file's current generation drive its committed offset:
	// pre-rotation entries carry offsets in the old inode's space (recoverable
	// via file.carried) and must not advance the new inode's checkpoint.
	gen int
}

// New creates a Tailer.
func New(cfg Config) *Tailer {
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = 500 * time.Millisecond
	}
	if cfg.FlushInterval <= 0 {
		cfg.FlushInterval = 2 * time.Second
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 1024
	}
	if cfg.MaxEntryBytes <= 0 {
		cfg.MaxEntryBytes = 1 << 20
	}
	if cfg.MaxBytesPerSweep <= 0 {
		cfg.MaxBytesPerSweep = 1 << 20
	}
	if cfg.RateLimit > 0 && cfg.RateBurst <= 0 {
		cfg.RateBurst = 2 * cfg.RateLimit
	}
	if cfg.MultilineTimeout <= 0 {
		cfg.MultilineTimeout = time.Second
	}
	if cfg.FingerprintBytes == 0 {
		cfg.FingerprintBytes = 1024
	} else if cfg.FingerprintBytes < 0 {
		cfg.FingerprintBytes = 0
	}
	log := cfg.Logger
	if log == nil {
		log = slog.Default()
	}
	sources := compileSources(cfg.Sources, cfg.Dir, cfg.Multiline)
	scanDirs := map[string]struct{}{}
	for _, s := range sources {
		for _, d := range s.scanBaseDirs() {
			scanDirs[d] = struct{}{}
		}
	}
	return &Tailer{
		cfg:          cfg,
		log:          log,
		sources:      sources,
		scanDirs:     scanDirs,
		files:        make(map[string]*file),
		retryBackoff: time.Second,
		statusEvery:  10 * time.Second,
	}
}

// Run tails until ctx is done, then flushes what it has.
func (t *Tailer) Run(ctx context.Context) {
	if t.cfg.Watch {
		if w, err := fsnotify.NewWatcher(); err != nil {
			t.log.Warn("fsnotify unavailable, falling back to polling", "error", err)
		} else {
			watched := 0
			for dir := range t.scanDirs {
				if err := w.Add(dir); err != nil {
					t.log.Warn("watching log directory failed", "dir", dir, "error", err)
					continue
				}
				watched++
			}
			if watched == 0 {
				t.log.Warn("no log directories watched, falling back to polling")
				_ = w.Close()
			} else {
				t.watcher = w
				t.watchRefs = make(map[string]int)
				defer func() { _ = w.Close() }()
			}
		}
	}
	var events <-chan fsnotify.Event
	var watchErrs <-chan error
	if t.watcher != nil {
		events = t.watcher.Events
		watchErrs = t.watcher.Errors
	}

	if t.cfg.PipelinedExport {
		t.exportCh = make(chan *inflight)
		go t.exportWorker()
	}

	t.scanDir(t.loadCheckpoints(), true)
	t.lastFlush = time.Now()

	dirTicker := time.NewTicker(2 * time.Second)
	defer dirTicker.Stop()
	poll := time.NewTicker(t.cfg.PollInterval)
	defer poll.Stop()
	// debounce coalesces bursts of write events into one dirty sweep.
	debounce := time.NewTimer(0)
	if !debounce.Stop() {
		<-debounce.C
	}
	defer debounce.Stop()

	for {
		select {
		case <-ctx.Done():
			// Settle any in-flight export and go synchronous for the final
			// drain, so the last flush commits before checkpointing.
			t.settleInflight()
			if t.exportCh != nil {
				close(t.exportCh)
				t.exportCh = nil
			}
			t.sweep(context.Background(), true)
			// Drain the pipelines; the emitted entries' offsets commit with
			// the final flush, so nothing is re-read after a restart.
			for _, f := range t.files {
				t.stopPipeline(context.Background(), f)
			}
			t.flush(context.Background())
			t.saveCheckpoints()
			return
		case <-dirTicker.C:
			t.scanDir(nil, false)
		case ev := <-events:
			if t.handleEvent(ev) {
				debounce.Reset(50 * time.Millisecond)
			}
		case err := <-watchErrs:
			t.log.Warn("fsnotify", "error", err)
		case <-debounce.C:
			t.sweep(ctx, false)
			t.housekeeping(ctx)
		case <-poll.C:
			t.sweep(ctx, true)
			t.housekeeping(ctx)
		}
	}
}

// housekeeping flushes, checkpoints and publishes status on their intervals.
func (t *Tailer) housekeeping(ctx context.Context) {
	t.pollInflight()
	if len(t.batch) > 0 && time.Since(t.lastFlush) >= t.cfg.FlushInterval {
		t.flush(ctx)
	}
	if (t.cfg.Positions != nil || t.cfg.CheckpointFile != "") && time.Since(t.lastCheckpoint) >= 10*time.Second {
		t.saveCheckpoints()
	}
	if time.Since(t.lastStatus) >= t.statusEvery {
		t.publishStatus()
	}
}

// handleEvent processes one fsnotify event; it reports whether a dirty sweep
// should be scheduled.
func (t *Tailer) handleEvent(ev fsnotify.Event) bool {
	dir := filepath.Dir(ev.Name)
	if _, isScanDir := t.scanDirs[dir]; isScanDir {
		// A file (or symlink) appeared/disappeared in a discovery directory:
		// rediscover immediately.
		if ev.Op&(fsnotify.Create|fsnotify.Remove|fsnotify.Rename) != 0 {
			t.scanDir(nil, false)
			return true
		}
		// The log file may live directly in the watched directory (no symlink
		// indirection): treat writes like target-dir events.
		if f, ok := t.files[ev.Name]; ok && ev.Op&fsnotify.Write != 0 {
			f.dirty = true
			return true
		}
		return false
	}
	// A write/create in a watched target directory: mark the files tailing
	// that directory (rotation creates a new file there, too).
	dirty := false
	for f := range t.byTargetDir[dir] {
		f.dirty = true
		dirty = true
	}
	return dirty
}

// watchTarget registers the file's resolved log directory with the watcher.
func (t *Tailer) watchTarget(f *file) {
	if t.watcher == nil || f.targetDir != "" {
		return
	}
	target, err := filepath.EvalSymlinks(f.path)
	if err != nil {
		return // next open retries
	}
	dir := filepath.Dir(target)
	if t.watchRefs[dir] == 0 {
		if err := t.watcher.Add(dir); err != nil {
			t.log.Debug("watching log target directory", "dir", dir, "error", err)
			return
		}
	}
	t.watchRefs[dir]++
	f.targetDir = dir
	if t.byTargetDir == nil {
		t.byTargetDir = make(map[string]map[*file]struct{})
	}
	set := t.byTargetDir[dir]
	if set == nil {
		set = make(map[*file]struct{})
		t.byTargetDir[dir] = set
	}
	set[f] = struct{}{}
}

// unwatchTarget releases the file's directory watch.
func (t *Tailer) unwatchTarget(f *file) {
	if t.watcher == nil || f.targetDir == "" {
		return
	}
	if t.watchRefs[f.targetDir]--; t.watchRefs[f.targetDir] <= 0 {
		delete(t.watchRefs, f.targetDir)
		_ = t.watcher.Remove(f.targetDir)
	}
	if set := t.byTargetDir[f.targetDir]; set != nil {
		delete(set, f)
		if len(set) == 0 {
			delete(t.byTargetDir, f.targetDir)
		}
	}
	f.targetDir = ""
}

// parseFileName extracts the container ID and namespace from a
// <pod>_<namespace>_<container>-<containerID>.log name.
func parseFileName(name string) (containerID, namespace string, ok bool) {
	name, found := strings.CutSuffix(name, ".log")
	if !found {
		return "", "", false
	}
	i := strings.LastIndexByte(name, '-')
	if i < 0 || i == len(name)-1 {
		return "", "", false
	}
	containerID = name[i+1:]
	if parts := strings.Split(name[:i], "_"); len(parts) == 3 {
		namespace = parts[1]
	}
	return containerID, namespace, true
}

// scanDir discovers new and removed log files across all sources by globbing
// their include patterns. checkpoints is non-nil only on the initial scan.
func (t *Tailer) scanDir(checkpoints map[string]checkpoint, initial bool) {
	seen := make(map[string]struct{})
	discovered := false
	listingOK := true
	defer func() {
		if !listingOK && !t.warnedListing {
			t.warnedListing = true
			t.log.Warn("a source glob failed; gone-detection is disabled until listings succeed")
		}
		if listingOK {
			t.warnedListing = false
		}
	}()
	for _, src := range t.sources {
		paths, ok := src.glob()
		if !ok {
			listingOK = false // a failed glob proves nothing about absent files
		}
		for _, path := range paths {
			if _, done := seen[path]; done {
				continue // an earlier source already claimed this file
			}
			if !src.matches(path) {
				continue // excluded
			}
			if st, err := os.Stat(path); err != nil || st.IsDir() {
				// A transient stat failure on a file we already track must
				// not mark it gone (drop would delete its checkpoint and a
				// rediscovery would re-ingest the whole file); only genuine
				// absence may.
				if err != nil && !os.IsNotExist(err) {
					if _, known := t.files[path]; known {
						seen[path] = struct{}{}
					}
				}
				continue
			}
			var id string
			if src.containerd {
				cid, namespace, ok := parseFileName(filepath.Base(path))
				if !ok || slices.Contains(t.cfg.ExcludeNamespaces, namespace) {
					continue
				}
				id = cid
			}
			seen[path] = struct{}{}
			if _, known := t.files[path]; known {
				continue
			}
			f := &file{
				path:        path,
				source:      src,
				containerID: id,
				compressed:  src.compressed || strings.HasSuffix(path, ".gz"),
				dirty:       true, // read on the first (event-driven) sweep
			}
			t.newPipeline(f)
			t.initFile(f, checkpoints, initial)
			t.files[path] = f
			discovered = true
		}
	}
	if listingOK {
		for path, f := range t.files {
			if _, ok := seen[path]; !ok {
				f.gone = true
			}
		}
	}
	obs.LogFiles.Set(float64(len(t.files)))
	if discovered && (t.cfg.Positions != nil || t.cfg.CheckpointFile != "") {
		// Persist immediately: until a file has a checkpoint entry, a crash
		// makes the restart treat it as pre-existing history and skip to its
		// end — the 10s periodic save left every new file a window in which
		// kill -9 lost its unread lines (and everything written while down).
		t.saveCheckpoints()
	}
}

// initFile seeds a newly discovered file's checkpoint/starting offset.
func (t *Tailer) initFile(f *file, checkpoints map[string]checkpoint, initial bool) {
	if !initial {
		return
	}
	if cp, ok := checkpoints[f.path]; ok {
		f.committed = cp.Offset
		f.inode = cp.Inode
		f.fp = fingerprint{Len: cp.FingerprintLen, Hash: cp.FingerprintHash}
		for _, pp := range cp.Pending {
			// A group straddled one or more rotations at shutdown/crash: its
			// prefixes are re-read from the rotated files (oldest first) before
			// this (new) inode is consumed. carriedFed is already false.
			f.carried = append(f.carried, rotatedPrefix{
				inode: pp.Inode,
				fp:    fingerprint{Len: pp.FingerprintLen, Hash: pp.FingerprintHash},
				from:  pp.From,
				to:    pp.To,
			})
		}
	} else if st, err := os.Stat(f.path); err == nil && !f.compressed {
		// Present before the agent started and no checkpoint: start at the end
		// to avoid re-ingesting history. Compressed archives are instead read
		// whole (committed stays 0).
		f.committed = st.Size()
	}
}

// newPipeline (re)creates the file's aggregation stages with empty state. A
// carried prefix (if any) is no longer present in the fresh pipeline and must
// be re-read before the current inode is consumed.
func (t *Tailer) newPipeline(f *file) {
	f.reset()
	f.keyStdout = f.containerID + "/stdout"
	f.keyStderr = f.containerID + "/stderr"
	if f.source.containerd {
		f.stStdout = f.state(f.keyStdout)
		f.stStderr = f.state(f.keyStderr)
		f.stPlain = f.state(f.containerID) // non-CRI passthrough lines
	} else {
		f.stStdout, f.stStderr = nil, nil
		f.stPlain = f.state(plainKey)
	}

	if f.source.multiline {
		f.traces = multiline.New(func(_ context.Context, e multiline.Entry[time.Time]) error {
			st := f.stateFor(e.Key)
			items := st.fifo
			// The multiline stage's line/byte caps can drop over-limit lines
			// without ever emitting them (their runs never complete), leaving
			// orphaned items that would freeze the watermark — and with it
			// this file's checkpoint — forever. The entry's first-line time
			// identifies the true head: timestamps are monotonic per stream,
			// so strictly-older leading items belong to dropped lines.
			for len(items) > 0 && items[0].when.Before(e.Data) {
				items = items[1:]
				obs.LogFifoDropped.Inc()
			}
			st.fifo = items
			n := min(e.Lines, len(items)) // Lines > len(items) must not happen; defensive
			if n == 0 {
				return nil
			}
			start, end := items[0].start, items[n-1].end
			st.fifo = items[n:]
			t.emit(f, entry{
				time: e.Data, stream: st.stream, body: e.Text,
				truncated: e.Truncated, match: e.Match, start: start, offset: end,
			})
			return nil
		}, multiline.WithMaxBytes(t.cfg.MaxEntryBytes), multiline.WithMaxLines(512))
	} else {
		f.traces = nil
	}

	// Containerd files run stage 1 (CRI P/F rejoin) ahead of the trace stage;
	// plain files feed the trace stage (or emit) directly from feedLine.
	// Emission is synchronous inside Add/Flush*, so the state's lastEnd is
	// exactly the end offset of the line's last fragment.
	if f.source.containerd {
		f.criStage = cri.New(func(ctx context.Context, key, line string, when time.Time, start int64) error {
			st := f.stateFor(key)
			var end int64
			if st.closed {
				// Deferred emission of an F-closed run: its boundaries were
				// pinned when the F line was fed (lastEnd has since moved on
				// to the line that triggered this flush). Both offsets are
				// ledger-side state, so a carried-rotation reanchor reaches
				// them (the cri-threaded start predates it).
				start, end = st.closedStart, st.closedEnd
				st.closed = false
				if st.hasNext {
					// Hand coverage over to the line that triggered the
					// flush; it is still buffered in the stage.
					st.runStart, st.hasRun, st.hasNext = st.nextStart, true, false
				} else {
					st.hasRun = false
				}
			} else {
				// Emission within the fed line's own AddParsed (single F,
				// passthrough) or a flush of an unclosed run: runStart is the
				// reanchor-aware first offset, lastEnd the newest line's end.
				if st.hasRun {
					start = st.runStart
				}
				st.hasRun = false
				end = st.lastEnd
			}
			if f.traces == nil {
				t.emit(f, entry{time: when, stream: st.stream, body: line, start: start, offset: end})
				return nil
			}
			st.fifo = append(st.fifo, logItem{start: start, end: end, when: when})
			return f.traces.AddAt(ctx, key, line, when, when)
		}, multiline.WithMaxBytes(t.cfg.MaxEntryBytes))
	} else {
		f.criStage = nil
	}
}

// emit appends one completed entry to the batch.
func (t *Tailer) emit(f *file, e entry) {
	e.file = f
	e.gen = f.gen
	t.batch = append(t.batch, e)
}

// streamOf extracts the stream from a pipeline key ("<id>/<stream>"); ""
// for non-CRI passthrough lines.
func streamOf(key string) string {
	if i := strings.LastIndexByte(key, '/'); i >= 0 {
		return key[i+1:]
	}
	return ""
}

// plainKey keys a plain file's single logical stream. It has no '/', so
// streamOf yields "" (plain files have no CRI stream). Each file owns its own
// pipeline, so one key per file is enough.
const plainKey = "line"

// feedLine pushes one raw physical line spanning [start, end) into the
// pipeline. Containerd files go through the CRI stage; plain files feed the
// trace stage (or emit) directly, sharing the same offset accounting so
// rotation and cross-rotation multi-line joining work identically.
func (t *Tailer) feedLine(ctx context.Context, f *file, raw string, start, end int64) {
	if !f.source.containerd {
		t.feedPlainLine(ctx, f, raw, start, end)
		return
	}
	st := f.stPlain // non-CRI passthrough
	l, ok := cri.Parse(raw)
	if ok {
		switch l.Stream {
		case "stdout":
			st = f.stStdout
		case "stderr":
			st = f.stStderr
		default:
			st = f.state(f.containerID + "/" + l.Stream)
		}
	}
	wasOpen := st.hasRun && !st.closed
	st.lastEnd = end
	if st.closed {
		// The pending closed run flushes inside this AddParsed; its callback
		// installs this line's registration afterwards (runStart must keep
		// pointing at the older, watermark-clamping offset until then).
		st.nextStart, st.hasNext = start, true
	} else if !st.hasRun {
		st.runStart, st.hasRun = start, true
	}
	if ok && !l.Partial && wasOpen {
		// The F line closes an open multi-fragment run. The stage defers the
		// emission to the next line fed for this key, so pin the run's own
		// boundaries now — at callback time lastEnd already belongs to that
		// next line.
		st.closed, st.closedStart, st.closedEnd = true, st.runStart, end
	}
	// AddParsed reuses this parse — the only one on the whole line's path.
	if err := f.criStage.AddParsed(ctx, f.containerID, raw, l, ok, start); err != nil {
		t.log.Warn("log pipeline", "path", f.path, "error", err)
	}
}

// feedPlainLine feeds one line of a non-containerd file. The record timestamp
// is the ingest time (enrich may override it from the line in flush). There is
// no stage-1 (CRI) buffer, so the fifo alone tracks the buffered lines and no
// runStart bookkeeping is needed: the line lands in the fifo before it is fed,
// so the watermark covers it until the trace stage emits it.
func (t *Tailer) feedPlainLine(ctx context.Context, f *file, raw string, start, end int64) {
	when := time.Now()
	if f.traces == nil {
		t.emit(f, entry{time: when, body: raw, start: start, offset: end})
		return
	}
	f.stPlain.fifo = append(f.stPlain.fifo, logItem{start: start, end: end, when: when})
	if err := f.traces.AddAt(ctx, plainKey, raw, when, when); err != nil {
		t.log.Warn("log pipeline", "path", f.path, "error", err)
	}
}

// stopPipeline drains both stages into the batch.
func (t *Tailer) stopPipeline(ctx context.Context, f *file) {
	if f.criStage != nil {
		_ = f.criStage.Stop(ctx)
	}
	if f.traces != nil {
		_ = f.traces.Stop(ctx)
	}
}

// sweep reads newly appended data; all sweeps every file (polling
// fallback), otherwise only files marked dirty by events are read.
func (t *Tailer) sweep(ctx context.Context, all bool) {
	cutoff := time.Now().Add(-t.cfg.MultilineTimeout)
	for path, f := range t.files {
		if f.gone {
			t.drop(f)
			delete(t.files, path)
			continue
		}
		if !all && !f.dirty {
			continue
		}
		if !f.resolved && !t.resolveMetadata(ctx, f) {
			continue
		}
		f.dirty = false
		if err := t.readFile(ctx, f); err != nil && !errors.Is(err, os.ErrNotExist) {
			t.log.Warn("reading log file", "path", path, "error", err)
		}
		// Age out fragment runs and multi-line groups that never completed.
		if f.criStage != nil {
			_ = f.criStage.FlushBefore(ctx, cutoff)
		}
		if f.traces != nil {
			_ = f.traces.FlushBefore(ctx, cutoff)
		}
		if len(t.batch) >= t.cfg.BatchSize {
			t.flush(ctx)
		}
	}
}

// resolveMetadata builds the file's resource attributes. Plain files resolve
// immediately from the source's static attributes plus node metadata;
// containerd files fetch pod metadata from the service (backing off between
// attempts), and are not consumed until it is available — the data waits on
// disk, nothing is lost.
func (t *Tailer) resolveMetadata(ctx context.Context, f *file) bool {
	if !f.source.containerd {
		return t.resolvePlain(f)
	}
	if time.Now().Before(f.nextMetaTry) {
		return false
	}
	md, err := t.cfg.Metadata.Container(ctx, f.containerID, t.cfg.MetadataWait)
	if err != nil {
		f.nextMetaTry = time.Now().Add(10 * time.Second)
		if metaclient.IsNotFound(err) {
			t.log.Debug("container metadata not found yet", "id", f.containerID)
		} else {
			t.log.Warn("fetching container metadata", "id", f.containerID, "error", err)
		}
		return false
	}
	res := pcommon.NewResource()
	actx := attrs.Context{Pod: &md.Pod, Container: &md.Container}
	if t.cfg.NodeInfo != nil {
		actx.Node = t.cfg.NodeInfo()
	}
	t.cfg.Attrs.Build(res, actx)
	f.resource = res
	f.resolved = true
	return true
}

// resolvePlain builds a non-containerd file's resource: node attributes from
// the builder plus the source's configured static attributes (which win). A
// source without an explicit service.name defaults it to the source name.
func (t *Tailer) resolvePlain(f *file) bool {
	res := pcommon.NewResource()
	actx := attrs.Context{}
	if t.cfg.NodeInfo != nil {
		actx.Node = t.cfg.NodeInfo()
	}
	t.cfg.Attrs.Build(res, actx)
	a := res.Attributes()
	if _, ok := f.source.attributes["service.name"]; !ok && f.source.name != "" {
		if _, set := a.Get("service.name"); !set {
			a.PutStr("service.name", f.source.name)
		}
	}
	for k, v := range f.source.attributes {
		a.PutStr(k, v)
	}
	f.resource = res
	f.resolved = true
	return true
}

// readFile ingests up to MaxBytesPerSweep appended bytes and detects
// rotation.
func (t *Tailer) readFile(ctx context.Context, f *file) error {
	if f.compressed {
		return t.readArchive(ctx, f)
	}
	if err := t.ensureOpen(f); err != nil {
		return err
	}
	// A group straddled a rename rotation and the pipeline was since discarded
	// (rewind or restart): re-read the rotated-away prefix before the new inode
	// so the group reconstructs.
	if f.carried != nil && !f.carriedFed {
		t.feedCarriedPrefix(ctx, f)
	}

	// A paused (rate-limited) file first retries its retained pending bytes;
	// reading resumes only once they drain.
	if f.limited {
		t.consume(ctx, f, false)
	}
	budget := t.cfg.MaxBytesPerSweep
	buf := t.scratch()
	read := 0
	for budget > 0 && !f.limited {
		limit := min(len(buf), budget)
		n, err := f.f.Read(buf[:limit])
		if n > 0 {
			budget -= n
			read += n
			obs.LogBytes.Add(float64(n))
			f.pending = append(f.pending, buf[:n]...)
			f.readPos += int64(n)
			t.consume(ctx, f, false)
		}
		if err != nil {
			if !errors.Is(err, io.EOF) {
				return err
			}
			break
		}
	}

	// Rotation/truncation detection. The rotation machinery must see settled
	// export state (a failed in-flight export rewinds this file first).
	st, err := os.Stat(f.path)
	if err != nil {
		return err
	}
	if inodeOf(st) != f.inode || st.Size() < f.readPos ||
		(read == 0 && !st.ModTime().Equal(f.lastMod) && !f.fp.matches(f.f)) {
		t.settle(f)
	}
	switch {
	case inodeOf(st) != f.inode:
		// Rename rotation: the path names a new file. Drain what the old
		// writer appended after our last read, then switch — carrying a
		// straddling multi-line group across the boundary.
		t.drainFile(ctx, f)
		t.reopen(ctx, f, true)
	case st.Size() < f.readPos:
		// In-place truncation: the unread tail is gone; restart at zero.
		// (Draining would read the replacement content mid-stream.)
		t.reopen(ctx, f, false)
	case read == 0 && !st.ModTime().Equal(f.lastMod) && !f.fp.matches(f.f):
		// The file changed without yielding new bytes past our offset and
		// its head no longer matches: truncated and rewritten to a size at
		// or beyond our position (same-size copytruncate). Restart.
		t.reopen(ctx, f, false)
	}
	f.lastMod = st.ModTime()
	return nil
}

// readArchive reads a gzip archive: bounded decompressed bytes per sweep, fed
// through the same pipeline (offsets are decompressed positions). gzip is not
// seekable, so on resume openArchive re-decompresses from the start and
// discards the committed prefix; at EOF the pipeline is drained and the file
// is done (committed == readPos means a restart discards everything).
func (t *Tailer) readArchive(ctx context.Context, f *file) error {
	if f.gz == nil {
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
	if f.limited {
		t.consume(ctx, f, false)
	}
	budget := t.cfg.MaxBytesPerSweep
	buf := t.scratch()
	for budget > 0 && !f.limited {
		n, err := f.gz.Read(buf[:min(len(buf), budget)])
		if n > 0 {
			budget -= n
			obs.LogBytes.Add(float64(n))
			f.pending = append(f.pending, buf[:n]...)
			f.readPos += int64(n)
			t.consume(ctx, f, false)
		}
		if err != nil {
			if errors.Is(err, io.EOF) {
				t.stopPipeline(ctx, f) // drain a trailing multi-line group
				t.closeArchive(f)
				// Record the consumed archive's identity so idle sweeps skip
				// it instead of re-decompressing it from scratch.
				if st, statErr := os.Stat(f.path); statErr == nil {
					f.archiveDone = true
					f.archiveSize = st.Size()
					f.archiveMod = st.ModTime()
				}
			}
			return nil
		}
	}
	return nil // hit the sweep budget; continue next sweep with f.gz retained
}

// openArchive opens the gzip file and positions it at the committed offset by
// discarding that many decompressed bytes.
func (t *Tailer) openArchive(f *file) error {
	fh, err := os.Open(f.path)
	if err != nil {
		return err
	}
	st, err := fh.Stat()
	if err != nil {
		_ = fh.Close()
		return err
	}
	inode := inodeOf(st)
	// A replaced file (different inode or head fingerprint) restarts at zero.
	if f.inode != 0 && (f.inode != inode || !f.fp.matches(fh)) {
		f.committed = 0
	}
	gz, err := gzip.NewReader(fh)
	if err != nil {
		_ = fh.Close()
		return fmt.Errorf("gzip %s: %w", f.path, err)
	}
	if f.committed > 0 {
		if _, err := io.CopyN(io.Discard, gz, f.committed); err != nil && !errors.Is(err, io.EOF) {
			_ = gz.Close()
			_ = fh.Close()
			return err
		}
	}
	if fp, err := computeFingerprint(fh, min(int64(t.cfg.FingerprintBytes), st.Size())); err == nil {
		f.fp = fp
	}
	f.f = fh
	f.gz = gz
	f.inode = inode
	f.readPos = f.committed
	f.lineStart = f.committed
	f.pending = f.pending[:0]
	t.watchTarget(f)
	return nil
}

// drainArchive finishes reading a mid-read archive from its still-open fd
// (the file may already be unlinked) so its remainder is not lost when the
// file drops.
func (t *Tailer) drainArchive(ctx context.Context, f *file) {
	if f.gz == nil {
		return
	}
	buf := t.scratch()
	if len(f.pending) > 0 {
		t.consume(ctx, f, true) // see drainFile: a paused file's pending would be lost
	}
	// Same circuit breaker as drainFile: an adversarial archive (a gzip bomb
	// deleted mid-read) must not decompress without bound into memory.
	const drainCap = 1 << 30
	var drained int64
	for drained < drainCap {
		n, err := f.gz.Read(buf)
		if n > 0 {
			drained += int64(n)
			obs.LogBytes.Add(float64(n))
			f.pending = append(f.pending, buf[:n]...)
			f.readPos += int64(n)
			// Bypass the rate limit like drainFile: pausing would lose the
			// remainder when the fd closes.
			t.consume(ctx, f, true)
			t.flushDuringDrain(ctx, f)
		}
		if err != nil {
			return
		}
	}
	t.log.Error("archive still yielding after draining 1GiB, abandoning remainder", "path", f.path)
}

// closeArchive releases the archive's readers.
func (t *Tailer) closeArchive(f *file) {
	if f.gz != nil {
		_ = f.gz.Close()
		f.gz = nil
	}
	if f.f != nil {
		_ = f.f.Close()
		f.f = nil
	}
}

// drainFile reads the (rotated-away or removed) file to EOF so no bytes
// written between our last read and the rotation are lost. Bounded to keep a
// still-active writer from pinning the sweep.
func (t *Tailer) drainFile(ctx context.Context, f *file) {
	if f.f == nil {
		return
	}
	// Drain to EOF: whatever is left in the rotated-away inode is unreachable
	// once the fd closes, so a byte budget here means permanent loss (a
	// backlog over the budget is realistic — kubelet rotates at 10MiB, and
	// rate-limit pause mode accumulates arbitrary backlogs). The cap below is
	// only a circuit breaker against a pathological writer that keeps the old
	// fd open and outruns us forever.
	const drainCap = 1 << 30
	buf := t.scratch()
	if len(f.pending) > 0 {
		// A rate-limit-paused file may hold already-read unconsumed lines;
		// they would be discarded with pending when the fd drops.
		t.consume(ctx, f, true)
	}
	var drained int64
	for drained < drainCap {
		n, err := f.f.Read(buf)
		if n > 0 {
			drained += int64(n)
			f.pending = append(f.pending, buf[:n]...)
			f.readPos += int64(n)
			// Bypass the rate limit: pausing a drain would lose the rotated
			// inode's remainder when the fd is dropped.
			t.consume(ctx, f, true)
			t.flushDuringDrain(ctx, f)
		}
		if err != nil {
			return
		}
	}
	t.log.Error("rotated file still growing after draining 1GiB, abandoning remainder", "path", f.path)
}

// flushDuringDrain keeps a large drain from accumulating everything into one
// batch (and one OTLP payload, likely over the collector's receive limit) and
// from starving the sweep for the drain's whole duration.
func (t *Tailer) flushDuringDrain(ctx context.Context, f *file) {
	if len(t.batch) >= t.cfg.BatchSize {
		t.flush(ctx)
	}
}

// consume splits pending bytes into physical lines and feeds the pipeline.
// unlimited bypasses the per-file rate limit (rotation drains, where pausing
// would lose the remainder of the rotated-away inode).
func (t *Tailer) consume(ctx context.Context, f *file, unlimited bool) {
	for {
		i := bytes.IndexByte(f.pending, '\n')
		if i < 0 {
			// Bound the carried incomplete physical line.
			if len(f.pending) > t.cfg.MaxEntryBytes+4096 {
				f.lineStart += int64(len(f.pending))
				f.pending = f.pending[:0]
			}
			return
		}
		if !unlimited && !t.allowLine(f) {
			if !t.cfg.RateDrop {
				// Pause: keep pending, stop reading until tokens refill.
				if !f.limited {
					f.limited = true
					obs.LogRateLimited.WithLabelValues("pause").Inc()
				}
				return
			}
			// Drop: discard the line, keep consuming.
			f.pending = f.pending[i+1:]
			f.lineStart += int64(i + 1)
			obs.LogRateLimited.WithLabelValues("drop").Inc()
			continue
		}
		f.limited = false
		line := f.pending[:i]
		start := f.lineStart
		f.pending = f.pending[i+1:]
		f.lineStart += int64(i + 1)

		if len(line) == 0 {
			continue
		}
		t.feedLine(ctx, f, string(line), start, f.lineStart)
	}
}

// allowLine takes one token from the file's rate-limit bucket, refilling it by
// elapsed time first. Always true when rate limiting is off.
func (t *Tailer) allowLine(f *file) bool {
	if t.cfg.RateLimit <= 0 {
		return true
	}
	now := time.Now()
	if f.lastRefill.IsZero() {
		f.tokens = t.cfg.RateBurst
	} else {
		f.tokens = min(t.cfg.RateBurst, f.tokens+now.Sub(f.lastRefill).Seconds()*t.cfg.RateLimit)
	}
	f.lastRefill = now
	if f.tokens < 1 {
		return false
	}
	f.tokens--
	return true
}

// ensureOpen opens the file at the committed offset on first use. The
// offset is only honored when the file's identity (inode and fingerprint)
// still matches; otherwise the path names a different file and reading
// starts from the top.
func (t *Tailer) ensureOpen(f *file) error {
	if f.f != nil {
		return nil
	}
	fh, err := os.Open(f.path)
	if err != nil {
		return err
	}
	st, err := fh.Stat()
	if err != nil {
		_ = fh.Close()
		return err
	}
	inode := inodeOf(st)
	start := f.committed
	if f.inode != 0 && (f.inode != inode || !f.fp.matches(fh)) {
		start = 0
	}
	if start > st.Size() {
		start = 0
	}
	if _, err := fh.Seek(start, 0); err != nil {
		_ = fh.Close()
		return err
	}
	fp, err := computeFingerprint(fh, min(int64(t.cfg.FingerprintBytes), st.Size()))
	if err != nil {
		_ = fh.Close()
		return err
	}
	f.f = fh
	f.inode = inode
	f.fp = fp
	f.readPos = start
	f.lineStart = start
	f.committed = start
	f.pending = f.pending[:0]
	t.watchTarget(f)
	return nil
}

// reopen switches to the file now at the path and resets the byte position so
// the next sweep reads the new inode from offset 0. The file is marked dirty
// so an event-driven loop picks it up immediately.
//
// On a rename rotation (renamed) where a multi-line group still straddles the
// boundary — data remains buffered in the pipeline after the old inode was
// drained — the pipeline is carried across instead of flushed, so the group
// joins the pre- and post-rotation lines into one record. The buffered offsets
// are re-anchored to the new inode's origin, the rotation generation bumps
// (so the already-drained pre-rotation entries do not advance the new inode's
// checkpoint), and the rotated-away file is recorded in f.carried so a crash
// before the group exports can re-read its tail on restart.
//
// Otherwise (truncation, copytruncate, or a rename with nothing buffered) the
// pipeline is flushed and reset as before — carrying makes no sense when the
// content was replaced.
func (t *Tailer) reopen(ctx context.Context, f *file, renamed bool) {
	obs.LogRotations.Inc()
	if f.f != nil {
		_ = f.f.Close()
		f.f = nil
	}
	if _, buffered := f.watermark(); renamed && buffered {
		// Append this rotation's tail; a group straddling several rotations
		// accumulates one entry per hop, all re-readable on crash.
		f.carried = append(f.carried, rotatedPrefix{inode: f.inode, fp: f.fp, from: f.committed, to: f.readPos})
		f.carriedFed = true // the prefixes are already live in the pipeline
		f.reanchor()
		f.gen++
	} else {
		t.stopPipeline(ctx, f)
		t.newPipeline(f)
		f.carried = nil
		if renamed && f.readPos > f.committed {
			// The drained range [committed, readPos) exists only as batch
			// entries now; if their export fails (or the process crashes),
			// the rotated-away file is the only copy. Record it so
			// feedCarriedPrefix can re-read it — carriedFed stays true, so
			// nothing is re-read unless a rewind/restart resets it; on a
			// successful export the commit clears it.
			f.carried = append(f.carried, rotatedPrefix{inode: f.inode, fp: f.fp, from: f.committed, to: f.readPos})
			f.carriedFed = true
		}
		// The entries stopPipeline just emitted carry old-content offsets; the
		// new inode starts at 0, so those offsets must not drive its
		// checkpoint. Bumping the generation makes flush's gen check discard
		// them for commit purposes (they still export).
		f.gen++
	}
	f.inode = 0
	f.fp = fingerprint{}
	f.committed = 0
	f.readPos = 0
	f.lineStart = 0
	f.pending = f.pending[:0]
	f.limited = false // pending is gone; a paused file must resume reading
	// Re-derive the watch: the symlink may now point into a different
	// directory (kept stale, events for the new target dir would be missed
	// until the poll ticker.)
	t.unwatchTarget(f)
	f.dirty = true
}

// feedCarriedPrefix re-reads the rotated-away files' tails (the unexported
// prefix of a straddling group) and feeds them, oldest first, into the fresh
// pipeline so the group reconstructs before the new inode's continuation is
// consumed. Fed at the pre-rotation generation and then re-anchored + bumped,
// exactly as the live rotation does, so the prefix offsets never advance the
// new inode's checkpoint. A prefix whose rotated file can no longer be found
// (already deleted/compressed by the runtime) is skipped — that segment is
// genuinely gone from disk.
func (t *Tailer) feedCarriedPrefix(ctx context.Context, f *file) {
	f.carriedFed = true
	defer func() {
		// Re-anchor + bump so the new inode is consumed at a fresh generation,
		// matching reopen's carry path.
		f.reanchor()
		f.gen++
	}()
	for i := range f.carried {
		t.feedPrefix(ctx, f, f.carried[i])
	}
}

// feedPrefix re-reads one rotated file's [from,to) range and feeds its lines
// into the pipeline.
func (t *Tailer) feedPrefix(ctx context.Context, f *file, p rotatedPrefix) {
	path, ok := t.findRotated(f, p)
	if !ok {
		t.log.Warn("carried log prefix source not found; multi-line group across rotation may be truncated",
			"path", f.path, "inode", p.inode)
		return
	}
	fh, err := os.Open(path)
	if err != nil {
		t.log.Warn("opening carried log prefix", "path", path, "error", err)
		return
	}
	defer func() { _ = fh.Close() }()
	if _, err := fh.Seek(p.from, 0); err != nil {
		t.log.Warn("seeking carried log prefix", "path", path, "error", err)
		return
	}

	remaining := p.to - p.from
	var carry []byte
	pos := p.from
	buf := t.scratch()
	for remaining > 0 {
		n, rerr := fh.Read(buf[:min(int64(len(buf)), remaining)])
		if n > 0 {
			remaining -= int64(n)
			carry = append(carry, buf[:n]...)
			for {
				i := bytes.IndexByte(carry, '\n')
				if i < 0 {
					break
				}
				line := carry[:i]
				start := pos
				pos += int64(i + 1)
				carry = carry[i+1:]
				if len(line) > 0 {
					t.feedLine(ctx, f, string(line), start, pos)
				}
			}
		}
		if rerr != nil {
			break
		}
	}
}

// findRotated locates the rotated-away file matching p's identity in the log's
// resolved target directory (where the runtime keeps rotated files).
func (t *Tailer) findRotated(f *file, p rotatedPrefix) (string, bool) {
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

// drop closes a removed file, draining first the fd (bytes appended since
// the last read) and then the pipeline.
func (t *Tailer) drop(f *file) {
	t.settle(f) // a failed in-flight export must rewind before we drain
	if f.resolved {
		if f.compressed {
			// A large archive is read incrementally across sweeps; a deletion
			// mid-read leaves the rest readable from the open fd.
			t.drainArchive(context.Background(), f)
		} else {
			t.drainFile(context.Background(), f)
		}
		t.stopPipeline(context.Background(), f)
	}
	if f.compressed {
		t.closeArchive(f)
	} else if f.f != nil {
		_ = f.f.Close()
		f.f = nil
	}
	t.unwatchTarget(f)
}

// logGrouper places records into ResourceLogs/ScopeLogs keyed by (file,
// resource attributes, scope attributes), so line-derived resource/scope
// attributes split records into the right resources. Without line attributes
// there is one scope per file (the plain map — the common case, avoiding the
// per-record key formatting), matching the previous behavior.
type logGrouper struct {
	ld     plog.Logs
	plain  map[*file]plog.ScopeLogs
	scopes map[scopeKey]plog.ScopeLogs
}

// scopeKey identifies one (file, line-derived resource attrs, scope attrs)
// group without the fmt.Sprintf allocation the old string key paid per
// attribute-carrying record.
type scopeKey struct {
	f          *file
	res, scope string
}

func (g *logGrouper) scope(f *file, resAttrs, scopeAttrs []logattrs.Attr) plog.ScopeLogs {
	if len(resAttrs) == 0 && len(scopeAttrs) == 0 {
		if sl, ok := g.plain[f]; ok {
			return sl
		}
		sl := g.newScope(f, nil, nil)
		g.plain[f] = sl
		return sl
	}
	key := scopeKey{f: f, res: logattrs.Key(resAttrs), scope: logattrs.Key(scopeAttrs)}
	if sl, ok := g.scopes[key]; ok {
		return sl
	}
	sl := g.newScope(f, resAttrs, scopeAttrs)
	g.scopes[key] = sl
	return sl
}

func (g *logGrouper) newScope(f *file, resAttrs, scopeAttrs []logattrs.Attr) plog.ScopeLogs {
	rl := g.ld.ResourceLogs().AppendEmpty()
	f.resource.CopyTo(rl.Resource())
	logattrs.Put(rl.Resource().Attributes(), resAttrs)
	sl := rl.ScopeLogs().AppendEmpty()
	sl.Scope().SetName("github.com/JohanLindvall/kubescrape/agent/tailer")
	logattrs.Put(sl.Scope().Attributes(), scopeAttrs)
	return sl
}

// metricResolver resolves metric label/value keys for one record: the record's
// attributes (line-derived + enriched) first, then the file's resource
// attributes (k8s metadata). The two closures are bound once per flush; per
// record only the rec/res fields change, so record evaluation allocates no
// closures.
type metricResolver struct {
	rec, res pcommon.Map
	sev      string // lowercased severity text, for __severity__
	labelFn  func(string) string
	valueFn  func(string) (float64, bool)
	ruleFn   func(string) string
}

func newMetricResolver() *metricResolver {
	r := &metricResolver{}
	r.labelFn = r.label
	r.valueFn = r.value
	r.ruleFn = r.ruleLookup
	return r
}

// ruleLookup is the label resolver for log rules: the synthetic __severity__
// key (the enriched severity text, lowercased) plus the usual record/resource
// attribute chain.
func (r *metricResolver) ruleLookup(k string) string {
	if k == "__severity__" {
		return r.sev
	}
	return r.label(k)
}

func (r *metricResolver) lookup(k string) (pcommon.Value, bool) {
	if v, ok := r.rec.Get(k); ok {
		return v, true
	}
	return r.res.Get(k)
}

func (r *metricResolver) label(k string) string {
	if v, ok := r.lookup(k); ok {
		return v.AsString()
	}
	return ""
}

func (r *metricResolver) value(k string) (float64, bool) {
	v, ok := r.lookup(k)
	if !ok {
		return 0, false
	}
	switch v.Type() {
	case pcommon.ValueTypeDouble:
		return v.Double(), true
	case pcommon.ValueTypeInt:
		return float64(v.Int()), true
	default:
		f, err := strconv.ParseFloat(v.AsString(), 64)
		return f, err == nil
	}
}

// flush exports the batch. On success offsets are committed; on failure the
// files are rewound to the committed offsets so the data is re-read.
func (t *Tailer) flush(ctx context.Context) {
	// Apply the previous pipelined export's result first: a failure rewinds
	// its files and purges their read-ahead entries from this batch.
	t.settleInflight()
	if len(t.batch) == 0 {
		t.lastFlush = time.Now()
		return
	}
	ld := plog.NewLogs()
	g := &logGrouper{ld: ld, plain: map[*file]plog.ScopeLogs{}, scopes: map[scopeKey]plog.ScopeLogs{}}
	maxOffsets := make(map[*file]int64)
	gens := make(map[*file]int)
	touched := make(map[*file]struct{})
	now := pcommon.NewTimestampFromTime(time.Now())
	// Per-file bound metric state (resource hash computed once per file) and
	// one reusable key resolver for the whole flush.
	var bound map[*file]metrics.BoundResource
	var resolver *metricResolver
	if t.cfg.LogMetrics != nil || t.cfg.Rules != nil {
		bound = make(map[*file]metrics.BoundResource)
		resolver = newMetricResolver()
	}
	// With rules configured, records are built in a one-record scratch slice
	// and only MOVED into the batch when kept, so drops never materialize a
	// resource/scope. Without rules they are built in place, as before.
	var scratch plog.LogRecordSlice
	if t.cfg.Rules != nil {
		scratch = plog.NewLogs().ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords()
	}
	kept := 0
	for _, e := range t.batch {
		// Extract configured line attributes; resource/scope ones drive the
		// grouping so records land under the right ResourceLogs/ScopeLogs.
		var extracted logattrs.Result
		if t.cfg.LogAttrs != nil {
			extracted = t.cfg.LogAttrs.Extract(e.body)
		}
		var lr plog.LogRecord
		if t.cfg.Rules != nil {
			lr = scratch.AppendEmpty()
		} else {
			lr = g.scope(e.file, extracted.Resource, extracted.Scope).LogRecords().AppendEmpty()
			kept++
		}
		lr.SetTimestamp(pcommon.NewTimestampFromTime(e.time))
		lr.SetObservedTimestamp(now)
		lr.Body().SetStr(e.body)
		if e.stream != "" {
			lr.Attributes().PutStr("log.iostream", e.stream)
		}
		if e.truncated {
			lr.Attributes().PutBool("log.truncated", true)
		}
		if e.match != "" {
			lr.Attributes().PutStr("log.multiline.match", e.match)
		}
		if t.cfg.FileAttributes {
			lr.Attributes().PutStr("log.file.name", filepath.Base(e.file.path))
			lr.Attributes().PutInt("log.file.position", e.start)
		}
		logattrs.Put(lr.Attributes(), extracted.Log)
		if t.cfg.Enrich {
			logenrich.Apply(lr, e.body)
		}
		if t.cfg.LogMetrics != nil {
			// Metric label/value keys resolve against the record's attributes
			// (line-derived + enriched) first, then the file's resource
			// attributes (k8s metadata); the file's resource attributes become
			// the metric's OTLP resource (hashed once per file via Bind).
			b, ok := bound[e.file]
			if !ok {
				b = t.cfg.LogMetrics.Bind(e.file.resource.Attributes())
				bound[e.file] = b
			}
			resolver.rec, resolver.res = lr.Attributes(), e.file.resource.Attributes()
			b.Add(resolver.valueFn, resolver.labelFn, e.body)
		}
		if t.cfg.Rules != nil {
			resolver.rec, resolver.res = lr.Attributes(), e.file.resource.Attributes()
			resolver.sev = strings.ToLower(lr.SeverityText())
			if t.cfg.Rules.Keep(resolver.ruleFn, e.body) {
				scratch.MoveAndAppendTo(g.scope(e.file, extracted.Resource, extracted.Scope).LogRecords())
				kept++
			} else {
				scratch.RemoveIf(func(plog.LogRecord) bool { return true })
				obs.LogRulesDropped.Inc()
			}
		}
		touched[e.file] = struct{}{}
		gens[e.file] = e.file.gen
		// Only entries of the file's current rotation generation advance its
		// checkpoint; pre-rotation entries carry old-inode offsets recoverable
		// via file.carried and must not move the new inode's commit point.
		if e.gen == e.file.gen && e.offset > maxOffsets[e.file] {
			maxOffsets[e.file] = e.offset
		}
	}

	inf := &inflight{
		ctx: ctx, ld: ld, kept: kept,
		offsets: maxOffsets, gens: gens, touched: touched,
		done: make(chan struct{}),
	}
	clear(t.batch) // unpin the exported bodies (a burst otherwise stays reachable)
	t.batch = t.batch[:0]
	t.lastFlush = time.Now()
	// An all-dropped batch has nothing to send but its offsets still commit.
	if kept > 0 && t.exportCh != nil {
		// Pipelined: hand off and keep reading; the result is applied at the
		// next flush (or when a rotation/drop settles it earlier).
		t.inflight = inf
		t.exportCh <- inf
		return
	}
	if kept > 0 {
		inf.err = t.exportWithRetry(ctx, ld)
	}
	if inf.err != nil {
		t.failBatch(inf)
	} else {
		t.commitBatch(inf)
	}
}

func (t *Tailer) exportWithRetry(ctx context.Context, ld plog.Logs) error {
	var err error
	backoff := t.retryBackoff
	for attempt := 0; attempt < 3; attempt++ {
		if err = t.cfg.Exporter.ExportLogs(ctx, ld); err == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return err
		case <-time.After(backoff):
		}
		backoff *= 2
	}
	return err
}

// rewind seeks a file back to its committed offset so unexported data is
// read again. Pipeline state is discarded without emitting: the buffered
// lines sit after the committed offset and will be re-read and re-fed.
func (t *Tailer) rewind(f *file) {
	if f.compressed {
		// gzip is not seekable: close so openArchive re-decompresses from the
		// committed offset next sweep. archiveDone must reset with it — the
		// rewound range needs re-reading even though the file is unchanged.
		t.closeArchive(f)
		f.archiveDone = false
		f.readPos = f.committed
		f.lineStart = f.committed
		f.pending = f.pending[:0]
		f.limited = false // pending is gone; a paused file must resume reading
		t.newPipeline(f)
		return
	}
	if f.f == nil {
		return
	}
	if _, err := f.f.Seek(f.committed, 0); err != nil {
		_ = f.f.Close()
		f.f = nil
		return
	}
	f.readPos = f.committed
	f.lineStart = f.committed
	f.pending = f.pending[:0]
	f.limited = false // pending is gone; a paused file must resume reading
	t.newPipeline(f)
}

// --- checkpoints ---

// checkpoint is one file's persisted position (shared shape with the
// unified positions store).
type checkpoint = positions.LogPos

func (t *Tailer) loadCheckpoints() map[string]checkpoint {
	if t.cfg.Positions != nil {
		return t.cfg.Positions.Logs()
	}
	if t.cfg.CheckpointFile == "" {
		return nil
	}
	data, err := os.ReadFile(t.cfg.CheckpointFile)
	if err != nil {
		return nil
	}
	var cps map[string]checkpoint
	if err := json.Unmarshal(data, &cps); err != nil {
		t.log.Warn("ignoring corrupt checkpoint file", "error", err)
		return nil
	}
	return cps
}

func (t *Tailer) saveCheckpoints() {
	t.lastCheckpoint = time.Now()
	if t.cfg.Positions == nil && t.cfg.CheckpointFile == "" {
		return
	}
	cps := make(map[string]checkpoint, len(t.files))
	for path, f := range t.files {
		// Extend the fingerprint once the file has grown past the initial
		// hash length, up to the configured size.
		if f.f != nil && f.fp.Len < int64(t.cfg.FingerprintBytes) {
			if st, err := f.f.Stat(); err == nil && st.Size() > f.fp.Len {
				if fp, err := computeFingerprint(f.f, min(int64(t.cfg.FingerprintBytes), st.Size())); err == nil {
					f.fp = fp
				}
			}
		}
		cp := checkpoint{
			Offset: f.committed, Inode: f.inode,
			FingerprintLen: f.fp.Len, FingerprintHash: f.fp.Hash,
		}
		for _, c := range f.carried {
			cp.Pending = append(cp.Pending, positions.Prefix{
				Inode:           c.inode,
				FingerprintLen:  c.fp.Len,
				FingerprintHash: c.fp.Hash,
				From:            c.from,
				To:              c.to,
			})
		}
		cps[path] = cp
	}
	if t.cfg.Positions != nil {
		if err := t.cfg.Positions.SetLogs(cps); err != nil {
			t.log.Warn("writing positions file", "error", err)
		}
		return
	}
	data, err := json.Marshal(cps)
	if err != nil {
		return
	}
	tmp := t.cfg.CheckpointFile + ".tmp"
	if err := writeFileSync(tmp, data); err != nil {
		t.log.Warn("writing checkpoint file", "error", err)
		return
	}
	if err := os.Rename(tmp, t.cfg.CheckpointFile); err != nil {
		t.log.Warn("replacing checkpoint file", "error", err)
	}
}

// writeFileSync is os.WriteFile plus an fsync before close, so the rename
// that follows cannot surface a zero-length file after a power loss.
func writeFileSync(path string, data []byte) error {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	return f.Close()
}

func inodeOf(st os.FileInfo) uint64 {
	if sys, ok := st.Sys().(*syscall.Stat_t); ok {
		return sys.Ino
	}
	return 0
}
