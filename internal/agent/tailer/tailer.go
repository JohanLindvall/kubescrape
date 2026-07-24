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
	"context"
	"errors"
	"log/slog"
	"os"
	"sync/atomic"
	"time"

	"github.com/fsnotify/fsnotify"
	"go.opentelemetry.io/collector/pdata/plog"

	"github.com/JohanLindvall/kubescrape/internal/agent/attrs"
	"github.com/JohanLindvall/kubescrape/internal/agent/logscrub"
	"github.com/JohanLindvall/kubescrape/internal/agent/positions"
	"github.com/JohanLindvall/kubescrape/internal/logline"
	"github.com/JohanLindvall/kubescrape/internal/metrics"
	"github.com/JohanLindvall/kubescrape/pkg/kubemeta"
	"github.com/JohanLindvall/kubescrape/pkg/logattrs"
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
	Sources []Source
	// Positions, when set, persists committed offsets (and, agent-wide, the
	// journald cursor) to the shared positions store; nil disables
	// persistence.
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
	// log.file.position (the record's START byte offset) on every emitted
	// record, for any file source. Opt-in.
	FileAttributes bool
	// Scrub redacts sensitive values from log bodies before anything copies
	// from them (nil disables).
	Scrub *logscrub.Scrubber
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

	// IdleClose closes the file descriptor of a fully-caught-up file after
	// this much inactivity. It bounds steady-state fd usage at one per ACTIVE
	// file rather than one per tracked file — but it FORFEITS THE ZERO-LOSS
	// GUARANTEE, so it is off (0) by default.
	//
	// The open fd is the only handle to an inode once its name is gone: it is
	// what lets drainFile read the remainder of a rotated-away or unlinked
	// file. With the fd released, lines written after the close and before the
	// tailer next reads (a container's final lines, say, followed by the
	// kubelet removing its log) are unrecoverable — the path no longer leads
	// to that inode. Enable it only where bounding fds on a node with
	// thousands of log files matters more than the tail of a dying file.
	IdleClose time.Duration

	// UnknownFiles decides where a file present at startup WITHOUT a
	// checkpoint entry starts: "end" (skip as pre-existing history), "start"
	// (read whole), or "auto" (default: "start" when the checkpoint store
	// already has entries — the file appeared while the agent was down, so
	// its content is unshipped — and "end" on a first-ever run). Note "auto"
	// and "start" mean adding a new source to a long-running agent ingests
	// those files' existing content.
	UnknownFiles string
	// Rules filters exported records (ordered keep/drop/sample, nil = keep
	// all). Evaluated after enrichment — severity is matchable via the
	// synthetic __severity__ key — and after LogMetrics, so metrics still see
	// every line. Dropped records advance offsets like exported ones.
	Rules *logline.LineFilter
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
	lastIdleScan   time.Time
	lastFlush      time.Time
	lastCheckpoint time.Time
	retryBackoff   time.Duration // initial export retry backoff

	// status is the published per-file snapshot for /debug/tailer (written by
	// the sweep goroutine in publishStatus, read from HTTP handlers).
	status      atomic.Pointer[[]FileStatus]
	lastStatus  time.Time
	statusEvery time.Duration // snapshot cadence (10s; tests shorten it)

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

	t.scanDir(t.loadCheckpoints(), true)
	t.lastFlush = time.Now()

	dirTicker := time.NewTicker(2 * time.Second)
	defer dirTicker.Stop()
	poll := time.NewTicker(t.cfg.PollInterval)
	defer poll.Stop()
	// debounce coalesces bursts of write events into one dirty sweep. It is
	// armed by the first event of a burst and NOT re-armed by subsequent
	// events (debouncePending): resetting per event would postpone the sweep
	// indefinitely under sustained writes (a busy file emits events more
	// often than the debounce interval), starving event-driven sweeps down
	// to the poll fallback — under which sub-poll-interval rename rotations
	// lose whole segments (the intermediate inode is never opened).
	debounce := time.NewTimer(0)
	if !debounce.Stop() {
		<-debounce.C
	}
	defer debounce.Stop()
	debouncePending := false

	for {
		select {
		case <-ctx.Done():
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
			if t.handleEvent(ev) && !debouncePending {
				debouncePending = true
				debounce.Reset(50 * time.Millisecond)
			}
		case err := <-watchErrs:
			t.log.Warn("fsnotify", "error", err)
		case <-debounce.C:
			debouncePending = false
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
	if len(t.batch) > 0 && time.Since(t.lastFlush) >= t.cfg.FlushInterval {
		t.flush(ctx)
	}
	if t.checkpointing() && time.Since(t.lastCheckpoint) >= 10*time.Second {
		t.saveCheckpoints()
	}
	if time.Since(t.lastStatus) >= t.statusEvery {
		t.publishStatus()
	}
	t.closeIdleFiles()
}

// closeIdleFiles releases the fds of fully-caught-up files that have been
// inactive for Config.IdleClose. Only files with nothing unread, unbuffered
// and uncommitted may close — a held fd is the only access to a rotated-away
// inode's remainder, so anything in flight keeps its fd.
func (t *Tailer) closeIdleFiles() {
	if t.cfg.IdleClose <= 0 {
		return
	}
	// Housekeeping runs on every debounced sweep (up to 20x/s under load);
	// a coarse inactivity timeout does not need scanning every file (and its
	// watermark) that often.
	now := time.Now()
	scanEvery := min(t.cfg.IdleClose/4, 30*time.Second)
	if now.Sub(t.lastIdleScan) < scanEvery {
		return
	}
	t.lastIdleScan = now
	for _, f := range t.files {
		if f.f == nil || f.compressed || f.dirty || f.limited {
			continue
		}
		if len(f.pending) > 0 || f.readPos != f.committed || len(f.segments) > 0 {
			continue
		}
		if _, buffered := f.watermark(); buffered {
			continue
		}
		if f.lastMod.IsZero() || now.Sub(f.lastMod) < t.cfg.IdleClose {
			continue
		}
		// lastMod is the cached mtime from the last read; re-stat so a write
		// the sweep has not consumed yet cannot have its fd pulled out from
		// under it.
		st, err := os.Stat(f.path)
		if err != nil || st.Size() != f.readPos || !st.ModTime().Equal(f.lastMod) {
			continue
		}
		_ = f.f.Close()
		f.f = nil // ensureOpen reopens and re-verifies identity on activity
	}
}

// sweep reads newly appended data; all sweeps every file (polling
// fallback), otherwise only files marked dirty by events are read.
func (t *Tailer) sweep(ctx context.Context, all bool) {
	cutoff := time.Now().Add(-t.cfg.MultilineTimeout)
	for path, f := range t.files {
		if f.gone {
			if _, err := os.Stat(f.path); err == nil {
				// A listing raced a rename+recreate rotation: the path was
				// momentarily absent but is alive again. Dropping now would
				// discard the tailing state (and, on the next checkpoint
				// save, its entry) and lose every inode rotated away before
				// rediscovery; clear the flag and let readFile's rotation
				// detection handle the identity change instead.
				f.gone = false
			} else {
				// The file is gone from disk; its remaining bytes live only
				// behind our fd. Drain, export, and only let the inode go once
				// the offsets commit — a failed export must be able to re-read
				// it (rewind seeks the still-open fd back), or a pod deleted
				// during a collector outage would lose its final lines.
				t.drainGone(f)
				t.flush(ctx)
				if t.settledGone(f) {
					t.release(f)
					delete(t.files, path)
				}
				continue
			}
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
