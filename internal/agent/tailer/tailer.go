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
	"github.com/JohanLindvall/kubescrape/internal/logdedupe"
	"github.com/JohanLindvall/kubescrape/internal/logline"
	"github.com/JohanLindvall/kubescrape/internal/metrics"
	"github.com/JohanLindvall/kubescrape/internal/obs"
	"github.com/JohanLindvall/kubescrape/pkg/kubemeta"
	"github.com/JohanLindvall/kubescrape/pkg/logattrs"
)

// maxReadWarnPaths bounds the distinct paths the read-failure warn throttles
// by. logdedupe saturation SUPPRESSES rather than clearing, so the one
// saturation line is what tells an operator the list is truncated; the counter
// keeps moving regardless.
const maxReadWarnPaths = 256

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
	// RateBurst is the token bucket size (default 2x RateLimit; floored at 1,
	// the smallest bucket a whole-token grant can ever come out of).
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
	// Transform applies the exporter-seam log transform to a just-built batch
	// IN PLACE, once, before the retry loop (nil = none; wired from
	// transform.Wrapper.TransformLogs — a func field so the tailer needs no
	// dependency on that package). When set, Exporter must be the chain BELOW
	// the transform layer (transform.Wrapper.Inner()), or every retry would
	// pay the wrapper's copy and re-run the script. The tailer owns the batch
	// it just built, so no copy is needed; a batch transformed to nothing
	// commits its offsets without a send, and a script error behaves like a
	// failed export — the rewound bytes are re-read and re-transformed under
	// whatever program is active by then.
	Transform func(ld plog.Logs) error
	// ParseLine is the transforms file's parse: hook (a func field for the
	// same no-dependency reason as Transform), consulted per line ONLY for
	// plain sources flagged parseScript. ok=false leaves the line untouched.
	ParseLine func(line string) (ParsedLine, bool)
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
	cfg      Config
	log      *slog.Logger
	sources  []*compiledSource
	scanDirs map[string]struct{} // fixed base dirs of all include globs, watched for new files
	files    map[string]*file    // by path
	// readWarn throttles the per-file "reading log file" warning. A
	// persistently unreadable file (bad permissions, an SELinux denial, EIO on
	// a failing disk) fails readFile on EVERY sweep — poll interval plus every
	// fsnotify event — so an unthrottled warn is a flood proportional to the
	// number of such files, the exact persisting-state complaint logdedupe
	// exists to bound. Keyed by path, re-warned on a schedule.
	readWarn *logdedupe.Table
	batch    []entry
	// flushed is the batch the CURRENT flush is exporting: flush swaps it out of
	// batch (so batch is empty again the moment the export starts, as every
	// caller's read loop requires) and walks it once more after the outcome, to
	// record which of its entries a rewind could still bring back
	// (file.observed / logchain.Input.Observed). The two slices ping-pong, so a
	// flush costs no per-entry copy and no allocation; the price is one extra
	// batch-sized array, cleared after every flush so it pins no bodies.
	flushed       []entry
	readBuf       []byte // reusable read scratch (single sweep goroutine)
	warnedListing bool   // a glob-failure warning was already emitted
	// lastListingOK reports whether the most recent scan actually listed the
	// sources. Checkpoint pruning is gated on it: a failed glob must never let
	// a save destroy the offsets of files it simply could not see.
	lastListingOK bool
	// checkpoints holds the STORED positions that have not yet been matched to
	// a discovered file. It is seeded from the positions store before the first
	// scan and consulted by EVERY scan, not just that one: a startup glob that
	// fails (an unreadable include base, a transient EIO — see
	// compiledSource.glob) discovers nothing, and the files the 2s dirTicker
	// then finds must still resume at their stored offsets, or the node
	// re-ingests every log file from byte 0 with every Pending prefix thrown
	// away and no counter moving. Applying an entry consumes it, and a listing that
	// SUCCEEDED without seeing a path drops it — the same proof, and the same
	// gate, that lets saveCheckpoints prune the store (a stale entry re-applied
	// to a recreated path would skip its first bytes as if they had shipped).
	checkpoints map[string]checkpoint
	// Whether discovery is still the STARTUP scan is per SOURCE
	// (compiledSource.startingUp), not per tailer: one source's failing glob
	// must not keep another's genuinely new files being skipped as history.

	// hadStoredCheckpoints records whether the store held ANY entry at startup,
	// which is what -logs-unknown-files=auto keys off ("the agent ran before").
	// It is captured once rather than read from t.checkpoints, whose entries are
	// consumed as files are discovered — mid-scan the map can already be empty
	// while the run is plainly not a first one.
	hadStoredCheckpoints bool

	// hopsUnsaved: at least one rotation hop this sweep recorded a segment that
	// no save has persisted. The sweep saves once at its end rather than once
	// per hop (see reopen).
	hopsUnsaved bool

	lastIdleScan   time.Time
	lastFlush      time.Time
	lastCheckpoint time.Time
	retryBackoff   time.Duration // initial export retry backoff
	resolveBudget  time.Duration // ceiling on one sweep's metadata resolutions
	shutdownBudget time.Duration // ceiling on the final sweep+drain+flush
	// segmentStallLimit bounds how long one segment's replay may make no
	// progress before it is given up on (chargeStall). A duration rather than a
	// pass count because the pass rate is the sweep rate, which ranges from the
	// poll interval to ~20/s under event-driven load.
	segmentStallLimit time.Duration
	// drainCap bounds one drainReader call (see defaultDrainCap).
	drainCap int64

	// status is the published per-file snapshot for /debug/tailer (written by
	// the sweep goroutine in publishStatus, read from HTTP handlers).
	status      atomic.Pointer[[]FileStatus]
	lastStatus  time.Time
	statusEvery time.Duration // snapshot cadence (10s; tests shorten it)

	// Event-driven mode (nil watcher = pure polling).
	watcher   *fsnotify.Watcher
	watchRefs map[string]int // watched target directories, refcounted
	// watchedScan tracks which discovery (scan) directories hold an OS watch.
	// Registration is retried from every discovery pass, not attempted only at
	// startup: an include base that appears later (a hostPath mounted after the
	// agent, a directory another workload creates) would otherwise have its
	// files discovered by polling but their writes produce no events — silently
	// degrading that source to poll cadence, under which sub-poll-interval
	// rename rotations lose segments.
	watchedScan map[string]struct{}
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

// defaultResolveBudget caps how long one sweep may spend resolving metadata
// before it gets on with reading the files it already knows about.
const defaultResolveBudget = 5 * time.Second

// defaultShutdownBudget bounds the tailer's final sweep, drain and flush. It
// has to leave room inside the pod's termination grace period for everything
// that runs after the tailer stops: the final log-metrics window, span metrics,
// self-metrics and the disk-buffer drain.
const defaultShutdownBudget = 10 * time.Second

// defaultSegmentStallLimit is how long a rotated segment's replay may make no
// progress before the file gives up on it and resumes collecting. Generous: an
// EMFILE or an EACCES from a runtime mid-rotation is worth waiting out, and the
// alternative to waiting is losing those lines.
const defaultSegmentStallLimit = 2 * time.Minute

// defaultDrainCap bounds ONE drain call (drainReader), so a source that outruns
// the drain cannot pin the single sweep goroutine indefinitely. It is not a
// budget on what may be recovered: hitting it reports the drain UNFINISHED, and
// the remainder is replayed as a segment (or re-drained next sweep, for a gone
// file). A test-settable field rather than a const because the alternative for a
// regression test is writing a gigabyte.
const defaultDrainCap = 1 << 30

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
	if cfg.RateLimit > 0 && cfg.RateBurst < 1 {
		// allowLine grants only whole tokens (tokens >= 1) and caps the
		// bucket at RateBurst, so a bucket that cannot HOLD one token never
		// grants: a sub-0.5 rate limit (burst derived as 2x) wedged every
		// file in pause mode — or discarded 100% in drop mode — and
		// -check-config passed it. The floor keeps fractional RATES exact
		// (refill still accrues at RateLimit/s); only the burst is lifted to
		// the smallest grantable bucket. validateConfig cannot carry this
		// (it lives in cmd/kubescrape-agent), so the constructor does.
		cfg.RateBurst = 1
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
		cfg: cfg,
		log: log,
		// Until a scan runs, treat the listing as good: a save before any
		// discovery has nothing to prune anyway, and starting false would make
		// the very first save copy the whole stored map back.
		lastListingOK:     true,
		sources:           sources,
		scanDirs:          scanDirs,
		files:             make(map[string]*file),
		readWarn:          logdedupe.New(maxReadWarnPaths, time.Minute),
		retryBackoff:      time.Second,
		resolveBudget:     defaultResolveBudget,
		shutdownBudget:    defaultShutdownBudget,
		segmentStallLimit: defaultSegmentStallLimit,
		drainCap:          defaultDrainCap,
		statusEvery:       10 * time.Second,
	}
}

// Run tails until ctx is done, then flushes what it has.
func (t *Tailer) Run(ctx context.Context) {
	if t.cfg.Watch {
		if w, err := fsnotify.NewWatcher(); err != nil {
			t.log.Warn("fsnotify unavailable, falling back to polling", "error", err)
		} else {
			t.watcher = w
			t.watchRefs = make(map[string]int)
			t.watchedScan = make(map[string]struct{})
			defer func() { _ = w.Close() }()
			for dir := range t.scanDirs {
				if err := w.Add(dir); err != nil {
					t.log.Warn("watching log directory failed; retrying on discovery passes", "dir", dir, "error", err)
					continue
				}
				t.watchedScan[dir] = struct{}{}
			}
			if len(t.watchedScan) == 0 {
				// The watcher is kept even with nothing watched: an include
				// base that does not exist YET (a hostPath mounted after
				// startup) is retried on every discovery pass
				// (retryScanWatches), and polling covers the meantime. Closing
				// the watcher here made the degradation permanent.
				t.log.Warn("no log directories watched yet; polling until one can be watched")
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
			// The shutdown work is BOUNDED. context.Background() here meant a
			// collector outage could hold the final sweep for as long as the
			// export retries take (three attempts at -otlp-timeout each), while
			// the kubelet's 30s grace runs out — and everything that salvages
			// in-memory state (the final log-metrics window, span metrics,
			// self-metrics, the buffer drain) runs AFTER this returns. A
			// deadline is what makes those reachable; work that does not fit is
			// re-read after the restart, which the offsets already guarantee.
			sctx, scancel := context.WithTimeout(context.WithoutCancel(ctx), t.shutdownBudget)
			t.sweep(sctx, true)
			// Drain the pipelines; the emitted entries' offsets commit with
			// the final flush, so nothing is re-read after a restart.
			for _, f := range t.files {
				t.stopPipeline(sctx, f)
			}
			t.flush(sctx)
			t.saveCheckpoints()
			scancel()
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
		// Caught up when everything FED has committed (fedEnd, not readPos):
		// trailing bytes that never entered the pipeline — a blank line, a
		// rate-DROPPED or oversize-discarded tail — can never produce a
		// committing entry, so readPos != committed held forever for such
		// files and their fds never closed. Every sibling completion decision
		// (segment `to`, goneEnd) already compares against fedEnd for the same
		// byte class; unread-on-disk bytes are the re-stat's job below, and
		// un-fed bytes still buffered are the watermark's.
		if len(f.pending) > 0 || f.fedEnd() > f.committed || len(f.segments) > 0 {
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
		// under it. Size compares against readPos (bytes READ), not fedEnd:
		// consumed-but-never-fed trailing bytes were still read, and only
		// bytes past readPos are unconsumed activity.
		st, err := os.Stat(f.path)
		if err != nil || st.Size() != f.readPos || !st.ModTime().Equal(f.lastMod) {
			continue
		}
		_ = f.f.Close()
		f.f = nil // readFile reopens (identity re-verified) on evidence of activity
		f.idleClosed = true
	}
}

// sweep reads newly appended data; all sweeps every file (polling
// fallback), otherwise only files marked dirty by events are read.
func (t *Tailer) sweep(ctx context.Context, all bool) {
	// Set lazily on the first unresolved file, so a sweep with nothing to
	// resolve pays nothing.
	var resolveDeadline time.Time
	now := time.Now()
	cutoff := now.Add(-t.cfg.MultilineTimeout)
	for path, f := range t.files {
		if f.gone {
			// The regularity check matters as much as the stat: a FIFO taking
			// the vanished path resurrects the file, and the readFile that
			// follows would drain, see a new inode, and re-open it — an
			// open(2) that blocks the sweep goroutine forever (openRegular
			// guards the opens; this guard keeps the impostor on the gone
			// path, where the old inode drains from our fd and settles).
			if st, err := os.Stat(f.path); err == nil && st.Mode().IsRegular() {
				// A listing raced a rename+recreate rotation: the path was
				// momentarily absent but is alive again. Dropping now would
				// discard the tailing state (and, on the next checkpoint
				// save, its entry) and lose every inode rotated away before
				// rediscovery; withdraw the verdict (file.resurrect owns what
				// that entails) and let readFile's rotation detection handle
				// the identity change instead.
				f.resurrect()
			} else {
				// The file is gone from disk; its remaining bytes live only
				// behind our fd. Drain, export, and only let the inode go once
				// the offsets commit — a failed export must be able to re-read
				// it (rewind seeks the still-open fd back), or a pod deleted
				// during a collector outage would lose its final lines.
				gen, committedBefore := f.rewindGen, f.committed
				t.drainGone(ctx, f)
				t.flush(ctx)
				if t.settledGone(f) || t.chargeGoneStall(f, gen, committedBefore) {
					t.release(f)
					delete(t.files, path)
				}
				continue
			}
		}
		if !all && !f.dirty {
			continue
		}
		if !f.resolved {
			// One shared budget for the whole sweep's resolutions. Each one can
			// block server-side for -metadata-wait, and they all run on this
			// single goroutine ahead of any reading, so without a ceiling a
			// handful of unresolvable files decides how often every OTHER file
			// on the node is read. Files not reached this sweep are retried on
			// the next one — nothing is lost, the data waits on disk.
			if resolveDeadline.IsZero() {
				resolveDeadline = time.Now().Add(t.resolveBudget)
			}
			if time.Now().After(resolveDeadline) {
				continue
			}
			if !t.resolveMetadata(ctx, f) {
				continue
			}
		} else {
			// The resource is not LATCHED at resolve: re-render it when the
			// node-metadata provider has produced something new (a node
			// relabel, or the first real fetch landing after the placeholder
			// the agent seeds selfmeta.Poll with). One pointer compare per
			// resolved file per sweep — see refreshNodeAttrs.
			t.refreshNodeAttrs(f)
		}
		if f.excluded {
			// The pod opted out via its annotation; nothing is read, the
			// file stays tracked (cheap) so rediscovery does not re-resolve
			// it every sweep.
			t.dropExcludedBacklog(f)
			f.dirty = false
			continue
		}
		f.dirty = false
		if err := t.readFile(ctx, f); err != nil {
			if errors.Is(err, os.ErrNotExist) {
				// The path no longer resolves. This is the gone-detection for
				// every file discovery skips the stat for (claimPath /
				// file.swept), and it covers a shape discovery's LISTING cannot
				// see at all: /var/log/containers/*.log are symlinks, and a
				// readdir-based glob keeps listing a dangling one forever. Take
				// the gone path — drain what is left behind our fd, export it,
				// and release once the offsets commit. A path merely absent for
				// an instant (a rename+recreate rotation caught mid-sweep) is
				// resurrected by the gone branch's own stat next sweep.
				f.gone = true
			} else {
				obs.LogReadErrors.Inc()
				// Throttled: a persistently unreadable file fails here on every
				// sweep, and an unthrottled warn is a flood (see readWarn). The
				// SATURATED flag is what keeps the throttle honest — past the
				// table's cap a new path is suppressed outright, and without
				// this line the condition would become invisible rather than
				// merely quiet. Every other Table user in the tree consumes it;
				// this one discarded it.
				if allow, saturated := t.readWarn.Allow(path); allow {
					t.log.Warn("reading log file", "path", path, "error", err)
				} else if saturated {
					t.log.Warn("too many distinct unreadable log files to warn about individually; "+
						"further paths are suppressed (kubescrape_log_read_errors_total still counts them)",
						"tracked", maxReadWarnPaths)
				}
			}
		}
		// Age out fragment runs and multi-line groups that never completed.
		//
		// The cutoff is compared against the LINE's OWN timestamp (feedLine
		// passes it to AddParsed/AddAt — the multiline package asks for the
		// log's own clock precisely so replaying old logs still groups
		// correctly). A wall-clock cutoff therefore does not measure "how long
		// has this group waited for its continuation"; it measures "how far
		// behind real time is this line". Any lag above MultilineTimeout — a
		// backlog after a restart, a slow collector, a busy node, or simply
		// reading a rotated file — made EVERY group instantly older than the
		// cutoff, tearing CRI fragment runs and splitting stack traces exactly
		// when the tailer is catching up.
		//
		// So while lines are still arriving, age out on the LOG's clock; once
		// the file has been quiet for the timeout, fall back to the wall clock
		// so a genuinely abandoned group still gets out. Live tailing is
		// unchanged: there the two clocks agree.
		fc := cutoff
		if !f.lastLineTime.IsZero() && now.Sub(f.lastFed) < t.cfg.MultilineTimeout {
			if lc := f.lastLineTime.Add(-t.cfg.MultilineTimeout); lc.Before(fc) {
				fc = lc
			}
		}
		if f.criStage != nil {
			_ = f.criStage.FlushBefore(ctx, fc)
		}
		if f.traces != nil {
			_ = f.traces.FlushBefore(ctx, fc)
		}
		// The backstop for entries the age-out flushes above emitted; the ones
		// consume feeds are checked against the threshold per RECORD, where
		// they are added. A rewind here needs no handling: this file's
		// iteration is over.
		_ = t.maybeFlush(ctx, f)
	}
	if t.hopsUnsaved {
		// One save for every rotation this sweep handled, instead of one per
		// hop — see reopen for why that still closes the crash window the
		// per-hop save was added for.
		t.saveCheckpoints()
	}
}

// dropExcludedBacklog retires the checkpoint-restored segments of a file whose
// resolve marked it excluded (pod annotation, or a source selector miss).
// Exclusion is authoritative and reaches the backlog too: the sweep never
// reads an excluded file, so restored segments (initFile) would never replay
// and never retire — their stale Prefix entries rewritten by every checkpoint
// save for the process lifetime — and a deletion would hand them to drainGone,
// whose feed EXPORTS the very content the workload opted out of (drainGone
// carries the matching guard). Dropped as intent, not loss: no counter moves.
// The immediate save is what removes the Prefix entries; the plain offset
// entry stays with the tracked file. Nothing else needs discarding — reads are
// gated on resolution, so when exclusion is learned no byte of this file has
// ever entered pending or the pipeline.
func (t *Tailer) dropExcludedBacklog(f *file) {
	if len(f.segments) == 0 {
		return
	}
	n := len(f.segments)
	f.closeSegments()
	t.log.Info("dropping the restored backlog of an excluded file", "path", f.path, "segments", n)
	if t.checkpointing() {
		t.saveCheckpoints()
	}
}

// ParsedLine is what the parse hook produced for one plain-source line
// (mirrors transform.Parsed; a local type so the tailer needs no transform
// dependency).
type ParsedLine struct {
	Body         string
	HasBody      bool
	SeverityText string
	TimeUnixNano int64
}
