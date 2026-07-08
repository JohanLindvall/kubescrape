// Package tailer tails containerd container log files under
// /var/log/containers and exports the entries as OTLP logs with resource
// attributes fetched from the kubescrape metadata service.
//
// Log lines flow through the two-stage github.com/JohanLindvall/multiline
// pipeline: the cri stage parses the CRI format and rejoins partial-line
// fragments, and the multiline stage joins application-level multi-line
// entries such as stack traces.
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
	"strings"
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
	Dir            string // /var/log/containers
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
	// LogAttrs lifts configured keys out of structured lines onto the record
	// as resource/scope/log attributes (nil = none).
	LogAttrs *logattrs.Extractor
	// MultilineTimeout flushes buffered fragment runs and multi-line groups
	// that have not completed within this duration.
	MultilineTimeout time.Duration
	// ExcludeNamespaces lists namespaces whose container logs are not
	// tailed (e.g. the observability namespace itself, to avoid feedback
	// loops through the collector's own output).
	ExcludeNamespaces []string
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
	files          map[string]*file // by path
	batch          []entry
	lastFlush      time.Time
	lastCheckpoint time.Time
	retryBackoff   time.Duration // initial export retry backoff

	// Event-driven mode (nil watcher = pure polling).
	watcher   *fsnotify.Watcher
	watchRefs map[string]int // watched target directories, refcounted
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
	path        string
	containerID string
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
	// Per pipeline key ("<containerID>/<stream>"): lastEnd is the end offset
	// of the newest physical line fed; runStart is the start offset of the
	// oldest physical line not yet emitted by stage 1; fifo holds the offset
	// ranges of logical lines buffered in stage 2.
	lastEnd  map[string]int64
	runStart map[string]int64
	fifo     map[string][]logItem

	resource    pcommon.Resource // resolved metadata, valid when resolved
	resolved    bool
	nextMetaTry time.Time
	gone        bool

	// gen counts rename rotations across which the pipeline was carried (a
	// multi-line group straddled the boundary). It bumps in reopen and stamps
	// emitted entries so flush commits only current-inode offsets.
	gen int
	// carried names the rotated-away files whose unexported tails are buffered
	// in the pipeline as part of a straddling group, oldest first; nil when
	// none. A group that straddles several rotations accumulates one entry per
	// rotation. Checkpointed so a crash before the group exports can re-read
	// every tail.
	carried []rotatedPrefix
	// carriedFed reports whether the carried prefixes are present in the
	// current pipeline incarnation. False after a fresh pipeline (rewind or
	// restart) with carried still set, meaning the prefixes must be re-read
	// before the new inode is consumed.
	carriedFed bool
}

type logItem struct{ start, end int64 }

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
func (f *file) watermark() (int64, bool) {
	wm := int64(-1)
	lower := func(v int64) {
		if wm < 0 || v < wm {
			wm = v
		}
	}
	for _, v := range f.runStart {
		lower(v)
	}
	for _, items := range f.fifo {
		if len(items) > 0 {
			lower(items[0].start)
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
	// offset is the file offset just after the physical line that completed
	// this entry; committing it marks the entry as exported.
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
	return &Tailer{
		cfg:          cfg,
		log:          log,
		files:        make(map[string]*file),
		retryBackoff: time.Second,
	}
}

// Run tails until ctx is done, then flushes what it has.
func (t *Tailer) Run(ctx context.Context) {
	if t.cfg.Watch {
		w, err := fsnotify.NewWatcher()
		if err != nil {
			t.log.Warn("fsnotify unavailable, falling back to polling", "error", err)
		} else if err := w.Add(t.cfg.Dir); err != nil {
			t.log.Warn("watching log directory failed, falling back to polling", "dir", t.cfg.Dir, "error", err)
			_ = w.Close()
		} else {
			t.watcher = w
			t.watchRefs = make(map[string]int)
			defer func() { _ = w.Close() }()
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
	// debounce coalesces bursts of write events into one dirty sweep.
	debounce := time.NewTimer(0)
	if !debounce.Stop() {
		<-debounce.C
	}
	defer debounce.Stop()

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

// housekeeping flushes and checkpoints on their intervals.
func (t *Tailer) housekeeping(ctx context.Context) {
	if len(t.batch) > 0 && time.Since(t.lastFlush) >= t.cfg.FlushInterval {
		t.flush(ctx)
	}
	if (t.cfg.Positions != nil || t.cfg.CheckpointFile != "") && time.Since(t.lastCheckpoint) >= 10*time.Second {
		t.saveCheckpoints()
	}
}

// handleEvent processes one fsnotify event; it reports whether a dirty sweep
// should be scheduled.
func (t *Tailer) handleEvent(ev fsnotify.Event) bool {
	dir := filepath.Dir(ev.Name)
	if dir == t.cfg.Dir {
		// Symlink appeared/disappeared: rediscover immediately.
		if ev.Op&(fsnotify.Create|fsnotify.Remove|fsnotify.Rename) != 0 {
			t.scanDir(nil, false)
			return true
		}
		// The log file may live directly in the watched directory (no
		// symlink indirection): treat writes like target-dir events.
		if f, ok := t.files[ev.Name]; ok && ev.Op&fsnotify.Write != 0 {
			f.dirty = true
			return true
		}
		return false
	}
	// A write/create in a watched target directory: mark the files tailing
	// that directory (rotation creates a new file there, too).
	dirty := false
	for _, f := range t.files {
		if f.targetDir == dir {
			f.dirty = true
			dirty = true
		}
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

// scanDir discovers new and removed log files. checkpoints is non-nil only
// on the initial scan.
func (t *Tailer) scanDir(checkpoints map[string]checkpoint, initial bool) {
	entries, err := os.ReadDir(t.cfg.Dir)
	if err != nil {
		t.log.Error("reading log directory", "dir", t.cfg.Dir, "error", err)
		return
	}
	seen := make(map[string]struct{}, len(entries))
	for _, de := range entries {
		name := de.Name()
		id, namespace, ok := parseFileName(name)
		if !ok || slices.Contains(t.cfg.ExcludeNamespaces, namespace) {
			continue
		}
		path := filepath.Join(t.cfg.Dir, name)
		seen[path] = struct{}{}
		if _, known := t.files[path]; known {
			continue
		}
		f := &file{
			path:        path,
			containerID: id,
			dirty:       true, // read on the first (event-driven) sweep
		}
		t.newPipeline(f)
		if initial {
			if cp, ok := checkpoints[path]; ok {
				f.committed = cp.Offset
				f.inode = cp.Inode
				f.fp = fingerprint{Len: cp.FingerprintLen, Hash: cp.FingerprintHash}
				for _, pp := range cp.Pending {
					// A group straddled one or more rotations at shutdown/crash:
					// its prefixes are re-read from the rotated files (oldest
					// first) before this (new) inode is consumed. carriedFed is
					// already false.
					f.carried = append(f.carried, rotatedPrefix{
						inode: pp.Inode,
						fp:    fingerprint{Len: pp.FingerprintLen, Hash: pp.FingerprintHash},
						from:  pp.From,
						to:    pp.To,
					})
				}
			} else if st, err := os.Stat(path); err == nil {
				// Present before the agent started and no checkpoint: start
				// at the end to avoid re-ingesting history.
				f.committed = st.Size()
			}
		}
		t.files[path] = f
	}
	for path, f := range t.files {
		if _, ok := seen[path]; !ok {
			f.gone = true
		}
	}
	obs.LogFiles.Set(float64(len(t.files)))
}

// newPipeline (re)creates the file's aggregation stages with empty state. A
// carried prefix (if any) is no longer present in the fresh pipeline and must
// be re-read before the current inode is consumed.
func (t *Tailer) newPipeline(f *file) {
	f.carriedFed = false
	f.lastEnd = make(map[string]int64)
	f.runStart = make(map[string]int64)
	f.fifo = make(map[string][]logItem)

	if t.cfg.Multiline {
		f.traces = multiline.New(func(_ context.Context, e multiline.Entry[time.Time]) error {
			items := f.fifo[e.Key]
			n := min(e.Lines, len(items)) // Lines > len(items) must not happen; defensive
			if n == 0 {
				return nil
			}
			end := items[n-1].end
			f.fifo[e.Key] = items[n:]
			t.emit(f, entry{
				time: e.Data, stream: streamOf(e.Key), body: e.Text,
				truncated: e.Truncated, match: e.Match, offset: end,
			})
			return nil
		}, multiline.WithMaxBytes(t.cfg.MaxEntryBytes), multiline.WithMaxLines(512))
	} else {
		f.traces = nil
	}

	// Stage 1 hands every rejoined logical line downstream. Emission is
	// synchronous inside Add/Flush*, so lastEnd[key] is exactly the end
	// offset of the line's last fragment.
	f.criStage = cri.New(func(ctx context.Context, key, line string, when time.Time, start int64) error {
		delete(f.runStart, key)
		end := f.lastEnd[key]
		if f.traces == nil {
			t.emit(f, entry{time: when, stream: streamOf(key), body: line, offset: end})
			return nil
		}
		f.fifo[key] = append(f.fifo[key], logItem{start: start, end: end})
		return f.traces.AddAt(ctx, key, line, when, when)
	}, multiline.WithMaxBytes(t.cfg.MaxEntryBytes))
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

// feedLine pushes one raw physical line spanning [start, end) into the
// pipeline.
func (t *Tailer) feedLine(ctx context.Context, f *file, raw string, start, end int64) {
	key := f.containerID
	if l, ok := cri.Parse(raw); ok {
		key += "/" + l.Stream
	}
	f.lastEnd[key] = end
	if _, ok := f.runStart[key]; !ok {
		f.runStart[key] = start
	}
	if err := f.criStage.Add(ctx, f.containerID, raw, start); err != nil {
		t.log.Warn("log pipeline", "path", f.path, "error", err)
	}
}

// stopPipeline drains both stages into the batch.
func (t *Tailer) stopPipeline(ctx context.Context, f *file) {
	_ = f.criStage.Stop(ctx)
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
		_ = f.criStage.FlushBefore(ctx, cutoff)
		if f.traces != nil {
			_ = f.traces.FlushBefore(ctx, cutoff)
		}
		if len(t.batch) >= t.cfg.BatchSize {
			t.flush(ctx)
		}
	}
}

// resolveMetadata fetches container metadata, backing off between attempts.
// The file is not consumed until metadata is available; the data waits on
// disk, nothing is lost.
func (t *Tailer) resolveMetadata(ctx context.Context, f *file) bool {
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

// readFile ingests up to MaxBytesPerSweep appended bytes and detects
// rotation.
func (t *Tailer) readFile(ctx context.Context, f *file) error {
	if err := t.ensureOpen(f); err != nil {
		return err
	}
	// A group straddled a rename rotation and the pipeline was since discarded
	// (rewind or restart): re-read the rotated-away prefix before the new inode
	// so the group reconstructs.
	if f.carried != nil && !f.carriedFed {
		t.feedCarriedPrefix(ctx, f)
	}

	budget := t.cfg.MaxBytesPerSweep
	buf := make([]byte, 64*1024)
	read := 0
	for budget > 0 {
		limit := min(len(buf), budget)
		n, err := f.f.Read(buf[:limit])
		if n > 0 {
			budget -= n
			read += n
			obs.LogBytes.Add(float64(n))
			f.pending = append(f.pending, buf[:n]...)
			f.readPos += int64(n)
			t.consume(ctx, f)
		}
		if err != nil {
			if !errors.Is(err, io.EOF) {
				return err
			}
			break
		}
	}

	// Rotation/truncation detection.
	st, err := os.Stat(f.path)
	if err != nil {
		return err
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

// drainFile reads the (rotated-away or removed) file to EOF so no bytes
// written between our last read and the rotation are lost. Bounded to keep a
// still-active writer from pinning the sweep.
func (t *Tailer) drainFile(ctx context.Context, f *file) {
	if f.f == nil {
		return
	}
	budget := 4 * t.cfg.MaxBytesPerSweep
	buf := make([]byte, 64*1024)
	for budget > 0 {
		limit := min(len(buf), budget)
		n, err := f.f.Read(buf[:limit])
		if n > 0 {
			budget -= n
			f.pending = append(f.pending, buf[:n]...)
			f.readPos += int64(n)
			t.consume(ctx, f)
		}
		if err != nil {
			return
		}
	}
	t.log.Warn("rotated file still growing, leaving remainder", "path", f.path)
}

// consume splits pending bytes into physical lines and feeds the pipeline.
func (t *Tailer) consume(ctx context.Context, f *file) {
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
		t.reanchorPipeline(f)
		f.gen++
	} else {
		t.stopPipeline(ctx, f)
		t.newPipeline(f)
		f.carried = nil
	}
	f.inode = 0
	f.fp = fingerprint{}
	f.committed = 0
	f.readPos = 0
	f.lineStart = 0
	f.pending = f.pending[:0]
	f.dirty = true
}

// reanchorPipeline resets the offsets of data buffered in the pipeline to the
// new inode's origin, so watermark holds the new inode's checkpoint at 0 until
// the carried group completes and the (new-inode) offset of its final line
// becomes the commit point.
func (t *Tailer) reanchorPipeline(f *file) {
	for k := range f.lastEnd {
		f.lastEnd[k] = 0
	}
	for k := range f.runStart {
		f.runStart[k] = 0
	}
	for _, items := range f.fifo {
		for i := range items {
			items[i] = logItem{}
		}
	}
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
		t.reanchorPipeline(f)
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
	buf := make([]byte, 64*1024)
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
	if f.resolved {
		t.drainFile(context.Background(), f)
		t.stopPipeline(context.Background(), f)
	}
	if f.f != nil {
		_ = f.f.Close()
		f.f = nil
	}
	t.unwatchTarget(f)
}

// logGrouper places records into ResourceLogs/ScopeLogs keyed by (file,
// resource attributes, scope attributes), so line-derived resource/scope
// attributes split records into the right resources. Without line attributes
// there is one scope per file, matching the previous behavior.
type logGrouper struct {
	ld     plog.Logs
	scopes map[string]plog.ScopeLogs
}

func (g *logGrouper) scope(f *file, resAttrs, scopeAttrs []logattrs.Attr) plog.ScopeLogs {
	key := fmt.Sprintf("%p\x01%s\x01%s", f, logattrs.Key(resAttrs), logattrs.Key(scopeAttrs))
	if sl, ok := g.scopes[key]; ok {
		return sl
	}
	rl := g.ld.ResourceLogs().AppendEmpty()
	f.resource.CopyTo(rl.Resource())
	logattrs.Put(rl.Resource().Attributes(), resAttrs)
	sl := rl.ScopeLogs().AppendEmpty()
	sl.Scope().SetName("github.com/JohanLindvall/kubescrape/agent/tailer")
	logattrs.Put(sl.Scope().Attributes(), scopeAttrs)
	g.scopes[key] = sl
	return sl
}

// flush exports the batch. On success offsets are committed; on failure the
// files are rewound to the committed offsets so the data is re-read.
func (t *Tailer) flush(ctx context.Context) {
	if len(t.batch) == 0 {
		t.lastFlush = time.Now()
		return
	}
	ld := plog.NewLogs()
	g := &logGrouper{ld: ld, scopes: map[string]plog.ScopeLogs{}}
	maxOffsets := make(map[*file]int64)
	touched := make(map[*file]struct{})
	now := pcommon.NewTimestampFromTime(time.Now())
	for _, e := range t.batch {
		// Extract configured line attributes; resource/scope ones drive the
		// grouping so records land under the right ResourceLogs/ScopeLogs.
		var extracted logattrs.Result
		if t.cfg.LogAttrs != nil {
			extracted = t.cfg.LogAttrs.Extract(e.body)
		}
		slr := g.scope(e.file, extracted.Resource, extracted.Scope)
		lr := slr.LogRecords().AppendEmpty()
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
		logattrs.Put(lr.Attributes(), extracted.Log)
		if t.cfg.Enrich {
			logenrich.Apply(lr, e.body)
		}
		touched[e.file] = struct{}{}
		// Only entries of the file's current rotation generation advance its
		// checkpoint; pre-rotation entries carry old-inode offsets recoverable
		// via file.carried and must not move the new inode's commit point.
		if e.gen == e.file.gen && e.offset > maxOffsets[e.file] {
			maxOffsets[e.file] = e.offset
		}
	}

	if err := t.exportWithRetry(ctx, ld); err != nil {
		t.log.Error("exporting logs failed, rewinding", "records", len(t.batch), "error", err)
		obs.LogExportFailures.Inc()
		for f := range touched {
			t.rewind(f)
		}
	} else {
		obs.LogEntries.Add(float64(len(t.batch)))
		for f := range touched {
			if off, ok := maxOffsets[f]; ok {
				// Never commit past lines still buffered in the pipeline; they
				// have not been exported yet.
				if wm, wok := f.watermark(); wok && wm < off {
					off = wm
				}
				if off > f.committed {
					f.committed = off
				}
			}
			// Once the carried group has fully drained (nothing buffered), its
			// record has been exported, so the rotated-away prefix is no longer
			// needed for recovery.
			if f.carried != nil {
				if _, wok := f.watermark(); !wok {
					f.carried = nil
				}
			}
		}
	}
	t.batch = t.batch[:0]
	t.lastFlush = time.Now()
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
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		t.log.Warn("writing checkpoint file", "error", err)
		return
	}
	if err := os.Rename(tmp, t.cfg.CheckpointFile); err != nil {
		t.log.Warn("replacing checkpoint file", "error", err)
	}
}

func inodeOf(st os.FileInfo) uint64 {
	if sys, ok := st.Sys().(*syscall.Stat_t); ok {
		return sys.Ino
	}
	return 0
}
