// Package tailer tails containerd container log files under
// /var/log/containers and exports the entries as OTLP logs with resource
// attributes fetched from the kubescrape metadata service.
//
// Design: a single sweep goroutine polls all files (bounded bytes per file
// per sweep), assembles CRI entries, and batches them. Export happens inline
// in the sweep with retries; file offsets are only committed (and
// checkpointed) after a successful export, and on failure the files are
// rewound to the committed offsets so the data is re-read — at-least-once
// delivery with no unbounded in-memory queue.
package tailer

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"time"

	"github.com/JohanLindvall/multiline"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"

	"github.com/JohanLindvall/kubescrape/internal/agent/attrs"
	"github.com/JohanLindvall/kubescrape/internal/agent/crilog"
	"github.com/JohanLindvall/kubescrape/internal/agent/metaclient"
	"github.com/JohanLindvall/kubescrape/internal/kubemeta"
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
	Dir              string // /var/log/containers
	CheckpointFile   string // "" disables checkpointing
	PollInterval     time.Duration
	FlushInterval    time.Duration
	BatchSize        int // flush after this many entries
	MaxEntryBytes    int // cap on one assembled log entry
	MaxBytesPerSweep int // per file; keeps one chatty container from starving others
	// Multiline joins application-level multi-line entries (stack traces,
	// ...) using github.com/JohanLindvall/multiline's default matcher.
	Multiline bool
	// MultilineTimeout flushes a buffered multi-line group that has not
	// completed within this duration.
	MultilineTimeout time.Duration
	// ExcludeNamespaces lists namespaces whose container logs are not
	// tailed (e.g. the observability namespace itself, to avoid feedback
	// loops through the collector's own output).
	ExcludeNamespaces []string
	MetadataWait      time.Duration
	Metadata          MetadataSource
	Exporter          LogExporter
	Logger            *slog.Logger
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
}

// file state invariant: lineStart + len(pending) == readPos, where lineStart
// is the file offset of pending[0] (the first unconsumed byte).
type file struct {
	path        string
	containerID string
	inode       uint64

	f         *os.File
	readPos   int64  // fd position
	lineStart int64  // offset of the first byte not yet consumed as a line
	committed int64  // offset covered by successful exports / checkpoint
	pending   []byte // incomplete physical line carried between sweeps
	asm       *crilog.Assembler
	// entryStart is the offset of the first physical line of the CRI entry
	// currently being assembled.
	entryStart int64

	// Multi-line aggregation (nil when disabled). fifo tracks, per stream,
	// the file offset range of every CRI entry buffered in the aggregator so
	// emitted groups map back to exact offsets.
	ml   *multiline.Multiline[mlData]
	fifo map[string][]mlItem

	resource    pcommon.Resource // resolved metadata, valid when resolved
	resolved    bool
	nextMetaTry time.Time
	gone        bool
}

type mlData struct {
	time   time.Time
	stream string
}

type mlItem struct {
	start, end int64
	trunc      bool
}

// watermark returns the lowest start offset still buffered in the
// aggregator; committed offsets must not advance past it.
func (f *file) watermark() (int64, bool) {
	wm := int64(-1)
	for _, items := range f.fifo {
		if len(items) > 0 && (wm < 0 || items[0].start < wm) {
			wm = items[0].start
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
	t.scanDir(t.loadCheckpoints(), true)
	t.lastFlush = time.Now()

	dirTicker := time.NewTicker(2 * time.Second)
	defer dirTicker.Stop()
	poll := time.NewTicker(t.cfg.PollInterval)
	defer poll.Stop()

	for {
		select {
		case <-ctx.Done():
			t.sweep(context.Background())
			// Emit multi-line groups still buffered; their offsets commit
			// with the final flush, so nothing is re-read after a restart.
			for _, f := range t.files {
				if f.ml != nil {
					_ = f.ml.Stop(context.Background())
				}
			}
			t.flush(context.Background())
			t.saveCheckpoints()
			return
		case <-dirTicker.C:
			t.scanDir(nil, false)
		case <-poll.C:
			t.sweep(ctx)
			if len(t.batch) > 0 && time.Since(t.lastFlush) >= t.cfg.FlushInterval {
				t.flush(ctx)
			}
			if t.cfg.CheckpointFile != "" && time.Since(t.lastCheckpoint) >= 10*time.Second {
				t.saveCheckpoints()
			}
		}
	}
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
			asm:         crilog.NewAssembler(t.cfg.MaxEntryBytes),
		}
		if t.cfg.Multiline {
			f.ml = t.newAggregator(f)
			f.fifo = make(map[string][]mlItem)
		}
		if initial {
			if cp, ok := checkpoints[path]; ok {
				f.committed = cp.Offset
				f.inode = cp.Inode
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
}

// sweep reads newly appended data from every file.
func (t *Tailer) sweep(ctx context.Context) {
	for path, f := range t.files {
		if f.gone {
			t.drop(f)
			delete(t.files, path)
			continue
		}
		if !f.resolved && !t.resolveMetadata(ctx, f) {
			continue
		}
		if err := t.readFile(ctx, f); err != nil && !errors.Is(err, os.ErrNotExist) {
			t.log.Warn("reading log file", "path", path, "error", err)
		}
		if f.ml != nil {
			// Emit buffered groups that did not complete in time.
			_ = f.ml.FlushBefore(ctx, time.Now().Add(-t.cfg.MultilineTimeout))
		}
		if len(t.batch) >= t.cfg.BatchSize {
			t.flush(ctx)
		}
	}
}

// newAggregator creates the per-file multi-line aggregator. Emitted groups
// pop their exact per-stream offset ranges from the FIFO (group size =
// joined-line count; CRI entry bodies never contain newlines).
func (t *Tailer) newAggregator(f *file) *multiline.Multiline[mlData] {
	return multiline.New(func(_ context.Context, line, match string, data mlData) error {
		items := f.fifo[data.stream]
		n := strings.Count(line, "\n") + 1
		if n > len(items) {
			n = len(items) // defensive; must not happen
		}
		if n == 0 {
			return nil
		}
		truncated := false
		for _, it := range items[:n] {
			truncated = truncated || it.trunc
		}
		end := items[n-1].end
		f.fifo[data.stream] = items[n:]
		t.batch = append(t.batch, entry{
			file:      f,
			time:      data.time,
			stream:    data.stream,
			body:      line,
			truncated: truncated,
			match:     match,
			offset:    end,
		})
		return nil
	}, multiline.WithMaxBytes(t.cfg.MaxEntryBytes), multiline.WithMaxLines(512))
}

// feed routes one assembled CRI entry into the batch, via the multi-line
// aggregator when enabled. [start, end) is the entry's file offset range.
func (t *Tailer) feed(ctx context.Context, f *file, e crilog.Entry, start, end int64) {
	if f.ml == nil {
		t.batch = append(t.batch, entry{
			file: f, time: e.Time, stream: e.Stream,
			body: string(e.Body), truncated: e.Truncated, offset: end,
		})
		return
	}
	f.fifo[e.Stream] = append(f.fifo[e.Stream], mlItem{start: start, end: end, trunc: e.Truncated})
	if err := f.ml.Add(ctx, string(e.Body), e.Stream, mlData{time: e.Time, stream: e.Stream}); err != nil {
		t.log.Warn("multiline aggregation", "path", f.path, "error", err)
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
	attrs.Pod(res, md.Pod)
	attrs.Container(res, md.Container)
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

	budget := t.cfg.MaxBytesPerSweep
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
			if !errors.Is(err, io.EOF) {
				return err
			}
			break
		}
	}

	// Rotation/truncation: the path now names a different file, or it
	// shrank below our position.
	st, err := os.Stat(f.path)
	if err != nil {
		return err
	}
	if inodeOf(st) != f.inode || st.Size() < f.readPos {
		t.reopen(ctx, f)
	}
	return nil
}

// consume splits pending bytes into physical lines and feeds the assembler.
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
		parsed, err := crilog.Parse(line)
		if err != nil {
			continue
		}
		if !f.asm.Pending() {
			f.entryStart = start
		}
		if e, done := f.asm.Add(parsed); done {
			t.feed(ctx, f, e, f.entryStart, f.lineStart)
		}
	}
}

// ensureOpen opens the file at the committed offset on first use.
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
	if f.inode != 0 && f.inode != inode {
		// The checkpoint refers to a rotated-away file; start from the top
		// of the current one.
		start = 0
	}
	if start > st.Size() {
		start = 0
	}
	if _, err := fh.Seek(start, 0); err != nil {
		_ = fh.Close()
		return err
	}
	f.f = fh
	f.inode = inode
	f.readPos = start
	f.lineStart = start
	f.committed = start
	f.pending = f.pending[:0]
	return nil
}

// reopen resets state after rotation; the next sweep reopens from offset 0.
func (t *Tailer) reopen(ctx context.Context, f *file) {
	if f.f != nil {
		_ = f.f.Close()
		f.f = nil
	}
	if e, ok := f.asm.Flush(); ok {
		t.feed(ctx, f, e, f.entryStart, f.lineStart)
	}
	if f.ml != nil {
		_ = f.ml.Stop(ctx) // emit buffered groups; offsets refer to the old file
		clear(f.fifo)
	}
	f.inode = 0
	f.committed = 0
	f.readPos = 0
	f.lineStart = 0
	f.pending = f.pending[:0]
}

// drop closes a removed file, flushing any assembled remainder.
func (t *Tailer) drop(f *file) {
	if e, ok := f.asm.Flush(); ok && f.resolved {
		t.feed(context.Background(), f, e, f.entryStart, f.lineStart)
	}
	if f.ml != nil {
		_ = f.ml.Stop(context.Background())
		clear(f.fifo)
	}
	if f.f != nil {
		_ = f.f.Close()
		f.f = nil
	}
}

// flush exports the batch. On success offsets are committed; on failure the
// files are rewound to the committed offsets so the data is re-read.
func (t *Tailer) flush(ctx context.Context) {
	if len(t.batch) == 0 {
		t.lastFlush = time.Now()
		return
	}
	ld := plog.NewLogs()
	var (
		cur        *file
		slr        plog.ScopeLogs
		maxOffsets = make(map[*file]int64)
	)
	now := pcommon.NewTimestampFromTime(time.Now())
	for _, e := range t.batch {
		if e.file != cur {
			cur = e.file
			rl := ld.ResourceLogs().AppendEmpty()
			cur.resource.CopyTo(rl.Resource())
			slr = rl.ScopeLogs().AppendEmpty()
			slr.Scope().SetName("github.com/JohanLindvall/kubescrape/agent/tailer")
		}
		lr := slr.LogRecords().AppendEmpty()
		lr.SetTimestamp(pcommon.NewTimestampFromTime(e.time))
		lr.SetObservedTimestamp(now)
		lr.Body().SetStr(e.body)
		lr.Attributes().PutStr("log.iostream", e.stream)
		if e.truncated {
			lr.Attributes().PutBool("log.truncated", true)
		}
		if e.match != "" {
			lr.Attributes().PutStr("log.multiline.match", e.match)
		}
		if e.offset > maxOffsets[e.file] {
			maxOffsets[e.file] = e.offset
		}
	}

	if err := t.exportWithRetry(ctx, ld); err != nil {
		t.log.Error("exporting logs failed, rewinding", "records", len(t.batch), "error", err)
		for f := range maxOffsets {
			t.rewind(f)
		}
	} else {
		for f, off := range maxOffsets {
			// Never commit past entries still buffered in the multi-line
			// aggregator; they have not been exported yet.
			if wm, ok := f.watermark(); ok && wm < off {
				off = wm
			}
			if off > f.committed {
				f.committed = off
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
// read again.
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
	_, _ = f.asm.Flush() // discard partial assembly; it will be rebuilt
	if f.ml != nil {
		// Discard buffered groups without emitting: their lines sit after
		// the committed offset and will be re-read and re-fed.
		f.ml = t.newAggregator(f)
		clear(f.fifo)
	}
}

// --- checkpoints ---

type checkpoint struct {
	Offset int64  `json:"offset"`
	Inode  uint64 `json:"inode"`
}

func (t *Tailer) loadCheckpoints() map[string]checkpoint {
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
	if t.cfg.CheckpointFile == "" {
		return
	}
	cps := make(map[string]checkpoint, len(t.files))
	for path, f := range t.files {
		cps[path] = checkpoint{Offset: f.committed, Inode: f.inode}
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
