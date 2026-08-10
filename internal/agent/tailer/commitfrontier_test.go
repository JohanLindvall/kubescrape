// Tests for the commit frontier over bytes the read CONSUMED but never FED
// (ledger.go: file.skipEnd / absorbSkipped) — rate-DROPPED lines, blank lines
// and fully discarded oversized ones. `committed` only ever advances to an
// EXPORTED entry's end, so without this accounting a TRAILING run of them is
// permanently uncommittable.
package tailer

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/plog"
)

// dropTailer builds a rate-limited tailer in DROP mode whose bucket grants
// exactly one line: everything after the first is discarded.
func dropTailer(t testing.TB, cfg Config) (*Tailer, *file) {
	t.Helper()
	cfg.RateLimit, cfg.RateBurst, cfg.RateDrop = 1, 1, true
	return benchTailer(t, cfg)
}

// A TRAILING run of rate-DROPPED lines froze `committed` at the last exported
// entry's end forever: the checkpoint stopped there, kubescrape_log_lag_bytes
// counted deliberately discarded bytes for the file's life, and a restart
// re-read from there and RE-DELIVERED lines this process had already dropped.
// Dropped bytes will never produce an entry, so nothing else can ever move the
// frontier past them.
func TestRateDroppedTailAdvancesTheCommitFrontier(t *testing.T) {
	ctx := context.Background()
	tl, f := dropTailer(t, Config{})

	ts := timeNowCRI()
	kept := ts + " stdout F kept"
	chunk := []byte(kept + "\n" +
		ts + " stdout F dropped-1\n" +
		ts + " stdout F dropped-2\n")
	tl.ingestChunk(ctx, f, chunk, false)

	// The kept line is emitted but NOT exported yet, so its bytes are still
	// owed: the drops behind it must not carry the frontier over it.
	if !emitted(tl, "kept") {
		t.Fatalf("precondition: the granted line was not fed: %v", bodies(tl))
	}
	if f.committed != 0 {
		t.Fatalf("committed = %d before the export, want 0: the skipped tail jumped "+
			"the frontier over a record that has not shipped", f.committed)
	}

	tl.flush(ctx)
	if f.committed != f.readPos {
		t.Fatalf("committed = %d, want readPos %d: the trailing rate-dropped lines "+
			"are uncommittable, so the checkpoint froze below them", f.committed, f.readPos)
	}

	// The bucket is empty now, so this chunk is dropped WHOLE — nothing is fed,
	// no export will ever run for this file, and consume is the only place the
	// frontier can move.
	tl.ingestChunk(ctx, f, []byte(ts+" stdout F dropped-3\n"+ts+" stdout F dropped-4\n"), false)
	if f.committed != f.readPos {
		t.Fatalf("committed = %d, want readPos %d: an all-dropped read left the "+
			"frontier behind with no export coming to move it", f.committed, f.readPos)
	}
}

// The frontier may cross skipped bytes only once every line fed AHEAD of them
// has been exported. A multi-line group still buffered in the pipeline is the
// case that would silently lose data: a crash after such a commit would resume
// past lines the group never emitted.
func TestSkippedTailNeverCommitsOverABufferedGroup(t *testing.T) {
	ctx := context.Background()
	tl, f := dropTailer(t, Config{Multiline: true})

	ts := timeNowCRI()
	// The granted line opens a stack-trace group that stays buffered (no
	// continuation, no age-out yet); the rest of the chunk is dropped.
	chunk := []byte(ts + " stderr F panic: boom\n" +
		ts + " stdout F dropped-1\n" +
		ts + " stdout F dropped-2\n")
	tl.ingestChunk(ctx, f, chunk, false)

	if _, buffered := f.watermark(); !buffered {
		t.Fatal("precondition: nothing is buffered, so this proves nothing")
	}
	if f.committed != 0 {
		t.Fatalf("committed = %d, want 0: the frontier crossed a buffered group's "+
			"bytes, so a crash here would skip lines that were never exported", f.committed)
	}

	// Drain the group and export it: only now may the frontier reach the end.
	tl.stopPipeline(ctx, f)
	tl.flush(ctx)
	if f.committed != f.readPos {
		t.Fatalf("committed = %d, want readPos %d", f.committed, f.readPos)
	}
}

// A blank physical line produces no record, so nothing ever commits its byte.
func TestBlankTrailingLineAdvancesTheCommitFrontier(t *testing.T) {
	ctx := context.Background()
	tl, f := benchTailer(t, Config{})

	tl.ingestChunk(ctx, f, []byte(timeNowCRI()+" stdout F one\n\n"), false)
	tl.flush(ctx)
	if f.committed != f.readPos {
		t.Fatalf("committed = %d, want readPos %d: a trailing blank line froze the frontier",
			f.committed, f.readPos)
	}
}

// The oversized-line discard has the same shape as a rate drop — bytes consumed
// and never fed — with one extra rule: the frontier may only ever name a line
// BOUNDARY, so it must not move while the discard window is open (mid-line).
// Committing there would resume a restart inside the line and export its
// arbitrary remainder as a record.
func TestDiscardedOversizedTailAdvancesTheCommitFrontier(t *testing.T) {
	ctx := context.Background()
	tl, f := benchTailer(t, Config{MaxEntryBytes: 1024})

	// One over-cap line delivered in two pieces: the prefix (no terminator yet)
	// opens the discard window, the second piece closes it.
	tl.ingestChunk(ctx, f, []byte(strings.Repeat("y", tl.cfg.MaxEntryBytes+oversizeSlack+1)), false)
	if !f.discarding {
		t.Fatal("precondition: the oversized prefix was not discarded")
	}
	if f.committed != 0 {
		t.Fatalf("committed = %d while the discard window is open, want 0: that offset "+
			"is mid-LINE and a restart resuming there exports the line's remainder", f.committed)
	}

	tl.ingestChunk(ctx, f, []byte("tail-of-the-oversized-line\n"), false)
	if f.discarding {
		t.Fatal("precondition: the terminator did not close the discard window")
	}
	if f.committed != f.readPos {
		t.Fatalf("committed = %d, want readPos %d: a discarded oversized tail froze the frontier",
			f.committed, f.readPos)
	}
}

// The end-to-end symptom: with the frontier frozen, the persisted checkpoint
// stops at the first trailing drop, the lag gauge counts the discarded bytes
// forever, and a restart re-reads from there and DELIVERS lines the previous
// process counted as dropped.
func TestRateDroppedTailIsNotReDeliveredAfterRestart(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	cp := filepath.Join(t.TempDir(), "positions.json")
	path := filepath.Join(dir, logName)

	exp := &fakeExporter{}
	tl := driveTailer(dir, exp)
	tl.cfg.Positions = mustOpenPositions(t, cp)
	tl.cfg.RateLimit, tl.cfg.RateBurst, tl.cfg.RateDrop = 1, 1, true

	// Discovered AFTER the startup scan, so it is read whole rather than
	// skipped as pre-existing history.
	tl.scanDir(tl.loadCheckpoints(), true)
	rateLines(t, dir, 0, 20)
	tl.scanDir(nil, false)
	tl.sweep(ctx, true)
	tl.flush(ctx)
	tl.saveCheckpoints()

	first := exp.get()
	if len(first) != 1 {
		t.Fatalf("precondition: want exactly the one granted line, got %v", first)
	}
	st, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if got := tl.files[path].committed; got != st.Size() {
		t.Fatalf("committed = %d, want the file size %d", got, st.Size())
	}

	// The published lag must be zero: kubescrape_log_lag_bytes is documented as
	// "bytes on disk not yet exported and committed", and these were discarded
	// on purpose.
	tl.publishStatus()
	for _, fs := range tl.Status() {
		if fs.Path == path && fs.Lag != 0 {
			t.Fatalf("lag = %d for a fully rate-dropped file, want 0 (phantom backlog)", fs.Lag)
		}
	}

	// A fresh process over the same positions store must find nothing owed.
	exp2 := &fakeExporter{}
	tl2 := driveTailer(dir, exp2)
	tl2.cfg.Positions = mustOpenPositions(t, cp)
	tl2.cfg.RateLimit, tl2.cfg.RateBurst, tl2.cfg.RateDrop = 1, 1, true
	tl2.scanDir(tl2.loadCheckpoints(), true)
	tl2.sweep(ctx, true)
	tl2.flush(ctx)
	if got := exp2.get(); len(got) != 0 {
		t.Fatalf("the restart re-delivered %v: lines this fleet already dropped were "+
			"shipped as new because the checkpoint never crossed them", got)
	}
}

// -logs-batch-size is documented (flag help, FLAGS.md, CONFIGURATION.md) as
// "flush after this many entries". The check used to sit at the END of each
// file's sweep iteration, i.e. after readFile had consumed up to
// MaxBytesPerSweep and fed every line of it into the batch — so the real
// threshold was a sweep's worth of lines, measured at 10x the configured size
// on a backlog and 90x with a small one.
func TestBatchSizeIsTheFlushThreshold(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	exp := &fakeExporter{}
	tl := driveTailer(dir, exp)
	tl.cfg.BatchSize = 50

	tl.scanDir(tl.loadCheckpoints(), true)
	const lines = 500 // ~23 KiB: one sweep reads the lot (MaxBytesPerSweep is 1 MiB)
	rateLines(t, dir, 0, lines)
	tl.scanDir(nil, false)
	tl.sweep(ctx, true)
	tl.flush(ctx)

	sizes := exp.batchSizes()
	for i, n := range sizes {
		if n > tl.cfg.BatchSize {
			t.Fatalf("export %d carried %d records against -logs-batch-size %d (all: %v)",
				i, n, tl.cfg.BatchSize, sizes)
		}
	}
	if len(sizes) < lines/tl.cfg.BatchSize {
		t.Fatalf("%d exports for %d lines at a batch size of %d, want >= %d (all: %v)",
			len(sizes), lines, tl.cfg.BatchSize, lines/tl.cfg.BatchSize, sizes)
	}
	if got := len(exp.get()); got != lines {
		t.Fatalf("delivered %d of %d records", got, lines)
	}
}

// A batch flush now happens INSIDE the read loop, so a failed one rewinds the
// file under its own caller: the fd is seeked back to the committed offset and
// the pipeline is purged. The loop must stop rather than read on — otherwise it
// re-feeds the same bytes into a batch whose export just failed, burning export
// attempts on the single sweep goroutine that serves every file on the node.
func TestFailedMidReadFlushStopsThePass(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	exp := &fakeExporter{}
	exp.fail = 3 // exportWithRetry's whole budget: this flush fails and rewinds
	tl := driveTailer(dir, exp)
	tl.cfg.BatchSize = 50

	tl.scanDir(tl.loadCheckpoints(), true)
	const lines = 500
	rateLines(t, dir, 0, lines)
	tl.scanDir(nil, false)
	tl.sweep(ctx, true)
	tl.flush(ctx)

	if exp.attempts != 3 {
		t.Fatalf("%d export attempts in the sweep whose first flush failed, want exactly 3: "+
			"the read loop kept going over the rewound fd", exp.attempts)
	}
	f := tl.files[filepath.Join(dir, logName)]
	if f.readPos != f.committed {
		t.Fatalf("readPos = %d, committed = %d: the pass did not stop at the rewind",
			f.readPos, f.committed)
	}

	// Nothing was delivered, so the re-read must deliver every line exactly once.
	driveUntil(t, ctx, tl, func() bool { return len(exp.get()) >= lines }, "the rewound backlog re-delivered")
	got := exp.get()
	if len(got) != lines {
		t.Fatalf("delivered %d records for %d lines (duplicates from the retried pass?)", len(got), lines)
	}
	if !slices.Contains(got, "line-000") || !slices.Contains(got, "line-499") {
		t.Fatal("the rewound range was not fully re-read")
	}
}

// closeOnFirstExport fails a whole exportWithRetry budget and closes the tailed
// file's fd on the way, so the rewind that follows finds its Seek failing and
// drops the handle — the one state in which readFile can reach its rotation
// check with no fd.
type closeOnFirstExport struct {
	fakeExporter
	fd   func() *os.File
	left int
}

func (c *closeOnFirstExport) ExportLogs(ctx context.Context, ld plog.Logs) error {
	if c.left > 0 {
		c.left--
		if fh := c.fd(); fh != nil {
			_ = fh.Close()
		}
		return errors.New("collector down")
	}
	return c.fakeExporter.ExportLogs(ctx, ld)
}

// With no fd left, the stored inode+fingerprint are the ONLY route back to a
// rotated-away incarnation: ensureOpen's replaced arm compares them against the
// file now at the path and records the old one as an open-ended segment. Running
// the rotation check in that state instead clears them (reopen), and the
// rotated-away inode's whole uncommitted range is then lost with every counter
// flat.
func TestRewindWithoutAnFdLeavesTheRotationToEnsureOpen(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	path := filepath.Join(dir, logName)

	exp := &closeOnFirstExport{}
	tl := driveTailer(dir, exp)
	tl.cfg.BatchSize = 5
	exp.fd = func() *os.File {
		if f := tl.files[path]; f != nil {
			return f.f
		}
		return nil
	}

	tl.scanDir(tl.loadCheckpoints(), true)
	writeLog(t, dir, timeNowCRI()+" stdout F first")
	tl.scanDir(nil, false)
	tl.sweep(ctx, true) // opens the fd and commits "first"
	tl.flush(ctx)
	f := tl.files[path]
	inode, fp := f.inode, f.fp
	if inode == 0 || fp.Len == 0 {
		t.Fatal("precondition: the file has no recorded identity")
	}

	// Appended to the inode we hold, then rotated away and replaced.
	rateLines(t, dir, 0, 20)
	rotateAway(t, dir, 1)
	writeLog(t, dir, timeNowCRI()+" stdout F after-rotate")

	exp.left = 3 // one whole exportWithRetry budget, armed only now
	tl.sweep(ctx, true)
	if f.inode != inode || f.fp != fp {
		t.Fatalf("the rotated-away identity was replaced (inode %d -> %d) while no fd was "+
			"held: nothing can name that incarnation again and its 20 appended lines "+
			"are lost uncounted", inode, f.inode)
	}
	if f.f != nil {
		t.Fatal("precondition: the rewind did not drop the fd, so this proves nothing")
	}

	driveUntil(t, ctx, tl, func() bool {
		got := exp.get()
		return slices.Contains(got, "line-019") && slices.Contains(got, "after-rotate")
	}, "the rotated-away remainder recovered through ensureOpen's replaced arm")
}

// The archive read loop holds the gzip reader across iterations, and a rewind
// CLOSES it (f.gz goes nil). A mid-read flush failure that does not stop the
// loop then dereferences that nil on the next iteration, taking down the single
// sweep goroutine — and with it log collection for the whole node. (An archive
// small enough to decompress in one read escapes the deref and hits the other
// half of the same bug instead: finishArchive marks the rewound file CONSUMED
// and closes its fd, losing every line silently.) The stream here is over one
// 64 KiB read so the loop genuinely iterates.
func TestArchiveFailedMidReadFlushDoesNotReadAClosedReader(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	exp := &fakeExporter{}
	exp.fail = 3
	tl := newArchiveTailer(dir, exp)
	tl.cfg.BatchSize = 50
	tl.retryBackoff = time.Millisecond

	const lines = 2000
	body := make([]string, 0, lines)
	for i := 0; i < lines; i++ {
		body = append(body, "archived line "+strconv.Itoa(i)+" "+strings.Repeat("x", 80))
	}
	writeGzip(t, filepath.Join(dir, "app.log.gz"), body...)

	tl.scanDir(tl.loadCheckpoints(), true)
	tl.sweep(ctx, true) // must not read the closed gzip reader
	tl.flush(ctx)

	driveUntil(t, ctx, tl, func() bool { return len(exp.get()) >= lines }, "the archive re-read after the failed flush")
	if got := len(exp.get()); got != lines {
		t.Fatalf("delivered %d records for %d archived lines", got, lines)
	}
}
