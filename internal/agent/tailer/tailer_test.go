// Tests for the tailer core (tailer.go): Config/New, sweep scheduling and
// idle-close fd management.
package tailer

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/JohanLindvall/kubescrape/internal/agent/positions"
	"go.opentelemetry.io/collector/pdata/pcommon"
)

func TestTailAndExport(t *testing.T) {
	dir := t.TempDir()
	exp := &fakeExporter{}
	tl := newTestTailer(dir, "", exp)
	stop := startTailer(t, tl)
	defer stop()

	// The file appears after the tailer starts, so it is read from the top.
	writeLog(t, dir,
		"2026-07-05T10:00:00Z stdout F hello",
		"2026-07-05T10:00:01Z stderr F oops",
		"2026-07-05T10:00:02Z stdout P multi ",
		"2026-07-05T10:00:03Z stdout F line",
	)
	waitFor(t, func() bool { return len(exp.get()) == 3 }, "3 log records")

	got := exp.get()
	if got[0] != "hello" || got[1] != "oops" || got[2] != "multi line" {
		t.Fatalf("records = %v", got)
	}

	// Appends are picked up incrementally.
	writeLog(t, dir, "2026-07-05T10:00:04Z stdout F more")
	waitFor(t, func() bool { return len(exp.get()) == 4 }, "4th record")
}

// TestEventSweepsNotStarvedByContinuousWrites guards the debounce against
// per-event re-arming: a file written more often than the debounce interval
// must still get event-driven sweeps (the poll interval here is far too long
// to deliver anything within the test deadline). With per-event Reset the
// debounce timer never fires under sustained writes, sweeps degrade to the
// poll fallback, and sub-poll-interval rename rotations silently lose whole
// segments.
func TestEventSweepsNotStarvedByContinuousWrites(t *testing.T) {
	dir := t.TempDir()
	exp := &fakeExporter{}
	tl := New(Config{
		Dir:           dir,
		Watch:         true,
		PollInterval:  time.Hour, // events must carry the test alone
		FlushInterval: 50 * time.Millisecond,
		BatchSize:     1000,
		MetadataWait:  time.Second,
		Metadata:      fakeMeta{},
		Exporter:      exp,
	})
	tl.retryBackoff = 10 * time.Millisecond
	stop := startTailer(t, tl)
	defer stop()

	// Continuous writes: an event at least every few milliseconds.
	writerCtx, cancelWriter := context.WithCancel(context.Background())
	defer cancelWriter()
	go func() {
		for i := 0; writerCtx.Err() == nil; i++ {
			writeLog(t, dir, timeNowCRI()+" stdout F line"+strconv.Itoa(i))
			time.Sleep(2 * time.Millisecond)
		}
	}()

	waitFor(t, func() bool { return len(exp.get()) > 0 }, "event-driven sweep exports under sustained writes")
}

// TestIdleCloseReleasesAndReopens: a fully-caught-up idle file's fd closes
// after IdleClose, and the file transparently reopens and resumes on new
// activity without loss or duplication.
func TestIdleCloseReleasesAndReopens(t *testing.T) {
	dir := t.TempDir()
	exp := &fakeExporter{}
	tl := newTestTailer(dir, filepath.Join(t.TempDir(), "chk"), exp)
	tl.cfg.IdleClose = 200 * time.Millisecond
	stop := startTailer(t, tl)
	defer stop()

	writeLog(t, dir, timeNowCRI()+" stdout F before-idle")
	waitFor(t, func() bool { return len(exp.get()) == 1 }, "first line exported")

	// Age the file's mtime past IdleClose so housekeeping closes the fd.
	old := time.Now().Add(-time.Minute)
	if err := os.Chtimes(filepath.Join(dir, logName), old, old); err != nil {
		t.Fatal(err)
	}
	time.Sleep(500 * time.Millisecond) // let housekeeping close the idle fd

	writeLog(t, dir, timeNowCRI()+" stdout F after-idle")
	waitFor(t, func() bool {
		recs := exp.get()
		return len(recs) == 2 && recs[1] == "after-idle"
	}, "file reopened and resumed after idle close")
}

// drop drains a vanished file into the batch and releases it unconditionally
// (test helper; production uses drainGone/release so the fd outlives a failed
// export).
func (t *Tailer) drop(f *file) {
	t.drainGone(context.Background(), f)
	t.release(f)
}

// idleFile builds a tracked, resolved file with `content` already on disk and
// the tailer caught up to `committed`.
func idleFile(t *testing.T, tl *Tailer, dir, content string, committed int64) *file {
	t.Helper()
	path := filepath.Join(dir, logName)
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	f := &file{
		path:        path,
		source:      &compiledSource{name: "containers", containerd: true},
		containerID: "0123456789abcdef",
		resolved:    true,
		resource:    pcommon.NewResource(),
	}
	tl.newPipeline(f)
	tl.files[path] = f
	if err := tl.ensureOpen(f); err != nil {
		t.Fatal(err)
	}
	f.committed, f.readPos, f.lineStart = committed, committed, committed
	if _, err := f.f.Seek(committed, 0); err != nil {
		t.Fatal(err)
	}
	return f
}

// The zero-loss baseline: a file whose fd is still held recovers its unread
// tail from an UNLINKED inode, because the fd is the only handle left to it.
// This is what -logs-idle-close is off by default to protect.
func TestDropRecoversUnlinkedTailWithFDHeld(t *testing.T) {
	dir := t.TempDir()
	tl := newTestTailer(dir, "", &fakeExporter{})
	first := timeNowCRI() + " stdout F first\n"
	f := idleFile(t, tl, dir, first, int64(len(first)))

	// The container writes its last line; the log file is then removed.
	last := timeNowCRI() + " stdout F LAST-LINE\n"
	fh, err := os.OpenFile(f.path, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fh.WriteString(last); err != nil {
		t.Fatal(err)
	}
	_ = fh.Close()
	if err := os.Remove(f.path); err != nil {
		t.Fatal(err)
	}

	tl.drop(f)
	if !emitted(tl, "LAST-LINE") {
		t.Fatalf("fd held: unread tail of the unlinked inode was NOT recovered; batch=%v", bodies(tl))
	}
}

// The counterpart: with the fd released (as -logs-idle-close does), the
// unlinked inode is unreachable and its tail is lost. This test PINS that
// trade-off — it is why the flag defaults to 0. If someone makes idle-close
// the default again, this is the guarantee they are giving up.
func TestIdleCloseForfeitsUnlinkedTail(t *testing.T) {
	dir := t.TempDir()
	tl := newTestTailer(dir, "", &fakeExporter{})
	first := timeNowCRI() + " stdout F first\n"
	f := idleFile(t, tl, dir, first, int64(len(first)))

	// Idle-close releases the fd (the file was caught up at that moment).
	_ = f.f.Close()
	f.f = nil

	last := timeNowCRI() + " stdout F LAST-LINE\n"
	fh, err := os.OpenFile(f.path, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fh.WriteString(last); err != nil {
		t.Fatal(err)
	}
	_ = fh.Close()
	if err := os.Remove(f.path); err != nil {
		t.Fatal(err)
	}

	tl.drop(f)
	if emitted(tl, "LAST-LINE") {
		t.Fatal("the unlinked tail was recovered without an fd — if this now works, " +
			"idle-close no longer forfeits the guarantee and the default can be revisited")
	}
}

// New() defaulting: batch size, rate burst 2x limit.
func TestNewConfigDefaults(t *testing.T) {
	tl := New(Config{Dir: "/tmp", RateLimit: 5, Metadata: fakeMeta{}, Exporter: nullExporter{}})
	if tl.cfg.RateBurst != 10 {
		t.Errorf("RateBurst = %v, want 10 (2x RateLimit)", tl.cfg.RateBurst)
	}
	if tl.cfg.BatchSize <= 0 {
		t.Errorf("BatchSize = %d, want a positive default", tl.cfg.BatchSize)
	}
	if tl.cfg.MaxEntryBytes <= 0 || tl.cfg.MaxBytesPerSweep <= 0 {
		t.Errorf("size defaults missing: MaxEntryBytes=%d MaxBytesPerSweep=%d", tl.cfg.MaxEntryBytes, tl.cfg.MaxBytesPerSweep)
	}
}

// closeIdleFiles' must-not-close guards: an idle deadline never pulls the fd
// of a file with uncommitted data, a rate-limit pause, or unread bytes on
// disk (the re-stat race guard).
func TestIdleCloseGuards(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	exp := &fakeExporter{}
	tl := driveTailer(dir, exp)
	tl.cfg.IdleClose = time.Millisecond

	tl.scanDir(tl.loadCheckpoints(), true)
	writeLog(t, dir, "2026-07-05T10:00:00Z stdout F one")
	tl.scanDir(nil, false)
	path := filepath.Join(dir, logName)

	// (a) uncommitted data (export failing): fd must stay.
	exp.mu.Lock()
	exp.fail = 3
	exp.mu.Unlock()
	tl.sweep(ctx, true)
	tl.flush(ctx) // fails; readPos rewound to committed=0 but content unshipped
	f := tl.files[path]
	tl.sweep(ctx, true) // re-reads: readPos > committed
	if f.readPos == f.committed {
		t.Fatalf("precondition: want uncommitted data (readPos=%d committed=%d)", f.readPos, f.committed)
	}
	time.Sleep(5 * time.Millisecond)
	tl.lastIdleScan = time.Time{}
	tl.closeIdleFiles()
	if f.f == nil {
		t.Fatal("idle-close pulled the fd of a file with uncommitted data")
	}

	// Deliver everything; now the file is caught up.
	exp.mu.Lock()
	exp.fail = 0
	exp.mu.Unlock()
	tl.flush(ctx)
	if f.readPos != f.committed {
		t.Fatalf("file not caught up (readPos=%d committed=%d)", f.readPos, f.committed)
	}

	// (b) bytes appended after the last read (unswept write): the re-stat
	// guard must keep the fd even though the deadline passed.
	writeLog(t, dir, "2026-07-05T10:00:01Z stdout F unswept")
	time.Sleep(5 * time.Millisecond)
	tl.lastIdleScan = time.Time{}
	tl.closeIdleFiles()
	if f.f == nil {
		t.Fatal("idle-close pulled the fd with unread bytes on disk")
	}
	tl.sweep(ctx, true)
	tl.flush(ctx)

	// (c) a rate-limit paused file keeps its fd.
	f.limited = true
	time.Sleep(5 * time.Millisecond)
	tl.lastIdleScan = time.Time{}
	tl.closeIdleFiles()
	if f.f == nil {
		t.Fatal("idle-close pulled the fd of a rate-limit paused file")
	}
	f.limited = false

	// Control: with every guard clear, the deadline DOES close the fd.
	time.Sleep(5 * time.Millisecond)
	tl.lastIdleScan = time.Time{}
	tl.closeIdleFiles()
	if f.f != nil {
		t.Fatal("idle-close kept the fd of a fully-caught-up idle file")
	}
	// And activity reopens it with identity re-verified.
	writeLog(t, dir, "2026-07-05T10:00:02Z stdout F reopened")
	tl.sweep(ctx, true)
	tl.flush(ctx)
	if got := exp.get(); !slices.Contains(got, "reopened") {
		t.Fatalf("closed idle file never reopened: %v", got)
	}
}

// closeIdle drives one file through idle-close: age its mtime past IdleClose,
// let a sweep observe the new mtime, and force the idle scan.
func closeIdle(t *testing.T, tl *Tailer, ctx context.Context, path string, f *file) {
	t.Helper()
	old := time.Now().Add(-time.Minute)
	if err := os.Chtimes(path, old, old); err != nil {
		t.Fatal(err)
	}
	tl.sweep(ctx, true) // re-stat: lastMod picks up the aged mtime, no content change
	tl.flush(ctx)
	tl.lastIdleScan = time.Time{}
	tl.closeIdleFiles()
	if f.f != nil {
		t.Fatal("precondition: idle fd not closed")
	}
	if !f.idleClosed {
		t.Fatal("precondition: idleClosed not marked")
	}
}

// An idle-closed fd must STAY closed across activity-free poll sweeps: the
// poll case sweeps every tracked file each PollInterval, and readFile's
// unconditional ensureOpen reopened any f.f == nil file (fingerprint read
// included) just for closeIdleFiles to re-close it on its own, coarser
// cadence — idle fds were open ~98% of steady state, defeating
// -logs-idle-close. An append IS activity and must still reopen and deliver.
func TestIdleClosedFileStaysClosedAcrossPollSweeps(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	exp := &fakeExporter{}
	tl := driveTailer(dir, exp)
	tl.cfg.IdleClose = time.Millisecond

	tl.scanDir(tl.loadCheckpoints(), true)
	writeLog(t, dir, timeNowCRI()+" stdout F one")
	tl.scanDir(nil, false)
	path := filepath.Join(dir, logName)
	tl.sweep(ctx, true)
	tl.flush(ctx)
	f := tl.files[path]
	closeIdle(t, tl, ctx, path, f)

	// (a) no activity: poll sweeps leave the fd closed.
	for range 5 {
		tl.sweep(ctx, true)
		tl.flush(ctx)
		if f.f != nil {
			t.Fatal("a poll sweep reopened an idle-closed file with no activity")
		}
	}

	// (b) an append is activity: the stat gate sees the size move, the file
	// reopens (identity re-verified) and the new line is delivered.
	writeLog(t, dir, timeNowCRI()+" stdout F after-idle")
	driveUntil(t, ctx, tl, func() bool { return slices.Contains(exp.get(), "after-idle") },
		"append after idle-close delivered")
}

// A rename rotation while the fd is idle-closed must still be recovered: the
// stat gate sees a different inode at the path and falls through to
// ensureOpen, whose replaced arm records the old incarnation as an open-ended
// segment and replays its unshipped remainder via findRotated. The gate must
// never swallow that detection.
func TestIdleClosedRotationRecovered(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	exp := &fakeExporter{}
	tl := driveTailer(dir, exp)
	tl.cfg.IdleClose = time.Millisecond

	tl.scanDir(tl.loadCheckpoints(), true)
	writeLog(t, dir, timeNowCRI()+" stdout F one")
	tl.scanDir(nil, false)
	path := filepath.Join(dir, logName)
	tl.sweep(ctx, true)
	tl.flush(ctx)
	f := tl.files[path]
	closeIdle(t, tl, ctx, path, f)

	// While no fd is held: the writer appends its final line, the runtime
	// rotates the file away, and a fresh incarnation appears at the path.
	writeLog(t, dir, timeNowCRI()+" stdout F final-before-rotate")
	rotateAway(t, dir, 1)
	writeLog(t, dir, timeNowCRI()+" stdout F after-rotate")

	driveUntil(t, ctx, tl, func() bool {
		got := exp.get()
		return slices.Contains(got, "final-before-rotate") && slices.Contains(got, "after-rotate")
	}, "rotation while idle-closed recovered through the replaced arm")
	driveUntil(t, ctx, tl, func() bool { return len(f.segments) == 0 },
		"open-ended segment retired after recovery")
}

// A caught-up file whose trailing bytes never entered the pipeline must still
// idle-close. The old guard compared readPos to committed, and such bytes can
// never commit (no entry ever covers them), so those files held their fd
// forever; the guard compares fedEnd() to committed like every sibling
// completion decision.
//
// The tail here is the prefix of an oversized line whose discard window is
// still OPEN, which is the one member of that class the commit frontier cannot
// absorb: file.skipEnd only ever names a line BOUNDARY, and this frontier sits
// mid-line (blank and rate-DROPPED tails are absorbed instead, so for those the
// two guards now agree — see TestBlankTrailingLineAdvancesTheCommitFrontier).
func TestIdleCloseWithNeverFedTrailingBytes(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	exp := &fakeExporter{}
	tl := driveTailer(dir, exp)
	tl.cfg.IdleClose = time.Millisecond
	tl.cfg.MaxEntryBytes = 1024

	tl.scanDir(tl.loadCheckpoints(), true)
	path := filepath.Join(dir, logName)
	// One over-cap line with no terminator: the prefix is discarded unseen and
	// the discard window stays open.
	if err := os.WriteFile(path, []byte(strings.Repeat("y", tl.cfg.MaxEntryBytes+oversizeSlack+1)), 0o644); err != nil {
		t.Fatal(err)
	}
	tl.scanDir(nil, false)
	tl.sweep(ctx, true)
	tl.flush(ctx)
	f := tl.files[path]
	if !f.discarding {
		t.Fatal("precondition: the oversized prefix was not discarded")
	}
	if f.readPos == f.committed {
		t.Fatalf("precondition: want never-fed trailing bytes (readPos=%d committed=%d)",
			f.readPos, f.committed)
	}

	old := time.Now().Add(-time.Minute)
	if err := os.Chtimes(path, old, old); err != nil {
		t.Fatal(err)
	}
	tl.sweep(ctx, true)
	tl.flush(ctx)
	tl.lastIdleScan = time.Time{}
	tl.closeIdleFiles()
	if f.f != nil {
		t.Fatal("a caught-up file with never-fed trailing bytes never idle-closed")
	}
}

// allowLine grants only whole tokens and caps the bucket at RateBurst, so an
// effective burst below 1 could never grant: -logs-rate-limit=0.4 (burst
// derived as 2x) wedged every file in pause mode and discarded 100% in drop
// mode, and -check-config passed it. New floors the burst at one grantable
// token — validateConfig lives in cmd/kubescrape-agent and cannot carry the
// guard, so the constructor does.
func TestSubOneRateBurstIsFlooredToGrantable(t *testing.T) {
	for _, cfg := range []Config{
		{RateLimit: 0.4},               // derived burst would be 0.8
		{RateLimit: 2, RateBurst: 0.5}, // explicit sub-1 burst
	} {
		cfg.Dir = t.TempDir()
		cfg.Metadata = fakeMeta{}
		cfg.Exporter = &fakeExporter{}
		tl := New(cfg)
		if tl.cfg.RateBurst < 1 {
			t.Fatalf("RateLimit=%v: RateBurst=%v cannot hold one token, so no line is ever granted",
				cfg.RateLimit, tl.cfg.RateBurst)
		}
		if !tl.allowLine(&file{}) {
			t.Fatalf("RateLimit=%v RateBurst=%v: first line refused", cfg.RateLimit, tl.cfg.RateBurst)
		}
	}
}

// An idle file whose ROTATED SEGMENT is still owed keeps its fd. The replay
// only ever runs from readFile, whose idle-close stat gate returns BEFORE
// feedSegments for a file that has not changed on disk — so releasing the fd
// here defers the rest of the segment until the live file is written to again,
// which for a stopped container is never: the lines are neither delivered nor
// counted lost, and the file never settles.
func TestIdleCloseKeepsTheFdWhileARotatedSegmentIsOwed(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	pos := mustOpenPositions(t, filepath.Join(t.TempDir(), "pos.json"))
	path := filepath.Join(dir, logName)

	// The rotated inode, checkpointed as an owed Pending range.
	rot := path + ".1"
	// Fixed-WIDTH timestamps: RFC3339Nano trims trailing zeros, so a per-line
	// byte budget derived from the first line would not hold for the rest.
	var segLines []string
	for i := 0; i < 6; i++ {
		segLines = append(segLines, fmt.Sprintf("2026-07-05T10:00:%02dZ stdout F seg-%d", i, i))
	}
	writeLines(t, rot, segLines...)
	rotIno := inodeOfPath(t, rot)
	rst, err := os.Stat(rot)
	if err != nil {
		t.Fatal(err)
	}

	// The live tail was fully shipped before the crash, so nothing about IT
	// keeps the fd: the segment is the only guard left.
	writeLog(t, dir, timeNowCRI()+" stdout F tail-one")
	tst, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := pos.SetLogs(map[string]positions.LogPos{path: {
		Offset: tst.Size(), Inode: inodeOfPath(t, path),
		Pending: []positions.Prefix{{Inode: rotIno, From: 0, To: rst.Size()}},
	}}); err != nil {
		t.Fatal(err)
	}

	exp := &fakeExporter{}
	tl := driveTailer(dir, exp)
	tl.cfg.Positions = pos
	tl.cfg.IdleClose = time.Millisecond
	tl.cfg.MaxBytesPerSweep = len(segLines[0]) + 1 // one segment line per sweep
	tl.scanDir(tl.loadCheckpoints(), true)

	tl.sweep(ctx, true) // the replay's first pass
	tl.flush(ctx)
	f := tl.files[path]
	if f == nil {
		t.Fatal("setup: file not tracked")
	}
	if len(f.segments) == 0 || f.segmentsFed {
		t.Fatalf("setup: want an unfinished replay (segments=%d fed=%v)", len(f.segments), f.segmentsFed)
	}
	if f.readPos != f.committed || f.readPos != tst.Size() {
		t.Fatalf("setup: tail not caught up (readPos=%d committed=%d size=%d)", f.readPos, f.committed, tst.Size())
	}

	// Age the tail's mtime past IdleClose and let a sweep observe it, so every
	// OTHER idle-close guard is clear.
	old := time.Now().Add(-time.Minute)
	if err := os.Chtimes(path, old, old); err != nil {
		t.Fatal(err)
	}
	tl.sweep(ctx, true)
	tl.flush(ctx)
	tl.lastIdleScan = time.Time{}
	tl.closeIdleFiles()
	if f.f == nil {
		t.Fatal("idle-close pulled the fd of a file with a rotated segment still owed: " +
			"readFile's idle stat gate now returns before feedSegments, so the replay never resumes")
	}

	// And the whole segment does finish, with no further writes to the file.
	driveUntil(t, ctx, tl, func() bool {
		return slices.Contains(exp.get(), "seg-5")
	}, "the rest of the rotated segment being replayed while the live file stays idle")
}
