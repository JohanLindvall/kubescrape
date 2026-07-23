// Tests for the tailer core (tailer.go): Config/New, sweep scheduling and
// idle-close fd management.
package tailer

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"testing"
	"time"

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
	t.drainGone(f)
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
