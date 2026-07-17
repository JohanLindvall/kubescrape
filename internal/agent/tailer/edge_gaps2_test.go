package tailer

// Coverage batch 2: the symlink-target watch path (the production layout),
// multiline fifo-orphan accounting, idle-close must-not-close guards, scanDir
// robustness under EACCES and glob failure, the retained-fd archive resume
// with a committed prefix, and per-source multiline overrides.

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"
)

// The production layout: the watched dir holds SYMLINKS to files in other
// directories (as /var/log/containers → /var/log/pods/...). Events fire in
// the TARGET dir; delivery and the watch handover on retarget both depend on
// byTargetDir/watchRefs, which files-directly-in-the-watched-dir tests never
// touch.
func TestSymlinkTargetDirEventsAndRetarget(t *testing.T) {
	linkDir := t.TempDir()
	dirB := t.TempDir()
	dirC := t.TempDir()
	exp := &fakeExporter{}
	tl := New(Config{
		Dir:           linkDir,
		Watch:         true,
		PollInterval:  time.Hour, // only events may drive delivery
		FlushInterval: 30 * time.Millisecond,
		BatchSize:     1000,
		MetadataWait:  time.Second,
		Metadata:      fakeMeta{},
		Exporter:      exp,
	})
	tl.retryBackoff = 10 * time.Millisecond
	stop := startTailer(t, tl)
	defer stop()

	target := filepath.Join(dirB, "0.log")
	writeLines(t, target, "") // exists before the link
	link := filepath.Join(linkDir, logName)
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}

	// (a) an append in the TARGET dir must deliver via its watch.
	time.Sleep(200 * time.Millisecond) // let discovery + watch registration land
	writeLines(t, target, "2026-07-05T10:00:00Z stdout F via-target-watch")
	waitFor(t, func() bool { return slices.Contains(exp.get(), "via-target-watch") },
		"append in the symlink target dir delivered by watch")

	// (b) retarget: the old target rotates away and the symlink points into a
	// NEW directory; the watch must hand over and deliver from there.
	if err := os.Rename(target, target+".1"); err != nil {
		t.Fatal(err)
	}
	newTarget := filepath.Join(dirC, "0.log")
	writeLines(t, newTarget, "2026-07-05T10:00:01Z stdout F via-new-dir")
	if err := os.Remove(link); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(newTarget, link); err != nil {
		t.Fatal(err)
	}
	// The retarget takes several sweeps (detect -> reopen -> read), and with
	// polling disabled only EVENTS schedule sweeps: nudge with discovery
	// events in the watched dir, as real pod churn constantly does.
	deadline := time.Now().Add(5 * time.Second)
	for i := 0; time.Now().Before(deadline); i++ {
		if slices.Contains(exp.get(), "via-new-dir") {
			return
		}
		nudge := filepath.Join(linkDir, "nudge")
		if err := os.WriteFile(nudge, nil, 0o644); err != nil {
			t.Fatal(err)
		}
		_ = os.Remove(nudge)
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("retargeted symlink never delivered from the new target dir: %v", exp.get())
}

// A record exported while ANOTHER stream's multi-line group is still buffered
// has its commit withheld by the build-time watermark clamp. Once the group
// resolves, the withheld high offset must be re-offered (file.exportedHigh) —
// without it, committed freezes below readPos FOREVER: the high entry belongs
// to an earlier batch no later maxOffsets ever sees, so a restart re-reads
// the tail (duplicates), idle-close can never release the fd, and the lag
// gauges show phantom backlog.
func TestWithheldCommitReleasedOnceGroupResolves(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	exp := &fakeExporter{}
	tl := New(Config{
		Dir:              dir,
		PollInterval:     20 * time.Millisecond,
		FlushInterval:    time.Millisecond,
		BatchSize:        1 << 20,
		Multiline:        true,
		MultilineTimeout: 50 * time.Millisecond,
		MaxEntryBytes:    64, // the group below blows straight through this
		MetadataWait:     time.Second,
		Metadata:         fakeMeta{},
		Exporter:         exp,
	})
	tl.retryBackoff = time.Millisecond

	tl.scanDir(tl.loadCheckpoints(), true)
	start, rest := panicLines()
	writeLog(t, dir, append(start, rest...)...)
	tl.scanDir(nil, false)

	deadline := time.Now().Add(5 * time.Second)
	path := filepath.Join(dir, logName)
	for time.Now().Before(deadline) {
		tl.sweep(ctx, true)
		tl.flush(ctx)
		f := tl.files[path]
		st, err := os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}
		if f != nil && f.committed == st.Size() {
			return // everything read is either exported or accounted; no freeze
		}
		time.Sleep(20 * time.Millisecond)
	}
	f := tl.files[path]
	t.Fatalf("checkpoint frozen below file size: committed=%d readPos=%d (withheld high never re-offered)",
		f.committed, f.readPos)
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

// A transient stat failure on a tracked file must not mark it gone —
// gone→drop would delete its checkpoint and a rediscovery would re-ingest
// the whole file. A symlink loop makes stat fail with ELOOP (not ENOENT)
// while the glob still lists the name: exactly the transient-error shape.
func TestScanDirTransientStatFailureKeepsFile(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	exp := &fakeExporter{}
	tl := newSourceTailer(exp, []Source{{
		Name:    "plain",
		Include: []string{filepath.Join(dir, "*.log")},
	}}, false)

	tl.scanDir(tl.loadCheckpoints(), true)
	path := filepath.Join(dir, "app.log")
	writeLines(t, path, "committed-line")
	tl.scanDir(nil, false)
	tl.sweep(ctx, true)
	tl.flush(ctx)
	f := tl.files[path]
	if f == nil || !slices.Contains(exp.get(), "committed-line") {
		t.Fatal("precondition: file not shipped")
	}
	committed := f.committed

	// The path transiently resolves to a symlink loop: stat fails, but the
	// failure proves nothing about the file being gone.
	if err := os.Rename(path, path+".save"); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(path, path); err != nil {
		t.Fatal(err)
	}
	tl.scanDir(nil, false)
	tl.sweep(ctx, true)
	if f.gone {
		t.Fatal("transient stat failure marked a tracked file gone")
	}
	if _, tracked := tl.files[path]; !tracked || f.committed != committed {
		t.Fatalf("file dropped or checkpoint reset (tracked=%v committed=%d)", tracked, f.committed)
	}
}

// A failing glob disables gone-detection for that scan (an errored pattern
// proves nothing about absent files) and warns once, not per sweep.
func TestGlobFailureSuppressesGoneDetection(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	exp := &fakeExporter{}
	tl := newSourceTailer(exp, []Source{{
		Name:    "plain",
		Include: []string{filepath.Join(dir, "*.log")},
	}}, false)

	tl.scanDir(tl.loadCheckpoints(), true)
	path := filepath.Join(dir, "app.log")
	writeLines(t, path, "hello")
	tl.scanDir(nil, false)
	tl.sweep(ctx, true)
	tl.flush(ctx)
	f := tl.files[path]
	if f == nil {
		t.Fatal("file not tracked")
	}

	// Swap in a source whose glob errors (bad pattern).
	good := tl.sources
	tl.sources = []*compiledSource{{name: "broken", include: []string{"/tmp/["}}}
	tl.scanDir(nil, false)
	if f.gone {
		t.Fatal("failed glob marked tracked files gone")
	}
	if !tl.warnedListing {
		t.Fatal("glob failure not flagged for the warn-once")
	}

	// A later good listing resets the warn-once and gone-detection works again.
	tl.sources = good
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	tl.scanDir(nil, false)
	if tl.warnedListing {
		t.Fatal("warn-once not reset by a good listing")
	}
	if !f.gone {
		t.Fatal("gone-detection did not resume after the good listing")
	}
}

// openArchive's retained-fd resume with committed > 0: after a partial commit
// and an unlink, the re-decompression must discard exactly the committed
// prefix — every line ships exactly once.
func TestArchiveRetainedFdResumeSkipsCommittedPrefix(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	exp := &fakeExporter{}
	tl := newSourceTailer(exp, []Source{{
		Name:    "archives",
		Include: []string{filepath.Join(dir, "*.log.gz")},
	}}, false)
	// One decompressed line is ~8 bytes; cap the sweep to read just line A.
	tl.cfg.MaxBytesPerSweep = 8

	path := filepath.Join(dir, "app.log.gz")
	tl.scanDir(tl.loadCheckpoints(), true)
	writeGzip(t, path, "line-A", "line-B", "line-C")
	tl.scanDir(nil, false)
	tl.sweep(ctx, true) // reads only line-A (byte budget)
	tl.flush(ctx)       // commits it
	if got := exp.get(); !slices.Equal(got, []string{"line-A"}) {
		t.Fatalf("first sweep = %v, want [line-A]", got)
	}

	// Read the rest; the export FAILS (rewind: reader closed, fd retained).
	exp.mu.Lock()
	exp.fail = 3 * 4
	exp.mu.Unlock()
	tl.cfg.MaxBytesPerSweep = 1 << 20
	tl.sweep(ctx, true)
	tl.flush(ctx)

	// The archive is pruned; the retained fd is the only handle.
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	exp.mu.Lock()
	exp.fail = 0
	exp.mu.Unlock()

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		tl.sweep(ctx, true)
		tl.flush(ctx)
		got := exp.get()
		if slices.Contains(got, "line-B") && slices.Contains(got, "line-C") {
			counts := map[string]int{}
			for _, r := range got {
				counts[r]++
			}
			if counts["line-A"] != 1 {
				t.Fatalf("committed prefix not skipped exactly (line-A x%d): %v", counts["line-A"], got)
			}
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("archive remainder never shipped from the retained fd: %v", exp.get())
}

// Per-source Multiline overrides the global default in both directions.
func TestPerSourceMultilineOverride(t *testing.T) {
	on, off := true, false
	for _, tc := range []struct {
		global bool
		source *bool
		want   bool
	}{
		{false, &on, true},
		{true, &off, false},
		{false, nil, false},
		{true, nil, true},
	} {
		srcs := compileSources([]Source{{
			Name:      "s",
			Include:   []string{"/tmp/*.log"},
			Multiline: tc.source,
		}}, "", tc.global)
		if got := srcs[0].multiline; got != tc.want {
			t.Errorf("global=%v source=%v: multiline = %v, want %v",
				tc.global, ptrStr(tc.source), got, tc.want)
		}
	}
}

func ptrStr(b *bool) string {
	if b == nil {
		return "nil"
	}
	if *b {
		return "true"
	}
	return "false"
}
