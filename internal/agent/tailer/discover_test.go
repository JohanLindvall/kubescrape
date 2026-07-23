// Tests for file discovery (discover.go): scanning, watching, source
// claiming and initial checkpoint state.
package tailer

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"syscall"
	"testing"
	"time"

	"github.com/fsnotify/fsnotify"
)

func TestPreexistingFileStartsAtEnd(t *testing.T) {
	dir := t.TempDir()
	writeLog(t, dir, "2026-07-05T09:59:59Z stdout F history")

	exp := &fakeExporter{}
	tl := newTestTailer(dir, "", exp)
	stop := startTailer(t, tl)
	defer stop()

	writeLog(t, dir, "2026-07-05T10:00:00Z stdout F fresh")
	waitFor(t, func() bool { return len(exp.get()) == 1 }, "fresh record")
	if exp.get()[0] != "fresh" {
		t.Fatalf("records = %v", exp.get())
	}
}

func TestWatchDeliversWithoutPolling(t *testing.T) {
	dir := t.TempDir()
	exp := &fakeExporter{}
	tl := newTestTailer(dir, "", exp)
	tl.cfg.Watch = true
	tl.cfg.PollInterval = time.Hour // events must carry everything
	stop := startTailer(t, tl)
	defer stop()

	writeLog(t, dir, "2026-07-05T10:00:00Z stdout F via-events")
	waitFor(t, func() bool { return len(exp.get()) == 1 }, "event-driven record")

	writeLog(t, dir, "2026-07-05T10:00:01Z stdout F more-events")
	waitFor(t, func() bool { return len(exp.get()) == 2 }, "second event-driven record")
}

func TestParseFileName(t *testing.T) {
	id, ns, ok := parseFileName("mypod_myns_app-abc123.log")
	if !ok || id != "abc123" || ns != "myns" {
		t.Fatalf("id=%q ns=%q ok=%v", id, ns, ok)
	}
	for _, bad := range []string{"noext", "nodash.log", "trailing-.log"} {
		if _, _, ok := parseFileName(bad); ok {
			t.Errorf("parseFileName(%q) should fail", bad)
		}
	}
}

func TestExcludeNamespaces(t *testing.T) {
	dir := t.TempDir()
	exp := &fakeExporter{}
	tl := newTestTailer(dir, "", exp)
	tl.cfg.ExcludeNamespaces = []string{"ns1"}
	stop := startTailer(t, tl)
	defer stop()

	writeLog(t, dir, "2026-07-05T10:00:00Z stdout F excluded")
	time.Sleep(3 * time.Second) // > dir rescan interval
	if got := exp.get(); len(got) != 0 {
		t.Fatalf("excluded namespace produced records: %v", got)
	}
}

// TestUnknownFileAutoReadsFromStart pins the "auto" unknown-files semantics:
// when the checkpoint store already has entries (the agent ran before), a
// file present at startup without an entry appeared while the agent was down
// — its content is unshipped, not history, and must be read from the start.
func TestUnknownFileAutoReadsFromStart(t *testing.T) {
	dir := t.TempDir()
	chk := filepath.Join(t.TempDir(), "chk")

	// First run: establish a checkpoint entry for one file.
	exp1 := &fakeExporter{}
	tl1 := newTestTailer(dir, chk, exp1)
	stop1 := startTailer(t, tl1)
	writeLog(t, dir, timeNowCRI()+" stdout F first-run")
	waitFor(t, func() bool { return len(exp1.get()) >= 1 }, "first run exported")
	stop1()

	// While "down": a NEW file appears with content.
	otherName := "pod2_ns1_app-fedcba9876543210.log"
	other := filepath.Join(dir, otherName)
	if err := os.WriteFile(other, []byte(timeNowCRI()+" stdout F while-down\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	// Second run (auto default): the unknown file must be read from offset 0.
	exp2 := &fakeExporter{}
	tl2 := newTestTailer(dir, chk, exp2)
	stop2 := startTailer(t, tl2)
	defer stop2()
	waitFor(t, func() bool {
		for _, r := range exp2.get() {
			if r == "while-down" {
				return true
			}
		}
		return false
	}, "content written while down is shipped")
}

// TestListingDuringRotationDoesNotDropFile reproduces a directory listing
// racing a rename+recreate rotation: scanDir runs in the instant the path is
// absent (between the rename and the recreate) and marks the live file gone.
// A later listing sees the recreated path and must unmark it — otherwise the
// next sweep drops the file with its state and checkpoint, losing every
// inode rotated away before rediscovery.
func TestListingDuringRotationDoesNotDropFile(t *testing.T) {
	dir := t.TempDir()
	exp := &fakeExporter{}
	tl := newTestTailer(dir, "", exp)
	ctx := context.Background()

	tl.scanDir(nil, true) // initial scan: empty dir
	writeLog(t, dir, timeNowCRI()+" stdout F one")
	tl.scanDir(nil, false)
	tl.sweep(ctx, true)
	tl.flush(ctx)

	path := filepath.Join(dir, logName)
	rotateAway(t, dir, 1)
	tl.scanDir(nil, false) // listing in the absent window: marks gone
	writeLog(t, dir, timeNowCRI()+" stdout F two")
	tl.scanDir(nil, false) // path is back: must clear gone

	tl.sweep(ctx, true)
	if _, ok := tl.files[path]; !ok {
		t.Fatal("file dropped after a listing raced the rename+recreate rotation")
	}
	tl.sweep(ctx, true) // reopen marked the file dirty; read the new inode
	tl.flush(ctx)
	got := exp.get()
	if len(got) != 2 || got[0] != "one" || got[1] != "two" {
		t.Fatalf("expected [one two] across the rotation, got %v", got)
	}
}

// TestGoneFileBackBeforeSweepSurvives covers the sweep-side guard: the file
// is marked gone by a listing that raced the rotation and NO further listing
// runs before the sweep. The sweep must re-stat the path and, finding it
// alive, keep the file instead of dropping it.
func TestGoneFileBackBeforeSweepSurvives(t *testing.T) {
	dir := t.TempDir()
	exp := &fakeExporter{}
	tl := newTestTailer(dir, "", exp)
	ctx := context.Background()

	tl.scanDir(nil, true)
	writeLog(t, dir, timeNowCRI()+" stdout F one")
	tl.scanDir(nil, false)
	tl.sweep(ctx, true)
	tl.flush(ctx)

	path := filepath.Join(dir, logName)
	rotateAway(t, dir, 1)
	tl.scanDir(nil, false) // marks gone
	writeLog(t, dir, timeNowCRI()+" stdout F two")

	tl.sweep(ctx, true) // no listing between recreate and sweep
	if _, ok := tl.files[path]; !ok {
		t.Fatal("sweep dropped a file whose path was alive again")
	}
	tl.sweep(ctx, true) // reopen marked the file dirty; read the new inode
	tl.flush(ctx)
	got := exp.get()
	if len(got) != 2 || got[0] != "one" || got[1] != "two" {
		t.Fatalf("expected [one two] across the rotation, got %v", got)
	}

	// A genuinely deleted file must still be dropped.
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	tl.scanDir(nil, false)
	tl.sweep(ctx, true)
	if _, ok := tl.files[path]; ok {
		t.Fatal("genuinely deleted file was not dropped")
	}
}

// unwatchTarget's refcounting and the scan-dir invariant: dropping one of two
// files sharing a target dir keeps the watch; dropping both removes it — unless
// the dir is a DISCOVERY dir, whose OS watch must never be removed (a rotation
// storm would otherwise silence events and cascade into lost segments).
func TestUnwatchTargetRefcountAndScanDirInvariant(t *testing.T) {
	dir := t.TempDir()
	targetDir := filepath.Join(dir, "pods")
	if err := os.MkdirAll(targetDir, 0o755); err != nil {
		t.Fatal(err)
	}
	for _, n := range []string{"a.log", "b.log"} {
		if err := os.WriteFile(filepath.Join(targetDir, n), []byte("x\n"), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	w, err := fsnotify.NewWatcher()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = w.Close() }()
	tl := driveTailer(dir, &fakeExporter{})
	tl.watcher = w
	tl.watchRefs = map[string]int{}
	tl.scanDirs = map[string]struct{}{}

	fa := &file{path: filepath.Join(targetDir, "a.log")}
	fb := &file{path: filepath.Join(targetDir, "b.log")}
	tl.watchTarget(fa)
	tl.watchTarget(fb)
	resolved := fa.targetDir // EvalSymlinks may canonicalize (e.g. /tmp symlinks)
	if resolved == "" || tl.watchRefs[resolved] != 2 {
		t.Fatalf("refs = %v, want 2 on %q", tl.watchRefs, resolved)
	}
	if !slices.Contains(w.WatchList(), resolved) {
		t.Fatalf("target dir not watched: %v", w.WatchList())
	}

	tl.unwatchTarget(fa)
	if tl.watchRefs[resolved] != 1 || !slices.Contains(w.WatchList(), resolved) {
		t.Fatalf("watch dropped with a file still registered: refs=%v list=%v", tl.watchRefs, w.WatchList())
	}
	tl.unwatchTarget(fb)
	if _, ok := tl.watchRefs[resolved]; ok {
		t.Fatalf("refs not cleaned: %v", tl.watchRefs)
	}
	if slices.Contains(w.WatchList(), resolved) {
		t.Fatalf("non-scan dir watch not removed: %v", w.WatchList())
	}
	if len(tl.byTargetDir) != 0 {
		t.Fatalf("byTargetDir not cleaned: %v", tl.byTargetDir)
	}

	// A target dir that IS a discovery dir keeps its OS watch at refcount 0.
	tl.scanDirs[resolved] = struct{}{}
	tl.watchTarget(fa)
	tl.unwatchTarget(fa)
	if !slices.Contains(w.WatchList(), resolved) {
		t.Fatalf("SCAN-DIR INVARIANT BROKEN: discovery dir unwatched at refcount 0: %v", w.WatchList())
	}
}

// A FIFO matched by a source's glob must never be tracked: open(2)/read(2) on
// it block indefinitely and would wedge the single sweep goroutine.
func TestFIFOIsNeverTracked(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	exp := &fakeExporter{}
	tl := newSourceTailer(exp, []Source{{
		Name:    "plain",
		Include: []string{filepath.Join(dir, "*.log")},
	}}, false)

	tl.scanDir(tl.loadCheckpoints(), true) // files created later are new
	fifo := filepath.Join(dir, "pipe.log")
	if err := syscall.Mkfifo(fifo, 0o644); err != nil {
		t.Fatal(err)
	}
	writeLines(t, filepath.Join(dir, "real.log"), "hello")
	tl.scanDir(nil, false)
	done := make(chan struct{})
	go func() {
		tl.sweep(ctx, true) // would block forever on the FIFO without the guard
		tl.flush(ctx)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("sweep wedged (FIFO opened)")
	}
	if _, tracked := tl.files[fifo]; tracked {
		t.Fatal("FIFO tracked")
	}
	if !slices.Contains(exp.get(), "hello") {
		t.Fatalf("regular file not exported alongside the FIFO: %v", exp.get())
	}
}

// An excluded-namespace containerd file is CLAIMED by the containerd source
// even though it is skipped: a later catch-all source must not resurrect it
// (ExcludeNamespaces is the observability feedback-loop guard).
func TestExcludedNamespaceNotResurrectedByLaterSource(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	exp := &fakeExporter{}
	tl := newSourceTailer(exp, []Source{
		{Name: "containers", Include: []string{filepath.Join(dir, "*.log")}, Containerd: true},
		{Name: "catchall", Include: []string{filepath.Join(dir, "*.log")}},
	}, false)
	tl.cfg.ExcludeNamespaces = []string{"ns1"}

	tl.scanDir(tl.loadCheckpoints(), true)
	writeLog(t, dir, "2026-07-05T10:00:00Z stdout F feedback-loop") // pod1_ns1_...
	tl.scanDir(nil, false)
	tl.sweep(ctx, true)
	tl.flush(ctx)

	if f, tracked := tl.files[filepath.Join(dir, logName)]; tracked {
		t.Fatalf("excluded file tracked by source %q", f.source.name)
	}
	if got := exp.get(); len(got) != 0 {
		t.Fatalf("excluded namespace exported via catch-all: %v", got)
	}
}

// A file matched by two sources is claimed by the FIRST (config order), and
// keeps that source's attributes.
func TestFirstMatchingSourceClaimsFile(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	exp := &fakeExporter{}
	tl := newSourceTailer(exp, []Source{
		{Name: "first", Include: []string{filepath.Join(dir, "*.log")}, Attributes: map[string]string{"who": "first"}},
		{Name: "second", Include: []string{filepath.Join(dir, "*.log")}, Attributes: map[string]string{"who": "second"}},
	}, false)

	tl.scanDir(tl.loadCheckpoints(), true)
	path := filepath.Join(dir, "app.log")
	writeLines(t, path, "hello")
	tl.scanDir(nil, false)
	tl.sweep(ctx, true)
	tl.flush(ctx)

	f := tl.files[path]
	if f == nil || f.source.name != "first" {
		t.Fatalf("file claimed by %q, want first", f.source.name)
	}
	if len(tl.files) != 1 {
		t.Fatalf("tracked files = %d, want 1", len(tl.files))
	}
	if !slices.Contains(exp.get(), "hello") {
		t.Fatal("record not exported")
	}
	exp.mu.Lock()
	who, okAttr := exp.resAttrs["who"]
	exp.mu.Unlock()
	if !okAttr || who != "first" {
		t.Fatalf("resource who = %v, want first", who)
	}
}

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
