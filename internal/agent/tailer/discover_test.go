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

	"github.com/JohanLindvall/kubescrape/internal/agent/positions"
	"github.com/JohanLindvall/kubescrape/internal/obs"
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

// A SOURCE's own excludeNamespaces is a prohibition, not a routing selector:
// the file it denies is claimed-and-skipped, so a later catch-all source can
// never resurrect it. Testing wantNamespace (which merges the deny and allow
// halves) instead of the deny alone lost this guard, and the consequence is
// the observability feedback loop — the agent tails the collector's namespace
// and amplifies exactly when the collector is struggling.
func TestSourceExcludedNamespaceNotResurrectedByLaterSource(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	exp := &fakeExporter{}
	tl := newSourceTailer(exp, []Source{
		{
			Name:              "containers",
			Include:           []string{filepath.Join(dir, "*.log")},
			Containerd:        true,
			ExcludeNamespaces: []string{"ns*"}, // logName is pod1_ns1_app-<id>.log
		},
		{Name: "catchall", Include: []string{filepath.Join(dir, "*.log")}},
	}, false)

	tl.scanDir(tl.loadCheckpoints(), true)
	writeLog(t, dir, "2026-07-05T10:00:00Z stdout F feedback-loop")
	tl.scanDir(nil, false)
	tl.sweep(ctx, true)
	tl.flush(ctx)

	if f, tracked := tl.files[filepath.Join(dir, logName)]; tracked {
		t.Fatalf("source-excluded file tracked by source %q", f.source.name)
	}
	if got := exp.get(); len(got) != 0 {
		t.Fatalf("source-excluded namespace exported via catch-all: %v", got)
	}
}

// The mirror image: an allowlist MISS is routing, not prohibition, so the file
// is deliberately left UNCLAIMED for a later source ("prod through source A,
// the rest through source B"). The deny fix above must not take this with it.
func TestSourceAllowlistMissFallsThroughToLaterSource(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	exp := &fakeExporter{}
	tl := newSourceTailer(exp, []Source{
		{
			Name:       "prod-only",
			Include:    []string{filepath.Join(dir, "*.log")},
			Containerd: true,
			Namespaces: []string{"prod"}, // logName's namespace is ns1
		},
		{Name: "catchall", Include: []string{filepath.Join(dir, "*.log")}, Containerd: true},
	}, false)

	tl.scanDir(tl.loadCheckpoints(), true)
	writeLog(t, dir, "2026-07-05T10:00:00Z stdout F routed")
	tl.scanDir(nil, false)
	driveUntil(t, ctx, tl, func() bool { return slices.Contains(exp.get(), "routed") },
		"the allowlist-missed file collected by the later source")

	f := tl.files[filepath.Join(dir, logName)]
	if f == nil || f.source.name != "catchall" {
		t.Fatalf("file claimed by %v, want the later catchall source", f)
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

// claimPath's non-regular guard covers DISCOVERY only: a TRACKED path
// replaced by a FIFO used to resurrect through the sweep's bare os.Stat, and
// the readFile that followed drained the old fd, saw a new inode, and
// re-opened the path — an open(2) O_RDONLY that blocks forever on a
// writer-less FIFO, wedging the single sweep goroutine (log collection stops
// node-wide with /readyz green and no counter moving).
func TestTrackedPathReplacedByFifoDoesNotWedgeSweep(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	exp := &fakeExporter{}
	tl := driveTailer(dir, exp)
	tl.scanDir(tl.loadCheckpoints(), true)
	writeLog(t, dir, timeNowCRI()+" stdout F before-fifo")
	tl.scanDir(nil, false)
	tl.sweep(ctx, true) // tracked, fd held

	path := filepath.Join(dir, logName)
	if err := os.Rename(path, path+".1"); err != nil {
		t.Fatal(err)
	}
	if err := syscall.Mkfifo(path, 0o644); err != nil {
		t.Fatal(err)
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		for range 3 {
			tl.scanDir(nil, false)
			tl.sweep(ctx, true)
			tl.flush(ctx)
		}
	}()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("sweep wedged opening the FIFO")
	}
	if got := exp.get(); !slices.Contains(got, "before-fifo") {
		t.Fatalf("old inode's lines lost: %q", got)
	}
	if _, tracked := tl.files[path]; tracked {
		t.Fatal("the FIFO impostor was tracked")
	}
}

// The kernel auto-removes an inotify watch when the watched directory itself
// is deleted or moved, and fsnotify drops it from its bookkeeping without an
// event this side can key an invalidation on (the event's Name is the dir, so
// handleEvent attributes it to the parent). A skip list over watchedScan
// therefore never re-added it: a recreated discovery dir was permanently
// degraded to poll cadence, under which sub-poll-interval rename rotations
// lose segments. retryScanWatches must re-register unconditionally.
func TestScanWatchRecoveredAfterDirRecreation(t *testing.T) {
	base := t.TempDir()
	dir := filepath.Join(base, "logs")
	if err := os.Mkdir(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	tl := driveTailer(dir, &fakeExporter{})
	w, err := fsnotify.NewWatcher()
	if err != nil {
		t.Skipf("fsnotify unavailable: %v", err)
	}
	defer func() { _ = w.Close() }()
	go func() { // drain so the watcher's event goroutine never blocks
		for range w.Events {
		}
	}()
	go func() {
		for range w.Errors {
		}
	}()
	tl.watcher = w
	tl.watchRefs = map[string]int{}
	tl.watchedScan = map[string]struct{}{}
	if err := w.Add(dir); err != nil {
		t.Fatal(err)
	}
	tl.watchedScan[dir] = struct{}{}

	if err := os.Remove(dir); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return !slices.Contains(w.WatchList(), dir) },
		"fsnotify dropping the deleted dir's watch")
	if err := os.Mkdir(dir, 0o755); err != nil {
		t.Fatal(err)
	}

	tl.retryScanWatches()
	if !slices.Contains(w.WatchList(), dir) {
		t.Fatal("recreated discovery dir left unwatched: discovery degraded to poll cadence")
	}
}

// A path the glob just listed can vanish before claimPath's stat — a rename
// rotation caught mid-scan. That ENOENT proves the file WAS there moments
// ago, not that it is stably absent: counted as proven absence, the same
// (successful) listing pruned its not-yet-consumed checkpoint entry, so a
// recreated path read from zero and the Pending prefixes — initFile's only
// route back to the rotated inode's unshipped tail — were destroyed with it.
func TestGlobListedButVanishedPathIsUnprovenAbsence(t *testing.T) {
	dir := t.TempDir()
	tl := driveTailer(dir, &fakeExporter{})
	tl.scanDir(tl.loadCheckpoints(), true)

	ghost := filepath.Join(dir, "ghost_ns1_app-ffffffff.log")
	sets := scanSets{seen: map[string]struct{}{}, unproven: map[string]struct{}{}}
	if tl.claimPath(tl.sources[0], ghost, &sets) {
		t.Fatal("a vanished path must not be tracked")
	}
	if _, ok := sets.unproven[ghost]; !ok {
		t.Fatal("glob-listed-but-ENOENT path not marked unproven: this scan would prune its checkpoint entry")
	}
	if _, ok := sets.seen[ghost]; ok {
		t.Fatal("an unstattable path must not count as PRESENT: that suppresses gone-marking of a tracked file")
	}
}

// /var/log/containers/*.log are SYMLINKS, and a readdir-based glob lists a
// DANGLING one forever while os.Stat follows it and returns ENOENT forever. So
// "the stat failed" is not evidence that this path is about to come back, and
// treating it as such — to spare the stored checkpoint, which genuinely needs
// that grace — suppressed gone-marking with it: the file was never released,
// pinning its fd, its unlinked inode's disk space, its files-map entry and its
// checkpoint entry for the process lifetime, with nothing logged and no counter
// moving. The two proofs are separate (scanSets), and this pins both halves at
// once: the tracked file goes, the un-consumed checkpoint entry stays.
func TestDanglingSymlinkGoesWhileItsCheckpointStays(t *testing.T) {
	dir := t.TempDir()
	targets := t.TempDir()

	target := filepath.Join(targets, "0.log")
	writeLines(t, target, timeNowCRI()+" stdout F hello")
	link := filepath.Join(dir, logName)
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}

	tl := driveTailer(dir, &fakeExporter{})
	tl.scanDir(tl.loadCheckpoints(), true)
	f, ok := tl.files[link]
	if !ok {
		t.Fatal("setup: the symlinked container log was not discovered")
	}
	if f.f != nil {
		t.Fatal("setup: this fixture never sweeps, so discovery must be the only gone-detector")
	}

	// A second dangling symlink, this one NOT tracked but carrying a stored
	// offset — the rename-rotation-caught-mid-scan shape the grace exists for.
	ghostTarget := filepath.Join(targets, "1.log")
	ghost := filepath.Join(dir, "other_ns1_app-ffffffffffffffff.log")
	writeLines(t, ghostTarget, "x")
	if err := os.Symlink(ghostTarget, ghost); err != nil {
		t.Fatal(err)
	}
	tl.checkpoints = map[string]checkpoint{ghost: {Offset: 7, Inode: 42}}

	// Both targets go; the symlinks stay, so the glob keeps listing both paths.
	if err := os.Remove(target); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(ghostTarget); err != nil {
		t.Fatal(err)
	}
	tl.scanDir(nil, false)

	if !f.gone {
		t.Fatal("a permanently dangling symlink was not marked gone: its fd, its unlinked inode, " +
			"its files-map entry and its checkpoint entry pin for the process lifetime")
	}
	if _, ok := tl.checkpoints[ghost]; !ok {
		t.Fatal("an unstattable path's stored offset was pruned: a recreated path would read from " +
			"zero and its Pending prefixes — the only route back to a rotated inode's tail — are gone")
	}
}

// Discovery skips its stat for a tracked, open file (claimPath / file.swept),
// because the sweep stats that same path itself once per sweep. That skip is
// only sound while the sweep's own ENOENT marks the file gone — otherwise it
// restores exactly the dangling-symlink leak, one layer down and for the files
// that matter most (the open ones).
func TestVanishedPathOfAnOpenFileIsReleased(t *testing.T) {
	dir := t.TempDir()
	targets := t.TempDir()
	ctx := context.Background()

	exp := &fakeExporter{}
	tl := driveTailer(dir, exp)
	tl.scanDir(tl.loadCheckpoints(), true) // empty dir: what follows is NEW, not history

	target := filepath.Join(targets, "0.log")
	writeLines(t, target, timeNowCRI()+" stdout F hello")
	link := filepath.Join(dir, logName)
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}
	tl.scanDir(nil, false)
	driveUntil(t, ctx, tl, func() bool { return slices.Contains(exp.get(), "hello") },
		"the symlinked log being read")
	if f := tl.files[link]; f == nil || f.f == nil {
		t.Fatal("setup: the file must be tracked with an open fd")
	}

	if err := os.Remove(target); err != nil {
		t.Fatal(err)
	}
	driveUntil(t, ctx, tl, func() bool { _, ok := tl.files[link]; return !ok },
		"the file whose path stopped resolving being drained and released")
}

// Segments belong to the incremental path: readArchive never calls
// feedSegments, so a Pending entry restored onto a COMPRESSED file is owed
// forever — settledGone can never fire and saveCheckpoints rewrites the stale
// list on every save for the life of the process. (Reachable when a path with
// Pending entries is later matched by a `compressed: true` source.) The
// open-ended synthesis right below it was already gated this way.
func TestCompressedFileRestoresNoPendingSegments(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	path := filepath.Join(dir, "app.log.gz")
	writeGzip(t, path, "arc-one", "arc-two")

	pos := mustOpenPositions(t, filepath.Join(t.TempDir(), "pos.json"))
	if err := pos.SetLogs(map[string]positions.LogPos{path: {
		Offset: 0, Inode: inodeOfPath(t, path),
		Pending: []positions.Prefix{{Inode: 424242, From: 0, To: 100}},
	}}); err != nil {
		t.Fatal(err)
	}

	exp := &fakeExporter{}
	tl := newArchiveTailer(dir, exp)
	tl.cfg.Positions = pos
	tl.scanDir(tl.loadCheckpoints(), true)

	f, ok := tl.files[path]
	if !ok {
		t.Fatal("setup: the archive was not discovered")
	}
	if n := len(f.segments); n != 0 {
		t.Fatalf("a compressed file restored %d Pending segment(s) nothing will ever feed: "+
			"settledGone can never fire and the stale list is rewritten on every save", n)
	}
	driveUntil(t, ctx, tl, func() bool { return slices.Contains(exp.get(), "arc-one") },
		"the archive still being read")
}

// A gone verdict is withdrawn by TWO paths — a listing that finds the path
// again (claimPath) and the sweep's own stat of a gone file — and withdrawing
// it is ONE decision: gone, goneEnd and goneStalledSince die together
// (file.resurrect). They were written twice and had already diverged, the
// discovery half leaving goneEnd pinned at the previous incarnation's EOF: a
// completion check then measures the new stream against the old one's end, so
// the file either never settles (entry, fd and checkpoint pinned for the
// process lifetime, nothing counted) or reports a remainder it never owed.
func TestBothResurrectPathsWithdrawTheWholeGoneVerdict(t *testing.T) {
	for _, tc := range []struct {
		name string
		// withdraw drives the path that must clear the verdict, with the
		// file's path present on disk.
		withdraw func(tl *Tailer, f *file)
	}{
		{"listing (claimPath)", func(tl *Tailer, _ *file) { tl.scanDir(nil, false) }},
		{"sweep's stat of a gone file", func(tl *Tailer, _ *file) { tl.sweep(context.Background(), true) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			exp := &fakeExporter{}
			tl := newTestTailer(dir, "", exp)
			writeLog(t, dir, "2026-07-05T10:00:00Z stdout F one")
			tl.scanDir(tl.loadCheckpoints(), true)
			f := tl.files[filepath.Join(dir, logName)]
			if f == nil {
				t.Fatal("setup: file not tracked")
			}

			f.gone = true
			f.goneEnd = 12345
			f.goneStalledSince = time.Now().Add(-time.Hour)

			tc.withdraw(tl, f)

			if f.gone {
				t.Fatal("gone not cleared for a path that is back on disk")
			}
			if f.goneEnd != 0 {
				t.Fatalf("goneEnd = %d, want 0: it pins the PREVIOUS incarnation's EOF, and the live "+
					"file's offsets are measured against it forever (settledGone never fires)", f.goneEnd)
			}
			if !f.goneStalledSince.IsZero() {
				t.Fatalf("goneStalledSince = %v, want the zero time: a later gone episode reads the stale "+
					"stamp as an already-spent budget and gives up on sight", f.goneStalledSince)
			}
		})
	}
}

// initFile CONSUMES the stored position: from the moment a file is discovered
// the tailer's own offset is authoritative. The scan-time prune covers a path
// a LISTING proved absent, but a dangling symlink is listed forever and is
// deliberately spared as "unproven" — so this is the one thing standing
// between a recreated path and its predecessor's offset. Applied a second
// time, that offset either skips the new file's first bytes as if they had
// shipped (the runtime reused the inode, and nothing catches it) or
// synthesizes an open-ended segment for an incarnation that no longer exists,
// counting a loss (obs.LogPrefixLost) that never happened. Which of the two
// depends only on whether the inode was reused, so neither may occur.
func TestDiscoveryConsumesTheStoredCheckpoint(t *testing.T) {
	dir := t.TempDir()
	targets := t.TempDir()
	ctx := context.Background()
	pos := mustOpenPositions(t, filepath.Join(t.TempDir(), "pos.json"))
	link := filepath.Join(dir, logName)

	// The first incarnation, fully shipped before this run started. The
	// /var/log/containers shape: a symlink the glob keeps listing even once
	// its target is gone.
	target := filepath.Join(targets, "0.log")
	// Fixed, equal-length timestamps: the stale offset must land exactly on a
	// line boundary of the REPLACEMENT for the skip to be unambiguous.
	writeLines(t, target, "2026-07-05T10:00:00Z stdout F old-one", "2026-07-05T10:00:01Z stdout F old-two")
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}
	st, err := os.Stat(link)
	if err != nil {
		t.Fatal(err)
	}
	if err := pos.SetLogs(map[string]positions.LogPos{link: {
		Offset: st.Size(), Inode: inodeOfPath(t, link),
	}}); err != nil {
		t.Fatal(err)
	}

	exp := &fakeExporter{}
	tl := driveTailer(dir, exp)
	tl.cfg.Positions = pos
	tl.scanDir(tl.loadCheckpoints(), true)
	tl.sweep(ctx, true)
	tl.flush(ctx)
	if got := exp.get(); len(got) != 0 {
		t.Fatalf("setup: a fully shipped file re-delivered %v", got)
	}

	// The target goes; the symlink stays, so the glob keeps listing the path
	// and the scan-time prune spares its (already consumed) entry.
	if err := os.Remove(target); err != nil {
		t.Fatal(err)
	}
	driveUntil(t, ctx, tl, func() bool { _, tracked := tl.files[link]; return !tracked },
		"the file whose path stopped resolving being released")

	// A NEW incarnation takes the path — same name, same line lengths, so the
	// predecessor's offset would land exactly past its first two lines.
	prefixBefore := obs.LogPrefixLost.Value()
	writeLines(t, target,
		"2026-07-05T11:00:00Z stdout F new-one",
		"2026-07-05T11:00:01Z stdout F new-two",
		"2026-07-05T11:00:02Z stdout F new-three")
	tl.scanDir(nil, false)
	driveUntil(t, ctx, tl, func() bool { return slices.Contains(exp.get(), "new-three") },
		"the recreated file being read")

	for _, want := range []string{"new-one", "new-two"} {
		if got := exp.get(); !slices.Contains(got, want) {
			t.Fatalf("the recreated file's line %q was skipped as if shipped: exported %v "+
				"(a consumed checkpoint entry was applied a second time)", want, got)
		}
	}
	if got := obs.LogPrefixLost.Value(); got != prefixBefore {
		t.Fatalf("LogPrefixLost = %v, want %v: a recreated path re-initialised from its predecessor's "+
			"identity synthesizes a rotated segment for an incarnation that never rotated", got, prefixBefore)
	}
}
