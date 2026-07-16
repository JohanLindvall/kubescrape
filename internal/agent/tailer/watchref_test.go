package tailer

import (
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/fsnotify/fsnotify"
)

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
