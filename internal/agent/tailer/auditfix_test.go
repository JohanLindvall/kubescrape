package tailer

// Regression tests for the audited discovery/rotation defects: the
// claim-and-skip ordering, CRI-name parsing, oversized-line accounting, the
// watcher-less findRotated fallback and the replay pass's byte budget.

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/JohanLindvall/kubescrape/internal/agent/positions"
	"github.com/JohanLindvall/kubescrape/internal/obs"
)

// A PROHIBITION claims the file so no later catch-all source can resurrect it —
// and that must hold for a STALE file too. claimPath's ignoreOlder test used to
// short-circuit ahead of both namespace prohibitions, so a file in an excluded
// namespace that had merely gone quiet past the cutoff fell through unclaimed
// and the next source (which has no namespace logic at all) tailed it: the
// observability feedback loop the exclude list exists to prevent, for the
// process lifetime (once tracked, tooOld never fires again).
func TestIgnoreOlderDoesNotBypassNamespaceProhibition(t *testing.T) {
	for _, tc := range []struct {
		name   string
		global []string
		source []string
	}{
		{name: "global", global: []string{"ns1"}},
		{name: "source", source: []string{"ns*"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			ctx := context.Background()
			exp := &fakeExporter{}
			tl := newSourceTailer(exp, []Source{
				{
					Name:              "containers",
					Include:           []string{filepath.Join(dir, "*.log")},
					Containerd:        true,
					ExcludeNamespaces: tc.source,
					IgnoreOlder:       "24h",
				},
				{Name: "catchall", Include: []string{filepath.Join(dir, "*.log")}},
			}, false)
			tl.cfg.ExcludeNamespaces = tc.global

			tl.scanDir(tl.loadCheckpoints(), true) // empty dir: what follows is NEW
			writeLog(t, dir, "2026-07-05T10:00:00Z stdout F feedback-loop")
			path := filepath.Join(dir, logName)
			old := time.Now().Add(-48 * time.Hour)
			if err := os.Chtimes(path, old, old); err != nil {
				t.Fatal(err)
			}
			tl.scanDir(nil, false)
			tl.sweep(ctx, true)
			tl.flush(ctx)

			if f, tracked := tl.files[path]; tracked {
				t.Fatalf("prohibited-but-stale file tracked by source %q", f.source.name)
			}
			if got := exp.get(); len(got) != 0 {
				t.Fatalf("excluded namespace exported via the catch-all source: %v", got)
			}
		})
	}
}

// A name without the <pod>_<namespace>_<container> structure is not a CRI name,
// and parseFileName must say so: claimPath claims-and-skips an unparseable name
// (its own comment promises exactly that), while an ok result with an empty
// namespace fell through and tracked the file with a bogus container ID —
// blocking on a metadata lookup that can never resolve, once a minute, forever.
func TestParseFileNameRejectsNonCRIStructure(t *testing.T) {
	for _, bad := range []string{
		"kube-proxy.log",             // no underscores at all
		"pod_ns-abc123.log",          // one underscore
		"pod_ns_container_x-abc.log", // three underscores
	} {
		if id, ns, ok := parseFileName(bad); ok {
			t.Errorf("parseFileName(%q) = (%q, %q, true), want !ok", bad, id, ns)
		}
	}
}

// The consequence at the call site: such a file is CLAIMED (so no later source
// picks it up either) and never tracked.
func TestNonCRIFileNameIsClaimedAndSkipped(t *testing.T) {
	dir := t.TempDir()
	tl := driveTailer(dir, &fakeExporter{})
	tl.scanDir(tl.loadCheckpoints(), true)

	path := filepath.Join(dir, "kube-proxy.log")
	writeLines(t, path, "2026-07-05T10:00:00Z stdout F hello")
	sets := scanSets{seen: map[string]struct{}{}, unproven: map[string]struct{}{}}
	if tl.claimPath(tl.sources[0], path, &sets) {
		t.Fatal("a file with no CRI name structure was tracked: it polls the metadata service forever with a bogus container ID")
	}
	if _, ok := sets.seen[path]; !ok {
		t.Fatal("an unparseable CRI name was not claimed: a later catch-all source resurrects it")
	}
	if _, tracked := tl.files[path]; tracked {
		t.Fatal("file tracked")
	}
}

// kubescrape_log_oversized_dropped_total counts LINES ("Unterminated lines
// discarded ..."), not the over-cap carry-buffer flushes that discard one line's
// successive slabs: consume runs per 64 KiB read chunk, so one long unterminated
// line bumped the counter lineLen/(MaxEntryBytes+4096) times.
func TestOversizedLineCountsOncePerLine(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	exp := &fakeExporter{}
	tl := driveTailer(dir, exp)
	tl.cfg.MaxEntryBytes = 1024

	tl.scanDir(tl.loadCheckpoints(), true)
	path := filepath.Join(dir, logName)
	// One unterminated line spanning several 64 KiB reads, each of which
	// crosses MaxEntryBytes+oversizeSlack on its own.
	if err := os.WriteFile(path, []byte(strings.Repeat("y", 200_000)), 0o644); err != nil {
		t.Fatal(err)
	}
	tl.scanDir(nil, false)

	before := obs.LogOversizedDropped.Value()
	tl.sweep(ctx, true)
	tl.flush(ctx)
	if got := obs.LogOversizedDropped.Value() - before; got != 1 {
		t.Fatalf("one oversized line counted %v times: the counter tracks over-cap carry flushes, not lines", got)
	}
	if got := exp.get(); len(got) != 0 {
		t.Fatalf("discarded blob exported: %v", got)
	}
}

// findRotated resolves a segment's rotated file by name in the log's TARGET
// directory, and by the time it runs the live path is often gone (a container
// GC'd after a CrashLoop restart removes the /var/log/containers symlink while
// its rotated files remain). The cached targetDir is the only route then — and
// it was only ever set when an fsnotify watcher existed, so with -logs-watch
// =false (or a failed watcher init) still-on-disk segments were counted lost
// and retired.
func TestFindRotatedWithoutWatcherResolvesTargetDir(t *testing.T) {
	linkDir := t.TempDir()
	targets := t.TempDir()
	ctx := context.Background()

	exp := &fakeExporter{}
	tl := driveTailer(linkDir, exp) // no watcher: tl.watcher stays nil
	tl.scanDir(tl.loadCheckpoints(), true)

	target := filepath.Join(targets, "0.log")
	writeLines(t, target, timeNowCRI()+" stdout F hello")
	link := filepath.Join(linkDir, logName)
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}
	tl.scanDir(nil, false)
	driveUntil(t, ctx, tl, func() bool { f := tl.files[link]; return f != nil && f.f != nil },
		"the symlinked log being opened")
	f := tl.files[link]

	// The container is GC'd: its log rotates away and the symlink goes, so
	// EvalSymlinks of the live path can no longer name the directory.
	rotated := target + ".1"
	if err := os.Rename(target, rotated); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(link); err != nil {
		t.Fatal(err)
	}
	got, ok := tl.findRotated(f, &segment{inode: inodeOfPath(t, rotated)})
	if !ok {
		t.Fatal("a rotated segment still on disk was declared unrecoverable: no watcher, so no target dir was ever cached")
	}
	if filepath.Base(got) != filepath.Base(rotated) {
		t.Fatalf("findRotated = %q, want %q", got, rotated)
	}
}

// replaySegment's per-sweep byte budget must bound the pass. The `overrun`
// escape exists so a line that straddles the budget still completes, but it was
// re-derived from `len(carry) > 0` — true after every read whose 64 KiB boundary
// did not happen to land on a newline — so once the budget ran out mid-line the
// pass read the WHOLE owed range (a 10 MiB rotated kubelet log) in one go, on
// the single sweep goroutine that serves every file on the node.
func TestReplayBudgetIsNotDefeatedByTheOverrunEscape(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	pos := mustOpenPositions(t, filepath.Join(t.TempDir(), "pos.json"))
	path := filepath.Join(dir, logName)

	// ~300 KiB of equal-length lines in the rotated-away inode.
	const nLines = 3000
	base := timeNowCRI() + " stdout F "
	lines := make([]string, 0, nLines)
	for i := range nLines {
		lines = append(lines, base+fmt.Sprintf("seg-%060d", i))
	}
	lineLen := len(lines[0]) + 1
	rot := path + ".1"
	writeLines(t, rot, lines...)
	rst, err := os.Stat(rot)
	if err != nil {
		t.Fatal(err)
	}
	writeLog(t, dir, timeNowCRI()+" stdout F tail-one")
	if err := pos.SetLogs(map[string]positions.LogPos{path: {
		Offset: 0, Inode: inodeOfPath(t, path),
		Pending: []positions.Prefix{{Inode: inodeOfPath(t, rot), From: 0, To: rst.Size()}},
	}}); err != nil {
		t.Fatal(err)
	}

	exp := &fakeExporter{}
	tl := driveTailer(dir, exp)
	tl.cfg.Positions = pos
	// A budget landing MID-LINE — the shape that arms the escape.
	tl.cfg.MaxBytesPerSweep = lineLen + lineLen/2
	tl.scanDir(tl.loadCheckpoints(), true)
	f := tl.files[path]
	if f == nil || len(f.segments) != 1 {
		t.Fatal("setup: the checkpointed segment was not restored")
	}
	sg := f.segments[0]

	tl.sweep(ctx, true)

	if sg.fedTo == 0 {
		t.Fatal("the budget-cut pass made no progress at all")
	}
	// One read past the budget is the escape doing its job (finish the open
	// line); the whole file is the bound not being honoured.
	if max := int64(tl.cfg.MaxBytesPerSweep + 64*1024 + lineLen); sg.fedTo > max {
		t.Fatalf("one replay pass fed %d bytes against a %d-byte budget (bound %d): the overrun escape read the whole owed range",
			sg.fedTo, tl.cfg.MaxBytesPerSweep, max)
	}
	if sg.fedTo >= rst.Size() {
		t.Fatalf("the pass read the segment to EOF (%d bytes) in one sweep", sg.fedTo)
	}
}
