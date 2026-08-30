package tailer

// The positions save is the tailer's one unbounded-cost operation on the SINGLE
// sweep goroutine that serves every log file on the node: it marshals the whole
// document and fsyncs twice (the file, then the directory), which measures
// ~11 ms at 100 tracked files and ~15 ms at 3000 on ext4/nvme — of which ~11 ms
// is the two fsyncs. scanDir calls it once per fsnotify event, so a rolling
// update was one whole-node log-shipping stall per new container log file.
//
// discoverySaveWindow coalesces THAT burst and nothing else. These tests pin
// both halves of the bargain: the ordinary discovery is still immediate, the
// coalesced one is still bounded, and — the invariant that outranks all of it —
// the ROTATION saves are untouched, because a missing hop is the one case a
// restart cannot reconstruct.

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/JohanLindvall/kubescrape/internal/agent/positions"
	"github.com/JohanLindvall/kubescrape/internal/obs"
)

// storedPending reads the positions file from scratch — never through the
// in-process store, whose in-memory doc says nothing about what a crash would
// leave behind.
func storedPending(t *testing.T, posPath, logPath string) int {
	t.Helper()
	s, err := positions.Open(posPath)
	if err != nil {
		t.Fatal(err)
	}
	return len(s.Logs()[logPath].Pending)
}

func storedHas(t *testing.T, posPath, logPath string) bool {
	t.Helper()
	s, err := positions.Open(posPath)
	if err != nil {
		t.Fatal(err)
	}
	_, ok := s.Logs()[logPath]
	return ok
}

// TestARotationHopIsPersistedInsideTheDiscoveryCoalescingWindow is the crash-
// window test. Coalescing the discovery saves creates a window in which a save
// "just happened", and the tempting next step is to route the rotation saves
// through the same gate. That must never happen: initFile can reconstruct ONE
// unsaved hop from a stale checkpoint (the path names a different incarnation
// than the stored identity, so it synthesizes an open-ended segment), but
// nothing on disk names the INTERMEDIATE inode of a second hop, so a crash
// loses that incarnation's tail outright. The discovery window is measured in
// milliseconds and a rotation is minutes of log volume apart, but the property
// must be enforced rather than argued.
func TestARotationHopIsPersistedInsideTheDiscoveryCoalescingWindow(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	path := filepath.Join(dir, logName)
	posPath := filepath.Join(t.TempDir(), "pos.json")

	tl := driveTailer(dir, &fakeExporter{})
	tl.cfg.Positions = mustOpenPositions(t, posPath)
	tl.scanDir(tl.loadCheckpoints(), true) // empty dir: what follows is NEW
	writeLog(t, dir, timeNowCRI()+" stdout F one")
	tl.scanDir(nil, false) // a discovery save: we are now INSIDE the window
	tl.sweep(ctx, true)
	f, ok := tl.files[path]
	if !ok {
		t.Fatal("setup: file not discovered")
	}
	if time.Since(tl.lastCheckpoint) >= discoverySaveWindow {
		t.Fatalf("setup: the discovery save is already %v old, so this test is not exercising the window",
			time.Since(tl.lastCheckpoint))
	}

	// First hop: recoverable from the stale checkpoint, so it need not be
	// written synchronously (the sweep's closing save covers it).
	rotateAway(t, dir, 1)
	writeLog(t, dir, timeNowCRI()+" stdout F two")
	tl.reopen(ctx, f, true, true)
	tl.sweep(ctx, true)
	saved := storedPending(t, posPath, path)
	if saved == 0 {
		t.Fatal("setup: the sweep did not persist the first hop")
	}

	// Second hop of the SAME file with no sweep between them, and with the
	// discovery save still fresh. This one has no route back and must be on
	// disk the moment it is recorded — no housekeeping, no window, no sweep.
	tl.reopen(ctx, f, true, true)
	tl.ingestChunk(ctx, f, []byte(timeNowCRI()+" stdout F three\n"), true)
	tl.reopen(ctx, f, true, true)
	if time.Since(tl.lastCheckpoint) >= discoverySaveWindow {
		t.Fatal("the hops took longer than the coalescing window; the test proves nothing")
	}
	if got := storedPending(t, posPath, path); got <= saved {
		t.Fatalf("two unsaved hops of one file left only %d pending on disk (was %d) while a discovery "+
			"save was fresh: the rotation saves have been folded into the discovery coalescing window, "+
			"and the intermediate inode a crash cannot reconstruct is gone", got, saved)
	}
}

// TestAnOrdinaryDiscoveryIsStillPersistedImmediately: the window must not cost
// anything in the case that is not a burst. A discovery whose previous save is
// older than the window writes synchronously, exactly as it always did.
func TestAnOrdinaryDiscoveryIsStillPersistedImmediately(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, logName)
	posPath := filepath.Join(t.TempDir(), "pos.json")

	tl := driveTailer(dir, &fakeExporter{})
	tl.cfg.Positions = mustOpenPositions(t, posPath)
	tl.scanDir(tl.loadCheckpoints(), true)
	// lastCheckpoint is the zero time after an empty startup scan, so the very
	// first discovery is unambiguously outside the window; stamp it anyway, so
	// this test says what it means rather than relying on that.
	tl.lastCheckpoint = time.Now().Add(-discoverySaveWindow)

	writeLog(t, dir, timeNowCRI()+" stdout F one")
	tl.scanDir(nil, false)
	if !storedHas(t, posPath, path) {
		t.Fatal("a discovery outside the coalescing window was not persisted synchronously: every " +
			"ordinary discovery must still reach disk before the next thing can happen to the file")
	}
	if tl.discoveryUnsaved {
		t.Fatal("the discovery is still marked unsaved after a synchronous save")
	}
}

// TestADiscoveryBurstCoalescesAndIsFlushedByHousekeeping: inside the window the
// save is deferred (that is the whole point), and housekeeping — which runs
// after every sweep, and a discovery always schedules one — is what bounds the
// deferral. The exposure is the window plus one housekeeping pass, never the
// 10-second cadence.
func TestADiscoveryBurstCoalescesAndIsFlushedByHousekeeping(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	posPath := filepath.Join(t.TempDir(), "pos.json")
	second := filepath.Join(dir, "pod2_ns1_app-fedcba9876543210.log")

	tl := driveTailer(dir, &fakeExporter{})
	tl.cfg.Positions = mustOpenPositions(t, posPath)
	tl.scanDir(tl.loadCheckpoints(), true)
	writeLog(t, dir, timeNowCRI()+" stdout F one")
	tl.scanDir(nil, false) // first discovery: immediate, stamps lastCheckpoint
	if !storedHas(t, posPath, filepath.Join(dir, logName)) {
		t.Fatal("setup: the first discovery was not persisted")
	}

	// A second file appears while the first save is still fresh — the burst.
	writeLines(t, second, timeNowCRI()+" stdout F two")
	tl.scanDir(nil, false)
	if _, ok := tl.files[second]; !ok {
		t.Fatal("setup: the second file was not discovered")
	}
	if storedHas(t, posPath, second) {
		t.Fatal("a discovery arriving inside the coalescing window was written anyway: a rolling " +
			"update pays one whole-node stall per new container log file")
	}
	if !tl.discoveryUnsaved {
		t.Fatal("a deferred discovery did not arm the flush flag, so nothing will ever write it")
	}

	// Housekeeping before the window has elapsed must not write either: the
	// coalescing is what caps the I/O while the churn lasts.
	tl.housekeeping(ctx)
	if storedHas(t, posPath, second) {
		t.Fatal("housekeeping flushed a deferred discovery before its window elapsed")
	}

	// Once the window has elapsed, the next housekeeping writes it. Rewinding
	// the stamp is the same trick the store's own tests use for time: the
	// alternative is a real 250 ms sleep in a test that proves nothing more.
	tl.lastCheckpoint = time.Now().Add(-discoverySaveWindow)
	tl.housekeeping(ctx)
	if !storedHas(t, posPath, second) {
		t.Fatalf("a coalesced discovery was never flushed: it would wait for the 10-second cadence, "+
			"which is what the immediate save exists to avoid (files on disk: %v)", storedPaths(t, posPath))
	}
	if tl.discoveryUnsaved {
		t.Fatal("the flush flag survived a successful save, so every later housekeeping pass re-saves")
	}
}

func storedPaths(t *testing.T, posPath string) []string {
	t.Helper()
	s, err := positions.Open(posPath)
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for p := range s.Logs() {
		out = append(out, filepath.Base(p))
	}
	return out
}

// TestAFailingStoreDoesNotRetryTheDeferredDiscoveryEveryWindow. A deferred
// discovery stays armed until a save succeeds, so under a persisting failure —
// a read-only mount, a full disk, the two ways this actually fails — a
// window-driven retry would be four failed saves a second from every node,
// inflating kubescrape_positions_save_errors_total far past the rate of the
// thing it measures. It falls back to the 10-second cadence, which is what a
// failed discovery save did before the window existed.
func TestAFailingStoreDoesNotRetryTheDeferredDiscoveryEveryWindow(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("running as root: permissions cannot make a write fail")
	}
	dir := t.TempDir()
	posDir := t.TempDir()
	ctx := context.Background()

	tl := driveTailer(dir, &fakeExporter{})
	tl.cfg.Positions = mustOpenPositions(t, filepath.Join(posDir, "pos.json"))
	tl.scanDir(tl.loadCheckpoints(), true)
	writeLog(t, dir, timeNowCRI()+" stdout F one")

	if err := os.Chmod(posDir, 0o500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(posDir, 0o755) })
	tl.scanDir(nil, false) // discovers, saves, fails
	if !tl.positionsFailing {
		t.Fatal("setup: the save into a read-only directory did not fail")
	}
	if !tl.discoveryUnsaved {
		t.Fatal("a failed save cleared the discovery flag, so the entry is lost until the next discovery")
	}

	before := obs.PositionsSaveErrors.Value()
	tl.lastCheckpoint = time.Now().Add(-discoverySaveWindow)
	for range 5 {
		tl.housekeeping(ctx)
	}
	if got := obs.PositionsSaveErrors.Value() - before; got != 0 {
		t.Fatalf("%v further save attempts inside the window while the store was failing: a persisting "+
			"read-only mount becomes a syscall storm on the sweep goroutine and a counter rate two "+
			"orders of magnitude above the condition", got)
	}

	// Recovery: the 10-second cadence writes it, and the flag clears with it.
	if err := os.Chmod(posDir, 0o755); err != nil {
		t.Fatal(err)
	}
	tl.saveCheckpoints()
	if tl.discoveryUnsaved || tl.positionsFailing {
		t.Fatal("the successful save did not clear the deferred discovery")
	}
	if !storedHas(t, filepath.Join(posDir, "pos.json"), filepath.Join(dir, logName)) {
		t.Fatal("the deferred discovery never reached disk after the store recovered")
	}
}

// TestShutdownAlwaysSavesADeferredDiscovery: the coalescing must not survive
// the process. Run's ctx.Done branch saves unconditionally, so a deferral can
// never turn a graceful stop into a lost entry.
func TestShutdownAlwaysSavesADeferredDiscovery(t *testing.T) {
	dir := t.TempDir()
	posPath := filepath.Join(t.TempDir(), "pos.json")
	path := filepath.Join(dir, logName)

	tl := driveTailer(dir, &fakeExporter{})
	tl.cfg.Positions = mustOpenPositions(t, posPath)
	tl.scanDir(tl.loadCheckpoints(), true)
	writeLog(t, dir, timeNowCRI()+" stdout F one")

	// Force the deferral: pretend a save happened this instant.
	tl.lastCheckpoint = time.Now()
	tl.scanDir(nil, false)
	if storedHas(t, posPath, path) {
		t.Fatal("setup: the discovery was not deferred")
	}
	// The unconditional save the shutdown branch performs.
	tl.saveCheckpoints()
	if !storedHas(t, posPath, path) {
		t.Fatal("the final save skipped a deferred discovery; a graceful stop must persist everything " +
			"the coalescing window is still holding")
	}
	_ = os.Remove(posPath)
}
