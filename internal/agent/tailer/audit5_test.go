package tailer

// Regression guards from the segment-model deep audit: segment retirement
// with trailing non-entry bytes, older-segment stranding across a mid-drain
// rewind + rotation, and the rotation-drain livelock under a collector
// outage.

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/JohanLindvall/kubescrape/internal/obs"
)

// A rotated-away file whose tail is a torn (unterminated) fragment: the
// fragment can never produce a committing entry, so the segment's owed range
// must END at the last fed line boundary — a `to` covering the fragment
// pinned the segment below retirement forever (fd + checkpoint entry + gone
// file leaked, one per rotation).
func TestSegmentWithTornTailRetires(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	exp := &fakeExporter{}
	tl := driveTailer(dir, exp)

	tl.scanDir(tl.loadCheckpoints(), true)
	path := filepath.Join(dir, logName)
	if err := os.WriteFile(path, []byte("2026-07-05T10:00:00Z stdout F whole\n2026-07-05T10:00:01Z stdout F torn-fragm"), 0o644); err != nil {
		t.Fatal(err)
	}
	tl.scanDir(nil, false)
	tl.sweep(ctx, true) // reads whole + the fragment (pending)

	rotateAway(t, dir, 1)
	writeLog(t, dir, "2026-07-05T10:00:02Z stdout F next")
	f := tl.files[path]
	driveUntil(t, ctx, tl, func() bool {
		return len(f.segments) == 0 && slices.Contains(exp.get(), "next")
	}, "torn-tail segment retires once its fed lines commit")
}

// Same wedge from an entirely ordinary file shape: a trailing blank line
// (consumed but never fed — no entry can ever cover it).
func TestSegmentWithTrailingBlankLineRetires(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	exp := &fakeExporter{}
	tl := newSourceTailer(exp, []Source{{
		Name:    "plain",
		Include: []string{filepath.Join(dir, "*.log")},
	}}, false)

	tl.scanDir(tl.loadCheckpoints(), true)
	path := filepath.Join(dir, "app.log")
	writeLines(t, path, "line-1", "") // trailing blank line
	tl.scanDir(nil, false)
	tl.sweep(ctx, true)

	if err := os.Rename(path, path+".1"); err != nil {
		t.Fatal(err)
	}
	writeLines(t, path, "line-2")
	f := tl.files[path]
	driveUntil(t, ctx, tl, func() bool {
		return len(f.segments) == 0 && slices.Contains(exp.get(), "line-2")
	}, "blank-tail segment retires once its fed lines commit")
}

// The stranded-segment interleaving: collector outage, rotation records
// segment A, a later flush failure purges A's re-fed lines, then a SECOND
// rotation lands. The rotation path must not overclaim segmentsFed — A's
// lines must still export once the collector recovers, in-process (no
// restart).
func TestOlderSegmentSurvivesMidOutageDoubleRotation(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	exp := &fakeExporter{}
	tl := driveTailer(dir, exp)

	tl.scanDir(tl.loadCheckpoints(), true)
	writeLog(t, dir, "2026-07-05T10:00:00Z stdout F a-1")
	tl.scanDir(nil, false)
	exp.mu.Lock()
	exp.fail = 1 << 30 // outage
	exp.mu.Unlock()
	tl.sweep(ctx, true)
	tl.flush(ctx) // fails; rewind

	rotateAway(t, dir, 1) // rotation 1: segment A recorded
	writeLog(t, dir, "2026-07-05T10:00:01Z stdout F b-1")
	tl.sweep(ctx, true)
	tl.flush(ctx) // fails again; rewind purges A's re-fed lines

	tl.sweep(ctx, true)   // re-feeds A, reads b-1
	rotateAway(t, dir, 2) // rotation 2 during the outage
	writeLog(t, dir, "2026-07-05T10:00:02Z stdout F c-1")
	tl.sweep(ctx, true)
	tl.flush(ctx) // still failing

	exp.mu.Lock()
	exp.fail = 0 // collector recovers
	exp.mu.Unlock()
	driveUntil(t, ctx, tl, func() bool {
		got := exp.get()
		return slices.Contains(got, "a-1") && slices.Contains(got, "b-1") && slices.Contains(got, "c-1")
	}, "all three rotations' lines delivered in-process after recovery")
}

// A rotation whose drain trips the batch size mid-way while the collector is
// DOWN: the failed flush rewinds the very fd being drained. The drain must
// abort and retry next sweep — re-reading in a hot loop burned thousands of
// export attempts inside ONE sweep call, monopolizing the node-wide goroutine.
func TestRotationDrainAbortsOnMidDrainFailure(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	exp := &fakeExporter{}
	tl := driveTailer(dir, exp)
	tl.cfg.BatchSize = 2         // the drain flushes mid-way
	tl.cfg.MaxBytesPerSweep = 40 // ~one line per read loop: the drain owns the backlog

	tl.scanDir(tl.loadCheckpoints(), true)
	writeLog(t, dir,
		"2026-07-05T10:00:00Z stdout F a-1",
		"2026-07-05T10:00:01Z stdout F a-2",
		"2026-07-05T10:00:02Z stdout F a-3",
		"2026-07-05T10:00:03Z stdout F a-4",
	)
	tl.scanDir(nil, false)
	exp.mu.Lock()
	exp.fail = 1 << 30
	exp.mu.Unlock()

	tl.sweep(ctx, true) // budget-reads one line; the rest stays unread
	rotateAway(t, dir, 1)
	writeLog(t, dir, "2026-07-05T10:00:04Z stdout F b-1")

	before := func() int {
		exp.mu.Lock()
		defer exp.mu.Unlock()
		return exp.attempts
	}()
	tl.sweep(ctx, true) // rotation detected; the drain reads a-2..a-4, trips
	// BatchSize, the flush fails and rewinds the drained fd: it must ABORT.
	after := func() int {
		exp.mu.Lock()
		defer exp.mu.Unlock()
		return exp.attempts
	}()
	// One aborted drain = at most a couple of flushes (3 retry attempts
	// each); the livelock burned hundreds on this shape.
	if attempts := after - before; attempts > 12 {
		t.Fatalf("one sweep burned %d export attempts (drain livelock)", attempts)
	}

	exp.mu.Lock()
	exp.fail = 0
	exp.mu.Unlock()
	driveUntil(t, ctx, tl, func() bool {
		got := exp.get()
		return slices.Contains(got, "a-1") && slices.Contains(got, "a-4") && slices.Contains(got, "b-1")
	}, "rotation completes and delivers everything after recovery")
}

// A rotation that happened while the agent was DOWN: the checkpoint names an
// identity the path no longer has, and the rotated file still holds the old
// tail's remainder. Restart must recover it via a synthesized open-ended
// segment (previously it was lost silently and uncounted).
func TestRotationWhileDownRecoversRemainder(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	ckpt := filepath.Join(dir, "positions.json")

	exp := &fakeExporter{}
	tl := newTestTailer(dir, ckpt, exp)
	tl.scanDir(tl.loadCheckpoints(), true)
	writeLog(t, dir, "2026-07-05T10:00:00Z stdout F shipped")
	tl.scanDir(nil, false)
	tl.sweep(ctx, true)
	tl.flush(ctx)
	tl.saveCheckpoints()
	if got := exp.get(); !slices.Contains(got, "shipped") {
		t.Fatalf("precondition: %v", got)
	}

	// Agent goes DOWN. One more line lands, then the runtime rotates.
	writeLog(t, dir, "2026-07-05T10:00:01Z stdout F written-while-down")
	rotateAway(t, dir, 1)
	writeLog(t, dir, "2026-07-05T10:00:02Z stdout F after-restart")

	// Restart on the same checkpoint.
	exp2 := &fakeExporter{}
	tl2 := newTestTailer(dir, ckpt, exp2)
	tl2.scanDir(tl2.loadCheckpoints(), true)
	driveUntil(t, ctx, tl2, func() bool {
		got := exp2.get()
		return slices.Contains(got, "written-while-down") && slices.Contains(got, "after-restart")
	}, "while-down remainder recovered from the rotated file")
	// And the synthetic segment retires (open-ended `to` pinned at EOF).
	f := tl2.files[filepath.Join(dir, logName)]
	driveUntil(t, ctx, tl2, func() bool { return len(f.segments) == 0 },
		"synthetic segment retires after recovery")
	// No duplicate of the already-shipped prefix.
	for _, r := range exp2.get() {
		if r == "shipped" {
			t.Fatalf("committed prefix re-shipped: %v", exp2.get())
		}
	}
}

// The same while-down rotation when the rotated file was PRUNED before
// restart: the loss must be counted and the synthetic segment retired — not
// left wedging retirement forever.
func TestRotationWhileDownPrunedCountsAndRetires(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	ckpt := filepath.Join(dir, "positions.json")

	exp := &fakeExporter{}
	tl := newTestTailer(dir, ckpt, exp)
	tl.scanDir(tl.loadCheckpoints(), true)
	writeLog(t, dir, "2026-07-05T10:00:00Z stdout F shipped")
	tl.scanDir(nil, false)
	tl.sweep(ctx, true)
	tl.flush(ctx)
	tl.saveCheckpoints()

	writeLog(t, dir, "2026-07-05T10:00:01Z stdout F written-while-down")
	rotateAway(t, dir, 1)
	if err := os.Remove(filepath.Join(dir, logName+".1")); err != nil {
		t.Fatal(err)
	}
	writeLog(t, dir, "2026-07-05T10:00:02Z stdout F after-restart")

	lostBefore := obs.LogPrefixLost.Value()
	exp2 := &fakeExporter{}
	tl2 := newTestTailer(dir, ckpt, exp2)
	tl2.scanDir(tl2.loadCheckpoints(), true)
	f := tl2.files[filepath.Join(dir, logName)]
	driveUntil(t, ctx, tl2, func() bool {
		return slices.Contains(exp2.get(), "after-restart") && len(f.segments) == 0
	}, "pruned while-down segment counted and retired")
	if got := obs.LogPrefixLost.Value(); got != lostBefore+1 {
		t.Fatalf("LogPrefixLost = %v, want %v", got, lostBefore+1)
	}
}
