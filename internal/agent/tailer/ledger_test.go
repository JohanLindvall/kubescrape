// Tests for file identity (ledger.go): fingerprints and checkpoint
// identity guards.
package tailer

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"
)

func TestFingerprintGuardsCheckpointResume(t *testing.T) {
	dir := t.TempDir()
	cpDir := t.TempDir()
	cp := filepath.Join(cpDir, "checkpoints.json")
	path := filepath.Join(dir, logName)

	// First run: establish a checkpoint past line one.
	writeLog(t, dir, "2026-07-05T09:00:00Z stdout F old-one", "2026-07-05T09:00:01Z stdout F old-two")
	exp1 := &fakeExporter{}
	tl1 := newTestTailer(dir, cp, exp1)
	tl1.cfg.FingerprintBytes = 64
	stop1 := startTailer(t, tl1)
	// Pre-existing file: skipped to end; append so something commits.
	writeLog(t, dir, "2026-07-05T09:00:02Z stdout F old-three")
	waitFor(t, func() bool { return len(exp1.get()) == 1 }, "first run record")
	stop1()

	// Replace the file with different content of GREATER length than the
	// committed offset (the inode may or may not be reused; the fingerprint
	// must catch the replacement either way).
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	writeLog(t, dir,
		"2026-07-06T09:00:00Z stdout F new-one",
		"2026-07-06T09:00:01Z stdout F new-two",
		"2026-07-06T09:00:02Z stdout F new-three",
		"2026-07-06T09:00:03Z stdout F new-four",
	)

	exp2 := &fakeExporter{}
	tl2 := newTestTailer(dir, cp, exp2)
	tl2.cfg.FingerprintBytes = 64
	stop2 := startTailer(t, tl2)
	defer stop2()
	// A different file must be read from the top, not from the stale offset.
	waitFor(t, func() bool { return len(exp2.get()) == 4 }, "replacement read from zero")
	if got := exp2.get(); got[0] != "new-one" {
		t.Fatalf("records = %v", got)
	}
}

func TestFingerprintAllowsResume(t *testing.T) {
	dir := t.TempDir()
	cp := filepath.Join(t.TempDir(), "checkpoints.json")

	exp1 := &fakeExporter{}
	tl1 := newTestTailer(dir, cp, exp1)
	tl1.cfg.FingerprintBytes = 64
	stop1 := startTailer(t, tl1)
	writeLog(t, dir, "2026-07-05T10:00:00Z stdout F one", "2026-07-05T10:00:01Z stdout F two")
	waitFor(t, func() bool { return len(exp1.get()) == 2 }, "first run records")
	stop1()

	writeLog(t, dir, "2026-07-05T10:00:02Z stdout F three")
	exp2 := &fakeExporter{}
	tl2 := newTestTailer(dir, cp, exp2)
	tl2.cfg.FingerprintBytes = 64
	stop2 := startTailer(t, tl2)
	defer stop2()
	waitFor(t, func() bool { return len(exp2.get()) == 1 }, "resumed record")
	if got := exp2.get(); got[0] != "three" {
		t.Fatalf("records = %v (history re-ingested?)", got)
	}
}

// TestCheckpointFingerprintKeepsCopyTruncateGuard: the new PRE-read guard
// in readFile trusts f.fp — but saveCheckpoints() RE-COMPUTES f.fp from the
// file's current head on every checkpoint while fp.Len < FingerprintBytes
// (1 KiB by default), i.e. for every log file smaller than 1 KiB: every quiet
// container, and every file during its first seconds.
//
// A copytruncate that lands between a sweep's read and the next checkpoint
// therefore rewrites the fingerprint to the NEW content's head before readFile
// ever compares it. The guard then matches, no rotation is detected, and the
// sweep reads from the stale offset into the middle of the new file — the exact
// loss e39542d set out to fix, still open for small files.
//
// (housekeeping calls saveCheckpoints on its own ticker, independent of the
// sweep; committing a batch persists too.)
func TestCheckpointFingerprintKeepsCopyTruncateGuard(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	exp := &fakeExporter{}
	tl := newTestTailer(dir, filepath.Join(t.TempDir(), "cp.json"), exp)
	path := filepath.Join(dir, logName)

	tl.scanDir(tl.loadCheckpoints(), true)
	writeLog(t, dir,
		"2026-07-05T10:00:00Z stdout F old-1",
		"2026-07-05T10:00:01Z stdout F old-2",
		"2026-07-05T10:00:02Z stdout F old-3")
	tl.scanDir(nil, false)
	tl.sweep(ctx, true)
	tl.flush(ctx) // committed == readPos, well under the 1 KiB fingerprint

	// Copytruncate: same inode, truncated, refilled PAST the read offset.
	time.Sleep(20 * time.Millisecond)
	fh, err := os.OpenFile(path, os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= 10; i++ {
		if _, err := fmt.Fprintf(fh, "2026-07-05T10:01:%02dZ stdout F new-%02d\n", i, i); err != nil {
			t.Fatal(err)
		}
	}
	_ = fh.Close()

	// A checkpoint lands before the next sweep (housekeeping's own ticker).
	tl.saveCheckpoints()

	for i := 0; i < 3; i++ {
		tl.sweep(ctx, true)
		tl.flush(ctx)
	}

	got := exp.get()
	t.Logf("EXPORTED: %v", got)
	for _, want := range []string{"new-01", "new-02", "new-03"} {
		if !slices.Contains(got, want) {
			t.Errorf("copytruncate prefix line %q lost: saveCheckpoints re-fingerprinted the file "+
				"(fp.Len < FingerprintBytes) and blinded readFile's pre-read guard (got %v)", want, got)
		}
	}
}

// Without a checkpoint store nothing used to extend fingerprints: a file first
// opened at size 0 kept the matches-anything empty fingerprint forever,
// blinding every fp-based rotation guard. The read path must extend it.
func TestFingerprintExtendsWithoutCheckpointStore(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	exp := &fakeExporter{}
	tl := driveTailer(dir, exp) // no positions store

	tl.scanDir(tl.loadCheckpoints(), true)
	// Discovered EMPTY: fp.Len == 0.
	path := filepath.Join(dir, logName)
	if err := os.WriteFile(path, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	tl.scanDir(nil, false)
	tl.sweep(ctx, true)

	line1 := "2026-07-05T10:00:00Z stdout F aaaa\n"
	if err := os.WriteFile(path, []byte(line1), 0o644); err != nil {
		t.Fatal(err)
	}
	tl.sweep(ctx, true)
	tl.flush(ctx)
	if got := exp.get(); !slices.Contains(got, "aaaa") {
		t.Fatalf("first line missing: %v", got)
	}
	f := tl.files[path]
	if f.fp.Len == 0 {
		t.Fatal("fingerprint never extended without a checkpoint store")
	}

	// Same-size copytruncate: identical length, different content, mtime moved.
	line2 := "2026-07-05T10:00:09Z stdout F bbbb\n"
	if len(line2) != len(line1) {
		t.Fatal("test geometry: lines must be same length")
	}
	if err := os.WriteFile(path, []byte(line2), 0o644); err != nil {
		t.Fatal(err)
	}
	future := time.Now().Add(2 * time.Second)
	if err := os.Chtimes(path, future, future); err != nil {
		t.Fatal(err)
	}
	driveUntil(t, ctx, tl, func() bool { return slices.Contains(exp.get(), "bbbb") },
		"same-size rewrite detected (fingerprint not blind)")
}

// FingerprintBytes < 0 disables content fingerprints: identity is the inode
// alone. Resume must still work, and a rename rotation must still be detected.
func TestNegativeFingerprintBytesInodeOnlyIdentity(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	exp := &fakeExporter{}
	ckpt := filepath.Join(dir, "ckpt.json")
	tl := driveTailer(dir, exp)
	tl.cfg.Positions = mustOpenPositions(t, ckpt)
	tl.cfg.FingerprintBytes = -1

	tl.scanDir(tl.loadCheckpoints(), true)
	writeLog(t, dir, "2026-07-05T10:00:00Z stdout F one")
	tl.scanDir(nil, false)
	tl.sweep(ctx, true)
	tl.flush(ctx)
	tl.saveCheckpoints()

	// Restart on the same inode: resume, not re-read.
	exp2 := &fakeExporter{}
	tl2 := driveTailer(dir, exp2)
	tl2.cfg.Positions = mustOpenPositions(t, ckpt)
	tl2.cfg.FingerprintBytes = -1
	tl2.scanDir(tl2.loadCheckpoints(), true)
	writeLog(t, dir, "2026-07-05T10:00:01Z stdout F two")
	tl2.scanDir(nil, false)
	tl2.sweep(ctx, true)
	tl2.flush(ctx)
	if got := exp2.get(); !slices.Equal(got, []string{"two"}) {
		t.Fatalf("inode-only resume exports = %v, want [two]", got)
	}

	// Rename rotation: new inode at the path is detected and read from zero.
	rotateAway(t, dir, 1)
	writeLog(t, dir, "2026-07-05T10:00:02Z stdout F three")
	for i := 0; i < 3 && !slices.Contains(exp2.get(), "three"); i++ {
		tl2.sweep(ctx, true)
		tl2.flush(ctx)
	}
	if got := exp2.get(); !slices.Contains(got, "three") {
		t.Fatalf("rotation missed under inode-only identity: %v", got)
	}
}

// pos.less orders SEGMENT-first, and that is what makes watermark() the lowest
// position in STREAM order rather than the smallest raw offset. Comparing
// offsets alone lets a NEWER segment's low-offset buffered item win (a fresh
// inode starts at 0), so the OLDER segment's commit candidate matches neither
// clamp arm in flush() — it commits its whole owed range and the segment is
// RETIRED with its lines still buffered, closing the only fd to a possibly
// unlinked inode and dropping its checkpoint entry.
//
// The shape, all through the real read/rotate/flush path, with stack-trace
// joining off so the only buffered state is a stage-1 CRI fragment run:
//
//	segment 1 (old inode)   stderr F alpha    [0, a)   -> entry in the batch
//	                        stdout P frag     [a, b)   -> stdout run OPEN at a
//	                        stderr F beta     [b, c)   -> entry in the batch
//	rename rotation: nothing left to drain and stdout's run is buffered, so the
//	pipeline is CARRIED and the old inode is recorded as segment 1 {0, c}
//	segment 2 (new inode)   stderr P frag2    [0, x)   -> stderr run OPEN at 0
//
// The watermark must be {seg 1, off a} — the stdout run buffered in the OLD
// segment precedes the stderr run at offset 0 of the new one.
func TestWatermarkOrdersBySegmentBeforeOffset(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	exp := &fakeExporter{}
	tl := driveTailer(dir, exp)
	path := filepath.Join(dir, logName)

	tl.scanDir(tl.loadCheckpoints(), true) // no files yet: the next one is new

	ts := timeNowCRI() // near now, or the sweep's age-out tears the run
	l1 := ts + " stderr F alpha"
	l2 := ts + " stdout P frag"
	l3 := ts + " stderr F beta"
	a := int64(len(l1) + 1)
	b := a + int64(len(l2)+1)
	c := b + int64(len(l3)+1)

	writeLog(t, dir, l1, l2, l3)
	tl.scanDir(nil, false)
	tl.sweep(ctx, true)

	f := tl.files[path]
	if f == nil {
		t.Fatal("setup: file not tracked")
	}
	if !f.stStdout.hasRun || f.stStdout.runStart != (pos{seg: f.tail, off: a}) {
		t.Fatalf("setup: stdout run = %+v hasRun=%v, want an open run at offset %d",
			f.stStdout.runStart, f.stStdout.hasRun, a)
	}
	if len(tl.batch) != 2 {
		t.Fatalf("setup: batch = %v, want the two stderr entries", bodies(tl))
	}

	oldSeg := f.tail
	rotateAway(t, dir, 1)
	writeLog(t, dir, ts+" stderr P frag2")
	tl.sweep(ctx, true) // handles the rotation
	tl.sweep(ctx, true) // reads the new inode

	sg := f.segmentByID(oldSeg)
	if sg == nil {
		t.Fatalf("setup: no segment recorded for the rotated inode (segments %+v)", f.segments)
	}
	if sg.to != c || sg.committed != 0 {
		t.Fatalf("setup: segment %d = {committed %d, to %d}, want {0, %d}", oldSeg, sg.committed, sg.to, c)
	}
	if !f.stStdout.hasRun || f.stStdout.runStart != (pos{seg: oldSeg, off: a}) {
		t.Fatalf("setup: stdout run after the rotation = %+v hasRun=%v, want it still open at {%d, %d}",
			f.stStdout.runStart, f.stStdout.hasRun, oldSeg, a)
	}
	if !f.stStderr.hasRun || f.stStderr.runStart != (pos{seg: f.tail, off: 0}) {
		t.Fatalf("setup: stderr run in the new inode = %+v hasRun=%v, want {%d, 0}",
			f.stStderr.runStart, f.stStderr.hasRun, f.tail)
	}

	if wm, ok := f.watermark(); !ok || wm != (pos{seg: oldSeg, off: a}) {
		t.Errorf("watermark = %+v (ok=%v), want {seg %d, off %d}: the stdout run buffered in the OLD "+
			"segment precedes the stderr run at offset 0 of the new one", wm, ok, oldSeg, a)
	}

	// The flush that exports alpha+beta: their end (c) is segment 1's whole
	// owed range, so only the watermark clamp keeps the segment alive.
	tl.flush(ctx)

	if sg := f.segmentByID(oldSeg); sg == nil {
		t.Fatalf("SEGMENT %d RETIRED with its lines still buffered: the stdout fragment run owns [%d, %d) "+
			"of the rotated-away inode, and retirement closed its fd and dropped its checkpoint entry",
			oldSeg, a, b)
	} else if sg.committed != a {
		t.Fatalf("segment %d committed = %d, want %d (clamped to the buffered stdout run)", oldSeg, sg.committed, a)
	}
}

// The consequence of that premature retirement, driven to an actual export: the
// rotated inode's fd is closed and its segment record gone, so the buffered
// stdout fragment can never be replayed. A later export failure rewinds the
// file and purges the pipeline; feedSegments must be able to re-read the owed
// range so the joined record ships whole ("frag"+"rest") instead of as "rest"
// alone — a silently truncated log line with no counter moving.
func TestCarriedFragmentSurvivesARewindAfterRotation(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	exp := &fakeExporter{}
	tl := driveTailer(dir, exp)
	path := filepath.Join(dir, logName)

	tl.scanDir(tl.loadCheckpoints(), true)

	ts := timeNowCRI()
	writeLog(t, dir, ts+" stderr F alpha", ts+" stdout P frag", ts+" stderr F beta")
	tl.scanDir(nil, false)
	tl.sweep(ctx, true)

	f := tl.files[path]
	oldSeg := f.tail
	rotateAway(t, dir, 1)
	writeLog(t, dir, ts+" stderr P frag2")
	tl.sweep(ctx, true)
	tl.sweep(ctx, true)
	tl.flush(ctx) // exports alpha+beta; the clamp must keep segment 1 alive

	// The stdout fragment run completes in the new inode.
	writeLines(t, path, ts+" stdout F rest")
	tl.sweep(ctx, true)
	if !emitted(tl, "fragrest") {
		t.Fatalf("setup: joined stdout entry not in the batch: %v", bodies(tl))
	}

	// That export fails: the file rewinds and the pipeline is purged, so the
	// prefix has to come back from the old segment's record.
	exp.fail = 3
	tl.flush(ctx)

	for i := 0; i < 6; i++ {
		tl.sweep(ctx, true)
		tl.flush(ctx)
	}
	if slices.Contains(exp.get(), "fragrest") {
		return
	}
	t.Fatalf("the fragment buffered in segment %d was lost across the rewind (segment retired early, "+
		"fd closed, nothing to replay): exported %v", oldSeg, exp.get())
}
