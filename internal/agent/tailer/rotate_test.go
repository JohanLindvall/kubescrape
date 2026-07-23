// Tests for rotation (rotate.go): drains, segments, carried prefixes and
// crash/outage recovery.
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
	"github.com/JohanLindvall/kubescrape/internal/obs"
)

func TestMultilineJoinsAcrossRotation(t *testing.T) {
	dir := t.TempDir()
	exp := &fakeExporter{}
	tl := newMultilineTailer(dir, "", exp, nil)
	stop := startTailer(t, tl)
	defer stop()

	path := filepath.Join(dir, logName)
	// Warm up so the file is open (its fd survives the rename below).
	writeLog(t, dir, timeNowCRI()+" stdout F first")
	waitFor(t, func() bool { return len(exp.get()) == 1 }, "warmup record")

	// The trace begins in the current inode, then the file is rotated away and
	// the continuation lands in a fresh inode — the group straddles both.
	start, rest := panicLines()
	writeLines(t, path, start...)
	rotateAway(t, dir, 1)
	writeLines(t, path, rest...)

	waitFor(t, func() bool { j, _ := panicRecords(exp); return j == 1 }, "trace joined across rotation")
	if j, n := panicRecords(exp); j != 1 || n != 1 {
		t.Fatalf("panic joined=%d count=%d (want 1/1 — not split): %q", j, n, exp.get())
	}
}

func TestMultilineRecoversPrefixAfterRestart(t *testing.T) {
	dir := t.TempDir()

	// The rotated-away file holds the start of a trace.
	start, rest := panicLines()
	oldPath := filepath.Join(dir, "0.log.rotated")
	writeLines(t, oldPath, start...)
	oldSt, err := os.Stat(oldPath)
	if err != nil {
		t.Fatal(err)
	}
	oldFh, err := os.Open(oldPath)
	if err != nil {
		t.Fatal(err)
	}
	oldFp, err := computeFingerprint(oldFh, min(1024, oldSt.Size()))
	_ = oldFh.Close()
	if err != nil {
		t.Fatal(err)
	}

	// The current file holds the continuation and the terminating line.
	writeLines(t, filepath.Join(dir, logName), rest...)

	// A checkpoint as a crash would have left it: committed 0 on the new inode,
	// with a pending prefix naming the rotated file.
	posPath := filepath.Join(dir, "positions.json")
	seed, _ := positions.Open(posPath)
	if err := seed.SetLogs(map[string]positions.LogPos{
		filepath.Join(dir, logName): {
			Offset: 0, Inode: 0,
			Pending: []positions.Prefix{{
				Inode: inodeOfPath(t, oldPath), FingerprintLen: oldFp.Len, FingerprintHash: oldFp.Hash,
				From: 0, To: oldSt.Size(),
			}},
		},
	}); err != nil {
		t.Fatal(err)
	}

	exp := &fakeExporter{}
	tl := newMultilineTailer(dir, "", exp, mustOpenPositions(t, posPath))
	stop := startTailer(t, tl)
	defer stop()

	waitFor(t, func() bool { j, _ := panicRecords(exp); return j == 1 },
		"trace reconstructed from rotated prefix + new inode")
	if j, n := panicRecords(exp); j != 1 || n != 1 {
		t.Fatalf("prefix recovery joined=%d count=%d (want 1/1): %q", j, n, exp.get())
	}
}

func TestMultilineJoinsAcrossDoubleRotation(t *testing.T) {
	dir := t.TempDir()
	exp := &fakeExporter{}
	tl := newMultilineTailer(dir, "", exp, nil)
	stop := startTailer(t, tl)
	defer stop()

	path := filepath.Join(dir, logName)
	writeLog(t, dir, timeNowCRI()+" stdout F first")
	waitFor(t, func() bool { return len(exp.get()) == 1 }, "warmup record")

	// The trace straddles two rename rotations: part A in inode0, B in inode1,
	// C (with the terminator) in inode2.
	a, b, c := panicLines3()
	base := obs.LogRotations.Value()
	writeLines(t, path, a...)
	rotateAway(t, dir, 1)
	writeLines(t, path, b...)
	// The join only works across rotations the tailer actually observes (it
	// follows the symlink, not every intermediate rotated file). Wait until it
	// has processed the first rotation, then let it read B from inode1 before
	// rotating again, so the group genuinely spans two hops. (Reading B into
	// the still-open group can't be observed via an emitted record, so settle
	// briefly — well under the 3s multiline timeout.)
	waitFor(t, func() bool { return obs.LogRotations.Value() >= base+1 }, "first rotation observed")
	time.Sleep(300 * time.Millisecond)
	rotateAway(t, dir, 2)
	writeLines(t, path, c...)

	waitFor(t, func() bool { j, _ := panicRecords(exp); return j == 1 }, "trace joined across two rotations")
	if j, n := panicRecords(exp); j != 1 || n != 1 {
		t.Fatalf("panic joined=%d count=%d (want 1/1 across two rotations): %q", j, n, exp.get())
	}
}

func TestMultilineRecoversAcrossDoubleRotation(t *testing.T) {
	dir := t.TempDir()
	a, b, c := panicLines3()

	// Two rotated-away files hold the first two segments of the trace.
	old1 := filepath.Join(dir, "0.log.1")
	old2 := filepath.Join(dir, "0.log.2")
	writeLines(t, old1, a...)
	writeLines(t, old2, b...)
	writeLines(t, filepath.Join(dir, logName), c...) // current inode

	pend := func(path string) positions.Prefix {
		st, err := os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}
		fh, err := os.Open(path)
		if err != nil {
			t.Fatal(err)
		}
		fp, err := computeFingerprint(fh, min(1024, st.Size()))
		_ = fh.Close()
		if err != nil {
			t.Fatal(err)
		}
		return positions.Prefix{Inode: inodeOf(st), FingerprintLen: fp.Len, FingerprintHash: fp.Hash, From: 0, To: st.Size()}
	}

	posPath := filepath.Join(dir, "positions.json")
	seed, _ := positions.Open(posPath)
	if err := seed.SetLogs(map[string]positions.LogPos{
		filepath.Join(dir, logName): {
			// Oldest first — the order they must be replayed.
			Pending: []positions.Prefix{pend(old1), pend(old2)},
		},
	}); err != nil {
		t.Fatal(err)
	}

	exp := &fakeExporter{}
	tl := newMultilineTailer(dir, "", exp, mustOpenPositions(t, posPath))
	stop := startTailer(t, tl)
	defer stop()

	waitFor(t, func() bool { j, _ := panicRecords(exp); return j == 1 },
		"trace reconstructed from two rotated prefixes + new inode")
	if j, n := panicRecords(exp); j != 1 || n != 1 {
		t.Fatalf("double-rotation recovery joined=%d count=%d (want 1/1): %q", j, n, exp.get())
	}
}

func TestExportFailureRewinds(t *testing.T) {
	dir := t.TempDir()
	exp := &fakeExporter{fail: 3} // one full retry cycle fails
	tl := newTestTailer(dir, "", exp)
	stop := startTailer(t, tl)
	defer stop()

	writeLog(t, dir, "2026-07-05T10:00:00Z stdout F precious")
	// After the failed cycle the file is rewound and re-read; the next
	// export succeeds and the record must not be lost.
	waitFor(t, func() bool { return len(exp.get()) == 1 }, "record after retry")
	if exp.get()[0] != "precious" {
		t.Fatalf("records = %v", exp.get())
	}
}

func TestRotation(t *testing.T) {
	dir := t.TempDir()
	exp := &fakeExporter{}
	tl := newTestTailer(dir, "", exp)
	stop := startTailer(t, tl)
	defer stop()

	writeLog(t, dir, "2026-07-05T10:00:00Z stdout F before")
	waitFor(t, func() bool { return len(exp.get()) == 1 }, "pre-rotation record")

	// Rotate: rename away and write a fresh file at the same path.
	rotateAway(t, dir, 1)
	writeLog(t, dir, "2026-07-05T10:00:01Z stdout F after")
	waitFor(t, func() bool { return len(exp.get()) == 2 }, "post-rotation record")
	if got := exp.get(); got[1] != "after" {
		t.Fatalf("records = %v", got)
	}
}

func TestRotationDrainsOldFile(t *testing.T) {
	dir := t.TempDir()
	exp := &fakeExporter{}
	tl := newTestTailer(dir, "", exp)
	stop := startTailer(t, tl)
	defer stop()

	writeLog(t, dir, "2026-07-05T10:00:00Z stdout F before")
	waitFor(t, func() bool { return len(exp.get()) == 1 }, "pre-rotation record")

	// Rotate: rename away, append a late line to the OLD (renamed) file —
	// mimicking a writer that has not reopened yet — then start the new
	// file. Nothing may be lost.
	path := filepath.Join(dir, logName)
	rotateAway(t, dir, 1)
	old, err := os.OpenFile(path+".1", os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = fmt.Fprintln(old, "2026-07-05T10:00:01Z stdout F late")
	_ = old.Close()
	writeLog(t, dir, "2026-07-05T10:00:02Z stdout F after")

	waitFor(t, func() bool { return len(exp.get()) == 3 }, "all records across rotation")
	got := exp.get()
	if got[1] != "late" || got[2] != "after" {
		t.Fatalf("records = %v", got)
	}
}

func TestDeletionDrainsRemainder(t *testing.T) {
	dir := t.TempDir()
	exp := &fakeExporter{}
	tl := newTestTailer(dir, "", exp)
	stop := startTailer(t, tl)
	defer stop()

	writeLog(t, dir, "2026-07-05T10:00:00Z stdout F first")
	waitFor(t, func() bool { return len(exp.get()) == 1 }, "first record")

	// Stop sweeping long enough to append + delete between sweeps is racy;
	// instead append and remove back to back — whichever sweep sees it, the
	// final drain in drop() must deliver the appended line.
	writeLog(t, dir, "2026-07-05T10:00:01Z stdout F last")
	if err := os.Remove(filepath.Join(dir, logName)); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return len(exp.get()) == 2 }, "record drained on deletion")
	if got := exp.get(); got[1] != "last" {
		t.Fatalf("records = %v", got)
	}
}

// A deleted file is drained (bytes appended since the last read are exported)
// and then dropped from tracking.
func TestFileDeletionDrainsAndDrops(t *testing.T) {
	dir := t.TempDir()
	exp := &fakeExporter{}
	tl := newTestTailer(dir, "", exp)
	stop := startTailer(t, tl)
	defer stop()

	writeLog(t, dir, timeNowCRI()+" stdout F before-delete")
	waitFor(t, func() bool { return len(exp.get()) == 1 }, "first record")

	// Append one more line and remove the file before the next sweep is
	// guaranteed to have read it — the drop path must drain the fd first.
	writeLog(t, dir, timeNowCRI()+" stdout F final-words")
	if err := os.Remove(filepath.Join(dir, logName)); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool {
		for _, r := range exp.get() {
			if r == "final-words" {
				return true
			}
		}
		return false
	}, "final line drained after deletion")

	// Exactly the two records, no duplicates from the drain.
	got := exp.get()
	if len(got) != 2 || got[0] != "before-delete" || got[1] != "final-words" {
		t.Fatalf("records = %q", got)
	}
}

// TestRotationDrainsFullBacklog: a rename rotation must drain the entire
// unread remainder of the rotated-away inode, not a byte-budgeted prefix.
func TestRotationDrainsFullBacklog(t *testing.T) {
	dir := t.TempDir()
	exp := &fakeExporter{}
	tl := newTestTailer(dir, filepath.Join(t.TempDir(), "chk"), exp)
	tl.cfg.MaxBytesPerSweep = 1024 // the old budget would abandon most of the backlog
	stop := startTailer(t, tl)
	defer stop()

	writeLog(t, dir, timeNowCRI()+" stdout F first")
	waitFor(t, func() bool { return len(exp.get()) >= 1 }, "first line exported")

	// Build a backlog far over 4*MaxBytesPerSweep, then rotate before the
	// tailer can catch up.
	var lines []string
	for i := 0; i < 120; i++ {
		lines = append(lines, timeNowCRI()+" stdout F backlog-"+strings.Repeat("y", 100)+"-"+strconv.Itoa(i))
	}
	writeLog(t, dir, lines...)
	rotateAway(t, dir, 1)
	writeLog(t, dir, timeNowCRI()+" stdout F post-rotate")

	waitFor(t, func() bool {
		recs := exp.get()
		n := 0
		for _, r := range recs {
			if strings.HasPrefix(r, "backlog-") {
				n++
			}
		}
		return n == 120
	}, "all 120 backlog lines exported")
}

// TestRotationDuringOutageKeepsDrainedTail: a rename rotation whose
// following flush FAILS, and whose next flush SUCCEEDS, permanently loses every
// line of the rotated-away inode.
//
// Interleaving:
//  1. sweep reads "precious" from inode A (committed=0, readPos=L).
//  2. flush -> export fails -> failBatch -> rewind: f.f != nil, so the pipeline
//     is reset and the bytes are re-read next sweep. (fine so far)
//  3. the file is rotated (rename A -> .1, new inode B at the path).
//  4. sweep re-reads "precious" from A's fd, detects the inode change, drains A,
//     and reopen() takes the non-carry branch: it records
//     carried=[{A, from:0, to:L}] and sets carriedFed=true, gen++, f.f=nil.
//  5. flush -> export fails -> failBatch -> rewind(f): f.f == nil -> EARLY
//     RETURN. carriedFed stays true and the batch was already cleared in flush,
//     so "precious" now exists nowhere: not in the batch, not in the pipeline,
//     and feedCarriedPrefix will never re-read it.
//  6. the collector recovers. sweep reads "after" from inode B; readFile skips
//     feedCarriedPrefix (carriedFed==true); flush succeeds; commitBatch sees an
//     empty watermark and clears f.carried. "precious" is gone for good.
//
// Severity: HIGH — an ordinary kubelet rotation during a collector outage that
// ends on the very next flush. No crash, no corruption, no unusual config.
func TestRotationDuringOutageKeepsDrainedTail(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	exp := &fakeExporter{}
	tl := driveTailer(dir, exp)

	// Initial scan with no files: a file created afterwards is "new" (read from
	// the top), exactly as startTailer arranges for the async tests.
	tl.scanDir(tl.loadCheckpoints(), true)

	writeLog(t, dir, "2026-07-05T10:00:00Z stdout F precious")
	tl.scanDir(nil, false)

	// exportWithRetry makes 3 attempts per flush; fail the next two flushes.
	exp.fail = 6

	tl.sweep(ctx, true) // reads "precious"
	tl.flush(ctx)       // FAILS -> rewind (f.f open, so this one works)

	// Rotate while the collector is still down.
	rotateAway(t, dir, 1)
	writeLog(t, dir, "2026-07-05T10:00:01Z stdout F after")

	tl.sweep(ctx, true) // re-reads "precious", detects rotation, reopen()
	tl.flush(ctx)       // FAILS -> rewind is a no-op (f.f == nil): loss happens here

	// Collector recovers.
	if exp.fail != 0 {
		t.Fatalf("test setup: %d failures left unconsumed", exp.fail)
	}
	tl.scanDir(nil, false)
	tl.sweep(ctx, true) // reads "after"; feedSegments is skipped
	tl.flush(ctx)       // succeeds; commitBatch retires the segments

	// Give the tailer every further chance to redeliver.
	for i := 0; i < 3; i++ {
		tl.sweep(ctx, true)
		tl.flush(ctx)
	}

	got := exp.get()
	if !slices.Contains(got, "after") {
		t.Fatalf("post-rotation line missing: %v", got)
	}
	if !slices.Contains(got, "precious") {
		t.Fatalf("AT-LEAST-ONCE VIOLATED: pre-rotation line lost; exported = %v", got)
	}
}

// TestDeletionDuringOutageKeepsDrainedTail: a log file removed while the
// collector is down loses its drained remainder if the export of that drain
// fails.
//
// Interleaving:
//  1. sweep reads "precious"; flush fails; rewind seeks the (still open) fd back
//     to committed=0.
//  2. the file is deleted (pod removed / runtime cleanup).
//  3. scanDir marks it gone; sweep -> drop(f): drainFile re-reads "precious"
//     from the still-open fd of the now-unlinked inode, stopPipeline emits it
//     into the batch, the fd is CLOSED (f.f = nil) and the file is removed from
//     t.files.
//  4. flush -> export fails -> failBatch -> rewind(f): f.f == nil -> early
//     return; and even a working rewind could not help, because f is no longer
//     in t.files and its inode is unlinked. The batch was cleared in flush.
//     "precious" is gone.
//
// Severity: MEDIUM-HIGH — a pod deleted during a collector outage silently
// loses its last drained lines. Unlike the rotation case there is no carried
// prefix to recover from: the fix has to keep the file (and its fd) alive until
// its offsets are committed.
func TestDeletionDuringOutageKeepsDrainedTail(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	exp := &fakeExporter{}
	tl := driveTailer(dir, exp)

	tl.scanDir(tl.loadCheckpoints(), true)

	writeLog(t, dir, "2026-07-05T10:00:00Z stdout F precious")
	tl.scanDir(nil, false)

	exp.fail = 6 // the next two flushes fail (3 attempts each)

	tl.sweep(ctx, true) // reads "precious"
	tl.flush(ctx)       // FAILS -> rewind

	if err := os.Remove(filepath.Join(dir, logName)); err != nil {
		t.Fatal(err)
	}
	tl.scanDir(nil, false) // marks the file gone
	tl.sweep(ctx, true)    // drop(): drains "precious" from the unlinked fd, closes it
	tl.flush(ctx)          // FAILS -> nothing to rewind into: loss happens here

	if exp.fail != 0 {
		t.Fatalf("test setup: %d failures left unconsumed", exp.fail)
	}
	for i := 0; i < 3; i++ {
		tl.scanDir(nil, false)
		tl.sweep(ctx, true)
		tl.flush(ctx)
	}

	if got := exp.get(); !slices.Contains(got, "precious") {
		t.Fatalf("AT-LEAST-ONCE VIOLATED: deleted file's drained tail lost; exported = %v", got)
	}
}

// openFDs counts this process's open descriptors.
func openFDs(t *testing.T) int {
	t.Helper()
	ents, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		t.Skipf("/proc unavailable: %v", err)
	}
	return len(ents)
}

// TestCarriedPrefixFdsBounded: reopen() now hands the rotated-away inode's fd to
// the segment it records, and a segment only retires (and its fd
// closed) by commitBatch — after a SUCCESSFUL export. Nothing bounds the list.
//
// A collector outage that spans N rotations therefore appends N prefixes, each
// holding an open fd to an unlinked inode: fds grow without limit (the tailer
// runs with the DaemonSet's default RLIMIT_NOFILE, shared with the scrape/ingest
// pipelines), and the pinned inodes keep the rotated files' disk space from
// being reclaimed — the classic "deleted but held open" disk-full, on the node's
// log volume, exactly when the collector is down.
//
// Nothing else caps it: rewinds re-feed every prefix, and both reopen branches
// append. 25 rotations here; a 12-hour outage with hourly logrotate on a chatty
// pod is the real thing.
func TestCarriedPrefixFdsBounded(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	exp := &fakeExporter{fail: 1 << 20} // collector down for the whole test
	tl := driveTailer(dir, exp)
	path := filepath.Join(dir, logName)

	tl.scanDir(tl.loadCheckpoints(), true)
	writeLog(t, dir, "2026-07-05T10:00:00Z stdout F line-0")
	tl.scanDir(nil, false)
	tl.sweep(ctx, true)
	tl.flush(ctx)

	base := openFDs(t)
	const rotations = 25
	for i := 1; i <= rotations; i++ {
		rotateAway(t, dir, i)
		writeLog(t, dir, fmt.Sprintf("2026-07-05T10:00:%02dZ stdout F line-%d", i, i))
		tl.scanDir(nil, false)
		tl.sweep(ctx, true)
		tl.flush(ctx) // fails: nothing commits, so nothing releases
	}

	f := tl.files[path]
	held := 0
	for _, c := range f.segments {
		if c.fd != nil {
			held++
		}
	}
	after := openFDs(t)
	t.Logf("carried segments=%d retained fds=%d, /proc/self/fd: %d -> %d", len(f.segments), held, base, after)
	if held > 4 || after-base > 4 {
		t.Fatalf("FD LEAK: %d rotations during one outage retained %d fds (open fds %d -> %d); "+
			"f.segments is unbounded and its fds are only released on a successful export",
			rotations, held, base, after)
	}
}

// A carried rotation hop must reach the checkpoint at ROTATION time, not on
// the 10s cadence: a crash in the window otherwise loses the rotated tail
// outright (the restart path has no other route to the rotated inode).
func TestCarriedHopPersistedAtRotation(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	posPath := filepath.Join(dir, "positions.json")

	exp := &fakeExporter{}
	tl := driveTailer(dir, exp)
	tl.cfg.Positions = mustOpenPositions(t, posPath)

	tl.scanDir(tl.loadCheckpoints(), true)
	writeLog(t, dir, "2026-07-05T10:00:00Z stdout F precious")
	tl.scanDir(nil, false)
	exp.mu.Lock()
	exp.fail = 1 << 30 // total collector outage
	exp.mu.Unlock()
	tl.sweep(ctx, true)
	tl.flush(ctx) // fails; rewind

	// Rename rotation during the outage.
	rotateAway(t, dir, 1)
	writeLog(t, dir, "2026-07-05T10:00:01Z stdout F second")
	tl.sweep(ctx, true) // re-reads precious, detects rotation, records the hop
	// CRASH: no shutdown, no checkpoint cadence — the tailer is abandoned.

	exp2 := &fakeExporter{}
	tl2 := driveTailer(dir, exp2)
	tl2.cfg.Positions = mustOpenPositions(t, posPath)
	tl2.scanDir(tl2.loadCheckpoints(), true)
	driveUntil(t, ctx, tl2, func() bool {
		got := exp2.get()
		return slices.Contains(got, "precious") && slices.Contains(got, "second")
	}, "rotated tail recovered across crash (hop persisted)")
}

// An unterminated final line of a rotated-away inode is dropped (its
// terminator can never arrive) — the loss must be counted.
func TestTornFinalLineAtRotationCounted(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	exp := &fakeExporter{}
	tl := driveTailer(dir, exp)

	tl.scanDir(tl.loadCheckpoints(), true)
	path := filepath.Join(dir, logName)
	// A complete line plus an unterminated fragment.
	if err := os.WriteFile(path, []byte("2026-07-05T10:00:00Z stdout F whole\n2026-07-05T10:00:01Z stdout F torn-fragm"), 0o644); err != nil {
		t.Fatal(err)
	}
	tl.scanDir(nil, false)
	tl.sweep(ctx, true)
	tl.flush(ctx)

	before := obs.LogTornFinalLines.Value()
	rotateAway(t, dir, 1)
	writeLog(t, dir, "2026-07-05T10:00:02Z stdout F next")
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		tl.sweep(ctx, true)
		tl.flush(ctx)
		if slices.Contains(exp.get(), "next") {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if got := obs.LogTornFinalLines.Value(); got != before+1 {
		t.Fatalf("LogTornFinalLines = %v, want %v", got, before+1)
	}
	if got := exp.get(); !slices.Contains(got, "whole") || !slices.Contains(got, "next") {
		t.Fatalf("surrounding lines lost: %v", got)
	}
}

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

// TestSecondRotationKeepsCarriedPrefix: reopen()'s NON-carry branch
// (tailer.go:1719-1731) does `f.carried = nil` unconditionally before recording
// the current hop. Any earlier hop's prefix — whose lines are live in the
// pipeline / in the unexported batch, i.e. NOT yet committed — is thereby
// forgotten, and the only remaining copies (batch entries) are purged by the
// next failed export's rewind.
//
// Interleaving (collector down across two ordinary rotations):
//  1. sweep reads "one" from inode A; flush FAILS; rewind.
//  2. rotate A -> .1, inode B gets "two".
//  3. sweep re-reads "one", detects the rotation, drains A, reopen(renamed):
//     nothing is buffered, so the else branch records carried=[{A,0,L1}].
//  4. flush FAILS; rewind resets carriedFed, so A will be re-read. (Correct.)
//  5. sweep re-reads A's prefix ("one") + B's "two"; flush FAILS; rewind.
//  6. rotate B -> .2, inode C gets "three".
//  7. sweep re-reads A's prefix + B's "two", detects the rotation, and
//     reopen(renamed) takes the else branch again: f.carried = nil DROPS A,
//     then records only [{B,0,L2}].
//  8. flush FAILS; rewind purges the batch (which held "one" and "two").
//     "one" now exists nowhere: not on any carried prefix, not in the batch.
//  9. the collector recovers: B's prefix and C are shipped; "one" is gone.
//
// Fix: the else branch must APPEND the new hop to f.carried rather than replace
// it — the earlier prefixes are recoverable-source-of-truth for lines that are
// still uncommitted. carried may only be cleared where it already is: in
// commitBatch, once a successful export leaves nothing buffered. (The same
// `f.carried = nil` also runs on the truncation/copytruncate path, discarding a
// pending rotation prefix whose lines are still uncommitted.)
//
// Severity: HIGH — two rename rotations inside one collector outage; no crash,
// no unusual config, default settings.
func TestSecondRotationKeepsCarriedPrefix(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	exp := &fakeExporter{}
	tl := driveTailer(dir, exp)
	path := filepath.Join(dir, logName)

	tl.scanDir(tl.loadCheckpoints(), true) // no files yet: later ones are new

	writeLog(t, dir, "2026-07-05T10:00:00Z stdout F one")
	tl.scanDir(nil, false)

	exp.fail = 3 * 4 // the next four flushes (3 attempts each) fail

	tl.sweep(ctx, true) // reads "one"
	tl.flush(ctx)       // FAILS -> rewind

	rotateAway(t, dir, 1)
	writeLog(t, dir, "2026-07-05T10:00:01Z stdout F two")

	tl.sweep(ctx, true) // re-reads "one", rotation -> segments=[A]
	tl.flush(ctx)       // FAILS -> rewind (re-arms segmentsFed)
	if len(tl.files[path].segments) != 1 {
		t.Fatalf("setup: segments = %+v, want the rotated-away inode A", tl.files[path].segments)
	}

	tl.sweep(ctx, true) // re-feeds A's prefix ("one") + reads "two"
	tl.flush(ctx)       // FAILS -> rewind

	rotateAway(t, dir, 2)
	writeLog(t, dir, "2026-07-05T10:00:02Z stdout F three")

	tl.sweep(ctx, true) // re-feeds A + re-reads "two", second rotation
	tl.flush(ctx)       // FAILS -> rewind purges "one" and "two" from the batch

	if exp.fail != 0 {
		t.Fatalf("test setup: %d export failures left unconsumed", exp.fail)
	}
	// Collector recovers; give the tailer every chance to redeliver.
	for i := 0; i < 4; i++ {
		tl.scanDir(nil, false)
		tl.sweep(ctx, true)
		tl.flush(ctx)
	}

	got := exp.get()
	if !slices.Contains(got, "three") {
		t.Fatalf("post-rotation line missing: %v", got)
	}
	if !slices.Contains(got, "two") {
		t.Fatalf("second-generation line lost: %v", got)
	}
	if !slices.Contains(got, "one") {
		t.Fatalf("AT-LEAST-ONCE VIOLATED: first-generation line lost across two rotations; exported = %v", got)
	}
}

// TestCarriedPrefixSurvivesRotatedFileDeletion: reopen() closes the old
// inode's fd (tailer.go:1707-1710) even when it has just recorded that inode in
// f.carried because its [from,to) range is still UNCOMMITTED. Recovery then
// depends on the rotated file still being reachable BY NAME (findRotated,
// tailer.go:1824). Container runtimes keep a bounded number of rotated files and
// delete the oldest immediately, so a rotation storm (or logrotate's
// "rotate 1") during a collector outage removes the only copy: feedPrefix logs
// "carried log prefix source not found" and the lines are gone.
//
// state: "one" read from A, export FAILS, A rotated away (carried=[{A,0,L}],
//
//	fd closed), export FAILS again (rewind re-arms segmentsFed), the runtime
//	deletes A.1 -> next sweep cannot find it -> "one" is never re-read.
//
// Fix: keep the rotated inode's fd open while a carried prefix references it
// (release it in commitBatch where f.carried is cleared) and re-read the prefix
// from that fd, falling back to findRotated only after a restart. The tailer
// already relies on exactly this "the fd is the only handle to an unlinked
// inode" property in drainGone/settledGone.
//
// Severity: MEDIUM — needs a deletion of the rotated file inside the outage
// window; it is called out as a caveat in CLAUDE.md, but it is avoidable, and
// the loss is silent (a Warn, no metric).
func TestCarriedPrefixSurvivesRotatedFileDeletion(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	exp := &fakeExporter{}
	tl := driveTailer(dir, exp)
	path := filepath.Join(dir, logName)

	tl.scanDir(tl.loadCheckpoints(), true)
	writeLog(t, dir, "2026-07-05T10:00:00Z stdout F one")
	tl.scanDir(nil, false)

	exp.fail = 3 * 2 // two failing flushes
	tl.sweep(ctx, true)
	tl.flush(ctx) // FAILS -> rewind

	rotateAway(t, dir, 1)
	writeLog(t, dir, "2026-07-05T10:00:01Z stdout F two")
	tl.sweep(ctx, true) // rotation -> segments=[A], A's fd closed
	tl.flush(ctx)       // FAILS -> rewind re-arms segmentsFed (A must be re-read)

	// The runtime prunes the rotated file before the tailer can re-read it.
	if err := os.Remove(path + ".1"); err != nil {
		t.Fatal(err)
	}

	if exp.fail != 0 {
		t.Fatalf("test setup: %d export failures left unconsumed", exp.fail)
	}
	for i := 0; i < 3; i++ {
		tl.scanDir(nil, false)
		tl.sweep(ctx, true)
		tl.flush(ctx)
	}

	got := exp.get()
	if !slices.Contains(got, "two") {
		t.Fatalf("post-rotation line missing: %v", got)
	}
	if !slices.Contains(got, "one") {
		t.Fatalf("AT-LEAST-ONCE VIOLATED: rotated-away line lost after its rotated file was deleted; exported = %v", got)
	}
}

// The carried-prefix restart fallback: the checkpoint names a rotated inode
// that no longer exists anywhere. The loss must be COUNTED (obs.LogPrefixLost)
// and the new inode's lines must still ship — no wedge on the missing prefix.
func TestMissingRotatedPrefixCountedAndSkipped(t *testing.T) {
	dir := t.TempDir()

	start, rest := panicLines()
	oldPath := filepath.Join(dir, "0.log.rotated")
	writeLines(t, oldPath, start...)
	oldSt, err := os.Stat(oldPath)
	if err != nil {
		t.Fatal(err)
	}
	oldFh, err := os.Open(oldPath)
	if err != nil {
		t.Fatal(err)
	}
	oldFp, err := computeFingerprint(oldFh, min(1024, oldSt.Size()))
	_ = oldFh.Close()
	if err != nil {
		t.Fatal(err)
	}
	writeLines(t, filepath.Join(dir, logName), rest...)

	posPath := filepath.Join(dir, "positions.json")
	seed, _ := positions.Open(posPath)
	if err := seed.SetLogs(map[string]positions.LogPos{
		filepath.Join(dir, logName): {
			Offset: 0, Inode: 0,
			Pending: []positions.Prefix{{
				Inode: inodeOfPath(t, oldPath), FingerprintLen: oldFp.Len, FingerprintHash: oldFp.Hash,
				From: 0, To: oldSt.Size(),
			}},
		},
	}); err != nil {
		t.Fatal(err)
	}
	// The rotated file is pruned before the restart: unrecoverable.
	if err := os.Remove(oldPath); err != nil {
		t.Fatal(err)
	}

	lostBefore := obs.LogPrefixLost.Value()
	exp := &fakeExporter{}
	tl := newMultilineTailer(dir, "", exp, mustOpenPositions(t, posPath))
	stop := startTailer(t, tl)
	defer stop()

	waitFor(t, func() bool { return len(exp.get()) > 0 }, "new inode's lines despite the lost prefix")
	if got := obs.LogPrefixLost.Value(); got != lostBefore+1 {
		t.Fatalf("LogPrefixLost = %v, want %v (the pruned prefix must be counted)", got, lostBefore+1)
	}
}

// A file deleted DURING a collector outage, after a rotation left a carried
// prefix, must still deliver the prefix's lines once the collector recovers:
// the gone path never reads the file again, so it must feed the carried
// prefixes itself, and settledGone must not release the retained fds while the
// prefix is unexported.
func TestGoneFileDeliversCarriedPrefix(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	exp := &fakeExporter{}
	tl := driveTailer(dir, exp)
	path := filepath.Join(dir, logName)

	tl.scanDir(tl.loadCheckpoints(), true)
	writeLog(t, dir, "2026-07-05T10:00:00Z stdout F one")
	tl.scanDir(nil, false)

	exp.fail = 3 * 4 // the next four flushes (3 attempts each) fail

	tl.sweep(ctx, true) // reads "one"
	tl.flush(ctx)       // FAILS -> rewind

	// Rename rotation while the collector is down: inode A becomes a carried
	// prefix, inode B holds "two".
	rotateAway(t, dir, 1)
	writeLog(t, dir, "2026-07-05T10:00:01Z stdout F two")
	tl.sweep(ctx, true) // re-reads "one", rotation -> carried=[A]
	tl.flush(ctx)       // FAILS -> rewind (re-arms segmentsFed)
	if len(tl.files[path].segments) != 1 {
		t.Fatalf("setup: segments = %+v, want the rotated-away inode A", tl.files[path].segments)
	}

	// Another sweep opens inode B and reads "two" (fd now held), still failing.
	tl.sweep(ctx, true)
	tl.flush(ctx) // FAILS -> rewind
	if f := tl.files[path]; f.f == nil {
		t.Fatal("setup: inode B's fd not held before deletion")
	}

	// The pod is deleted mid-outage: both the live file and the rotated copy
	// vanish by NAME; only the retained fds still reach the bytes.
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(path + ".1"); err != nil {
		t.Fatal(err)
	}
	tl.scanDir(nil, false) // marks it gone

	// Drain + recover: everything must ship.
	for i := 0; i < 5; i++ {
		tl.sweep(ctx, true)
		tl.flush(ctx)
	}
	got := exp.get()
	for _, want := range []string{"one", "two"} {
		if !slices.Contains(got, want) {
			t.Fatalf("line %q lost after deletion during outage; exported = %v", want, got)
		}
	}
}

// A deleted plain file with an UNTERMINATED final line must deliver the tail
// and settle — settledGone's pending check would otherwise hold the fd and the
// files-map entry forever (the missing newline can never arrive).
func TestGoneFileUnterminatedTailDeliversAndSettles(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	exp := &fakeExporter{}
	tl := newSourceTailer(exp, []Source{{
		Name:    "plain",
		Include: []string{filepath.Join(dir, "*.txt")},
	}}, false)
	path := filepath.Join(dir, "app.txt")

	tl.scanDir(tl.loadCheckpoints(), true)
	if err := os.WriteFile(path, []byte("done line\nunterminated tail"), 0o644); err != nil {
		t.Fatal(err)
	}
	tl.scanDir(nil, false)
	tl.sweep(ctx, true)
	tl.flush(ctx)

	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	tl.scanDir(nil, false) // gone
	for i := 0; i < 4; i++ {
		tl.sweep(ctx, true)
		tl.flush(ctx)
	}

	got := exp.get()
	if !slices.Contains(got, "unterminated tail") {
		t.Fatalf("unterminated tail lost: %v", got)
	}
	if _, tracked := tl.files[path]; tracked {
		t.Fatal("gone file with unterminated tail never settled (fd/map leak)")
	}
}

// A gone file whose tail bytes never entered the pipeline (here: a trailing
// blank line on a plain source) must still settle and be released: goneEnd is
// the FED boundary, not readPos — committed can never reach bytes that never
// produced an entry, and comparing against readPos held the fd and the
// files-map entry forever.
func TestGoneFileWithTrailingBlankLineSettles(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	exp := &fakeExporter{}
	tl := newSourceTailer(exp, []Source{{Name: "plain", Include: []string{dir + "/*.log"}}}, false)
	path := filepath.Join(dir, "app.log")
	tl.scanDir(tl.loadCheckpoints(), true) // initial scan first: a pre-existing file would be skipped to its end
	writeLines(t, path, "a", "b", "")      // trailing blank: consumed, never fed
	tl.scanDir(nil, false)                 // discover it
	driveUntil(t, ctx, tl, func() bool { return len(exp.get()) == 2 }, "both lines exported")

	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	driveUntil(t, ctx, tl, func() bool {
		tl.scanDir(nil, false)
		_, tracked := tl.files[path]
		return !tracked
	}, "gone file released despite the never-fed trailing blank")
}

// The rate-DROP variant of the same disease: a chatty pod killed while over
// its rate limit leaves dropped (never-fed) bytes at the tail. The file must
// still settle once everything FED has committed.
func TestGoneFileWithRateDroppedTailSettles(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	exp := &fakeExporter{}
	tl := driveTailer(dir, exp)
	tl.cfg.RateLimit = 2
	tl.cfg.RateBurst = 2
	tl.cfg.RateDrop = true

	tl.scanDir(tl.loadCheckpoints(), true)
	rateLines(t, dir, 0, 6) // burst 2: the tail lines are dropped, never fed
	tl.scanDir(nil, false)
	driveUntil(t, ctx, tl, func() bool { return len(exp.get()) >= 2 }, "burst lines exported")

	path := filepath.Join(dir, logName)
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	driveUntil(t, ctx, tl, func() bool {
		tl.scanDir(nil, false)
		_, tracked := tl.files[path]
		return !tracked
	}, "gone file released despite rate-dropped tail bytes")
}

// An unresolved gone file that restored checkpointed Pending segments must
// count its loss ONCE, retire the segments (their content is unattributable),
// and be released — not re-warn and re-count every sweep forever while
// settledGone stares at segments that can never commit.
func TestUnresolvedGoneFileWithSegmentsSettlesOnce(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	// A previous incarnation: real file with a committed checkpoint carrying
	// an incomplete Pending segment.
	posPath := filepath.Join(t.TempDir(), "pos.json")
	pos := mustOpenPositions(t, posPath)
	path := filepath.Join(dir, logName)
	writeLog(t, dir, "2026-07-05T10:00:00Z stdout F seed")
	ino := inodeOfPath(t, path)
	if err := pos.SetLogs(map[string]positions.LogPos{path: {
		Offset: 0, Inode: ino,
		Pending: []positions.Prefix{{Inode: ino + 999, From: 0, To: 100}},
	}}); err != nil {
		t.Fatal(err)
	}

	exp := &fakeExporter{}
	tl := driveTailer(dir, exp)
	tl.cfg.Positions = pos
	tl.cfg.Metadata = &flakyMeta{fails: 1 << 30} // never resolves
	unresolvedBefore := obs.LogUnresolvedLost.Value()
	prefixBefore := obs.LogPrefixLost.Value()

	tl.scanDir(tl.loadCheckpoints(), true)
	tl.sweep(ctx, true) // tracked, unresolved, segments restored
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	driveUntil(t, ctx, tl, func() bool {
		tl.scanDir(nil, false)
		_, tracked := tl.files[path]
		return !tracked
	}, "unresolved gone file with segments released")

	if got := obs.LogUnresolvedLost.Value(); got != unresolvedBefore+1 {
		t.Fatalf("LogUnresolvedLost = %v, want exactly %v (one count, not one per sweep)", got, unresolvedBefore+1)
	}
	if got := obs.LogPrefixLost.Value(); got != prefixBefore+1 {
		t.Fatalf("LogPrefixLost = %v, want %v (the retired segment)", got, prefixBefore+1)
	}
}

// A checkpointed segment whose rotated file is SHORTER than the recorded
// range (truncated while the agent was down) must count the missing tail as
// lost and retire through the normal commit path — not wedge forever below
// an offset no commit can reach.
func TestSegmentShorterThanRangeRetires(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	posPath := filepath.Join(t.TempDir(), "pos.json")
	pos := mustOpenPositions(t, posPath)
	path := filepath.Join(dir, logName)
	line := timeNowCRI() + " stdout F short"
	writeLog(t, dir, line) // the "rotated" content: one short line
	ino := inodeOfPath(t, path)
	st, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	// The checkpointed identity matches the live file (tail caught up at its
	// size), and the Pending segment claims its range ran to offset 1000 —
	// far past the file's actual EOF (the rotated source was truncated while
	// the agent was down; findRotated resolves the inode to this short file).
	if err := pos.SetLogs(map[string]positions.LogPos{path: {
		Offset: st.Size(), Inode: ino,
		Pending: []positions.Prefix{{Inode: ino, From: 0, To: 1000}},
	}}); err != nil {
		t.Fatal(err)
	}

	exp := &fakeExporter{}
	tl := driveTailer(dir, exp)
	tl.cfg.Positions = pos
	prefixBefore := obs.LogPrefixLost.Value()

	tl.scanDir(tl.loadCheckpoints(), true)
	driveUntil(t, ctx, tl, func() bool {
		f, tracked := tl.files[path]
		return tracked && len(f.segments) == 0
	}, "short segment retired")

	if got := obs.LogPrefixLost.Value(); got != prefixBefore+1 {
		t.Fatalf("LogPrefixLost = %v, want %v (the unreachable tail)", got, prefixBefore+1)
	}
	if got := exp.get(); len(got) != 1 || got[0] != "short" {
		t.Fatalf("fed prefix must still deliver: %v", got)
	}
}
