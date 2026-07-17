package tailer

// Regression guards (data-integrity, at-least-once) for log-line LOSS bugs
// found by the adversarial audit of the log delivery chain, since fixed. Both
// shared one root cause: rewind() early-returned when f.f == nil, skipping
// newPipeline() -> ledger.reset() -> the segments-fed re-arm — the only
// in-process mechanism that re-reads a rotated-away file's range — so a flush
// that failed right after a rotation rewound nothing at all. See the tests
// below for the exact interleavings.
//
// Terminology: the step narrations below predate the segment refactor and use
// its internal names — carried/gen/carriedFed/feedCarriedPrefix. The current
// model is segment-qualified positions (pos{seg,off}), the per-file f.segments
// list, the segmentsFed re-arm, and feedSegments; the interleavings and the
// guarantees are unchanged.

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"
)

// driveTailer builds a tailer that is driven synchronously by the test (no Run
// goroutine), so the sweep/flush interleavings below are exact.
func driveTailer(dir string, exp *fakeExporter) *Tailer {
	tl := New(Config{
		Dir:           dir,
		PollInterval:  20 * time.Millisecond,
		FlushInterval: time.Millisecond,
		BatchSize:     1 << 20, // never auto-flush mid-sweep; the test flushes
		MetadataWait:  time.Second,
		Metadata:      fakeMeta{},
		Exporter:      exp,
	})
	tl.retryBackoff = time.Millisecond
	return tl
}

// driveMultilineTailer is driveTailer with stack-trace joining enabled and a
// small entry cap — multiline is baked into the compiled sources at New()
// time, so it cannot be toggled on a driveTailer after construction.
func driveMultilineTailer(dir string, exp *fakeExporter) *Tailer {
	tl := New(Config{
		Dir:              dir,
		PollInterval:     20 * time.Millisecond,
		FlushInterval:    time.Millisecond,
		BatchSize:        1 << 20,
		Multiline:        true,
		MultilineTimeout: 50 * time.Millisecond,
		MaxEntryBytes:    64,
		MetadataWait:     time.Second,
		Metadata:         fakeMeta{},
		Exporter:         exp,
	})
	tl.retryBackoff = time.Millisecond
	return tl
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
	path := filepath.Join(dir, logName)
	if err := os.Rename(path, path+".1"); err != nil {
		t.Fatal(err)
	}
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
