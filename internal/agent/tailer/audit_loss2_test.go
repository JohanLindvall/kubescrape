package tailer

// Terminology: the step narrations in this file predate the segment refactor
// and use its internal names (carried/gen/carriedFed, and source-line
// references that have since moved). The current model records each rotated
// incarnation as a segment on f.segments with its own commit progress; a
// segment retires individually once its range commits (it is no longer a
// single list "cleared when empty"). The interleavings and the guarantees are
// unchanged; these remain valid regression guards.

// Regression guards (at-least-once) for log-line LOSS bugs found by audit
// round 2 in the areas the suite exercised least (multi-hop rotation
// bookkeeping, copytruncate refill, compressed archives), since fixed. See
// the comment on each test for the exact interleaving it pins.

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

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

	if err := os.Rename(path, path+".1"); err != nil {
		t.Fatal(err)
	}
	writeLog(t, dir, "2026-07-05T10:00:01Z stdout F two")

	tl.sweep(ctx, true) // re-reads "one", rotation -> segments=[A]
	tl.flush(ctx)       // FAILS -> rewind (re-arms segmentsFed)
	if len(tl.files[path].segments) != 1 {
		t.Fatalf("setup: segments = %+v, want the rotated-away inode A", tl.files[path].segments)
	}

	tl.sweep(ctx, true) // re-feeds A's prefix ("one") + reads "two"
	tl.flush(ctx)       // FAILS -> rewind

	if err := os.Rename(path, path+".2"); err != nil {
		t.Fatal(err)
	}
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

// TestCopyTruncateRefillPastOffsetKeepsPrefix: readFile's identity re-check
// (tailer.go:1365-1366, 1380) only hashes the file head when the sweep read ZERO
// new bytes:
//
//	read == 0 && !st.ModTime().Equal(f.lastMod) && !f.fp.matches(f.f)
//
// A copytruncate (logrotate's copytruncate, or any writer that truncates and
// keeps writing) whose writer refills the file PAST our read offset before the
// next sweep therefore yields bytes (read > 0) and is never identified as a
// rewrite: the sweep reads from the stale offset into the middle of the NEW
// content. Everything the new content holds below the old offset is skipped
// forever, and the first line read is a mid-line fragment.
//
// state: committed = readPos = 108 (3 old lines shipped)
//
//	-> writer truncates to 0 and writes 10 new lines (370 bytes)
//	-> sweep: inode unchanged, size 370 >= readPos 108, read yields 262 bytes
//	-> new-01..new-02 (and half of new-03) are never read: DATA LOST.
//
// Fix: hoist the identity check ahead of the read — stat first, and whenever the
// mtime changed since the last sweep verify f.fp before consuming bytes at the
// stale offset (the fingerprint hash is 1 KiB per changed file per sweep), or
// track the expected size and re-verify whenever size < readPos OR the head hash
// moved. The current post-read check can only ever catch a rewrite that lands at
// a size <= the old offset.
//
// Severity: MEDIUM-HIGH — silent, unbounded loss; needs copytruncate rotation
// (not what kubelet does, but standard for logrotate-managed plain sources,
// which the tailer explicitly supports) and a writer that outruns one poll
// interval.
func TestCopyTruncateRefillPastOffsetKeepsPrefix(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	exp := &fakeExporter{}
	tl := driveTailer(dir, exp)
	path := filepath.Join(dir, logName)

	tl.scanDir(tl.loadCheckpoints(), true)
	writeLog(t, dir,
		"2026-07-05T10:00:00Z stdout F old-1",
		"2026-07-05T10:00:00Z stdout F old-2",
		"2026-07-05T10:00:00Z stdout F old-3",
	)
	tl.scanDir(nil, false)
	tl.sweep(ctx, true)
	tl.flush(ctx) // the three old lines are shipped and committed

	f := tl.files[path]
	if f.committed == 0 {
		t.Fatal("setup: nothing committed")
	}

	// copytruncate: the content is copied away, the file truncated in place, and
	// the writer immediately refills it past our committed offset.
	if err := os.Truncate(path, 0); err != nil {
		t.Fatal(err)
	}
	var lines []string
	for i := 1; i <= 10; i++ {
		lines = append(lines, "2026-07-05T10:00:0"+string(rune('0'+i%10))+"Z stdout F new-"+string(rune('0'+i/10))+string(rune('0'+i%10)))
	}
	writeLog(t, dir, lines...)
	if st, err := os.Stat(path); err != nil || st.Size() <= f.committed {
		t.Fatalf("setup: refilled size must exceed the committed offset (%d)", f.committed)
	}

	for i := 0; i < 3; i++ {
		tl.scanDir(nil, false)
		tl.sweep(ctx, true)
		tl.flush(ctx)
	}

	got := exp.get()
	for i := 1; i <= 10; i++ {
		want := "new-0" + string(rune('0'+i%10))
		if i == 10 {
			want = "new-10"
		}
		if !slices.Contains(got, want) {
			t.Fatalf("AT-LEAST-ONCE VIOLATED: %q never exported after copytruncate refill; exported = %v", want, got)
		}
	}
}

// TestCompressedArchiveSurvivesDeletionAfterFailedExport: readArchive closes
// the archive's fd at EOF (tailer.go:1430-1438, closeArchive) BEFORE its entries
// have been exported. If that export fails, rewind() (tailer.go:2169-2177) plans
// to re-decompress from the path on the next sweep — but if the archive is
// removed in the meantime (logrotate deleting its own .gz, a pod's log dir being
// cleaned) there is no fd left to recover it from, and drainGone/drainArchive
// (tailer.go:1491-1494) returns immediately because f.gz == nil. goneEnd is then
// max(goneEnd, readPos) with readPos already rewound to committed, so
// settledGone() reports the file as fully shipped and it is released. The whole
// archive is silently lost.
//
// state: 3-line .gz fully read (committed=0, readPos=N) -> export FAILS ->
//
//	rewind (fd already closed, readPos=0) -> file removed -> drainGone drains
//	nothing, goneEnd=0 == committed -> settledGone -> released. 3 lines lost.
//
// Fix: hold the fd (close only the gzip.Reader, or don't close at all) until
// committed >= readPos, exactly as the incremental path holds the fd of an
// unlinked inode; drainArchive must then be able to re-decompress the
// uncommitted suffix from that still-open fd (Seek(0) + a fresh gzip.Reader +
// CopyN discard of the committed prefix). Alternatively refuse to release a gone
// compressed file whose committed < goneEnd, and make goneEnd sticky at the
// archive's true EOF (it already uses max, but readPos is rewound before it is
// ever recorded here).
//
// Severity: MEDIUM — compressed sources are opt-in, but the failure needs only
// one failed export coinciding with the archive's deletion.
func TestCompressedArchiveSurvivesDeletionAfterFailedExport(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	exp := &fakeExporter{}
	tl := newArchiveTailer(dir, exp)
	path := filepath.Join(dir, "old.log.gz")

	tl.scanDir(tl.loadCheckpoints(), true)
	writeGzip(t, path, "arch-one", "arch-two", "arch-three")
	tl.scanDir(nil, false)

	exp.fail = 3 // the first flush (3 attempts) fails

	tl.sweep(ctx, true) // reads the whole archive, closes it at EOF
	tl.flush(ctx)       // FAILS -> rewind: plans to re-decompress from the path

	// The archive is removed before the tailer gets to re-read it.
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}

	if exp.fail != 0 {
		t.Fatalf("test setup: %d export failures left unconsumed", exp.fail)
	}
	for i := 0; i < 4; i++ {
		tl.scanDir(nil, false)
		tl.sweep(ctx, true)
		tl.flush(ctx)
	}

	got := exp.get()
	for _, want := range []string{"arch-one", "arch-two", "arch-three"} {
		if !slices.Contains(got, want) {
			t.Fatalf("AT-LEAST-ONCE VIOLATED: %q lost (archive deleted after a failed export); exported = %v",
				want, got)
		}
	}
	if strings.Join(got, ",") == "" {
		t.Fatal("nothing exported at all")
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

	if err := os.Rename(path, path+".1"); err != nil {
		t.Fatal(err)
	}
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
