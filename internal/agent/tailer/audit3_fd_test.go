package tailer

// AUDIT round 5 (adversarial review of e39542d): the tailer now RETAINS file
// descriptors in two new places — one per carried rotation prefix, and the
// compressed archive's fd past EOF/rewind. These repros pin what that broke.

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"
)

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
// the rotatedPrefix it records, and f.carried is only cleared (and its fds
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
		if err := os.Rename(path, fmt.Sprintf("%s.%d", path, i)); err != nil {
			t.Fatal(err)
		}
		writeLog(t, dir, fmt.Sprintf("2026-07-05T10:00:%02dZ stdout F line-%d", i, i))
		tl.scanDir(nil, false)
		tl.sweep(ctx, true)
		tl.flush(ctx) // fails: nothing commits, so nothing releases
	}

	f := tl.files[path]
	held := 0
	for _, c := range f.carried {
		if c.fd != nil {
			held++
		}
	}
	after := openFDs(t)
	t.Logf("carried prefixes=%d retained fds=%d, /proc/self/fd: %d -> %d", len(f.carried), held, base, after)
	if held > 4 || after-base > 4 {
		t.Fatalf("FD LEAK: %d rotations during one outage retained %d fds (open fds %d -> %d); "+
			"f.carried is unbounded and its fds are only released on a successful export",
			rotations, held, base, after)
	}
}

// TestArchiveReplacementRead: openArchive's new fd-reuse fast path
// (f.f != nil -> Seek(0) + re-decompress) is taken WITHOUT the inode/fingerprint
// identity check the open-by-path path performs. readArchive's own comment still
// claims "let openArchive's inode+fingerprint identity check decide".
//
// logrotate replaces a rotated .gz by RENAME (a new inode at the same path).
// After a failed export the tailer holds the old inode's fd, so:
//   - openArchive re-decompresses the OLD inode forever; the NEW file at the
//     path is never read, and
//   - at EOF, archiveDone is stamped with os.Stat(f.path) — the NEW file's size
//     and mtime — so every later sweep sees it "unchanged" and skips it.
//
// The replacement archive's lines are lost permanently, with no error.
func TestArchiveReplacementRead(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	exp := &fakeExporter{fail: 1} // one failed export: the fd is retained
	tl := newSourceTailer(exp, []Source{{
		Name:    "archives",
		Include: []string{filepath.Join(dir, "*.log.gz")},
	}}, false)
	path := filepath.Join(dir, "app.log.gz")

	tl.scanDir(tl.loadCheckpoints(), true)
	writeGzip(t, path, "old-1", "old-2")
	tl.scanDir(nil, false)
	tl.sweep(ctx, true)
	tl.flush(ctx) // FAILS -> rewind, fd retained, gz reader dropped

	// logrotate's actual behaviour: a NEW inode is renamed over the path.
	time.Sleep(20 * time.Millisecond)
	other := filepath.Join(dir, "next.tmp")
	writeGzip(t, other, "new-1", "new-2")
	if err := os.Rename(other, path); err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 5; i++ {
		tl.scanDir(nil, false)
		tl.sweep(ctx, true)
		tl.flush(ctx)
	}

	got := exp.get()
	t.Logf("EXPORTED: %v", got)
	// The old inode is still reachable through the retained fd, so its lines
	// must ship (that is the fix's whole point) — and so must the new file's.
	for _, want := range []string{"old-1", "old-2", "new-1", "new-2"} {
		if !slices.Contains(got, want) {
			t.Errorf("line %q never exported (got %v)", want, got)
		}
	}
}

// TestArchiveRewriteInPlaceDetected: the same missing identity check,
// in the case scanDir cannot rescue — the .gz is rewritten IN PLACE (same
// inode: os.WriteFile/O_TRUNC, `gzip -c > x.gz`), so the file record survives
// and the retained fd is reused.
//
// With a non-zero committed offset (a large archive read across sweeps, the
// first chunk exported) openArchive's reuse path re-decompresses the fd and
// io.CopyN-discards `committed` bytes — of the NEW content. Everything the new
// archive holds below the old decompressed offset is skipped forever, and the
// first line read is a mid-line fragment: precisely the copytruncate bug this
// same commit fixed for the plain path, reintroduced for archives by skipping
// the inode+fingerprint check on the reuse path.
func TestArchiveRewriteInPlaceDetected(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	exp := &fakeExporter{}
	tl := newSourceTailer(exp, []Source{{
		Name:    "archives",
		Include: []string{filepath.Join(dir, "*.log.gz")},
	}}, false)
	tl.cfg.MaxBytesPerSweep = 40 // read the archive across several sweeps
	path := filepath.Join(dir, "app.log.gz")

	tl.scanDir(tl.loadCheckpoints(), true)
	writeGzip(t, path, "old-aaaaaaaaaaaaaaa", "old-bbbbbbbbbbbbbbb", "old-ccccccccccccccc", "old-ddddddddddddddd")
	tl.scanDir(nil, false)
	tl.sweep(ctx, true) // partial read (40 bytes of decompressed data)
	tl.flush(ctx)       // succeeds: committed > 0, mid-archive
	if f := tl.files[path]; f.committed == 0 {
		t.Fatalf("setup: expected a partial commit, committed=%d", f.committed)
	}

	// The archive is rewritten in place (same inode) with entirely new content.
	time.Sleep(20 * time.Millisecond)
	writeGzip(t, path, "new-1", "new-2", "new-3", "new-4")

	for i := 0; i < 8; i++ {
		tl.scanDir(nil, false)
		tl.sweep(ctx, true)
		tl.flush(ctx)
	}

	got := exp.get()
	t.Logf("EXPORTED: %v", got)
	for _, want := range []string{"new-1", "new-2", "new-3", "new-4"} {
		if !slices.Contains(got, want) {
			t.Errorf("replacement archive line %q never exported: the reuse path resumed at the "+
				"stale decompressed offset (got %v)", want, got)
		}
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
