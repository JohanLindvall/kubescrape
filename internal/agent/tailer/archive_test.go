// Tests for compressed archives (archive.go): one-shot gzip reads, retained
// fds, in-place replacement detection and rate-limit interplay.
package tailer

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/JohanLindvall/kubescrape/internal/obs"
)

func TestCompressedSourceReadWhole(t *testing.T) {
	dir := t.TempDir()
	exp := &fakeExporter{}
	// A .gz source; multiline on to prove archives use the pipeline too.
	tl := newSourceTailer(exp, []Source{{
		Name:    "archives",
		Include: []string{filepath.Join(dir, "*.log.gz")},
	}}, true)
	stop := startTailer(t, tl)
	defer stop()

	// Unlike plain tailing, a compressed archive that appears is read in full
	// (not skipped to the end), including a multi-line Python traceback.
	writeGzip(t, filepath.Join(dir, "old.log.gz"),
		"line one",
		"Traceback (most recent call last):",
		`  File "x.py", line 3, in <module>`,
		"    raise RuntimeError('boom')",
		"line after")

	waitFor(t, func() bool { return len(exp.get()) == 3 }, "3 records (line + joined traceback + line)")
	got := exp.get()
	if got[0] != "line one" || got[2] != "line after" {
		t.Fatalf("records = %q", got)
	}
	if !strings.Contains(got[1], "Traceback") || !strings.Contains(got[1], "raise RuntimeError") {
		t.Fatalf("traceback not joined from archive: %q", got[1])
	}

	// The archive is read once: no duplicate records over subsequent sweeps.
	time.Sleep(200 * time.Millisecond)
	if n := len(exp.get()); n != 3 {
		t.Fatalf("archive re-read: %d records", n)
	}
}

// A compressed archive is read to EOF, its export FAILS (so the fd is retained
// for recovery), and the runtime then REPLACES the .gz at that path with a new
// archive. The old fd's uncommitted lines must still ship — and the
// REPLACEMENT's lines must be read too. Regression guard for the fd-reuse path
// in openArchive, which skips the inode/fingerprint identity check that the
// open-by-path path performs.
func TestReplacedArchiveAfterFailedExportIsRead(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	exp := &fakeExporter{fail: 50} // a sustained outage: every retry fails
	tl := newArchiveTailer(dir, exp)
	path := filepath.Join(dir, "app.log.gz")

	tl.scanDir(tl.loadCheckpoints(), true)
	writeGzip(t, path, "old-1", "old-2")
	tl.scanDir(nil, false)
	tl.sweep(ctx, true) // reads to EOF; every export fails -> rewind, fd retained
	tl.flush(ctx)
	if got := exp.get(); len(got) != 0 {
		t.Fatalf("nothing should have been exported during the outage, got %v", got)
	}

	// The archive is UNLINKED and a new one takes its place (logrotate pruning
	// and re-compressing). The old inode survives only through our retained fd;
	// its uncommitted lines must still ship, and the replacement must be read.
	time.Sleep(20 * time.Millisecond)
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	writeGzip(t, path, "new-1", "new-2")

	// Collector recovers.
	exp.mu.Lock()
	exp.fail = 0
	exp.mu.Unlock()

	for i := 0; i < 4; i++ {
		tl.scanDir(nil, false)
		tl.sweep(ctx, true)
		tl.flush(ctx)
	}

	got := exp.get()
	t.Logf("EXPORTED: %v", got)
	want := map[string]bool{"old-1": false, "old-2": false, "new-1": false, "new-2": false}
	for _, g := range got {
		if _, ok := want[g]; ok {
			want[g] = true
		}
	}
	for line, seen := range want {
		if !seen {
			t.Errorf("line %q never exported (got %v)", line, got)
		}
	}
}

// A compressed archive is read into the batch (not yet flushed) and then
// REWRITTEN in place (same inode). The archiveReplaced restart must issue a
// fresh tail segment id (newTail): without it the batched entries carry the
// OLD stream's segment-qualified positions into commitBatch, stamping
// committed past the replacement stream's readPos — a later rewind (or a
// restart via the checkpoint) would then skip that many bytes of the NEW
// stream, silently losing its lines. The fresh id makes the old positions
// resolve to a dead segment (committing nothing) instead.
func TestInPlaceRewrittenArchiveDoesNotCommitStaleOffsets(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	exp := &fakeExporter{}
	tl := newArchiveTailer(dir, exp)
	path := filepath.Join(dir, "app.log.gz")

	tl.scanDir(tl.loadCheckpoints(), true)
	long := strings.Repeat("x", 40)
	writeGzip(t, path, long+"-1", long+"-2", long+"-3") // ~130 decompressed bytes
	tl.scanDir(nil, false)
	tl.sweep(ctx, true) // old stream in the batch, deliberately NOT flushed

	// Rewritten in place with SHORTER content, so a stale commit is visible as
	// committed > readPos.
	writeGzip(t, path, "new-1", "new-2")
	tl.sweep(ctx, true) // archiveReplaced fires: restart + read the replacement
	tl.flush(ctx)       // export succeeds

	f := tl.files[path]
	if f == nil {
		t.Fatal("file not tracked")
	}
	if f.committed > f.readPos {
		t.Fatalf("committed = %d > readPos = %d: the old stream's offsets were committed into the replacement's offset space",
			f.committed, f.readPos)
	}
	got := exp.get()
	for _, want := range []string{"new-1", "new-2"} {
		if !slices.Contains(got, want) {
			t.Fatalf("replacement line %q missing from exports: %v", want, got)
		}
	}
}

// A pause-mode rate-limited archive (f.limited set, pending retained) is
// rewritten in place. The restart clears pending, and only an ALLOWED line
// ever clears f.limited — so the restart must also reset the flag, or the
// file is wedged forever: both read gates sit behind f.limited, tokens refill
// but nothing consults them, and the replacement is never read.
func TestInPlaceRewrittenArchiveClearsRateLimitPause(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	exp := &fakeExporter{}
	tl := newArchiveTailer(dir, exp)
	tl.cfg.RateLimit = 1 // the burst covers one line; the next pauses the file
	tl.cfg.RateBurst = 1

	path := filepath.Join(dir, "app.log.gz")
	tl.scanDir(tl.loadCheckpoints(), true)
	writeGzip(t, path, "old-1", "old-2", "old-3")
	tl.scanDir(nil, false)
	tl.sweep(ctx, true)
	f := tl.files[path]
	if f == nil || !f.limited {
		t.Fatalf("precondition: file should be paused by the rate limit (limited=%v)", f != nil && f.limited)
	}

	writeGzip(t, path, "new-1") // rewritten in place while paused

	driveUntil(t, ctx, tl, func() bool { return slices.Contains(exp.get(), "new-1") },
		"replacement line exported (file not wedged)")
}

// The general form of the wedge, no rewrite involved: a pause-mode
// rate-limited archive hits EOF with its tail lines still pending. The
// archiveDone short-circuit must not starve the pending retry, or those lines
// never export while the refilled tokens sit unconsulted.
func TestRateLimitedArchiveTailIsNotStarvedByDoneShortCircuit(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	exp := &fakeExporter{}
	tl := newArchiveTailer(dir, exp)
	tl.cfg.RateLimit = 5
	tl.cfg.RateBurst = 1

	path := filepath.Join(dir, "app.log.gz")
	tl.scanDir(tl.loadCheckpoints(), true)
	writeGzip(t, path, "line-1", "line-2", "line-3")
	tl.scanDir(nil, false)

	driveUntil(t, ctx, tl, func() bool { return slices.Contains(exp.get(), "line-3") },
		"archive tail exported (not starved by the done short-circuit)")
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
	tl := newArchiveTailer(dir, exp)
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
	tl := newArchiveTailer(dir, exp)
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

// openArchive must clear a stale rate-limit pause when it wipes pending: the
// wiped bytes are re-read from committed and re-metered, but nothing else ever
// clears the flag, and both read gates sit behind it — the file wedges with a
// full token bucket. Interleaving: pause lands exactly at archive EOF, then
// the archive grows (appended gzip member, head intact) and is re-opened.
func TestPausedArchiveReopenDoesNotWedge(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	exp := &fakeExporter{}
	tl := newArchiveTailer(dir, exp)
	tl.cfg.RateLimit = 1
	tl.cfg.RateBurst = 1

	path := filepath.Join(dir, "app.log.gz")
	tl.scanDir(tl.loadCheckpoints(), true)
	writeGzip(t, path, "a-1", "a-2", "a-3")
	tl.scanDir(nil, false)
	tl.sweep(ctx, true) // reads to EOF; pause hits mid-consume
	tl.flush(ctx)

	// Append a second gzip member: valid gzip, head intact — NOT a rewrite,
	// so archiveDone clears and openArchive re-opens (wiping pending).
	appendGzip(t, path, "b-1")

	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		tl.sweep(ctx, true)
		tl.flush(ctx)
		got := exp.get()
		if slices.Contains(got, "a-3") && slices.Contains(got, "b-1") {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("paused archive wedged after reopen: %v (limited=%v)",
		exp.get(), tl.files[path].limited)
}

// A non-gzip file matched by a compressed source is quarantined under its
// current identity (no per-sweep reopen/warn loop), and a valid rewrite
// recovers on its own.
func TestNonGzipArchiveQuarantinedUntilRewritten(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	exp := &fakeExporter{}
	tl := newArchiveTailer(dir, exp)

	path := filepath.Join(dir, "bad.log.gz")
	if err := os.WriteFile(path, []byte("this is not gzip"), 0o644); err != nil {
		t.Fatal(err)
	}
	tl.scanDir(tl.loadCheckpoints(), true)
	tl.scanDir(nil, false)
	tl.sweep(ctx, true)
	f := tl.files[path]
	if f == nil || !f.archiveDone {
		t.Fatalf("non-gzip archive not quarantined (archiveDone=%v)", f != nil && f.archiveDone)
	}

	// Rewritten with valid gzip: the identity change lifts the quarantine.
	time.Sleep(10 * time.Millisecond)
	writeGzip(t, path, "recovered")
	driveUntil(t, ctx, tl, func() bool { return slices.Contains(exp.get(), "recovered") },
		"rewritten archive recovers from quarantine")
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

// A gzip archive that GROWS after having been read to completion (a second
// member appended): the new content must be delivered.
func TestCompressedArchiveGrowsAfterFirstRead(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	exp := &fakeExporter{}
	tl := newArchiveTailer(dir, exp)
	path := filepath.Join(dir, "grow.log.gz")

	tl.scanDir(tl.loadCheckpoints(), true)
	writeGzip(t, path, "g-one", "g-two")
	tl.scanDir(nil, false)
	tl.sweep(ctx, true)
	tl.flush(ctx)

	// Append a second gzip member (concatenated gzip = valid multistream).
	appendGzip(t, path, "g-three")

	for i := 0; i < 3; i++ {
		tl.scanDir(nil, false)
		tl.sweep(ctx, true)
		tl.flush(ctx)
	}
	if got := exp.get(); !slices.Contains(got, "g-three") {
		t.Fatalf("appended archive member not delivered; exported = %v", got)
	}
}

// A gzip archive whose trailer is cut off: the decodable prefix must still be
// exported (and the tailer must not spin or wedge).
func TestCompressedTruncatedArchiveExportsPrefix(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	exp := &fakeExporter{}
	tl := newArchiveTailer(dir, exp)
	path := filepath.Join(dir, "cut.log.gz")

	tl.scanDir(tl.loadCheckpoints(), true)
	writeGzip(t, path, "c-one", "c-two", "c-three")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data[:len(data)-8], 0o644); err != nil { // drop the CRC/size trailer
		t.Fatal(err)
	}
	tl.scanDir(nil, false)
	for i := 0; i < 3; i++ {
		tl.sweep(ctx, true)
		tl.flush(ctx)
	}
	if got := exp.get(); !slices.Contains(got, "c-one") || !slices.Contains(got, "c-three") {
		t.Fatalf("truncated archive: decodable prefix not exported; got = %v", got)
	}
}

// A fully-consumed archive's fd is released at EOF (committed >= readPos), and
// idle sweeps do not reopen it.
func TestConsumedArchiveReleasesFd(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	exp := &fakeExporter{}
	tl := newArchiveTailer(dir, exp)
	path := filepath.Join(dir, "app.log.gz")

	tl.scanDir(tl.loadCheckpoints(), true)
	writeGzip(t, path, "only-line")
	tl.scanDir(nil, false)
	tl.sweep(ctx, true)
	tl.flush(ctx)
	tl.sweep(ctx, true) // EOF settling sweep

	f := tl.files[path]
	if f == nil {
		t.Fatal("archive not tracked")
	}
	if !slices.Contains(exp.get(), "only-line") {
		t.Fatalf("archive line missing: %v", exp.get())
	}
	if !f.archiveDone {
		t.Fatal("archive not marked done")
	}
	if f.f != nil || f.gz != nil {
		t.Fatalf("consumed archive retains fd (f=%v gz=%v)", f.f != nil, f.gz != nil)
	}
	// Idle sweeps stay no-ops (no reopen).
	before := f.readPos
	tl.sweep(ctx, true)
	if f.readPos != before || f.f != nil {
		t.Fatal("idle sweep reopened a consumed archive")
	}
}

// openArchive's retained-fd resume with committed > 0: after a partial commit
// and an unlink, the re-decompression must discard exactly the committed
// prefix — every line ships exactly once.
func TestArchiveRetainedFdResumeSkipsCommittedPrefix(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	exp := &fakeExporter{}
	tl := newArchiveTailer(dir, exp)
	// One decompressed line is ~8 bytes; cap the sweep to read just line A.
	tl.cfg.MaxBytesPerSweep = 8

	path := filepath.Join(dir, "app.log.gz")
	tl.scanDir(tl.loadCheckpoints(), true)
	writeGzip(t, path, "line-A", "line-B", "line-C")
	tl.scanDir(nil, false)
	tl.sweep(ctx, true) // reads only line-A (byte budget)
	tl.flush(ctx)       // commits it
	if got := exp.get(); !slices.Equal(got, []string{"line-A"}) {
		t.Fatalf("first sweep = %v, want [line-A]", got)
	}

	// Read the rest; the export FAILS (rewind: reader closed, fd retained).
	exp.mu.Lock()
	exp.fail = 3 * 4
	exp.mu.Unlock()
	tl.cfg.MaxBytesPerSweep = 1 << 20
	tl.sweep(ctx, true)
	tl.flush(ctx)

	// The archive is pruned; the retained fd is the only handle.
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	exp.mu.Lock()
	exp.fail = 0
	exp.mu.Unlock()

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		tl.sweep(ctx, true)
		tl.flush(ctx)
		got := exp.get()
		if slices.Contains(got, "line-B") && slices.Contains(got, "line-C") {
			counts := map[string]int{}
			for _, r := range got {
				counts[r]++
			}
			if counts["line-A"] != 1 {
				t.Fatalf("committed prefix not skipped exactly (line-A x%d): %v", counts["line-A"], got)
			}
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("archive remainder never shipped from the retained fd: %v", exp.get())
}

// A corrupt gzip stream (here: trailing garbage after a valid member) must
// deliver what decoded, count the loss, and SETTLE — not silently re-read
// the same error from the retained reader every sweep forever, holding the
// fd and reader with nothing reported.
func TestCorruptArchiveTailSettlesCounted(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	exp := &fakeExporter{}
	tl := newArchiveTailer(dir, exp)
	path := filepath.Join(dir, "app.log.gz")
	writeGzip(t, path, "one", "two")
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte("this is not gzip")); err != nil {
		t.Fatal(err)
	}
	_ = f.Close()
	errsBefore := obs.LogArchiveErrors.Value()

	tl.scanDir(tl.loadCheckpoints(), true)
	driveUntil(t, ctx, tl, func() bool {
		fl, tracked := tl.files[path]
		return tracked && fl.archiveDone && fl.f == nil && fl.gz == nil
	}, "corrupt archive settled and released")

	if got := exp.get(); len(got) != 2 || got[0] != "one" || got[1] != "two" {
		t.Fatalf("decoded prefix must deliver: %v", got)
	}
	if got := obs.LogArchiveErrors.Value(); got != errsBefore+1 {
		t.Fatalf("LogArchiveErrors = %v, want %v (loss must be visible, once)", got, errsBefore+1)
	}
}
