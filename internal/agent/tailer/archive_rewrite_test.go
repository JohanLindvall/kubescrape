package tailer

import (
	"context"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

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
