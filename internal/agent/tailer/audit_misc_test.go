package tailer

// AUDIT round 2: checkpoint-vs-truncation and rate-limit-vs-rotation angles.

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/JohanLindvall/kubescrape/pkg/logattrs"
)

// A checkpoint whose offset is past the (truncated) file's size must restart at
// zero, not skip the new content.
func TestCheckpointBeyondTruncatedSizeRereads(t *testing.T) {
	dir := t.TempDir()
	cp := filepath.Join(t.TempDir(), "checkpoint")
	ctx := context.Background()
	exp := &fakeExporter{}

	tl := driveTailer(dir, exp)
	tl.cfg.CheckpointFile = cp
	tl.scanDir(tl.loadCheckpoints(), true)
	writeLog(t, dir,
		"2026-07-05T10:00:00Z stdout F old-1",
		"2026-07-05T10:00:00Z stdout F old-2",
		"2026-07-05T10:00:00Z stdout F old-3",
	)
	tl.scanDir(nil, false)
	tl.sweep(ctx, true)
	tl.flush(ctx)
	tl.saveCheckpoints()

	// The file is truncated in place and refilled with SHORTER content while the
	// tailer is down; the checkpoint offset now exceeds its size.
	if err := os.Truncate(filepath.Join(dir, logName), 0); err != nil {
		t.Fatal(err)
	}
	writeLog(t, dir, "2026-07-05T10:00:01Z stdout F fresh")

	tl2 := driveTailer(dir, exp)
	tl2.cfg.CheckpointFile = cp
	tl2.scanDir(tl2.loadCheckpoints(), true)
	tl2.sweep(ctx, true)
	tl2.flush(ctx)

	if got := exp.get(); !slices.Contains(got, "fresh") {
		t.Fatalf("post-truncation content skipped by a stale checkpoint offset; exported = %v", got)
	}
}

// Rate-limit PAUSE mode across a rename rotation: the paused backlog of the
// rotated-away inode must still be drained (the drain bypasses the limiter),
// with nothing lost.
func TestRateLimitPauseAcrossRotation(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	exp := &fakeExporter{}
	tl := driveTailer(dir, exp)
	tl.cfg.RateLimit = 5 // lines/s
	tl.cfg.RateBurst = 2 // only 2 lines pass before the file pauses

	tl.scanDir(tl.loadCheckpoints(), true)
	var lines []string
	for i := 0; i < 20; i++ {
		lines = append(lines, fmt.Sprintf("2026-07-05T10:00:00Z stdout F line-%02d", i))
	}
	writeLog(t, dir, lines...)
	tl.scanDir(nil, false)

	tl.sweep(ctx, true) // consumes the burst, then pauses (f.limited)
	tl.flush(ctx)
	if f := tl.files[filepath.Join(dir, logName)]; !f.limited {
		t.Fatalf("setup: file not paused by the rate limit (tokens=%v)", f.tokens)
	}

	// Rotate while paused: the backlog lives only in the rotated-away inode.
	rotateAway(t, dir, 1)
	writeLog(t, dir, "2026-07-05T10:00:01Z stdout F after-rotation")

	// Sweep until the (rate-limited) new inode's line gets through too; the
	// bucket refills at 5/s, so a few hundred ms of sweeps suffice.
	for i := 0; i < 30 && !slices.Contains(exp.get(), "after-rotation"); i++ {
		tl.sweep(ctx, true)
		tl.flush(ctx)
		time.Sleep(50 * time.Millisecond)
	}

	got := exp.get()
	for i := 0; i < 20; i++ {
		want := fmt.Sprintf("line-%02d", i)
		if !slices.Contains(got, want) {
			t.Fatalf("AT-LEAST-ONCE VIOLATED: %q lost — rate-limit pause + rotation; exported = %v", want, got)
		}
	}
	if !slices.Contains(got, "after-rotation") {
		t.Fatalf("post-rotation line missing: %v", got)
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

// A flush whose records carry line-derived RESOURCE attributes (several
// ResourceLogs per file) and which FAILS: every group must rewind together and
// be re-shipped — the grouping must not change the offset accounting.
func TestLogAttrsGroupsRewindOnFailedFlush(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	exp := &fakeExporter{}
	tl := driveTailer(dir, exp)
	ex, err := logattrs.New(&logattrs.Config{Rules: []logattrs.Rule{
		{Key: "tenant", Attribute: "tenant.id", Target: logattrs.TargetResource},
	}})
	if err != nil {
		t.Fatal(err)
	}
	tl.cfg.LogAttrs = ex

	tl.scanDir(tl.loadCheckpoints(), true)
	writeLog(t, dir,
		`2026-07-05T10:00:00Z stdout F {"tenant":"a","msg":"ra1"}`,
		`2026-07-05T10:00:01Z stdout F {"tenant":"b","msg":"rb1"}`,
		`2026-07-05T10:00:02Z stdout F {"tenant":"a","msg":"ra2"}`,
	)
	tl.scanDir(nil, false)

	exp.fail = 3 // the first flush fails: all three groups must rewind
	tl.sweep(ctx, true)
	tl.flush(ctx)

	for i := 0; i < 3; i++ {
		tl.sweep(ctx, true)
		tl.flush(ctx)
	}
	got := exp.get()
	for _, want := range []string{"ra1", "rb1", "ra2"} {
		found := false
		for _, g := range got {
			if strings.Contains(g, want) {
				found = true
			}
		}
		if !found {
			t.Fatalf("AT-LEAST-ONCE VIOLATED: %q lost after a failed flush of attribute-split groups; exported = %v", want, got)
		}
	}
}
