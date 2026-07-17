package tailer

// Regression guards from the tailer deep-audit round: rate-limit pause
// interactions with rotation/truncation/archives, the settle-rewind truncation
// blind spot, crash-persistence of carried hops, fingerprint extension without
// a checkpoint store, FIFO discovery, exclude-namespace claiming, and the
// non-gzip quarantine.

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"syscall"
	"testing"
	"time"

	"github.com/JohanLindvall/kubescrape/internal/obs"
)

// Pause-mode rate limiting leaves complete read lines in pending. An in-place
// truncation must not discard them (reopen salvages pending before the reset):
// pause mode's contract is "no loss".
func TestPausePendingSurvivesTruncation(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	exp := &fakeExporter{}
	tl := driveTailer(dir, exp)
	tl.cfg.RateLimit = 1
	tl.cfg.RateBurst = 1

	tl.scanDir(tl.loadCheckpoints(), true)
	writeLog(t, dir,
		"2026-07-05T10:00:00Z stdout F line-1",
		"2026-07-05T10:00:01Z stdout F line-2",
		"2026-07-05T10:00:02Z stdout F line-3",
	)
	tl.scanDir(nil, false)
	tl.sweep(ctx, true) // burst covers line-1; line-2/3 pause in pending
	path := filepath.Join(dir, logName)
	f := tl.files[path]
	if f == nil || !f.limited || len(f.pending) == 0 {
		t.Fatalf("precondition: want paused file with pending (limited=%v pending=%d)",
			f != nil && f.limited, len(f.pending))
	}

	// Copytruncate lands while paused: content replaced, smaller.
	if err := os.WriteFile(path, []byte("2026-07-05T10:00:03Z stdout F fresh\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		tl.sweep(ctx, true)
		tl.flush(ctx)
		got := exp.get()
		if slices.Contains(got, "line-2") && slices.Contains(got, "line-3") && slices.Contains(got, "fresh") {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("pause-retained lines lost across truncation: %v", exp.get())
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
	tl := newSourceTailer(exp, []Source{{
		Name:    "archives",
		Include: []string{filepath.Join(dir, "*.log.gz")},
	}}, false)
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
	fh, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	second := filepath.Join(dir, "second.gz")
	writeGzip(t, second, "b-1")
	data, err := os.ReadFile(second)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fh.Write(data); err != nil {
		t.Fatal(err)
	}
	_ = fh.Close()
	_ = os.Remove(second)

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
	path := filepath.Join(dir, logName)
	if err := os.Rename(path, path+".1"); err != nil {
		t.Fatal(err)
	}
	writeLog(t, dir, "2026-07-05T10:00:01Z stdout F second")
	tl.sweep(ctx, true) // re-reads precious, detects rotation, records the hop
	// CRASH: no shutdown, no checkpoint cadence — the tailer is abandoned.

	exp2 := &fakeExporter{}
	tl2 := driveTailer(dir, exp2)
	tl2.cfg.Positions = mustOpenPositions(t, posPath)
	tl2.scanDir(tl2.loadCheckpoints(), true)
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		tl2.sweep(ctx, true)
		tl2.flush(ctx)
		got := exp2.get()
		if slices.Contains(got, "precious") && slices.Contains(got, "second") {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("rotated tail lost across crash (hop not persisted): %v", exp2.get())
}

// Without a checkpoint store nothing used to extend fingerprints: a file first
// opened at size 0 kept the matches-anything empty fingerprint forever,
// blinding every fp-based rotation guard. The read path must extend it.
func TestFingerprintExtendsWithoutCheckpointStore(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	exp := &fakeExporter{}
	tl := driveTailer(dir, exp) // no CheckpointFile, no Positions

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
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		tl.sweep(ctx, true)
		tl.flush(ctx)
		if slices.Contains(exp.get(), "bbbb") {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("same-size rewrite undetected (fingerprint blind): %v", exp.get())
}

// A FIFO matched by a source's glob must never be tracked: open(2)/read(2) on
// it block indefinitely and would wedge the single sweep goroutine.
func TestFIFOIsNeverTracked(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	exp := &fakeExporter{}
	tl := newSourceTailer(exp, []Source{{
		Name:    "plain",
		Include: []string{filepath.Join(dir, "*.log")},
	}}, false)

	tl.scanDir(tl.loadCheckpoints(), true) // files created later are new
	fifo := filepath.Join(dir, "pipe.log")
	if err := syscall.Mkfifo(fifo, 0o644); err != nil {
		t.Fatal(err)
	}
	writeLines(t, filepath.Join(dir, "real.log"), "hello")
	tl.scanDir(nil, false)
	done := make(chan struct{})
	go func() {
		tl.sweep(ctx, true) // would block forever on the FIFO without the guard
		tl.flush(ctx)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("sweep wedged (FIFO opened)")
	}
	if _, tracked := tl.files[fifo]; tracked {
		t.Fatal("FIFO tracked")
	}
	if !slices.Contains(exp.get(), "hello") {
		t.Fatalf("regular file not exported alongside the FIFO: %v", exp.get())
	}
}

// An excluded-namespace containerd file is CLAIMED by the containerd source
// even though it is skipped: a later catch-all source must not resurrect it
// (ExcludeNamespaces is the observability feedback-loop guard).
func TestExcludedNamespaceNotResurrectedByLaterSource(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	exp := &fakeExporter{}
	tl := newSourceTailer(exp, []Source{
		{Name: "containers", Include: []string{filepath.Join(dir, "*.log")}, Containerd: true},
		{Name: "catchall", Include: []string{filepath.Join(dir, "*.log")}},
	}, false)
	tl.cfg.ExcludeNamespaces = []string{"ns1"}

	tl.scanDir(tl.loadCheckpoints(), true)
	writeLog(t, dir, "2026-07-05T10:00:00Z stdout F feedback-loop") // pod1_ns1_...
	tl.scanDir(nil, false)
	tl.sweep(ctx, true)
	tl.flush(ctx)

	if f, tracked := tl.files[filepath.Join(dir, logName)]; tracked {
		t.Fatalf("excluded file tracked by source %q", f.source.name)
	}
	if got := exp.get(); len(got) != 0 {
		t.Fatalf("excluded namespace exported via catch-all: %v", got)
	}
}

// A non-gzip file matched by a compressed source is quarantined under its
// current identity (no per-sweep reopen/warn loop), and a valid rewrite
// recovers on its own.
func TestNonGzipArchiveQuarantinedUntilRewritten(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	exp := &fakeExporter{}
	tl := newSourceTailer(exp, []Source{{
		Name:    "archives",
		Include: []string{filepath.Join(dir, "*.log.gz")},
	}}, false)

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
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		tl.sweep(ctx, true)
		tl.flush(ctx)
		if slices.Contains(exp.get(), "recovered") {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("rewritten archive never recovered from quarantine: %v", exp.get())
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
	if err := os.Rename(path, path+".1"); err != nil {
		t.Fatal(err)
	}
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
