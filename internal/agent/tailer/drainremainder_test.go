package tailer

// Regressions for the drain's circuit breaker and the torn-final-line report:
// what happens to bytes the drain did not reach, and which loss counter is
// allowed to speak for them.

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/JohanLindvall/kubescrape/internal/obs"
)

// capTailer drives synchronously with a small per-sweep read budget and a tiny
// drain cap, so a rotation leaves an un-drained remainder without writing a
// gigabyte.
func capTailer(dir string, exp *fakeExporter, sweepBytes int, drainCap int64) *Tailer {
	tl := New(Config{
		Dir:              dir,
		PollInterval:     20 * time.Millisecond,
		FlushInterval:    time.Millisecond,
		BatchSize:        1 << 20, // the test flushes
		MaxBytesPerSweep: sweepBytes,
		MetadataWait:     time.Second,
		Metadata:         fakeMeta{},
		Exporter:         exp,
	})
	tl.retryBackoff = time.Millisecond
	tl.drainCap = drainCap
	return tl
}

// TestDrainCapReplaysTheRemainderInsteadOfAbandoningIt pins that the per-drain
// circuit breaker is a bound on ONE drain call, not a licence to discard the
// rest of a rotated-away inode. It used to return the same `true` as EOF, so
// handleRotation completed the rotation as if the inode were exhausted and
// everything past the cap became unreachable — with every loss counter flat.
func TestDrainCapReplaysTheRemainderInsteadOfAbandoningIt(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	exp := &fakeExporter{}
	// 4 KiB read budget, 8 KiB drain cap. drainReader reads in 64 KiB chunks
	// and checks the cap between them, so the drain covers one chunk and stops
	// — the file has to be comfortably larger than that for a genuine remainder
	// to be left owed.
	tl := capTailer(dir, exp, 4096, 8192)
	tl.scanDir(tl.loadCheckpoints(), true)

	const lines = 2000
	ts := timeNowCRI()
	body := strings.Repeat("x", 80)
	all := make([]string, 0, lines)
	for i := 0; i < lines; i++ {
		all = append(all, fmt.Sprintf("%s stdout F %04d %s", ts, i, body))
	}
	writeLog(t, dir, all...)
	tl.scanDir(nil, false)

	prefixLost, tornBefore := obs.LogPrefixLost.Value(), obs.LogTornFinalLines.Value()
	tl.sweep(ctx, true) // reads the first MaxBytesPerSweep only
	tl.flush(ctx)

	rotateAway(t, dir, 1)
	writeLog(t, dir, ts+" stdout F after-rotation")

	driveUntil(t, ctx, tl, func() bool {
		for _, r := range exp.get() {
			if strings.Contains(r, "after-rotation") {
				return true
			}
		}
		return false
	}, "the post-rotation line to export")

	seen := map[string]bool{}
	for _, r := range exp.get() {
		if len(r) >= 4 {
			seen[r[:4]] = true
		}
	}
	var missing []string
	for i := 0; i < lines; i++ {
		if k := fmt.Sprintf("%04d", i); !seen[k] {
			missing = append(missing, k)
		}
	}
	if len(missing) > 0 {
		t.Fatalf("%d of %d lines never exported (first missing %v); prefix_lost moved by %v",
			len(missing), lines, missing[:min(5, len(missing))], obs.LogPrefixLost.Value()-prefixLost)
	}
	// Nothing was given up on, so no loss counter may have moved: the remainder
	// was replayed, not written off. The torn-final-line counter is part of
	// that — the drain stops mid-line at the cap, and reopen must not report
	// the fragment lost when the segment replay is about to re-read it.
	if got := obs.LogPrefixLost.Value() - prefixLost; got != 0 {
		t.Fatalf("kubescrape_log_prefix_lost_total moved by %v for a remainder that was fully delivered", got)
	}
	if got := obs.LogTornFinalLines.Value() - tornBefore; got != 0 {
		t.Fatalf("kubescrape_log_torn_final_lines_total moved by %v for a fragment the replay completed", got)
	}
}

// TestGoneFileDrainCapDoesNotSettleWithBytesUnread is the same rule on the
// vanished-file path: settling releases the fd, which is the ONLY handle to an
// unlinked inode, so a cycle whose drain stopped at the cap must not stamp
// goneEnd from the boundary it reached.
func TestGoneFileDrainCapDoesNotSettleWithBytesUnread(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	exp := &fakeExporter{}
	tl := capTailer(dir, exp, 4096, 8192)
	tl.scanDir(tl.loadCheckpoints(), true)

	const lines = 2000
	ts := timeNowCRI()
	body := strings.Repeat("y", 80)
	all := make([]string, 0, lines)
	for i := 0; i < lines; i++ {
		all = append(all, fmt.Sprintf("%s stdout F %04d %s", ts, i, body))
	}
	writeLog(t, dir, all...)
	tl.scanDir(nil, false)

	tl.sweep(ctx, true)
	tl.flush(ctx)

	// Unlink the path while the fd is held: everything left is reachable only
	// through that fd.
	if err := os.Remove(filepath.Join(dir, logName)); err != nil {
		t.Fatal(err)
	}
	driveUntil(t, ctx, tl, func() bool {
		return len(tl.files) == 0
	}, "the gone file to settle and release")

	seen := map[string]bool{}
	for _, r := range exp.get() {
		if len(r) >= 4 {
			seen[r[:4]] = true
		}
	}
	for i := 0; i < lines; i++ {
		if k := fmt.Sprintf("%04d", i); !seen[k] {
			t.Fatalf("line %s of %d was never exported: the gone drain settled with bytes still behind its fd", k, lines)
		}
	}
}

// TestOversizedLineRemainderIsNotCountedAsATornFinalLine pins that ONE physical
// line moves ONE loss counter. An oversized line's prefix is discarded and
// counted by obs.LogOversizedDropped; the residual fragment left in pending is
// the TAIL of that same line, not an unterminated final line a rotation
// destroyed — reporting it as one double-counts the drop and hands the operator
// a WARN naming a few KiB for a drop of megabytes.
func TestOversizedLineRemainderIsNotCountedAsATornFinalLine(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	exp := &fakeExporter{}
	tl := driveTailer(dir, exp)
	tl.cfg.MaxEntryBytes = 1024
	tl.scanDir(tl.loadCheckpoints(), true)

	// One unterminated line far past MaxEntryBytes+oversizeSlack, with no
	// newline anywhere: consume discards its prefix and leaves the remainder in
	// pending with f.discarding set.
	path := filepath.Join(dir, logName)
	if err := os.WriteFile(path, []byte(strings.Repeat("z", 200_000)), 0o644); err != nil {
		t.Fatal(err)
	}
	tl.scanDir(nil, false)

	overBefore, tornBefore := obs.LogOversizedDropped.Value(), obs.LogTornFinalLines.Value()
	tl.sweep(ctx, true)
	tl.flush(ctx)
	f := tl.files[path]
	if f == nil || !f.discarding || len(f.pending) == 0 {
		t.Fatalf("setup: want a discarding file with a pending remainder, got %+v", f)
	}
	if got := obs.LogOversizedDropped.Value() - overBefore; got != 1 {
		t.Fatalf("setup: oversized_dropped moved by %v, want 1", got)
	}

	rotateAway(t, dir, 1)
	writeLog(t, dir, timeNowCRI()+" stdout F next")
	driveUntil(t, ctx, tl, func() bool {
		for _, r := range exp.get() {
			if r == "next" {
				return true
			}
		}
		return false
	}, "the post-rotation line to export")

	if got := obs.LogTornFinalLines.Value() - tornBefore; got != 0 {
		t.Fatalf("one oversized line also moved kubescrape_log_torn_final_lines_total by %v (oversized moved by %v)",
			got, obs.LogOversizedDropped.Value()-overBefore)
	}
}
