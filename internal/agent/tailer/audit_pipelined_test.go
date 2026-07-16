package tailer

// AUDIT round 2: pipelined-export interleavings the existing pipelined_test.go
// does not cover — a failed in-flight export whose rewind must not eat another
// file's read-ahead, a file deleted while an export is in flight, and a
// shutdown that has to re-export a failed in-flight batch.

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"testing"
)

const logName2 = "pod2_ns1_app-fedcba9876543210.log"

func writeLog2(t *testing.T, dir string, lines ...string) {
	t.Helper()
	writeLines(t, filepath.Join(dir, logName2), lines...)
}

// drivePipelined is driveTailer with pipelined export, driven synchronously by
// the test (the worker goroutine is started here, not by Run).
func drivePipelined(t *testing.T, dir string, exp *fakeExporter) *Tailer {
	t.Helper()
	tl := driveTailer(dir, exp)
	tl.cfg.PipelinedExport = true
	tl.exportCh = make(chan *inflight)
	go tl.exportWorker()
	t.Cleanup(func() {
		if tl.exportCh != nil {
			close(tl.exportCh)
		}
	})
	return tl
}

// A failed in-flight export rewinds ITS files and purges their entries from the
// batch — the read-ahead of an untouched file must survive and ship.
func TestPipelinedFailureKeepsOtherFilesReadAhead(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	exp := &fakeExporter{}
	tl := drivePipelined(t, dir, exp)

	tl.scanDir(tl.loadCheckpoints(), true)
	writeLog(t, dir, "2026-07-05T10:00:00Z stdout F a1")
	tl.scanDir(nil, false)

	exp.fail = 3 // the first handed-off export fails (3 attempts)
	tl.sweep(ctx, true)
	tl.flush(ctx) // hands off; a1's export is in flight and will fail

	// Read-ahead on a DIFFERENT file while that export is in flight.
	writeLog2(t, dir, "2026-07-05T10:00:01Z stdout F b1")
	tl.scanDir(nil, false)
	tl.sweep(ctx, true)

	tl.flush(ctx) // settles the failure (rewinds A, purges A's entries), ships b1
	for i := 0; i < 3; i++ {
		tl.sweep(ctx, true)
		tl.flush(ctx)
	}

	got := exp.get()
	if !slices.Contains(got, "b1") {
		t.Fatalf("other file's read-ahead lost by the rewind purge: %v", got)
	}
	if !slices.Contains(got, "a1") {
		t.Fatalf("rewound file's line never re-shipped: %v", got)
	}
}

// A file deleted while its export is in flight, where that export FAILS: the
// drained tail must be recoverable from the still-open fd (settle before drain,
// hold the fd until the offsets commit).
func TestPipelinedDeletionDuringFailedExport(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	exp := &fakeExporter{}
	tl := drivePipelined(t, dir, exp)

	tl.scanDir(tl.loadCheckpoints(), true)
	writeLog(t, dir, "2026-07-05T10:00:00Z stdout F precious")
	tl.scanDir(nil, false)

	exp.fail = 3
	tl.sweep(ctx, true)
	tl.flush(ctx) // in flight, will fail

	if err := os.Remove(filepath.Join(dir, logName)); err != nil {
		t.Fatal(err)
	}
	tl.scanDir(nil, false) // marks it gone

	for i := 0; i < 4; i++ {
		tl.sweep(ctx, true) // gone branch: settle -> rewind -> drain -> flush
		tl.flush(ctx)
	}

	if got := exp.get(); !slices.Contains(got, "precious") {
		t.Fatalf("AT-LEAST-ONCE VIOLATED: deleted file's tail lost across a failed in-flight export; exported = %v", got)
	}
}

// Shutdown with a FAILED export in flight: Run settles it, the rewind re-reads
// the data, and the final synchronous flush ships it.
func TestPipelinedShutdownReExportsFailedInflight(t *testing.T) {
	dir := t.TempDir()
	exp := &fakeExporter{fail: 3} // the first (pipelined) export fails
	tl := newPipelinedTailer(dir, "", exp)
	stop := startTailer(t, tl)

	writeLog(t, dir, timeNowCRI()+" stdout F shutdown-line")
	// Wait until the handed-off export has burnt its three attempts, i.e. the
	// in-flight batch has failed; then shut down while that result is unapplied.
	waitFor(t, func() bool {
		exp.mu.Lock()
		defer exp.mu.Unlock()
		return exp.fail == 0
	}, "the in-flight export to fail")
	stop()

	if got := exp.get(); !slices.Contains(got, "shutdown-line") {
		t.Fatalf("AT-LEAST-ONCE VIOLATED: line lost when shutdown raced a failed in-flight export; exported = %v", got)
	}
}
