package tailer

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Pipelined export: drainFile's flushDuringDrain can hand off a new in-flight
// export mid-rotation-handling. reopen must not run under it — it bumps f.gen,
// which would make the export's later failure skip this file's rewind (gen
// mismatch) and permanently lose the drained backlog.
func TestPipelinedRotationDrainSurvivesMidDrainFailure(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	exp := &fakeExporter{}
	tl := drivePipelined(t, dir, exp)
	tl.cfg.BatchSize = 2         // flushDuringDrain fires every 2 entries
	tl.cfg.MaxBytesPerSweep = 80 // the normal read leaves unread backlog on A

	tl.scanDir(tl.loadCheckpoints(), true)
	var want []string
	for i := 0; i < 8; i++ {
		want = append(want, fmt.Sprintf("line%d", i))
		writeLog(t, dir, "2026-07-05T10:00:00Z stdout F "+want[i])
	}
	// A trailing PARTIAL line keeps the pipeline buffered across the rotation,
	// so reopen takes the carry branch (carriedFed=true) — the interleaving
	// where a mid-drain in-flight failure loses the backlog.
	writeLog(t, dir, "2026-07-05T10:00:00Z stdout P straddle-")
	tl.scanDir(nil, false)

	exp.fail = 9 // the next three export attempts' retries fail (3x3)

	tl.sweep(ctx, true) // budget-limited partial read of A
	tl.flush(ctx)       // hands off batch1 (will fail)

	// Rename rotation with most of A unread; B gets a line of its own.
	path := filepath.Join(dir, logName)
	if err := os.Rename(path, path+".1"); err != nil {
		t.Fatal(err)
	}
	writeLog(t, dir, "2026-07-05T10:00:01Z stdout F after")
	want = append(want, "after")

	// The rotation sweep: settle applies batch1's failure, drainFile re-reads A
	// with flushDuringDrain handing off mid-drain, then reopen switches to B.
	for i := 0; i < 8; i++ {
		tl.sweep(ctx, true)
		tl.flush(ctx)
	}

	// Join-insensitive completeness: the straddling P-run may legitimately
	// join with B's first line, so assert every token is present in SOME
	// exported record rather than as a standalone line.
	all := strings.Join(exp.get(), "\n")
	for _, w := range append(want, "straddle-") {
		if !strings.Contains(all, w) {
			t.Fatalf("content %q lost across pipelined rotation drain; exported = %v", w, exp.get())
		}
	}
}
