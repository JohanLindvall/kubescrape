package tailer

import (
	"context"
	"path/filepath"
	"slices"
	"testing"
)

// TestPipelinedCommitHeldByBuildWatermark is the regression test for the
// pipelined-export commit advancing past emitted-but-unexported lines.
//
// Interleaved streams: a stderr P/F run (offsets [0,92)) brackets a stdout line
// (sline). Feeding all three emits sline into batch_1 while the completed stderr
// run stays buffered. Handing batch_1 off (pipelined) and reading on, a later
// stderr line flushes the stderr run into batch_2. When batch_1's commit is then
// applied, it must NOT advance committed past the stderr run — that run's record
// (part1end, starting at offset 0) rides in batch_2, which is not yet exported.
// The old code re-read the LIVE watermark at apply time (a flush later), by then
// the stderr run had moved into batch_2 and the watermark no longer covered it,
// so committed jumped to 92 and a batch_2 failure / crash would lose part1end.
func TestPipelinedCommitHeldByBuildWatermark(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	exp := &fakeExporter{}
	tl := drivePipelined(t, dir, exp)
	path := filepath.Join(dir, logName)

	tl.scanDir(tl.loadCheckpoints(), true)
	ts := timeNowCRI()
	writeLog(t, dir,
		ts+" stderr P part1",
		ts+" stdout F sline",
		ts+" stderr F end",
	)
	tl.scanDir(nil, false)
	tl.sweep(ctx, true)
	tl.flush(ctx) // hands batch_1 (sline) off; the stderr run stays buffered

	// Read on: a trigger stderr line flushes the completed stderr run into batch_2.
	writeLog(t, dir, ts+" stderr F trigger")
	tl.scanDir(nil, false)
	tl.sweep(ctx, true)
	tl.flush(ctx) // settles batch_1's commit; hands batch_2 (part1end) off

	// batch_1's commit has now been applied. part1end (offset 0..92) is in the
	// not-yet-confirmed batch_2, so committed must still be held at 0 — never
	// advanced to the stdout line's end (92) past the buffered stderr run.
	if got := tl.files[path].committed; got != 0 {
		t.Fatalf("committed advanced to %d past the buffered stderr run [0,92); a batch_2 failure would lose part1end", got)
	}

	// Happy path: everything drains and every logical line ships exactly once.
	for i := 0; i < 4; i++ {
		tl.sweep(ctx, true)
		tl.flush(ctx)
	}
	got := exp.get()
	for _, want := range []string{"sline", "part1end", "trigger"} {
		if !slices.Contains(got, want) {
			t.Fatalf("logical line %q lost; exported = %v", want, got)
		}
	}
	// No failures on the happy path, so delivery is exactly-once.
	if len(got) != 3 {
		t.Fatalf("expected exactly 3 logical lines, got %d: %v", len(got), got)
	}
}
