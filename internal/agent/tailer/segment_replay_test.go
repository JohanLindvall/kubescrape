package tailer

// Regression tests for incomplete-segment replay after a restart.

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/JohanLindvall/kubescrape/internal/agent/positions"
)

// feedSegments must not range over the LIVE f.segments slice: replaySegment
// retires the segment it is replaying when the source is unrecoverable, and
// retire compacts the slice with slices.DeleteFunc, which nils the vacated tail
// of the backing array. The loop then read a nil *segment and panicked on
// sg.id — in the tailer's single sweep goroutine, taking log collection down
// for the whole node. A second, still-recoverable segment must also survive the
// first one's retirement and be replayed.
func TestFeedSegmentsSurvivesRetireDuringReplay(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	pos := mustOpenPositions(t, filepath.Join(t.TempDir(), "pos.json"))
	path := filepath.Join(dir, logName)
	writeLog(t, dir, timeNowCRI()+" stdout F live")
	ino := inodeOfPath(t, path)

	// A rotated-away file that still exists and must be replayed.
	rot := path + ".1"
	writeLines(t, rot, timeNowCRI()+" stdout F rotated-line")
	rotIno := inodeOfPath(t, rot)
	rst, err := os.Stat(rot)
	if err != nil {
		t.Fatal(err)
	}

	// Pending[0] names an inode nothing on disk has (the runtime pruned that
	// rotation) so it is retired mid-loop; Pending[1] is the recoverable one.
	if err := pos.SetLogs(map[string]positions.LogPos{path: {
		Offset: 0, Inode: ino,
		Pending: []positions.Prefix{
			{Inode: rotIno + 999999, From: 0, To: 50},
			{Inode: rotIno, From: 0, To: rst.Size()},
		},
	}}); err != nil {
		t.Fatal(err)
	}

	exp := &fakeExporter{}
	tl := driveTailer(dir, exp)
	tl.cfg.Positions = pos
	tl.scanDir(tl.loadCheckpoints(), true)

	func() {
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("panic in sweep/feedSegments: %v", r)
			}
		}()
		tl.sweep(ctx, true)
	}()
	tl.flush(ctx)

	if got := exp.get(); !slices.Contains(got, "rotated-line") {
		t.Fatalf("the still-recoverable segment was skipped after an earlier one retired: exported %v", got)
	}
}

// A rotation that happened while the agent was DOWN is recovered from the
// checkpointed identity. This must not be conditional on a NON-ZERO committed
// offset: a checkpoint is persisted as soon as a file is discovered, so an
// export failing since startup leaves identity known and offset 0 — the case
// where the ENTIRE rotated inode is unshipped, i.e. maximum loss, not a no-op.
func TestRotationWhileDownRecoveredAtZeroOffset(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	pos := mustOpenPositions(t, filepath.Join(t.TempDir(), "pos.json"))
	path := filepath.Join(dir, logName)

	// The previous incarnation: read but never committed (export was failing).
	writeLog(t, dir, timeNowCRI()+" stdout F unshipped-old")
	oldIno := inodeOfPath(t, path)
	fh, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	st, _ := fh.Stat()
	fp, err := computeFingerprint(fh, min(int64(1024), st.Size()))
	if err != nil {
		t.Fatal(err)
	}
	_ = fh.Close()
	if err := pos.SetLogs(map[string]positions.LogPos{path: {
		Offset: 0, Inode: oldIno,
		FingerprintLen: fp.Len, FingerprintHash: fp.Hash,
	}}); err != nil {
		t.Fatal(err)
	}

	// Agent is down: rename rotation, then a fresh log at the same path.
	if err := os.Rename(path, path+".1"); err != nil {
		t.Fatal(err)
	}
	writeLog(t, dir, timeNowCRI()+" stdout F new-incarnation")

	exp := &fakeExporter{}
	tl := driveTailer(dir, exp)
	tl.cfg.Positions = pos
	tl.scanDir(tl.loadCheckpoints(), true)
	for range 5 {
		tl.sweep(ctx, true)
		tl.flush(ctx)
	}

	got := exp.get()
	if !slices.Contains(got, "new-incarnation") {
		t.Fatalf("new inode not read: %v", got)
	}
	if !slices.Contains(got, "unshipped-old") {
		t.Fatalf("rotated-while-down inode's uncommitted content lost: exported %v", got)
	}
}

// A replay stopped by the per-sweep byte budget leaves the segment's
// remainder owed: the tail must not be read until the replay finishes, and a
// resumed pass must continue from the FEED frontier, not the commit frontier.
// Reading the tail early fed newer lines into the same pipeline key — an F
// line closed the old segment's still-open P-run and the joiner fused
// fragments across the gap into a record no file ever contained — and a
// committed-offset resume re-fed the buffered fragment into its own run,
// duplicating it inside one joined record.
func TestBudgetCutReplayDefersTailRead(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	pos := mustOpenPositions(t, filepath.Join(t.TempDir(), "pos.json"))
	path := filepath.Join(dir, logName)

	l1 := timeNowCRI() + " stdout F old-one"
	l2 := timeNowCRI() + " stdout P frag-a"
	l3 := timeNowCRI() + " stdout F frag-b"
	rot := path + ".1"
	writeLines(t, rot, l1, l2, l3)
	rotIno := inodeOfPath(t, rot)
	rst, err := os.Stat(rot)
	if err != nil {
		t.Fatal(err)
	}
	writeLog(t, dir, timeNowCRI()+" stdout F tail-one")
	if err := pos.SetLogs(map[string]positions.LogPos{path: {
		Offset: 0, Inode: inodeOfPath(t, path),
		Pending: []positions.Prefix{{Inode: rotIno, From: 0, To: rst.Size()}},
	}}); err != nil {
		t.Fatal(err)
	}

	exp := &fakeExporter{}
	tl := driveTailer(dir, exp)
	tl.cfg.Positions = pos
	// The budget lands exactly on the P fragment's line boundary: the replay
	// pass ends mid-segment with the fragment's run open in the pipeline.
	tl.cfg.MaxBytesPerSweep = len(l1) + len(l2) + 2
	tl.scanDir(tl.loadCheckpoints(), true)

	driveUntil(t, ctx, tl, func() bool {
		all := strings.Join(exp.get(), "\x00")
		return strings.Contains(all, "old-one") && strings.Contains(all, "frag-b") &&
			strings.Contains(all, "tail-one")
	}, "segment and tail content delivered")

	for _, r := range exp.get() {
		if strings.Contains(r, "frag-a") && strings.Contains(r, "tail-one") {
			t.Fatalf("old-segment fragment fused with the tail line: %q", r)
		}
		if strings.Contains(r, "frag-afrag-a") {
			t.Fatalf("budget-resumed replay re-fed a buffered fragment: %q", r)
		}
	}
	if got := exp.get(); !slices.Contains(got, "tail-one") {
		t.Fatalf("tail line not delivered standalone: %q", got)
	}
}
