package tailer

// Regression tests for incomplete-segment replay after a restart.

import (
	"context"
	"os"
	"path/filepath"
	"slices"
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
