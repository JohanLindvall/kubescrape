package tailer

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"testing"
)

// Two rename rotations inside one sweep window must not destroy the middle
// incarnation. reopen clears the fd and identity, so until the file is opened
// again a second rotation has nothing to drain and records no segment.
func TestProbeTwoRotationsInOneWindow(t *testing.T) {
	dir := t.TempDir()
	exp := &fakeExporter{}
	tl := driveTailer(dir, exp)
	ctx := context.Background()
	path := filepath.Join(dir, logName)
	ts := timeNowCRI()

	writeLog(t, dir, ts+" stdout F A-line")
	tl.scanDir(nil, false)
	tl.sweep(ctx, true)
	tl.flush(ctx)

	// Rotation 1, then content on the NEW inode.
	if err := os.Rename(path, path+".1"); err != nil {
		t.Fatal(err)
	}
	writeLog(t, dir, ts+" stdout F B-line")
	tl.scanDir(nil, false)
	tl.sweep(ctx, true) // observes the rotation
	tl.flush(ctx)

	// Rotation 2 before any further read of that inode.
	if err := os.Rename(path, path+".2"); err != nil {
		t.Fatal(err)
	}
	writeLog(t, dir, ts+" stdout F C-line")
	for range 6 {
		tl.scanDir(nil, false)
		tl.sweep(ctx, true)
		tl.flush(ctx)
	}

	got := exp.get()
	t.Logf("exported %v", got)
	for _, want := range []string{"A-line", "B-line", "C-line"} {
		if !slices.Contains(got, want) {
			t.Errorf("%s lost across two rotations in one sweep window: %v", want, got)
		}
	}
}
