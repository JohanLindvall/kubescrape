package tailer

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/JohanLindvall/kubescrape/internal/agent/attrs"
)

// drivePlainTailer builds a plain (non-containerd) source tailer driven
// synchronously by the test, BatchSize huge so no mid-sweep auto-flush.
func drivePlainTailer(dir string, exp *fakeExporter) *Tailer {
	tl := New(Config{
		Sources: []Source{{
			Name:    "host",
			Include: []string{filepath.Join(dir, "*.log")},
		}},
		PollInterval:  20 * time.Millisecond,
		FlushInterval: time.Millisecond,
		BatchSize:     1 << 20,
		MetadataWait:  time.Second,
		Metadata:      fakeMeta{},
		NodeInfo:      func() *attrs.NodeInfo { return &attrs.NodeInfo{Name: "node1"} },
		Exporter:      exp,
	})
	tl.retryBackoff = time.Millisecond
	return tl
}

func hasRecord(got []string, s string) bool {
	for _, r := range got {
		if r == s {
			return true
		}
	}
	return false
}

// TestPlainRotationExportFailureNoLoss guards the plain-path (non-containerd)
// analogue of the segment-recording invariant: fed-but-uncommitted lines must
// survive a rename rotation whose export fails, because the rotated-away inode
// is then the only copy. feedPlainLine sets streamState.lastEnd (mirroring
// feedLine's CRI streams) so fedEnd() advances and rotate.go records a segment
// for the old inode; without that the plain stream's lastEnd stayed at zero,
// fedEnd()==committed, no segment was recorded, and a,b were lost outright.
func TestPlainRotationExportFailureNoLoss(t *testing.T) {
	dir := t.TempDir()
	exp := &fakeExporter{}
	tl := drivePlainTailer(dir, exp)
	ctx := context.Background()
	path := filepath.Join(dir, "app.log")

	// 1) Old inode: two lines. Discover then sweep reads them into the batch.
	writeLines(t, path, "a", "b")
	tl.scanDir(nil, false)
	tl.sweep(ctx, true)
	if !emitted(tl, "a") || !emitted(tl, "b") {
		t.Fatalf("expected a,b in batch, got %v", bodies(tl))
	}

	// 2) kubelet-style rename rotation: rename aside, create a fresh inode.
	if err := os.Rename(path, path+".1"); err != nil {
		t.Fatal(err)
	}
	writeLines(t, path, "c", "d")

	// 3) Sweep detects the inode change and reopen()s, recording a segment for
	//    the old inode's owed [committed, fedEnd) range.
	tl.scanDir(nil, false)
	tl.sweep(ctx, true)

	// 4) Flush the batch (holding a,b under the old segment) with the export
	//    failing so failBatch rewinds. The old inode is re-read via the segment.
	exp.fail = 3 // exhaust all 3 retries so failBatch actually rewinds
	tl.flush(ctx)

	// 5) Drive to completion; the new inode's c,d must ship too.
	driveUntil(t, ctx, tl, func() bool {
		g := exp.get()
		return hasRecord(g, "c") && hasRecord(g, "d")
	}, "c,d shipped")

	got := exp.get()
	if !hasRecord(got, "a") || !hasRecord(got, "b") {
		t.Errorf("DATA LOSS: a=%v b=%v were never delivered after plain rename rotation + export failure; records=%v",
			hasRecord(got, "a"), hasRecord(got, "b"), got)
	}
}

// TestContainerdRotationExportFailureNoLoss is the containerd contrast: feedLine
// sets lastEnd, so fedEnd tracks the fed boundary, a segment is recorded across
// the rotation, and the failed export's lines are re-read and delivered. It
// pins that the plain fix brings feedPlainLine to parity with this path.
func TestContainerdRotationExportFailureNoLoss(t *testing.T) {
	dir := t.TempDir()
	exp := &fakeExporter{}
	tl := driveTailer(dir, exp)
	ctx := context.Background()
	path := filepath.Join(dir, logName)
	ts := timeNowCRI()

	writeLog(t, dir, ts+" stdout F a", ts+" stdout F b")
	tl.scanDir(nil, false)
	tl.sweep(ctx, true)

	if err := os.Rename(path, path+".1"); err != nil {
		t.Fatal(err)
	}
	writeLog(t, dir, ts+" stdout F c", ts+" stdout F d")
	tl.scanDir(nil, false)
	tl.sweep(ctx, true)

	exp.fail = 3
	tl.flush(ctx)

	driveUntil(t, ctx, tl, func() bool {
		g := exp.get()
		return hasRecord(g, "c") && hasRecord(g, "d")
	}, "c,d shipped")

	got := exp.get()
	if !hasRecord(got, "a") || !hasRecord(got, "b") {
		t.Errorf("containerd lost data too: a=%v b=%v records=%v", hasRecord(got, "a"), hasRecord(got, "b"), got)
	}
}
