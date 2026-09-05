package tailer

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestGoneFileMidDrainExportFailureDoesNotSettle is the vanished-file path's
// rule for a drain cut by a FAILED EXPORT rather than by the cap: the batch
// fills mid-drain, its flush fails and rewinds the fd, and the drain reports
// itself unfinished. goneEnd has not been stamped yet on a first cycle, so a
// settle gate that reads "committed >= goneEnd" as "nothing owed" released the
// fd — the ONLY handle to the unlinked inode — with every line past the rewind
// still behind it, and no counter moved. A pod deleted during a collector
// outage is exactly this shape, and it is the case the gone drain exists for.
func TestGoneFileMidDrainExportFailureDoesNotSettle(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	exp := &fakeExporter{}
	tl := driveTailer(dir, exp)
	tl.cfg.BatchSize = 8 // the drain flushes mid-way
	tl.scanDir(tl.loadCheckpoints(), true)

	ts := timeNowCRI()
	line := func(i int) string { return fmt.Sprintf("%s stdout F %04d %s", ts, i, strings.Repeat("y", 40)) }
	// One line first, so the file is opened and its fd held before the rest
	// arrives; then the backlog the drain will read from that fd.
	writeLog(t, dir, line(0))
	tl.scanDir(nil, false)
	tl.sweep(ctx, true)
	tl.flush(ctx)
	const lines = 40
	var rest []string
	for i := 1; i < lines; i++ {
		rest = append(rest, line(i))
	}
	fh, err := os.OpenFile(filepath.Join(dir, logName), os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fh.WriteString(strings.Join(rest, "\n") + "\n"); err != nil {
		t.Fatal(err)
	}
	if err := fh.Close(); err != nil {
		t.Fatal(err)
	}

	// Unlink while the fd is held, and let the drain's first mid-way flush
	// (three attempts) fail: the fd is rewound under the drain.
	if err := os.Remove(filepath.Join(dir, logName)); err != nil {
		t.Fatal(err)
	}
	tl.scanDir(nil, false) // proves absence: gone
	exp.mu.Lock()
	exp.fail = 3
	exp.mu.Unlock()
	tl.sweep(ctx, true)

	driveUntil(t, ctx, tl, func() bool { return len(tl.files) == 0 }, "the gone file to settle and release")

	seen := map[string]bool{}
	for _, r := range exp.get() {
		if len(r) >= 4 {
			seen[r[:4]] = true
		}
	}
	for i := range lines {
		if k := fmt.Sprintf("%04d", i); !seen[k] {
			t.Fatalf("line %s of %d was never exported: the gone drain settled after a mid-drain export failure "+
				"with bytes still behind its fd", k, lines)
		}
	}
}
