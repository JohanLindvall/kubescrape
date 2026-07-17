package tailer

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// A compressed archive is read to EOF, its export FAILS (so the fd is retained
// for recovery), and the runtime then REPLACES the .gz at that path with a new
// archive. The old fd's uncommitted lines must still ship — and the
// REPLACEMENT's lines must be read too. Regression guard for the fd-reuse path
// in openArchive, which skips the inode/fingerprint identity check that the
// open-by-path path performs.
func TestReplacedArchiveAfterFailedExportIsRead(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	exp := &fakeExporter{fail: 50} // a sustained outage: every retry fails
	tl := newArchiveTailer(dir, exp)
	path := filepath.Join(dir, "app.log.gz")

	tl.scanDir(tl.loadCheckpoints(), true)
	writeGzip(t, path, "old-1", "old-2")
	tl.scanDir(nil, false)
	tl.sweep(ctx, true) // reads to EOF; every export fails -> rewind, fd retained
	tl.flush(ctx)
	if got := exp.get(); len(got) != 0 {
		t.Fatalf("nothing should have been exported during the outage, got %v", got)
	}

	// The archive is UNLINKED and a new one takes its place (logrotate pruning
	// and re-compressing). The old inode survives only through our retained fd;
	// its uncommitted lines must still ship, and the replacement must be read.
	time.Sleep(20 * time.Millisecond)
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	writeGzip(t, path, "new-1", "new-2")

	// Collector recovers.
	exp.mu.Lock()
	exp.fail = 0
	exp.mu.Unlock()

	for i := 0; i < 4; i++ {
		tl.scanDir(nil, false)
		tl.sweep(ctx, true)
		tl.flush(ctx)
	}

	got := exp.get()
	t.Logf("EXPORTED: %v", got)
	want := map[string]bool{"old-1": false, "old-2": false, "new-1": false, "new-2": false}
	for _, g := range got {
		if _, ok := want[g]; ok {
			want[g] = true
		}
	}
	for line, seen := range want {
		if !seen {
			t.Errorf("line %q never exported (got %v)", line, got)
		}
	}
}
