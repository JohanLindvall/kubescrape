package tailer

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/JohanLindvall/kubescrape/internal/agent/positions"
)

// A save while the listing FAILED must not destroy the offsets of files it
// could not see. Rebuilding the map from t.files means a not-yet-mounted log
// dir, a transient EIO or a mistyped include glob would wipe every persisted
// offset — after which the next start skips those files to their end as
// "history" and their unshipped window is gone.
func TestFailedListingDoesNotWipeCheckpoints(t *testing.T) {
	posPath := filepath.Join(t.TempDir(), "pos.json")
	pos := mustOpenPositions(t, posPath)
	if err := pos.SetLogs(map[string]positions.LogPos{
		"/var/log/containers/a.log": {Offset: 100, Inode: 1},
		"/var/log/containers/b.log": {Offset: 200, Inode: 2},
	}); err != nil {
		t.Fatal(err)
	}

	tl := New(Config{
		Dir: t.TempDir(), Positions: pos, PollInterval: time.Hour,
		FlushInterval: time.Hour, MetadataWait: time.Second, Metadata: fakeMeta{},
	})
	tl.lastListingOK = false // the scan could not list the sources
	tl.saveCheckpoints()

	if got := len(pos.Logs()); got != 2 {
		t.Fatalf("checkpoints after a failed-listing save = %d, want 2 preserved", got)
	}

	// A SUCCESSFUL listing that genuinely sees no files still prunes.
	tl.lastListingOK = true
	tl.saveCheckpoints()
	if got := len(pos.Logs()); got != 0 {
		t.Fatalf("checkpoints after a successful empty listing = %d, want 0 (pruning must still work)", got)
	}
}

// A corrupt positions file decodes to NOTHING, so the store looks exactly like
// a first run — and -logs-unknown-files=auto would skip every existing file to
// its end, losing the whole unshipped window. It must re-read instead.
func TestCorruptPositionsPrefersReReadOverSkip(t *testing.T) {
	posPath := filepath.Join(t.TempDir(), "pos.json")
	if err := os.WriteFile(posPath, []byte(`{"logs":{"a":{"offset":`), 0o600); err != nil {
		t.Fatal(err)
	}
	pos, err := positions.Open(posPath)
	if err != nil {
		t.Fatal(err)
	}
	if !pos.Corrupt() {
		t.Fatal("a file that failed to decode must report Corrupt, or it is indistinguishable from a first run")
	}
	if len(pos.Logs()) != 0 {
		t.Fatal("setup: expected an empty store")
	}

	dir := t.TempDir()
	path := filepath.Join(dir, logName)
	writeLog(t, dir, timeNowCRI()+" stdout F unshipped")

	tl := New(Config{
		Dir: dir, Positions: pos, PollInterval: time.Hour, FlushInterval: time.Hour,
		MetadataWait: time.Second, Metadata: fakeMeta{}, Exporter: &fakeExporter{},
	})
	tl.scanDir(tl.loadCheckpoints(), true)

	f := tl.files[path]
	if f == nil {
		t.Fatal("file not tracked")
	}
	if f.committed != 0 {
		t.Fatalf("committed = %d, want 0: a corrupt store must re-read, not skip to EOF", f.committed)
	}
}
