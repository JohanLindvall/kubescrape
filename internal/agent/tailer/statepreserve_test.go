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

// A STARTUP listing that fails discovers nothing, and the 2s dirTicker then
// finds every file on a later pass. Those files must still resume at their
// stored offsets: seeding the checkpoints on the initial scan alone made a
// transient glob failure (an unreadable or not-yet-mounted include base) start
// every file on the node at byte 0 — a node-wide re-ingest that also threw away
// the Pending prefixes of unfinished rotated segments, with no counter moving.
func TestFailedStartupListingStillResumesFromCheckpoints(t *testing.T) {
	base := t.TempDir()
	dir := filepath.Join(base, "containers")
	if err := os.Mkdir(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, logName)
	writeLog(t, dir, timeNowCRI()+" stdout F shipped", timeNowCRI()+" stdout F fresh")
	st, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	shipped := int64(len(timeNowCRI()+" stdout F shipped") + 1)

	posPath := filepath.Join(base, "pos.json")
	pos := mustOpenPositions(t, posPath)
	if err := pos.SetLogs(map[string]positions.LogPos{path: {
		Offset: shipped, Inode: inodeOf(st),
		Pending: []positions.Prefix{{Inode: 4242, From: 10, To: 90}},
	}}); err != nil {
		t.Fatal(err)
	}

	// Make the include base unreadable so the first glob fails the way a
	// transient EACCES/EIO on a log directory does (doublestar with
	// WithFailOnIOErrors). The file itself is untouched, so the checkpoint's
	// identity still matches when the listing recovers.
	unreadable(t, dir)

	tl := New(Config{
		Dir: dir, Positions: pos, PollInterval: time.Hour, FlushInterval: time.Hour,
		MetadataWait: time.Second, Metadata: fakeMeta{}, Exporter: &fakeExporter{},
	})
	tl.scanDir(tl.loadCheckpoints(), true)
	if tl.lastListingOK {
		t.Fatal("setup: the initial listing was expected to fail")
	}
	if len(tl.files) != 0 {
		t.Fatalf("setup: %d files tracked by a failed listing", len(tl.files))
	}

	readable(t, dir)
	tl.scanDir(nil, false) // the dirTicker pass that finally sees the file

	f := tl.files[path]
	if f == nil {
		t.Fatal("file not discovered by the successful listing")
	}
	if f.committed != shipped {
		t.Fatalf("committed = %d, want %d: a file discovered after a failed startup listing re-ingests from byte 0", f.committed, shipped)
	}
	if len(f.segments) != 1 || f.segments[0].inode != 4242 {
		t.Fatalf("segments = %+v, want the checkpoint's one Pending prefix restored", f.segments)
	}
}

// The startup half of the same fix: -logs-unknown-files governs the files
// present when the agent starts, and a failed first listing does not end
// startup. A file with no stored position discovered on the retry is still part
// of that startup set ("end" skips it as history), while one appearing after a
// listing has SUCCEEDED was created under our eyes and is read whole whatever
// the setting says.
func TestUnknownFilesPolicySurvivesAFailedStartupListing(t *testing.T) {
	base := t.TempDir()
	dir := filepath.Join(base, "containers")
	if err := os.Mkdir(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, logName)
	writeLog(t, dir, timeNowCRI()+" stdout F history")
	st, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	unreadable(t, dir)

	tl := New(Config{
		Dir: dir, PollInterval: time.Hour, FlushInterval: time.Hour,
		UnknownFiles: "end",
		MetadataWait: time.Second, Metadata: fakeMeta{}, Exporter: &fakeExporter{},
	})
	tl.scanDir(tl.loadCheckpoints(), true)
	if tl.lastListingOK {
		t.Fatal("setup: the initial listing was expected to fail")
	}
	readable(t, dir)
	tl.scanDir(nil, false)

	f := tl.files[path]
	if f == nil {
		t.Fatal("file not discovered by the successful listing")
	}
	if f.committed != st.Size() {
		t.Fatalf("committed = %d, want %d: the startup file skipped by -logs-unknown-files=end was re-read instead", f.committed, st.Size())
	}

	// Now the listing has succeeded: a file created afterwards is new, and
	// "end" must not skip a container's first lines.
	fresh := filepath.Join(dir, "pod2_ns1_app-fedcba9876543210.log")
	writeLines(t, fresh, timeNowCRI()+" stdout F first line")
	tl.scanDir(nil, false)
	if nf := tl.files[fresh]; nf == nil {
		t.Fatal("newly created file not discovered")
	} else if nf.committed != 0 {
		t.Fatalf("committed = %d, want 0: a file created while running is new, not history", nf.committed)
	}
}

// unreadable makes dir's listing fail with EACCES, which is what doublestar's
// WithFailOnIOErrors reports as a failed glob. (A merely ABSENT base directory
// is not an I/O error and lists as empty; only a real read failure — the
// unreadable mount, the transient EIO — reproduces the bug.)
func unreadable(t *testing.T, dir string) {
	t.Helper()
	if os.Geteuid() == 0 {
		t.Skip("running as root: permissions cannot make a listing fail")
	}
	if err := os.Chmod(dir, 0o000); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(dir, 0o755) }) // or TempDir cleanup fails
}

func readable(t *testing.T, dir string) {
	t.Helper()
	if err := os.Chmod(dir, 0o755); err != nil {
		t.Fatal(err)
	}
}
