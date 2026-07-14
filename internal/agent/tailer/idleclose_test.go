package tailer

import (
	"os"
	"path/filepath"
	"testing"

	"go.opentelemetry.io/collector/pdata/pcommon"
)

// idleFile builds a tracked, resolved file with `content` already on disk and
// the tailer caught up to `committed`.
func idleFile(t *testing.T, tl *Tailer, dir, content string, committed int64) *file {
	t.Helper()
	path := filepath.Join(dir, logName)
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	f := &file{
		path:        path,
		source:      &compiledSource{name: "containers", containerd: true},
		containerID: "0123456789abcdef",
		resolved:    true,
		resource:    pcommon.NewResource(),
	}
	tl.newPipeline(f)
	tl.files[path] = f
	if err := tl.ensureOpen(f); err != nil {
		t.Fatal(err)
	}
	f.committed, f.readPos, f.lineStart = committed, committed, committed
	if _, err := f.f.Seek(committed, 0); err != nil {
		t.Fatal(err)
	}
	return f
}

// The zero-loss baseline: a file whose fd is still held recovers its unread
// tail from an UNLINKED inode, because the fd is the only handle left to it.
// This is what -logs-idle-close is off by default to protect.
func TestDropRecoversUnlinkedTailWithFDHeld(t *testing.T) {
	dir := t.TempDir()
	tl := newTestTailer(dir, "", &fakeExporter{})
	first := timeNowCRI() + " stdout F first\n"
	f := idleFile(t, tl, dir, first, int64(len(first)))

	// The container writes its last line; the log file is then removed.
	last := timeNowCRI() + " stdout F LAST-LINE\n"
	fh, err := os.OpenFile(f.path, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fh.WriteString(last); err != nil {
		t.Fatal(err)
	}
	_ = fh.Close()
	if err := os.Remove(f.path); err != nil {
		t.Fatal(err)
	}

	tl.drop(f)
	if !emitted(tl, "LAST-LINE") {
		t.Fatalf("fd held: unread tail of the unlinked inode was NOT recovered; batch=%v", bodies(tl))
	}
}

// The counterpart: with the fd released (as -logs-idle-close does), the
// unlinked inode is unreachable and its tail is lost. This test PINS that
// trade-off — it is why the flag defaults to 0. If someone makes idle-close
// the default again, this is the guarantee they are giving up.
func TestIdleCloseForfeitsUnlinkedTail(t *testing.T) {
	dir := t.TempDir()
	tl := newTestTailer(dir, "", &fakeExporter{})
	first := timeNowCRI() + " stdout F first\n"
	f := idleFile(t, tl, dir, first, int64(len(first)))

	// Idle-close releases the fd (the file was caught up at that moment).
	_ = f.f.Close()
	f.f = nil

	last := timeNowCRI() + " stdout F LAST-LINE\n"
	fh, err := os.OpenFile(f.path, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fh.WriteString(last); err != nil {
		t.Fatal(err)
	}
	_ = fh.Close()
	if err := os.Remove(f.path); err != nil {
		t.Fatal(err)
	}

	tl.drop(f)
	if emitted(tl, "LAST-LINE") {
		t.Fatal("the unlinked tail was recovered without an fd — if this now works, " +
			"idle-close no longer forfeits the guarantee and the default can be revisited")
	}
}

func emitted(tl *Tailer, body string) bool {
	for _, e := range tl.batch {
		if e.body == body {
			return true
		}
	}
	return false
}

func bodies(tl *Tailer) []string {
	var out []string
	for _, e := range tl.batch {
		out = append(out, e.body)
	}
	return out
}
