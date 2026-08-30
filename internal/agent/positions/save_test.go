package positions

import (
	"os"
	"path/filepath"
	"reflect"
	"syscall"
	"testing"
)

func inodeOf(t *testing.T, path string) uint64 {
	t.Helper()
	st, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	sys, ok := st.Sys().(*syscall.Stat_t)
	if !ok {
		t.Skip("no inode on this platform")
	}
	return sys.Ino
}

// TestUnchangedDocumentIsNotRewritten: a save writes, fsyncs the file, renames
// and fsyncs the directory — ~11 ms of it fsync on ext4/nvme — and it runs on
// the tailer's single sweep goroutine, which serves every log file on the node.
// Writing bytes that are already durable at that path achieves nothing, so it
// is skipped. The rename replaces the inode, which is what makes "did a write
// happen?" answerable exactly.
func TestUnchangedDocumentIsNotRewritten(t *testing.T) {
	path := filepath.Join(t.TempDir(), "positions.json")
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.SetLogs(map[string]LogPos{"/a.log": {Offset: 1}}); err != nil {
		t.Fatal(err)
	}
	first := inodeOf(t, path)

	for range 5 {
		if err := s.SetLogs(map[string]LogPos{"/a.log": {Offset: 1}}); err != nil {
			t.Fatal(err)
		}
	}
	if got := inodeOf(t, path); got != first {
		t.Fatalf("an unchanged document was rewritten (inode %d -> %d): five saves is ten fsyncs "+
			"on the sweep goroutine for no change on disk", first, got)
	}
	// The cursor lives in the same document, so changing it must write.
	if err := s.SetJournalCursor("s=abc;i=1"); err != nil {
		t.Fatal(err)
	}
	if got := inodeOf(t, path); got == first {
		t.Fatal("a changed document was not written")
	}
	// And the skip is per DOCUMENT, not per section: setting the cursor back
	// to what it already is writes nothing more.
	after := inodeOf(t, path)
	if err := s.SetJournalCursor("s=abc;i=1"); err != nil {
		t.Fatal(err)
	}
	if got := inodeOf(t, path); got != after {
		t.Fatal("a redundant cursor write rewrote the document")
	}

	// Everything is still readable from scratch — the skip must never leave the
	// file behind the in-memory document.
	s2, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if s2.JournalCursor() != "s=abc;i=1" || s2.Logs()["/a.log"].Offset != 1 {
		t.Fatalf("on-disk document diverged from memory: cursor=%q logs=%+v", s2.JournalCursor(), s2.Logs())
	}
}

// TestFirstSaveOfAProcessAlwaysWrites: the skip is keyed on a document THIS
// process wrote. The file on disk may have been left by an older build or an
// operator's editor, and re-marshalling what we loaded is not proof that the
// bytes match — so the identity starts invalid.
func TestFirstSaveOfAProcessAlwaysWrites(t *testing.T) {
	path := filepath.Join(t.TempDir(), "positions.json")
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.SetLogs(map[string]LogPos{"/a.log": {Offset: 1}}); err != nil {
		t.Fatal(err)
	}
	before := inodeOf(t, path)

	// A second process opens the same file and saves the identical document.
	s2, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := s2.SetLogs(s2.Logs()); err != nil {
		t.Fatal(err)
	}
	if got := inodeOf(t, path); got == before {
		t.Fatal("a freshly opened store skipped its first save; it has no evidence about the bytes " +
			"on disk, which may have come from another build entirely")
	}
}

// TestAFailedSaveRecordsNoIdentity. The skip is keyed on "this process wrote
// these exact bytes and the write returned success". A write that FAILED must
// leave no identity behind: today every error path returns before the rename
// (a failed CreateTemp, a failed write or fsync — the temp is removed), so the
// file still holds the last good document and skipping would happen to be
// harmless, but that is a property of the current error paths and not of the
// skip. Recording an identity for a write that did not happen is the shape of
// bug that turns a full disk into silently stale offsets, so the invariant is
// pinned directly rather than through a scenario that only holds by accident.
func TestAFailedSaveRecordsNoIdentity(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("running as root: permissions cannot make a write fail")
	}
	dir := t.TempDir()
	path := filepath.Join(dir, "positions.json")
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.SetLogs(map[string]LogPos{"/a.log": {Offset: 1}}); err != nil {
		t.Fatal(err)
	}
	if !s.writtenValid {
		t.Fatal("a successful save recorded no identity, so every later save rewrites")
	}

	if err := os.Chmod(dir, 0o500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(dir, 0o755) })
	if err := s.SetLogs(map[string]LogPos{"/a.log": {Offset: 2}}); err == nil {
		t.Fatal("a save into a read-only directory reported success")
	}
	if s.writtenValid {
		t.Fatal("a FAILED save left an identity recorded: the store now believes a document it never " +
			"managed to rename is durable, and an unchanged save after it would skip")
	}

	// And it recovers: the next save writes and the document is readable.
	if err := os.Chmod(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := s.SetLogs(map[string]LogPos{"/a.log": {Offset: 2}}); err != nil {
		t.Fatal(err)
	}
	s2, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if got := s2.Logs()["/a.log"].Offset; got != 2 {
		t.Fatalf("stored offset = %d, want 2", got)
	}
}

// TestSetLogsOwnedTakesTheMapAndStillHandsOutCopies pins both halves of the
// ownership transfer: the store must not copy on the way in (that copy is the
// whole point — the tailer builds a fresh map on every save), and Logs must
// still copy on the way out, because callers use the result on their own
// goroutine outside this store's mutex.
func TestSetLogsOwnedTakesTheMapAndStillHandsOutCopies(t *testing.T) {
	path := filepath.Join(t.TempDir(), "positions.json")
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	m := map[string]LogPos{"/a.log": {Offset: 1, Pending: []Prefix{{Inode: 7, From: 0, To: 10}}}}
	if err := s.SetLogsOwned(m); err != nil {
		t.Fatal(err)
	}
	// Taken, not copied — the copy SetLogs makes is exactly what this method
	// exists to skip, and pointer identity is the only way to say so (a copy is
	// otherwise indistinguishable from the original).
	if reflect.ValueOf(s.doc.Logs).UnsafePointer() != reflect.ValueOf(m).UnsafePointer() {
		t.Fatal("SetLogsOwned copied the map: the tailer builds a fresh one on every save and drops " +
			"it on return, so that copy duplicates every entry and every Pending slice on the sweep " +
			"goroutine for nothing")
	}
	// Handed out as a copy, deep through Pending.
	out := s.Logs()
	out["/a.log"].Pending[0].From = 99
	delete(out, "/a.log")
	if got := s.Logs()["/a.log"]; got.Offset != 1 || len(got.Pending) != 1 || got.Pending[0].From != 0 {
		t.Fatalf("a caller mutating the map Logs returned reached the store: %+v", got)
	}
	// And what reached disk is the map that was handed over.
	s2, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if got := s2.Logs()["/a.log"]; got.Offset != 1 || len(got.Pending) != 1 || got.Pending[0].To != 10 {
		t.Fatalf("stored = %+v", got)
	}
}
