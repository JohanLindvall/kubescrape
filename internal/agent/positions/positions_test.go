package positions

import (
	"bytes"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/JohanLindvall/kubescrape/internal/obs"
)

func TestLogsAndCursorPersistTogether(t *testing.T) {
	path := filepath.Join(t.TempDir(), "positions.json")

	s, _ := Open(path)
	if err := s.SetLogs(map[string]LogPos{"/var/log/a.log": {Offset: 100, Inode: 7}}); err != nil {
		t.Fatal(err)
	}
	if err := s.SetJournalCursor("s=abc;i=1"); err != nil {
		t.Fatal(err)
	}

	// Reopening sees both sections: neither writer clobbered the other.
	s2, _ := Open(path)
	logs := s2.Logs()
	if len(logs) != 1 || logs["/var/log/a.log"].Offset != 100 || logs["/var/log/a.log"].Inode != 7 {
		t.Errorf("logs = %+v", logs)
	}
	if s2.JournalCursor() != "s=abc;i=1" {
		t.Errorf("cursor = %q", s2.JournalCursor())
	}

	// Updating logs preserves the cursor and vice versa.
	if err := s2.SetLogs(map[string]LogPos{"/var/log/b.log": {Offset: 5}}); err != nil {
		t.Fatal(err)
	}
	s3, _ := Open(path)
	if s3.JournalCursor() != "s=abc;i=1" {
		t.Errorf("cursor lost after log update: %q", s3.JournalCursor())
	}
	if _, ok := s3.Logs()["/var/log/b.log"]; !ok {
		t.Errorf("logs not updated: %+v", s3.Logs())
	}
}

func TestLogsReturnsCopy(t *testing.T) {
	s, _ := Open(filepath.Join(t.TempDir(), "p.json"))
	_ = s.SetLogs(map[string]LogPos{"a": {Offset: 1}})
	m := s.Logs()
	m["a"] = LogPos{Offset: 999}
	if s.Logs()["a"].Offset != 1 {
		t.Error("Logs() returned a live map, not a copy")
	}
}

// The documented copies are deep through Pending in BOTH directions: the
// caller keeps mutating its map and slices on its own goroutine after
// SetLogs, and mutates what Logs returns — a shared slice header would put
// those writes outside the store's mutex.
func TestPendingIsDeepCopied(t *testing.T) {
	s, _ := Open(filepath.Join(t.TempDir(), "p.json"))

	pending := []Prefix{{Inode: 3, From: 0, To: 10}}
	in := map[string]LogPos{"f": {Offset: 1, Pending: pending}}
	if err := s.SetLogs(in); err != nil {
		t.Fatal(err)
	}
	pending[0].To = 999           // the caller's slice stays the caller's
	in["f"] = LogPos{Offset: 777} // and so does its map
	got := s.Logs()["f"]
	if got.Offset != 1 || len(got.Pending) != 1 || got.Pending[0].To != 10 {
		t.Fatalf("SetLogs retained the caller's memory: %+v", got)
	}

	out := s.Logs()["f"].Pending
	out[0].To = 555
	if s.Logs()["f"].Pending[0].To != 10 {
		t.Error("Logs() shared the stored Pending backing array")
	}
}

func TestOpenMissingAndCorrupt(t *testing.T) {
	// Missing file: starts empty, usable.
	s, _ := Open(filepath.Join(t.TempDir(), "absent.json"))
	if len(s.Logs()) != 0 || s.JournalCursor() != "" {
		t.Error("missing file not empty")
	}

	// Corrupt file: tolerated but counted, overwritten on next save.
	dir := t.TempDir()
	path := filepath.Join(dir, "corrupt.json")
	_ = os.WriteFile(path, []byte("{not json"), 0o644)
	before := obs.PositionsCorrupt.Value()
	s, _ = Open(path)
	if got := obs.PositionsCorrupt.Value(); got != before+1 {
		t.Errorf("PositionsCorrupt = %v, want %v", got, before+1)
	}
	if err := s.SetJournalCursor("c"); err != nil {
		t.Fatal(err)
	}
	if s2, _ := Open(path); s2.JournalCursor() != "c" {
		t.Error("corrupt file not recovered")
	}
	if got := obs.PositionsCorrupt.Value(); got != before+1 {
		t.Error("clean reopen bumped PositionsCorrupt")
	}
}

// A TYPE error (unlike a syntax error) keeps decoding past the offending
// value, so the well-formed entries survive alongside corrupt=true — the
// warn text and Corrupt's contract describe a partial store, not an empty
// one.
func TestCorruptTypeErrorKeepsValidEntries(t *testing.T) {
	path := filepath.Join(t.TempDir(), "p.json")
	// journalCursor is mistyped; the logs section is well-formed.
	_ = os.WriteFile(path, []byte(`{"logs":{"/a.log":{"offset":42,"inode":9}},"journalCursor":7}`), 0o644)
	before := obs.PositionsCorrupt.Value()
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if !s.Corrupt() {
		t.Error("type error not reported as corrupt")
	}
	if got := obs.PositionsCorrupt.Value(); got != before+1 {
		t.Errorf("PositionsCorrupt = %v, want %v", got, before+1)
	}
	if lp := s.Logs()["/a.log"]; lp.Offset != 42 || lp.Inode != 9 {
		t.Errorf("well-formed entry lost on a type error: %+v", s.Logs())
	}
}

// An absent FILE is a first run; an absent (or non-directory) PARENT means
// every future save must fail — a mistyped -positions-file runs with no
// persistence at all, and callers only Warn on failed saves. Open refuses it
// so startup, where the error is fatal, is what reports it.
func TestOpenRefusesBadParentDir(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "no-such-dir", "p.json")
	_, err := Open(missing)
	if err == nil {
		t.Fatal("Open accepted a path whose parent directory does not exist")
	}
	for _, want := range []string{missing, filepath.Dir(missing)} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not name %q", err, want)
		}
	}

	// A parent that exists but is a regular file must be refused too (the
	// ENOTDIR from ReadFile takes the any-other-read-error return).
	occupied := filepath.Join(t.TempDir(), "occupied")
	if err := os.WriteFile(occupied, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(filepath.Join(occupied, "p.json")); err == nil {
		t.Fatal("Open accepted a path whose parent is a regular file")
	}
}

// A failed save is counted (kubescrape_positions_save_errors_total): callers
// only Warn and carry on, so the counter is the one signal that offsets are
// silently not being persisted.
func TestSaveFailuresAreCounted(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "sub")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "p.json")
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}

	// The directory vanishes underneath the store: CreateTemp must fail.
	if err := os.RemoveAll(dir); err != nil {
		t.Fatal(err)
	}
	before := obs.PositionsSaveErrors.Value()
	if err := s.SetJournalCursor("c1"); err == nil {
		t.Fatal("save into a removed directory succeeded")
	}
	if got := obs.PositionsSaveErrors.Value(); got != before+1 {
		t.Errorf("PositionsSaveErrors = %v, want %v", got, before+1)
	}

	// A successful save does not bump it.
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := s.SetJournalCursor("c2"); err != nil {
		t.Fatal(err)
	}
	if got := obs.PositionsSaveErrors.Value(); got != before+1 {
		t.Errorf("successful save bumped PositionsSaveErrors to %v", got)
	}
}

// A hard kill between CreateTemp and Rename leaves a "<base>.tmp-*" file
// nothing will ever rename or reuse; Open sweeps exactly that shape. Deleting
// a live concurrent writer's in-flight temp is safe — its rename simply
// fails, a save the design already treats as losable — but files of any other
// name must survive.
func TestOpenSweepsOrphanedTemps(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "p.json")
	if err := os.WriteFile(path, []byte(`{"journalCursor":"c"}`), 0o644); err != nil {
		t.Fatal(err)
	}
	orphans := []string{"p.json.tmp-123456", "p.json.tmp-"}
	keep := []string{"p.json.tmp", "p.json.bak", "other.json.tmp-99"}
	for _, name := range append(append([]string{}, orphans...), keep...) {
		if err := os.WriteFile(filepath.Join(dir, name), []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if s.JournalCursor() != "c" {
		t.Errorf("store not loaded: cursor = %q", s.JournalCursor())
	}
	for _, name := range orphans {
		if _, err := os.Stat(filepath.Join(dir, name)); !os.IsNotExist(err) {
			t.Errorf("orphan %s not removed (err=%v)", name, err)
		}
	}
	for _, name := range append(keep, "p.json") {
		if _, err := os.Stat(filepath.Join(dir, name)); err != nil {
			t.Errorf("unrelated file %s removed: %v", name, err)
		}
	}
}

// TestOpenReportsWhereItResumes pins the two startup lines an operator reads on
// a first live run. The FIRST-RUN line matters because of what silently follows
// from it: with no stored positions -logs-unknown-files=auto resolves to "end",
// so every log file already on the node is skipped to its end and its current
// contents are never exported — which is the intended behaviour and also the
// most common "the agent is collecting nothing" report. The RESUME line matters
// because a restart that silently began from zero (a wiped volume, a
// -positions-file pointing somewhere new) looks exactly like a healthy one
// until the duplicates arrive downstream.
func TestOpenReportsWhereItResumes(t *testing.T) {
	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo})))
	t.Cleanup(func() { slog.SetDefault(prev) })

	path := filepath.Join(t.TempDir(), "positions.json")
	s, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if !strings.Contains(buf.String(), "no positions file yet") {
		t.Fatalf("a first run did not say so; got:\n%s", buf.String())
	}

	if err := s.SetLogs(map[string]LogPos{"/var/log/containers/a.log": {Offset: 42}}); err != nil {
		t.Fatalf("SetLogs: %v", err)
	}
	buf.Reset()
	if _, err := Open(path); err != nil {
		t.Fatalf("reopen: %v", err)
	}
	out := buf.String()
	if !strings.Contains(out, "positions loaded") || !strings.Contains(out, "entries=1") {
		t.Fatalf("the resume line did not report what was restored; got:\n%s", out)
	}
	// The cursor is opaque and the paths are unbounded: neither belongs on a
	// startup line, so only their presence and count are reported.
	if strings.Contains(out, "/var/log/containers/a.log") {
		t.Fatalf("the resume line listed stored paths; it must report counts only. got:\n%s", out)
	}
}
