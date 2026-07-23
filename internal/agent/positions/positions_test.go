package positions

import (
	"os"
	"path/filepath"
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
