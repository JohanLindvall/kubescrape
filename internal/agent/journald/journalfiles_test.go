package journald

import (
	"bytes"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

// The probe has to name the same directories openJournal opens, or its verdict
// describes a journal nobody reads.
func TestJournalRootsMirrorOpenJournal(t *testing.T) {
	if got := journalRoots(""); !slices.Equal(got, []string{"/run/log/journal", "/var/log/journal"}) {
		t.Errorf("journalRoots(\"\") = %v; sd_journal_open reads the volatile journal AND the persistent one", got)
	}
	if got := journalRoots("/x/y"); !slices.Equal(got, []string{"/x/y"}) {
		t.Errorf("journalRoots(%q) = %v; an explicit -journald-dir is opened alone (sd_journal_open_directory)", "/x/y", got)
	}
}

func TestCountJournalFiles(t *testing.T) {
	root := t.TempDir()
	writeJournalFile(t, filepath.Join(root, "system.journal"))
	writeJournalFile(t, filepath.Join(root, "notes.txt"))
	mid := filepath.Join(root, "3f2b0c9e1a2b4c5d")
	if err := os.MkdirAll(filepath.Join(mid, "deeper"), 0o755); err != nil {
		t.Fatal(err)
	}
	// The <machine-id> layout, live and archived. `.journal~` counts: a resumed
	// cursor still reads it.
	writeJournalFile(t, filepath.Join(mid, "system.journal"))
	writeJournalFile(t, filepath.Join(mid, "system@0005.journal~"))
	writeJournalFile(t, filepath.Join(mid, "README"))
	writeJournalFile(t, filepath.Join(mid, "deeper", "system.journal")) // two levels down: not searched

	n, err := countJournalFiles(root)
	if err != nil {
		t.Fatal(err)
	}
	if n != 3 {
		t.Errorf("countJournalFiles = %d, want 3 (the root's file plus the machine-id dir's live and archived ones)", n)
	}
	if _, err := countJournalFiles(filepath.Join(root, "absent")); err == nil {
		t.Error("a missing root must report its error; zero-with-no-reason is what the warning has to explain")
	}
}

func TestProbeReportsEveryRoot(t *testing.T) {
	root := t.TempDir()
	found, detail := probeJournalFiles(root)
	if found != 0 || len(detail) != 1 || !strings.Contains(detail[0], "0 journal files") {
		t.Errorf("empty directory: found=%d detail=%v", found, detail)
	}
	writeJournalFile(t, filepath.Join(root, "system.journal"))
	if found, _ := probeJournalFiles(root); found != 1 {
		t.Errorf("found=%d after writing one journal file", found)
	}
	_, detail = probeJournalFiles(filepath.Join(root, "gone"))
	if len(detail) != 1 || !strings.Contains(detail[0], "no such directory") {
		t.Errorf("missing directory detail = %v; the message has to name which path is missing and why", detail)
	}
}

// The measured failure: an agent with the shipped /var/log-only volume set and
// -journald on a node with a volatile journal starts cleanly, logs "journald
// reader started", answers /readyz 200 and exports zero records — with no
// counter moving, because an untouched counter is never exported. This warning
// is the only signal that exists, so it must fire exactly then, name the
// directory it searched and name the mount that is missing.
func TestWarnsOnlyWhenTheJournalHoldsNothing(t *testing.T) {
	root := t.TempDir()
	var buf bytes.Buffer
	log := slog.New(slog.NewTextHandler(&buf, nil))

	warnIfJournalEmpty(Config{Dir: root, Logger: log})
	out := buf.String()
	if !strings.Contains(out, "level=WARN") || !strings.Contains(out, "export nothing") {
		t.Fatalf("a journal with no files produced no warning: %q", out)
	}
	if !strings.Contains(out, root) {
		t.Errorf("the warning does not name the resolved directory: %q", out)
	}
	if !strings.Contains(out, "/run/log/journal") {
		t.Errorf("the warning does not name the mount that is most likely missing: %q", out)
	}

	buf.Reset()
	writeJournalFile(t, filepath.Join(root, "system.journal"))
	warnIfJournalEmpty(Config{Dir: root, Logger: log})
	if buf.Len() != 0 {
		t.Errorf("warned about a journal that has files — a warning on healthy nodes is a warning nobody reads: %q", buf.String())
	}
}

// New fills Logger, but this path runs to report a misconfiguration and must
// not become one: there is no recover() in this binary.
func TestWarnWithNoLoggerDoesNotPanic(t *testing.T) {
	warnIfJournalEmpty(Config{Dir: t.TempDir()})
}

func writeJournalFile(t *testing.T, path string) {
	t.Helper()
	if err := os.WriteFile(path, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
}
