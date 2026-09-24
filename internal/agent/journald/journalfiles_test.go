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
	mid := filepath.Join(root, "3f2b0c9e1a2b4c5d3f2b0c9e1a2b4c5d")
	if err := os.MkdirAll(filepath.Join(mid, "deeper"), 0o755); err != nil {
		t.Fatal(err)
	}
	// The <machine-id> layout, live and archived. `.journal~` counts: a resumed
	// cursor still reads it.
	writeJournalFile(t, filepath.Join(mid, "system.journal"))
	writeJournalFile(t, filepath.Join(mid, "system@0005.journal~"))
	writeJournalFile(t, filepath.Join(mid, "README"))
	writeJournalFile(t, filepath.Join(mid, "deeper", "system.journal")) // two levels down: not searched

	// An explicit -journald-dir takes no gate (sd_journal_open_directory, no
	// flags), so every id-named subdirectory counts.
	n, refused, err := countJournalFiles(root, machineGate{})
	if err != nil {
		t.Fatal(err)
	}
	if n != 3 || refused != 0 {
		t.Errorf("countJournalFiles = %d found, %d refused; want 3/0 (the root's file plus the machine-id dir's live and archived ones)", n, refused)
	}
	if _, _, err := countJournalFiles(filepath.Join(root, "absent"), machineGate{}); err == nil {
		t.Error("a missing root must report its error; zero-with-no-reason is what the warning has to explain")
	}
}

// The gate is libsystemd's, and getting it wrong in either direction is bad:
// too loose and the probe stays quiet through the failure it exists to report,
// too strict and it warns on every healthy node.
func TestMachineGateMirrorsLocalOnly(t *testing.T) {
	const mid = "0123456789abcdef0123456789abcdef"
	other := "fedcba9876543210fedcba9876543210"
	for _, tc := range []struct {
		name  string
		gate  machineGate
		root  string
		dir   string
		allow bool
	}{
		{"explicit dir takes no gate", machineGate{}, "/x", other, true},
		{"volatile root is exempt", machineGate{on: true}, "/run/log/journal", other, true},
		{"persistent root, matching id", machineGate{on: true, id: mid}, "/var/log/journal", mid, true},
		{"persistent root, other node's id", machineGate{on: true, id: mid}, "/var/log/journal", other, false},
		{"persistent root, no machine id at all", machineGate{on: true}, "/var/log/journal", mid, false},
		// A namespaced directory is not the default namespace's, and the reader
		// asks for no namespace — under EVERY root, an explicit one included.
		{"persistent root, namespaced dir", machineGate{on: true, id: mid}, "/var/log/journal", mid + ".custom", false},
		{"volatile root, namespaced dir", machineGate{on: true}, "/run/log/journal", mid + ".custom", false},
		{"explicit dir, namespaced dir", machineGate{}, "/x", mid + ".custom", false},
		// Only 128-bit-id names are descended into at all (dirent_is_journal_subdir):
		// systemd-journal-remote's `remote/` is never read, whatever the root.
		{"explicit dir, non-id dir", machineGate{}, "/x", "remote", false},
		{"volatile root, non-id dir", machineGate{on: true}, "/run/log/journal", "remote", false},
		{"explicit dir, 16-hex dir", machineGate{}, "/x", "3f2b0c9e1a2b4c5d", false},
		// id128_is_valid takes either case and the dashed UUID form, and the
		// comparison is by the parsed id, not the spelling.
		{"persistent root, uppercase own id", machineGate{on: true, id: mid}, "/var/log/journal", strings.ToUpper(mid), true},
		{"persistent root, UUID-form own id", machineGate{on: true, id: mid}, "/var/log/journal",
			"01234567-89ab-cdef-0123-456789abcdef", true},
	} {
		if got := tc.gate.allows(tc.root, tc.dir); got != tc.allow {
			t.Errorf("%s: allows(%q, %q) = %v, want %v", tc.name, tc.root, tc.dir, got, tc.allow)
		}
	}
}

// The SHIPPED failure, and the one this probe could not see before: /var/log is
// mounted, the node's persistent journal is right there, and libsystemd reads
// none of it because the container has no /etc/machine-id. Counting those files
// as found is what kept the warning silent through a pipeline exporting zero
// records.
func TestPersistentJournalWithNoMachineIDWarnsAndNamesTheRemedy(t *testing.T) {
	root := t.TempDir()
	const nodeID = "0123456789abcdef0123456789abcdef"
	if err := os.MkdirAll(filepath.Join(root, nodeID), 0o755); err != nil {
		t.Fatal(err)
	}
	writeJournalFile(t, filepath.Join(root, nodeID, "system.journal"))

	// The default open's two roots, redirected at the fixture.
	defer swap(t, &defaultJournalRoots, []string{filepath.Join(root, "absent"), root})()

	// No /etc/machine-id: the shipped image has none.
	defer swap(t, &machineIDPath, filepath.Join(root, "no-such-machine-id"))()

	found, refused, detail := probeJournalFiles("")
	if found != 0 || refused != 1 {
		t.Fatalf("found=%d refused=%d; the one file under %s is present and unreadable, not found", found, refused, nodeID)
	}
	joined := strings.Join(detail, "; ")
	if !strings.Contains(joined, "machine-id directory sd_journal will not open") || !strings.Contains(joined, machineIDPath) {
		t.Errorf("the detail does not say what was refused or why: %q", joined)
	}

	var buf bytes.Buffer
	warnIfJournalUnreadable(Config{Logger: slog.New(slog.NewTextHandler(&buf, nil))})
	out := buf.String()
	if !strings.Contains(out, "level=WARN") {
		t.Fatalf("no warning for a journal libsystemd refuses to open: %q", out)
	}
	if !strings.Contains(out, "/etc/machine-id") || !strings.Contains(out, "journald-dir") {
		t.Errorf("the warning names the wrong remedy — a missing machine id is not a missing mount: %q", out)
	}

	// The PARTIAL shape: the volatile journal has a file the reader can see, so
	// records flow — and the persistent one, which is where the node keeps its
	// history, is still refused. Every counter looks healthy, so this too has to
	// produce a line.
	volatile := t.TempDir()
	writeJournalFile(t, filepath.Join(volatile, "system.journal"))
	restoreRoots := swap(t, &defaultJournalRoots, []string{volatile, root})
	buf.Reset()
	warnIfJournalUnreadable(Config{Logger: slog.New(slog.NewTextHandler(&buf, nil))})
	out = buf.String()
	if !strings.Contains(out, "level=WARN") || !strings.Contains(out, "refusing the rest") {
		t.Errorf("a partially readable journal produced no partial warning: %q", out)
	}
	restoreRoots()

	// Mount the node's machine-id and the same journal becomes readable, so the
	// warning must stop: a warning on healthy nodes is one nobody reads.
	idFile := filepath.Join(root, "machine-id")
	if err := os.WriteFile(idFile, []byte(nodeID+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	defer swap(t, &machineIDPath, idFile)()
	if found, refused, _ := probeJournalFiles(""); found != 1 || refused != 0 {
		t.Errorf("with the machine id mounted: found=%d refused=%d, want 1/0", found, refused)
	}
	buf.Reset()
	warnIfJournalUnreadable(Config{Logger: slog.New(slog.NewTextHandler(&buf, nil))})
	if buf.Len() != 0 {
		t.Errorf("warned about a journal it can read: %q", buf.String())
	}
}

// A CORRECTLY mounted node must stay silent, whatever else sits beside its
// journal. libsystemd never opens a `<id>.<ns>` directory (no namespace is
// requested) nor a non-id one such as systemd-journal-remote's `remote/`, and
// under LOCAL_ONLY it skips a FOREIGN id — the directory a node cloned from a
// template keeps from before its machine id was regenerated. None of them is
// fixable by mounting /etc/machine-id, and counting them as "refused" fired the
// partial warning on a healthy node, naming a remedy (mount the machine id) that
// was already applied and a fallback (-journald-dir) that would read the
// template's journal as this node's.
func TestForeignAndNamespacedJournalDirsDoNotDriveTheMachineIDRemedy(t *testing.T) {
	const nodeID = "0123456789abcdef0123456789abcdef"
	const oldID = "fedcba9876543210fedcba9876543210"
	root := t.TempDir()
	for _, dir := range []string{nodeID, oldID, nodeID + ".audit", "remote"} {
		if err := os.MkdirAll(filepath.Join(root, dir), 0o755); err != nil {
			t.Fatal(err)
		}
		writeJournalFile(t, filepath.Join(root, dir, "system.journal"))
	}
	idFile := filepath.Join(t.TempDir(), "machine-id")
	if err := os.WriteFile(idFile, []byte(nodeID+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	defer swap(t, &machineIDPath, idFile)()
	defer swap(t, &defaultJournalRoots, []string{filepath.Join(root, "absent"), root})()

	if found, refused, detail := probeJournalFiles(""); found != 1 || refused != 0 {
		t.Errorf("gated: found=%d refused=%d (%v), want 1/0 — only %s/ is read, and nothing else is the machine id's fault",
			found, refused, detail, nodeID)
	}
	var buf bytes.Buffer
	warnIfJournalUnreadable(Config{Logger: slog.New(slog.NewTextHandler(&buf, nil))})
	if buf.Len() != 0 {
		t.Errorf("warned on a correctly mounted node: %q", buf.String())
	}

	// The volatile root is exempt from the machine-id rule but not from the
	// name rules — the allows() table pins that per directory, and the name
	// check in countJournalFiles is the same for every root, which the explicit
	// directory below exercises.

	// And with an explicit -journald-dir: no gate, the same name rules. Only the
	// two bare ids are read (the foreign one too — the directory was named
	// outright), so the namespaced and non-id files must not reach `found` and
	// hide a "nothing readable" warning they cannot justify.
	if n, ref, err := countJournalFiles(root, machineGate{}); err != nil || n != 2 || ref != 0 {
		t.Errorf("explicit dir: found=%d refused=%d err=%v, want 2/0 (the two bare-id directories)", n, ref, err)
	}
	onlyUnread := t.TempDir()
	for _, dir := range []string{nodeID + ".audit", "remote"} {
		if err := os.MkdirAll(filepath.Join(onlyUnread, dir), 0o755); err != nil {
			t.Fatal(err)
		}
		writeJournalFile(t, filepath.Join(onlyUnread, dir, "system.journal"))
	}
	buf.Reset()
	warnIfJournalUnreadable(Config{Dir: onlyUnread, Logger: slog.New(slog.NewTextHandler(&buf, nil))})
	if !strings.Contains(buf.String(), "export nothing") {
		t.Errorf("an explicit dir holding only directories libsystemd never opens was reported readable: %q", buf.String())
	}

	// The machine id alone is not proof the node's directory is there: with
	// NO directory named after it, the other ids ARE what the gate refuses, and
	// the remedy is the right one.
	noOwn := t.TempDir()
	if err := os.MkdirAll(filepath.Join(noOwn, oldID), 0o755); err != nil {
		t.Fatal(err)
	}
	writeJournalFile(t, filepath.Join(noOwn, oldID, "system.journal"))
	if n, ref, err := countJournalFiles(noOwn, machineGate{on: true, id: nodeID}); err != nil || n != 0 || ref != 1 {
		t.Errorf("no directory for the mounted id: found=%d refused=%d err=%v, want 0/1", n, ref, err)
	}
	// ...and it still WARNS: a readable id that names no directory (one baked
	// into an image, or a machine-id mounted from the wrong source) is the
	// silent failure this probe exists for, so silencing foreign ids must not
	// key on the id being readable alone.
	defer swap(t, &defaultJournalRoots, []string{noOwn})()
	buf.Reset()
	warnIfJournalUnreadable(Config{Logger: slog.New(slog.NewTextHandler(&buf, nil))})
	if out := buf.String(); !strings.Contains(out, "none of them named "+nodeID) || !strings.Contains(out, "machine-id") {
		t.Errorf("a mounted machine id matching no journal directory did not warn with the machine-id remedy: %q", out)
	}
}

// swap sets *p to v and returns a restore func.
func swap[T any](t *testing.T, p *T, v T) func() {
	t.Helper()
	old := *p
	*p = v
	return func() { *p = old }
}

func TestProbeReportsEveryRoot(t *testing.T) {
	root := t.TempDir()
	found, _, detail := probeJournalFiles(root)
	if found != 0 || len(detail) != 1 || !strings.Contains(detail[0], "0 journal files") {
		t.Errorf("empty directory: found=%d detail=%v", found, detail)
	}
	writeJournalFile(t, filepath.Join(root, "system.journal"))
	if found, _, _ := probeJournalFiles(root); found != 1 {
		t.Errorf("found=%d after writing one journal file", found)
	}
	_, _, detail = probeJournalFiles(filepath.Join(root, "gone"))
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

	warnIfJournalUnreadable(Config{Dir: root, Logger: log})
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
	warnIfJournalUnreadable(Config{Dir: root, Logger: log})
	if buf.Len() != 0 {
		t.Errorf("warned about a journal that has files — a warning on healthy nodes is a warning nobody reads: %q", buf.String())
	}
}

// New fills Logger, but this path runs to report a misconfiguration and must
// not become one: there is no recover() in this binary.
func TestWarnWithNoLoggerDoesNotPanic(t *testing.T) {
	warnIfJournalUnreadable(Config{Dir: t.TempDir()})
}

func writeJournalFile(t *testing.T, path string) {
	t.Helper()
	if err := os.WriteFile(path, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
}
