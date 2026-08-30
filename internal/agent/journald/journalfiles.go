package journald

import (
	"errors"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// The startup probe for "this reader has nothing to read".
//
// sd_journal_open succeeds against a journal holding NO journal files: the
// reader logs that it started, Next returns 0 forever, sd_journal_wait blocks,
// /readyz stays 200 and not one counter moves — an untouched counter is never
// exported at all, so the fleet-wide symptom of a missing mount is a complete
// absence of signal, indistinguishable from quiet nodes. That is what this
// probe exists to turn into one line. The shipped DaemonSet is exactly where it
// bites: /var/log carries the PERSISTENT journal, and a node whose journal is
// volatile (systemd Storage=volatile, or auto with no /var/log/journal) keeps
// it only under /run/log/journal, which is a separate hostPath.
//
// It WARNS rather than refusing, deliberately, and the asymmetry with the
// build-tag refusal (buildtags.go: -journald on a binary without the reader
// fails startup, naming the tag) is the point. That one is a fact about the
// BINARY — identical on every node, decidable before anything runs, so
// -check-config catches it. This one is a guess about the NODE, from outside
// libsystemd, about paths it may not be the only judge of; and a node
// legitimately running Storage=none has no journal files, where a refusal
// would CrashLoop the DaemonSet forever and take that node's logs, metrics and
// cadvisor pipelines down with the one pipeline that has nothing to do.

// defaultJournalRoots are the directories sd_journal_open reads when no
// directory is configured: the volatile journal and the persistent one. They
// live here so the warning names exactly what openJournal is about to read.
var defaultJournalRoots = []string{"/run/log/journal", "/var/log/journal"}

// journalRoots mirrors openJournal's choice: an explicit Dir is opened ALONE
// (sd_journal_open_directory), otherwise the system journal's two locations.
func journalRoots(dir string) []string {
	if dir != "" {
		return []string{dir}
	}
	return defaultJournalRoots
}

// isJournalFile matches what sd_journal reads: live `.journal` files and
// archived `.journal~` ones, whose entries a resumed cursor still needs.
func isJournalFile(name string) bool {
	return strings.HasSuffix(name, ".journal") || strings.HasSuffix(name, ".journal~")
}

// countJournalFiles counts the journal files under root — in root itself and
// one level down, the <machine-id>[.<namespace>] layout systemd writes and
// sd_journal searches. A subdirectory that cannot be listed contributes
// nothing: what this process cannot read, the libsystemd inside it cannot read
// either.
func countJournalFiles(root string) (int, error) {
	ents, err := os.ReadDir(root)
	if err != nil {
		return 0, err
	}
	n := 0
	for _, e := range ents {
		if !e.IsDir() {
			if isJournalFile(e.Name()) {
				n++
			}
			continue
		}
		sub, err := os.ReadDir(filepath.Join(root, e.Name()))
		if err != nil {
			continue
		}
		for _, s := range sub {
			if !s.IsDir() && isJournalFile(s.Name()) {
				n++
			}
		}
	}
	return n, nil
}

// probeJournalFiles reports how many journal files the configured journal
// holds, plus one phrase per root for the warning — the operator has to be told
// WHICH path was searched and why it yielded nothing, or the message sends them
// looking at the wrong one.
func probeJournalFiles(dir string) (found int, detail []string) {
	for _, root := range journalRoots(dir) {
		n, err := countJournalFiles(root)
		if err != nil {
			detail = append(detail, root+": "+rootProblem(err))
			continue
		}
		found += n
		detail = append(detail, root+": "+strconv.Itoa(n)+" journal files")
	}
	return found, detail
}

// rootProblem renders why a root yielded nothing. os.ReadDir's own text repeats
// the path, which the caller has already written.
func rootProblem(err error) string {
	switch {
	case errors.Is(err, fs.ErrNotExist):
		return "no such directory"
	case errors.Is(err, fs.ErrPermission):
		return "permission denied"
	}
	return err.Error()
}

// warnIfJournalEmpty logs the one signal there is when the journal about to be
// opened holds no journal files (see the block at the top of this file).
func warnIfJournalEmpty(cfg Config) {
	found, detail := probeJournalFiles(cfg.Dir)
	if found > 0 {
		return
	}
	// New fills Logger, but a nil one here would panic in the code path whose
	// whole job is to report a misconfiguration — and this package has no
	// recover() anywhere, by design.
	log := cfg.Logger
	if log == nil {
		log = slog.Default()
	}
	log.Warn("-journald is enabled but the journal holds no readable journal files: this reader will export nothing",
		"lookedIn", strings.Join(detail, "; "),
		"note", "mount the node's journal into the pod — a VOLATILE journal (systemd Storage=volatile, or auto with no /var/log/journal) lives only under /run/log/journal, which the /var/log mount does not cover; the Helm chart adds that hostPath when agent.journald.enabled is set, and deploy/agent.yaml carries it commented out beside the flag")
}
