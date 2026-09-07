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
// bites, in TWO different ways: /var/log carries the PERSISTENT journal, and a
// node whose journal is volatile (systemd Storage=volatile, or auto with no
// /var/log/journal) keeps it only under /run/log/journal, which is a separate
// hostPath — while a node whose journal IS persistent needs the container's own
// /etc/machine-id as well, or libsystemd refuses every directory under
// /var/log/journal without a word (machineGate, below). The two have different
// remedies, so the warning names them apart.
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

// machineIDPath is where sd_id128_get_machine() reads this process's own
// machine id. A var so the probe's tests can point it at a fixture.
var machineIDPath = "/etc/machine-id"

// readMachineID returns the container's own machine id in the 32-hex form
// systemd names journal directories with, or "" when there is none to read.
// Absent is the interesting case and it is the SHIPPED one: the agent image
// carries no /etc/machine-id, so unless the node's is mounted in,
// sd_id128_get_machine() fails and every /var/log/journal/<id> directory is
// refused (see machineGate).
func readMachineID() string {
	b, err := os.ReadFile(machineIDPath)
	if err != nil {
		return ""
	}
	id := strings.TrimSpace(string(b))
	if len(id) != 32 {
		return ""
	}
	for i := 0; i < len(id); i++ {
		if c := id[i]; (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return ""
		}
	}
	return id
}

// machineGate is libsystemd's LOCAL_ONLY rule for a root's SUBDIRECTORIES, and
// it is the reason this probe cannot just count files.
//
// go-systemd's sd_journal_open passes SD_JOURNAL_LOCAL_ONLY, under which
// sd-journal's add_directory() considers a subdirectory local — and therefore
// opens it — only when the root lives under /run or the subdirectory's name is
// this process's OWN machine id. The persistent journal is /var/log/journal/
// <machine-id>/, and it is the default on Debian, Ubuntu and RHEL; so an agent
// with the node's /var/log mounted but no /etc/machine-id of its own reads
// exactly nothing from it, silently, while the volatile /run/log/journal path
// (which the rule exempts) works. Counting the files libsystemd is refusing to
// open is what let this probe stay quiet through precisely that failure.
//
// An explicit -journald-dir does NOT take the gate: sd_journal_open_directory
// is called with no flags, so it reads every subdirectory whatever it is named.
type machineGate struct {
	on bool   // SD_JOURNAL_LOCAL_ONLY applies (i.e. no explicit Dir)
	id string // this container's machine id; "" when none is readable
}

// allows reports whether sd_journal will open root/name.
func (g machineGate) allows(root, name string) bool {
	if !g.on || root == "/run" || strings.HasPrefix(root, "/run/") {
		return true
	}
	return g.id != "" && name == g.id
}

// countJournalFiles counts the journal files under root — in root itself and
// one level down, the <machine-id>[.<namespace>] layout systemd writes and
// sd_journal searches. A subdirectory that cannot be listed contributes
// nothing: what this process cannot read, the libsystemd inside it cannot read
// either. `refused` counts the files in subdirectories the gate above rules
// out, which are NOT part of `found`: they exist, and the reader will never see
// one, which is a different remedy from a missing mount and has to be said so.
func countJournalFiles(root string, gate machineGate) (found, refused int, err error) {
	ents, err := os.ReadDir(root)
	if err != nil {
		return 0, 0, err
	}
	for _, e := range ents {
		if !e.IsDir() {
			if isJournalFile(e.Name()) {
				found++
			}
			continue
		}
		sub, err := os.ReadDir(filepath.Join(root, e.Name()))
		if err != nil {
			continue
		}
		n := 0
		for _, s := range sub {
			if !s.IsDir() && isJournalFile(s.Name()) {
				n++
			}
		}
		if gate.allows(root, e.Name()) {
			found += n
		} else {
			refused += n
		}
	}
	return found, refused, nil
}

// probeJournalFiles reports how many journal files the configured journal
// holds, plus one phrase per root for the warning — the operator has to be told
// WHICH path was searched and why it yielded nothing, or the message sends them
// looking at the wrong one. `refused` is the count libsystemd's LOCAL_ONLY rule
// rules out (machineGate), which selects the remedy the warning names.
func probeJournalFiles(dir string) (found, refused int, detail []string) {
	gate := machineGate{on: dir == "", id: readMachineID()}
	for _, root := range journalRoots(dir) {
		n, ref, err := countJournalFiles(root, gate)
		if err != nil {
			detail = append(detail, root+": "+rootProblem(err))
			continue
		}
		found += n
		refused += ref
		d := root + ": " + strconv.Itoa(n) + " journal files"
		if ref > 0 {
			d += " (" + strconv.Itoa(ref) + " more in a machine-id directory sd_journal will not open"
			if gate.id == "" {
				d += ", this container having no readable " + machineIDPath
			} else {
				d += ", none of them named " + gate.id
			}
			d += ")"
		}
		detail = append(detail, d)
	}
	return found, refused, detail
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

// warnIfJournalUnreadable logs the one signal there is when the journal about to
// be opened holds nothing the reader can see (see the block at the top of this
// file). It fires in TWO shapes, and the second is why it is not called
// warnIfJournalEmpty any more: NOTHING readable, and something readable beside
// journal files libsystemd is refusing. The second is a partial, and a partial
// is worth a line precisely because the pipeline looks healthy — records flow,
// every counter moves, and what is missing is the half a node keeps its
// persistent history in.
//
// It stays silent on the shape a correctly mounted agent has (files found,
// none refused), which is the property that makes it worth reading at all.
func warnIfJournalUnreadable(cfg Config) {
	found, refused, detail := probeJournalFiles(cfg.Dir)
	if found > 0 && refused == 0 {
		return
	}
	// New fills Logger, but a nil one here would panic in the code path whose
	// whole job is to report a misconfiguration — and this package has no
	// recover() anywhere, by design.
	log := cfg.Logger
	if log == nil {
		log = slog.Default()
	}
	msg := "-journald is enabled but the journal holds no readable journal files: this reader will export nothing"
	if found > 0 {
		// systemd writes to ONE of the two locations, so this is normally a
		// node whose live journal is the persistent one and whose readable
		// /run/log/journal holds only pre-mount early-boot entries: the reader
		// runs, exports, and covers almost nothing.
		msg = "-journald is reading part of this node's journal and libsystemd is refusing the rest: records will flow and most of the node's history will be missing, with every counter looking healthy"
	}
	log.Warn(msg,
		"lookedIn", strings.Join(detail, "; "),
		"note", emptyJournalNote(refused))
}

// emptyJournalNote names the remedy, and the two are not alike: journal files
// that are THERE and refused is a missing /etc/machine-id, which no amount of
// mounting the journal itself fixes, while no files at all is a missing mount.
func emptyJournalNote(refused int) string {
	if refused > 0 {
		return "the journal IS mounted and libsystemd is refusing it: under SD_JOURNAL_LOCAL_ONLY a /var/log/journal/<id> directory is opened only when <id> is this container's OWN machine id, so mount the node's /etc/machine-id read-only at /etc/machine-id (the Helm chart does when agent.journald.enabled is set, and deploy/agent.yaml carries it commented out beside the flag) — or set -journald-dir=/var/log/journal, which opens the directory outright at the cost of the volatile journal"
	}
	return "mount the node's journal into the pod — a VOLATILE journal (systemd Storage=volatile, or auto with no /var/log/journal) lives only under /run/log/journal, which the /var/log mount does not cover; the Helm chart adds that hostPath when agent.journald.enabled is set, and deploy/agent.yaml carries it commented out beside the flag"
}
