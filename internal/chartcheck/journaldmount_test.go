package chartcheck

import (
	"regexp"
	"strings"
	"testing"
)

// machineIDHostPath matches the hostPath volume and its type in one go.
var machineIDHostPath = regexp.MustCompile(`(?m)^\s*path: /etc/machine-id\n\s*type: File\s*$`)

// A node has ONE of the two journal layouts and the chart cannot tell which, so
// `agent.journald.enabled` has to cover both — and the persistent half needs
// something that reads like an unrelated file.
//
// go-systemd opens the journal with SD_JOURNAL_LOCAL_ONLY, under which
// libsystemd's add_directory() reads a /var/log/journal/<id> directory only
// when the root is under /run or <id> equals the CONTAINER's own machine id
// (sd_id128_get_machine(), i.e. /etc/machine-id). The kubescrape image ships no
// /etc/machine-id, so before this mount an agent on a node with a PERSISTENT
// journal — the Debian/Ubuntu/RHEL default — opened the journal successfully,
// blocked on entries that could never arrive, answered /readyz 200 and exported
// zero records with no counter moving. /run/log/journal is exempt from the rule,
// which is why the volatile case worked, and why kind (whose journal is
// volatile) means hack/e2e.sh could never have caught it.
//
// The golden pins the current rendering; this pins the RULE, so a regeneration
// cannot quietly drop either mount again. It also pins `type: File`: an EMPTY
// /etc/machine-id fails sd_id128_get_machine() exactly as an absent one does, so
// FileOrCreate would hand the agent a mount that looks right and reads nothing.
func TestJournaldMountsCoverBothJournalLayouts(t *testing.T) {
	helm := helmBin(t)

	off, err := helmTemplate(helm, "monitoring")
	if err != nil {
		t.Fatalf("helm template failed: %v\n%s", err, off)
	}
	if doc := agentDaemonSet(t, string(off)); strings.Contains(doc, "machine-id") {
		t.Error("the machine-id mount renders with agent.journald.enabled unset; it exists for the journal reader and nothing else")
	}

	on, err := helmTemplate(helm, "monitoring", "--set", "agent.journald.enabled=true")
	if err != nil {
		t.Fatalf("helm template with journald failed: %v\n%s", err, on)
	}
	doc := agentDaemonSet(t, string(on))

	for _, want := range []string{
		// The volatile journal's own directory: nothing under /var/log carries it.
		"mountPath: /run/log/journal",
		"path: /run/log/journal",
		// The persistent journal's gate. Without it /var/log/journal is
		// mounted and unreadable, silently.
		"mountPath: /etc/machine-id",
		"path: /etc/machine-id",
	} {
		if !strings.Contains(doc, want) {
			t.Errorf("agent.journald.enabled did not render %q — a node with the journal layout it covers "+
				"would start the reader, report ready and export nothing, with no counter moving", want)
		}
	}
	// /var/log is mounted unconditionally and is what carries the persistent
	// journal; the machine id is the half that was missing.
	if !strings.Contains(doc, "mountPath: /var/log") {
		t.Error("the /var/log mount is gone; it is what carries the persistent journal")
	}
	// `type: File` and not FileOrCreate. Matched as a line-anchored pair: the
	// DaemonSet document can end on this very line, so a trailing-newline match
	// would be a test that passes for the wrong reason the moment a volume is
	// appended after it.
	if !machineIDHostPath.MatchString(doc) {
		t.Error("the machine-id hostPath does not declare `type: File` — an empty or fabricated " +
			"/etc/machine-id fails sd_id128_get_machine() exactly as an absent one does, so a " +
			"FileOrCreate here hides the very failure the mount exists to fix")
	}
}
