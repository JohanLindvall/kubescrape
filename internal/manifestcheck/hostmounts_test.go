package manifestcheck

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
)

// Flags say what to RUN; a host mount says whether it can read anything, and
// -journald is where the two came apart. The reader opens a journal that holds
// no journal files perfectly happily: it logs "journald reader started", blocks
// in sd_journal_wait, answers /readyz 200 and exports zero records — and moves
// no counter either, because an untouched counter is never exported at all. So
// the failure is INVISIBLE unless the manifest hands the pod the journal.
//
// charts/kubescrape/templates/agent.yaml mounts /run/log/journal behind
// agent.journald.enabled. deploy/agent.yaml — the copy the docs tell you to
// `kubectl apply -f` — contained the string "journal" ZERO times, so the two
// shipped install paths disagreed about whether the pipeline was even offered,
// and on the raw one an operator who added the flag by hand (the only way to
// enable it there) got the silent-nothing above on every node with a volatile
// journal.
//
// The rule pinned here is therefore parity plus co-occurrence, not a hardcoded
// spelling of one manifest: a host-path pipeline offered by ANY per-node agent
// manifest must be offered by ALL of them, and within each manifest the flag
// and its mount must be in the SAME state. Commented out is a perfectly good
// state — that is how deploy/ carries every other opt-in surface (see the
// azure block in deploy/events.yaml). HALF commented is the bug.
//
// A MOUNT is two entries and both are checked: the hostPath VOLUME naming the
// node path, and a volumeMount naming that volume. Only those lines count. The
// guard used to count any line containing the path, and both manifests explain
// their mounts in prose — `#` comments in deploy/, `{{/* */}}` template
// comments in the chart — so deleting the real volume and mount left the prose
// standing in for them and the guard green, on deploy/ in particular, which no
// rendered test covers.
//
// A flag may need more than one host path, one row each.
type hostPathPipeline struct {
	flag     string // the bare flag name, no dashes
	hostPath string
	why      string
}

var hostPathPipelines = []hostPathPipeline{
	{
		flag:     "journald",
		hostPath: "/run/log/journal",
		why: "volatile journals (systemd Storage=volatile, or auto on a node with no /var/log/journal) live only under /run/log/journal, " +
			"which the /var/log mount does not cover; without it the reader starts, waits and exports nothing",
	},
	{
		flag:     "journald",
		hostPath: "/etc/machine-id",
		why: "a persistent journal (/var/log/journal/<id>, the Debian/Ubuntu/RHEL default) is opened under SD_JOURNAL_LOCAL_ONLY only when <id> " +
			"is the container's own machine id, and the image ships none; without it the reader starts, waits and exports nothing",
	},
	{
		flag:     "cgroup-stats",
		hostPath: "/sys/fs/cgroup",
		why: "the agent's own /sys/fs/cgroup shows only its OWN cgroup (a container gets a cgroup namespace), so without the host's hierarchy " +
			"bind-mounted in, discovery finds no pod cgroups and the sampler exports nothing on every node",
	},
}

// manifestDirs is Dirs resolved from THIS package. Dirs is spelled relative to
// a cmd/<binary> package and this one happens to sit at the same depth, but
// the strings are repeated rather than reused so that moving either package
// fails loudly in ManifestFiles below (WalkDir returns the missing root's
// lstat error, which ManifestFiles propagates) instead of quietly checking no
// manifests.
var manifestDirs = []string{"../../charts/kubescrape/templates", "../../deploy"}

// mention records whether something appears in a manifest at all and whether
// any occurrence of it is live (not inside a YAML comment).
type mention struct{ seen, live bool }

// hostMountProblems is the guard's verdict on ONE per-node agent document for
// one pipeline: whether the document offers the flag at all, and every way the
// flag and its host mount disagree. Both the real-manifest test and the
// discrimination test below go through it, so the fixtures exercise the same
// decision the shipped manifests get.
func hostMountProblems(doc string, p hostPathPipeline) (offered bool, problems []string) {
	flag := flagMention(doc, p.flag)
	if !flag.seen {
		return false, nil
	}
	vol, mnt := hostMount(doc, p.hostPath)
	for _, part := range []struct {
		what string
		m    mention
	}{
		{"a hostPath volume with path " + p.hostPath, vol},
		{"a volumeMount of that volume", mnt},
	} {
		switch {
		case !part.m.seen:
			problems = append(problems, fmt.Sprintf("offers -%s but has no %s: %s. Add the volume and its mount (commented out beside the flag is fine)",
				p.flag, part.what, p.why))
		case part.m.live != flag.live:
			problems = append(problems, fmt.Sprintf("has -%s %s while %s is %s: %s",
				p.flag, liveness(flag.live), part.what, liveness(part.m.live), p.why))
		}
	}
	return true, problems
}

// hostMountViolations is the guard's WHOLE verdict over a set of per-node
// agent documents (keyed as agentDaemonSetDocs keys them): every per-document
// problem hostMountProblems finds, plus the two cross-document rules — a
// pipeline no document offers at all, and a pipeline some documents offer and
// others do not. The real-manifest test reports what this returns and the
// discrimination tests below feed it fixtures, so the parity rule is exercised
// by a test that can fail rather than only by the shipped tree, where it
// happens to hold.
func hostMountViolations(docs map[string]string, pipelines []hostPathPipeline) []string {
	paths := keys(docs)
	sort.Strings(paths)
	var out []string
	for _, p := range pipelines {
		var offering []string
		for _, path := range paths {
			offered, problems := hostMountProblems(docs[path], p)
			if offered {
				offering = append(offering, path)
			}
			for _, problem := range problems {
				out = append(out, path+" "+problem)
			}
		}
		if len(offering) == 0 {
			out = append(out, fmt.Sprintf("no per-node agent manifest offers -%s at all; if the pipeline was removed, drop it from hostPathPipelines", p.flag))
			continue
		}
		if len(offering) != len(docs) {
			out = append(out, fmt.Sprintf("-%s is offered by %v but not by the other per-node agent manifests (%v): the chart and the raw manifests are two install paths for the SAME DaemonSet, "+
				"and the one that does not mention it leaves an operator adding the flag by hand with no mount — %s",
				p.flag, offering, missing(docs, offering), p.why))
		}
	}
	return out
}

func TestPerNodeAgentManifestsCarryTheirHostMounts(t *testing.T) {
	docs := agentDaemonSetDocs(t, manifestDirs...)
	// Both shipped install paths must be in there, or a discovery bug (a moved
	// directory, a renamed kind) would make every assertion below vacuous.
	// DOCUMENTS, not files: agentDaemonSetDocs keys one entry per document.
	if len(docs) < 2 {
		t.Fatalf("found %d per-node agent DaemonSet documents (%v); the chart's and deploy/'s are both expected", len(docs), keys(docs))
	}
	for _, v := range hostMountViolations(docs, hostPathPipelines) {
		t.Error(v)
	}
}

// An OFFER must stay invisible to the production extractor. Flags asserts that
// every flag a manifest PASSES exists in that binary's flag set, and a
// commented-out flag is not passed: extracting it would assert a manifest
// against a pipeline it does not run — and, in the other direction, let a
// commented sibling stand in for a live flag. This is what makes "offer it
// commented out" a safe house pattern rather than a way around the guard.
func TestACommentedOfferIsNotReadAsAPassedFlag(t *testing.T) {
	byFile, err := Flags(manifestDirs, true)
	if err != nil {
		t.Fatal(err)
	}
	if len(byFile) == 0 {
		t.Fatal("Flags found no agent manifests; the assertions below would be vacuous")
	}
	for key, doc := range agentDaemonSetDocs(t, manifestDirs...) {
		path, _, _ := strings.Cut(key, "#")
		for _, p := range hostPathPipelines {
			m := flagMention(doc, p.flag)
			if !m.seen || m.live {
				continue
			}
			for _, name := range byFile[path] {
				if name == p.flag {
					t.Errorf("%s only OFFERS -%s (commented out) but Flags reports it as passed", path, p.flag)
				}
			}
		}
	}
}

// agentDaemonSetDocs returns the DaemonSet documents under dirs running the
// agent binary, keyed by "<path>#<n>" (n the document's index in its file, so
// two DaemonSets in one file are two entries rather than the second
// overwriting the first), with Go-template comments stripped: helm never
// renders them, so their prose must not stand in for a mount or a flag.
// DaemonSet because these are the PER-NODE pipelines: the singleton Deployment
// runs the same binary and reads no node-local path, so a journal mount there
// would be meaningless.
func agentDaemonSetDocs(t *testing.T, dirs ...string) map[string]string {
	t.Helper()
	out := map[string]string{}
	// The SAME walk Flags uses: a subdirectory template or a .yml one is part
	// of the shipped corpus, and a host-mount check that never opened it is a
	// green tick over a file nobody read.
	paths, err := ManifestFiles(dirs...)
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range paths {
		b, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("reading %s: %v", path, err)
		}
		for i, doc := range Documents(stripTemplateComments(string(b))) {
			if !isAgentDoc(doc) || !isKind(doc, "DaemonSet") {
				continue
			}
			out[fmt.Sprintf("%s#%d", path, i)] = doc
		}
	}
	return out
}

// isKind matches the kind LINE exactly, so "kind: DaemonSetFoo" or a kind named
// in prose does not count. Text parsing, like the rest of this package: the
// chart file is a Helm TEMPLATE and does not parse as YAML, and the point is to
// read the source an operator edits rather than one rendering of it.
func isKind(doc, kind string) bool {
	for line := range strings.SplitSeq(doc, "\n") {
		if strings.TrimSpace(line) == "kind: "+kind {
			return true
		}
	}
	return false
}

// flagMention reports whether the doc names exactly this flag in an args list.
// The name is compared whole: `-journald-dir` is a different flag and must not
// stand in for `-journald`.
func flagMention(doc, name string) mention {
	var m mention
	for _, g := range argEntry.FindAllStringSubmatch(doc, -1) {
		if g[3] != name {
			continue
		}
		m.seen = true
		if g[1] == "" {
			m.live = true
		}
	}
	return m
}

// nameEntry matches a list entry's `- name: X` line, live or commented out: it
// opens a volume and a volumeMount alike, so it is what ties a mount to the
// volume it mounts.
var nameEntry = regexp.MustCompile(`^[ \t]*(?:#[ \t]*)?-[ \t]+name:[ \t]*(\S+)[ \t]*$`)

// mountEntry matches a volume's `path:` line or a volumeMount's `mountPath:`
// line, live or commented out, with an optional list marker.
var mountEntry = regexp.MustCompile(`^[ \t]*(#[ \t]*)?(?:-[ \t]+)?(mountPath|path):[ \t]*(.*?)[ \t]*$`)

// hostMount reports the hostPath VOLUME whose path is exactly hostPath and the
// volumeMount of that volume (matched by the name of the list entry each line
// sits in), each live or commented out. Only those two kinds of line count:
// prose that merely names the path is neither.
//
// The mount is found through the volume's NAME rather than by its own path,
// because the mount point is not the host path — deploy/ and the chart mount
// the cgroup hierarchy at /host/sys/fs/cgroup, and the chart spells even that
// as a template expression.
func hostMount(doc, hostPath string) (volume, mount mention) {
	volumes := map[string]bool{}
	type mountLine struct {
		name string
		live bool
	}
	var mounts []mountLine
	name := ""
	for line := range strings.SplitSeq(doc, "\n") {
		if m := nameEntry.FindStringSubmatch(line); m != nil {
			name = m[1]
			continue
		}
		m := mountEntry.FindStringSubmatch(line)
		if m == nil {
			continue
		}
		live := m[1] == ""
		switch m[2] {
		case "path":
			if strings.Trim(m[3], `"'`) == hostPath {
				volume.seen = true
				volume.live = volume.live || live
				volumes[name] = true
			}
		case "mountPath":
			mounts = append(mounts, mountLine{name, live})
		}
	}
	for _, ml := range mounts {
		if volumes[ml.name] {
			mount.seen = true
			mount.live = mount.live || ml.live
		}
	}
	return volume, mount
}

func liveness(live bool) string {
	if live {
		return "enabled"
	}
	return "commented out"
}

func keys(m map[string]string) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

func missing(all map[string]string, have []string) []string {
	got := map[string]bool{}
	for _, h := range have {
		got[h] = true
	}
	var out []string
	for k := range all {
		if !got[k] {
			out = append(out, k)
		}
	}
	return out
}

// The guard is only worth having if it DISCRIMINATES, so this runs its real
// per-document verdict over the shapes it exists to reject: the manifest that
// offers the flag with no mount (deploy/agent.yaml as shipped before this
// test), the half-commented one (the flag live, the mount left commented out —
// the same silent nothing, arrived at by an operator uncommenting one of the
// two), a volume kept with its mount deleted, and — the reason the matcher
// reads volume and mount LINES only — a manifest whose only trace of the path
// is the prose explaining it, in a YAML comment or a template comment.
// The fixture pieces the discrimination tests assemble per-node agent
// DaemonSet documents from: a head ending in an open args list, and the
// /run/log/journal volume and mount, each live or commented out.
const (
	head           = "apiVersion: apps/v1\nkind: DaemonSet\nspec:\n  template:\n    spec:\n      containers:\n        - name: agent\n          command: [\"" + AgentCommand + "\"]\n          args:\n"
	liveMount      = "          volumeMounts:\n            - name: runlogjournal\n              mountPath: /run/log/journal\n"
	commentedMount = "          volumeMounts:\n            #- name: runlogjournal\n            #  mountPath: /run/log/journal\n"
	liveVolume     = "      volumes:\n        - name: runlogjournal\n          hostPath:\n            path: /run/log/journal\n"
	commentedVol   = "      volumes:\n        #- name: runlogjournal\n        #  hostPath:\n        #    path: /run/log/journal\n"
)

// journal is the one-row pipeline the fixtures are judged against.
var journal = hostPathPipeline{flag: "journald", hostPath: "/run/log/journal", why: "test"}

func TestHostMountGuardCatchesAFlagWithoutItsMount(t *testing.T) {
	for _, tc := range []struct {
		name, doc    string
		wantOffered  bool
		wantProblems int
	}{
		{
			name:         "flag offered, no mount anywhere",
			doc:          head + "            #- -journald\n",
			wantOffered:  true,
			wantProblems: 2,
		},
		{
			name:         "flag live, mount and volume still commented out",
			doc:          head + "            - -journald\n" + commentedMount + commentedVol,
			wantOffered:  true,
			wantProblems: 2,
		},
		{
			name:        "both commented out: a clean offer",
			doc:         head + "            #- -journald\n" + commentedMount + commentedVol,
			wantOffered: true,
		},
		{
			name:        "all live",
			doc:         head + "            - -journald\n" + liveMount + liveVolume,
			wantOffered: true,
		},
		{
			name:         "volume kept, its mount deleted",
			doc:          head + "            - -journald\n" + liveVolume,
			wantOffered:  true,
			wantProblems: 1,
		},
		{
			name:         "a mount of a DIFFERENT volume does not count",
			doc:          head + "            - -journald\n          volumeMounts:\n            - name: varlog\n              mountPath: /run/log/journal\n" + liveVolume,
			wantOffered:  true,
			wantProblems: 1,
		},
		{
			name: "prose alone is not a mount",
			doc: head + "            #- -journald\n" +
				"            # A volatile journal lives only under /run/log/journal, which\n" +
				"            # the mountPath: below would cover if it were still here.\n",
			wantOffered:  true,
			wantProblems: 2,
		},
		{
			name: "a template comment is not a mount either",
			// Unstripped, these lines would read as a LIVE volume and mount.
			doc: stripTemplateComments(head + "            - -journald\n" +
				"            {{- /* what the enabled branch renders:\n" +
				"            - name: runlogjournal\n" +
				"              mountPath: /run/log/journal\n" +
				"        - name: runlogjournal\n" +
				"          hostPath:\n" +
				"            path: /run/log/journal\n" +
				"            */}}\n"),
			wantOffered:  true,
			wantProblems: 2,
		},
		{
			name: "-journald-dir alone is a different flag and offers nothing",
			doc:  head + "            - -journald-dir=/run/log/journal\n",
		},
		{
			// YAML strips the quotes, so this RUNS the pipeline exactly as a
			// bare `- -journald` does; skipping it passed a manifest that reads
			// a journal it was never handed.
			name:         "a quoted flag is a flag",
			doc:          head + "            - \"-journald\"\n",
			wantOffered:  true,
			wantProblems: 2,
		},
		{
			name:         "a single-quoted GNU-spelled flag is a flag",
			doc:          head + "            - '--journald'\n",
			wantOffered:  true,
			wantProblems: 2,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if !isKind(tc.doc, "DaemonSet") || !isAgentDoc(tc.doc) {
				t.Fatal("fixture is not recognised as a per-node agent document")
			}
			offered, problems := hostMountProblems(tc.doc, journal)
			if offered != tc.wantOffered || len(problems) != tc.wantProblems {
				t.Errorf("offered=%v problems=%d %q, want offered=%v problems=%d", offered, len(problems), problems, tc.wantOffered, tc.wantProblems)
			}
		})
	}
}

// Deleting the real volume and mount from deploy/agent.yaml must turn the guard
// red. This is the shipped manifest with exactly those lines removed — the
// regression the guard exists for — and every path deploy/ explains in prose
// (all three of them) is still named by that prose afterwards.
func TestHostMountGuardFiresOnTheShippedManifestWithItsMountsDeleted(t *testing.T) {
	var deploy string
	for key, doc := range agentDaemonSetDocs(t, manifestDirs...) {
		if strings.Contains(key, "deploy") {
			deploy = doc
		}
	}
	if deploy == "" {
		t.Fatal("deploy/agent.yaml's DaemonSet was not found")
	}
	for _, p := range hostPathPipelines {
		if _, problems := hostMountProblems(deploy, p); len(problems) != 0 {
			t.Fatalf("fixture: the shipped manifest already fails for %s: %v", p.hostPath, problems)
		}
		var kept []string
		for line := range strings.SplitSeq(deploy, "\n") {
			if m := mountEntry.FindStringSubmatch(line); m != nil && strings.Contains(m[3], strings.TrimPrefix(p.hostPath, "/")) {
				continue // the volume's path: line and the mount's mountPath: line
			}
			kept = append(kept, line)
		}
		stripped := strings.Join(kept, "\n")
		if !strings.Contains(stripped, p.hostPath) {
			t.Fatalf("fixture: no prose names %s once its entries are gone, so this does not reproduce the vacuous pass", p.hostPath)
		}
		if _, problems := hostMountProblems(stripped, p); len(problems) == 0 {
			t.Errorf("deploy/agent.yaml with every volume and mount line for %s deleted still passes the host-mount guard", p.hostPath)
		}
	}
}

// The cross-document half of the guard, which the shipped tree cannot exercise
// negatively because it holds: a pipeline offered by SOME per-node agent
// manifests and not others is the deploy/agent.yaml-without-journald shape this
// file was written for, and a pipeline NO manifest offers is a stale row in
// hostPathPipelines. Each must be a violation, and a clean offer on every
// document — live or commented out — must not be.
func TestHostMountGuardRequiresParityAcrossAgentManifests(t *testing.T) {
	var (
		cleanOffer = head + "            #- -journald\n" + commentedMount + commentedVol
		liveOffer  = head + "            - -journald\n" + liveMount + liveVolume
		noOffer    = head + "            - -logs\n"
	)
	for _, tc := range []struct {
		name string
		docs map[string]string
		want int
	}{
		{"every document offers it, commented out", map[string]string{"chart#0": cleanOffer, "deploy#0": cleanOffer}, 0},
		{"one live, one commented out", map[string]string{"chart#0": liveOffer, "deploy#0": cleanOffer}, 0},
		{"only one install path offers it", map[string]string{"chart#0": liveOffer, "deploy#0": noOffer}, 1},
		{"no install path offers it", map[string]string{"chart#0": noOffer, "deploy#0": noOffer}, 1},
		{
			// Both rules fire together: the offering document is itself
			// broken (flag, no mount) AND the other does not offer it.
			"the one offer is also missing its mount",
			map[string]string{"chart#0": head + "            - -journald\n", "deploy#0": noOffer},
			3,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := hostMountViolations(tc.docs, []hostPathPipeline{journal}); len(got) != tc.want {
				t.Errorf("hostMountViolations = %d %q, want %d", len(got), got, tc.want)
			}
		})
	}
}

// Two agent DaemonSets in ONE file are two documents to check. Keyed by path,
// the second overwrote the first, so the first — here the one passing -journald
// with no mount — was never judged, and the document-count floor in the
// real-tree test counted files and could not notice.
func TestTwoAgentDaemonSetsInOneFileAreBothChecked(t *testing.T) {
	dir := t.TempDir()
	broken := head + "            - -journald\n"
	clean := head + "            #- -journald\n" + commentedMount + commentedVol
	if err := os.WriteFile(filepath.Join(dir, "two.yaml"), []byte(broken+"---\n"+clean), 0o600); err != nil {
		t.Fatal(err)
	}
	docs := agentDaemonSetDocs(t, dir)
	if len(docs) != 2 {
		t.Fatalf("agentDaemonSetDocs found %d documents %v in a file holding two agent DaemonSets", len(docs), keys(docs))
	}
	if got := hostMountViolations(docs, []hostPathPipeline{journal}); len(got) != 2 {
		t.Errorf("hostMountViolations = %d %q, want the first document's 2 problems (no volume, no mount)", len(got), got)
	}
}
