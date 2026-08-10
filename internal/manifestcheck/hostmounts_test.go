package manifestcheck

import (
	"os"
	"path/filepath"
	"regexp"
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
var hostPathPipelines = []struct {
	flag     string // the bare flag name, no dashes
	hostPath string
	why      string
}{
	{
		flag:     "journald",
		hostPath: "/run/log/journal",
		why: "volatile journals (systemd Storage=volatile, or auto on a node with no /var/log/journal) live only under /run/log/journal, " +
			"which the /var/log mount does not cover; without it the reader starts, waits and exports nothing",
	},
}

// manifestDirs is Dirs resolved from THIS package. Dirs is spelled relative to
// a cmd/<binary> package and this one happens to sit at the same depth, but
// the strings are repeated rather than reused so that moving either package
// fails loudly in ReadDir below instead of quietly checking no manifests.
var manifestDirs = []string{"../../charts/kubescrape/templates", "../../deploy"}

// argLine matches an argument list entry whether it is live or commented out —
// `- -journald`, `#- -journald`, `# - --journald` — because a commented-out
// flag is how the raw manifests OFFER an opt-in pipeline, and this test is
// about what a manifest offers, not only about what it runs. (argPattern, the
// production one, deliberately sees live args only: a commented-out flag must
// not satisfy the "this flag still exists in the binary" check.)
var argLine = regexp.MustCompile(`(?m)^[ \t]*(#[ \t]*)?-[ \t]+--?([A-Za-z0-9][A-Za-z0-9-]*)`)

// mention records whether something appears in a manifest at all and whether
// any occurrence of it is live (not inside a YAML comment).
type mention struct{ seen, live bool }

func TestPerNodeAgentManifestsCarryTheirHostMounts(t *testing.T) {
	docs := agentDaemonSetDocs(t)
	// Both shipped install paths must be in there, or a discovery bug (a moved
	// directory, a renamed kind) would make every assertion below vacuous.
	if len(docs) < 2 {
		t.Fatalf("found %d per-node agent DaemonSet documents (%v); the chart's and deploy/'s are both expected", len(docs), keys(docs))
	}

	for _, p := range hostPathPipelines {
		var offering []string
		for path, doc := range docs {
			flag := flagMention(doc, p.flag)
			mount := pathMention(doc, p.hostPath)
			if flag.seen {
				offering = append(offering, path)
			}
			if flag.seen && !mount.seen {
				t.Errorf("%s offers -%s but never mentions %s: %s. Add the volume and mount (commented out beside the flag is fine)",
					path, p.flag, p.hostPath, p.why)
			}
			if flag.seen && mount.seen && flag.live != mount.live {
				t.Errorf("%s has -%s %s while %s is %s: %s",
					path, p.flag, liveness(flag.live), p.hostPath, liveness(mount.live), p.why)
			}
		}
		if len(offering) == 0 {
			t.Errorf("no per-node agent manifest offers -%s at all; if the pipeline was removed, drop it from hostPathPipelines", p.flag)
			continue
		}
		if len(offering) != len(docs) {
			t.Errorf("-%s is offered by %v but not by the other per-node agent manifests (%v): the chart and the raw manifests are two install paths for the SAME DaemonSet, "+
				"and the one that does not mention it leaves an operator adding the flag by hand with no mount — %s",
				p.flag, offering, missing(docs, offering), p.why)
		}
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
	for path, doc := range agentDaemonSetDocs(t) {
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

// agentDaemonSetDocs returns the DaemonSet documents running the agent binary,
// keyed by "<path>#<n>". DaemonSet because these are the PER-NODE pipelines: the
// singleton Deployment runs the same binary and reads no node-local path, so a
// journal mount there would be meaningless.
func agentDaemonSetDocs(t *testing.T) map[string]string {
	t.Helper()
	out := map[string]string{}
	for _, dir := range manifestDirs {
		entries, err := os.ReadDir(dir)
		if err != nil {
			t.Fatalf("reading %s: %v", dir, err)
		}
		for _, e := range entries {
			if e.IsDir() || filepath.Ext(e.Name()) != ".yaml" {
				continue
			}
			path := filepath.Join(dir, e.Name())
			b, err := os.ReadFile(path)
			if err != nil {
				t.Fatalf("reading %s: %v", path, err)
			}
			for _, doc := range docSeparator.Split(string(b), -1) {
				if !strings.Contains(doc, AgentCommand) || !isKind(doc, "DaemonSet") {
					continue
				}
				out[path] = doc
			}
		}
	}
	return out
}

// isKind matches the kind LINE exactly, so "kind: DaemonSetFoo" or a kind named
// in prose does not count. Text parsing, like the rest of this package: the
// chart file is a Helm TEMPLATE and does not parse as YAML, and the point is to
// read the source an operator edits rather than one rendering of it.
func isKind(doc, kind string) bool {
	for _, line := range strings.Split(doc, "\n") {
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
	for _, g := range argLine.FindAllStringSubmatch(doc, -1) {
		if g[2] != name {
			continue
		}
		m.seen = true
		if g[1] == "" {
			m.live = true
		}
	}
	return m
}

// pathMention reports whether the doc names this host path, live or in a
// comment.
func pathMention(doc, path string) mention {
	var m mention
	for _, line := range strings.Split(doc, "\n") {
		if !strings.Contains(line, path) {
			continue
		}
		m.seen = true
		if !strings.HasPrefix(strings.TrimSpace(line), "#") {
			m.live = true
		}
	}
	return m
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

// The guard is only worth having if it DISCRIMINATES, so this runs it over the
// two shapes it exists to reject: the manifest that offers the flag with no
// mount (deploy/agent.yaml as shipped before this test), and the half-commented
// one (the flag live, the mount left commented out — the same silent nothing,
// arrived at by an operator uncommenting one of the two).
func TestHostMountGuardCatchesAFlagWithoutItsMount(t *testing.T) {
	const head = "apiVersion: apps/v1\nkind: DaemonSet\nspec:\n  template:\n    spec:\n      containers:\n        - name: agent\n          command: [\"" + AgentCommand + "\"]\n          args:\n"

	for _, tc := range []struct {
		name, doc          string
		wantSeen, wantLive bool
		mountSeen          bool
	}{
		{
			name:     "flag offered, no mount anywhere",
			doc:      head + "            #- -journald\n",
			wantSeen: true,
		},
		{
			name:      "flag live, mount still commented out",
			doc:       head + "            - -journald\n          volumeMounts:\n            #- mountPath: /run/log/journal\n",
			wantSeen:  true,
			wantLive:  true,
			mountSeen: true,
		},
		{
			name:      "both live",
			doc:       head + "            - -journald\n          volumeMounts:\n            - mountPath: /run/log/journal\n",
			wantSeen:  true,
			wantLive:  true,
			mountSeen: true,
		},
		{
			name: "-journald-dir alone is a different flag and offers nothing",
			doc:  head + "            - -journald-dir=/run/log/journal\n",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if !isKind(tc.doc, "DaemonSet") || !strings.Contains(tc.doc, AgentCommand) {
				t.Fatal("fixture is not recognised as a per-node agent document")
			}
			flag := flagMention(tc.doc, "journald")
			if flag.seen != tc.wantSeen || flag.live != tc.wantLive {
				t.Errorf("flagMention = %+v, want seen=%v live=%v", flag, tc.wantSeen, tc.wantLive)
			}
			mount := pathMention(tc.doc, "/run/log/journal")
			if mount.seen != (tc.mountSeen || strings.Contains(tc.doc, "journald-dir")) {
				t.Errorf("pathMention = %+v, want seen=%v", mount, tc.mountSeen)
			}
			// The two states the shipped manifests must never be in.
			if tc.name == "flag live, mount still commented out" && flag.live == mount.live {
				t.Error("the half-commented shape read as consistent; the guard would not catch an operator uncommenting only the flag")
			}
		})
	}
}
