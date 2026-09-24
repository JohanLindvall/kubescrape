// Package manifestcheck reads the shipped Kubernetes manifests (the Helm chart
// templates and deploy/) and extracts the command-line flags they pass to each
// binary, so a test in each binary's package can assert that every one of them
// exists in that binary's flag set.
//
// It exists because the chart is the documented install path and the copy
// nobody exercises: the kind-based E2E deploys deploy/kubernetes.yaml and
// deploy/agent.yaml (and nothing deploys the other two), so a template
// rendering a flag the code no longer defines reaches users as a fleet-wide
// CrashLoopBackOff — flag.Parse uses ExitOnError — with a single line of
// container log. That is how `-runtime-metrics` shipped after the flag was
// deleted from both binaries.
//
// The manifests are read as TEXT rather than rendered, so this needs no helm
// and no cluster: a flag NAME never comes from a template expression, only its
// value does. A block of flags a template takes from a _helpers.tpl define is
// spliced in where it is included before anything is read (helpers.go).
package manifestcheck

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
)

// argEntry matches an argument list entry, live or commented out:
//
//	args:
//	  - -log-level={{ .Values.logLevel }}
//	  - --log-level={{ .Values.logLevel }}
//	  - "-log-level={{ .Values.logLevel }}"
//	  - '--journald'
//	  - -journald
//	  #- -journald
//	  # - --journald
//
// Group 1 is the comment marker (empty on a LIVE entry), group 2 the opening
// quote, group 3 the flag name and group 4 the text after its `=` (empty when
// there is none; ArgEntry trims it and drops the closing quote). The second
// dash is optional because Go's flag package accepts both spellings equally: a
// manifest written the GNU way must be checked, not skipped. The opening QUOTE
// is optional for the same reason: YAML strips it before the container sees the
// argument, quoting a list item is a common Helm idiom, and a flag passed that
// way is exactly as able to CrashLoop the pod as a bare one.
//
// ONE pattern serves every reader, so a spelling one of them learns the others
// cannot miss: Flags takes the live entries only (a commented-out flag is not
// passed, and must never satisfy the "this flag still exists" check on behalf
// of a live one), OfferedFlags the commented ones, the host-mount guard beside
// this package's tests reads both, and internal/chartcheck's values-path scan
// reads the live ones' values line by line through ArgEntry.
var argEntry = regexp.MustCompile(`(?m)^[ \t]*(#[ \t]*)?-[ \t]+(["']?)--?([A-Za-z0-9][A-Za-z0-9-]*)(?:=(.*))?`)

// ArgEntry parses ONE line as an argument list entry, in argEntry's grammar:
// the flag's name, the text after its `=` (empty when there is none), and
// whether the entry is commented out. A quoted list item's closing quote is
// YAML's, not the value's, and is removed, so `- "-a={{ .Values.a }}"` yields
// the value `{{ .Values.a }}`. ok is false for a line that is no argument
// entry.
func ArgEntry(line string) (name, value string, commented, ok bool) {
	m := argEntry.FindStringSubmatch(line)
	if m == nil {
		return "", "", false, false
	}
	value = strings.TrimSpace(m[4])
	if q := m[2]; q != "" {
		value = strings.TrimSpace(strings.TrimSuffix(value, q))
	}
	return m[3], value, m[1] != "", true
}

// docSeparator is the YAML document separator line: `---` alone in column 0,
// trailing blanks allowed.
var docSeparator = regexp.MustCompile(`(?m)^---[ \t]*$`)

// Documents splits a multi-document YAML stream — a manifest file, or helm's
// rendered output — at its separator lines. It is the one splitter every guard
// over the manifests uses. An element may be empty (the text before a leading
// `---`) or carry the newlines around a separator; a YAML decode reads either
// as an empty or unchanged document.
//
// Classification is per DOCUMENT, not per file: a file holding both a service
// workload and an agent one would otherwise have every flag in it asserted
// against whichever binary the file as a whole was taken for — the exact
// bypass this package exists to prevent, on the manifests most likely to grow
// that way.
func Documents(s string) []string { return docSeparator.Split(s, -1) }

// AgentCommand identifies the manifests that run the agent: it is the binary
// the DaemonSet and the singleton Deployment override the image entrypoint
// with. Manifests without it run the metadata service.
const AgentCommand = "/kubescrape-agent"

// agentBinary matches AgentCommand as a WHOLE path, never as the tail of a
// longer one. A substring test also matched the agent's state directory,
// /var/lib/kubescrape-agent, so a service manifest that ever mounted or named
// it would have been asserted against the AGENT's flag set — and failed on the
// service-only flags every service manifest passes, a red build pointing at
// the wrong binary.
var agentBinary = regexp.MustCompile(`(?:^|[^A-Za-z0-9_./-])` + regexp.QuoteMeta(AgentCommand) + `(?:$|[^A-Za-z0-9_./-])`)

// isAgentDoc reports whether one manifest document runs the agent binary. The
// document should already be stripped of template comments
// (stripTemplateComments): prose there is never rendered, so it must not
// classify anything.
func isAgentDoc(doc string) bool { return agentBinary.MatchString(doc) }

// templateComment matches a Go-template comment, `{{/* ... */}}` or
// `{{- /* ... */ -}}`, across lines.
var templateComment = regexp.MustCompile(`\{\{-?\s*/\*[\s\S]*?\*/\s*-?\}\}`)

// stripTemplateComments blanks every Go-template comment in a chart template,
// keeping its NEWLINES so every later line keeps its line number.
//
// helm never renders a template comment, so nothing in one is live — but the
// text scans in this package (and the host-mount guard beside them) read the
// template SOURCE, where a comment line does not start with `#` and therefore
// looked live: prose explaining a mount satisfied the mount check, a comment
// naming the agent binary could classify a document, and an arg-shaped line in
// one would be read as a passed flag.
func stripTemplateComments(src string) string {
	return templateComment.ReplaceAllStringFunc(src, func(c string) string {
		return strings.Repeat("\n", strings.Count(c, "\n"))
	})
}

// IsManifest reports whether a file name is one of the shipped manifests.
//
// BOTH YAML extensions, because helm and kubectl both accept both: a template
// renamed .yml would otherwise have every flag it passes silently excluded from
// the assertion, and the vacuity guard in each binary's test is whole-corpus
// (it only asks whether ANY manifest was found), so it cannot see one file drop
// out.
func IsManifest(name string) bool {
	switch filepath.Ext(name) {
	case ".yaml", ".yml":
		return true
	}
	return false
}

// ManifestFiles walks dirs and returns every manifest under them, in a stable
// order. It is the ONE listing every guard over the shipped manifests uses —
// this package's flag check, its host-mount check, and internal/chartcheck's
// scans — because a guard that reads a SUBSET of the corpus reports "pass" for
// a file it never opened, which is worse than no guard.
//
// WALKED rather than listed, because helm renders a template in a subdirectory
// of templates/ exactly like a flat one (verified against the pinned helm:
// `helm template` on templates/sub/deep.yaml emits it). A flat os.ReadDir meant
// that grouping templates into templates/rbac/ dropped them all out of the
// check silently.
func ManifestFiles(dirs ...string) ([]string, error) {
	var out []string
	for _, dir := range dirs {
		err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if !d.IsDir() && IsManifest(d.Name()) {
				out = append(out, path)
			}
			return nil
		})
		if err != nil {
			return nil, fmt.Errorf("reading %s: %w", dir, err)
		}
	}
	// WalkDir is lexical per directory; sorting the whole result makes the
	// order independent of how the dirs were split up.
	slices.Sort(out)
	return out, nil
}

// Flags returns the flag names each manifest under dirs passes to a binary,
// keyed by manifest path. When agent is true only the manifests running the
// agent are considered, otherwise only those running the metadata service.
//
// The tree is WALKED, not listed. helm renders a template in a SUBDIRECTORY of
// templates/ exactly like a flat one (verified against the pinned helm), so a
// flat os.ReadDir meant that moving one template into templates/rbac/ — or
// adding a chart of nested templates — dropped every flag in it out of the
// check with nothing to say so, on the one artefact this package exists to
// guard: a flag the binary no longer defines is a fleet-wide CrashLoopBackOff
// with one line of container log.
func Flags(dirs []string, agent bool) (map[string][]string, error) {
	return scanArgs(dirs, agent, false)
}

// OfferedFlags returns the flag names each manifest under dirs OFFERS to a
// binary: the commented-out list entries (`#- -journald`) that are how the raw
// manifests in deploy/ carry an opt-in pipeline, keyed and classified exactly
// as Flags keys and classifies the passed ones.
//
// It is separate from Flags so an offer still never counts as passed. What a
// caller asserts of it is EXISTENCE only: uncommenting the line is the
// documented way to enable the pipeline, so an offer naming a flag the binary
// no longer defines is a CrashLoop waiting for the operator who follows the
// docs — `flag provided but not defined`, exit 2, on every rolled pod — while
// every test that reads only live args stays green through the rename. And
// existence is safe to assert for an offer, because the optional-pipeline
// flags are registered on every build variant (only ENABLING one a build
// lacks fails, from validateConfig).
func OfferedFlags(dirs []string, agent bool) (map[string][]string, error) {
	return scanArgs(dirs, agent, true)
}

// scanArgs is Flags and OfferedFlags: the one walk, the one per-DOCUMENT
// classification and the one argument pattern, selecting the commented-out
// entries when offered is true and the live ones otherwise.
func scanArgs(dirs []string, agent, offered bool) (map[string][]string, error) {
	paths, err := ManifestFiles(dirs...)
	if err != nil {
		return nil, err
	}
	helpers, err := Helpers(dirs...)
	if err != nil {
		return nil, err
	}
	out := map[string][]string{}
	for _, path := range paths {
		b, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("reading %s: %w", path, err)
		}
		// Comments blanked and included helpers spliced in (helpers.go), so a
		// flag a helper passes is read, and classified, where helm renders it.
		lines, err := ExpandIncludes(path, string(b), helpers)
		if err != nil {
			return nil, err
		}
		var names []string
		for _, doc := range Documents(joinLines(lines)) {
			// Only container specs carry flags; skip RBAC, Services, CRDs.
			if !strings.Contains(doc, "args:") && !strings.Contains(doc, "command:") {
				continue
			}
			if isAgentDoc(doc) != agent {
				continue
			}
			for _, m := range argEntry.FindAllStringSubmatch(doc, -1) {
				if commented := m[1] != ""; commented == offered {
					names = append(names, m[3])
				}
			}
		}
		if len(names) > 0 {
			out[path] = names
		}
	}
	return out, nil
}

// Dirs are the shipped manifest directories, relative to a cmd/<binary>
// package (where the tests that use this live).
var Dirs = []string{"../../charts/kubescrape/templates", "../../deploy"}

// RenderedDirs are the chart's golden RENDERS, one per value fixture, relative
// to a cmd/<binary> package. The source scan above reads the templates line by
// line, so an argument a template emits from a range loop or a computed define
// is invisible to it; the render is what the pod actually receives. The
// goldens are only as current as internal/chartcheck's TestChartGolden keeps
// them, which CI enforces.
var RenderedDirs = []string{"../../internal/chartcheck/testdata/golden"}
