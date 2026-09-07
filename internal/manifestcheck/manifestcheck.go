// Package manifestcheck reads the shipped Kubernetes manifests (the Helm chart
// templates and deploy/) and extracts the command-line flags they pass to each
// binary, so a test in each binary's package can assert that every one of them
// exists in that binary's flag set.
//
// It exists because the chart is the documented install path and the copy
// nobody exercises: the kind-based E2E deploys deploy/*.yaml, so a template
// rendering a flag the code no longer defines reaches users as a fleet-wide
// CrashLoopBackOff — flag.Parse uses ExitOnError — with a single line of
// container log. That is how `-runtime-metrics` shipped after the flag was
// deleted from both binaries.
//
// The manifests are read as TEXT rather than rendered, so this needs no helm
// and no cluster: a flag NAME never comes from a template expression, only its
// value does.
package manifestcheck

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
)

// argPattern matches a rendered argument in a YAML args list:
//
//   - -log-level={{ .Values.logLevel }}
//   - --log-level={{ .Values.logLevel }}
//   - -journald
//
// The second dash is optional because Go's flag package accepts both spellings
// equally: a manifest written the GNU way must be checked, not skipped.
var argPattern = regexp.MustCompile(`(?m)^\s*-\s+--?([A-Za-z0-9][A-Za-z0-9-]*)`)

// docSeparator splits a multi-document YAML file. Classification is per
// DOCUMENT, not per file: a file holding both a service workload and an agent
// one would otherwise have every flag in it asserted against whichever binary
// the file as a whole was taken for — the exact bypass this package exists to
// prevent, on the manifests most likely to grow that way.
var docSeparator = regexp.MustCompile(`(?m)^---[ \t]*$`)

// AgentCommand identifies the manifests that run the agent: it is the binary
// the DaemonSet and the singleton Deployment override the image entrypoint
// with. Manifests without it run the metadata service.
const AgentCommand = "/kubescrape-agent"

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
	sort.Strings(out)
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
	paths, err := ManifestFiles(dirs...)
	if err != nil {
		return nil, err
	}
	out := map[string][]string{}
	for _, path := range paths {
		b, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("reading %s: %w", path, err)
		}
		var names []string
		for _, doc := range docSeparator.Split(string(b), -1) {
			// Only container specs carry flags; skip RBAC, Services, CRDs.
			if !strings.Contains(doc, "args:") && !strings.Contains(doc, "command:") {
				continue
			}
			if strings.Contains(doc, AgentCommand) != agent {
				continue
			}
			for _, m := range argPattern.FindAllStringSubmatch(doc, -1) {
				names = append(names, m[1])
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
