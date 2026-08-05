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
	"os"
	"path/filepath"
	"regexp"
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

// Flags returns the flag names each manifest under dirs passes to a binary,
// keyed by manifest path. When agent is true only the manifests running the
// agent are considered, otherwise only those running the metadata service.
func Flags(dirs []string, agent bool) (map[string][]string, error) {
	out := map[string][]string{}
	for _, dir := range dirs {
		entries, err := os.ReadDir(dir)
		if err != nil {
			return nil, fmt.Errorf("reading %s: %w", dir, err)
		}
		for _, e := range entries {
			if e.IsDir() || filepath.Ext(e.Name()) != ".yaml" {
				continue
			}
			path := filepath.Join(dir, e.Name())
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
	}
	return out, nil
}

// Dirs are the shipped manifest directories, relative to a cmd/<binary>
// package (where the tests that use this live).
var Dirs = []string{"../../charts/kubescrape/templates", "../../deploy"}
