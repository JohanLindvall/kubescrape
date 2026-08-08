package owners

import (
	"os"
	"strings"
	"testing"
)

// The RBAC guard for AllGVRs. Main wires one metadata informer per entry, and a
// resource the ClusterRole does not grant 403-loops its initial LIST: that
// informer's HasSynced never flips, WaitForCacheSync never returns, and /readyz
// answers 503 for the process' life. The list is spelled in Go here and again
// in TWO shipped ClusterRoles, and nothing related them — manifestcheck extracts
// FLAGS and skips RBAC documents by construction, chartcheck's goldens are
// regenerated from the chart itself, and hack/e2e.sh applies only deploy/*.yaml,
// so a new owner kind added by the (previously incomplete) instruction in
// owners.go shipped a Helm chart whose Deployment never becomes Ready, with no
// test failing.
var clusterRoleManifests = []string{
	"../../deploy/kubernetes.yaml",
	"../../charts/kubescrape/templates/service.yaml",
}

func TestClusterRolesGrantEveryOwnerGVR(t *testing.T) {
	for _, path := range clusterRoleManifests {
		granted := clusterRoleResources(t, path)
		if len(granted) == 0 {
			t.Fatalf("%s: no ClusterRole rules found — the parser or the manifest moved", path)
		}
		for _, gvr := range AllGVRs {
			if !granted[gvr.Resource] {
				t.Errorf("owners.AllGVRs gained %q: add it to the ClusterRole in %s", gvr.Resource, path)
			}
		}
	}
}

// clusterRoleResources collects every resource named by a `resources: [...]`
// line of a ClusterRole document. Text parsing, like internal/manifestcheck:
// the chart file is a Helm TEMPLATE and does not parse as YAML, and the point
// is to read the source an operator edits, not a rendering of it. Commented-out
// rules are skipped, so an optional rule left commented cannot satisfy the
// check.
func clusterRoleResources(t *testing.T, path string) map[string]bool {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading %s: %v", path, err)
	}
	out := map[string]bool{}
	for _, doc := range strings.Split(string(b), "\n---") {
		if !isClusterRole(doc) {
			continue
		}
		for _, line := range strings.Split(doc, "\n") {
			line = strings.TrimSpace(line)
			if strings.HasPrefix(line, "#") {
				continue
			}
			line = strings.TrimPrefix(line, "- ")
			rest, ok := strings.CutPrefix(line, "resources:")
			if !ok {
				continue
			}
			for _, item := range strings.Split(strings.Trim(strings.TrimSpace(rest), "[]"), ",") {
				if name := strings.Trim(strings.TrimSpace(item), `"'`); name != "" {
					out[name] = true
				}
			}
		}
	}
	return out
}

// isClusterRole matches the kind LINE exactly: "kind: ClusterRoleBinding"
// contains "kind: ClusterRole" as a prefix and grants nothing.
func isClusterRole(doc string) bool {
	for _, line := range strings.Split(doc, "\n") {
		if strings.TrimSpace(line) == "kind: ClusterRole" {
			return true
		}
	}
	return false
}
