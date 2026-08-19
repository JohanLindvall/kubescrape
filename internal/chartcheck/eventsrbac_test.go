package chartcheck

import (
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"sigs.k8s.io/yaml"
)

// The singleton's Role used to grant get/create/update on ALL ConfigMaps and
// ALL Leases in the release namespace, while the code names exactly one of
// each: events.ConfigMapStore only ever asks for -events-position-configmap and
// leader.Run only for -events-lease (a single-named resourcelock.LeaseLock).
// The gap is WRITE-ONLY lateral movement — an unscoped `update` reaches the
// <release>-agent-config ConfigMap this same chart renders into that same
// namespace, so rewriting its `export:`/`routing:` redirects every agent's logs
// on their next start (and instantly, if the operator also mounted a
// hot-reloaded -transforms-file ConfigMap there).
//
// The golden files pin the rendered bytes and so would notice the rules
// changing, but they cannot say WHICH shape is correct — a future edit that
// widens the grant back regenerates the golden and stays green. This test pins
// the intent instead, and pins it as a RELATION rather than a spelling: the
// resourceNames on the scoped rules must equal the names the SAME manifest
// passes to the flags. A name changed in one place and not the other is a pod
// that starts and then cannot read its own position, which is exactly the
// failure a hardcoded expectation here would not catch.
//
// `create` is deliberately exempt: RBAC ignores resourceNames on create (the
// object has no name until the request is admitted), and create alone cannot
// touch an object that already exists, so leaving it unscoped costs nothing.
var scopedEventsResources = []struct {
	resource   string // the RBAC resource whose get/update must be name-scoped
	flag       string // the flag naming the ONE object the code touches
	defaultVal string // that flag's default, for a manifest that does not pass it
}{
	// Defaults mirrored from cmd/kubescrape-agent/main.go, which deploy/events.yaml
	// relies on by not passing either flag. charts/kubescrape/values.yaml carries
	// the same two strings; the chart render below asserts the chart's own copies
	// agree with its RBAC, so only deploy/ is checked against these.
	{resource: "configmaps", flag: "events-position-configmap", defaultVal: "kubescrape-events-position"},
	{resource: "leases", flag: "events-lease", defaultVal: "kubescrape-cluster-leader"},
}

// rbacRule is the subset of a PolicyRule this test judges.
type rbacRule struct {
	Resources     []string `json:"resources"`
	Verbs         []string `json:"verbs"`
	ResourceNames []string `json:"resourceNames"`
}

type rbacDoc struct {
	Kind  string     `json:"kind"`
	Rules []rbacRule `json:"rules"`
}

// docSep splits a multi-document YAML stream, the way internal/manifestcheck
// does — no dependency on a streaming decoder for three documents.
var docSep = regexp.MustCompile(`(?m)^---[ \t]*$`)

// eventsFlagValue returns the value the manifest passes to -<flag>, or def when
// it passes the flag nowhere. Both dash spellings are accepted because Go's
// flag package accepts both.
func eventsFlagValue(manifest, flag, def string) string {
	re := regexp.MustCompile(`(?m)^\s*-\s+--?` + regexp.QuoteMeta(flag) + `=(\S+)\s*$`)
	if m := re.FindStringSubmatch(manifest); m != nil {
		return m[1]
	}
	return def
}

// assertEventsRBACIsScoped is the whole invariant, applied to one rendered
// manifest stream: for each of the two singleton-owned objects, every Role rule
// granting a verb other than `create` on that resource must carry exactly the
// resourceNames the manifest's own flags name.
func assertEventsRBACIsScoped(t *testing.T, where, manifest string) {
	t.Helper()
	checked := 0
	for _, doc := range docSep.Split(manifest, -1) {
		var d rbacDoc
		if err := yaml.Unmarshal([]byte(doc), &d); err != nil {
			continue // not a Kubernetes object (helm's leading comment block, say)
		}
		if d.Kind != "Role" && d.Kind != "ClusterRole" {
			continue
		}
		for _, rule := range d.Rules {
			for _, want := range scopedEventsResources {
				if !contains(rule.Resources, want.resource) {
					continue
				}
				beyondCreate := false
				for _, v := range rule.Verbs {
					if v != "create" {
						beyondCreate = true
					}
				}
				if !beyondCreate {
					continue // the unscoped create rule, which is the correct shape
				}
				checked++
				name := eventsFlagValue(manifest, want.flag, want.defaultVal)
				if len(rule.ResourceNames) != 1 || rule.ResourceNames[0] != name {
					t.Errorf("%s: %s rule with verbs %v on %q has resourceNames %v, want exactly [%q] — "+
						"the code names one %s (-%s) and nothing else, and an unscoped write reaches every "+
						"other ConfigMap/Lease in the namespace, the agent-config ConfigMap included",
						where, d.Kind, rule.Verbs, want.resource, rule.ResourceNames, name, want.resource, want.flag)
				}
			}
		}
	}
	// Zero rules checked means the scan stopped matching — a green test that
	// asserts nothing, which is how the unscoped grant survived the goldens.
	if checked != len(scopedEventsResources) {
		t.Errorf("%s: checked %d name-scoped rules, want %d (one per resource in scopedEventsResources); "+
			"the Role's shape has changed and this guard no longer sees it", where, checked, len(scopedEventsResources))
	}
}

func contains(hay []string, needle string) bool {
	for _, s := range hay {
		if s == needle {
			return true
		}
	}
	return false
}

// TestEventsRoleScopesWritesToTheObjectsTheFlagsName covers the CHART half,
// rendered with NON-DEFAULT names so a hardcoded resourceNames would fail: the
// scope has to follow the values that render the flags, not a constant.
func TestEventsRoleScopesWritesToTheObjectsTheFlagsName(t *testing.T) {
	helm := helmBin(t)
	out, err := exec.Command(helm, "template", "kubescrape", "../../charts/kubescrape",
		"--namespace", "monitoring",
		"--set", "events.enabled=true",
		"--set", "events.leaseName=custom-leader-lease",
		"--set", "events.positionConfigMap=custom-position-cm",
	).CombinedOutput()
	if err != nil {
		t.Fatalf("helm template failed: %v\n%s", err, out)
	}
	assertEventsRBACIsScoped(t, "rendered chart (events.enabled)", string(out))
}

// TestDeployEventsRoleScopesWritesLikeTheChart covers the OTHER install path.
// deploy/events.yaml is the copy the docs tell you to `kubectl apply -f` and
// the one no rendering test would otherwise reach; it passes neither flag, so
// the expectation falls back to the binary defaults.
func TestDeployEventsRoleScopesWritesLikeTheChart(t *testing.T) {
	t.Parallel()
	path := filepath.Join("..", "..", "deploy", "events.yaml")
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading %s: %v", path, err)
	}
	assertEventsRBACIsScoped(t, path, string(b))
	// The two install paths must agree on the names as well as the shape: the
	// chart's values and the binary's flag defaults are separate copies of the
	// same two strings, and deploy/ silently relies on them being equal.
	values, err := os.ReadFile(filepath.Join("..", "..", "charts", "kubescrape", "values.yaml"))
	if err != nil {
		t.Fatalf("reading values.yaml: %v", err)
	}
	for _, want := range scopedEventsResources {
		if !strings.Contains(string(values), ": "+want.defaultVal) {
			t.Errorf("charts/kubescrape/values.yaml no longer defaults to %q, which deploy/events.yaml's "+
				"resourceNames assume for -%s; the two install paths have drifted", want.defaultVal, want.flag)
		}
	}
}
