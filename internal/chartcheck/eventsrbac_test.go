package chartcheck

import (
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"

	"sigs.k8s.io/yaml"

	"github.com/JohanLindvall/kubescrape/internal/manifestcheck"
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
//
// A manifest that passes neither flag — deploy/events.yaml — relies on the
// binary's DEFAULTS, and those are read from the generated docs/FLAGS.md
// (flagDefaultFromDocs) rather than copied here: a hand copy is exactly the
// hardcoded expectation the paragraph above rules out, and it stayed green
// when a default changed in main.go while deploy/'s Role went on naming the
// old object.
var scopedEventsResources = []struct {
	resource string // the RBAC resource whose get/update must be name-scoped
	flag     string // the flag naming the ONE object the code touches
}{
	{resource: "configmaps", flag: "events-position-configmap"},
	{resource: "leases", flag: "events-lease"},
}

// rbacRule is the subset of a PolicyRule this test judges.
type rbacRule struct {
	APIGroups     []string `json:"apiGroups"`
	Resources     []string `json:"resources"`
	Verbs         []string `json:"verbs"`
	ResourceNames []string `json:"resourceNames"`
}

type rbacDoc struct {
	Kind  string     `json:"kind"`
	Rules []rbacRule `json:"rules"`
}

// eventsFlagValue returns the value the manifest passes to -<flag>, or the
// binary's registered default (flagDefaultFromDocs) when it passes the flag
// nowhere. Both dash spellings are accepted because Go's flag package accepts
// both, and so is a quoted list item (`- "-events-lease=x"`), because YAML
// strips the quotes: skipping it would fall back to the DEFAULT and judge the
// Role against a name the manifest does not use, and keeping the closing quote
// in the value would judge it against `x"`. Neither quote may appear in the
// value itself — an object name cannot contain one.
func eventsFlagValue(t *testing.T, manifest, flag string) string {
	t.Helper()
	re := regexp.MustCompile(`(?m)^\s*-\s+["']?--?` + regexp.QuoteMeta(flag) + `=([^\s"']+)["']?\s*$`)
	if m := re.FindStringSubmatch(manifest); m != nil {
		return m[1]
	}
	return flagDefaultFromDocs(t, flag)
}

// The name the Role is judged against is the one the manifest PASSES, in every
// spelling that passes it. A quoted list item used to miss entirely — the
// check then fell back to the binary default and judged a Role scoped to a
// custom name as wrong, or one still scoped to the default as right.
func TestEventsFlagValueReadsEverySpelling(t *testing.T) {
	for _, line := range []string{
		"            - -events-lease=custom-lease",
		"            - --events-lease=custom-lease",
		`            - "-events-lease=custom-lease"`,
		`            - '--events-lease=custom-lease'`,
	} {
		if got := eventsFlagValue(t, "args:\n"+line+"\n", "events-lease"); got != "custom-lease" {
			t.Errorf("%s: eventsFlagValue = %q, want %q", strings.TrimSpace(line), got, "custom-lease")
		}
	}
}

// assertEventsRBACIsScoped is the whole invariant, applied to one rendered
// manifest stream: for each of the two singleton-owned objects, every Role rule
// granting a verb other than `create` on that resource must carry exactly the
// resourceNames the manifest's own flags name.
func assertEventsRBACIsScoped(t *testing.T, where, manifest string) {
	t.Helper()
	checked := 0
	for _, doc := range manifestcheck.Documents(manifest) {
		var d rbacDoc
		if err := yaml.Unmarshal([]byte(doc), &d); err != nil {
			continue // not a Kubernetes object (helm's leading comment block, say)
		}
		if d.Kind != "Role" && d.Kind != "ClusterRole" {
			continue
		}
		for _, rule := range d.Rules {
			for _, want := range scopedEventsResources {
				if !slices.Contains(rule.Resources, want.resource) {
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
				name := eventsFlagValue(t, manifest, want.flag)
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

// assertEventsClusterRoleReadsOnlyWhatTheReaderReads is the other half of the
// same argument, applied to the ONE cluster-wide grant this workload gets.
//
// internal/agent/events is core/v1 only and does exactly two things with
// events: the initial LIST (which the relist path re-runs, paginated) and one
// WATCH — `CoreV1().Events(ns).List` and `.Watch`. There is no single-object
// Get in the package and no `EventsV1()` call anywhere, so `get` and an
// `events.k8s.io` mirror rule grant reads nothing exercises. Neither is a
// runtime hazard (both representations are read-only views of the same
// objects); what they cost is that an operator hand-managing RBAC — the
// audience deploy/events.yaml's comments are written for — is told to grant
// more than the code uses, and a least-privilege review cannot tell from the
// manifest which representation the reader actually takes. The goldens pin the
// bytes but cannot say which shape is CORRECT, so the intent is pinned here.
func assertEventsClusterRoleReadsOnlyWhatTheReaderReads(t *testing.T, where, manifest string) {
	t.Helper()
	seen := 0
	for _, doc := range manifestcheck.Documents(manifest) {
		var d rbacDoc
		if err := yaml.Unmarshal([]byte(doc), &d); err != nil {
			continue
		}
		if d.Kind != "ClusterRole" {
			continue
		}
		for _, rule := range d.Rules {
			if !slices.Contains(rule.Resources, "events") {
				continue
			}
			seen++
			if !slices.Contains(rule.APIGroups, "") || len(rule.APIGroups) != 1 {
				t.Errorf("%s: ClusterRole rule on events has apiGroups %v, want exactly [\"\"] — "+
					"the reader uses CoreV1() and never EventsV1(), so an events.k8s.io grant is "+
					"unexercised and misdescribes which representation is read",
					where, rule.APIGroups)
			}
			if !equalSets(rule.Verbs, []string{"list", "watch"}) {
				t.Errorf("%s: ClusterRole rule on events has verbs %v, want exactly [list watch] — "+
					"the reader does one LIST (paginated on a relist) plus one WATCH and no "+
					"single-object Get; `get` here is a grant nothing exercises",
					where, rule.Verbs)
			}
		}
	}
	// Exactly one rule, or the scan is describing a shape that has moved.
	if seen != 1 {
		t.Errorf("%s: found %d ClusterRole rules granting `events`, want 1; the events singleton's "+
			"one cluster-wide grant has changed shape and this guard no longer sees it", where, seen)
	}
}

// equalSets reports whether two verb lists hold the same members, order and
// duplicates aside.
func equalSets(got, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	for _, w := range want {
		if !slices.Contains(got, w) {
			return false
		}
	}
	return true
}

// TestEventsRoleScopesWritesToTheObjectsTheFlagsName covers the CHART half,
// rendered with NON-DEFAULT names so a hardcoded resourceNames would fail: the
// scope has to follow the values that render the flags, not a constant.
func TestEventsRoleScopesWritesToTheObjectsTheFlagsName(t *testing.T) {
	helm := helmBin(t)
	out, err := helmTemplate(helm, "monitoring",
		"--set", "events.enabled=true",
		"--set", "events.leaseName=custom-leader-lease",
		"--set", "events.positionConfigMap=custom-position-cm",
	)
	if err != nil {
		t.Fatalf("helm template failed: %v\n%s", err, out)
	}
	assertEventsRBACIsScoped(t, "rendered chart (events.enabled)", string(out))
	assertEventsClusterRoleReadsOnlyWhatTheReaderReads(t, "rendered chart (events.enabled)", string(out))
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
	assertEventsClusterRoleReadsOnlyWhatTheReaderReads(t, path, string(b))
	// The two install paths must agree on the names as well as the shape: the
	// chart's values and the binary's flag defaults are separate copies of the
	// same two strings, and deploy/ silently relies on them being equal.
	values, err := os.ReadFile(filepath.Join(chartDir, "values.yaml"))
	if err != nil {
		t.Fatalf("reading values.yaml: %v", err)
	}
	for _, want := range scopedEventsResources {
		def := flagDefaultFromDocs(t, want.flag)
		if !strings.Contains(string(values), ": "+def) {
			t.Errorf("charts/kubescrape/values.yaml no longer defaults to %q, the binary's default for -%s "+
				"that deploy/events.yaml's resourceNames rely on; the two install paths have drifted", def, want.flag)
		}
	}
}
