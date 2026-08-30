package chartcheck

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// The chart documents an escape hatch for the pod-level seccompProfile it sets
// on all four workloads: an operator whose policy needs the field ABSENT (a
// cluster running a custom profile through a mutating webhook, an admission
// controller that refuses a pod which sets it) is told how to remove it.
//
// The instruction has to be `null`, and this test is why. `{}` cannot work and
// is not a matter of template logic: helm COALESCES a user-supplied map onto the
// chart's default map, so `seccompProfile: {}` in a -f file arrives inside the
// template as `{type: RuntimeDefault}` — indistinguishable from unset — and
// `with` renders it. The chart said `{}` in five places (values.yaml and one
// comment per workload template), so an operator following the documentation
// removed nothing, got no error from helm, and shipped pods carrying a field
// they had explicitly asked to omit.
//
// Both halves are pinned: that `null` really omits it on EVERY workload, and
// that `{}` really does not — the second is what stops the documentation
// drifting back, since a reader who assumes helm replaces maps will write `{}`
// again.
func TestSeccompProfileIsOmittedByNullNotEmptyMap(t *testing.T) {
	helm := helmBin(t)
	// Every workload, so the escape hatch cannot work on three of four.
	const on = "serviceGraph.enabled=true,serviceGraph.tokenSecret.name=sg,events.enabled=true"

	render := func(t *testing.T, valuesYAML string) string {
		t.Helper()
		dir := t.TempDir()
		f := filepath.Join(dir, "values.yaml")
		if err := os.WriteFile(f, []byte(valuesYAML), 0o600); err != nil {
			t.Fatal(err)
		}
		out, err := exec.Command(helm, "template", "kubescrape", "../../charts/kubescrape",
			"--namespace", "monitoring", "--set", on, "-f", f).CombinedOutput()
		if err != nil {
			t.Fatalf("helm template with %q failed: %v\n%s", valuesYAML, err, out)
		}
		return string(out)
	}

	// The default: present on all four pod templates.
	if n := strings.Count(render(t, "{}\n"), "seccompProfile:"); n != 4 {
		t.Fatalf("the default renders seccompProfile on %d workloads, want 4 "+
			"(agent DaemonSet, metadata service, events singleton, trace tier)", n)
	}

	// The documented escape hatch.
	if got := render(t, "seccompProfile: null\n"); strings.Contains(got, "seccompProfile:") {
		t.Error("`seccompProfile: null` still rendered the field: the one documented way to " +
			"omit it does not omit it, and an operator whose admission policy refuses the " +
			"field has no way to satisfy it through values")
	}

	// And the DOCUMENTATION is pinned to what the two renders above prove,
	// because the defect was never in the template — the template has always
	// behaved this way. It was five copies of an instruction that does nothing.
	for _, doc := range []string{
		"../../charts/kubescrape/values.yaml",
		"../../charts/kubescrape/templates/agent.yaml",
		"../../charts/kubescrape/templates/service.yaml",
		"../../charts/kubescrape/templates/events.yaml",
		"../../charts/kubescrape/templates/servicegraph.yaml",
	} {
		b, err := os.ReadFile(doc)
		if err != nil {
			t.Fatal(err)
		}
		text := string(b)
		if !strings.Contains(text, "seccompProfile") {
			t.Fatalf("%s no longer mentions seccompProfile: this test names the files that "+
				"carry the instruction, so a moved comment must be re-pointed here", doc)
		}
		if strings.Contains(text, "`seccompProfile: {}` to omit") || strings.Contains(text, "Set to `{}`") {
			t.Errorf("%s tells the operator to omit the seccompProfile with `{}`, which the "+
				"render above proves does nothing (helm coalesces the empty map onto the "+
				"chart default). It must say `null`", doc)
		}
	}

	// The trap the documentation used to send people into. If helm ever stops
	// coalescing maps this flips, and the comments in values.yaml and the four
	// templates become wrong in the other direction — so pin it rather than
	// leave it to be rediscovered.
	if n := strings.Count(render(t, "seccompProfile: {}\n"), "seccompProfile:"); n != 4 {
		t.Errorf("`seccompProfile: {}` rendered the field on %d workloads, want 4: helm no "+
			"longer coalesces an empty user map onto the chart default, so the "+
			"`null, NOT {}` comments in values.yaml and the four workload templates "+
			"are now misleading and must be updated", n)
	}
}
