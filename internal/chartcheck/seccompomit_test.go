package chartcheck

import (
	"io/fs"
	"os"
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
		out, err := helmTemplate(helm, "monitoring", "--set", on, "-f", f)
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

	// The documentation is pinned to what these renders prove by
	// TestSeccompDocumentationSaysNullNotEmptyMap, which needs no helm.

	// The trap the documentation used to send people into. If helm ever stops
	// coalescing maps this flips, and the comments in values.yaml and
	// _helpers.tpl become wrong in the other direction — so pin it rather than
	// leave it to be rediscovered.
	if n := strings.Count(render(t, "seccompProfile: {}\n"), "seccompProfile:"); n != 4 {
		t.Errorf("`seccompProfile: {}` rendered the field on %d workloads, want 4: helm no "+
			"longer coalesces an empty user map onto the chart default, so the "+
			"`null, NOT {}` comments in values.yaml and _helpers.tpl "+
			"(kubescrape.podSecurityContext) are now misleading and must be updated", n)
	}
}

// seccompCarriers are the files known to carry the seccompProfile instruction:
// values.yaml, and _helpers.tpl's kubescrape.podSecurityContext, which all four
// workload templates include — the instruction is written there ONCE, where it
// used to be copied into every template. They are the floor, not the scope —
// the scan below reads every file in the chart.
var seccompCarriers = []string{
	"values.yaml",
	"templates/_helpers.tpl",
}

// wrongSeccompAdvice returns every file under root that tells the operator to
// omit the seccompProfile with `{}`, which TestSeccompProfileIsOmittedByNullNotEmptyMap
// proves does nothing. Every regular file is read, not a list of the ones that
// carry the instruction today: a list is a SUBSET of the chart by construction
// (see manifestcheck.ManifestFiles), and a fifth workload template repeating
// the wrong advice beside its own seccompProfile would never have been opened.
func wrongSeccompAdvice(root string) ([]string, error) {
	var bad []string
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		b, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		text := string(b)
		if strings.Contains(text, "`seccompProfile: {}` to omit") || strings.Contains(text, "Set to `{}`") {
			bad = append(bad, path)
		}
		return nil
	})
	return bad, err
}

// The DOCUMENTATION is pinned to what the renders above prove, because the
// defect was never in the template — the template has always behaved this way.
// It was five copies of an instruction that does nothing. This half needs no
// helm, so it runs on every machine.
func TestSeccompDocumentationSaysNullNotEmptyMap(t *testing.T) {
	bad, err := wrongSeccompAdvice(chartDir)
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range bad {
		t.Errorf("%s tells the operator to omit the seccompProfile with `{}`, which "+
			"TestSeccompProfileIsOmittedByNullNotEmptyMap proves does nothing (helm coalesces "+
			"the empty map onto the chart default). It must say `null`", path)
	}
	// The floor: the known carriers still carry it. A comment moved elsewhere
	// is still scanned (the walk reads everything); this only makes a deleted
	// instruction a deliberate act.
	for _, rel := range seccompCarriers {
		b, err := os.ReadFile(filepath.Join(chartDir, rel))
		if err != nil {
			t.Fatal(err)
		}
		if text := string(b); !strings.Contains(text, "seccompProfile") || !strings.Contains(text, "`null`") {
			t.Errorf("%s no longer tells the operator to omit seccompProfile with `null`; if the instruction moved, move it in seccompCarriers", rel)
		}
	}
}

// The scan reaches a file nobody listed: a new template repeating the wrong
// advice, in a subdirectory at that, is reported.
func TestSeccompDocumentationScanReadsEveryFileInTheChart(t *testing.T) {
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "templates", "extra"), 0o700); err != nil {
		t.Fatal(err)
	}
	newTemplate := filepath.Join(root, "templates", "extra", "worker.yaml")
	for path, text := range map[string]string{
		filepath.Join(root, "values.yaml"): "# seccompProfile: set to `null` to omit it.\n",
		newTemplate:                        "{{- /* seccompProfile: Set to `{}` in values to omit it. */}}\n",
	} {
		if err := os.WriteFile(path, []byte(text), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	bad, err := wrongSeccompAdvice(root)
	if err != nil {
		t.Fatal(err)
	}
	if len(bad) != 1 || bad[0] != newTemplate {
		t.Errorf("wrongSeccompAdvice = %q, want exactly [%s]", bad, newTemplate)
	}
}
