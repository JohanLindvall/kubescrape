// Package chartcheck pins the RENDERED output of the Helm chart with golden
// files, one per value fixture. internal/manifestcheck asserts that the
// static manifests' flags exist; this covers the half it cannot — the chart's
// TEMPLATE LOGIC: the logsExcludeNamespaces null-vs-empty derivation and its
// in-cluster endpoint detection, the scrape-auth token precedence, which
// flags an enabled feature renders and where, the shared singleton for
// events/azure. A template edit that changes any rendered manifest fails here
// with a diff instead of shipping silently.
//
// Requires a helm binary (hack/bin/helm — `make helm-lint` bootstraps it via
// hack/ensure-helm.sh — or PATH); the test skips when neither exists, so a
// bare `go test ./...` on a helm-less machine stays green while `make check`
// always exercises it. Regenerate after an intended template change with:
//
//	go test ./internal/chartcheck -run TestChartGolden -update-chart-golden
//
// Fixtures deliberately pin everything nondeterministic (the scrape-auth
// token value; helm's lookup() returns nothing without a cluster, and the
// fixture must not fall through to randAlphaNum). Golden output can differ
// across helm MAJOR versions in whitespace details, so hack/ensure-helm.sh
// bootstraps the pinned version — but only hack/bin/helm IS that pin: a helm
// found on PATH here is whatever the machine (or the CI runner image) ships,
// and regenerating goldens under one while comparing under another is what the
// pin exists to prevent. Run `make helm-lint` once before `-update-chart-golden`.
package chartcheck

import (
	"bytes"
	"flag"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

var updateChartGolden = flag.Bool("update-chart-golden", false, "rewrite testdata/golden from the current chart")

func helmBin(t *testing.T) string {
	t.Helper()
	if p, err := filepath.Abs("../../hack/bin/helm"); err == nil {
		if _, statErr := os.Stat(p); statErr == nil {
			return p
		}
	}
	if p, err := exec.LookPath("helm"); err == nil {
		return p
	}
	// A developer without helm gets a skip; CI does NOT. This whole guard is a
	// no-op without a helm binary, and the CI job that runs the Go tests never
	// installed one — so the rendered-chart goldens, the only check that can
	// see template LOGIC (the logsExcludeNamespaces derivation that the
	// text-reading manifestcheck cannot), silently protected nothing on every
	// PR. A guard that quietly downgrades to "pass" in the one environment
	// that gates merges is worse than no guard, because the green check is
	// read as coverage. GitHub Actions sets CI=true.
	if os.Getenv("CI") != "" {
		t.Fatal("no helm binary (hack/bin/helm or PATH), but CI is set: the chart golden " +
			"guard must not silently skip in CI — install helm in this job (see .github/workflows/ci.yml)")
	}
	t.Skip("no helm binary (hack/bin/helm or PATH); run `make helm-lint` once to bootstrap it")
	return ""
}

func TestChartGolden(t *testing.T) {
	helm := helmBin(t)
	fixtures, err := filepath.Glob(filepath.Join("testdata", "values", "*.yaml"))
	if err != nil || len(fixtures) == 0 {
		t.Fatalf("no value fixtures under testdata/values (err=%v)", err)
	}
	for _, fixture := range fixtures {
		name := strings.TrimSuffix(filepath.Base(fixture), ".yaml")
		t.Run(name, func(t *testing.T) {
			// Release name and namespace are part of the rendered output, so
			// they are pinned here exactly like the fixture's values.
			cmd := exec.Command(helm, "template", "kubescrape", "../../charts/kubescrape",
				"--namespace", "monitoring", "-f", fixture)
			out, err := cmd.CombinedOutput()
			if err != nil {
				t.Fatalf("helm template failed: %v\n%s", err, out)
			}
			golden := filepath.Join("testdata", "golden", name+".yaml")
			if *updateChartGolden {
				if err := os.MkdirAll(filepath.Dir(golden), 0o755); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(golden, out, 0o644); err != nil {
					t.Fatal(err)
				}
				return
			}
			want, err := os.ReadFile(golden)
			if err != nil {
				t.Fatalf("%v — regenerate with: go test ./internal/chartcheck -run TestChartGolden -update-chart-golden", err)
			}
			if !bytes.Equal(out, want) {
				t.Errorf("rendered chart differs from %s:\n%s\nIf the change is intended, regenerate with: go test ./internal/chartcheck -run TestChartGolden -update-chart-golden",
					golden, unifiedDiff(t, want, out))
			}
		})
	}
}

// unifiedDiff shells out to diff for a readable report, falling back to a
// byte-count note when diff is unavailable.
func unifiedDiff(t *testing.T, want, got []byte) string {
	t.Helper()
	dir := t.TempDir()
	wantPath := filepath.Join(dir, "want.yaml")
	gotPath := filepath.Join(dir, "got.yaml")
	if os.WriteFile(wantPath, want, 0o600) != nil || os.WriteFile(gotPath, got, 0o600) != nil {
		return "(diff unavailable)"
	}
	out, _ := exec.Command("diff", "-u", wantPath, gotPath).CombinedOutput()
	if len(out) == 0 {
		return "(files differ but diff produced no output)"
	}
	const maxDiff = 8000
	if len(out) > maxDiff {
		return string(out[:maxDiff]) + "\n… (diff truncated)"
	}
	return string(out)
}

// values.schema.json makes a typo'd value a helm-time error instead of a
// silently-ignored key. Both directions matter: the default values (and every
// fixture above) must VALIDATE — helm lint and TestChartGolden cover that,
// which is why everything.yaml carries a NON-EMPTY nodeSelector/affinity and a
// fractional logs rate: a schema that types a free-form map as
// `properties: {}, additionalProperties: false` refuses every legal value, and
// only a fixture that sets one renders it — and an unknown key must REFUSE to
// render.
func TestValuesSchemaRejectsUnknownKeys(t *testing.T) {
	helm := helmBin(t)
	for _, set := range []string{
		"agent.ingest.grpcMaxRecvByte=1", // the typo the schema exists for
		"agent.injest.enabled=true",
		"servceGraph.enabled=true",
	} {
		out, err := exec.Command(helm, "template", "kubescrape", "../../charts/kubescrape",
			"--namespace", "monitoring", "--set", set).CombinedOutput()
		if err == nil {
			t.Errorf("--set %s rendered fine; the schema should have refused it", set)
		} else if !strings.Contains(string(out), "Additional property") && !strings.Contains(string(out), "additional propert") {
			t.Errorf("--set %s failed for another reason: %s", set, out)
		}
	}
}
