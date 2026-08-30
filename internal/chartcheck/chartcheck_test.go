// Package chartcheck pins the RENDERED output of the Helm chart with golden
// files, one per value fixture. internal/manifestcheck asserts that the
// static manifests' flags exist; this covers the half it cannot — the chart's
// TEMPLATE LOGIC: the logsExcludeNamespaces null-vs-empty derivation and its
// in-cluster endpoint detection, the scrape-auth token precedence, which
// flags an enabled feature renders and where, the shared singleton for
// events/azure. A template edit that changes any rendered manifest fails here
// with a diff instead of shipping silently.
//
// Requires the PINNED helm (hack/bin/helm — `make helm-lint` bootstraps it via
// hack/ensure-helm.sh — or a matching one on PATH); the test skips when neither
// is present, so a bare `go test ./...` on a helm-less machine stays green while
// `make check` always exercises it. Regenerate after an intended template change
// with:
//
//	go test ./internal/chartcheck -run TestChartGolden -update-chart-golden
//
// Fixtures deliberately pin everything nondeterministic (the scrape-auth
// token value; helm's lookup() returns nothing without a cluster, and the
// fixture must not fall through to randAlphaNum).
//
// Golden output differs across helm MAJOR versions in whitespace details, so
// the binary's VERSION is checked against the pin in hack/helm-version and a
// mismatch is refused — the version is the only thing that makes "the chart
// renders to this" a fact rather than a property of whoever ran it. This used
// to trust any binary it could find: CI installed an unpinned helm, the runner
// image moved to v4 (which emits one more blank line after a list), and every
// golden failed against goldens no template change had touched. A PATH helm is
// still accepted, but only when it IS the pinned version.
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

// pinnedHelmVersion reads hack/helm-version, the one home for the version
// hack/ensure-helm.sh downloads and this test compares under.
func pinnedHelmVersion(t *testing.T) string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("..", "..", "hack", "helm-version"))
	if err != nil {
		t.Fatalf("reading the helm pin: %v", err)
	}
	v := strings.TrimSpace(string(b))
	if v == "" {
		t.Fatal("hack/helm-version is empty")
	}
	return v
}

// helmVersion returns the short version of a helm binary ("v3.19.0"), dropping
// the "+gSHA" build suffix `helm version --short` appends.
func helmVersion(bin string) (string, error) {
	out, err := exec.Command(bin, "version", "--short").Output()
	if err != nil {
		return "", err
	}
	v, _, _ := strings.Cut(strings.TrimSpace(string(out)), "+")
	return v, nil
}

func helmBin(t *testing.T) string {
	t.Helper()
	want := pinnedHelmVersion(t)
	var candidates []string
	if p, err := filepath.Abs("../../hack/bin/helm"); err == nil {
		if _, statErr := os.Stat(p); statErr == nil {
			candidates = append(candidates, p)
		}
	}
	if p, err := exec.LookPath("helm"); err == nil {
		candidates = append(candidates, p)
	}
	// The VERSION is what is pinned, not the path. Taking the first binary that
	// existed is how CI came to render the goldens under helm v4 while they were
	// recorded under v3: every fixture failed, on a commit that touched no
	// template. A helm anywhere is fine as long as it is this one.
	var found []string
	for _, p := range candidates {
		got, err := helmVersion(p)
		if err != nil {
			continue
		}
		if got == want {
			return p
		}
		found = append(found, p+" ("+got+")")
	}
	// A developer without the pinned helm gets a skip; CI does NOT. This whole
	// guard is a no-op without a helm binary, and the CI job that runs the Go
	// tests once installed none — so the rendered-chart goldens, the only check
	// that can see template LOGIC (the logsExcludeNamespaces derivation that the
	// text-reading manifestcheck cannot), silently protected nothing on every
	// PR. A guard that quietly downgrades to "pass" in the one environment that
	// gates merges is worse than no guard, because the green check is read as
	// coverage. GitHub Actions sets CI=true.
	detail := "no helm binary found (hack/bin/helm or PATH)"
	if len(found) > 0 {
		detail = "found " + strings.Join(found, ", ") + " but the goldens are recorded under " + want
	}
	if os.Getenv("CI") != "" {
		t.Fatal("chart golden guard needs helm " + want + ": " + detail +
			" — bootstrap it with hack/ensure-helm.sh (see .github/workflows/ci.yml)")
	}
	t.Skip("chart golden guard needs helm " + want + ": " + detail +
		"; run `make helm-lint` once to bootstrap it")
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

// selfMetricsInterval's own test lived here: "0 disables" is what an operator
// copies, YAML reads a bare 0 as a NUMBER, and the string-only schema refused
// the install before rendering anything. It has moved into duration_test.go's
// generic tables — the field turned out to be one of FIFTEEN chart values
// rendered into a flag.Duration, and a per-field test is exactly what let its
// twin (agent.logsIdleClose) keep half a guard.

// A human-format agent.logsMetrics.maxBytes ("3MiB") used to pass the
// string-typed schema and render `-logs-metrics-max-bytes=0`: helm's int64
// parses any non-number to 0, and the agent defines 0 as "no byte bound, one
// payload per export" — the OPPOSITE of the requested bound, with no signal
// anywhere. Two layers now refuse it and this exercises both: the schema's
// digits-only pattern in the normal path, and the template's own fail()
// (kubescrape.logsMetricsMaxBytes) under --skip-schema-validation, which is
// what protects subchart use, where the parent's schema does not apply. Each
// assertion is on the one thing both error texts name — the value's key —
// so a future reshuffle of which layer fires first stays green.
// serviceGraph.enabled alone used to render a complete StatefulSet that the
// binary then refused to start: the shard's internal span receiver is reachable
// from every pod in the cluster, so the agent requires
// -service-graph-token-file, and the template simply omitted the flag when
// tokenSecret.name was empty. `helm install` succeeded, every shard
// CrashLoopBackOff'd, and the reason was visible only in a pod log. The refusal
// belongs where it can still be acted on. Both directions are asserted, because
// a guard that also refuses the WORKING config is worse than none: without a
// token the render must fail naming the value, and with one it must render.
func TestServiceGraphWithoutTokenIsRefusedAtRender(t *testing.T) {
	helm := helmBin(t)

	out, err := exec.Command(helm, "template", "kubescrape", "../../charts/kubescrape",
		"--namespace", "monitoring", "--set", "serviceGraph.enabled=true").CombinedOutput()
	if err == nil {
		t.Errorf("serviceGraph.enabled with no tokenSecret.name rendered fine; the binary refuses that flag list, so this is a CrashLoop shipped as a successful install:\n%s", out)
	} else if !strings.Contains(string(out), "serviceGraph.tokenSecret.name") {
		t.Errorf("the refusal does not name the value an operator has to set: %s", out)
	}

	if out, err := exec.Command(helm, "template", "kubescrape", "../../charts/kubescrape",
		"--namespace", "monitoring",
		"--set", "serviceGraph.enabled=true",
		"--set", "serviceGraph.tokenSecret.name=kubescrape-service-graph").CombinedOutput(); err != nil {
		t.Errorf("naming a tokenSecret must render: %v\n%s", err, out)
	}
}

func TestLogsMetricsMaxBytesRejectsHumanSizes(t *testing.T) {
	helm := helmBin(t)
	for _, extra := range [][]string{
		nil,                          // the schema pattern refuses it
		{"--skip-schema-validation"}, // the template guard refuses it
	} {
		args := append([]string{"template", "kubescrape", "../../charts/kubescrape",
			"--namespace", "monitoring", "--set", "agent.logsMetrics.maxBytes=3MiB"}, extra...)
		out, err := exec.Command(helm, args...).CombinedOutput()
		if err == nil {
			t.Errorf("maxBytes=3MiB rendered fine (extra flags %v); it must be refused, not parsed to 0:\n%s", extra, out)
		} else if !strings.Contains(string(out), "maxBytes") {
			t.Errorf("the refusal (extra flags %v) does not name maxBytes: %s", extra, out)
		}
	}
}
