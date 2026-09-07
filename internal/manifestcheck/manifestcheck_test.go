package manifestcheck

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

// One file, two workloads. The classification is what decides which binary's
// flag set a manifest is checked against, so it has to follow the CONTAINER: a
// per-file verdict asserted one document's flags against the other binary, and
// a flag that exists in neither would then have been reported as existing in
// one — the CrashLoop this package exists to catch, passing its own check.
//
// Both dash spellings are extracted, because Go's flag package accepts both: a
// manifest written the GNU way used to yield no flags at all and be asserted on
// vacuously.
func TestFlagsClassifiesPerDocumentAndReadsBothDashSpellings(t *testing.T) {
	dir := t.TempDir()
	manifest := `apiVersion: apps/v1
kind: Deployment
spec:
  template:
    spec:
      containers:
        - name: kubescrape
          args:
            - -listen=:8081
            - --wait-timeout=5s
---
apiVersion: apps/v1
kind: DaemonSet
spec:
  template:
    spec:
      containers:
        - name: agent
          command: ["` + AgentCommand + `"]
          args:
            - -logs
            - --journald
`
	path := filepath.Join(dir, "both.yaml")
	if err := os.WriteFile(path, []byte(manifest), 0o600); err != nil {
		t.Fatal(err)
	}

	for _, tc := range []struct {
		agent bool
		want  []string
	}{
		{false, []string{"listen", "wait-timeout"}},
		{true, []string{"logs", "journald"}},
	} {
		byFile, err := Flags([]string{dir}, tc.agent)
		if err != nil {
			t.Fatal(err)
		}
		if got := byFile[path]; !reflect.DeepEqual(got, tc.want) {
			t.Errorf("Flags(agent=%v) = %v, want %v", tc.agent, got, tc.want)
		}
	}
}

// A document with no container spec carries no flags, and a file with none at
// all is not reported at all — the callers fail when the result is EMPTY, so a
// phantom entry would be worse than none.
func TestFlagsSkipsManifestsWithoutContainers(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "rbac.yaml")
	if err := os.WriteFile(path, []byte("apiVersion: v1\nkind: ServiceAccount\nmetadata:\n  name: kubescrape\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	byFile, err := Flags([]string{dir}, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(byFile) != 0 {
		t.Errorf("Flags = %v, want nothing for a manifest with no container", byFile)
	}
}

// The manifest corpus is WALKED, and both YAML extensions count.
//
// A flat listing of *.yaml was the whole reach of this package: helm renders a
// template in a subdirectory of templates/ exactly like a flat one, so grouping
// templates into templates/rbac/ — or renaming one to .yml — took every flag it
// passes out of the assertion. Nothing would have said so, because the vacuity
// guard in each binary's test is whole-corpus (it asks only whether ANY
// manifest was found), so the four flat files kept it green while the new one
// shipped a flag the binary no longer defines: flag.Parse's ExitOnError, exit 2
// on every node, one line of container log.
func TestFlagsWalksSubdirectoriesAndBothYAMLExtensions(t *testing.T) {
	dir := t.TempDir()
	manifest := func(flagName string) []byte {
		return []byte(`apiVersion: apps/v1
kind: Deployment
spec:
  template:
    spec:
      containers:
        - name: kubescrape
          args:
            - -` + flagName + `=:8081
`)
	}
	sub := filepath.Join(dir, "rbac", "deep")
	if err := os.MkdirAll(sub, 0o700); err != nil {
		t.Fatal(err)
	}
	flat := filepath.Join(dir, "flat.yaml")
	nested := filepath.Join(sub, "nested.yaml")
	yml := filepath.Join(dir, "renamed.yml")
	for path, name := range map[string]string{
		flat:   "flat-flag",
		nested: "nested-flag",
		yml:    "yml-flag",
	} {
		if err := os.WriteFile(path, manifest(name), 0o600); err != nil {
			t.Fatal(err)
		}
	}

	byFile, err := Flags([]string{dir}, false)
	if err != nil {
		t.Fatal(err)
	}
	for path, want := range map[string]string{
		flat:   "flat-flag",
		nested: "nested-flag",
		yml:    "yml-flag",
	} {
		got := byFile[path]
		if !reflect.DeepEqual(got, []string{want}) {
			t.Errorf("Flags did not read %s (got %v, want [%s]); every flag it passes is unchecked", path, got, want)
		}
	}
}

// ManifestFiles is the one listing, and it must not pick up the things that sit
// beside a chart's templates.
func TestManifestFilesTakesOnlyYAMLAndIsStablyOrdered(t *testing.T) {
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "sub"), 0o700); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"b.yaml", "a.yml", "_helpers.tpl", "values.schema.json", "NOTES.txt", "sub/c.yaml"} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte("{}\n"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	got, err := ManifestFiles(dir)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{
		filepath.Join(dir, "a.yml"),
		filepath.Join(dir, "b.yaml"),
		filepath.Join(dir, "sub", "c.yaml"),
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("ManifestFiles = %v, want %v", got, want)
	}
}
