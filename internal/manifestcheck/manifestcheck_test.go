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
