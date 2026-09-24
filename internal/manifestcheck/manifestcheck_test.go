package manifestcheck

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
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
// vacuously. So is a QUOTED list item, in either quote style: YAML strips the
// quotes before the container sees the argument, so `- "-flag=x"` passes the
// flag exactly as `- -flag=x` does, and it used to be skipped — the same
// bypass the dash spelling was, one character earlier.
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
            - "-quoted-flag={{ .Values.x }}"
            - '--single-quoted'
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
            - "--cgroup-stats"
`
	path := filepath.Join(dir, "both.yaml")
	if err := os.WriteFile(path, []byte(manifest), 0o600); err != nil {
		t.Fatal(err)
	}

	for _, tc := range []struct {
		agent bool
		want  []string
	}{
		{false, []string{"listen", "wait-timeout", "quoted-flag", "single-quoted"}},
		{true, []string{"logs", "journald", "cgroup-stats"}},
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

// The agent binary is recognised as a WHOLE path. A substring test also
// matched the agent's state directory, so a service manifest naming
// /var/lib/kubescrape-agent was asserted against the AGENT's flag set and
// failed on the service-only flags every service manifest passes. A comment in
// a chart template is never rendered, so it classifies nothing either.
func TestFlagsClassifiesByTheAgentBinaryNotBySubstrings(t *testing.T) {
	dir := t.TempDir()
	manifest := `apiVersion: apps/v1
kind: Deployment
spec:
  template:
    spec:
      containers:
        - name: kubescrape
          {{- /* unlike the agent, this runs the image's default entrypoint,
                 not ` + AgentCommand + ` */}}
          args:
            - -wait-timeout=5s
            - -positions-dir=/var/lib/kubescrape-agent
          volumeMounts:
            - name: state
              mountPath: /var/lib/kubescrape-agent/cache
`
	path := filepath.Join(dir, "service.yaml")
	if err := os.WriteFile(path, []byte(manifest), 0o600); err != nil {
		t.Fatal(err)
	}
	service, err := Flags([]string{dir}, false)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := service[path], []string{"wait-timeout", "positions-dir"}; !reflect.DeepEqual(got, want) {
		t.Errorf("service flags = %v, want %v: the document was not classified as the metadata service", got, want)
	}
	agent, err := Flags([]string{dir}, true)
	if err != nil {
		t.Fatal(err)
	}
	if got := agent[path]; len(got) != 0 {
		t.Errorf("a service document was classified as the agent (agent flags = %v)", got)
	}

	// And each spelling the real manifests use for the binary itself still is.
	for _, doc := range []string{
		`command: ["` + AgentCommand + `"]`,
		`command: [` + AgentCommand + `]`,
		"command:\n  - " + AgentCommand,
	} {
		if !isAgentDoc(doc) {
			t.Errorf("%q is not recognised as running the agent", doc)
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

// A commented-out list entry is how the raw manifests OFFER an opt-in pipeline,
// and uncommenting it is the documented way to enable one — so the offer has
// to name a flag the binary defines, or it is a CrashLoop waiting for whoever
// follows the docs. OfferedFlags is what makes that assertable: the commented
// entries, in every spelling Flags reads a live one in, classified per
// document exactly as Flags classifies, and never mixed into the passed set.
func TestOfferedFlagsReadsTheCommentedOffersPerBinary(t *testing.T) {
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
            #- -scrape-auth-secrets
            # - --scrape-auth-token-file=/etc/token
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
            # A prose comment - naming nothing - is not an offer.
            #- -journald
            #   -   --cgroup-stats
            # - "-kubelet-summary"
            {{- /*
            #- -rendered-never
            */}}
`
	path := filepath.Join(dir, "both.yaml")
	if err := os.WriteFile(path, []byte(manifest), 0o600); err != nil {
		t.Fatal(err)
	}

	for _, tc := range []struct {
		agent          bool
		offered, lived []string
	}{
		{false, []string{"scrape-auth-secrets", "scrape-auth-token-file"}, []string{"listen"}},
		{true, []string{"journald", "cgroup-stats", "kubelet-summary"}, []string{"logs"}},
	} {
		offered, err := OfferedFlags([]string{dir}, tc.agent)
		if err != nil {
			t.Fatal(err)
		}
		if got := offered[path]; !reflect.DeepEqual(got, tc.offered) {
			t.Errorf("OfferedFlags(agent=%v) = %v, want %v", tc.agent, got, tc.offered)
		}
		passed, err := Flags([]string{dir}, tc.agent)
		if err != nil {
			t.Fatal(err)
		}
		if got := passed[path]; !reflect.DeepEqual(got, tc.lived) {
			t.Errorf("Flags(agent=%v) = %v, want %v: an offer must never count as passed", tc.agent, got, tc.lived)
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

// ArgEntry is argEntry's grammar applied to one line, returning the VALUE too:
// internal/chartcheck's values-path scan reads through it, so both scanners of
// the same templates accept exactly the same argument spellings.
func TestArgEntryReadsEverySpellingAndItsValue(t *testing.T) {
	type got struct {
		name, value string
		commented   bool
	}
	for _, tc := range []struct {
		line string
		want *got
	}{
		{`        - -a={{ .Values.a }}`, &got{"a", "{{ .Values.a }}", false}},
		{`        - --gnu={{ .Values.a }}`, &got{"gnu", "{{ .Values.a }}", false}},
		{`        - "-dq={{ .Values.a }}"`, &got{"dq", "{{ .Values.a }}", false}},
		{`        - '--sq={{ .Values.a }}'`, &got{"sq", "{{ .Values.a }}", false}},
		{`        - -quoted-value="{{ .Values.a }}"`, &got{"quoted-value", `"{{ .Values.a }}"`, false}},
		{`        - -journald`, &got{"journald", "", false}},
		{`        #- -journald`, &got{"journald", "", true}},
		{`        # - --lease=x`, &got{"lease", "x", true}},
		{`        -  -two-spaces=1`, &got{"two-spaces", "1", false}},
		{`        - name: cgroup`, nil},
		{`        -journald`, nil},
		{`prose - not -an entry`, nil},
	} {
		name, value, commented, ok := ArgEntry(tc.line)
		switch {
		case tc.want == nil && ok:
			t.Errorf("ArgEntry(%q) = (%q, %q, %v), want no entry", tc.line, name, value, commented)
		case tc.want != nil && !ok:
			t.Errorf("ArgEntry(%q) found no entry, want %+v", tc.line, *tc.want)
		case tc.want != nil && (got{name, value, commented} != *tc.want):
			t.Errorf("ArgEntry(%q) = %+v, want %+v", tc.line, got{name, value, commented}, *tc.want)
		}
	}
}

// Documents splits on the separator LINE, trailing blanks included, and never
// on a `---` that is not alone in column 0 (an indented block-scalar line, or
// text that merely contains one).
func TestDocumentsSplitsOnSeparatorLinesOnly(t *testing.T) {
	src := "---\nkind: A\n---  \nkind: B\n  ---\nx: a---b\n---\nkind: C\n"
	docs := Documents(src)
	if len(docs) != 4 {
		t.Fatalf("Documents gave %d parts, want 4 (an empty head and three documents): %q", len(docs), docs)
	}
	for i, want := range []string{"kind: A", "kind: B", "kind: C"} {
		if !strings.Contains(docs[i+1], want) {
			t.Errorf("part %d = %q, want it to hold %q", i+1, docs[i+1], want)
		}
	}
	if !strings.Contains(docs[2], "  ---") {
		t.Errorf("an indented --- split a document: %q", docs)
	}
}
