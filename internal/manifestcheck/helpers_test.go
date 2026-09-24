package manifestcheck

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

// A flag a template takes from an included helper is passed exactly as one
// written in place, so Flags must read it — and classify it by the document
// that INCLUDES it, since a helper names no binary. Read as bare text, the
// including template showed only its include line and every flag in the helper
// went unchecked, which is what kept the chart's shared -otlp-* block
// hand-copied into three templates (and drifting) for as long as it was.
func TestFlagsReadArgumentsPassedThroughAnIncludedHelper(t *testing.T) {
	dir := t.TempDir()
	helpers := `{{/* A comment naming {{- define "not.a.helper" }} defines nothing. */}}
{{- define "t.agentArgs" }}
            - -helped={{ .Values.a }}
            {{- with .Values.b }}
            - --helped-with={{ . }}
            {{- end }}
            {{- include "t.nested" . }}
{{- end }}

{{- define "t.nested" -}}
- -nested
{{- end -}}

{{- define "t.inline" -}}
{{ .Values.image }}
{{- end -}}
`
	manifest := `apiVersion: apps/v1
kind: DaemonSet
spec:
  template:
    spec:
      containers:
        - name: agent
          image: {{ include "t.inline" . }}
          command: ["` + AgentCommand + `"]
          args:
            - -logs
            {{- include "t.agentArgs" . }}
            {{- include "t.unknown" . | nindent 12 }}
---
apiVersion: apps/v1
kind: Deployment
spec:
  template:
    spec:
      containers:
        - name: kubescrape
          args:
            - -listen=:8081
`
	path := filepath.Join(dir, "agent.yaml")
	for p, text := range map[string]string{filepath.Join(dir, "_helpers.tpl"): helpers, path: manifest} {
		if err := os.WriteFile(p, []byte(text), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	for _, tc := range []struct {
		agent bool
		want  []string
	}{
		{true, []string{"logs", "helped", "helped-with", "nested"}},
		{false, []string{"listen"}},
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

// Every expanded line says where it came from, so a guard reporting one points
// at the text to edit: the helper's own file and line for a spliced-in line,
// the template's for the rest — whose numbering does NOT shift past an include.
func TestExpandIncludesKeepsEachLinesOrigin(t *testing.T) {
	helpers := map[string]Helper{}
	if err := readHelpers("_helpers.tpl", "{{- define \"h\" }}\n- -a\n- -b\n{{- end }}\n", helpers); err != nil {
		t.Fatal(err)
	}
	got, err := ExpandIncludes("dir/w.yaml", "args:\n  {{- include \"h\" . }}\n  - -c\n", helpers)
	if err != nil {
		t.Fatal(err)
	}
	want := []Line{
		{"args:", "w.yaml:1"},
		{"- -a", "_helpers.tpl:2"},
		{"- -b", "_helpers.tpl:3"},
		{"  - -c", "w.yaml:3"},
		{"", "w.yaml:4"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("ExpandIncludes = %q, want %q", got, want)
	}
}

// A helper the line scanner would read WRONG is refused rather than guessed at:
// a wrong read is a set of flags checked wrong, which a green run would hide.
func TestHelpersRefusesWhatItCannotReadLineByLine(t *testing.T) {
	for name, src := range map[string]string{
		"body on the define line": "{{- define \"h\" }}- -a\n{{- end }}\n",
		"body on the end line":    "{{- define \"h\" }}\n- -a{{- end }}\n",
		"never closed":            "{{- define \"h\" }}\n{{- if .x }}\n- -a\n{{- end }}\n",
		"defined twice":           "{{- define \"h\" }}\n{{- end }}\n{{- define \"h\" }}\n{{- end }}\n",
	} {
		if err := readHelpers("_helpers.tpl", src, map[string]Helper{}); err == nil {
			t.Errorf("%s: readHelpers accepted %q", name, src)
		}
	}
	self := map[string]Helper{}
	if err := readHelpers("_helpers.tpl", "{{- define \"h\" }}\n{{- include \"h\" . }}\n{{- end }}\n", self); err != nil {
		t.Fatal(err)
	}
	if _, err := ExpandIncludes("w.yaml", "{{- include \"h\" . }}\n", self); err == nil || !strings.Contains(err.Error(), "deep") {
		t.Errorf("a helper including itself expanded without error (err=%v)", err)
	}
}
