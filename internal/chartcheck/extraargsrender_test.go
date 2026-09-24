package chartcheck

import (
	"slices"
	"testing"

	"sigs.k8s.io/yaml"

	"github.com/JohanLindvall/kubescrape/internal/manifestcheck"
)

// extraArgs is operator free text rendered into an args list, so it must reach
// the container VERBATIM whatever it contains. The DaemonSet and the metadata
// service rendered it as `- {{ . }}`, a bare plain scalar: "-x=a: b" became a
// MAP (which the API server rejects for args) and "-x=y #z" was silently cut at
// the comment marker, while the two singletons, which used toYaml, were right.
// This renders all four workloads with the two hostile shapes and requires each
// container's args to END with exactly the strings supplied.
func TestExtraArgsReachEveryWorkloadVerbatim(t *testing.T) {
	helm := helmBin(t)
	want := []string{"-foo=a: b", "-bar=x #y", `-baz="quoted" 'too'`}
	var set []string
	for _, key := range []string{"agent", "service", "events", "serviceGraph"} {
		for i, v := range want {
			// --set-string would still split at commas; index each element so
			// helm sees one scalar per entry, spelled exactly as given.
			set = append(set, key+".extraArgs["+string(rune('0'+i))+"]="+v)
		}
	}
	out := renderAllWorkloads(t, helm, set...)

	type workload struct {
		Kind     string `json:"kind"`
		Metadata struct {
			Name string `json:"name"`
		} `json:"metadata"`
		Spec struct {
			Template struct {
				Spec struct {
					Containers []struct {
						Args []any `json:"args"`
					} `json:"containers"`
				} `json:"spec"`
			} `json:"template"`
		} `json:"spec"`
	}
	seen := 0
	for _, doc := range manifestcheck.Documents(out) {
		var w workload
		if err := yaml.Unmarshal([]byte(doc), &w); err != nil {
			t.Fatalf("unmarshal: %v\n%s", err, doc)
		}
		switch w.Kind {
		case "DaemonSet", "Deployment", "StatefulSet":
		default:
			continue
		}
		seen++
		args := w.Spec.Template.Spec.Containers[0].Args
		if len(args) < len(want) {
			t.Errorf("%s %s: %d args, want at least %d", w.Kind, w.Metadata.Name, len(args), len(want))
			continue
		}
		var tail []string
		for _, a := range args[len(args)-len(want):] {
			s, ok := a.(string)
			if !ok {
				t.Errorf("%s %s: an extraArgs entry rendered as %T (%v), not a string — the API server rejects it", w.Kind, w.Metadata.Name, a, a)
			}
			tail = append(tail, s)
		}
		if !slices.Equal(tail, want) {
			t.Errorf("%s %s: extraArgs rendered as %q, want %q verbatim", w.Kind, w.Metadata.Name, tail, want)
		}
	}
	if seen != 4 {
		t.Errorf("checked %d workloads, want 4", seen)
	}
}
