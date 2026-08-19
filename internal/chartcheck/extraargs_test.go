package chartcheck

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"sigs.k8s.io/yaml"
)

// TestFlowSequenceExtraArgsExamplesDoNotSplitAtAComma pins the one thing a
// golden file cannot see: the extraArgs SNIPPETS the chart documents in
// comments. helm never evaluates a comment, so every rendered-output test in
// this package is blind to them — yet a backticked snippet is precisely what an
// operator copies verbatim.
//
// The hazard is YAML's, not the chart's: a PLAIN scalar inside a flow sequence
// ends at the comma, so `extraArgs: [-monitor-namespaces=monitoring,platform]`
// is two entries, and the chart faithfully renders `- -monitor-namespaces=monitoring`
// followed by a bare `- platform`. Neither binary inspects flag.Args(), so that
// stray positional is ignored AND terminates flag.Parse: every extraArgs entry
// after it is silently never parsed, including one that would otherwise exit 2
// as undefined. For -monitor-namespaces — a multi-tenancy gate — the mis-parse
// fails closed to a narrower allowlist whose only trace is a Debug line and
// kubescrape_monitor_namespace_refused_total, both indistinguishable from the
// gate working exactly as configured.
//
// The invariant asserted is the general one rather than "quote this one line":
// every element of a documented flow-sequence extraArgs example must be a FLAG.
// A bare word can only be a plain scalar that split at a comma (or an example
// that would not work anyway), so the check needs no list of known-bad values
// and catches the next snippet someone writes. Block-style examples are exempt
// by construction — a block plain scalar may contain commas — which is why the
// scan looks only for `extraArgs:` followed by `[`.
func TestFlowSequenceExtraArgsExamplesDoNotSplitAtAComma(t *testing.T) {
	t.Parallel()
	var files []string
	for _, glob := range []string{
		filepath.Join("..", "..", "charts", "kubescrape", "*.yaml"),
		filepath.Join("..", "..", "charts", "kubescrape", "templates", "*.yaml"),
		filepath.Join("..", "..", "deploy", "*.yaml"),
	} {
		found, err := filepath.Glob(glob)
		if err != nil {
			t.Fatalf("globbing %s: %v", glob, err)
		}
		files = append(files, found...)
	}
	if len(files) == 0 {
		t.Fatal("no chart or manifest files found to scan")
	}

	examples := 0
	for _, path := range files {
		b, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("reading %s: %v", path, err)
		}
		for i, line := range strings.Split(string(b), "\n") {
			seq, ok := flowSequenceAfter(line, "extraArgs:")
			if !ok {
				continue
			}
			var args []string
			if err := yaml.Unmarshal([]byte("args: "+seq), &struct {
				Args *[]string `json:"args"`
			}{Args: &args}); err != nil {
				t.Errorf("%s:%d: extraArgs example %s is not a YAML string list: %v", path, i+1, seq, err)
				continue
			}
			if len(args) > 0 {
				examples++
			}
			for _, arg := range args {
				if !strings.HasPrefix(arg, "-") {
					t.Errorf("%s:%d: extraArgs example %s parses to %q, whose element %q is not a flag — "+
						"a plain scalar in a flow sequence ends at the comma, so this renders an extra "+
						"container arg that flag.Parse treats as a positional and stops at, silently "+
						"dropping every later entry. Quote the element or write the example block style.",
						path, i+1, seq, args, arg)
				}
			}
		}
	}
	// The shipped values are all `extraArgs: []`, so only a documented EXAMPLE
	// has elements to check. A scan that silently found none would go green
	// after a reword moved every snippet out of the matched form — the same
	// failure as no test at all.
	if examples == 0 {
		t.Error("found no non-empty flow-sequence extraArgs example; the scan has stopped matching the documented form")
	}
}

// flowSequenceAfter returns the `[...]` flow sequence following key on line,
// comment prefix and any surrounding backticks stripped by construction (the
// slice starts at `[` and ends at the first `]`). Single-line only: every
// example the chart documents in this form fits on one line, and a multi-line
// flow sequence in a comment is not a copy-pasteable snippet.
func flowSequenceAfter(line, key string) (string, bool) {
	k := strings.Index(line, key)
	if k < 0 {
		return "", false
	}
	rest := strings.TrimSpace(line[k+len(key):])
	if !strings.HasPrefix(rest, "[") {
		return "", false
	}
	end := strings.Index(rest, "]")
	if end < 0 {
		return "", false
	}
	return rest[:end+1], true
}
