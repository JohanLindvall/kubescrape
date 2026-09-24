package chartcheck

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"sigs.k8s.io/yaml"

	"github.com/JohanLindvall/kubescrape/internal/manifestcheck"
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
//
// Two corpora, each with its own floor: the chart and deploy/ manifests, and
// the MARKDOWN docs (README.md and everything under docs/). The same snippet
// for the same tenancy gate is documented in both, and the doc copy is the one
// an operator is likelier to find first — a guard over the chart alone would
// stay green while README taught the splitting form.
func TestFlowSequenceExtraArgsExamplesDoNotSplitAtAComma(t *testing.T) {
	t.Parallel()
	// manifestcheck.ManifestFiles, not a glob: it WALKS and takes .yml too, so
	// a template grouped into a subdirectory (which helm renders exactly like a
	// flat one) or renamed cannot drop out of the scan silently.
	manifests, err := manifestcheck.ManifestFiles(
		chartDir,
		filepath.Join("..", "..", "deploy"),
	)
	if err != nil {
		t.Fatal(err)
	}
	docs, err := markdownDocs(filepath.Join("..", "..", "README.md"), filepath.Join("..", "..", "docs"))
	if err != nil {
		t.Fatal(err)
	}

	for _, corpus := range []struct {
		name  string
		files []string
	}{
		{"chart and deploy manifests", manifests},
		{"markdown docs", docs},
	} {
		if len(corpus.files) == 0 {
			t.Errorf("no %s found to scan", corpus.name)
			continue
		}
		problems, examples, err := extraArgsSplitProblems(corpus.files)
		if err != nil {
			t.Fatal(err)
		}
		for _, p := range problems {
			t.Error(p)
		}
		// The shipped values are all `extraArgs: []`, so only a documented
		// EXAMPLE has elements to check. A scan that silently found none would
		// go green after a reword moved every snippet out of the matched form —
		// the same failure as no test at all.
		if examples == 0 {
			t.Errorf("found no non-empty flow-sequence extraArgs example in the %s; the scan has stopped matching the documented form", corpus.name)
		}
	}
}

// The doc corpus is markdown, where the snippet sits in inline backticks and,
// in CONFIGURATION.md, inside a table cell beside other backticked text. This
// pins that the scan reads those shapes — and that an unquoted example there is
// reported, which is the whole point of adding the docs to the scan.
func TestExtraArgsScanReadsMarkdownSnippets(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	write := func(name, body string) string {
		path := filepath.Join(dir, name)
		if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
		return path
	}
	good := write("good.md", "Quote it: `extraArgs: [\"-monitor-namespaces=monitoring,platform\"]`, or it splits.\n"+
		"| `-monitor-namespaces` | — | via `extraArgs`, QUOTED: `extraArgs: [\"-monitor-namespaces=a,b\"]` — a plain scalar ends at the comma |\n")
	bad := write("bad.md", "| `-monitor-namespaces` | — | via `extraArgs: [-monitor-namespaces=monitoring,platform]` |\n")

	problems, examples, err := extraArgsSplitProblems([]string{good})
	if err != nil {
		t.Fatal(err)
	}
	if len(problems) != 0 || examples != 2 {
		t.Errorf("quoted markdown snippets: %d problems, %d examples, want 0 and 2: %v", len(problems), examples, problems)
	}
	problems, _, err = extraArgsSplitProblems([]string{bad})
	if err != nil {
		t.Fatal(err)
	}
	if len(problems) != 1 || !strings.Contains(problems[0], `"platform"`) {
		t.Errorf("an unquoted markdown snippet must be reported naming the split-off element, got %v", problems)
	}
}

// extraArgsSplitProblems scans files for single-line flow-sequence extraArgs
// examples and returns one problem per example element that is not a flag,
// plus how many non-empty examples it found (the callers' vacuity floor).
func extraArgsSplitProblems(files []string) (problems []string, examples int, err error) {
	for _, path := range files {
		b, err := os.ReadFile(path)
		if err != nil {
			return nil, 0, fmt.Errorf("reading %s: %w", path, err)
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
				problems = append(problems, fmt.Sprintf("%s:%d: extraArgs example %s is not a YAML string list: %v", path, i+1, seq, err))
				continue
			}
			if len(args) > 0 {
				examples++
			}
			for _, arg := range args {
				if !strings.HasPrefix(arg, "-") {
					problems = append(problems, fmt.Sprintf("%s:%d: extraArgs example %s parses to %q, whose element %q is not a flag — "+
						"a plain scalar in a flow sequence ends at the comma, so this renders an extra "+
						"container arg that flag.Parse treats as a positional and stops at, silently "+
						"dropping every later entry. Quote the element or write the example block style.",
						path, i+1, seq, args, arg))
				}
			}
		}
	}
	return problems, examples, nil
}

// markdownDocs returns readme plus every *.md file under docsDir, WALKED (a
// doc grouped into a subdirectory must not drop out of the scan) and sorted.
func markdownDocs(readme, docsDir string) ([]string, error) {
	out := []string{readme}
	err := filepath.WalkDir(docsDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() && filepath.Ext(path) == ".md" {
			out = append(out, path)
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("walking %s: %w", docsDir, err)
	}
	sort.Strings(out[1:])
	return out, nil
}

// flowSequenceAfter returns the `[...]` flow sequence following key on line,
// comment prefix and any surrounding backticks stripped by construction (the
// slice starts at `[` and ends at the first `]`). Single-line only: every
// example the chart documents in this form fits on one line, and a multi-line
// flow sequence in a comment is not a copy-pasteable snippet.
func flowSequenceAfter(line, key string) (string, bool) {
	_, after, ok := strings.Cut(line, key)
	if !ok {
		return "", false
	}
	rest := strings.TrimSpace(after)
	if !strings.HasPrefix(rest, "[") {
		return "", false
	}
	end := strings.Index(rest, "]")
	if end < 0 {
		return "", false
	}
	return rest[:end+1], true
}
