package docscheck

import (
	"flag"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"testing"
	"time"
)

// docFiles are the files whose table rows inventory flags: EVERY markdown doc
// under docs/, found by walking it, plus the README.
//
// It was a hardcoded three (CONFIGURATION.md, FLAGS.md, README.md), which meant
// the package doc's claim to cover "the hand-written docs" was false one file
// over: the `-buffer-dir` row in docs/FIRST-RUN.md — the operator-facing
// first-ten-minutes guide — was read by no guard in either direction, so
// renaming or deleting that flag would have left the row standing. That is the
// exact `-runtime-metrics` drift this package was written to stop. A walk also
// covers a doc that does not exist yet, which a list by construction cannot;
// the row pattern ignores prose, so a doc with no flag table costs nothing.
func docFiles(t *testing.T) []string {
	t.Helper()
	paths, err := markdownUnder(filepath.Join("..", "..", "docs"))
	if err != nil {
		t.Fatal(err)
	}
	if len(paths) == 0 {
		t.Fatal("no markdown found under docs/ — the walk is broken and every doc is unscanned")
	}
	return append(paths, filepath.Join("..", "..", "README.md"))
}

// markdownUnder returns every .md file under root, in a stable order.
//
// WALKED, not globbed: `docs/*.md` is the top level only, so a flag table in a
// docs/<subdir>/ page — the first time the docs grow a section of their own —
// would have been read by nothing, with the completeness test below (then a
// flat listing too) agreeing. manifestcheck.ManifestFiles made the same move
// for the same reason.
func markdownUnder(root string) ([]string, error) {
	var out []string
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() && filepath.Ext(d.Name()) == ".md" {
			out = append(out, path)
		}
		return nil
	})
	sort.Strings(out)
	return out, err
}

// And the walk really is one: a doc added anywhere under docs/ is scanned
// without anyone remembering to list it. The tree is enumerated independently
// here and compared by path RELATIVE to docs/ — a base name would let
// docs/a/X.md stand in for an unscanned docs/b/X.md.
func TestEveryMarkdownDocIsScannedForFlagRows(t *testing.T) {
	docsDir := filepath.Join("..", "..", "docs")
	scanned := map[string]bool{}
	for _, p := range docFiles(t) {
		scanned[filepath.Clean(p)] = true
	}
	err := filepath.WalkDir(docsDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() || filepath.Ext(d.Name()) != ".md" {
			return err
		}
		if !scanned[filepath.Clean(path)] {
			rel, _ := filepath.Rel(docsDir, path)
			t.Errorf("docs/%s is not scanned for flag table rows; a flag it documents can be renamed or deleted with nothing to notice", filepath.ToSlash(rel))
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if !scanned[filepath.Join("..", "..", "README.md")] {
		t.Error("README.md is not scanned; it is where -runtime-metrics survived its own deletion")
	}
}

// A doc in a SUBDIRECTORY is a doc. The real docs/ has none yet, which is
// exactly why this is pinned on a fixture: the first nested page would
// otherwise arrive unscanned with every test green.
func TestNestedDocsAreScanned(t *testing.T) {
	root := t.TempDir()
	for _, rel := range []string{"top.md", "guide/nested.md", "guide/deep/deeper.md", "guide/notes.txt"} {
		path := filepath.Join(root, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
			t.Fatal(err)
		}
		writeFile(t, path, "| `-some-flag` | `1` | x |\n")
	}
	got, err := markdownUnder(root)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{
		filepath.Join(root, "guide", "deep", "deeper.md"),
		filepath.Join(root, "guide", "nested.md"),
		filepath.Join(root, "top.md"),
	}
	if !slices.Equal(got, want) {
		t.Errorf("markdownUnder = %q, want %q", got, want)
	}
}

// Every flag a doc table documents must be registered by one of the binaries.
// This is the direction TestFlagsDocIsCurrent (per-binary, generated) cannot
// cover: a flag deleted from the code stays in the hand-written tables with
// nothing else to notice — README documented `-runtime-metrics` long after
// the flag was gone, and only the manifests' copy of that drift had a guard.
//
// The registered set is the GENERATED docs/FLAGS.md (see the package doc), so
// a failure here means one of two things and the message names both: the flag
// is really gone, or FLAGS.md is stale — which TestFlagsDocIsCurrent reports
// in the same run.
func TestDocumentedFlagsAreRegistered(t *testing.T) {
	documented, err := TableFlags(docFiles(t)...)
	if err != nil {
		t.Fatal(err)
	}
	if len(documented) == 0 {
		t.Fatal("no flags found in any doc table — the table pattern is broken")
	}
	registered, err := TableFlags(flagsDoc)
	if err != nil {
		t.Fatal(err)
	}
	if len(registered) < 50 {
		t.Fatalf("docs/FLAGS.md lists only %d flags; both binaries register far more, so the generated tables or the row pattern changed", len(registered))
	}
	for name, where := range documented {
		if _, ok := registered[name]; !ok {
			t.Errorf("%s documents -%s, which the generated docs/FLAGS.md does not list: neither binary registers it, or FLAGS.md is stale (regenerate with go test ./cmd/<binary> -run TestFlagsDocIsCurrent -update-flags-doc)", where, name)
		}
	}
}

// flagsDoc is the generated flag inventory, relative to this package.
var flagsDoc = filepath.Join("..", "..", "docs", "FLAGS.md")

// DocumentedFlags is the one skip rule the inventory and each binary's
// mention test share: the named harness flags and go test's own "test." flags
// are left out, everything else is in, in lexical order.
func TestDocumentedFlagsSkipsHarnessAndTestFlags(t *testing.T) {
	fs := flag.NewFlagSet("x", flag.ContinueOnError)
	fs.Bool("b-flag", false, "")
	fs.Bool("update-flags-doc", false, "")
	fs.Bool("test.v", false, "")
	fs.String("a-flag", "", "")
	fs.String("update-config-schema", "", "")
	var got []string
	for _, f := range DocumentedFlags(fs, "update-flags-doc", "update-config-schema") {
		got = append(got, f.Name)
	}
	if want := []string{"a-flag", "b-flag"}; !slices.Equal(got, want) {
		t.Errorf("DocumentedFlags = %q, want %q", got, want)
	}
}

// ParseFlagRow is FlagTable's inverse for the name and default cells, pinned by
// a round trip over the cell shapes FlagTable produces: a plain default, the
// `—` placeholder for none and for a map's zero value, and a default holding
// the pipe escapeCell escapes.
func TestParseFlagTableRoundTripsFlagTable(t *testing.T) {
	fs := flag.NewFlagSet("x", flag.ContinueOnError)
	fs.Duration("interval", 30*time.Second, "a duration | with a pipe")
	fs.String("empty", "", "no default")
	fs.String("piped", "a|b", "a pipe in the default")
	fs.Var(mapFlag{}, "headers", "a map flag")
	fs.Bool("harness", false, "skipped")
	got := ParseFlagTable("prose\n" + FlagTable(fs, "harness") + "| not | a row |\n")
	want := []FlagRow{
		{Name: "empty", Default: ""},
		{Name: "headers", Default: ""},
		{Name: "interval", Default: "30s"},
		{Name: "piped", Default: "a|b"},
	}
	if !slices.Equal(got, want) {
		t.Errorf("ParseFlagTable(FlagTable(fs)) = %+v, want %+v", got, want)
	}
	// And the real inventory parses: every row it holds reads back.
	b, err := os.ReadFile(flagsDoc)
	if err != nil {
		t.Fatal(err)
	}
	rows := ParseFlagTable(string(b))
	names, err := TableFlags(flagsDoc)
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	for _, r := range rows {
		seen[r.Name] = true
	}
	if len(rows) == 0 || len(seen) != len(names) {
		t.Errorf("ParseFlagTable read %d rows naming %d flags from docs/FLAGS.md, whose table rows name %d", len(rows), len(seen), len(names))
	}
}

// mapFlag renders the `map[]` zero value FlagTable documents as no default.
type mapFlag map[string]string

func (m mapFlag) String() string     { return fmt.Sprint(map[string]string(m)) }
func (m mapFlag) Set(v string) error { m[v] = v; return nil }

func TestTableFlagsReadsFirstCellOnly(t *testing.T) {
	// The description cell's cross-reference must not count as inventory.
	dir := t.TempDir()
	path := dir + "/doc.md"
	writeFile(t, path, "| `-real-flag` | `1` | see `-other-flag` |\n| (`-enrich`) | `true` | shared |\nprose about `-prose-flag`\n")
	got, err := TableFlags(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := got["real-flag"]; !ok {
		t.Errorf("first-cell flag missed: %v", got)
	}
	if _, ok := got["enrich"]; !ok {
		t.Errorf("parenthesized shared-flag row missed: %v", got)
	}
	for _, absent := range []string{"other-flag", "prose-flag"} {
		if _, ok := got[absent]; ok {
			t.Errorf("%s should not be inventory: %v", absent, got)
		}
	}
}

func TestFlagMentionedIsWordBounded(t *testing.T) {
	doc := "use `-logs-rate-limit` here"
	if !FlagMentioned(doc, "logs-rate-limit") {
		t.Error("exact mention missed")
	}
	if FlagMentioned(doc, "logs-rate") || FlagMentioned(doc, "rate-limit") {
		t.Error("substring matched as a mention")
	}
}

func TestFlagTableAndReplaceSection(t *testing.T) {
	fs := flag.NewFlagSet("x", flag.ContinueOnError)
	fs.Bool("b-flag", true, "a bool | with a pipe")
	fs.String("a-flag", "", "no default")
	table := FlagTable(fs, "skipped")
	want := "| Flag | Default | Description |\n|---|---|---|\n" +
		"| `-a-flag` | — | no default |\n" +
		"| `-b-flag` | `true` | a bool \\| with a pipe |\n"
	if table != want {
		t.Errorf("table:\n%s\nwant:\n%s", table, want)
	}

	doc := "head\n<!-- B -->\nold\n<!-- E -->\ntail"
	got, err := ReplaceSection(doc, "<!-- B -->", "<!-- E -->", "new")
	if err != nil {
		t.Fatal(err)
	}
	if got != "head\n<!-- B -->\n\nnew\n<!-- E -->\ntail" {
		t.Errorf("replaced = %q", got)
	}
	if _, err := ReplaceSection("no markers", "<!-- B -->", "<!-- E -->", "x"); err == nil {
		t.Error("missing marker: want error")
	}
}

func writeFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
}
