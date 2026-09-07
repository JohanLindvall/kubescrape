package docscheck

import (
	"flag"
	"os"
	"path/filepath"
	"testing"
)

// docFiles are the files whose table rows inventory flags: EVERY markdown doc,
// found by glob, plus the README.
//
// It was a hardcoded three (CONFIGURATION.md, FLAGS.md, README.md), which meant
// the package doc's claim to cover "the hand-written docs" was false one file
// over: the `-buffer-dir` row in docs/FIRST-RUN.md — the operator-facing
// first-ten-minutes guide — was read by no guard in either direction, so
// renaming or deleting that flag would have left the row standing. That is the
// exact `-runtime-metrics` drift this package was written to stop. A glob also
// covers a doc that does not exist yet, which a list by construction cannot;
// the row pattern ignores prose, so a doc with no flag table costs nothing.
func docFiles(t *testing.T) []string {
	t.Helper()
	paths, err := filepath.Glob("../../docs/*.md")
	if err != nil {
		t.Fatal(err)
	}
	if len(paths) == 0 {
		t.Fatal("no docs/*.md matched — the glob is broken and every doc is unscanned")
	}
	return append(paths, "../../README.md")
}

// And the glob really is one: a doc added under docs/ is scanned without anyone
// remembering to list it.
func TestEveryMarkdownDocIsScannedForFlagRows(t *testing.T) {
	scanned := map[string]bool{}
	for _, p := range docFiles(t) {
		scanned[filepath.Base(p)] = true
	}
	entries, err := os.ReadDir("../../docs")
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if e.IsDir() || filepath.Ext(e.Name()) != ".md" {
			continue
		}
		if !scanned[e.Name()] {
			t.Errorf("docs/%s is not scanned for flag table rows; a flag it documents can be renamed or deleted with nothing to notice", e.Name())
		}
	}
	if !scanned["README.md"] {
		t.Error("README.md is not scanned; it is where -runtime-metrics survived its own deletion")
	}
}

// Every flag a doc table documents must be registered by one of the binaries.
// This is the direction TestFlagsDocIsCurrent (per-binary, generated) cannot
// cover: a flag deleted from the code stays in the hand-written tables with
// nothing else to notice — README documented `-runtime-metrics` long after
// the flag was gone, and only the manifests' copy of that drift had a guard.
func TestDocumentedFlagsAreRegistered(t *testing.T) {
	documented, err := TableFlags(docFiles(t)...)
	if err != nil {
		t.Fatal(err)
	}
	if len(documented) == 0 {
		t.Fatal("no flags found in any doc table — the table pattern is broken")
	}
	registered, err := SourceFlags(CmdDirs...)
	if err != nil {
		t.Fatal(err)
	}
	if len(registered) == 0 {
		t.Fatal("no flag registrations found — the registration pattern is broken")
	}
	for name, where := range documented {
		if !registered[name] {
			t.Errorf("%s documents -%s, which neither binary registers", where, name)
		}
	}
}

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
