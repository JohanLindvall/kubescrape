package docscheck

import (
	"flag"
	"os"
	"testing"
)

// Docs are the hand-written files whose table rows inventory flags.
var docs = []string{
	"../../docs/CONFIGURATION.md",
	"../../docs/FLAGS.md",
	"../../README.md",
}

// Every flag a doc table documents must be registered by one of the binaries.
// This is the direction TestFlagsDocIsCurrent (per-binary, generated) cannot
// cover: a flag deleted from the code stays in the hand-written tables with
// nothing else to notice — README documented `-runtime-metrics` long after
// the flag was gone, and only the manifests' copy of that drift had a guard.
func TestDocumentedFlagsAreRegistered(t *testing.T) {
	documented, err := TableFlags(docs...)
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
