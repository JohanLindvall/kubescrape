package main

import (
	"flag"
	"os"
	"strings"
	"testing"

	"github.com/JohanLindvall/kubescrape/internal/docscheck"
)

var updateFlagsDoc = flag.Bool("update-flags-doc", false, "rewrite this binary's generated section of docs/FLAGS.md")

const (
	flagsDocPath   = "../../docs/FLAGS.md"
	flagsBeginMark = "<!-- BEGIN GENERATED: kubescrape-agent flags -->"
	flagsEndMark   = "<!-- END GENERATED: kubescrape-agent flags -->"
)

// The agent's section of docs/FLAGS.md is generated from the registered flag
// set, so a new flag, a rename, or a changed default fails here until the doc
// is regenerated — defaults in the doc cannot drift, because they are read
// from the same registrations the binary parses.
func TestFlagsDocIsCurrent(t *testing.T) {
	doc, err := os.ReadFile(flagsDocPath)
	if err != nil {
		t.Fatal(err)
	}
	table := docscheck.FlagTable(flag.CommandLine, "update-flags-doc", "update-config-schema")
	want, err := docscheck.ReplaceSection(string(doc), flagsBeginMark, flagsEndMark, table)
	if err != nil {
		t.Fatal(err)
	}
	if want == string(doc) {
		return
	}
	if *updateFlagsDoc {
		if err := os.WriteFile(flagsDocPath, []byte(want), 0o644); err != nil {
			t.Fatal(err)
		}
		return
	}
	t.Errorf("docs/FLAGS.md is stale for kubescrape-agent; regenerate with:\n  go test ./cmd/kubescrape-agent -run TestFlagsDocIsCurrent -update-flags-doc")
}

// Every registered flag must be at least MENTIONED in CONFIGURATION.md — the
// narrative doc is where an operator learns what a flag is for, and a flag
// that exists only in the generated inventory is a feature nobody can find.
func TestFlagsAreMentionedInConfigurationDoc(t *testing.T) {
	doc, err := os.ReadFile("../../docs/CONFIGURATION.md")
	if err != nil {
		t.Fatal(err)
	}
	flag.CommandLine.VisitAll(func(f *flag.Flag) {
		if f.Name == "update-flags-doc" || f.Name == "update-config-schema" || strings.HasPrefix(f.Name, "test.") {
			return
		}
		if !docscheck.FlagMentioned(string(doc), f.Name) {
			t.Errorf("-%s is not mentioned anywhere in docs/CONFIGURATION.md", f.Name)
		}
	})
}
