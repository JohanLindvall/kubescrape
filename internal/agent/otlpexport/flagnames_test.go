package otlpexport

// A remediation hint is copied into DaemonSet args by the operator it helps, so
// a flag it names that neither binary registers is worse than no hint: the
// fleet exits 2 on `flag provided but not defined`. Diagnose named
// -otlp-ca-file and -otlp-insecure-skip-verify (the flags are -otlp-tls-*) and
// a test pinned the misspelling. The guard is over the package's SOURCE rather
// than Diagnose's outputs, so a new hint arm, a warning's `flag=` value or a
// comment naming a flag is covered the day it is written.

import (
	"flag"
	"os"
	"regexp"
	"sort"
	"strings"
	"testing"

	"github.com/JohanLindvall/kubescrape/internal/cli"
)

// perBinaryOTLPFlags are the -otlp-* flags ONE binary registers beside the
// shared block. They live in the two mains, which this package cannot import;
// each is covered there by TestFlagsAreMentionedInConfigurationDoc and the
// generated docs/FLAGS.md, so a rename there fails that side.
var perBinaryOTLPFlags = map[string]string{
	"otlp-retry-attempts": "kubescrape-agent",
	"otlp-retry-backoff":  "kubescrape-agent",
	"otlp-max-send-bytes": "kubescrape-agent",
	"otlp-header":         "kubescrape",
}

func TestEveryOTLPFlagThisPackageNamesIsRegistered(t *testing.T) {
	fs := flag.NewFlagSet("otlp", flag.ContinueOnError)
	cli.RegisterOTLPFlags(fs, "")
	registered := func(name string) bool {
		if fs.Lookup(name) != nil {
			return true
		}
		_, ok := perBinaryOTLPFlags[name]
		return ok
	}

	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	flagToken := regexp.MustCompile(`-otlp-[a-z0-9-]*[a-z0-9]`)
	seen := map[string][]string{}
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		src, err := os.ReadFile(name)
		if err != nil {
			t.Fatal(err)
		}
		for _, tok := range flagToken.FindAllString(string(src), -1) {
			seen[tok[1:]] = append(seen[tok[1:]], name)
		}
	}
	if len(seen) == 0 {
		t.Fatal("found no -otlp-* token in the package source; the scan is not reading what it thinks it is")
	}
	var bad []string
	for name, files := range seen {
		if !registered(name) {
			bad = append(bad, "-"+name+" (in "+strings.Join(files, ", ")+")")
		}
	}
	sort.Strings(bad)
	for _, b := range bad {
		t.Errorf("the package names %s, which neither binary registers — an operator copying it gets `flag provided but not defined`", b)
	}
}
