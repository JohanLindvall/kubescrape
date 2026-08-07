// Package docscheck keeps the flag documentation and the flag sets from
// drifting apart, in both directions:
//
//   - docs/FLAGS.md is GENERATED from each binary's registered flags
//     (FlagTable + ReplaceSection, driven by flagsdoc_test.go in each cmd
//     package with an -update-flags-doc flag — the METRICS.md pattern).
//   - Every flag named in a markdown TABLE ROW of the hand-written docs must
//     exist in one of the binaries (TableFlags vs SourceFlags, asserted by
//     this package's own test). That is how a renamed or deleted flag stops
//     being quietly documented — the prose sibling of internal/manifestcheck,
//     which does the same for the shipped manifests.
//
// The hand-written docs are read as TEXT and the registrations are parsed from
// SOURCE (both cmd trees, tagged files included), so the test needs neither
// build tags nor a running binary and covers both binaries from one package.
package docscheck

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
)

// tableFlagPattern matches a markdown table row whose first cell names a flag:
//
//	| `-scrape-interval` | `30s` | ... |
//
// Only the FIRST cell is read: flags mentioned in description cells are
// cross-references, not inventory, and prose mentions are deliberately out of
// scope (bounded-word matching over prose is what FlagMentioned is for).
var tableFlagPattern = regexp.MustCompile("(?m)^\\|\\s*\\(?`--?([A-Za-z0-9][A-Za-z0-9-]*)")

// registerPattern matches a flag registration in source. The name is always a
// string literal in this repo (a computed name could not be matched — nothing
// registers one, and the FLAGS.md generator would catch the omission anyway).
var registerPattern = regexp.MustCompile(`flag\.(?:String|Bool|Int64|Int|Uint64|Uint|Float64|Duration|Var|Func|TextVar)\(\s*(?:&[A-Za-z0-9_.]+,\s*)?"([A-Za-z0-9][A-Za-z0-9-]*)"`)

// TableFlags returns the flag names documented in markdown table rows of the
// given files, deduplicated, with the file and line of the first occurrence.
func TableFlags(paths ...string) (map[string]string, error) {
	out := map[string]string{}
	for _, path := range paths {
		b, err := os.ReadFile(path)
		if err != nil {
			return nil, err
		}
		for _, loc := range tableFlagPattern.FindAllSubmatchIndex(b, -1) {
			name := string(b[loc[2]:loc[3]])
			// `-otlp-*`-style rows cross-reference a flag FAMILY documented
			// elsewhere; the trailing dash is the wildcard's stem, not a flag.
			if strings.HasSuffix(name, "-") {
				continue
			}
			if _, seen := out[name]; !seen {
				line := 1 + strings.Count(string(b[:loc[0]]), "\n")
				out[name] = fmt.Sprintf("%s:%d", path, line)
			}
		}
	}
	return out, nil
}

// SourceFlags returns the union of flag names registered by the .go files
// (tests excluded) under the given directories.
func SourceFlags(dirs ...string) (map[string]bool, error) {
	out := map[string]bool{}
	for _, dir := range dirs {
		entries, err := os.ReadDir(dir)
		if err != nil {
			return nil, err
		}
		for _, e := range entries {
			if e.IsDir() || !strings.HasSuffix(e.Name(), ".go") || strings.HasSuffix(e.Name(), "_test.go") {
				continue
			}
			b, err := os.ReadFile(filepath.Join(dir, e.Name()))
			if err != nil {
				return nil, err
			}
			for _, m := range registerPattern.FindAllSubmatch(b, -1) {
				out[string(m[1])] = true
			}
		}
	}
	return out, nil
}

// CmdDirs are the two binaries' source directories, relative to this package.
var CmdDirs = []string{"../../cmd/kubescrape", "../../cmd/kubescrape-agent"}

// FlagMentioned reports whether doc names the flag anywhere, as a whole word
// preceded by a dash — `-scrape-interval` matches, `-scrape-interval-x` and
// `-logs-scrape-interval` do not. This is the "every registered flag appears
// in CONFIGURATION.md" direction, where a table row is not required (some
// flags are documented in prose).
func FlagMentioned(doc, name string) bool {
	re := regexp.MustCompile(`(^|[^A-Za-z0-9-])--?` + regexp.QuoteMeta(name) + `($|[^A-Za-z0-9-])`)
	return re.MatchString(doc)
}

// FlagTable renders the markdown table for every flag in fs, sorted by name.
// Flags named in skip (test harness flags like -update-flags-doc) are omitted,
// as is everything under the "test." prefix that `go test` registers.
func FlagTable(fs *flag.FlagSet, skip ...string) string {
	skipSet := map[string]bool{}
	for _, s := range skip {
		skipSet[s] = true
	}
	var rows []string
	fs.VisitAll(func(f *flag.Flag) {
		if skipSet[f.Name] || strings.HasPrefix(f.Name, "test.") {
			return
		}
		def := "—"
		// "map[]" is what a flag.Var over a map renders as its zero value —
		// an implementation artifact, not a default worth documenting.
		if f.DefValue != "" && f.DefValue != "map[]" {
			def = "`" + escapeCell(f.DefValue) + "`"
		}
		rows = append(rows, fmt.Sprintf("| `-%s` | %s | %s |", f.Name, def, escapeCell(f.Usage)))
	})
	sort.Strings(rows)
	return "| Flag | Default | Description |\n|---|---|---|\n" + strings.Join(rows, "\n") + "\n"
}

// escapeCell makes an arbitrary usage string safe inside one table cell.
func escapeCell(s string) string {
	s = strings.ReplaceAll(s, "\n", " ")
	return strings.ReplaceAll(s, "|", "\\|")
}

// ReplaceSection substitutes the region between the begin and end marker lines
// (exclusive) with content, and errors if either marker is missing — a doc
// edit that loses a marker must fail the generator loudly, not regenerate the
// whole file around it.
func ReplaceSection(doc, begin, end, content string) (string, error) {
	bi := strings.Index(doc, begin)
	if bi < 0 {
		return "", fmt.Errorf("marker %q not found", begin)
	}
	rest := doc[bi+len(begin):]
	ei := strings.Index(rest, end)
	if ei < 0 {
		return "", fmt.Errorf("marker %q not found after %q", end, begin)
	}
	return doc[:bi+len(begin)] + "\n\n" + content + "\n" + rest[ei:], nil
}
