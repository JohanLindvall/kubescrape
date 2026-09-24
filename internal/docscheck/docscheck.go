// Package docscheck keeps the configuration documentation and the code it
// documents from drifting apart. Two of its guards are about FLAGS, in both
// directions; the third is about the agent's -config file:
//
//   - docs/FLAGS.md is GENERATED from each binary's registered flags
//     (FlagTable + ReplaceSection, driven by flagsdoc_test.go in each cmd
//     package with an -update-flags-doc flag — the METRICS.md pattern).
//   - Every flag named in a markdown TABLE ROW of the hand-written docs must
//     exist in one of the binaries (TableFlags over the docs vs TableFlags over
//     the generated docs/FLAGS.md, asserted by this package's own test). That
//     is how a renamed or deleted flag stops being quietly documented — the
//     prose sibling of internal/manifestcheck, which does the same for the
//     shipped manifests.
//   - docs/agent-config.schema.json is GENERATED from the agent's config
//     structs (ConfigSchema, jsonschema.go, driven by cmd/kubescrape-agent's
//     configschema_test.go with an -update-config-schema flag). It lives here
//     rather than in a package of its own for FlagTable's reason: both render
//     a documentation artefact from the code it describes, so neither can
//     drift from it.
//
// Everything is read as TEXT, so the test needs neither build tags nor a
// running binary and covers both binaries from one package. The registered set
// is docs/FLAGS.md itself: it is generated from the flag sets the binaries
// actually parse, TestFlagsDocIsCurrent holds it current in every build variant
// CI runs, and the optional-pipeline flags are registered on every variant, so
// it lists exactly what is registered. (It used to be re-derived by a regular
// expression over the cmd SOURCE, comments included — a second derivation of
// the same set that had already missed the flag.XxxVar spellings once, and
// could fail OPEN: a comment quoting a removed registration kept its stale doc
// row green.)
package docscheck

import (
	"flag"
	"fmt"
	"os"
	"regexp"
	"slices"
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

// FlagMentioned reports whether doc names the flag anywhere, as a whole word
// preceded by a dash — `-scrape-interval` matches, `-scrape-interval-x` and
// `-logs-scrape-interval` do not. This is the "every registered flag appears
// in CONFIGURATION.md" direction, where a table row is not required (some
// flags are documented in prose).
func FlagMentioned(doc, name string) bool {
	re := regexp.MustCompile(`(^|[^A-Za-z0-9-])--?` + regexp.QuoteMeta(name) + `($|[^A-Za-z0-9-])`)
	return re.MatchString(doc)
}

// DocumentedFlags returns the flags of fs that the documentation covers, in
// lexical order: every flag except those named in skip (the test binary's own
// harness flags, like -update-flags-doc) and everything under the "test."
// prefix that `go test` registers. It is the ONE statement of that rule, read
// by FlagTable and by each binary's every-flag-is-mentioned test alike, so the
// generated inventory and the mention check cannot disagree about which flags
// exist.
func DocumentedFlags(fs *flag.FlagSet, skip ...string) []*flag.Flag {
	var out []*flag.Flag
	fs.VisitAll(func(f *flag.Flag) {
		if slices.Contains(skip, f.Name) || strings.HasPrefix(f.Name, "test.") {
			return
		}
		out = append(out, f)
	})
	return out
}

// FlagTable renders the markdown table for every flag DocumentedFlags returns,
// sorted by row. ParseFlagRow reads a row back.
func FlagTable(fs *flag.FlagSet, skip ...string) string {
	var rows []string
	for _, f := range DocumentedFlags(fs, skip...) {
		def := "—"
		// "map[]" is what a flag.Var over a map renders as its zero value —
		// an implementation artifact, not a default worth documenting.
		if f.DefValue != "" && f.DefValue != "map[]" {
			def = "`" + escapeCell(f.DefValue) + "`"
		}
		rows = append(rows, fmt.Sprintf("| `-%s` | %s | %s |", f.Name, def, escapeCell(f.Usage)))
	}
	slices.Sort(rows)
	return "| Flag | Default | Description |\n|---|---|---|\n" + strings.Join(rows, "\n") + "\n"
}

// escapeCell makes an arbitrary usage string safe inside one table cell.
func escapeCell(s string) string {
	s = strings.ReplaceAll(s, "\n", " ")
	return strings.ReplaceAll(s, "|", "\\|")
}

// FlagRow is the part of one FlagTable row a consumer reads back: the flag's
// name and its documented default.
type FlagRow struct {
	Name string
	// Default is the default cell unescaped, or "" for the `—` placeholder
	// (no default, or a map's `map[]` zero value).
	Default string
}

// flagRowPattern matches one row as FlagTable renders it.
var flagRowPattern = regexp.MustCompile("^\\|\\s*`-([A-Za-z0-9][A-Za-z0-9-]*)`\\s*\\|\\s*(?:`([^`]*)`|—)\\s*\\|")

// ParseFlagRow reads one line of docs/FLAGS.md back as the row FlagTable
// rendered, and reports whether it is one. It is FlagTable's inverse for the
// name and default cells, so a reader of the generated inventory (the chart
// guards in internal/chartcheck take their defaults from it) follows the
// format from the one package that writes it rather than from a copy.
func ParseFlagRow(line string) (FlagRow, bool) {
	m := flagRowPattern.FindStringSubmatch(line)
	if m == nil {
		return FlagRow{}, false
	}
	return FlagRow{Name: m[1], Default: strings.ReplaceAll(m[2], "\\|", "|")}, true
}

// ParseFlagTable returns every FlagTable row in doc, in document order. A flag
// both binaries register has a row in each binary's section.
func ParseFlagTable(doc string) []FlagRow {
	var out []FlagRow
	for line := range strings.SplitSeq(doc, "\n") {
		if r, ok := ParseFlagRow(line); ok {
			out = append(out, r)
		}
	}
	return out
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
