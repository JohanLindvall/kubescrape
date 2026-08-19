package obs

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// Some help strings enumerate, exhaustively, the CAUSES that move a counter
// that has NO LABELS at all. TestHelpEnumeratingLabelValuesNamesThemAll cannot
// see those: it reads a help's enumeration of LABEL VALUES against the values
// the call sites emit, and here there are no label values to read. The help is
// still the only description the series has (docs/METRICS.md is generated from
// it), so a cause the code increments and the enumeration omits sends an
// operator after the wrong knob.
//
// This had happened. kubescrape_ingest_rejected_total's help named two causes —
// "concurrent in-flight pushes or buffered payload bytes" — after a third,
// Server.chargeDecoded's decoded-structure budget, had started incrementing the
// same counter. An operator whose senders batch harder sees 429s and this
// counter climbing, reads the help, and raises -ingest-max-in-flight and the
// body cap, neither of which touches the decoded budget.
//
// The check is deliberately NOT "count the increment sites and compare that to
// a number in the help": sites and causes are not one to one (the raw byte
// budget is refused once per transport, in two files). It pins the SITES
// instead, each mapped to the cause it belongs to, and requires the help to
// name every cause in the map. A new site therefore fails here, and the only
// way to make it pass is to decide which cause it is and check that the help
// says so — which is exactly the reading that was skipped.
//
// What it cannot see, so silence is not mistaken for coverage: a cause whose
// site is already listed but whose MEANING changed, and any prose in the help
// beyond the phrases named below.
func TestUnlabelledCauseEnumerationsNameEveryIncrementSite(t *testing.T) {
	// obs var -> "<repo-relative file>:<enclosing func>" -> the phrase the help
	// must carry for that cause. Sites sharing a cause share a phrase. The
	// match is case-insensitive, so a help may SHOUT a cause without this
	// having an opinion about it.
	causeSites := map[string]map[string]string{
		"IngestRejected": {
			"internal/agent/otlpingest/server.go:(*Server).acquire":       "-ingest-max-in-flight",
			"internal/agent/otlpingest/server.go:(*Server).chargeDecoded": "decoded budget",
			"internal/agent/otlpingest/admit.go:(*Server).tapAdmit":       "raw byte budget",
			"internal/agent/otlpingest/httpbody.go:(*BodyReader).Read":    "raw byte budget",
		},
	}
	// The metric each var registers, so a failure names the series an operator
	// would read rather than a Go identifier.
	varToMetric := map[string]string{"IngestRejected": "kubescrape_ingest_rejected_total"}

	docs, err := ParseMetricDocs("obs.go")
	if err != nil {
		t.Fatalf("ParseMetricDocs: %v", err)
	}
	help := map[string]string{}
	for _, d := range docs {
		help[d.Name] = d.Help
	}

	found := map[string]map[string]bool{}
	err = filepath.WalkDir("../..", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		if d.IsDir() {
			switch d.Name() {
			case ".git", "vendor", "bin":
				return filepath.SkipDir
			}
			return nil
		}
		// Tests bump counters to assert on them; only production code defines
		// what actually moves the series in a cluster.
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		fset := token.NewFileSet()
		af, perr := parser.ParseFile(fset, path, nil, parser.SkipObjectResolution)
		if perr != nil {
			// Another package mid-edit must not fail this test; the vacuity
			// guard below is what keeps a broken scan from passing silently.
			t.Logf("skipping unparseable %s: %v", path, perr)
			return nil
		}
		rel := filepath.ToSlash(strings.TrimPrefix(filepath.ToSlash(path), "../../"))
		for _, decl := range af.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok {
				continue
			}
			ast.Inspect(fn, func(n ast.Node) bool {
				call, ok := n.(*ast.CallExpr)
				if !ok {
					return true
				}
				sel, ok := call.Fun.(*ast.SelectorExpr)
				if !ok || (sel.Sel.Name != "Inc" && sel.Sel.Name != "Add") {
					return true
				}
				recv, ok := sel.X.(*ast.SelectorExpr)
				if !ok {
					return true
				}
				pkg, ok := recv.X.(*ast.Ident)
				if !ok || pkg.Name != "obs" {
					return true
				}
				v := recv.Sel.Name
				if _, watched := causeSites[v]; !watched {
					return true
				}
				if found[v] == nil {
					found[v] = map[string]bool{}
				}
				found[v][rel+":"+causeFuncIdent(fn)] = true
				return true
			})
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walking the repo: %v", err)
	}

	for _, v := range causeSortedVars(causeSites) {
		want := causeSites[v]
		got := found[v]
		// Vacuity guard: this is a search, and a search that finds nothing
		// passes every assertion below.
		if len(got) == 0 {
			t.Fatalf("obs.%s: found no increment sites at all — the scan is broken, not the help string", v)
		}
		for _, site := range causeSortedSiteKeys(got) {
			if _, ok := want[site]; !ok {
				t.Errorf("obs.%s (%s): %s increments it and is not in causeSites.\n"+
					"Decide which cause it is, check that the help ENUMERATES that cause "+
					"— the help is the only description this unlabelled series has — and list the site here.",
					v, varToMetric[v], site)
			}
		}
		for _, site := range causeSortedPhraseKeys(want) {
			if !got[site] {
				t.Errorf("obs.%s (%s): causeSites lists %s, which no longer increments it. "+
					"Drop the entry, and drop the cause from the help if nothing else raises it.",
					v, varToMetric[v], site)
			}
		}
		h := help[varToMetric[v]]
		if h == "" {
			t.Fatalf("obs.%s: no help found for %s — the doc extractor is broken", v, varToMetric[v])
		}
		seen := map[string]bool{}
		for _, site := range causeSortedPhraseKeys(want) {
			phrase := want[site]
			if seen[phrase] {
				continue
			}
			seen[phrase] = true
			if !strings.Contains(strings.ToLower(h), strings.ToLower(phrase)) {
				t.Errorf("obs.%s (%s): %s increments it for a cause the help does not name (%q missing).\n"+
					"An operator reading only this help chases the wrong bound.",
					v, varToMetric[v], site, phrase)
			}
		}
	}
}

// causeFuncIdent renders a FuncDecl as it is written, receiver included, so a site
// key reads like the declaration a reviewer will go and open.
func causeFuncIdent(fn *ast.FuncDecl) string {
	if fn.Recv == nil || len(fn.Recv.List) == 0 {
		return fn.Name.Name
	}
	return "(" + causeRecvString(fn.Recv.List[0].Type) + ")." + fn.Name.Name
}

func causeRecvString(e ast.Expr) string {
	switch t := e.(type) {
	case *ast.StarExpr:
		return "*" + causeRecvString(t.X)
	case *ast.Ident:
		return t.Name
	case *ast.IndexExpr: // a generic receiver, Store[T]
		return causeRecvString(t.X)
	case *ast.SelectorExpr:
		return causeRecvString(t.X) + "." + t.Sel.Name
	}
	return "?"
}

func causeSortedVars(m map[string]map[string]string) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func causeSortedPhraseKeys(m map[string]string) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func causeSortedSiteKeys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
