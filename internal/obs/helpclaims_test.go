package obs

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
)

// A help string that ENUMERATES the values of one of its labels is read as
// exhaustive — it is the only description an operator has, since docs/METRICS.md
// is generated from it — so a value the code emits and the enumeration omits is
// a live series that no documentation admits exists. An operator's dashboard
// filters the documented domain and hides exactly that series.
//
// This had happened: kubescrape_scrape_samples_dropped_total documented
// `filter` and `relabel` (the two an operator CONFIGURED) while the converter
// also emitted `accumulator` — the one reason that is a refusal of OURS rather
// than a rule somebody wrote, i.e. the only one of the three worth alerting on.
//
// Neither existing guard can see it. TestDocumentedMetricsExist checks label
// NAMES (it truncates a documented selector at the first of `=!~`), and
// TestMetricsDocIsCurrent only keeps the generated file in step with whatever
// obs.go happens to say. So the domain is checked here, against the call sites.
//
// The rule is deliberately SELF-SCOPING — it fires only where the help already
// names at least one value of that label position. A terse help that enumerates
// nothing (kubescrape_export_requests_total's `signal`, say) promises nothing
// and is left alone; the defect is a PARTIAL enumeration, which reads as
// complete.
//
// What it cannot see, said plainly so silence is not mistaken for coverage: a
// value reached through a variable or a constant rather than written at the
// call site (BodyReader.noteRejected classifies into `reason` and passes the
// result; bodyRejectReasons is that domain's list). Resolving constants was
// tried and dropped — comparing a constant's VALUE against the help brings back
// the substring problem namesWord exists to avoid, and a guard that cries wolf
// gets loosened rather than obeyed.
func TestHelpEnumeratingLabelValuesNamesThemAll(t *testing.T) {
	docs, err := ParseMetricDocs("obs.go")
	if err != nil {
		t.Fatalf("ParseMetricDocs: %v", err)
	}
	byName := map[string]MetricDoc{}
	for _, d := range docs {
		byName[d.Name] = d
	}
	// The Go identifier each registration is assigned to — call sites name
	// obs.Ingested, not kubescrape_ingest_resources_total.
	varToMetric, err := registrationVars("obs.go")
	if err != nil {
		t.Fatalf("registrationVars: %v", err)
	}
	if len(varToMetric) == 0 {
		t.Fatal("no registrations found — the extractor is broken, not the help strings")
	}

	// metric var -> argument position -> value -> the file that writes it.
	emitted := map[string]map[int]map[string]string{}
	total := 0

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
		// Tests pass arbitrary values (reading a counter back is how half of
		// them assert), so only production call sites define the domain.
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		fset := token.NewFileSet()
		af, perr := parser.ParseFile(fset, path, nil, parser.SkipObjectResolution)
		if perr != nil {
			// Another package mid-edit must not fail this test; the vacuity
			// check below is what keeps a broken scan from passing silently.
			t.Logf("skipping unparseable %s: %v", path, perr)
			return nil
		}
		ast.Inspect(af, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			sel, ok := call.Fun.(*ast.SelectorExpr)
			if !ok || sel.Sel.Name != "WithLabelValues" {
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
			if _, ok := varToMetric[v]; !ok {
				return true
			}
			for i, a := range call.Args {
				lit, ok := constString(a)
				if !ok {
					continue
				}
				if emitted[v] == nil {
					emitted[v] = map[int]map[string]string{}
				}
				if emitted[v][i] == nil {
					emitted[v][i] = map[string]string{}
				}
				emitted[v][i][lit] = filepath.ToSlash(path)
				total++
			}
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatalf("walking the repo: %v", err)
	}
	// Vacuity guard: this is a search, and a search that finds nothing passes.
	// There are dozens of literal call sites; a handful means the walk or the
	// matching broke, not that the help strings got better.
	if total < 20 {
		t.Fatalf("found only %d literal label values across the repo — the scan is broken, not the help strings", total)
	}

	for _, v := range sortedKeys(emitted) {
		d, ok := byName[varToMetric[v]]
		if !ok {
			continue
		}
		for _, idx := range sortedIntKeys(emitted[v]) {
			label := "argument " + strconv.Itoa(idx)
			if idx < len(d.Labels) {
				label = d.Labels[idx]
			}
			var named, missing []string
			for _, val := range sortedKeys(emitted[v][idx]) {
				if namesWord(d.Help, val) {
					named = append(named, val)
				} else {
					missing = append(missing, val)
				}
			}
			if len(named) == 0 || len(missing) == 0 {
				continue // enumerates nothing, or enumerates everything
			}
			t.Errorf("%s{%s}: the help enumerates %v but the code also emits %v — a partial enumeration reads "+
				"as complete, so those series are undocumented in docs/METRICS.md, which is generated from this "+
				"string. Name them, or stop naming the others.\n\tfirst seen in: %s",
				d.Name, label, named, missing, emitted[v][idx][missing[0]])
		}
	}
}

// registrationVars maps the identifier a Registry.* registration is assigned to
// onto the metric name it registers. ParseMetricDocs deliberately does not keep
// it (docs/METRICS.md names metrics, not variables), and the call sites name
// only the identifier.
func registrationVars(filename string) (map[string]string, error) {
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, filename, nil, parser.SkipObjectResolution)
	if err != nil {
		return nil, err
	}
	out := map[string]string{}
	ast.Inspect(f, func(n ast.Node) bool {
		spec, ok := n.(*ast.ValueSpec)
		if !ok || len(spec.Names) != 1 || len(spec.Values) != 1 {
			return true
		}
		call, ok := spec.Values[0].(*ast.CallExpr)
		if !ok {
			return true
		}
		sel, ok := call.Fun.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		if id, ok := sel.X.(*ast.Ident); !ok || id.Name != "Registry" {
			return true
		}
		if len(call.Args) == 0 {
			return true
		}
		if name, ok := constString(call.Args[0]); ok && strings.HasPrefix(name, "kubescrape_") {
			out[spec.Names[0].Name] = name
		}
		return true
	})
	return out, nil
}

// constString evaluates a string literal, including the `"a"+"b"` form gofmt
// leaves behind (the same rule as metricdoc.go's stringLit, which is unexported
// there and takes only what its own caller needs).
func constString(e ast.Expr) (string, bool) {
	switch v := e.(type) {
	case *ast.BasicLit:
		if v.Kind != token.STRING {
			return "", false
		}
		s, err := strconv.Unquote(v.Value)
		return s, err == nil
	case *ast.BinaryExpr:
		if v.Op != token.ADD {
			return "", false
		}
		l, lok := constString(v.X)
		r, rok := constString(v.Y)
		if lok && rok {
			return l + r, true
		}
	case *ast.ParenExpr:
		return constString(v.X)
	}
	return "", false
}

// namesWord reports whether help mentions val as a WHOLE token. Substring
// matching would read the "summary" in "histogram/summary family" as the
// `summary` pipeline value and report a partial enumeration that is not there —
// and a false alarm on a guard like this is worse than no guard, because the
// fix everyone reaches for is to loosen it.
func namesWord(help, val string) bool {
	if val == "" {
		return false
	}
	for i := 0; i <= len(help)-len(val); {
		j := strings.Index(help[i:], val)
		if j < 0 {
			return false
		}
		start := i + j
		if !identByte(help, start-1) && !identByte(help, start+len(val)) {
			return true
		}
		i = start + 1
	}
	return false
}

// identByte reports whether help[i] continues an identifier-like token. `.` and
// `_` count: metric and attribute names are written with both, and `peer_ip`
// must not match inside `peer_ip_rejected`.
func identByte(help string, i int) bool {
	if i < 0 || i >= len(help) {
		return false
	}
	c := help[i]
	return c == '_' || c == '.' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9')
}

func sortedKeys[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func sortedIntKeys[V any](m map[int]V) []int {
	out := make([]int, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Ints(out)
	return out
}

// Three help strings described a counter that in fact does something else, and
// each sent an operator looking in a different wrong place. They are pinned
// LITERALLY — the retired sentence, and one fragment of the fact that replaced
// it — because there is nothing structural left to check here: a compiler
// cannot tell a true sentence from a plausible one, and all three were
// plausible. Rewording is fine and expected; it just has to be a deliberate act
// that comes back to this table, which is the whole point of the pin.
//
// The same review's fourth defect of this shape — a reason value the help never
// listed at all — is pinned structurally instead, by
// TestHelpEnumeratingLabelValuesNamesThemAll above. Prefer that form when a fix
// admits it; this one only catches the mistake it already knows about.
func TestRetiredHelpClaimsStayRetired(t *testing.T) {
	docs, err := ParseMetricDocs("obs.go")
	if err != nil {
		t.Fatalf("ParseMetricDocs: %v", err)
	}
	byName := map[string]MetricDoc{}
	for _, d := range docs {
		byName[d.Name] = d
	}

	cases := []struct {
		metric  string
		retired string // the claim that was wrong
		why     string // why it was wrong, so a reword does not restore it by accident
		nowSays string // a fragment of what the code actually does
	}{
		{
			metric:  "kubescrape_summary_unresolved_total",
			retired: "the pod resolved, but the summary named a container it does not list",
			why: "that case returns resolved=true from resolveContext and increments NOTHING. The counter " +
				"keys on the ABSENCE of container.id — which is what the join to the cadvisor row is made " +
				"of — and its dominant real cause is a container the pod document lists from the SPEC " +
				"while its status has not reached the API server yet",
			nowSays: "container.id",
		},
		{
			metric:  "kubescrape_ingest_log_chain_skipped_total",
			retired: "line-derived processing (body enrichment, log-metrics observation) was skipped",
			why: "no reason skips both halves. resource_too_wide and resources_capped clear only `observe`, " +
				"so an operator reading this hunted for missing log.template / exception.* / trace linkage " +
				"that was in fact present, while the real symptom — the sender's missing log-metric series " +
				"— was the other half of the same sentence",
			nowSays: "OBSERVATION only",
		},
		{
			metric:  "kubescrape_ingest_resources_total",
			retired: "Distinct pushed identities",
			why: "no path counts identities. The resource path counts one per pushed ResourceLogs/" +
				"ResourceMetrics — five resources naming ONE container id count five, the memo being on the " +
				"lookup and not on the counter — and only the datapoint/split path is once per described " +
				"object, so an attribution ratio built from this read 5x on the default mode",
			nowSays: "one count per PUSHED",
		},
	}

	for _, c := range cases {
		d, ok := byName[c.metric]
		if !ok {
			t.Errorf("%s is not registered any more; if it was renamed, move this pin with it", c.metric)
			continue
		}
		if strings.Contains(d.Help, c.retired) {
			t.Errorf("%s's help says %q again.\n\tIt is wrong because: %s", c.metric, c.retired, c.why)
		}
		if !strings.Contains(d.Help, c.nowSays) {
			t.Errorf("%s's help no longer says %q, which is what it actually does.\n\tThe claim that replaced: %s",
				c.metric, c.nowSays, c.why)
		}
	}
}
