package obs

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"sort"
	"strconv"
	"strings"
)

// MetricDoc describes one registered metric, as declared in obs.go.
type MetricDoc struct {
	Name   string
	Kind   string // Counter, CounterVec, Gauge, GaugeFunc, GaugeFuncVec, HistogramVec, CounterFunc
	Help   string
	Labels []string
}

// ParseMetricDocs extracts every Registry.* registration from a Go source
// file. It is the single source of truth behind both docs/METRICS.md and the
// test that keeps the prose honest.
//
// Names and label names are a public interface — dashboards and alert rules
// select on them, and a selector naming something that does not exist matches
// silently instead of erroring — so they are read from the AST rather than
// maintained by hand in two places.
func ParseMetricDocs(filename string) ([]MetricDoc, error) {
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, filename, nil, parser.ParseComments)
	if err != nil {
		return nil, fmt.Errorf("parsing %s: %w", filename, err)
	}

	var out []MetricDoc
	ast.Inspect(f, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
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
		if len(call.Args) < 2 {
			return true
		}
		name, ok := stringLit(call.Args[0])
		if !ok || !strings.HasPrefix(name, "kubescrape_") {
			return true
		}
		help, _ := stringLit(call.Args[1])

		md := MetricDoc{Name: name, Kind: sel.Sel.Name, Help: help}
		// Label names are the remaining string literals: a description is
		// argument 1, buckets are a composite literal and funcs are not
		// literals at all, so anything else that is a plain string is a label.
		for _, a := range call.Args[2:] {
			if v, ok := stringLit(a); ok {
				md.Labels = append(md.Labels, v)
			}
		}
		out = append(out, md)
		return true
	})
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

func stringLit(e ast.Expr) (string, bool) {
	bl, ok := e.(*ast.BasicLit)
	if !ok || bl.Kind != token.STRING {
		return "", false
	}
	v, err := strconv.Unquote(bl.Value)
	if err != nil {
		return "", false
	}
	return v, true
}
