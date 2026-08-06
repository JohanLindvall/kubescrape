package main

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"strings"
	"testing"
)

// Defers run LIFO, so run()'s registration order IS its shutdown order,
// reversed: everything registered BEFORE the producer drain runs AFTER it. That
// is the whole point of the drain — stop and join every started goroutine
// before the exporter, the spools and the sibling-shard clients are closed
// under it — and it is enforced by nothing but where a line sits. The
// resharder's Close was registered below the drain and so ran FIRST, which any
// new early return between building it and starting the receivers would turn
// into a close under a live goroutine.
//
// Source order is the property, so the test reads the source.
func TestEveryCloseIsRegisteredBeforeTheProducerDrain(t *testing.T) {
	const file = "main.go"
	src, err := os.ReadFile(file)
	if err != nil {
		t.Fatal(err)
	}
	fset := token.NewFileSet()
	parsed, err := parser.ParseFile(fset, file, src, 0)
	if err != nil {
		t.Fatal(err)
	}
	var run *ast.FuncDecl
	for _, d := range parsed.Decls {
		if fn, ok := d.(*ast.FuncDecl); ok && fn.Recv == nil && fn.Name.Name == "run" {
			run = fn
		}
	}
	if run == nil {
		t.Fatalf("no run() in %s", file)
	}

	text := func(n ast.Node) string {
		return string(src[fset.Position(n.Pos()).Offset:fset.Position(n.End()).Offset])
	}
	var drain ast.Node
	var closes []ast.Node
	ast.Inspect(run.Body, func(n ast.Node) bool {
		d, ok := n.(*ast.DeferStmt)
		if !ok {
			return true
		}
		switch s := text(d); {
		case strings.Contains(s, "waitFor(&wg"):
			drain = d
		case strings.Contains(s, ".Close()"):
			closes = append(closes, d)
		}
		return true
	})
	if drain == nil {
		t.Fatal("no producer-drain defer (waitFor(&wg, ...)) in run()")
	}
	if len(closes) == 0 {
		t.Fatal("no Close defers found in run(); the check would pass vacuously")
	}
	for _, c := range closes {
		if c.Pos() > drain.Pos() {
			t.Errorf("%s: registered after the producer drain, so LIFO closes it BEFORE the goroutines using it stop:\n%s",
				fset.Position(c.Pos()), text(c))
		}
	}
}
