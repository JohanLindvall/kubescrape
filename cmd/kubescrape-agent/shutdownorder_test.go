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
		case strings.Contains(s, "cli.WaitFor(&wg"):
			drain = d
		case strings.Contains(s, ".Close()"):
			closes = append(closes, d)
		}
		return true
	})
	if drain == nil {
		t.Fatal("no producer-drain defer (cli.WaitFor(&wg, ...)) in run()")
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

// The other half of the same invariant, and the half the comment above the
// drain defer asserted while the code broke it: nothing may be STARTED above
// that defer either.
//
// The disk buffer's Run and the transform watcher used to be spawned where they
// are built, which is above the route-client and transform-compile early
// returns. Those returns then ran the exporter and spool Close defers under two
// live goroutines and cancelled their context only afterwards (`defer stop()`
// is registered far higher, so LIFO runs it last) — a startup that failed on an
// unreadable route CA file narrated a collector outage instead.
//
// Source order is the property, so the test reads the source. A start is any
// of the three spellings run() has for one: a `go` statement, a
// sync.WaitGroup.Go call (`wg.Go(...)`, which the hand-rolled Add/go/Done
// blocks became) and a `p.spawn(...)`. Counting only `go` statements would
// have made the conversion to WaitGroup.Go disarm this check — first by
// tripping the vacuity guard below, and then, once someone "fixed" that,
// silently. Two more shapes can hand a goroutine to the producer wg WITHOUT
// any of those appearing in run() — `wg.Add(1)` followed by a helper that
// does the `go` itself, and a helper that is simply given `&wg` — so both
// count as starts too: each is a producer join entered above the drain.
func TestNoGoroutineIsStartedBeforeTheProducerDrainIsRegistered(t *testing.T) {
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
	// isWG reports whether e names the producer WaitGroup (run()'s `wg`, or a
	// field such as p.wg).
	isWG := func(e ast.Expr) bool {
		switch x := e.(type) {
		case *ast.Ident:
			return x.Name == "wg"
		case *ast.SelectorExpr:
			return x.Sel.Name == "wg"
		}
		return false
	}
	var drain ast.Node
	var starts []ast.Node
	ast.Inspect(run.Body, func(n ast.Node) bool {
		switch s := n.(type) {
		case *ast.DeferStmt:
			if strings.Contains(text(s), "cli.WaitFor(&wg") {
				drain = s
			}
		case *ast.GoStmt:
			starts = append(starts, s)
		case *ast.CallExpr:
			if sel, ok := s.Fun.(*ast.SelectorExpr); ok {
				switch {
				case sel.Sel.Name == "Go" || sel.Sel.Name == "spawn":
					starts = append(starts, s)
					return true
				case sel.Sel.Name == "Add" && isWG(sel.X):
					// wg.Add(n) — never time.Now().Add(...), whose receiver
					// is not the producer group.
					starts = append(starts, s)
					return true
				}
			}
			for _, arg := range s.Args {
				if u, ok := arg.(*ast.UnaryExpr); ok && u.Op == token.AND && isWG(u.X) {
					starts = append(starts, s) // helper(&wg): joins the producer group
					break
				}
			}
		}
		return true
	})
	if drain == nil {
		t.Fatal("no producer-drain defer (cli.WaitFor(&wg, ...)) in run()")
	}
	if len(starts) == 0 {
		t.Fatal("no goroutine starts (`go`, `wg.Go(...)`, `p.spawn(...)`) found in run(); the check would pass vacuously")
	}
	for _, g := range starts {
		if g.Pos() < drain.Pos() {
			t.Errorf("%s: started before the producer drain is registered, so an early `return err` below closes its exporter and spools under it:\n%s",
				fset.Position(g.Pos()), text(g))
		}
	}
}

// ACQUIRE BEFORE SERVING, run()'s half (startServiceGraph's half is
// TestServiceGraphReceiverIsNotServedWhenTheResharderFails). The debug token's
// read is FATAL, and it used to happen inside startDebugServer — the last start,
// after the ingest listeners and the trace tier's receivers were already
// acking pushes. An unreadable token file then returned out of run() with the
// tier's tail buffer still holding spans its senders had been told had landed,
// and the only Flush is in the shutdown sequence that early return skips.
//
// Source order is the property, so the test reads the source.
func TestDebugGuardIsAcquiredBeforeAnyPipelineStarts(t *testing.T) {
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

	var guard ast.Node
	var starts []*ast.CallExpr
	ast.Inspect(run.Body, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		switch fn := call.Fun.(type) {
		case *ast.Ident:
			if fn.Name == "newDebugGuard" {
				guard = call
			}
		case *ast.SelectorExpr:
			if x, ok := fn.X.(*ast.Ident); ok && x.Name == "p" &&
				strings.HasPrefix(fn.Sel.Name, "start") && fn.Sel.Name != "startDebugServer" {
				starts = append(starts, call)
			}
		}
		return true
	})
	if guard == nil {
		t.Fatal("run() does not build the debug guard itself: its fatal token read must happen before any pipeline starts serving")
	}
	if len(starts) == 0 {
		t.Fatal("no p.start* calls found in run(); the check would pass vacuously")
	}
	for _, s := range starts {
		if s.Pos() < guard.Pos() {
			t.Errorf("%s: a pipeline starts before the debug guard's fatal token read, so an unreadable -debug-token-file returns out of run() after it may have acked data",
				fset.Position(s.Pos()))
		}
	}
}
