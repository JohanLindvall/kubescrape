package cli_test

// The structural guard behind the logfmt guarantee: no log call may use a key
// slog itself writes.
//
// logfmt_test.go proves the HANDLER emits logfmt. That is necessary and not
// sufficient — a call site can still corrupt a record without the handler doing
// anything wrong, by naming an attribute after a built-in field. slog writes
// `time=`, `level=` and `msg=` itself and does not dedupe against user
// attributes, so
//
//	log.Debug("...", "level", "container")
//
// renders `time=... level=DEBUG msg="..." level=container`. Both pairs are
// valid logfmt, so a parser accepts the line and resolves level to the LAST
// one: the record reads as DEBUG to a human and as level="container" to Loki,
// where a severity filter then silently drops it. That is a log line the
// operator cannot see, produced by code that looks correct.
//
// It was a live defect (the cadvisor unresolved-row line, found by reading the
// agent's actual debug output on a kind cluster), which is why this is a
// structural check over the whole repo rather than a note in a comment.
//
// Deliberately NOT reserved here: "source". slog writes it only under
// HandlerOptions.AddSource, which cli.NewLogfmtHandler does not set.

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// reserved are the keys slog's own record fields occupy.
var reserved = map[string]bool{"time": true, "level": true, "msg": true}

// keyStart is where the alternating key/value pairs begin, per method name.
// A method not listed here is not a logging call.
var keyStart = map[string]int{
	// (msg, key, value, ...)
	"Debug": 1, "Info": 1, "Warn": 1, "Error": 1,
	// (ctx, msg, key, value, ...)
	"DebugContext": 2, "InfoContext": 2, "WarnContext": 2, "ErrorContext": 2,
	// (ctx, level, msg, key, value, ...)
	"Log": 3,
	// (key, value, ...)
	"With": 0,
}

func TestNoLogCallUsesASlogReservedKey(t *testing.T) {
	root := filepath.Join("..", "..")
	fset := token.NewFileSet()

	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			// bin/ holds build output; testdata holds fixtures that are not
			// this repo's code.
			if n := d.Name(); n == "bin" || n == "testdata" || n == ".git" {
				return fs.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		f, perr := parser.ParseFile(fset, path, nil, 0)
		if perr != nil {
			// A file this test cannot parse is not this test's business (build
			// tags never make a file unparseable; a syntax error fails the build).
			return nil
		}
		ast.Inspect(f, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			sel, ok := call.Fun.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			start, ok := keyStart[sel.Sel.Name]
			if !ok {
				return true
			}
			// Alternating pairs from start. A non-literal key cannot be checked
			// statically and is skipped rather than guessed at.
			for i := start; i < len(call.Args); i += 2 {
				lit, ok := call.Args[i].(*ast.BasicLit)
				if !ok || lit.Kind != token.STRING {
					continue
				}
				key, uerr := strconv.Unquote(lit.Value)
				if uerr != nil || !reserved[key] {
					continue
				}
				t.Errorf("%s: %s(..., %q, ...) uses a key slog writes itself.\n"+
					"  Both pairs render, a logfmt reader keeps the LAST, and the record's %s is destroyed.\n"+
					"  Rename the attribute (e.g. %q -> %q). A METRIC label of this name is fine.",
					fset.Position(lit.Pos()), sel.Sel.Name, key, key, key, "object"+strings.ToUpper(key[:1])+key[1:])
			}
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatalf("walking the repo: %v", err)
	}
}
