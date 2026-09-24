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
// valid logfmt, so a parser accepts the line — and WHICH of the two a consumer
// keeps depends on the reader: this repo's logfmt.Get keeps the first
// (TestBuiltinKeyCollisionShadowsRatherThanCorrupts), while a consumer that
// builds a map from the pairs typically keeps the last. Either way the record's
// own field is lost to one of them: the line reads as DEBUG to a human and as
// level="container" to a map-building pipeline, where a severity filter then
// silently drops it. That is a log line the operator cannot see, produced by
// code that looks correct.
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

// keyStart is where the attribute arguments begin, per method name. A method
// not listed here is not a logging call.
var keyStart = map[string]int{
	// (msg, args...)
	"Debug": 1, "Info": 1, "Warn": 1, "Error": 1,
	// (ctx, msg, args...)
	"DebugContext": 2, "InfoContext": 2, "WarnContext": 2, "ErrorContext": 2,
	// (ctx, level, msg, args...)
	"Log": 3,
	// (ctx, level, msg, attrs ...slog.Attr) — every argument is one Attr.
	"LogAttrs": 3,
	// (args...)
	"With": 0,
}

// attrsOnly are the methods whose trailing arguments are all slog.Attr values
// rather than slog's mixed key/value-or-Attr list.
var attrsOnly = map[string]bool{"LogAttrs": true}

// attrCtors are the slog constructors that take the attribute KEY as their
// first argument. Each is ONE argument in a log call, where a bare string key
// is two — which is why a scan stepping by two misaligned on the first Attr.
var attrCtors = map[string]bool{
	"String": true, "Int": true, "Int64": true, "Uint64": true, "Float64": true,
	"Bool": true, "Duration": true, "Time": true, "Any": true, "Group": true,
}

// reservedUse is one argument that names a key slog writes itself.
type reservedUse struct {
	pos token.Pos
	key string
}

// reservedKeyUses returns every argument of call that puts a reserved key at
// the TOP level of the record, walking the arguments the way slog's own
// argsToAttr consumes them: a string literal is a key and takes its value with
// it (two slots); a slog.<Ctor>("key", …) call or a slog.Attr{Key: "key"}
// literal is one Attr (one slot), whose literal key is checked. A slog.Group
// with an EMPTY key is inlined by slog, so its own arguments are walked as
// top-level ones; any other group's keys render as "group.key" and cannot
// collide. An argument of any other shape — a variable, a call returning a
// string — cannot be classified without type information: it is taken as a
// key (two slots), the common case, and not checked.
func reservedKeyUses(call *ast.CallExpr) []reservedUse { return keyUses(call, reserved) }

// keyUses is reservedKeyUses over any key set: every argument of call that puts
// a key in bad at the TOP level of the record.
func keyUses(call *ast.CallExpr, bad map[string]bool) []reservedUse {
	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok {
		return nil
	}
	start, ok := keyStart[sel.Sel.Name]
	if !ok || start > len(call.Args) {
		return nil
	}
	return scanAttrArgs(call.Args[start:], attrsOnly[sel.Sel.Name], bad)
}

func scanAttrArgs(args []ast.Expr, onlyAttrs bool, bad map[string]bool) []reservedUse {
	var out []reservedUse
	check := func(e ast.Expr) {
		if key, ok := stringLit(e); ok && bad[key] {
			out = append(out, reservedUse{e.Pos(), key})
		}
	}
	for i := 0; i < len(args); {
		arg := args[i]
		if key, ok := stringLit(arg); ok && !onlyAttrs {
			if bad[key] {
				out = append(out, reservedUse{arg.Pos(), key})
			}
			i += 2
			continue
		}
		if c, ok := arg.(*ast.CallExpr); ok && isSlogCtor(c) {
			if len(c.Args) > 0 {
				if name := c.Fun.(*ast.SelectorExpr).Sel.Name; name == "Group" {
					if key, ok := stringLit(c.Args[0]); ok && key == "" {
						out = append(out, scanAttrArgs(c.Args[1:], false, bad)...)
					}
				} else {
					check(c.Args[0])
				}
			}
			i++
			continue
		}
		if lit, ok := arg.(*ast.CompositeLit); ok && isSlogAttrType(lit.Type) {
			for _, el := range lit.Elts {
				if kv, ok := el.(*ast.KeyValueExpr); ok {
					if id, ok := kv.Key.(*ast.Ident); ok && id.Name == "Key" {
						check(kv.Value)
					}
				}
			}
			i++
			continue
		}
		if onlyAttrs {
			i++
		} else {
			i += 2
		}
	}
	return out
}

func stringLit(e ast.Expr) (string, bool) {
	lit, ok := e.(*ast.BasicLit)
	if !ok || lit.Kind != token.STRING {
		return "", false
	}
	s, err := strconv.Unquote(lit.Value)
	return s, err == nil
}

// isSlogCtor reports whether c is slog.<Ctor>(…) for a key-taking constructor.
func isSlogCtor(c *ast.CallExpr) bool {
	sel, ok := c.Fun.(*ast.SelectorExpr)
	if !ok || !attrCtors[sel.Sel.Name] {
		return false
	}
	pkg, ok := sel.X.(*ast.Ident)
	return ok && pkg.Name == "slog"
}

func isSlogAttrType(t ast.Expr) bool {
	sel, ok := t.(*ast.SelectorExpr)
	if !ok || sel.Sel.Name != "Attr" {
		return false
	}
	pkg, ok := sel.X.(*ast.Ident)
	return ok && pkg.Name == "slog"
}

// The scanner itself, over the shapes a pairwise walk got wrong: a positional
// Attr shifts every later key onto the offset a step-by-two walk skips, a key
// inside a constructor was never examined, and LogAttrs was not a log call.
func TestReservedKeyScannerFollowsSlogArgumentRules(t *testing.T) {
	for _, tc := range []struct {
		src  string
		want []string // reserved keys found, in order
	}{
		{`log.Info("m", "level", x)`, []string{"level"}},
		{`log.Info("m", slog.Int("n", 1), "level", x)`, []string{"level"}},
		{`log.Info("m", slog.String("msg", v))`, []string{"msg"}},
		{`log.Info("m", slog.Attr{Key: "time", Value: v})`, []string{"time"}},
		{`log.LogAttrs(ctx, lvl, "m", slog.Int("n", 1), slog.Any("level", v))`, []string{"level"}},
		{`log.Info("m", slog.Group("", "msg", x))`, []string{"msg"}},
		{`log.Info("m", slog.Group("g", "msg", x))`, nil}, // renders g.msg
		{`log.With(slog.Bool("ok", true), "time", x)`, []string{"time"}},
		{`log.Info("m", "a", "level", "b", x)`, nil},             // "level" is a VALUE here
		{`log.Info("m", k, "level", "msg", x)`, []string{"msg"}}, // unknown k taken as a key: "level" is its value
	} {
		expr, err := parser.ParseExpr(tc.src)
		if err != nil {
			t.Fatalf("%s: %v", tc.src, err)
		}
		var got []string
		for _, u := range reservedKeyUses(expr.(*ast.CallExpr)) {
			got = append(got, u.key)
		}
		if strings.Join(got, ",") != strings.Join(tc.want, ",") {
			t.Errorf("%s: found %q, want %q", tc.src, got, tc.want)
		}
	}
}

func TestNoLogCallUsesASlogReservedKey(t *testing.T) {
	forEachLogCall(t, func(fset *token.FileSet, call *ast.CallExpr) {
		for _, u := range reservedKeyUses(call) {
			t.Errorf("%s: %s(..., %q, ...) uses a key slog writes itself.\n"+
				"  Both pairs render, and which one a consumer keeps depends on its reader (this repo's\n"+
				"  logfmt.Get keeps the first, a map-building consumer typically the last), so the\n"+
				"  record's own %s is lost to one of them.\n"+
				"  Rename the attribute (e.g. %q -> %q). A METRIC label of this name is fine.",
				fset.Position(u.pos), call.Fun.(*ast.SelectorExpr).Sel.Name, u.key, u.key, u.key,
				"object"+strings.ToUpper(u.key[:1])+u.key[1:])
		}
	})
}

// neverSpellings are the synonyms the vocabulary in cli.go names NEVER, each
// with the key to use instead. A vocabulary nothing enforces drifts: `file`,
// `podUID`, `remoteAddr` and three spellings of elapsed time had all crept in
// beside the entries they duplicate, and a grep for `path=` or `elapsed=` then
// silently misses the lines that spelled it differently. "reason" is NEVER for
// the error but a real key for a metric-label classification, so it cannot be
// listed here.
var neverSpellings = map[string]string{
	"err":          "error",
	"cause":        "error",
	"file":         "path",
	"ns":           "namespace",
	"podNamespace": "namespace",
	"podUID":       "uid",
	"remoteAddr":   "peer",
	"took":         "elapsed",
	"waited":       "elapsed",
}

func TestNoLogCallUsesANeverSpelling(t *testing.T) {
	bad := make(map[string]bool, len(neverSpellings))
	for k := range neverSpellings {
		bad[k] = true
	}
	forEachLogCall(t, func(fset *token.FileSet, call *ast.CallExpr) {
		for _, u := range keyUses(call, bad) {
			t.Errorf("%s: %s(..., %q, ...) uses a spelling internal/cli's key vocabulary names NEVER; use %q",
				fset.Position(u.pos), call.Fun.(*ast.SelectorExpr).Sel.Name, u.key, neverSpellings[u.key])
		}
	})
}

// forEachLogCall parses every non-test Go file in the repo and hands each call
// expression to fn (keyUses decides whether it is a log call).
func forEachLogCall(t *testing.T, fn func(*token.FileSet, *ast.CallExpr)) {
	t.Helper()
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
			if call, ok := n.(*ast.CallExpr); ok {
				fn(fset, call)
			}
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatalf("walking the repo: %v", err)
	}
}
