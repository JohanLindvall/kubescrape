package transform

// Bounding the two amplifiers that are OPERATORS.
//
// A builtin can be bounded by shadowing it: name resolution consults
// predeclared before the universe (resolve.useToplevel), so `list` resolves to
// the wrapper in limits.go. `*` and `+` have no such seam — they compile to a
// bytecode instruction that calls starlark-go's Binary() directly — and they
// are precisely the constructs that allocate arbitrarily much in ONE
// instruction: `[0] * ((1<<30)-1)` is ~15 steps and ~17 GB, `s = s + s`
// reaches 2 GiB in 74. A watchdog cannot help there, because thread.Cancel is
// only observed BETWEEN steps, and the allocation happens inside one.
//
// So the parse tree is rewritten before it is resolved: `x * y` becomes
// __kubescrape_mul__(x, y), `x + y` becomes __kubescrape_add__(x, y), and the
// augmented forms rewrite in place to the same calls. The guards bound the
// result and then delegate to starlark.Binary, so every shape's semantics —
// including the error text for an unsupported operand pair — is the library's.
//
// The augmented forms cost one extra rule, because the target has to appear
// TWICE in the rewritten tree — once written, once read. It is DUPLICATED
// (dupTarget: a deep copy, since a node cannot be resolved into two binding
// sites and the operator rewrite mutates nodes in place), which is faithful
// only where evaluating the target twice is free of consequence. It is for a
// name, for a field (`r.body += " tag"`) and for an index (`r.attributes["n"]
// += 1`): every read a script can perform on a host object of this package is
// pure — no Attr, Get or Has in hostobj.go or hooks.go mutates anything,
// blocks, or allocates more than the value it returns — and the same holds for
// starlark's own dicts, lists and strings. It is NOT free for a target
// containing a CALL (`d[f()] += 1`), which is refused by name: the call would
// run twice.
//
// Refusing the field and index forms outright, as this used to (only a bare
// NAME was accepted), was the worse bug: a compile failure is FATAL at startup,
// so a transforms file using the ordinary `r.body += " tag"` CrashLooped every
// agent in the fleet on an image upgrade with no config change at all — no
// operator mistake needed, unlike the OOM this layer exists to prevent.
//
// `x += y` gets its own guard (iaddGuard) in ALL THREE forms, because the
// library's compiler emits INPLACE_ADD for all three (internal/compile, the
// augmented-assignment case): for a list it EXTENDS the receiver and stores the
// same object back rather than building a new one, and an aliasing script has
// to keep seeing that.
//
// The ONE behavioural difference the duplication leaves, documented rather than
// papered over: the library evaluates a target's ADDRESS once, before the
// right-hand side, and stores through it; the rewrite evaluates the target
// again for the store. Those differ only if the right-hand side MUTATES a
// container the target reads its receiver or key from — `d[a[0]] += f()` where
// f writes a[0], which then stores under the new key. That is exactly what the
// hand-written `d[a[0]] = d[a[0]] + f()` does, and writing it out by hand was
// the only spelling available before this, so the rewrite matches the explicit
// form rather than the augmented one. TestTargetIsReEvaluatedForTheStore pins
// both halves.
//
// The rewriter walks with syntax.Walk and mutates the PARENT's field, so a
// node type whose Expr children it forgets would silently leave that operator
// unguarded — the exact hole it exists to close. verifyRewritten re-walks with
// the library's own exhaustive visitor and refuses the script if any PLUS or
// STAR survived, so an incomplete rewriter is a clean config error rather than
// a fleet-wide OOM.

import (
	"fmt"

	"go.starlark.net/syntax"
)

// The guard names. Reserved: a script that uses one AS A NAME is refused,
// because a global of that name would shadow the predeclared guard (globals
// resolve first) and quietly redefine every `*` and `+` in the file. Only as a
// name — see checkReservedNames for the Idents that cannot bind and are
// therefore left alone.
const (
	addGuard  = "__kubescrape_add__"
	iaddGuard = "__kubescrape_iadd__"
	mulGuard  = "__kubescrape_mul__"
)

func isGuardName(s string) bool {
	return s == addGuard || s == iaddGuard || s == mulGuard
}

// rewriteAmplifiers rewrites f in place. The file must be freshly parsed and
// not yet resolved.
func rewriteAmplifiers(f *syntax.File) error {
	// Before the rewrite, because the rewrite INSERTS idents with these names.
	if err := checkReservedNames(f); err != nil {
		return err
	}
	var err error
	fail := func(pos syntax.Position, format string, args ...any) {
		if err == nil {
			err = fmt.Errorf("%s: "+format, append([]any{pos}, args...)...)
		}
	}
	syntax.Walk(f, func(n syntax.Node) bool {
		if err != nil {
			return false
		}
		switch n := n.(type) {
		case *syntax.ExprStmt:
			n.X = guarded(n.X)
		case *syntax.IfStmt:
			n.Cond = guarded(n.Cond)
		case *syntax.AssignStmt:
			rewriteAssign(n, fail)
			n.RHS = guarded(n.RHS)
		case *syntax.ForStmt:
			n.X = guarded(n.X)
		case *syntax.WhileStmt:
			n.Cond = guarded(n.Cond)
		case *syntax.ReturnStmt:
			if n.Result != nil {
				n.Result = guarded(n.Result)
			}
		case *syntax.ListExpr:
			guardAll(n.List)
		case *syntax.TupleExpr:
			guardAll(n.List)
		case *syntax.ParenExpr:
			n.X = guarded(n.X)
		case *syntax.CondExpr:
			n.Cond, n.True, n.False = guarded(n.Cond), guarded(n.True), guarded(n.False)
		case *syntax.IndexExpr:
			n.X, n.Y = guarded(n.X), guarded(n.Y)
		case *syntax.DictEntry:
			n.Key, n.Value = guarded(n.Key), guarded(n.Value)
		case *syntax.SliceExpr:
			n.X = guarded(n.X)
			if n.Lo != nil {
				n.Lo = guarded(n.Lo)
			}
			if n.Hi != nil {
				n.Hi = guarded(n.Hi)
			}
			if n.Step != nil {
				n.Step = guarded(n.Step)
			}
		case *syntax.Comprehension:
			// For a DICT comprehension the body is a *DictEntry, which guarded
			// passes through — its halves are rewritten by the DictEntry case.
			n.Body = guarded(n.Body)
		case *syntax.IfClause:
			n.Cond = guarded(n.Cond)
		case *syntax.ForClause:
			n.X = guarded(n.X)
		case *syntax.UnaryExpr:
			if n.X != nil { // nil for the bare * of `def f(a, *, b)`
				n.X = guarded(n.X)
			}
		case *syntax.BinaryExpr:
			n.X, n.Y = guarded(n.X), guarded(n.Y)
		case *syntax.DotExpr:
			n.X = guarded(n.X) // never n.Name: that is the attribute, not an operand
		case *syntax.CallExpr:
			n.Fn = guarded(n.Fn)
			guardAll(n.Args)
		case *syntax.LambdaExpr:
			n.Body = guarded(n.Body)
		}
		return true
	})
	if err != nil {
		return err
	}
	return verifyRewritten(f)
}

// checkReservedNames refuses a script that uses a guard name AS A NAME: a
// global (or a parameter, or a def) of that name resolves before the
// predeclared guard and would silently redefine every `*` and `+` in the file.
//
// Two Ident nodes are NOT names and are skipped, because refusing them would
// CrashLoop a fleet over a spelling that cannot shadow anything: the attribute
// of a field access (`r.__kubescrape_add__`, resolved by the host object) and
// the label of a keyword argument (`f(__kubescrape_add__=1)`, resolved against
// the callee's parameter list — the resolver itself only descends into the
// VALUE, see resolve.go's CallExpr case). A guard name in a string literal or a
// dict key was never an Ident and was never refused. A def or lambda PARAMETER
// of that name is a binding and stays refused, which is why the skip is keyed
// on the parent being a CallExpr rather than on the `k=v` shape alone (a
// parameter default wears the identical BinaryExpr{EQ}).
//
// The skip set is filled from the PARENT, which syntax.Walk always visits
// before the child — the ordering is the library's contract, and
// TestGuardNameInAnAttributeOrKeywordIsNotRefused is what fails if it changes.
func checkReservedNames(f *syntax.File) error {
	notNames := map[*syntax.Ident]bool{}
	var bad error
	syntax.Walk(f, func(n syntax.Node) bool {
		if bad != nil {
			return false
		}
		switch n := n.(type) {
		case *syntax.DotExpr:
			notNames[n.Name] = true
		case *syntax.CallExpr:
			for _, arg := range n.Args {
				if kw, ok := arg.(*syntax.BinaryExpr); ok && kw.Op == syntax.EQ {
					if id, ok := kw.X.(*syntax.Ident); ok {
						notNames[id] = true
					}
				}
			}
		case *syntax.Ident:
			if isGuardName(n.Name) && !notNames[n] {
				bad = fmt.Errorf("%s: %s is a reserved identifier: the * and + operators compile to calls to it", n.NamePos, n.Name)
			}
		}
		return true
	})
	return bad
}

func guardAll(es []syntax.Expr) {
	for i := range es {
		es[i] = guarded(es[i])
	}
}

// guarded replaces `x * y` / `x + y` with a call to the bounding builtin, and
// returns everything else untouched. The operands travel into the call
// unchanged: the walk descends into the new node, so an operand that is itself
// a product or a sum is rewritten in turn.
func guarded(e syntax.Expr) syntax.Expr {
	b, ok := e.(*syntax.BinaryExpr)
	if !ok {
		return e
	}
	switch b.Op {
	case syntax.STAR:
		return guardCall(mulGuard, b.OpPos, b.X, b.Y)
	case syntax.PLUS:
		return guardCall(addGuard, b.OpPos, b.X, b.Y)
	}
	return e
}

func guardCall(name string, pos syntax.Position, x, y syntax.Expr) syntax.Expr {
	return &syntax.CallExpr{
		Fn:     &syntax.Ident{NamePos: pos, Name: name},
		Lparen: pos,
		Args:   []syntax.Expr{x, y},
		Rparen: pos,
	}
}

// rewriteAssign turns `t *= y` into `t = __kubescrape_mul__(t, y)` in place,
// and `t += y` into `t = __kubescrape_iadd__(t, y)`, for the three target
// shapes starlark can assign to: a name, a field and an index. See the file
// comment for why duplicating the target is faithful for those and only those.
func rewriteAssign(n *syntax.AssignStmt, fail func(syntax.Position, string, ...any)) {
	var guard string
	switch n.Op {
	case syntax.STAR_EQ:
		guard = mulGuard
	case syntax.PLUS_EQ:
		guard = iaddGuard
	default:
		return
	}
	op := "*"
	if n.Op == syntax.PLUS_EQ {
		op = "+"
	}
	target := unparen(n.LHS)
	switch target.(type) {
	case *syntax.Ident, *syntax.DotExpr, *syntax.IndexExpr:
	default:
		// Not an assignable shape at all — starlark's own resolver refuses a
		// tuple, a list or a slice target one stage later. Saying so here keeps
		// the message about the target rather than about the rewrite.
		pos, _ := n.LHS.Span()
		fail(pos, "%s is not supported on a %s target: only a name (`x %s= 1`), a field (`r.body %s= \" tag\"`) or an index (`d[k] %s= 1`) can be assigned to",
			n.Op, targetKind(target), op, op, op)
		return
	}
	read, bad := dupTarget(n.LHS)
	if bad != nil {
		pos, _ := bad.Span()
		if _, isCall := bad.(*syntax.CallExpr); isCall {
			fail(pos, "%s is not supported on a target that contains a call: bounding the operator rewrites `t %s= x` into `t = t %s x`, which evaluates the target twice — the call would run twice. Assign its result to a name first: `k = f()` then `d[k] %s= x`",
				n.Op, op, op, op)
			return
		}
		fail(pos, "%s is not supported on a target containing a %s: bounding the operator evaluates the target twice, and a second evaluation would build a second one. Assign it to a name first, or write `t = t %s x` — a target that is a name, a field or an index of names, literals and fields is supported",
			n.Op, targetKind(bad), op)
		return
	}
	n.RHS = guardCall(guard, n.OpPos, read, n.RHS)
	n.Op = syntax.EQ
}

// unparen strips the parentheses starlark's own compiler strips before it
// classifies an assignment target (internal/compile, `unparen(stmt.LHS)`), so
// `(d["k"]) += 1` is classified as the index it is. The duplicated READ keeps
// its parens: they cost nothing and change nothing.
func unparen(e syntax.Expr) syntax.Expr {
	for {
		p, ok := e.(*syntax.ParenExpr)
		if !ok {
			return e
		}
		e = p.X
	}
}

// dupTarget deep-copies an assignment target so the rewrite can READ it in the
// right-hand side, or reports the node that makes the copy unfaithful.
//
// A copy rather than the node itself, for two independent reasons: an Ident
// cannot be resolved into two binding sites (the target is a bind, the read is
// a use), and the operator rewrite mutates nodes in place through their PARENT's
// field, so one child shared by two parents would be rewritten under one of
// them only.
//
// The switch is the allowlist. A CALL is refused because a second evaluation
// would run it twice; a comprehension, a lambda and a dict literal are refused
// because a second evaluation builds a second value, so the write would land on
// a value the script has already thrown away. Anything the starlark grammar
// grows that this file has not been taught lands in the default and is refused
// too — an unknown shape must be a clean config error, never a silent
// mis-copy.
func dupTarget(e syntax.Expr) (syntax.Expr, syntax.Expr) {
	switch e := e.(type) {
	case *syntax.Ident:
		return &syntax.Ident{NamePos: e.NamePos, Name: e.Name}, nil
	case *syntax.Literal:
		return &syntax.Literal{Token: e.Token, TokenPos: e.TokenPos, Raw: e.Raw, Value: e.Value}, nil
	case *syntax.ParenExpr:
		x, bad := dupTarget(e.X)
		if bad != nil {
			return nil, bad
		}
		return &syntax.ParenExpr{Lparen: e.Lparen, X: x, Rparen: e.Rparen}, nil
	case *syntax.DotExpr:
		x, bad := dupTarget(e.X)
		if bad != nil {
			return nil, bad
		}
		// e.Name is the ATTRIBUTE, resolved by the host object rather than
		// bound — copied anyway, since sharing it would share a node.
		return &syntax.DotExpr{X: x, Dot: e.Dot, NamePos: e.NamePos, Name: &syntax.Ident{NamePos: e.Name.NamePos, Name: e.Name.Name}}, nil
	case *syntax.IndexExpr:
		x, bad := dupTarget(e.X)
		if bad != nil {
			return nil, bad
		}
		y, bad := dupTarget(e.Y)
		if bad != nil {
			return nil, bad
		}
		return &syntax.IndexExpr{X: x, Lbrack: e.Lbrack, Y: y, Rbrack: e.Rbrack}, nil
	case *syntax.UnaryExpr: // d[-1] += 1
		if e.X == nil {
			return nil, e // the bare * of a parameter list: never a target
		}
		x, bad := dupTarget(e.X)
		if bad != nil {
			return nil, bad
		}
		return &syntax.UnaryExpr{OpPos: e.OpPos, Op: e.Op, X: x}, nil
	case *syntax.BinaryExpr: // d[i + 1] += 1 — both copies are rewritten in turn
		x, bad := dupTarget(e.X)
		if bad != nil {
			return nil, bad
		}
		y, bad := dupTarget(e.Y)
		if bad != nil {
			return nil, bad
		}
		return &syntax.BinaryExpr{X: x, OpPos: e.OpPos, Op: e.Op, Y: y}, nil
	case *syntax.CondExpr: // d[a if c else b] += 1
		cond, bad := dupTarget(e.Cond)
		if bad != nil {
			return nil, bad
		}
		yes, bad := dupTarget(e.True)
		if bad != nil {
			return nil, bad
		}
		no, bad := dupTarget(e.False)
		if bad != nil {
			return nil, bad
		}
		return &syntax.CondExpr{If: e.If, Cond: cond, True: yes, ElsePos: e.ElsePos, False: no}, nil
	case *syntax.SliceExpr: // d[s[1:3]] += 1
		x, bad := dupTarget(e.X)
		if bad != nil {
			return nil, bad
		}
		lo, bad := dupOptional(e.Lo)
		if bad != nil {
			return nil, bad
		}
		hi, bad := dupOptional(e.Hi)
		if bad != nil {
			return nil, bad
		}
		step, bad := dupOptional(e.Step)
		if bad != nil {
			return nil, bad
		}
		return &syntax.SliceExpr{X: x, Lbrack: e.Lbrack, Lo: lo, Hi: hi, Step: step, Rbrack: e.Rbrack}, nil
	case *syntax.ListExpr: // d[[a, b][0]] += 1
		list, bad := dupList(e.List)
		if bad != nil {
			return nil, bad
		}
		return &syntax.ListExpr{Lbrack: e.Lbrack, List: list, Rbrack: e.Rbrack}, nil
	case *syntax.TupleExpr: // d[(a, b)] += 1 — a tuple key is an ordinary dict key
		list, bad := dupList(e.List)
		if bad != nil {
			return nil, bad
		}
		return &syntax.TupleExpr{Lparen: e.Lparen, List: list, Rparen: e.Rparen}, nil
	}
	return nil, e
}

func dupOptional(e syntax.Expr) (syntax.Expr, syntax.Expr) {
	if e == nil {
		return nil, nil
	}
	return dupTarget(e)
}

func dupList(es []syntax.Expr) ([]syntax.Expr, syntax.Expr) {
	out := make([]syntax.Expr, len(es))
	for i, e := range es {
		c, bad := dupTarget(e)
		if bad != nil {
			return nil, bad
		}
		out[i] = c
	}
	return out, nil
}

// targetKind names a construct for a refusal message, in a spelling an operator
// reading their own script recognises.
func targetKind(e syntax.Expr) string {
	switch e.(type) {
	case *syntax.CallExpr:
		return "call"
	case *syntax.TupleExpr:
		return "tuple"
	case *syntax.ListExpr:
		return "list"
	case *syntax.DictExpr, *syntax.DictEntry:
		return "dict literal"
	case *syntax.SliceExpr:
		return "slice"
	case *syntax.Comprehension:
		return "comprehension"
	case *syntax.LambdaExpr:
		return "lambda"
	case *syntax.Literal:
		return "literal"
	}
	return "expression"
}

// verifyRewritten is the fail-closed half: the rewriter above enumerates the
// node types that hold operands, and a missed one would leave an operator
// unbounded with no symptom until a script hit it. This walks with the
// library's own visitor — the one that knows every node type — and turns a
// miss into a refused config.
func verifyRewritten(f *syntax.File) error {
	var bad error
	syntax.Walk(f, func(n syntax.Node) bool {
		if bad != nil {
			return false
		}
		switch n := n.(type) {
		case *syntax.BinaryExpr:
			if n.Op == syntax.STAR || n.Op == syntax.PLUS {
				bad = fmt.Errorf("%s: internal: the %s operator was not bounded (transform/rewrite.go is missing a node type); refusing the script rather than running it unbounded",
					n.OpPos, n.Op)
			}
		case *syntax.AssignStmt:
			if n.Op == syntax.STAR_EQ || n.Op == syntax.PLUS_EQ {
				bad = fmt.Errorf("%s: internal: the %s operator was not bounded (transform/rewrite.go is missing a node type); refusing the script rather than running it unbounded",
					n.OpPos, n.Op)
			}
		}
		return true
	})
	return bad
}
