package transform

// What these pin: `r.body += " tag"`, `r.attributes["n"] += 1` and `a[i] *= 2`
// are ordinary transform scripts, and the amplifier rewrite (rewrite.go) once
// refused every one of them because it needs the target twice in the tree. A
// compile failure is FATAL at startup, so that refusal CrashLooped every agent
// in a fleet on an image upgrade with NO config change — a worse outcome than
// the OOM the rewrite exists to prevent, and one needing no operator mistake.
//
// So: each newly supported shape has a case proving it compiles, runs, and is
// STILL BOUNDED at runtime through the new spelling; each still-refused shape
// has a case pinning the refusal AND that its message names the reason and the
// spelling that works.

import (
	"fmt"
	"strings"
	"testing"

	"go.starlark.net/starlark"
	"go.starlark.net/syntax"
)

// --- the shapes that must work ---

// Every one of these was a hard config error before, and every one of them is
// what an operator writes without thinking about it.
func TestAugmentedAssignmentOnFieldAndIndexTargets(t *testing.T) {
	for _, tc := range []struct{ name, body, want string }{
		{"host-object-field", `r.body += " tag"
out = r.body`, "hello tag"},
		{"record-attribute", `r.attributes["n"] = 1
r.attributes["n"] += 2
out = r.attributes["n"]`, "3"},
		{"resource-attribute", `r.resource["env"] = "pr"
r.resource["env"] += "od"
out = r.resource["env"]`, "prod"},
		{"dict-key", `d = {"k": 1}
d["k"] += 2
out = d["k"]`, "3"},
		{"list-index-multiply", `a = [1, 2]
i = 1
a[i] *= 3
out = a`, "[1, 6]"},
		{"negative-index", `a = [1, 2]
a[-1] += 5
out = a`, "[1, 7]"},
		{"nested-index", `d = {"a": {"b": 1}}
d["a"]["b"] += 1
out = d["a"]["b"]`, "2"},
		{"parenthesised-target", `d = {"k": 1}
(d["k"]) += 1
out = d["k"]`, "2"},
		{"tuple-key", `d = {(1, 2): 1}
d[(1, 2)] += 1
out = d[(1, 2)]`, "2"},
		// The key holds an operator of its own, so BOTH copies of the target
		// have to be rewritten in turn — the write's and the read's.
		{"computed-key", `d = {"x1": 1}
i = "1"
d["x" + i] += 1
out = d["x1"]`, "2"},
		// An index whose key is itself a field read off a host object.
		{"key-read-from-the-record", `d = {"info": 5}
d[r.attributes["level"]] += 1
out = d["info"]`, "6"},
		// The multiply guard on a field target, which is the same seam.
		{"field-multiply-through-a-number", `d = {"n": 2}
d["n"] *= 3
out = d["n"]`, "6"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := evalToAttr(t, augBody(tc.body)); got != tc.want {
				t.Fatalf("got %q, want %q", got, tc.want)
			}
		})
	}
}

// The claim the rewrite rests on is that `t op= x` and `t = t op x` are the
// same program for these targets. The oracle for that is starlark itself: the
// same snippet run UNREWRITTEN by the library has to produce the same value.
// (Host-object targets cannot appear here — the library has no host objects —
// which is what the value cases above cover instead.)
func TestAugmentedAssignmentMatchesUnrewrittenStarlark(t *testing.T) {
	for _, tc := range []struct{ name, snippet string }{
		{"plain-name", `x = 1
x += 2
out = x`},
		// Parenthesised, which the old plain-name test refused as "not a name".
		{"parenthesised-name", `x = 1
(x) += 2
out = x`},
		{"dict-key", `d = {"k": 1}
d["k"] += 2
out = d["k"]`},
		{"list-index", `a = [1, 2]
a[1] *= 3
out = a`},
		// The one that a naive `t = t + x` rewrite would silently change:
		// INPLACE_ADD extends the list the container already holds, so an alias
		// taken beforehand sees the append. The library emits INPLACE_ADD for a
		// field and an index target too, not just for a name.
		{"index-target-extends-in-place", `d = {"l": [1]}
b = d["l"]
d["l"] += [2]
out = b`},
		{"index-target-self-extend", `a = [[1], [2]]
a[0] += a[1]
out = a`},
		{"string-concat", `s = {"x": "a"}
s["x"] += "b"
out = s["x"]`},
		{"negative-index", `a = [1, 2, 3]
a[-1] += 10
out = a`},
		{"tuple-key", `d = {(1, 2): 1}
d[(1, 2)] += 1
out = d[(1, 2)]`},
		{"computed-key", `d = {"x1": 1}
i = "1"
d["x" + i] += 1
out = d`},
		{"nested-index", `d = {"a": [0, 1]}
d["a"][1] += 41
out = d`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			want := unrewritten(t, tc.snippet)
			if got := evalToAttr(t, augBody(tc.snippet)); got != want {
				t.Fatalf("rewritten = %q, unrewritten starlark = %q", got, want)
			}
		})
	}
}

// The point of the whole layer: the new spellings are BOUNDED, and bounded at
// RUN time by the allocation guard rather than at compile time by a refusal —
// a script that never builds anything huge must never fail to start.
func TestAugmentedAssignmentOnFieldAndIndexIsStillBounded(t *testing.T) {
	half := fmt.Sprintf(`s = "x" * %d`, maxStringBytes/2+1)
	for _, tc := range []struct{ name, body, want string }{
		{"field-concat", half + `
r.body = s
r.body += s`, "byte limit"},
		{"attribute-concat", half + `
r.attributes["big"] = s
r.attributes["big"] += s`, "byte limit"},
		// Refused on the arithmetic, before a byte is allocated.
		{"dict-value-repeat", fmt.Sprintf(`d = {"a": [0, 0, 0, 0]}
d["a"] *= %d`, maxSeqElems), "element limit"},
		{"list-element-repeat", fmt.Sprintf(`a = [[0, 0, 0, 0]]
a[0] *= %d`, maxSeqElems), "element limit"},
		// Squaring doubles the bit length per step, so the step budget never
		// notices; the per-value bit cap has to, through the index target.
		{"bignum-through-an-index", `d = {"n": 1 << 500}
for _i in range(16):
    d["n"] *= d["n"]`, "bit limit"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body := recordBody(tc.body)
			// Compile FIRST and on its own: the bound must be a script error,
			// not a startup failure. A compile refusal here is the regression
			// this whole change is about.
			prog, err := Compile(logsScript(body))
			if err != nil {
				t.Fatalf("the script did not compile — a bound must fire at run time, not refuse the config: %v", err)
			}
			_, err = prog.logs.runLogs(logsPayload("hello"), nil)
			mustContain(t, err, tc.want)
		})
	}
}

// --- the shapes that stay refused, and what they say ---

// A call in the TARGET is the one shape duplication cannot express: the call
// would run twice. The message has to name that reason and the spelling that
// works, because the operator's script is legal starlark and they will
// otherwise read the refusal as a bug.
func TestAugmentedAssignmentRefusesACallInTheTarget(t *testing.T) {
	for _, tc := range []struct{ name, src string }{
		{"call-in-the-key", "def f():\n    return \"k\"\ndef transform(batch):\n    d = {\"k\": 1}\n    d[f()] += 1\n"},
		{"call-as-the-receiver", "def f():\n    return {\"k\": 1}\ndef transform(batch):\n    f()[\"k\"] += 1\n"},
		{"method-call-in-the-key", "def transform(batch):\n    d = {\"K\": 1}\n    s = \"k\"\n    d[s.upper()] *= 2\n"},
		{"call-under-a-field", "def f():\n    return None\ndef transform(batch):\n    f().x += 1\n"},
		{"call-inside-parentheses", "def f():\n    return \"k\"\ndef transform(batch):\n    d = {\"k\": 1}\n    (d[f()]) += 1\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Compile(script(tc.src))
			mustContain(t, err, "contains a call")
			mustContain(t, err, "evaluates the target twice")
			mustContain(t, err, "k = f()") // the spelling that works
		})
	}
}

// The mirror of the above: only the TARGET is restricted. A call on the
// right-hand side is duplicated by nothing and must keep working.
func TestACallOnTheRightHandSideIsNotRefused(t *testing.T) {
	src := "def two():\n    return 2\ndef transform(batch):\n    d = {\"k\": 1}\n    d[\"k\"] += two()\n    for r in batch:\n        r.attributes[\"out\"] = str(d[\"k\"])\n"
	if got := runScriptOut(t, src); got != "3" {
		t.Fatalf("out = %q, want \"3\"", got)
	}
}

// The one behavioural difference duplication leaves: the library evaluates a
// target's ADDRESS once and stores through it, while the rewrite evaluates the
// target again for the store. Reachable only when the right-hand side mutates a
// container the target's key is read from — and then the rewritten form is
// exactly the hand-written `t = t + x`, which was the ONLY spelling this
// rewriter accepted before, so the difference is from the augmented form
// TOWARDS the explicit one. Both halves are pinned so a later change to the
// duplication has to face the question rather than move the behaviour quietly.
func TestTargetIsReEvaluatedForTheStore(t *testing.T) {
	const preamble = `def f(a):
    a[0] = "k2"
    return 5
`
	const setup = `a = ["k1"]
d = {"k1": 1, "k2": 0}
`
	ours := runScriptOut(t, preamble+"def transform(batch):\n    "+
		strings.ReplaceAll(setup+"d[a[0]] += f(a)\n", "\n", "\n    ")+
		"\n    for r in batch:\n        r.attributes[\"out\"] = str(d)\n")

	explicit := unrewritten(t, preamble+setup+"d[a[0]] = d[a[0]] + f(a)\nout = d")
	if ours != explicit {
		t.Fatalf("rewritten `+=` = %s, hand-written `t = t + x` = %s: the rewrite must be the explicit form", ours, explicit)
	}
	augmented := unrewritten(t, preamble+setup+"d[a[0]] += f(a)\nout = d")
	if ours == augmented {
		t.Skip("starlark now evaluates an augmented target's address the way the rewrite does; the caveat in rewrite.go can go")
	}
	t.Logf("documented divergence: rewritten %s, starlark's own `+=` %s", ours, augmented)
}

// Targets starlark itself cannot assign to. These never worked — the library's
// own resolver refuses them one stage later — so the only thing to pin is that
// the message is about the TARGET and lists the shapes that are supported,
// rather than the old "only supported on a plain name", which is now false.
func TestAugmentedAssignmentRefusesAnUnassignableTarget(t *testing.T) {
	for _, tc := range []struct{ name, body, kind string }{
		{"tuple", "a = 1\nb = 2\na, b += 1\n", "tuple"},
		{"list", "a = [1]\n[a] += [2]\n", "list"},
		{"slice", "a = [1, 2, 3]\na[0:2] += [4]\n", "slice"},
		{"literal", "\"s\" += \"t\"\n", "literal"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := runBody(t, tc.body)
			mustContain(t, err, "not supported on a "+tc.kind+" target")
			mustContain(t, err, "`r.body += \" tag\"`") // a supported spelling
		})
	}
}

// A shape inside the target that a second evaluation would REBUILD rather than
// re-read. Refused for the same duplication reason as a call, with its own
// remedy, and unreachable in any script that does something.
func TestAugmentedAssignmentRefusesARebuiltTargetOperand(t *testing.T) {
	for _, body := range []string{
		"y = [0]\nd = {0: 1}\nd[[x for x in y][0]] += 1\n", // in the key
		"y = [1]\n[x for x in y][0] += 1\n",                // as the receiver
	} {
		err := runBody(t, body)
		mustContain(t, err, "target containing a comprehension")
		mustContain(t, err, "evaluates the target twice")
	}
}

// The claim that makes duplicating a target safe is that every read a script
// can perform on a host object is PURE. The seam where that could most
// plausibly be false is the verbs: `drop`, `route` and `emit_metric` are read
// as values and only DO anything when called, so reading one twice must mark
// nothing. (`r.drop += 1` is then a type error, like it always was.)
func TestReadingAVerbTwiceDoesNotInvokeIt(t *testing.T) {
	prog, err := Compile(logsScript(recordBody("r.drop += 1")))
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	ld := logsPayload("hello")
	if _, err := prog.logs.runLogs(ld, nil); err == nil {
		t.Fatal("adding to a builtin should be a type error")
	}
	if _, marked := ld.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0).Attributes().Get(DropMarker); marked {
		t.Fatal("reading r.drop marked the record: a target read is not pure, and duplicating it is unsafe")
	}
}

// --- the reserved guard names ---

// The guard names cannot be script-visible AS NAMES: a global, parameter or def
// of one resolves before the predeclared guard and would redefine every `*` and
// `+` in the file.
func TestGuardNamesAreReservedForEveryBindingForm(t *testing.T) {
	for _, tc := range []struct{ name, src string }{
		{"global", "__kubescrape_mul__ = 1\ndef transform(batch): pass\n"},
		{"direct-use", "def transform(batch):\n    return __kubescrape_add__(1, 2)\n"},
		{"def-name", "def __kubescrape_iadd__(a, b): return a\ndef transform(batch): pass\n"},
		// A parameter default wears the same BinaryExpr{EQ} shape as a keyword
		// ARGUMENT, and unlike one it binds. The narrowing below must not reach
		// it.
		{"param-default", "def f(__kubescrape_mul__ = 1): return 1\ndef transform(batch): pass\n"},
		{"param", "def f(__kubescrape_add__): return 1\ndef transform(batch): pass\n"},
		{"lambda-param", "f = lambda __kubescrape_mul__: 1\ndef transform(batch): pass\n"},
		{"loop-variable", "def transform(batch):\n    for __kubescrape_add__ in []:\n        pass\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Compile(script(tc.src))
			mustContain(t, err, "reserved identifier")
		})
	}
}

// ...and only as names. An Ident that CANNOT bind or resolve — the attribute of
// a field access, the label of a keyword argument — must not be refused:
// nothing about it can shadow the guard, and a fatal compile error over an
// incidental spelling would CrashLoop a fleet for no reason at all. A string
// literal or dict key was never an Ident and is pinned here beside them so the
// whole class is one test.
func TestGuardNameInAnAttributeOrKeywordIsNotRefused(t *testing.T) {
	for _, tc := range []struct{ name, src string }{
		{"attribute-selector", "def transform(batch):\n    for r in batch:\n        if False:\n            r.attributes[\"x\"] = r.__kubescrape_add__\n"},
		{"keyword-argument", "def f(**kw):\n    return len(kw)\n_x = f(__kubescrape_add__ = 1)\ndef transform(batch): pass\n"},
		{"string-literal", "def transform(batch):\n    for r in batch:\n        r.attributes[\"__kubescrape_mul__\"] = \"__kubescrape_add__\"\n"},
		{"dict-key", "_d = {\"__kubescrape_iadd__\": 1}\ndef transform(batch): pass\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := Compile(script(tc.src)); err != nil {
				t.Fatalf("an incidental mention of a guard name was refused: %v", err)
			}
		})
	}
}

// --- helpers ---

// script wraps a whole module as the transforms file's logs: section.
func script(src string) []byte {
	return []byte("logs: |\n  " + strings.ReplaceAll(strings.TrimRight(src, "\n"), "\n", "\n  ") + "\n")
}

// recordBody runs a snippet once per record, with the record bound to r (so a
// snippet may use r.body / r.attributes).
func recordBody(snippet string) string {
	var b strings.Builder
	b.WriteString("for r in batch:\n")
	for _, line := range strings.Split(strings.TrimRight(snippet, "\n"), "\n") {
		b.WriteString("    " + line + "\n")
	}
	return b.String()
}

// augBody turns a snippet ending in `out = <value>` into a transform() body
// that publishes str(out) the way evalToAttr reads it.
func augBody(snippet string) string {
	return recordBody(snippet + "\nr.attributes[\"out\"] = str(out)")
}

// runScriptOut compiles and runs a whole logs: module and returns the "out"
// attribute its transform() left on the batch's one record.
func runScriptOut(t *testing.T, src string) string {
	t.Helper()
	prog, err := Compile(script(src))
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	ld := logsPayload("hello")
	if _, err := prog.logs.runLogs(ld, nil); err != nil {
		t.Fatalf("run: %v", err)
	}
	v, ok := ld.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0).Attributes().Get("out")
	if !ok {
		t.Fatal(`the script did not set r.attributes["out"]`)
	}
	return v.Str()
}

// unrewritten runs a snippet through the LIBRARY, with no rewrite and no
// bounded builtins, and returns str(out) — the oracle for "the rewrite is
// invisible".
func unrewritten(t *testing.T, snippet string) string {
	t.Helper()
	opts := &syntax.FileOptions{Set: true, While: true, GlobalReassign: true}
	th := &starlark.Thread{Name: "oracle"}
	globals, err := starlark.ExecFileOptions(opts, th, "oracle.star", snippet+"\nout = str(out)\n", nil)
	if err != nil {
		t.Fatalf("the oracle snippet is not valid starlark: %v", err)
	}
	s, ok := globals["out"].(starlark.String)
	if !ok {
		t.Fatalf("the oracle snippet did not leave a string in out")
	}
	return string(s)
}
