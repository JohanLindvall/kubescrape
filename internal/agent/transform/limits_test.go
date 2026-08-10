package transform

// What these pin: a transforms file is an operator-edited ConfigMap, and
// before limits.go/rewrite.go one line of it — `_hog = list(range(1<<26))` —
// OOM-killed a whole agent fleet within ~75s of one kubectl apply and then
// CrashLooped it, because the step limit counts bytecode instructions and that
// line is eleven of them. Every case below is a shape that was measured to
// spend megabytes-to-gigabytes, or minutes, against a budget it barely
// touched.

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"go.starlark.net/starlark"
	"go.starlark.net/syntax"

	"github.com/JohanLindvall/kubescrape/internal/obs"
)

// logsScript wraps a transform() body as the transforms file's logs: section.
func logsScript(body string) []byte {
	var b strings.Builder
	b.WriteString("logs: |\n  def transform(batch):\n")
	for _, line := range strings.Split(strings.TrimRight(body, "\n"), "\n") {
		b.WriteString("      " + line + "\n")
	}
	return []byte(b.String())
}

// runBody compiles a transform() body and runs it over a one-record batch,
// reporting whichever of the two stages failed.
func runBody(t *testing.T, body string) error {
	t.Helper()
	prog, err := Compile(logsScript(body))
	if err != nil {
		return err
	}
	_, err = prog.logs.runLogs(logsPayload("hello"), nil)
	return err
}

// evalToAttr runs `expr` and returns str(expr) as the script saw it, so a
// semantics case can assert on a VALUE rather than on the absence of an error.
func evalToAttr(t *testing.T, body string) string {
	t.Helper()
	prog, err := Compile(logsScript(body))
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

func mustContain(t *testing.T, err error, want string) {
	t.Helper()
	if err == nil {
		t.Fatalf("no error; want one mentioning %q", want)
	}
	if !strings.Contains(err.Error(), want) {
		t.Fatalf("error %q does not mention %q", err, want)
	}
}

// --- the incident, and the promise it broke ---

// The exact ConfigMap line that killed the fleet, plus its siblings: each is a
// handful of interpreter steps and hundreds of megabytes to gigabytes. They
// run at MODULE level, which is the CrashLoop half — CompileFile re-evaluates
// the module at every startup, so a process that dies here dies before there
// is a last-good program to fall back to.
func TestModuleLevelHogsAreRefusedNotFatal(t *testing.T) {
	for _, tc := range []struct{ name, src, want string }{
		{"list-of-range", "_hog = list(range(1<<26))", "range of 67108864 values"},
		{"list-repeat", "_hog = [0] * ((1<<30)-1)", "element limit"},
		{"string-repeat", `_hog = "x" * (1<<30)`, "byte limit"},
		{"comprehension", "_hog = [x for x in range(1<<26)]", "range of 67108864 values"},
		{"sorted-of-range", "_hog = sorted(range(1<<26))", "range of 67108864 values"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			done := make(chan error, 1)
			go func() {
				_, err := Compile([]byte("logs: |\n  " + tc.src + "\n  def transform(batch): pass\n"))
				done <- err
			}()
			select {
			case err := <-done:
				mustContain(t, err, tc.want)
			case <-time.After(30 * time.Second):
				t.Fatal("compile did not return: the module hog is unbounded")
			}
		})
	}
}

// Doubling: 74 steps to 2 GiB, so the step budget never notices. The refusal
// must name the string limit, since "concatenate less" is the operator's fix.
func TestStringDoublingIsBounded(t *testing.T) {
	err := runBody(t, "s = \"x\"\nfor _i in range(64):\n    s = s + s\n")
	mustContain(t, err, "byte limit")
	mustContain(t, err, "+ would build")
}

// The reload guarantee the incident disproved: a hog in a NEW file must leave
// the last good program running, exactly like a syntax error, instead of
// taking the process with it.
func TestLastGoodProgramSurvivesAHogEdit(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "transforms.yaml")
	good := "logs: |\n  def transform(batch):\n      for r in batch:\n          r.attributes[\"v\"] = \"1\"\n"
	hog := "logs: |\n  _hog = list(range(1<<26))\n  def transform(batch): pass\n"
	writeAtomic(t, path, good)
	prog, err := CompileFile(path)
	if err != nil {
		t.Fatal(err)
	}
	next := &capExp{}
	w := Wrap(next, next, prog)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { defer close(done); Reload(ctx, w, path, 20*time.Millisecond, testLogger()) }()

	failed := obs.TransformReloads.WithLabelValues("failed")
	before := failed.Value()
	writeAtomic(t, path, hog)
	waitFor(t, "the hog edit to be read and rejected", func() bool { return failed.Value() > before })
	cancel()
	<-done
	if got, want := w.Active().Hash, contentHash([]byte(good)); got != want {
		t.Fatalf("hog edit displaced the last good program: active %s, want %s", got, want)
	}
}

// --- the bounded builtins ---

// The shadowing is the whole mechanism (name resolution consults predeclared
// before the universe), so it gets a direct proof rather than an inference
// from a refusal: every wrapper must be what the script's call reaches, and
// every wrapped name must still EXIST in the universe — a starlark-go upgrade
// that renamed one would otherwise silently un-shadow it.
func TestBoundedBuiltinsShadowTheUniverse(t *testing.T) {
	pre := predeclared("logs")
	for _, name := range append([]string{"range"}, materialisers...) {
		if _, ok := starlark.Universe[name]; !ok {
			t.Errorf("universe has no %q: the shadow no longer shadows anything", name)
		}
		got, ok := pre[name].(*starlark.Builtin)
		if !ok {
			t.Errorf("%s is not predeclared: scripts get the unbounded universe one", name)
			continue
		}
		if got == starlark.Universe[name] {
			t.Errorf("%s is predeclared as the universe builtin itself, unbounded", name)
		}
	}
	// And the wrapper really is what runs: a bounded call still WORKS (the
	// delegation is intact) while the over-limit one is refused by name.
	if got := evalToAttr(t, "for r in batch:\n    r.attributes[\"out\"] = str(list(range(3)))\n"); got != "[0, 1, 2]" {
		t.Fatalf("bounded list(range(3)) = %s; the wrapper broke delegation", got)
	}
}

// Each materialising builtin at its limit and one past it. range is the choke
// point — it is the universe's one lazy unbounded source — so the over-limit
// cases go through it, which is also the shape an operator actually writes.
func TestBoundedBuiltinsAtAndOverTheLimit(t *testing.T) {
	for _, name := range materialisers {
		call := name + "(range(%d))"
		switch name {
		case "dict", "bytes":
			// dict() wants pairs and bytes() wants ints in 0..255: neither
			// takes a bare range, so they are exercised through a list.
			call = name + "(list(range(%d)))"
			if name == "dict" {
				continue // no cheap over-limit pair sequence exists; the input check below covers it
			}
		}
		t.Run(name, func(t *testing.T) {
			small := fmt.Sprintf("_x = "+call+"\n", 8)
			if _, err := Compile([]byte("logs: |\n  " + small + "  def transform(batch): pass\n")); err != nil {
				t.Fatalf("%s at a legal size was refused: %v", name, err)
			}
			big := fmt.Sprintf("_x = "+call+"\n", maxSeqElems+1)
			_, err := Compile([]byte("logs: |\n  " + big + "  def transform(batch): pass\n"))
			mustContain(t, err, "limit")
		})
	}
}

// range() itself: at the cap it is a legal (lazy, allocation-free) value, one
// past it names the range rather than whatever would have materialised it.
func TestRangeIsCappedAtItsLength(t *testing.T) {
	if err := runBody(t, fmt.Sprintf("_r = range(%d)\n", maxSeqElems)); err != nil {
		t.Fatalf("range at the cap was refused: %v", err)
	}
	err := runBody(t, fmt.Sprintf("_r = range(%d)\n", maxSeqElems+1))
	mustContain(t, err, "range of 1048577 values")
	// A three-argument range is bounded by its LENGTH, not by its bounds.
	if err := runBody(t, "_r = range(0, 1<<40, 1<<30)\n"); err != nil {
		t.Fatalf("a long-but-short-strided range was refused: %v", err)
	}
}

// --- the bounded operators ---

func TestRepeatOperatorIsBounded(t *testing.T) {
	for _, tc := range []struct{ name, body, want string }{
		{"list-at-limit", fmt.Sprintf("_x = [0] * %d\n", maxSeqElems), ""},
		{"list-over-limit", fmt.Sprintf("_x = [0] * %d\n", maxSeqElems+1), "element limit"},
		{"list-huge", "_x = [0] * ((1<<30)-1)", "element limit"},
		{"tuple-over-limit", fmt.Sprintf("_x = (0,) * %d\n", maxSeqElems+1), "element limit"},
		{"string-over-limit", fmt.Sprintf("_x = \"ab\" * %d\n", maxStringBytes), "byte limit"},
		{"count-on-the-left", fmt.Sprintf("_x = %d * [0]\n", maxSeqElems+1), "element limit"},
		{"bignum-square", "_x = 1 << 511\nfor _i in range(64):\n    _x = _x * _x\n", "bit limit"},
		{"plain-arithmetic", "_x = 6 * 7\n", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := runBody(t, tc.body)
			if tc.want == "" {
				if err != nil {
					t.Fatalf("legal repeat refused: %v", err)
				}
				return
			}
			mustContain(t, err, tc.want)
		})
	}
}

func TestConcatOperatorIsBounded(t *testing.T) {
	for _, tc := range []struct{ name, body, want string }{
		{"list-over-limit", fmt.Sprintf("a = [0] * %d\n_x = a + a\n", maxSeqElems/2+1), "element limit"},
		{"string-over-limit", fmt.Sprintf("a = \"x\" * %d\n_x = a + a\n", maxStringBytes/2+1), "byte limit"},
		{"legal", "_x = [1] + [2]\n", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := runBody(t, tc.body)
			if tc.want == "" {
				if err != nil {
					t.Fatalf("legal concat refused: %v", err)
				}
				return
			}
			mustContain(t, err, tc.want)
		})
	}
}

// The augmented forms are the natural way to write the doubling loop, so they
// are bounded too — and `+=` on a list must stay an in-place EXTEND, which is
// what a naive `x = x + y` rewrite would have silently changed.
func TestAugmentedAssignmentIsBoundedAndKeepsItsSemantics(t *testing.T) {
	t.Run("bounded", func(t *testing.T) {
		mustContain(t, runBody(t, "s = \"x\"\nfor _i in range(64):\n    s += s\n"), "byte limit")
		mustContain(t, runBody(t, "a = [0]\nfor _i in range(64):\n    a *= 2\n"), "element limit")
	})
	t.Run("list-alias-still-sees-the-extend", func(t *testing.T) {
		got := evalToAttr(t, "a = [1]\nb = a\na += [2]\nfor r in batch:\n    r.attributes[\"out\"] = str(b)\n")
		if got != "[1, 2]" {
			t.Fatalf("b = %s, want [1, 2]: += on a list must extend in place, not rebind", got)
		}
	})
	t.Run("self-extend", func(t *testing.T) {
		if got := evalToAttr(t, "a = [1, 2]\na += a\nfor r in batch:\n    r.attributes[\"out\"] = str(a)\n"); got != "[1, 2, 1, 2]" {
			t.Fatalf("a += a = %s, want [1, 2, 1, 2]", got)
		}
	})
	t.Run("non-list-shapes", func(t *testing.T) {
		if got := evalToAttr(t, "s = \"a\"\ns += \"b\"\nn = 1\nn += 2\nfor r in batch:\n    r.attributes[\"out\"] = s + str(n)\n"); got != "ab3" {
			t.Fatalf("got %q, want \"ab3\"", got)
		}
	})
	// A field and an index target are bounded through the same guards, and
	// they are ordinary scripts rather than a refusal — see augassign_test.go
	// for the whole class, including the shapes duplication cannot express.
	t.Run("bounded-on-a-field-or-index-target", func(t *testing.T) {
		mustContain(t, runBody(t, "d = {\"k\": [0]}\nfor _i in range(64):\n    d[\"k\"] *= 2\n"), "element limit")
		mustContain(t, runBody(t, "for r in batch:\n    for _i in range(64):\n        r.body += r.body\n"), "byte limit")
	})
}

// Delegating to starlark.Binary is what keeps the rewrite invisible: the
// values, the types and the library's own error text all have to survive it.
func TestBoundOperatorsPreserveSemantics(t *testing.T) {
	for _, tc := range []struct{ expr, want string }{
		{"6 * 7", "42"},
		{"1.5 * 2", "3.0"},
		{"2 * 1.5", "3.0"},
		{`"ab" * 3`, "ababab"},
		{`3 * "ab"`, "ababab"},
		{"str([1, 2] * 2)", "[1, 2, 1, 2]"},
		{"str((1,) * 2)", "(1, 1)"},
		{"1 + 2", "3"},
		{"1 + 2.5", "3.5"},
		{`"a" + "b"`, "ab"},
		{"str([1] + [2])", "[1, 2]"},
		{"str((1,) + (2,))", "(1, 2)"},
		{"str(1 << 70)", "1180591620717411303424"},
		{"str((1 << 70) + 1)", "1180591620717411303425"},
		{"str((1 << 70) * 2)", "2361183241434822606848"},
		{"str([x * 2 for x in range(3)])", "[0, 2, 4]"},
		{`"%s-%d" % ("a", 2)`, "a-2"},
	} {
		t.Run(tc.expr, func(t *testing.T) {
			body := "for r in batch:\n    r.attributes[\"out\"] = str(" + tc.expr + ")\n"
			if got := evalToAttr(t, body); got != tc.want {
				t.Fatalf("%s = %q, want %q", tc.expr, got, tc.want)
			}
		})
	}
	// An unsupported operand pair still reports the library's own message.
	mustContain(t, runBody(t, "_x = \"a\" * \"b\"\n"), "unknown binary op: string * string")
}

// --- the rewriter's coverage ---

// The rewriter enumerates the node types that hold operands, and a forgotten
// one would leave that position unbounded with no symptom until a script used
// it. verifyRewritten is the fail-closed oracle (it walks with the library's
// own exhaustive visitor), so the coverage test is: for an operator in every
// syntactic position, the snippet must contain one BEFORE the rewrite — which
// is what keeps a case from passing vacuously — and none after.
func TestRewriteReachesEveryOperandPosition(t *testing.T) {
	positions := []struct{ name, src string }{
		{"expr-stmt", "def f():\n  [0] * 2\n"},
		{"if-cond", "def f():\n  if 1 * 2:\n    pass\n"},
		{"assign-rhs", "x = 1 * 2\n"},
		{"augmented", "def f(n):\n  n *= 2\n  n += 1\n  return n\n"},
		{"augmented-field", "def f(o):\n  o.f *= 2\n  o.f += 1\n"},
		{"augmented-index", "def f(d, k):\n  d[k] *= 2\n  d[k] += 1\n"},
		// The target itself holds an operator, so the DUPLICATED read has to be
		// rewritten too — verifyRewritten walks both copies.
		{"augmented-index-computed-key", "def f(d, i):\n  d[i + 1] *= 2\n"},
		{"for-x", "def f():\n  for v in [0] * 2:\n    pass\n"},
		{"while-cond", "def f():\n  while 0 * 1:\n    break\n"},
		{"return", "def f():\n  return 1 * 2\n"},
		{"list-elem", "x = [1 * 2]\n"},
		{"tuple-elem", "x = (1 * 2, 3)\n"},
		{"paren", "x = (1 * 2)\n"},
		{"cond-expr", "x = (1 * 2) if (3 * 4) else (5 * 6)\n"},
		{"index-x", "x = ([1, 2] * 2)[0]\n"},
		{"index-y", "x = [1, 2, 3][1 * 2]\n"},
		{"dict-key", "x = {1 * 2: 3}\n"},
		{"dict-value", "x = {1: 2 * 3}\n"},
		{"slice", "x = [1, 2, 3][0 * 1:1 * 2:1 + 0]\n"},
		{"comprehension-body", "x = [v * 2 for v in range(3)]\n"},
		{"comprehension-dict-body", "x = {v: v * 2 for v in range(3)}\n"},
		{"comprehension-for-x", "x = [v for v in [1] * 2]\n"},
		{"comprehension-if", "x = [v for v in range(3) if v * 2]\n"},
		{"unary", "x = -(1 * 2)\n"},
		{"binary-operand", "x = (1 * 2) - (3 + 4)\n"},
		{"dot-x", "x = (\"a\" * 2).upper()\n"},
		{"call-fn", "def f():\n  return len\nx = [f][0 * 0]([1])\n"},
		{"call-arg", "x = len([1] * 2)\n"},
		{"lambda-body", "f = lambda v: v * 2\n"},
		{"param-default", "def f(v = 1 * 2):\n  return v\n"},
	}
	opts := &syntax.FileOptions{Set: true, While: true, GlobalReassign: true}
	for _, tc := range positions {
		t.Run(tc.name, func(t *testing.T) {
			f, err := opts.Parse("t.star", tc.src, 0)
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			if verifyRewritten(f) == nil {
				t.Fatal("the snippet holds no + or * before the rewrite: this case proves nothing")
			}
			if err := rewriteAmplifiers(f); err != nil {
				t.Fatalf("rewrite: %v", err)
			}
			if err := verifyRewritten(f); err != nil {
				t.Fatalf("an operand position is unbounded: %v", err)
			}
		})
	}
}

// The guard names cannot be script-visible AS NAMES (a global of one resolves
// before the predeclared guard and would redefine every operator in the file),
// and an Ident that cannot bind — an attribute, a keyword-argument label — must
// not be refused for merely being spelled that way. Both halves live in
// augassign_test.go's TestGuardNamesAreReservedForEveryBindingForm and
// TestGuardNameInAnAttributeOrKeywordIsNotRefused, which is where the rest of
// the rewriter's refusals are pinned.

// --- wall clock, steps, cumulative allocation ---

// The measured pathology: `s = s + "0123456789abcdef"` is O(n) per step, so
// 200,000 iterations took 1m51.8s while spending 20% of the step budget, and
// through the real seam it held Wrapper.ExportLogs for 31.3s. Nothing about it
// is illegal — only long — so the bound has to be the clock.
func TestWallClockBudgetFires(t *testing.T) {
	withWallClock(t, 25*time.Millisecond)
	start := time.Now()
	err := runBody(t, "s = \"\"\nfor _i in range(200000):\n    s = s + \"0123456789abcdef\"\n")
	mustContain(t, err, "over the 25ms budget")
	if d := time.Since(start); d > 5*time.Second {
		t.Fatalf("the script ran %s past a 25ms budget: the checkpoint is too coarse", d)
	}
}

// The checkpoint has to fire from inside a tight loop that allocates nothing
// at all, i.e. from OnMaxSteps rather than from a guard.
func TestWallClockBudgetFiresInAPureLoop(t *testing.T) {
	withWallClock(t, 25*time.Millisecond)
	mustContain(t, runBody(t, "n = 0\nfor _i in range(1000000):\n    n = n\n"), "over the 25ms budget")
}

// The MODULE evaluation is on the same clock: CompileFile is synchronous in
// the agent's startup and in the reloader, so a slow top-level loop hangs the
// process before it serves anything (or wedges the reload goroutine, which is
// the other half of the keep-the-last-good-program promise).
func TestWallClockBudgetBoundsTheCompileToo(t *testing.T) {
	withWallClock(t, 25*time.Millisecond)
	_, err := Compile([]byte("logs: |\n  _x = [i for i in range(1000000)]\n  def transform(batch): pass\n"))
	mustContain(t, err, "over the 25ms budget")
}

// The cumulative charge covers module level too: this is the incident's exact
// shape once each individual value is small enough to pass the per-value caps.
func TestAllocationBudgetBoundsTheCompileToo(t *testing.T) {
	_, err := Compile([]byte("logs: |\n  _x = [\"x\" * (1<<20) for i in range(200)]\n  def transform(batch): pass\n"))
	mustContain(t, err, "in one invocation")
}

// A script that finishes inside its budget must not be disturbed by the
// checkpoint machinery.
func TestFastScriptIsUnaffected(t *testing.T) {
	withWallClock(t, time.Second)
	if err := runBody(t, "n = 0\nfor _i in range(10000):\n    n = n\n"); err != nil {
		t.Fatalf("a legal script was refused: %v", err)
	}
}

// The step budget is still a budget: an unbounded loop that never allocates
// and finishes each step instantly ends on steps, not on the clock.
func TestStepBudgetStillBounds(t *testing.T) {
	// The clock is raised out of the way so the STEP bound is unambiguously
	// what fires: under -race an interpreter step costs ~15x more, and the
	// default budget would otherwise stop this on time instead.
	withWallClock(t, time.Minute)
	err := runBody(t, "n = 0\nwhile True:\n    n = n\n")
	mustContain(t, err, "step budget")
}

// Per-value caps cannot bound a loop that keeps building bounded values, so
// the invocation carries a cumulative charge as well.
func TestCumulativeAllocationBudget(t *testing.T) {
	err := runBody(t, "s = \"x\" * (1<<20)\nfor _i in range(1000):\n    _t = s + s\n")
	mustContain(t, err, "in one invocation")
}

// The budget is per INVOCATION, and threads are pooled: a spend (or a
// cancellation) must not leak into the next batch.
func TestBudgetResetsBetweenInvocations(t *testing.T) {
	// Half the allocation budget per run, so two runs on one pooled thread
	// would trip the cumulative cap if it were not reset. No wall-clock
	// override here: this is about the CHARGE, and a lowered clock would make
	// the case fail for the other reason on a loaded machine.
	withWallClock(t, time.Minute) // the charge is the subject; the clock must not decide
	prog, err := Compile(logsScript(fmt.Sprintf("s = \"x\" * (1<<20)\nfor _i in range(%d):\n    _t = s + s\n", maxAllocBytes/(4<<20))))
	if err != nil {
		t.Fatal(err)
	}
	for i := range 3 {
		if _, err := prog.logs.runLogs(logsPayload("hello"), nil); err != nil {
			t.Fatalf("invocation %d: %v", i, err)
		}
	}
}

// Steps accumulate on a thread for its lifetime, so a pooled one has to have
// its counter zeroed: otherwise the Nth batch inherits N-1 batches' steps and
// the per-invocation budget silently becomes a per-PROCESS quota that fails
// every export once it runs out.
func TestPooledThreadResetsItsStepCounter(t *testing.T) {
	withWallClock(t, time.Minute) // steps are the subject; the clock must not decide
	// ~2.1M steps per invocation, so six of them on one pooled thread are over
	// the 10M budget if the counter carries over.
	prog, err := Compile(logsScript("n = 0\nfor _i in range(700000):\n    n = n\n"))
	if err != nil {
		t.Fatal(err)
	}
	for i := range 6 {
		if _, err := prog.logs.runLogs(logsPayload("hello"), nil); err != nil {
			t.Fatalf("invocation %d: %v", i, err)
		}
	}
}

// A thread whose invocation was CANCELLED goes back in the pool, and
// starlark-go's cancellation is a latch: without the Uncancel on the way out
// of the pool, one runaway batch would fail every later batch on that thread
// with a stale reason.
func TestPooledThreadSurvivesACancellation(t *testing.T) {
	withWallClock(t, time.Minute) // the latch is the subject; the clock must not decide
	// ONE program, so the second batch is served by the very thread the first
	// one's cancellation latched. A script that only runs away on a marked
	// record is what makes the second batch's success meaningful.
	prog, err := Compile(logsScript("for r in batch:\n    if r.body == \"spin\":\n        n = 0\n        while True:\n            n = n\n    r.attributes[\"out\"] = \"done\"\n"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := prog.logs.runLogs(logsPayload("spin"), nil); err == nil {
		t.Fatal("the runaway batch was not stopped")
	} else if !strings.Contains(err.Error(), "step budget") {
		t.Fatalf("unexpected refusal: %v", err)
	}
	ld := logsPayload("hello")
	if _, err := prog.logs.runLogs(ld, nil); err != nil {
		t.Fatalf("the batch after a cancellation inherited it: %v", err)
	}
	if _, ok := ld.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0).Attributes().Get("out"); !ok {
		t.Fatal("the batch after a cancellation was not transformed at all")
	}
}

// One compiled program serves every concurrent export, and the guards it was
// compiled with are shared by all of them: the budget has to be the THREAD's,
// or two busy pipelines would spend one ceiling between them (and race on it —
// this is a -race case as much as a semantic one).
func TestConcurrentInvocationsHaveIndependentBudgets(t *testing.T) {
	// A third of the allocation budget per invocation: three concurrent runs
	// sharing one counter would trip it, three independent ones cannot.
	prog, err := Compile(logsScript(fmt.Sprintf("s = \"x\" * (1<<20)\nfor _i in range(%d):\n    _t = s + s\n", maxAllocBytes/(6<<20))))
	if err != nil {
		t.Fatal(err)
	}
	errs := make(chan error, 24)
	for range 8 {
		go func() {
			for range 3 {
				_, err := prog.logs.runLogs(logsPayload("hello"), nil)
				errs <- err
			}
		}()
	}
	for i := range 24 {
		if err := <-errs; err != nil {
			t.Fatalf("concurrent invocation %d: %v", i, err)
		}
	}
}

func withWallClock(t *testing.T, d time.Duration) {
	t.Helper()
	old := wallClock
	wallClock = d
	t.Cleanup(func() { wallClock = old })
}

// --- print ---

// print() is in the universe, reachable from every script, and with
// Thread.Print unset starlark-go writes it with fmt.Fprintln(os.Stderr):
// unstructured, unthrottled lines injected raw into the agent's own log stream
// — which the agent then collects, the feedback loop log()'s throttle exists
// to prevent. One in-cluster minute measured 68 such lines against 17 throttled
// log() ones.
func TestPrintGoesToTheThrottledScriptLog(t *testing.T) {
	var logged bytes.Buffer
	old := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logged, nil)))
	t.Cleanup(func() { slog.SetDefault(old) })
	// The 1/s gate is per SIGNAL and process-wide, so a sibling test that
	// logged within the last second would otherwise swallow this line.
	scriptLogGates.Delete("logs")
	t.Cleanup(func() { scriptLogGates.Delete("logs") })

	// Capture anything that reaches the raw stderr fallback.
	r, wpipe, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	oldErr := os.Stderr
	os.Stderr = wpipe
	t.Cleanup(func() { os.Stderr = oldErr })

	if err := runBody(t, "for r in batch:\n    print(\"from-print\")\n"); err != nil {
		t.Fatal(err)
	}
	_ = wpipe.Close()
	var raw bytes.Buffer
	_, _ = raw.ReadFrom(r)
	if raw.Len() != 0 {
		t.Fatalf("print reached the raw stderr fallback: %q", raw.String())
	}
	if !strings.Contains(logged.String(), "from-print") {
		t.Fatalf("print did not reach the script log: %q", logged.String())
	}
	if !strings.Contains(logged.String(), "signal=logs") {
		t.Fatalf("the script log line lost its signal: %q", logged.String())
	}
}

// print() and log() share ONE throttle: the flood is what matters, not which
// spelling produced it.
func TestPrintIsThrottledTogetherWithLog(t *testing.T) {
	var logged bytes.Buffer
	old := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logged, nil)))
	t.Cleanup(func() { slog.SetDefault(old) })
	scriptLogGates.Delete("logs")
	t.Cleanup(func() { scriptLogGates.Delete("logs") })

	if err := runBody(t, "for _i in range(50):\n    print(\"p\")\n    log(\"l\")\n"); err != nil {
		t.Fatal(err)
	}
	if n := strings.Count(logged.String(), "transform script log"); n != 1 {
		t.Fatalf("100 script log calls in one second produced %d lines, want 1", n)
	}
	// And that one line is the PRINT — it came first and took the gate. With
	// print on its own path, log()'s line would be the only one here and the
	// prints would be 50 unthrottled lines somewhere else.
	if !strings.Contains(logged.String(), "msg=p") {
		t.Fatalf("the throttled line is not print's: %q", logged.String())
	}
}

// Every thread this package creates carries Print: the compile-time one is the
// one that would otherwise print during module evaluation, on a path with no
// export goroutine to attribute it to.
func TestCompileTimePrintIsAlsoRouted(t *testing.T) {
	var logged bytes.Buffer
	old := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logged, nil)))
	t.Cleanup(func() { slog.SetDefault(old) })
	scriptLogGates.Delete("logs")
	t.Cleanup(func() { scriptLogGates.Delete("logs") })

	r, wpipe, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	oldErr := os.Stderr
	os.Stderr = wpipe
	t.Cleanup(func() { os.Stderr = oldErr })

	if _, err := Compile([]byte("logs: |\n  print(\"at-module-level\")\n  def transform(batch): pass\n")); err != nil {
		t.Fatal(err)
	}
	_ = wpipe.Close()
	var raw bytes.Buffer
	_, _ = raw.ReadFrom(r)
	if raw.Len() != 0 {
		t.Fatalf("a module-level print reached the raw stderr fallback: %q", raw.String())
	}
	if !strings.Contains(logged.String(), "at-module-level") {
		t.Fatalf("a module-level print did not reach the script log: %q", logged.String())
	}
}
