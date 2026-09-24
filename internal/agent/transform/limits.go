package transform

// What bounds one Starlark invocation, and why the step limit alone does not.
//
// starlark-go charges thread.Steps++ once per BYTECODE INSTRUCTION in the
// interpreter loop (starlark/interp.go) and nowhere else, so every byte a
// Go-implemented builtin or operator allocates is free. Measured against the
// 10,000,000-step budget: `list(range(1<<26))` is ELEVEN steps and ~1 GB,
// `[0] * ((1<<30)-1)` is ~15 steps and ~17 GB (starlark-go's own maxAlloc
// guard applies to the repeat operator only and counts ELEMENTS, and a Value
// is 16 bytes), `s = s + s` reaches 2 GiB in 74 steps, and the same gap bounds
// no wall time either — `s = s + "0123456789abcdef"` is O(n) per step, so
// 200,000 iterations took 1m51.8s while spending 20% of the budget.
//
// That is not academic. The transforms file is an operator-edited ConfigMap
// that HOT-RELOADS: one module-level `_hog = list(range(1<<26))` OOM-killed
// every agent in a cluster within ~75s of one kubectl apply and then
// CrashLooped them permanently, because CompileFile re-evaluates the module at
// startup and the process died before there was a last-good program to keep.
// Compile-then-commit protects against SYNTAX errors only.
//
// So every construct that can turn a small input into a large value is bounded
// at its entry point, and a refusal is an ordinary script error — which the
// hooks' fail-open and the reloader's keep-the-last-good-program already
// handle. Three layers:
//
//   - PER VALUE (the guards below): no single string, sequence or bignum a
//     script BUILDS may exceed maxStringBytes / maxSeqElems / maxIntBits. This
//     is the only layer that can bound a ONE-INSTRUCTION allocation, which is
//     why the `*` and `+` operators are rewritten into calls to it (rewrite.go)
//     — an operator has no hook, and a watchdog that can only interrupt
//     BETWEEN steps never sees the 17 GB happen. Every guard here is
//     PREDICTIVE for the same reason: a charge taken after the value exists
//     can report the pathology but cannot prevent it, which is why every
//     render — str()/repr(), log(), print(), fail() and an emit_metric label —
//     is projected first (renderSize, checkRender) and re.replace/re.findall
//     project their result (builtins.go) instead of charging what they just
//     built.
//   - PER INVOCATION (budget.alloc): every guarded allocation is charged, so
//     accumulating bounded values in a loop is bounded too.
//   - WALL CLOCK (budget.start): checked between interpreter steps via
//     OnMaxSteps and on entry to every builtin this package defines, because a
//     script can spend minutes inside a handful of O(n) steps. The invocation
//     runs on the exporting goroutine — on the tailer, the single sweep
//     goroutine serving every log file on the node.
//
// Residual, deliberately: the String METHODS (.replace/.format/.join) and the
// `%` operator can still amplify an operand the script already holds, in one
// instruction, and the dict/list methods allocate uncharged. Bounding those
// would mean rewriting every attribute access, which costs more than it buys
// here — the transforms file is a privileged config surface, and these limits
// exist to make an operator's ACCIDENT survivable, not to sandbox someone who
// can already point the agent's exporter anywhere.

import (
	"bytes"
	"fmt"
	"math"
	"slices"
	"strconv"
	"time"
	"unicode/utf8"

	"go.starlark.net/starlark"
	"go.starlark.net/syntax"
)

const (
	// maxSteps bounds one invocation's interpreter instructions. NOT "a few ms
	// of work", as this said for as long as nobody measured it: a pure
	// `while True: n = n + 1` spends the whole budget in 130.04 ms, and that is
	// the CHEAPEST possible step. 130 ms is what one runaway batch costs the
	// exporting goroutine — on the tailer, the single sweep goroutine serving
	// every log file on the node, which ships nothing while it waits. A step
	// that does real work costs arbitrarily more, which is what the wall-clock
	// budget below is for.
	maxSteps = 10_000_000

	// stepsPerCheck is how often the wall clock is consulted between steps
	// (OnMaxSteps raises the limit rather than cancelling). ~50µs of pure
	// interpretation at 13ns/step, i.e. ~0.1% overhead, and fine-grained
	// enough that a cheap-step runaway overshoots its deadline by microseconds.
	// Expensive steps are covered from the other side: every guard below
	// checks the deadline itself.
	stepsPerCheck = 4096

	// maxSeqElems bounds the elements of any list/tuple/set/dict a script
	// builds in one operation, and the length of a range(). 1Mi elements is
	// 16 MiB of interface words before any payload — 100x the largest batch a
	// script ever sees (a 10k-point promscrape chunk) — while a script wanting
	// to iterate more than that would exhaust maxSteps anyway.
	maxSeqElems = 1 << 20

	// maxStringBytes bounds one string/bytes value a script builds. It matches
	// the 16 MiB OTLP body cap the ingest receiver accepts as a WHOLE payload,
	// so no legitimate script — one joining the bodies of a batch, say — can
	// reach it, while `s = s + s` stops at 16 MiB instead of 2 GiB.
	maxStringBytes = 16 << 20

	// maxIntBits bounds bignum arithmetic: `a = a * a` DOUBLES the bit length
	// per step, so 34 steps reach 137 GB. 1Mi bits (128 KiB) is far past any
	// arithmetic a transform has business doing (machine-word ints are free —
	// they never reach this check).
	maxIntBits = 1 << 20

	// maxIntStringLen bounds the STRING an int() call parses. Go's big.Int
	// decimal parse is O(len²), and it happens inside a single Starlark step —
	// so the step and clock guards cannot interrupt it: one int() over a long
	// attacker-influenced string (a log body a script does int() on) spends
	// seconds on the single export goroutine before OnMaxSteps ever runs
	// (measured: a 1 MiB string ≈ 3s, superlinear). A real number in a log line
	// is a handful of digits; 4096 is astronomically larger and keeps one
	// int() call in the microseconds.
	maxIntStringLen = 4096

	// maxAllocBytes bounds what ONE invocation may build in total. Per-value
	// caps cannot bound a loop that keeps appending bounded values: 500
	// iterations of `l.append("x" * (16<<20))` is 8 GiB and costs ~2500 steps.
	// Only the guarded amplifiers are charged (concat, repeat, the
	// materialising builtins, a re.replace that matched) — never the batch
	// itself. What that does NOT make it is independent of the batch: an
	// invocation is one BATCH, so a script rewriting every record's body spends
	// (rewrites per record) x (the batch's body bytes), and 128 MiB is eight
	// full rewrites of a 16 MiB ingest push. A re.replace that matches nothing
	// builds nothing and is not charged (builtins.go), which is what keeps the
	// ordinary redaction list — a dozen patterns, most missing most records —
	// far inside it.
	maxAllocBytes = 128 << 20

	// bytesPerValue is what one element of a materialised sequence costs
	// before its payload: a starlark.Value is a 16-byte interface word pair.
	bytesPerValue = 16

	// maxRenderDepth is how deeply nested a value renderSize will walk before
	// it REFUSES the render rather than guessing at the rest. Fail-closed on
	// purpose: an earlier version stopped descending at depth 4 and charged
	// everything below it a byte, so `str([[[[[s] * 200]]]])` projected a few
	// hundred bytes and built 200 MiB. The walk has to follow a value all the
	// way down to be a bound, and following it all the way down is exactly
	// what starlark-go's own render does — recursively, with an O(depth) cycle
	// check per level, so a deep chain is also a CPU amplifier inside one
	// interpreter step. No transform renders anything a tenth this deep. A
	// CYCLE is not depth: it renders as `[...]`, and the walk stops there the
	// way the render does.
	maxRenderDepth = 64
)

// wallClock bounds one invocation's elapsed time. Generous — a no-op pass over
// a 1024-record batch is ~13µs and the heaviest realistic script is
// milliseconds — but small enough that a runaway is a failed export rather
// than a node whose logs stop moving: the measured pathology held
// Wrapper.ExportLogs for 31.3s, and extrapolating the step budget gave ~45
// minutes ending in SUCCESS.
//
// A var, not a const, only so the tests can lower it: proving the bound fires
// otherwise costs two real seconds of spinning per case.
var wallClock = 2 * time.Second

// now is the budget's clock, injectable for the same reason store.now and
// series.now are: a test that asserts the wall-clock bound FIRES must not be a
// race between the interpreter and the machine.
//
// TestWallClockBudgetFiresInAPureLoop used to bet that 1,000,000 interpreter
// steps outrun a lowered 25ms budget. That is a property of the hardware, not
// of this package: on a fast CPU the loop finishes first and the test fails
// with "no error", on a slow one it passes, and on a loaded one it flaps.
// Advancing a fake clock instead makes the assertion exact — the checkpoint
// either consults the clock and cancels, or it does not.
var now = time.Now

// budgetKey names the thread-local the guards read. It has to be thread-local:
// the guard builtins are created ONCE per compiled program (predeclared is
// bound at compile time) and shared by every concurrent invocation of it, so
// the per-invocation state can only travel on the thread.
const budgetKey = "kubescrape.transform.budget"

// budget is one invocation's spend.
type budget struct {
	start time.Time
	alloc int64
}

func budgetOf(th *starlark.Thread) *budget {
	b, _ := th.Local(budgetKey).(*budget)
	return b
}

// reset arms the budget for a new invocation (threads are pooled).
func (b *budget) reset() {
	b.start = now()
	b.alloc = 0
}

// spend charges n bytes of script-built data.
func (b *budget) spend(n int64) error {
	if b == nil {
		return nil
	}
	b.alloc = satAdd(b.alloc, n)
	return b.overBudget(b.alloc)
}

// project reports whether building an n-byte value WOULD blow the invocation
// budget, without charging it. It is what the predictive guards ask before
// allocating; spend charges the value once it exists, so the two never
// double-count.
func (b *budget) project(n int64) error {
	if b == nil {
		return nil
	}
	return b.overBudget(satAdd(b.alloc, n))
}

// remaining is what this invocation may still build. It is the ceiling the
// predictive guards short-circuit against, so a probe never costs more than
// the value it is deciding about.
func (b *budget) remaining() int64 {
	if b == nil {
		return maxAllocBytes
	}
	if b.alloc >= maxAllocBytes {
		return 0
	}
	return maxAllocBytes - b.alloc
}

// valueCeiling is the most one value may still cost: the per-value limit, or
// what is left of the invocation budget once pending — bytes the caller has
// committed but not yet charged — is set aside, whichever is smaller. It is the
// ceiling every predictive walk stops at, so a projection never costs more
// than the value it is deciding about.
func (b *budget) valueCeiling(pending int64) int64 {
	return max(0, min(int64(maxStringBytes), b.remaining()-pending))
}

// checkRender is the one refusal every RENDER goes through before it is built
// — str(), repr(), log(), print(), fail() and an emit_metric label — given the
// size renderSize projected at valueCeiling(pending) and whether the value was
// too deep to walk. It refuses a value over the per-value limit, and one that
// would take the invocation past its budget once pending is added.
//
// Both halves are needed, and one copy of this dropped the second: renderSize
// stops walking at the ceiling, so once less than a per-value limit of budget
// is left it returns a TRUNCATED figure that passes the per-value check — and
// the whole value was then rendered and refused only afterwards (measured at
// 923 MiB allocated for an emit_metric label). The error names no subject;
// callers prefix their own.
func (b *budget) checkRender(sz int64, tooDeep bool, pending int64) error {
	if tooDeep {
		return fmt.Errorf("nests containers more than %d deep, past what a render is allowed to walk — flatten the value",
			int64(maxRenderDepth))
	}
	if sz > maxStringBytes {
		return fmt.Errorf("would render at least %d bytes, over the %d-byte limit for one value — render less per call",
			sz, int64(maxStringBytes))
	}
	return b.project(satAdd(pending, sz))
}

func (b *budget) overBudget(total int64) error {
	if total > maxAllocBytes {
		return fmt.Errorf("script allocated more than %d bytes in one invocation (strings, sequences and materialising builtins are charged; the batch itself is not) — build less per batch",
			int64(maxAllocBytes))
	}
	return nil
}

// overtime reports the wall-clock deadline. Every builtin this package defines
// calls it on entry: OnMaxSteps checks the clock every stepsPerCheck steps,
// which is useless when a single step spends a second inside Go code.
func (b *budget) overtime() error {
	if b == nil {
		return nil
	}
	if d := now().Sub(b.start); d > wallClock {
		return fmt.Errorf("script ran for %s, over the %s budget for one invocation — it runs on the exporting goroutine (on the tailer, the single sweep goroutine serving every log file on the node)",
			d.Round(time.Millisecond), wallClock)
	}
	return nil
}

// onMaxSteps is the interpreter's periodic callback: the library calls it once
// the step limit is reached, and the limit is deliberately set to a small
// SLICE of the budget so this runs as a checkpoint rather than as the end.
// Raising the limit resumes; cancelling ends the invocation with the reason.
func (b *budget) onMaxSteps(th *starlark.Thread) {
	if th.Steps >= maxSteps {
		th.Cancel(fmt.Sprintf("script exceeded its %d-step budget", int64(maxSteps)))
		return
	}
	if err := b.overtime(); err != nil {
		th.Cancel(err.Error())
		return
	}
	th.SetMaxExecutionSteps(th.Steps + stepsPerCheck)
}

// positioned prefixes a refusal with the SCRIPT position that caused it.
// starlark-go's EvalError renders as its message alone (the backtrace is a
// separate accessor nothing in this package reads), so without this a bound
// reports what was too big but not which of a hundred lines to change. Frame 0
// is the guard builtin itself; frame 1 is the operator or call in the script.
func positioned(th *starlark.Thread, err error) error {
	if err == nil || th.CallStackDepth() < 2 {
		return err
	}
	return fmt.Errorf("%s: %w", th.CallFrame(1).Pos, err)
}

// --- per-value size arithmetic ---

func textLen(v starlark.Value) (int64, bool) {
	switch v := v.(type) {
	case starlark.String:
		return int64(len(v)), true
	case starlark.Bytes:
		return int64(len(v)), true
	}
	return 0, false
}

func seqLen(v starlark.Value) (int64, bool) {
	switch v := v.(type) {
	case *starlark.List:
		return int64(v.Len()), true
	case starlark.Tuple:
		return int64(len(v)), true
	}
	return 0, false
}

// intBits is an Int's magnitude in bits, and whether it is a BIGNUM: a
// machine-word Int is stored inline and allocates nothing, so it is never
// worth charging or capping.
func intBits(v starlark.Value) (int64, bool) {
	i, ok := v.(starlark.Int)
	if !ok {
		return 0, false
	}
	if _, fits := i.Int64(); fits {
		return 64, false
	}
	return int64(i.BigInt().BitLen()), true
}

// satAdd is a + b saturated at MaxInt64, for the same reason as satMul: a
// projection sums terms that are themselves saturated, and a wrapped negative
// would read as "small". Both operands are non-negative sizes.
func satAdd(a, b int64) int64 {
	if a > math.MaxInt64-b {
		return math.MaxInt64
	}
	return a + b
}

// satMul is a * b saturated at MaxInt64: the product of a repeat count and an
// operand length overflows long before it becomes allocatable, and a wrapped
// negative would read as "small".
func satMul(a, b int64) int64 {
	if a == 0 || b == 0 {
		return 0
	}
	if a > math.MaxInt64/b {
		return math.MaxInt64
	}
	return a * b
}

// repeatCount is the Int operand of a repeat, saturated: a bignum count is
// definitively over every limit.
func repeatCount(v starlark.Value) (int64, bool) {
	i, ok := v.(starlark.Int)
	if !ok {
		return 0, false
	}
	n, fits := i.Int64()
	if !fits {
		return math.MaxInt64, true
	}
	if n < 0 {
		return 0, true
	}
	return n, true
}

// starSize is what `x * y` will allocate, refusing the shapes that would blow
// a per-value limit. It reports 0 for the shapes that allocate nothing worth
// bounding (float and machine-word int arithmetic).
func starSize(x, y starlark.Value) (int64, error) {
	if n, ok := repeatCount(y); ok {
		if sz, matched, err := repeatSize(x, n); matched {
			return sz, err
		}
	}
	if n, ok := repeatCount(x); ok {
		if sz, matched, err := repeatSize(y, n); matched {
			return sz, err
		}
	}
	xb, xbig := intBits(x)
	yb, ybig := intBits(y)
	if !xbig && !ybig {
		return 0, nil
	}
	// A bignum product's bit length is the SUM of the operands', so `a = a * a`
	// doubles it every step: 34 steps from a machine word is 137 GB.
	bits := xb + yb
	if bits > maxIntBits {
		return 0, fmt.Errorf("* would build a %d-bit integer, over the %d-bit limit for one value", bits, int64(maxIntBits))
	}
	return bits / 8, nil
}

// repeatSize is the cost of repeating v n times; matched reports whether v is
// a repeatable operand at all.
func repeatSize(v starlark.Value, n int64) (sz int64, matched bool, err error) {
	if l, ok := textLen(v); ok {
		total := satMul(l, n)
		if total > maxStringBytes {
			return 0, true, fmt.Errorf("* would build a %d-byte string, over the %d-byte limit for one value — repeat fewer times, or build the string outside the script",
				total, int64(maxStringBytes))
		}
		return total, true, nil
	}
	if l, ok := seqLen(v); ok {
		total := satMul(l, n)
		if total > maxSeqElems {
			return 0, true, fmt.Errorf("* would build a %d-element sequence, over the %d-element limit for one value (%d bytes per element before any payload)",
				total, int64(maxSeqElems), bytesPerValue)
		}
		return satMul(total, bytesPerValue), true, nil
	}
	return 0, false, nil
}

// plusSize is what `x + y` will allocate, refusing the shapes that would blow
// a per-value limit. Concatenation is the DOUBLING amplifier: `s = s + s`
// reaches 2 GiB in 74 steps, each of which is one interpreter instruction.
func plusSize(x, y starlark.Value) (int64, error) {
	if xl, ok := textLen(x); ok {
		if yl, ok := textLen(y); ok {
			total := xl + yl
			if total > maxStringBytes {
				return 0, fmt.Errorf("+ would build a %d-byte string, over the %d-byte limit for one value — a script that concatenates in a loop doubles until it hits this",
					total, int64(maxStringBytes))
			}
			return total, nil
		}
	}
	if xl, ok := seqLen(x); ok {
		if yl, ok := seqLen(y); ok {
			total := xl + yl
			if total > maxSeqElems {
				return 0, fmt.Errorf("+ would build a %d-element sequence, over the %d-element limit for one value", total, int64(maxSeqElems))
			}
			return satMul(total, bytesPerValue), nil
		}
	}
	xb, xbig := intBits(x)
	yb, ybig := intBits(y)
	if !xbig && !ybig {
		return 0, nil
	}
	// Addition grows a bignum by at most one bit, so there is nothing to
	// refuse here — only the copy to charge, which is what stops a loop from
	// accumulating them.
	return max(xb, yb) / 8, nil
}

// --- the guarded operators (rewrite.go turns `x * y` into a call to these) ---

func guardBinary(th *starlark.Thread, op syntax.Token, x, y starlark.Value) (starlark.Value, error) {
	b := budgetOf(th)
	if err := b.overtime(); err != nil {
		return nil, positioned(th, err)
	}
	var (
		sz  int64
		err error
	)
	if op == syntax.STAR {
		sz, err = starSize(x, y)
	} else {
		sz, err = plusSize(x, y)
	}
	if err != nil {
		return nil, positioned(th, err)
	}
	if err := b.spend(sz); err != nil {
		return nil, positioned(th, err)
	}
	return starlark.Binary(op, x, y)
}

// guardArgs refuses the argument counts the rewriter cannot produce, so a
// script that reaches these names some other way (it cannot: rewrite.go
// refuses a source that uses one AS A NAME — an attribute or a keyword label
// spelled the same way reaches no builtin) still gets a clean error.
func guardArgs(b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) error {
	if len(args) != 2 || len(kwargs) != 0 {
		return fmt.Errorf("%s: internal operator helper takes exactly 2 positional arguments", b.Name())
	}
	return nil
}

func mulBuiltin() *starlark.Builtin {
	return starlark.NewBuiltin(mulGuard, func(th *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
		if err := guardArgs(b, args, kwargs); err != nil {
			return nil, err
		}
		return guardBinary(th, syntax.STAR, args[0], args[1])
	})
}

func addBuiltin() *starlark.Builtin {
	return starlark.NewBuiltin(addGuard, func(th *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
		if err := guardArgs(b, args, kwargs); err != nil {
			return nil, err
		}
		return guardBinary(th, syntax.PLUS, args[0], args[1])
	})
}

// iaddBuiltin is `x += y`, which is NOT `x = x + y` for a list: starlark-go's
// INPLACE_ADD extends the receiver in place, so an aliased list sees the
// append. Reproducing that here is what keeps the rewrite semantics-preserving.
func iaddBuiltin() *starlark.Builtin {
	return starlark.NewBuiltin(iaddGuard, func(th *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
		if err := guardArgs(b, args, kwargs); err != nil {
			return nil, err
		}
		x, y := args[0], args[1]
		xl, isList := x.(*starlark.List)
		_, isIterable := y.(starlark.Iterable)
		if !isList || !isIterable {
			// Not the in-place shape: `x += y` is `x = x + y` for everything
			// else, INCLUDING a list plus a non-iterable, which must produce
			// the library's own type error.
			return guardBinary(th, syntax.PLUS, x, y)
		}
		bud := budgetOf(th)
		if err := bud.overtime(); err != nil {
			return nil, positioned(th, err)
		}
		// The added length is known up front for a Sequence; a lazy iterable is
		// bounded per element by extend below.
		if yl, ok := y.(starlark.Sequence); ok {
			if total := int64(xl.Len()) + int64(yl.Len()); total > maxSeqElems {
				return nil, positioned(th, fmt.Errorf("+= would build a %d-element list, over the %d-element limit for one value", total, int64(maxSeqElems)))
			}
		}
		added, err := extend(xl, y)
		if err != nil {
			return nil, positioned(th, err)
		}
		if err := bud.spend(satMul(added, bytesPerValue)); err != nil {
			return nil, positioned(th, err)
		}
		return xl, nil
	})
}

// extend appends y's elements to xl, reporting how many.
//
// An INDEXABLE y (list, tuple) is read by position rather than through an
// iterator for two reasons that both bite on `x += x`: starlark's iterator
// marks its receiver temporarily immutable, so appending to it would fail
// where the library's own INPLACE_ADD succeeds, and an iterator over a list
// being appended to chases its own growth to the cardinality cap. The length
// is snapshotted first, which is what the library's `append(x.elems,
// y.elems...)` does implicitly.
//
// One divergence, unobservable in practice: a FROZEN receiver only errors once
// there is an element to append, where INPLACE_ADD checks first. Reaching it
// needs a frozen list on the left of `+=`, and the only frozen lists a script
// can see are module globals, which `+=` inside a function cannot name.
func extend(xl *starlark.List, y starlark.Value) (int64, error) {
	room := func() error {
		if int64(xl.Len()) >= maxSeqElems {
			return fmt.Errorf("+= would build a list over the %d-element limit for one value", int64(maxSeqElems))
		}
		return nil
	}
	if yi, ok := y.(starlark.Indexable); ok {
		n := yi.Len()
		for i := range n {
			if err := room(); err != nil {
				return 0, err
			}
			if err := xl.Append(yi.Index(i)); err != nil {
				return 0, err
			}
		}
		return int64(n), nil
	}
	var added int64
	iter := y.(starlark.Iterable).Iterate()
	defer iter.Done()
	var v starlark.Value
	for iter.Next(&v) {
		if err := room(); err != nil {
			return 0, err
		}
		if err := xl.Append(v); err != nil {
			return 0, err
		}
		added++
	}
	return added, nil
}

// --- the bounded universe builtins ---

// boundedRange caps the LENGTH of a range. range is the one lazy unbounded
// source in the universe — it allocates nothing itself, which is exactly why
// `list(range(1<<26))` is eleven steps and a gigabyte — so capping it here
// bounds every materialiser that could be pointed at it, and reports the
// operator's actual mistake (the range) rather than the materialising call.
func boundedRange() *starlark.Builtin {
	inner := starlark.Universe["range"].(*starlark.Builtin)
	return starlark.NewBuiltin("range", func(th *starlark.Thread, _ *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
		if err := budgetOf(th).overtime(); err != nil {
			return nil, positioned(th, err)
		}
		v, err := inner.CallInternal(th, args, kwargs)
		if err != nil {
			return nil, err
		}
		if s, ok := v.(starlark.Sequence); ok && int64(s.Len()) > maxSeqElems {
			return nil, positioned(th, fmt.Errorf("range of %d values is over the %d-element limit; a script iterating more than that exhausts the %d-step budget anyway",
				s.Len(), int64(maxSeqElems), int64(maxSteps)))
		}
		return v, nil
	})
}

// boundedInt shadows the universe `int`. Unlike the materialisers, its danger
// is not the result size but the PARSE: int(<long decimal string>) is O(len²)
// in Go's big.Int and runs as one uninterruptible Starlark step, so a script
// doing int() on an attacker-influenced value (a log body/attribute) stalls the
// export goroutine for seconds before any step/clock guard can fire. Refuse an
// over-length string argument up front; every non-string form (and short
// strings) falls straight through to the library.
func boundedInt() *starlark.Builtin {
	inner := starlark.Universe["int"].(*starlark.Builtin)
	return starlark.NewBuiltin("int", func(th *starlark.Thread, _ *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
		if err := budgetOf(th).overtime(); err != nil {
			return nil, positioned(th, err)
		}
		// BOTH argument forms: the builtin is int(x, base=10), so checking
		// args[0] alone left `int(x = "…")` free to spend seconds in one
		// uninterruptible step — the guard closed nothing for a script that
		// used the keyword form.
		check := func(v starlark.Value) error {
			if s, ok := v.(starlark.String); ok && len(s) > maxIntStringLen {
				return fmt.Errorf("int() of a %d-character string is over the %d-character limit (the parse is quadratic and uninterruptible)", len(s), maxIntStringLen)
			}
			return nil
		}
		if len(args) > 0 {
			if err := check(args[0]); err != nil {
				return nil, positioned(th, err)
			}
		}
		for _, kv := range kwargs {
			if len(kv) != 2 {
				continue
			}
			if name, ok := starlark.AsString(kv[0]); ok && name == "x" {
				if err := check(kv[1]); err != nil {
					return nil, positioned(th, err)
				}
			}
		}
		return inner.CallInternal(th, args, kwargs)
	})
}

// renderSize projects how many bytes v.String() will build — the render that
// str() of a container, repr(), log() and an emit_metric label of a non-string
// value, and print()/fail() of one, all write — WITHOUT building anything. It
// is the missing half of the materialiser bound: the element-COUNT check below
// bounds the references a materialiser copies, which is the whole cost of
// list()/tuple()/sorted() and friends, but a render writes out each element's
// CONTENTS, so a legal small sequence of large strings (`str([body] * 1024)`)
// renders to a multiple of the batch with every count and per-value bound
// respected.
//
// It mirrors starlark-go's writeValue case for case, so it is EXACT — never
// below the real render and never above it, which is what both sides of a
// refusal need: a value whose projection fits is never refused, and one whose
// projection is over the ceiling really is over it. Two earlier shortcuts were
// neither: a string was charged len(s) where the render QUOTES it (a control
// or invalid byte is written \xNN, four bytes for one), and a scalar was
// charged one byte whatever it held (a million-bit integer renders ~315k
// digits). The single inexact case is a bignum, whose digit count is a LOWER
// bound derived from its bit length — the decimal conversion is the O(n²)
// cost being avoided.
//
// The walk stops once it passes limit (children get what is left of it, so a
// value already over the ceiling is refused after a handful of elements), and
// a value nested more than maxRenderDepth containers deep reports tooDeep
// instead of a size — the render is refused, not guessed at. A cycle renders
// as `[...]`/`{...}` exactly where writeValue detects it (lists and dicts on
// the current path), so a self-referential value terminates the same way.
func renderSize(v starlark.Value, limit int64) (size int64, tooDeep bool) {
	var w renderWalk
	size = w.size(v, 0, limit)
	return size, w.deep
}

// renderWalk carries writeValue's cycle path — the lists and dicts currently
// being rendered, and only those, since writeValue appends nothing else — and
// the too-deep verdict, which ends the walk wherever it is found.
type renderWalk struct {
	path []starlark.Value
	deep bool
}

func (w *renderWalk) onPath(v starlark.Value) bool {
	return slices.Contains(w.path, v)
}

func (w *renderWalk) size(v starlark.Value, depth int, limit int64) int64 {
	switch x := v.(type) {
	case nil:
		return int64(len("<nil>"))
	case starlark.NoneType:
		return int64(len("None"))
	case starlark.Bool:
		if x {
			return int64(len("True"))
		}
		return int64(len("False"))
	case starlark.Int:
		return intRenderLen(x)
	case starlark.Float:
		return floatRenderLen(x)
	case starlark.String:
		return quotedLen(string(x), false, limit)
	case starlark.Bytes:
		return quotedLen(string(x), true, limit)
	case *starlark.List:
		if w.onPath(x) {
			return int64(len("[...]"))
		}
		if depth >= maxRenderDepth {
			w.deep = true
			return 0
		}
		w.path = append(w.path, x)
		total := int64(len("[]"))
		for i, n := 0, x.Len(); i < n && total <= limit && !w.deep; i++ {
			if i > 0 {
				total = satAdd(total, int64(len(", ")))
			}
			total = satAdd(total, w.size(x.Index(i), depth+1, limit-total))
		}
		w.path = w.path[:len(w.path)-1]
		return total
	case starlark.Tuple:
		if depth >= maxRenderDepth {
			w.deep = true
			return 0
		}
		total := int64(len("()"))
		if len(x) == 1 {
			total += int64(len(",")) // (x,)
		}
		for i := 0; i < len(x) && total <= limit && !w.deep; i++ {
			if i > 0 {
				total = satAdd(total, int64(len(", ")))
			}
			total = satAdd(total, w.size(x[i], depth+1, limit-total))
		}
		return total
	case *starlark.Dict:
		if w.onPath(x) {
			return int64(len("{...}"))
		}
		if depth >= maxRenderDepth {
			w.deep = true
			return 0
		}
		total := int64(len("{}"))
		iter := x.Iterate()
		defer iter.Done()
		var k starlark.Value
		for first := true; total <= limit && !w.deep && iter.Next(&k); first = false {
			if !first {
				total = satAdd(total, int64(len(", ")))
			}
			// writeValue renders a KEY on the dict's parent path and the VALUE
			// with the dict pushed; keys are hashable, so only the value can
			// lead back to it.
			total = satAdd(total, w.size(k, depth+1, limit-total))
			total = satAdd(total, int64(len(": ")))
			if val, found, err := x.Get(k); found && err == nil {
				w.path = append(w.path, x)
				total = satAdd(total, w.size(val, depth+1, limit-total))
				w.path = w.path[:len(w.path)-1]
			}
		}
		return total
	case *starlark.Set:
		if depth >= maxRenderDepth {
			w.deep = true
			return 0
		}
		total := int64(len("set([])"))
		iter := x.Iterate()
		defer iter.Done()
		var e starlark.Value
		for first := true; total <= limit && !w.deep && iter.Next(&e); first = false {
			if !first {
				total = satAdd(total, int64(len(", ")))
			}
			total = satAdd(total, w.size(e, depth+1, limit-total))
		}
		return total
	}
	// Everything else renders through its own String(), which is what
	// writeValue's default arm calls — the host objects' constant names,
	// functions, builtins, range, a module. Measuring it by calling it is
	// exact, and the walk stops at the limit after the first one that is
	// large.
	return int64(len(v.String()))
}

// quotedLen is len(syntax.Quote(s, isBytes)) — the form a string takes inside
// every render but str()'s top level — computed without building it, and
// abandoned once it passes limit. It mirrors Quote's escape table exactly: a
// printable rune as itself, `"` and `\` backslashed, the seven C escapes as
// two bytes, other control bytes, DEL and every INVALID byte as \xNN, and a
// non-printable rune as \uNNNN or \UNNNNNNNN.
func quotedLen(s string, isBytes bool, limit int64) int64 {
	n := int64(len(`""`))
	if isBytes {
		n += int64(len("b"))
	}
	for i := 0; i < len(s) && n <= limit; {
		c := s[i]
		if c < utf8.RuneSelf {
			i++
			switch {
			case c == '"' || c == '\\':
				n += 2
			case c >= 0x20 && c < 0x7f:
				n++
			case c == '\a' || c == '\b' || c == '\f' || c == '\n' || c == '\r' || c == '\t' || c == '\v':
				n += 2
			default:
				n += 4 // \xNN
			}
			continue
		}
		r, width := utf8.DecodeRuneInString(s[i:])
		i += width
		switch {
		case width == 1: // an invalid byte (Quote's `width == 1 && r == RuneError`)
			n += 4
		case strconv.IsPrint(r):
			n += int64(width)
		case r < 0x10000:
			n += 6 // \uNNNN
		default:
			n += 10 // \UNNNNNNNN
		}
	}
	return n
}

// transcodedLen is the length of what str() of a bytes value returns: the
// bytes themselves when they are valid UTF-8, and otherwise each byte of an
// invalid sequence replaced by U+FFFD — three bytes for one.
func transcodedLen(s string, limit int64) int64 {
	var n int64
	for i := 0; i < len(s) && n <= limit; {
		r, width := utf8.DecodeRuneInString(s[i:])
		i += width
		if r == utf8.RuneError && width == 1 {
			n += int64(utf8.RuneLen(utf8.RuneError))
		} else {
			n += int64(width)
		}
	}
	return n
}

// intRenderLen is how many bytes an Int renders to: exact for a machine word,
// and for a bignum a lower bound read off its bit length — |x| >= 2^(bits-1),
// which has floor((bits-1)*log10(2))+1 digits. The constant is log10(2)
// truncated, so float rounding can only make the bound lower, never above
// the real digit count.
func intRenderLen(x starlark.Int) int64 {
	if v, ok := x.Int64(); ok {
		n := int64(1)
		u := uint64(v)
		if v < 0 {
			n++
			u = -u // two's complement: correct for MinInt64 as well
		}
		for ; u >= 10; u /= 10 {
			n++
		}
		return n
	}
	b := x.BigInt()
	n := int64(float64(b.BitLen()-1)*0.30102999) + 1
	if b.Sign() < 0 {
		n++
	}
	return n
}

// floatRenderLen is how many bytes a Float renders to, formatted exactly as
// starlark-go's %g does — shortest round-trip digits, plus ".0" when that
// leaves neither a point nor an exponent — into a stack buffer.
func floatRenderLen(x starlark.Float) int64 {
	f := float64(x)
	switch {
	case math.IsNaN(f):
		return int64(len("nan"))
	case math.IsInf(f, 0):
		return int64(len("+inf"))
	}
	var buf [32]byte
	s := strconv.AppendFloat(buf[:0], f, 'g', -1, 64)
	n := int64(len(s))
	if bytes.IndexByte(s, 'e') < 0 && bytes.IndexByte(s, '.') < 0 {
		n += int64(len(".0"))
	}
	return n
}

// materialisers are the universe builtins whose result grows with their input:
// each is shadowed by a wrapper that charges what it built. Name resolution
// consults predeclared BEFORE the universe (resolve.useToplevel), so these
// wrappers are what a script's `list(...)` actually calls —
// TestBoundedBuiltinsShadowTheUniverse proves it.
var materialisers = []string{"bytes", "dict", "enumerate", "list", "repr", "reversed", "set", "sorted", "str", "tuple", "zip"}

// renderers are the materialisers whose output is a STRING built from its
// input's contents rather than a sequence of references to it, i.e. the ones
// whose result is not bounded by the element-count check. bytes() is not one:
// its result is either its string argument or one byte per element, both
// already inside maxStringBytes/maxSeqElems.
var renderers = map[string]bool{"repr": true, "str": true}

// materialisedRenderSize is what str(v) or repr(v) builds. repr is the render
// itself. str differs only at the TOP level: a string comes back as the operand
// it is (charged at its length, as before, though nothing is copied), and
// bytes are transcoded to UTF-8 with each invalid byte widened to U+FFFD.
func materialisedRenderSize(name string, v starlark.Value, limit int64) (int64, bool) {
	if name == "str" {
		switch x := v.(type) {
		case starlark.String:
			return int64(len(x)), false
		case starlark.Bytes:
			return transcodedLen(string(x), limit), false
		}
	}
	return renderSize(v, limit)
}

func boundedMaterialiser(name string) *starlark.Builtin {
	inner, ok := starlark.Universe[name].(*starlark.Builtin)
	if !ok {
		panic("transform: no universe builtin named " + name) // a starlark-go upgrade removed it
	}
	renders := renderers[name] // resolved once per program, not per call
	return starlark.NewBuiltin(name, func(th *starlark.Thread, _ *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
		bud := budgetOf(th)
		if err := bud.overtime(); err != nil {
			return nil, positioned(th, err)
		}
		// Refuse an over-limit input before it is copied. Unreachable while
		// range is the only unbounded source (nothing that big can exist), and
		// kept because the next lazy Sequence added to this package would make
		// it reachable again.
		for _, a := range args {
			if s, ok := a.(starlark.Sequence); ok && int64(s.Len()) > maxSeqElems {
				return nil, positioned(th, fmt.Errorf("%s() of %d elements is over the %d-element limit", name, s.Len(), int64(maxSeqElems)))
			}
		}
		// A RENDER is bounded by what it will build, not by what it is given:
		// the count check above bounds a copy of references, which is the
		// whole cost of the other materialisers, but str()/repr() write out
		// every element's contents.
		if renders && len(args) == 1 {
			sz, deep := materialisedRenderSize(name, args[0], bud.valueCeiling(0))
			if err := bud.checkRender(sz, deep, 0); err != nil {
				return nil, positioned(th, fmt.Errorf("%s(): %w", name, err))
			}
		}
		v, err := inner.CallInternal(th, args, kwargs)
		if err != nil {
			return nil, err
		}
		if err := bud.spend(materialisedBytes(name, v)); err != nil {
			return nil, positioned(th, err)
		}
		return v, nil
	})
}

// boxedSliceBytes is the heap header a slice-typed value costs once it is
// stored in an interface: a starlark.Tuple element of a list is boxed.
const boxedSliceBytes = 24

// materialisedBytes is what the named materialiser BUILT, which for two of
// them is more than the references valueBytes charges: enumerate() and zip()
// return a list of FRESH tuples, each one list word, its slots in a shared
// backing array, and a boxed slice header (starlark-go's library.go builds
// exactly that). Charging them as a plain list of len words was 4.5x short —
// seven retained `enumerate(range(1<<20))` allocated 504 MiB in one
// invocation against the 128 MiB budget, where a plain list() loop is refused
// well before it. Small ints need no charge: starlark-go stores them in the
// interface word itself.
func materialisedBytes(name string, v starlark.Value) int64 {
	if name == "enumerate" || name == "zip" {
		if l, ok := v.(*starlark.List); ok && l.Len() > 0 {
			arity := int64(2) // enumerate's (index, value)
			if t, ok := l.Index(0).(starlark.Tuple); ok {
				arity = int64(len(t)) // zip's: one slot per argument
			}
			per := satAdd(satAdd(bytesPerValue, satMul(arity, bytesPerValue)), boxedSliceBytes)
			return satMul(int64(l.Len()), per)
		}
	}
	return valueBytes(v)
}

// valueBytes is what a materialised value costs, charged AFTER it is built.
//
// This used to claim these builtins "cannot exceed a per-value limit without
// an over-limit input (which is refused above)", and that was false for the
// two whose result is a RENDER: `str([body] * 1024)` passes the element-count
// check on a 1024-element sequence and builds a thousand copies of the body,
// so the charge arrived after ~1.5 GiB had already been allocated. What the
// input check bounds is the number of references a materialiser COPIES;
// renderSize bounds what a render WRITES, before the call. The charge here
// still exists to bound REPETITION — a loop of individually legal calls.
func valueBytes(v starlark.Value) int64 {
	switch v := v.(type) {
	case starlark.String:
		return int64(len(v))
	case starlark.Bytes:
		return int64(len(v))
	case *starlark.Dict:
		return satMul(int64(v.Len()), 2*bytesPerValue)
	case *starlark.Set:
		return satMul(int64(v.Len()), 2*bytesPerValue)
	case starlark.Sequence:
		return satMul(int64(v.Len()), bytesPerValue)
	}
	return 0
}
