package transform

// The predeclared environment every script compiles against. Deliberately
// tiny — Starlark here is hermetic (no I/O, no imports, no clock), and each
// addition below keeps that property:
//
//   - re.match/find/findall/replace/groups — RE2 over strings, with a
//     bounded compiled-pattern cache (lock-free hits, arbitrary eviction at
//     its entry, byte and instruction bounds — see compiledPattern).
//     This was the single most-hit wall in real migrations: OTTL conditions
//     are IsMatch/replace_pattern shaped, and string methods only cover the
//     patterns that happen to be closed alternations.
//   - log(msg) — a THROTTLED line into the agent log for script debugging
//     (1/s per signal; a script logging per record must not turn the export
//     path into a log flood of its own). The universe print() is shadowed onto
//     the same gate, and it and fail() project what they render, like str().
//
// Plus, since name resolution consults predeclared BEFORE the universe, the
// bounded shadows of the amplifying universe builtins and the two guards the
// `*`/`+` rewrite compiles to (limits.go, rewrite.go). Everything else in the
// universe stays as it is.

import (
	"fmt"
	"log/slog"
	"regexp"
	resyntax "regexp/syntax"
	"strings"
	"sync"
	"time"

	"go.starlark.net/starlark"
	"go.starlark.net/starlarkstruct"

	"github.com/JohanLindvall/kubescrape/internal/clip"
	"github.com/JohanLindvall/kubescrape/internal/logdedupe"
	"github.com/JohanLindvall/kubescrape/internal/regexcost"
)

// maxCachedPatterns bounds the compiled-regex cache. Scripts use literal
// patterns, so the working set is tiny; the bound exists because a script
// COULD build patterns dynamically, and an unbounded cache keyed by
// attacker-influenced strings is a leak.
const maxCachedPatterns = 1024

// maxPatternBytes bounds ONE pattern's TEXT. A count is not a memory bound:
// the cache key is the pattern STRING, and `re.match(r.attributes["p"],
// r.body)` keys it on data, so 1024 entries of whatever size happened to land
// there is 1024x that size retained for the process' lifetime on a DaemonSet
// pod the chart limits to 512Mi. 8 KiB is far past any pattern a human writes
// — the longest realistic shape is one alternation of names, which the parser
// factors down to a few hundred instructions.
//
// What the length does NOT bound is what the pattern compiles INTO, and this
// comment used to claim it did: a counted repeat multiplies, so the 8,000-byte
// `\w{1000}` x 1000 is a million-instruction program (42 MB retained, 263 ms of
// compile inside one interpreter step), and 2,700 capture groups fit in the
// same 8 KiB. maxPatternInsts and maxPatternGroups are those bounds.
const maxPatternBytes = 8 << 10

// maxCachedPatternBytes bounds the pattern strings the cache retains, so
// eviction bounds MEMORY and not just entries — which holds only because a
// miss stores a COPY of the pattern (compiledPattern), never the caller's
// view into a larger string. It bounds the KEYS only: the
// compiled programs beside them are maxCachedInsts' job. regexp's own ErrLarge
// ceiling is no bound worth the name here — it admits ~3.3M instructions, i.e.
// ~130 MB, per pattern.
const maxCachedPatternBytes = 1 << 20

// maxPatternInsts bounds ONE pattern's compiled program, ESTIMATED from its
// parse tree before anything is compiled (regexcost.Insts — the same
// arithmetic regexp/syntax applies for its own ErrLarge check, so the estimate
// tracks the real program to within the instructions every program carries). It is
// the bound on both costs a data-derived pattern can amplify: the compile (and
// the memory it retains in the cache), and every MATCH, since RE2's matcher is
// linear in the subject with a constant proportional to the program. 16Ki is
// ~8x the largest counted repeat a human writes (`.{0,1000}` is 2000) and two
// orders past a factored alternation of names; the million-instruction shape
// above is refused, in O(pattern) time, before it compiles.
const maxPatternInsts = 1 << 14

// maxPatternGroups bounds capture groups. The submatch paths — re.groups, and
// re.replace with a `$` reference — copy the capture slots per matcher thread
// per step, which makes one call roughly cubic in the group count on an
// adversarial subject: measured at 2.09 s for 1,200 groups over a 1,200-byte
// subject, with 2,700 fitting in maxPatternBytes. At 64 the same shape costs
// ~2.6x a group-free match of the same subject (160 ms against 61 ms over 64
// KiB), and real parsers — an access-log line — use a dozen.
const maxPatternGroups = 64

// maxCachedInsts bounds the compiled programs the cache retains, measured in
// the same estimated instructions: 16 maximum-size patterns, ~11 MB at the
// ~42 bytes per instruction a compiled program measured.
const maxCachedInsts = 1 << 18

// cachedPattern is one cache entry: the program and the instruction estimate
// it was admitted (and must be evicted) under.
type cachedPattern struct {
	re    *regexp.Regexp
	insts int
}

var (
	// reCache maps a pattern to its *cachedPattern. A sync.Map rather than a
	// mutex-guarded map because the HIT is the hot path — every re verb on
	// every signal, from every export goroutine — and under a mutex every
	// lookup in the process serialised on one lock: aggregate throughput of an
	// anchored lookup+match went from ~90 ns/op at one CPU to ~150 at sixteen,
	// while the match alone scaled from ~64 to ~9. Keys are written once and
	// read many times, which is the case sync.Map is built for.
	reCache sync.Map
	// reMu serialises insertion and eviction, and guards the three tallies
	// below. A load never takes it; the tallies change only under it, beside
	// the Store/Delete they account for, so they always describe the map.
	reMu         sync.Mutex
	reCacheN     int // entries
	reCacheLen   int // sum of the cached patterns' lengths
	reCacheInsts int // sum of their instruction estimates
)

func compiledPattern(pat string) (*regexp.Regexp, error) {
	if len(pat) > maxPatternBytes {
		return nil, fmt.Errorf("pattern of %d bytes is over the %d-byte limit (patterns are cached for the life of the process, so a data-derived one must not be arbitrarily large)",
			len(pat), maxPatternBytes)
	}
	if c, ok := reCache.Load(pat); ok {
		return c.(*cachedPattern).re, nil
	}
	// Everything below runs on a MISS only, OUTSIDE the lock: this cache is
	// shared by every re verb on every signal, so a compile under it would
	// serialise every concurrent invocation in the process — and a pattern
	// that fails to compile (or is refused) is never cached, so a stream of
	// them pays this per call. A racing duplicate compile is wasted work and
	// nothing worse.
	insts, err := patternCost(pat)
	if err != nil {
		return nil, err
	}
	// A COPY, exactly len(pat) long, before anything retains it: the cache
	// keeps the string twice — as the map key, and as the compiled Regexp's
	// source text — and a data-derived pattern is usually a VIEW of something
	// much larger (r.body[0:16] slices the body; an escape-free lifted JSON
	// attribute aliases the whole line), so without the copy each entry pinned
	// its subject for the life of the process while reCacheLen counted only
	// the pattern: 32 sixteen-byte patterns sliced from 1 MiB bodies kept 32
	// MiB live under a tally reading 512 bytes.
	pat = strings.Clone(pat)
	re, err := regexp.Compile(pat)
	if err != nil {
		return nil, err
	}
	reMu.Lock()
	defer reMu.Unlock()
	if c, ok := reCache.Load(pat); ok {
		return c.(*cachedPattern).re, nil // lost the race; one *Regexp per pattern
	}
	// Evict arbitrarily until every bound holds: correctness never depends on
	// the cache, only cost does. Arbitrary is not the attrs builder's shape —
	// its generational genCache promotes a hot entry across a rotation, where
	// here a working set above the bounds recompiles on most misses — and is
	// tolerable only because scripts use literal patterns, so the working set
	// is tiny.
	for reCacheN >= maxCachedPatterns || reCacheLen+len(pat) > maxCachedPatternBytes ||
		reCacheInsts+insts > maxCachedInsts {
		if !evictOnePattern() {
			break
		}
	}
	reCache.Store(pat, &cachedPattern{re: re, insts: insts})
	reCacheN++
	reCacheLen += len(pat)
	reCacheInsts += insts
	return re, nil
}

// evictOnePattern removes an arbitrary entry and its tallies. The caller holds
// reMu, which is what keeps a concurrent insertion from being evicted twice or
// its tallies from drifting.
func evictOnePattern() bool {
	evicted := false
	reCache.Range(func(k, v any) bool {
		reCache.Delete(k)
		reCacheN--
		reCacheLen -= len(k.(string))
		reCacheInsts -= v.(*cachedPattern).insts
		evicted = true
		return false
	})
	return evicted
}

// patternCost parses pat — as regexp.Compile will, with the Perl flags — and
// refuses it when its capture groups or its ESTIMATED program exceed the
// bounds above, returning the estimate the cache accounts it under. It costs a
// second parse on a cache miss, and the walk is O(pattern): what it must not
// do is compile to count (Simplify + syntax.Compile measured 297 ms for the
// million-instruction shape, as much as the compile it would guard).
func patternCost(pat string) (int, error) {
	tree, err := resyntax.Parse(pat, resyntax.Perl)
	if err != nil {
		return 0, err // regexp.Compile would fail identically; same error text
	}
	if g := tree.MaxCap(); g > maxPatternGroups {
		return 0, fmt.Errorf("pattern with %d capture groups is over the %d-group limit (each submatch step copies every group's slots, so the cost grows much faster than the count) — use (?:...) for groups nothing reads",
			g, maxPatternGroups)
	}
	n := regexcost.Insts(tree, maxPatternInsts)
	if n > maxPatternInsts {
		return 0, fmt.Errorf("pattern compiles to more than %d instructions (a counted repeat multiplies what it repeats: x{1000} is a thousand copies of x) — the program is retained in the cache and walked per subject byte, so a data-derived pattern must not be arbitrarily large",
			maxPatternInsts)
	}
	return n, nil
}

// bytesPerMatch is what one re.findall match costs: the []string entry the
// regexp package returns, the boxed starlark.String and the list's own
// element word. The match TEXT is a slice of the subject and is not copied.
const bytesPerMatch = 3 * bytesPerValue

// replaceSize is an upper bound on what re.replace will build, computed BEFORE
// ReplaceAllString builds it. Without it the `re` module is the one amplifier
// in the predeclared environment with no predictive cap at all: an empty
// pattern matches at every position, a `$1` expands to a capture group's text,
// and the charge used to land after the string existed — measured at 805 MiB
// allocated for a 129 MiB result that was then refused. Neither step guard can
// help, because the whole growth happens inside ONE interpreter step.
//
// The bound, and why it is not a multiple of the subject PER MATCH (which is
// what an earlier version charged, refusing an ordinary `$1=***` redaction of
// a 1 MiB log line from its fifteenth match on — and a refusal is a script
// error, which on the tailer rewinds and rebuilds the same batch every sweep,
// i.e. stops log shipping on the node). With m matches covering M bytes of s
// and refs = the number of `$` in repl:
//
//   - the unmatched text is copied once: len(s) - M;
//   - each match writes repl's literal bytes, at most len(repl): m*len(repl);
//   - each `$` starts at most one reference ($$ and a malformed $ write one
//     byte and are already counted in len(repl)), and a reference expands to a
//     capture group, which RE2 — having no lookaround — places INSIDE its own
//     match. Matches do not overlap, so across ALL matches one reference
//     position expands to at most M bytes in total: refs*M.
//
// So out <= len(s) - M + m*len(repl) + refs*M, and with M <= len(s) and
// m <= len(s)+1 (a pattern matches at most once per position, empty matches
// included) the no-scan worst case is len(s)*max(1, refs) + (len(s)+1)*len(repl).
// When THAT fits the ceiling nothing is scanned — the common case, a short
// replacement over a log line. When it does not, m and M are measured exactly
// by one pass through the same match loop ReplaceAllString uses (so anchors,
// word boundaries and empty-match rules agree), whose scratch is the unmatched
// text — at most the subject, which the real call copies anyway. There is no
// cap on that pass: an earlier probe stopped at 64Ki matches and read "the
// probe filled up" as "over the limit", refusing a digit redaction whose real
// output was 2 MiB.
func replaceSize(re *regexp.Regexp, repl, s string, limit int64) int64 {
	n, r := int64(len(s)), int64(len(repl))
	refs := int64(strings.Count(repl, "$"))
	worst := satAdd(satMul(n, max(refs, 1)), satMul(n+1, r))
	if worst <= limit || r == 0 {
		return worst // r == 0 means no references either: the output is at most s
	}
	var m, matched int64
	re.ReplaceAllStringFunc(s, func(match string) string {
		m++
		matched += int64(len(match))
		return ""
	})
	return satAdd(satAdd(n-matched, satMul(m, r)), satMul(refs, matched))
}

// rePatternAndString unpacks the (pattern, s) argument shape shared by most
// re functions. It also checks the invocation's wall clock: a regex over a
// megabyte-scale body is exactly the kind of single interpreter step that runs
// for seconds, which the between-steps checkpoint cannot see.
func rePatternAndString(th *starlark.Thread, name string, args starlark.Tuple, kwargs []starlark.Tuple) (*regexp.Regexp, string, error) {
	if err := budgetOf(th).overtime(); err != nil {
		return nil, "", positioned(th, err)
	}
	var pat, s string
	if err := starlark.UnpackPositionalArgs(name, args, kwargs, 2, &pat, &s); err != nil {
		return nil, "", err
	}
	re, err := compiledPattern(pat)
	if err != nil {
		return nil, "", fmt.Errorf("%s: %w", name, err)
	}
	return re, s, nil
}

func reModule() *starlarkstruct.Module {
	return &starlarkstruct.Module{
		Name: "re",
		Members: starlark.StringDict{
			// re.match(pattern, s) -> bool: does the pattern match anywhere
			// (unanchored, like OTTL's IsMatch — anchor with ^$ yourself).
			"match": starlark.NewBuiltin("re.match", func(th *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
				re, s, err := rePatternAndString(th, b.Name(), args, kwargs)
				if err != nil {
					return nil, err
				}
				return starlark.Bool(re.MatchString(s)), nil
			}),
			// re.find(pattern, s) -> str | None: the first match.
			"find": starlark.NewBuiltin("re.find", func(th *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
				re, s, err := rePatternAndString(th, b.Name(), args, kwargs)
				if err != nil {
					return nil, err
				}
				loc := re.FindStringIndex(s)
				if loc == nil {
					return starlark.None, nil
				}
				return starlark.String(s[loc[0]:loc[1]]), nil
			}),
			// re.findall(pattern, s) -> [str]: every non-overlapping match.
			"findall": starlark.NewBuiltin("re.findall", func(th *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
				re, s, err := rePatternAndString(th, b.Name(), args, kwargs)
				if err != nil {
					return nil, err
				}
				// A match list grows with the SUBJECT — an empty pattern
				// matches at every position — so the count is bounded before
				// the slice exists rather than charged after it does. The
				// match limit is the only size argument FindAllString has, so
				// it IS the bound: ask for one more than the per-value and
				// budget ceilings can pay for, and refuse when it comes back
				// full.
				bud := budgetOf(th)
				n := min(int64(maxSeqElems), bud.remaining()/bytesPerMatch)
				ms := re.FindAllString(s, int(n)+1)
				if int64(len(ms)) > n {
					return nil, positioned(th, fmt.Errorf("%s: more than %d matches in a %d-byte subject, over the %d-element limit for one value or what is left of the invocation's %d-byte budget — match less per call",
						b.Name(), n, len(s), int64(maxSeqElems), int64(maxAllocBytes)))
				}
				if err := bud.spend(satMul(int64(len(ms)), bytesPerMatch)); err != nil {
					return nil, positioned(th, err)
				}
				out := make([]starlark.Value, len(ms))
				for i, m := range ms {
					out[i] = starlark.String(m)
				}
				return starlark.NewList(out), nil
			}),
			// re.groups(pattern, s) -> [str] | None: the first match's whole
			// text and capture groups ([0] = the match, [1:] = groups; an
			// unmatched optional group is "").
			"groups": starlark.NewBuiltin("re.groups", func(th *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
				re, s, err := rePatternAndString(th, b.Name(), args, kwargs)
				if err != nil {
					return nil, err
				}
				m := re.FindStringSubmatch(s)
				if m == nil {
					return starlark.None, nil
				}
				out := make([]starlark.Value, len(m))
				for i, g := range m {
					out[i] = starlark.String(g)
				}
				return starlark.NewList(out), nil
			}),
			// re.replace(pattern, repl, s) -> str: every match replaced; repl
			// uses Go's $1/${name} group references.
			"replace": starlark.NewBuiltin("re.replace", func(th *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
				if err := budgetOf(th).overtime(); err != nil {
					return nil, positioned(th, err)
				}
				var pat, repl, s string
				if err := starlark.UnpackPositionalArgs(b.Name(), args, kwargs, 3, &pat, &repl, &s); err != nil {
					return nil, err
				}
				re, err := compiledPattern(pat)
				if err != nil {
					return nil, fmt.Errorf("%s: %w", b.Name(), err)
				}
				// A subject the pattern does not match comes back AS IT IS, and
				// uncharged: nothing is built, while ReplaceAllString would copy
				// it twice and the charge below would bill a full body per call.
				// That charge is per INVOCATION, i.e. per batch, so a redaction
				// list run over every record spent patterns x (the batch's body
				// bytes) of the 128 MiB budget whether or not anything matched —
				// ten patterns over a 16 MiB push refused the batch, and a refused
				// batch fails the same way on every retry.
				if re.FindStringIndex(s) == nil {
					return starlark.String(s), nil
				}
				bud := budgetOf(th)
				if sz := replaceSize(re, repl, s, bud.valueCeiling(0)); sz > maxStringBytes {
					return nil, positioned(th, fmt.Errorf("%s: replacing in a %d-byte subject could build up to %d bytes, over the %d-byte limit for one value — replace less per call",
						b.Name(), len(s), sz, int64(maxStringBytes)))
				} else if err := bud.project(sz); err != nil {
					return nil, positioned(th, err)
				}
				out := re.ReplaceAllString(s, repl)
				if err := bud.spend(int64(len(out))); err != nil {
					return nil, positioned(th, err)
				}
				return starlark.String(out), nil
			}),
		},
	}
}

// scriptLogGates throttle a script's output to one line per second per signal:
// the surface exists for debugging a predicate, and a script calling it per
// record on a busy node would flood the agent's own log stream — which is
// itself collected, so the flood is also input.
//
// A fixed struct over the closed signal set, like runWarnGates, and not the
// sync.Map it was: LoadOrStore built its &Throttle{} candidate and boxed the
// signal into an `any` key on EVERY call, i.e. two heap allocations per
// log()/print() — including the suppressed calls, which are the ones the
// throttle exists to make cheap (a script logging per record paid 2048 per
// 1024-record batch).
var scriptLogGates struct {
	logs, metrics, traces, ingest, targets, sample, parse logdedupe.Throttle
	// other is for a signal this list does not name; none is compiled today.
	other logdedupe.Throttle
}

// scriptLogGate is the signal's gate in scriptLogGates.
func scriptLogGate(signal string) *logdedupe.Throttle {
	switch signal {
	case "logs":
		return &scriptLogGates.logs
	case "metrics":
		return &scriptLogGates.metrics
	case "traces":
		return &scriptLogGates.traces
	case "ingest":
		return &scriptLogGates.ingest
	case "targets":
		return &scriptLogGates.targets
	case "sample":
		return &scriptLogGates.sample
	case "parse":
		return &scriptLogGates.parse
	}
	return &scriptLogGates.other
}

// scriptLog is the ONE path a script's output takes into the agent log, shared
// by log(msg), print() and — as the backstop Thread.Print — anything else that
// reaches the thread's printer: the universe's print is reachable from every
// script, and with Thread.Print unset starlark-go falls back to
// fmt.Fprintln(os.Stderr) — unstructured lines injected raw into the agent's
// own stream, unthrottled. One in-cluster minute measured 68 such print lines
// against 17 correctly throttled log() ones. They share ONE gate deliberately:
// the flood is what matters, not which spelling produced it.
//
// The attribute is `output` and NOT `msg`, which is what it used to be: `msg`
// is slog's own key for the record's message, so the line carried two of them
// — `msg="transform script log" … msg="<whatever the script said>"` — and a
// consumer resolving duplicates last-wins reads the SCRIPT's text as the log
// message. That is a script writing into a reserved field of the agent's own
// log, which is a worse property than the unthrottled stderr this function
// exists to prevent.
func scriptLog(signal, msg string) {
	if scriptLogAllowed(signal) {
		writeScriptLog(signal, msg)
	}
}

// writeScriptLog writes one already-admitted line: the builtins below take the
// gate BEFORE they render, so they must not pass through scriptLog's gate a
// second time — it would suppress the line it had just allowed.
//
// The line is clipped (scriptText). The throttle bounds how OFTEN a script
// writes here and nothing bounded how MUCH: a debugging `log(r.body)` wrote the
// whole body — up to the 16 MiB ingest cap — once a second per signal, into a
// stream the agent itself collects and the kubelet rotates at 10Mi.
func writeScriptLog(signal, msg string) {
	slog.Info("transform script log", "signal", signal, "output", scriptText(msg))
}

// maxScriptLogBytes bounds any script-authored text this package writes into
// the agent's own log — a log()/print() line, the text of a script's runtime
// error. 4 KiB is a screenful: enough to debug a predicate by, far short of a
// body.
const maxScriptLogBytes = 4 << 10

// scriptText clips script-authored text for the agent's log, on a rune
// boundary and marked, like every caller-supplied log attribute (internal/clip).
func scriptText(s string) string { return clip.Ellipsis(s, maxScriptLogBytes) }

// scriptError is a script failure whose TEXT is clipped (scriptText) while the
// error it wraps stays reachable: errors.As still finds the *starlark.EvalError
// scriptPos reads the position from. The text is what leaves this package — in
// the runtime-error and hook-error Warns, in the error every producer logs
// when its export fails, in the gRPC status an ingest sender receives — and a
// script's own words are in it: `fail(r.body)` put the body in all of them.
type scriptError struct{ err error }

func (e scriptError) Error() string { return scriptText(e.err.Error()) }
func (e scriptError) Unwrap() error { return e.err }

func scriptLogAllowed(signal string) bool {
	return scriptLogGate(signal).Allow(time.Second)
}

// logBuiltin returns the per-signal log(msg) function. The gate is taken
// BEFORE the value is rendered: a non-string argument renders through
// Value.String(), which walks the whole value, and paying that on every
// suppressed call would make the throttle bound only the output.
func logBuiltin(signal string) *starlark.Builtin {
	return starlark.NewBuiltin("log", func(th *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
		// The wall-clock check every builtin here owes (limits.go).
		bud := budgetOf(th)
		if err := bud.overtime(); err != nil {
			return nil, positioned(th, err)
		}
		var msg starlark.Value
		if err := starlark.UnpackPositionalArgs(b.Name(), args, kwargs, 1, &msg); err != nil {
			return nil, err
		}
		if !scriptLogAllowed(signal) {
			return starlark.None, nil
		}
		s, ok := starlark.AsString(msg)
		if !ok {
			// The render is str()'s amplifier under another name, and this one
			// ends up in the agent's OWN log stream — which is collected — so
			// it is projected before it is built.
			sz, deep := renderSize(msg, bud.valueCeiling(0))
			if err := bud.checkRender(sz, deep, 0); err != nil {
				return nil, positioned(th, fmt.Errorf("%s: %w", b.Name(), err))
			}
			s = msg.String()
		}
		writeScriptLog(signal, s)
		return starlark.None, nil
	})
}

// joinedRenderSize projects the message print() and fail() build: prefix, then
// every argument separated by sep — a string written as it is (and, for
// print, bytes too), anything else rendered as str() of a container would
// render it. It stops once it passes limit.
func joinedRenderSize(prefix string, args starlark.Tuple, sep string, rawBytes bool, limit int64) (int64, bool) {
	total := int64(len(prefix))
	for i := 0; i < len(args) && total <= limit; i++ {
		if i > 0 {
			total = satAdd(total, int64(len(sep)))
		}
		if s, ok := starlark.AsString(args[i]); ok {
			total = satAdd(total, int64(len(s)))
			continue
		}
		if bs, ok := args[i].(starlark.Bytes); ok && rawBytes {
			total = satAdd(total, int64(len(bs)))
			continue
		}
		sz, deep := renderSize(args[i], max(0, limit-total))
		if deep {
			return total, true
		}
		total = satAdd(total, sz)
	}
	return total, false
}

// printBuiltin shadows the universe print(*args, sep=" "), which renders every
// argument — a container through the same writeValue str() uses — into one
// string BEFORE it calls Thread.Print, i.e. before the throttle can suppress
// it and with no projection at all. So `print([body] * 64)` built and logged a
// 64 MiB line where `str()` of the same value is refused, and did it at module
// level too, inside Compile at every startup and every reload, where a process
// that dies dies before there is a last-good program. Here the gate is taken
// first and the message is projected before it is built, as log() does, and
// the line is written directly (writeScriptLog) because the gate is already
// spent. Thread.Print stays wired (engine.go) as the backstop.
func printBuiltin(signal string) *starlark.Builtin {
	return starlark.NewBuiltin("print", func(th *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
		bud := budgetOf(th)
		if err := bud.overtime(); err != nil {
			return nil, positioned(th, err)
		}
		sep := " "
		if err := starlark.UnpackArgs(b.Name(), nil, kwargs, "sep?", &sep); err != nil {
			return nil, err
		}
		if !scriptLogAllowed(signal) {
			return starlark.None, nil
		}
		sz, deep := joinedRenderSize("", args, sep, true, bud.valueCeiling(0))
		if err := bud.checkRender(sz, deep, 0); err != nil {
			return nil, positioned(th, fmt.Errorf("%s: %w", b.Name(), err))
		}
		// Only what the line can carry is built: writeScriptLog clips at
		// maxScriptLogBytes, so one byte past it (enough for the clip to
		// notice and mark the cut) is all that is ever written out. A
		// container argument still renders whole — that render is what the
		// projection above bounds.
		const carry = maxScriptLogBytes + 1
		var buf strings.Builder
		buf.Grow(int(min(sz, carry)))
		for i, v := range args {
			if i > 0 {
				buf.WriteString(sep[:min(len(sep), max(0, carry-buf.Len()))])
			}
			room := carry - buf.Len()
			if room <= 0 {
				break
			}
			s, ok := starlark.AsString(v)
			if !ok {
				if bs, isBytes := v.(starlark.Bytes); isBytes {
					s = string(bs)
				} else {
					s = v.String()
				}
			}
			buf.WriteString(s[:min(len(s), room)])
		}
		writeScriptLog(signal, buf.String())
		return starlark.None, nil
	})
}

// failBuiltin shadows the universe fail(*args, sep=" "), which renders its
// arguments into the error text exactly as print() does — str()'s amplifier
// with no projection — so `fail([body] * 40)` built a 40 MiB error that was
// then copied through the error wrapping and into a Warn line, while `str()` of
// the same value is refused after a megabyte. The message is projected here
// and the universe builtin then builds it, so the error text is unchanged.
func failBuiltin() *starlark.Builtin {
	inner := starlark.Universe["fail"].(*starlark.Builtin)
	return starlark.NewBuiltin("fail", func(th *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
		bud := budgetOf(th)
		if err := bud.overtime(); err != nil {
			return nil, positioned(th, err)
		}
		sep := " "
		if err := starlark.UnpackArgs(b.Name(), nil, kwargs, "sep?", &sep); err != nil {
			return nil, err
		}
		sz, deep := joinedRenderSize("fail: ", args, sep, false, bud.valueCeiling(0))
		if err := bud.checkRender(sz, deep, 0); err != nil {
			return nil, positioned(th, fmt.Errorf("%s: %w", b.Name(), err))
		}
		return inner.CallInternal(th, args, kwargs)
	})
}

// predeclared is the environment a signal's script compiles against.
func predeclared(signal string) starlark.StringDict {
	d := starlark.StringDict{
		"re":    reModule(),
		"log":   logBuiltin(signal),
		"print": printBuiltin(signal),
		"fail":  failBuiltin(),
		// The `*` and `+` rewrite targets. Named, not anonymous, because the
		// resolver only binds names — checkReservedNames keeps a script from
		// binding them itself.
		mulGuard:  mulBuiltin(),
		addGuard:  addBuiltin(),
		iaddGuard: iaddBuiltin(),
		"range":   boundedRange(),
		"int":     boundedInt(),
	}
	for _, name := range materialisers {
		d[name] = boundedMaterialiser(name)
	}
	return d
}
