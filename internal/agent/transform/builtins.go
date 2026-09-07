package transform

// The predeclared environment every script compiles against. Deliberately
// tiny — Starlark here is hermetic (no I/O, no imports, no clock), and each
// addition below keeps that property:
//
//   - re.match/find/findall/replace/groups — RE2 over strings, with a
//     bounded compiled-pattern cache (the attrs builder's eviction shape).
//     This was the single most-hit wall in real migrations: OTTL conditions
//     are IsMatch/replace_pattern shaped, and string methods only cover the
//     patterns that happen to be closed alternations.
//   - log(msg) — a THROTTLED line into the agent log for script debugging
//     (1/s per signal; a script logging per record must not turn the export
//     path into a log flood of its own).
//
// Plus, since name resolution consults predeclared BEFORE the universe, the
// bounded shadows of the amplifying universe builtins and the two guards the
// `*`/`+` rewrite compiles to (limits.go, rewrite.go). Everything else in the
// universe stays as it is.

import (
	"fmt"
	"log/slog"
	"regexp"
	"strings"
	"sync"
	"time"

	"go.starlark.net/starlark"
	"go.starlark.net/starlarkstruct"

	"github.com/JohanLindvall/kubescrape/internal/logdedupe"
)

// maxCachedPatterns bounds the compiled-regex cache. Scripts use literal
// patterns, so the working set is tiny; the bound exists because a script
// COULD build patterns dynamically, and an unbounded cache keyed by
// attacker-influenced strings is a leak.
const maxCachedPatterns = 1024

// maxPatternBytes bounds ONE pattern. A count is not a memory bound: the cache
// key is the pattern STRING, and `re.match(r.attributes["p"], r.body)` keys it
// on data, so 1024 entries of whatever size happened to land there is 1024x
// that size retained for the process' lifetime on a DaemonSet pod the chart
// limits to 512Mi. 8 KiB is far past any pattern a human writes — the longest
// realistic shape is one alternation of names — and it also bounds the compile
// itself, which is what keeps a dynamic pattern from being a CPU amplifier.
const maxPatternBytes = 8 << 10

// maxCachedPatternBytes bounds the pattern strings the cache retains, so
// eviction bounds MEMORY and not just entries. The compiled programs beside
// them are bounded by regexp's own ErrLarge ceiling.
const maxCachedPatternBytes = 1 << 20

var (
	reMu       sync.Mutex
	reCache    = map[string]*regexp.Regexp{}
	reCacheLen int // sum of the cached patterns' lengths
)

func compiledPattern(pat string) (*regexp.Regexp, error) {
	if len(pat) > maxPatternBytes {
		return nil, fmt.Errorf("pattern of %d bytes is over the %d-byte limit (patterns are cached for the life of the process, so a data-derived one must not be arbitrarily large)",
			len(pat), maxPatternBytes)
	}
	reMu.Lock()
	if re, ok := reCache[pat]; ok {
		reMu.Unlock()
		return re, nil
	}
	reMu.Unlock()
	// Compiled OUTSIDE the lock: this cache is shared by every re verb on
	// every signal, so a compile under it serialises every concurrent
	// invocation in the process — and a pattern that FAILS to compile is
	// never cached, so a stream of malformed ones would pay it per call. A
	// racing duplicate compile is wasted work and nothing worse.
	re, err := regexp.Compile(pat)
	if err != nil {
		return nil, err
	}
	reMu.Lock()
	defer reMu.Unlock()
	if cached, ok := reCache[pat]; ok {
		return cached, nil // lost the race; one *Regexp per pattern
	}
	// Evict arbitrarily (map order) until both bounds hold: correctness never
	// depends on the cache, only cost does.
	for len(reCache) >= maxCachedPatterns || reCacheLen+len(pat) > maxCachedPatternBytes {
		k, ok := anyKey(reCache)
		if !ok {
			break
		}
		delete(reCache, k)
		reCacheLen -= len(k)
	}
	reCache[pat] = re
	reCacheLen += len(pat)
	return re, nil
}

func anyKey(m map[string]*regexp.Regexp) (string, bool) {
	for k := range m {
		return k, true
	}
	return "", false
}

// maxMatchProbe bounds how many matches one PREDICTIVE probe may collect
// before it gives up and answers with the worst case. The probe exists to
// rescue a call whose worst case is enormous but whose real match count is
// small (a long replacement applied at a handful of places); past this many
// matches the answer is the same either way, so paying for a longer scan buys
// nothing. 64Ki index pairs is ~2.6 MB of transient scratch, an order below
// what the call it is deciding about would allocate.
const maxMatchProbe = 1 << 16

// bytesPerMatch is what one re.findall match costs: the []string entry the
// regexp package returns, the boxed starlark.String and the list's own
// element word. The match TEXT is a slice of the subject and is not copied.
const bytesPerMatch = 3 * bytesPerValue

// replExpansion is an upper bound on ONE expanded replacement. len(repl) is
// not it: Go's $1/${name} references expand to a capture group's text, so a
// short repl full of references expands to a multiple of the SUBJECT. Every
// `$` starts at most one reference and a group is at most the whole subject.
func replExpansion(repl string, subj int) int64 {
	refs := int64(strings.Count(repl, "$"))
	return satAdd(int64(len(repl)), satMul(refs, int64(max(subj, 1))))
}

// replaceSize projects re.replace's output BEFORE ReplaceAllString builds it.
// Without this the `re` module is the one amplifier in the predeclared
// environment with no predictive cap at all: the output is (matches x expanded
// replacement) bytes, an empty pattern matches at every position, and the
// charge landed after the string existed — measured at 805 MiB allocated for a
// 129 MiB result that was then refused. Neither step guard can help, because
// the whole growth happens inside ONE interpreter step.
//
// Two tiers, because the cheap bound alone would refuse honest scripts. A
// pattern matches at most once per position (empty matches included), so the
// output is at most len(s) + (len(s)+1) x one expanded replacement — and when
// THAT already fits, nothing needs to be scanned. When it does not, the
// matches are COUNTED, with the probe itself bounded: it asks for one more
// match than the ceiling could pay for, so the scan allocates in proportion to
// what we were prepared to let the call build, and reaching its cap means the
// projection is over the ceiling anyway.
func replaceSize(re *regexp.Regexp, repl, s string, limit int64) int64 {
	l := replExpansion(repl, len(s))
	worst := satAdd(int64(len(s)), satMul(int64(len(s))+1, l))
	if worst <= limit {
		return worst
	}
	n := limit/max(l, 1) + 1 // matches the ceiling can still pay for
	n = min(n, maxMatchProbe, int64(len(s))+1)
	locs := re.FindAllStringIndex(s, int(n))
	if int64(len(locs)) >= n {
		return worst
	}
	return satAdd(int64(len(s)), satMul(int64(len(locs)), l))
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
				bud := budgetOf(th)
				limit := int64(maxStringBytes)
				if r := bud.remaining(); r < limit {
					limit = r
				}
				if sz := replaceSize(re, repl, s, limit); sz > maxStringBytes {
					return nil, positioned(th, fmt.Errorf("%s: a %d-byte subject and a replacement expanding to up to %d bytes could build past the %d-byte limit for one value — replace less per call",
						b.Name(), len(s), replExpansion(repl, len(s)), int64(maxStringBytes)))
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
var scriptLogGates sync.Map // signal -> *logdedupe.Throttle

// scriptLog is the ONE path a script's output takes into the agent log, shared
// by log(msg) and by print(): the universe's print is reachable from every
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
		slog.Info("transform script log", "signal", signal, "output", msg)
	}
}

func scriptLogAllowed(signal string) bool {
	gate, _ := scriptLogGates.LoadOrStore(signal, &logdedupe.Throttle{})
	return gate.(*logdedupe.Throttle).Allow(time.Second)
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
			limit := int64(maxStringBytes)
			if r := bud.remaining(); r < limit {
				limit = r
			}
			if sz := renderSize(msg, 0, limit); sz > maxStringBytes {
				return nil, positioned(th, fmt.Errorf("%s: the argument would render at least %d bytes, over the %d-byte limit for one value", b.Name(), sz, int64(maxStringBytes)))
			} else if err := bud.project(sz); err != nil {
				return nil, positioned(th, err)
			}
			s = msg.String()
		}
		slog.Info("transform script log", "signal", signal, "output", s)
		return starlark.None, nil
	})
}

// predeclared is the environment a signal's script compiles against.
func predeclared(signal string) starlark.StringDict {
	d := starlark.StringDict{
		"re":  reModule(),
		"log": logBuiltin(signal),
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
