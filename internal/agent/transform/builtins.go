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

var (
	reMu    sync.Mutex
	reCache = map[string]*regexp.Regexp{}
)

func compiledPattern(pat string) (*regexp.Regexp, error) {
	reMu.Lock()
	defer reMu.Unlock()
	if re, ok := reCache[pat]; ok {
		return re, nil
	}
	re, err := regexp.Compile(pat)
	if err != nil {
		return nil, err
	}
	if len(reCache) >= maxCachedPatterns {
		// Evict arbitrarily (map order): correctness never depends on the
		// cache, only cost does.
		for k := range reCache {
			delete(reCache, k)
			break
		}
	}
	reCache[pat] = re
	return re, nil
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
				ms := re.FindAllString(s, -1)
				// A match list grows with the subject, so it is charged like any
				// other materialised sequence.
				if err := budgetOf(th).spend(satMul(int64(len(ms)), bytesPerValue)); err != nil {
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
				out := re.ReplaceAllString(s, repl)
				if err := budgetOf(th).spend(int64(len(out))); err != nil {
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
	return starlark.NewBuiltin("log", func(_ *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
		var msg starlark.Value
		if err := starlark.UnpackPositionalArgs(b.Name(), args, kwargs, 1, &msg); err != nil {
			return nil, err
		}
		if !scriptLogAllowed(signal) {
			return starlark.None, nil
		}
		s, ok := starlark.AsString(msg)
		if !ok {
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
