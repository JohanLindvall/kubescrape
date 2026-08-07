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
// re functions.
func rePatternAndString(name string, args starlark.Tuple, kwargs []starlark.Tuple) (*regexp.Regexp, string, error) {
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
			"match": starlark.NewBuiltin("re.match", func(_ *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
				re, s, err := rePatternAndString(b.Name(), args, kwargs)
				if err != nil {
					return nil, err
				}
				return starlark.Bool(re.MatchString(s)), nil
			}),
			// re.find(pattern, s) -> str | None: the first match.
			"find": starlark.NewBuiltin("re.find", func(_ *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
				re, s, err := rePatternAndString(b.Name(), args, kwargs)
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
			"findall": starlark.NewBuiltin("re.findall", func(_ *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
				re, s, err := rePatternAndString(b.Name(), args, kwargs)
				if err != nil {
					return nil, err
				}
				ms := re.FindAllString(s, -1)
				out := make([]starlark.Value, len(ms))
				for i, m := range ms {
					out[i] = starlark.String(m)
				}
				return starlark.NewList(out), nil
			}),
			// re.groups(pattern, s) -> [str] | None: the first match's whole
			// text and capture groups ([0] = the match, [1:] = groups; an
			// unmatched optional group is "").
			"groups": starlark.NewBuiltin("re.groups", func(_ *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
				re, s, err := rePatternAndString(b.Name(), args, kwargs)
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
			"replace": starlark.NewBuiltin("re.replace", func(_ *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
				var pat, repl, s string
				if err := starlark.UnpackPositionalArgs(b.Name(), args, kwargs, 3, &pat, &repl, &s); err != nil {
					return nil, err
				}
				re, err := compiledPattern(pat)
				if err != nil {
					return nil, fmt.Errorf("%s: %w", b.Name(), err)
				}
				return starlark.String(re.ReplaceAllString(s, repl)), nil
			}),
		},
	}
}

// scriptLogGates throttle log() to one line per second per signal: the
// builtin exists for debugging a predicate, and a script calling it per
// record on a busy node would flood the agent's own log stream.
var scriptLogGates sync.Map // signal -> *logdedupe.Throttle

// logBuiltin returns the per-signal log(msg) function.
func logBuiltin(signal string) *starlark.Builtin {
	gate, _ := scriptLogGates.LoadOrStore(signal, &logdedupe.Throttle{})
	throttle := gate.(*logdedupe.Throttle)
	return starlark.NewBuiltin("log", func(_ *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
		var msg starlark.Value
		if err := starlark.UnpackPositionalArgs(b.Name(), args, kwargs, 1, &msg); err != nil {
			return nil, err
		}
		if throttle.Allow(time.Second) {
			if s, ok := starlark.AsString(msg); ok {
				slog.Info("transform script log", "signal", signal, "msg", s)
			} else {
				slog.Info("transform script log", "signal", signal, "msg", msg.String())
			}
		}
		return starlark.None, nil
	})
}

// predeclared is the environment a signal's script compiles against.
func predeclared(signal string) starlark.StringDict {
	return starlark.StringDict{
		"re":  reModule(),
		"log": logBuiltin(signal),
	}
}
