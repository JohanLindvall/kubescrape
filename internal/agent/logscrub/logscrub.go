// Package logscrub redacts sensitive values from log bodies before export:
// a curated set of built-in patterns (tokens, credentials, keys) plus
// user-defined regexes, applied in the tailer, journald and OTLP-ingest log
// paths. Redaction happens on the agent so secrets never leave the node.
//
// Per-line cost discipline: every built-in pattern carries a cheap literal
// prefilter — the regex only runs on lines that contain a telltale substring,
// so the no-match hot path is a handful of strings.Contains calls and zero
// allocations. A scrubbed line allocates (it must — the body changes).
package logscrub

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/JohanLindvall/kubescrape/internal/obs"
)

// Config is the agent config's logScrubbing section.
type Config struct {
	// Builtin enables named built-in patterns. The special name "defaults"
	// enables the low-false-positive set (bearer, basic-auth, secret-kv,
	// aws-key, private-key). "email" and "credit-card" are opt-in by name —
	// they redact legitimate content too often to be defaults.
	Builtin []string `json:"builtin,omitempty"`
	// Rules are additional user patterns, applied after the built-ins.
	Rules []Rule `json:"rules,omitempty"`
}

// Rule is one user-defined redaction.
type Rule struct {
	// Name labels the pattern in the drop metric.
	Name string `json:"name"`
	// Regexp is the pattern; the WHOLE match is replaced.
	Regexp string `json:"regexp"`
	// Replacement substitutes the match ($1-style group references work);
	// empty means "[REDACTED]".
	Replacement string `json:"replacement,omitempty"`
}

const redacted = "[REDACTED]"

// pattern is one compiled redaction with its prefilter.
type pattern struct {
	name string
	re   *regexp.Regexp
	repl string
	// prefilter cheaply rejects lines that cannot match (nil = always run).
	prefilter func(string) bool
}

func containsFold(sub string) func(string) bool {
	lower := strings.ToLower(sub)
	upper := strings.ToUpper(sub)
	title := strings.ToUpper(sub[:1]) + sub[1:]
	return func(s string) bool {
		// Three exact scans beat a per-line ToLower allocation; mixed-case
		// beyond Title/UPPER falls through to the (case-insensitive) regex
		// only when one of the common forms appears.
		return strings.Contains(s, lower) || strings.Contains(s, title) || strings.Contains(s, upper)
	}
}

// digitRun reports a run of >= n digits, ignoring single spaces/dashes inside.
func digitRun(n int) func(string) bool {
	return func(s string) bool {
		run := 0
		for i := 0; i < len(s); i++ {
			c := s[i]
			switch {
			case c >= '0' && c <= '9':
				run++
				if run >= n {
					return true
				}
			case (c == ' ' || c == '-') && run > 0:
				// separator inside a group: allowed, does not reset
			default:
				run = 0
			}
		}
		return false
	}
}

// Prefilter closures are built ONCE — containsFold allocates its case
// variants at construction, so building them per line would put ~20 allocs
// on the no-match hot path.
var (
	pfKey    = containsFold("key")
	pfSecret = containsFold("secret")
	pfPassw  = containsFold("passw")
	pfPwd    = containsFold("pwd")
	pfToken  = containsFold("token")
)

// builtins is the catalog. Every pattern replaces the whole match unless it
// captures a prefix group to keep (the kv patterns keep the key and the
// separator so the log line stays readable).
var builtins = map[string]pattern{
	"bearer": {
		name:      "bearer",
		re:        regexp.MustCompile(`(?i)\b(bearer\s+)[A-Za-z0-9\-._~+/]+=*`),
		repl:      "${1}" + redacted,
		prefilter: containsFold("bearer"),
	},
	"basic-auth": {
		name:      "basic-auth",
		re:        regexp.MustCompile(`(?i)\b(basic\s+)[A-Za-z0-9+/]{8,}=*`),
		repl:      "${1}" + redacted,
		prefilter: containsFold("basic"),
	},
	"secret-kv": {
		name: "secret-kv",
		re:   regexp.MustCompile(`(?i)\b((?:api[_-]?key|secret|password|passwd|pwd|token|access[_-]?key)["']?\s*[:=]\s*["']?)[^\s"'&,;]+`),
		repl: "${1}" + redacted,
		prefilter: func(s string) bool {
			return pfKey(s) || pfSecret(s) || pfPassw(s) || pfPwd(s) || pfToken(s)
		},
	},
	"aws-key": {
		name:      "aws-key",
		re:        regexp.MustCompile(`\b(?:AKIA|ASIA)[0-9A-Z]{16}\b`),
		prefilter: func(s string) bool { return strings.Contains(s, "AKIA") || strings.Contains(s, "ASIA") },
		repl:      redacted,
	},
	"private-key": {
		name:      "private-key",
		re:        regexp.MustCompile(`-----BEGIN [A-Z ]*PRIVATE KEY-----[\s\S]*?(?:-----END [A-Z ]*PRIVATE KEY-----|$)`),
		prefilter: func(s string) bool { return strings.Contains(s, "PRIVATE KEY") },
		repl:      redacted,
	},
	"email": {
		name:      "email",
		re:        regexp.MustCompile(`\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b`),
		prefilter: func(s string) bool { return strings.ContainsRune(s, '@') },
		repl:      redacted,
	},
	"credit-card": {
		name:      "credit-card",
		re:        regexp.MustCompile(`\b(?:\d[ -]?){12}\d{1,4}\b`),
		prefilter: digitRun(13),
		repl:      redacted,
	},
}

// defaultSet is the low-false-positive selection "defaults" expands to.
var defaultSet = []string{"bearer", "basic-auth", "secret-kv", "aws-key", "private-key"}

// Scrubber applies the configured redactions.
type Scrubber struct {
	patterns []pattern
}

// New compiles the config. Unknown built-in names and invalid regexes fail
// fast — a scrubber that silently skips a pattern is a compliance bug.
func New(cfg Config) (*Scrubber, error) {
	var s Scrubber
	seen := map[string]bool{}
	add := func(name string) error {
		if seen[name] {
			return nil
		}
		p, ok := builtins[name]
		if !ok {
			return fmt.Errorf("unknown builtin scrub pattern %q", name)
		}
		seen[name] = true
		s.patterns = append(s.patterns, p)
		return nil
	}
	for _, name := range cfg.Builtin {
		if name == "defaults" {
			for _, n := range defaultSet {
				if err := add(n); err != nil {
					return nil, err
				}
			}
			continue
		}
		if err := add(name); err != nil {
			return nil, err
		}
	}
	for i, r := range cfg.Rules {
		if r.Regexp == "" {
			return nil, fmt.Errorf("scrub rule %d: regexp is required", i)
		}
		re, err := regexp.Compile(r.Regexp)
		if err != nil {
			return nil, fmt.Errorf("scrub rule %d (%s): %w", i, r.Name, err)
		}
		name := r.Name
		if name == "" {
			name = fmt.Sprintf("rule-%d", i)
		}
		repl := r.Replacement
		if repl == "" {
			repl = redacted
		}
		s.patterns = append(s.patterns, pattern{name: name, re: re, repl: repl})
	}
	if len(s.patterns) == 0 {
		return nil, fmt.Errorf("logScrubbing configured with no patterns (set builtin: [defaults] or add rules)")
	}
	return &s, nil
}

// Scrub redacts body. The unchanged fast path performs no allocation.
func (s *Scrubber) Scrub(body string) string {
	for i := range s.patterns {
		p := &s.patterns[i]
		if p.prefilter != nil && !p.prefilter(body) {
			continue
		}
		if !p.re.MatchString(body) {
			continue
		}
		body = p.re.ReplaceAllString(body, p.repl)
		obs.LogScrubbed.WithLabelValues(p.name).Inc()
	}
	return body
}
