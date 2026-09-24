// Package logscrub redacts sensitive values from log bodies before export:
// a curated set of built-in patterns (tokens, credentials, keys) plus
// user-defined regexes, applied in the tailer, journald and OTLP-ingest log
// paths. Redaction happens on the agent so secrets never leave the node.
//
// Per-line cost discipline: every built-in pattern carries a cheap prefilter,
// so the no-match hot path is a scan or two and zero allocations. A prefilter
// must admit everything its regex can match (narrower means a secret ships
// unredacted) and as little else as possible — running the regex is the
// expensive part, and a pattern admitted on a bare keyword cost 100 ms on a
// 1 MiB record. Most are literal scans; secret-kv's checks the assignment
// SHAPE (see secretKVCandidate); a user rule gets a Contains gate when its
// regex proves a literal prefix (see New). A scrubbed line allocates (it must
// — the body changes).
package logscrub

import (
	"errors"
	"fmt"
	"regexp"
	"strings"

	"github.com/JohanLindvall/kubescrape/internal/obs"
)

// Config is the agent config's logScrubbing section.
type Config struct {
	// Builtin enables named built-in patterns. The special name "defaults"
	// enables the low-false-positive set (bearer, basic-auth, secret-kv,
	// aws-key, private-key, url-userinfo) — see defaultSet, which is the
	// authoritative list. "email" and "credit-card" are opt-in by name — they
	// redact legitimate content too often to be defaults.
	//
	// Keep this enumeration in step with defaultSet. It, the README and
	// CONFIGURATION.md all said "five" long after url-userinfo made it six, so
	// an operator who spelled the list out instead of writing `defaults` — the
	// natural thing to do for a reviewable compliance control — silently lost
	// DSN password redaction. TestDefaultSetIsDocumented pins it now.
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

// builtins is the catalog. Every pattern replaces the whole match unless it
// captures a prefix group to keep (the kv patterns keep the key and the
// separator so the log line stays readable).
var builtins = map[string]pattern{
	"bearer": {
		name:      "bearer",
		re:        regexp.MustCompile(`\b(` + asciiFold("bearer") + `\s+)[A-Za-z0-9\-._~+/]+=*`),
		repl:      "${1}" + redacted,
		prefilter: containsFold("bearer"),
	},
	"basic-auth": {
		name:      "basic-auth",
		re:        regexp.MustCompile(`\b(` + asciiFold("basic") + `\s+)[A-Za-z0-9+/]{8,}=*`),
		repl:      "${1}" + redacted,
		prefilter: containsFold("basic"),
	},
	"secret-kv": {
		name:      "secret-kv",
		re:        secretKVRegexp,
		repl:      "${1}${2}${3}${4}${5}" + redacted,
		prefilter: secretKVCandidate,
	},
	"url-userinfo": {
		name: "url-userinfo",
		// scheme://user:PASSWORD@host — the credential is the password half.
		// Connection strings reach logs through dial-failure messages and
		// config dumps, where no key=value shape exists for secret-kv to match.
		// The password charset excludes the same delimiters secret-kv's does
		// (quotes, comma, semicolon, closing brackets) plus '/'. Bounding it
		// by whitespace and '@' alone let it walk THROUGH a JSON value's
		// closing quote, past intervening fields, to any later '@' — so
		// `{"dsn":"redis://cache:6379","user":"admin@corp.com"}` lost its port,
		// its `user` field and its well-formedness, with no credential present
		// anywhere. Scrubbing runs BEFORE logAttributes, enrich, logMetrics
		// and the rules, so a corrupted line silently kills every field
		// extraction downstream of it — the very mistake secret-kv's value
		// charset records fixing.
		//
		// The user half is `*`, not `+`: `redis://:hunter2@host` is the
		// standard Redis/Sentinel spelling and was the one credential form
		// this pattern MISSED.
		//
		// '@' is deliberately NOT excluded from the PASSWORD class, even though
		// it terminates it. The class is greedy, so leaving '@' in runs the
		// match to the LAST '@' inside the token and a password containing one
		// — `svc:p@ss@db-1`, an everyday DSN — is removed WHOLE. Excluding it
		// stopped at the FIRST '@' and emitted `svc:[REDACTED]@ss@db-1`: a line
		// that claims a redaction and still carries most of the credential.
		// What bounds the match is the DELIMITER set above, not '@', so this
		// widens nothing that matters: every line the excluding form matched
		// still matches, over the same span or a longer one, and the only line
		// newly matched is one whose password is itself an '@'.
		// TestURLUserinfoDoesNotEatSurroundingContent is the corpus that keeps
		// the delimiters honest.
		re:   regexp.MustCompile(`([a-zA-Z][a-zA-Z0-9+.-]*://[^\s:/@]*:)[^\s"'&,;}\])/]+(@)`),
		repl: "${1}" + redacted + "${2}",
		prefilter: func(s string) bool {
			return strings.Contains(s, "://")
		},
	},
	"aws-key": {
		name:      "aws-key",
		re:        regexp.MustCompile(`\b(?:AKIA|ASIA)[0-9A-Z]{16}\b`),
		prefilter: func(s string) bool { return strings.Contains(s, "AKIA") || strings.Contains(s, "ASIA") },
		repl:      redacted,
	},
	// LIMITATION: scrubbing runs per log RECORD. For the line-at-a-time
	// producers (tailer, journald) a multi-line PEM key logged across
	// physical lines only has its "-----BEGIN … PRIVATE KEY-----" line
	// redacted; the base64 body lines are separate records that lack the
	// "PRIVATE KEY" telltale and pass through. The whole key is redacted only
	// when it arrives in ONE record (a JSON-embedded key, or the OTLP-ingest
	// path). This is documented in AGENTS.md and the config docs; apps should
	// not log raw private keys.
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
var defaultSet = []string{"bearer", "basic-auth", "secret-kv", "aws-key", "private-key", "url-userinfo"}

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
		p := pattern{name: name, re: re, repl: repl}
		// User rules get the same never-narrower gate discipline as the
		// built-ins wherever the engine can prove one: LiteralPrefix is a
		// literal every match must BEGIN with (complete or not), so a line not
		// containing it cannot match anywhere and a Contains scan is a sound
		// superset gate — here guaranteed by the regexp package's contract
		// rather than by hand-matched construction. A rule whose pattern
		// yields no literal (a case fold, a leading class or alternation) runs
		// ungated, paying the full regex per record as before.
		if lit, _ := re.LiteralPrefix(); lit != "" {
			p.prefilter = func(s string) bool { return strings.Contains(s, lit) }
		}
		s.patterns = append(s.patterns, p)
	}
	if len(s.patterns) == 0 {
		return nil, errors.New("logScrubbing configured with no patterns (set builtin: [defaults] or add rules)")
	}
	return &s, nil
}

// Scrub redacts body. The unchanged fast path performs no allocation. Each
// pattern that redacted something counts once into obs.LogScrubbed — a
// per-RECORD tally, so a producer that can hand the SAME record here twice
// must use ScrubUncounted for the repeat.
func (s *Scrubber) Scrub(body string) string {
	return s.scrub(body, true)
}

// ScrubUncounted is Scrub for a record an earlier pass already scrubbed and
// counted: the same redaction, with obs.LogScrubbed left alone. It is
// logenrich.ApplyUncounted's sibling and exists for the same reason — a
// producer that rebuilds a failed batch from source (the tailer re-reads a
// rewound file) would otherwise count one redaction once per ATTEMPT, 6 for 3
// delivered records across a single rewind.
func (s *Scrubber) ScrubUncounted(body string) string {
	return s.scrub(body, false)
}

func (s *Scrubber) scrub(body string, count bool) string {
	for i := range s.patterns {
		p := &s.patterns[i]
		if p.prefilter != nil && !p.prefilter(body) {
			continue
		}
		if !p.re.MatchString(body) {
			continue
		}
		body = p.re.ReplaceAllString(body, p.repl)
		if count {
			obs.LogScrubbed.WithLabelValues(p.name).Inc()
		}
	}
	return body
}
