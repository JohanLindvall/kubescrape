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

// keySuffix lets a keyword be a PREFIX of the key, but only where the suffix
// is itself a secret word: SECRET_KEY, secretKey, secretValue, TOKEN_VALUE.
//
// The first attempt allowed ANY compound suffix, which redacted everyday
// Kubernetes vocabulary — `secretName=my-tls-cert` (a Secret's NAME),
// `secretRef: registry-creds`, `token_count=42`, `passwordPolicy=strict`,
// `tokenBucket=full` — destroying ordinary log content, and destroying it
// BEFORE logAttributes, enrich, logMetrics and the rules run on the line. A
// scrubber that eats real fields is not a safer scrubber: it makes operators
// turn the defaults off.
//
// RE2 has no negative lookahead, so the safe suffixes are excluded by
// construction: only these words may follow the keyword.
var keySuffix = `(?:[_-]?(?:` +
	asciiFold("key") + `|` + asciiFold("value") + `|` + asciiFold("token") +
	`|` + asciiFold("secret") + `|` + asciiFold("password") + `|` +
	asciiFold("passwd") + `|` + asciiFold("pwd") + `))?`

// asciiFold renders a literal keyword as a regex matching exactly its ASCII
// case variants ("key" -> "[Kk][Ee][Yy]").
//
// The built-in patterns use this instead of (?i) because Go's (?i) folds via
// unicode.SimpleFold: `(?i)password` also matches `paſsword` (U+017F LATIN
// SMALL LETTER LONG S) and `(?i)token` matches `toKen` (U+212A KELVIN SIGN).
// The literal prefilters that gate those regexes fold ASCII only, so such a
// line was rejected by the prefilter, the pattern was skipped entirely, and
// the secret shipped unredacted — a security control quietly narrower than the
// regex it advertises. Constraining the regex to ASCII case makes the two
// exactly equal, which is what the prefilter optimisation requires to be safe.
func asciiFold(word string) string {
	var b strings.Builder
	for i := 0; i < len(word); i++ {
		c := word[i]
		switch {
		case c >= 'a' && c <= 'z':
			b.WriteByte('[')
			b.WriteByte(c - 'a' + 'A')
			b.WriteByte(c)
			b.WriteByte(']')
		case c >= 'A' && c <= 'Z':
			b.WriteByte('[')
			b.WriteByte(c)
			b.WriteByte(c - 'A' + 'a')
			b.WriteByte(']')
		default:
			b.WriteString(regexp.QuoteMeta(string(c)))
		}
	}
	return b.String()
}

func containsFold(sub string) func(string) bool {
	lower := strings.ToLower(sub)
	return func(s string) bool {
		// A TRUE ASCII case-insensitive scan, matching the ASCII-only case
		// classes the guarded regexes use (see asciiFold): the prefilter must
		// be neither narrower NOR wider than its regex. The original
		// three-casing (lower/UPPER/Title) check let a mixed-case keyword
		// (`bEaReR`, `pAsSwOrD`) skip redaction — a real secret-leak gap.
		// Zero-alloc.
		return asciiIndexFold(s, lower) >= 0
	}
}

// asciiIndexFold reports whether lowerSub (already lowercase) occurs in s,
// ASCII-case-insensitively, without allocating. The prefilter needle is short,
// so the naive scan is cheaper than the per-line ToLower it replaces.
func asciiIndexFold(s, lowerSub string) int {
	n := len(lowerSub)
	if n == 0 {
		return 0
	}
	for i := 0; i+n <= len(s); i++ {
		k := 0
		for ; k < n; k++ {
			c := s[i+k]
			if 'A' <= c && c <= 'Z' {
				c += 'a' - 'A'
			}
			if c != lowerSub[k] {
				break
			}
		}
		if k == n {
			return i
		}
	}
	return -1
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
		name: "secret-kv",
		// The keyword may be a SUFFIX of a compound key: `\b` treats `_` as a
		// word character, so `\baccess_token` could never match — and
		// access_token, refresh_token, client_secret, AWS_SECRET_ACCESS_KEY,
		// DB_PASSWORD and every other snake_case / SCREAMING_SNAKE / camelCase
		// spelling shipped in CLEAR. Those are the common forms; the pattern
		// was catching only the rarest ones. The prefilters already accept
		// these lines (they scan for the bare keyword anywhere), so this only
		// widens the regex to the reach its guard always had.
		//
		// The value charset also excludes closing brackets: without them an
		// unquoted JSON value swallowed the closing brace, corrupting the line
		// for logattrs, enrich and log-metrics — which all run AFTER scrubbing.
		//
		// The keyword may equally be a PREFIX of the key — SECRET_KEY,
		// secret_key, secretKey, secretValue, TOKEN_VALUE — which the
		// suffix-only form above missed entirely, shipping the whole Django /
		// AWS-SDK / camelCase-JSON family in clear (see keySuffix).
		re: regexp.MustCompile(`((?:^|[^0-9A-Za-z_.-])[0-9A-Za-z_.-]*?(?:` +
			asciiFold("api") + `[_-]?` + asciiFold("key") +
			`|` + asciiFold("secret") + `|` + asciiFold("password") + `|` + asciiFold("passwd") +
			`|` + asciiFold("pwd") + `|` + asciiFold("token") +
			`|` + asciiFold("access") + `[_-]?` + asciiFold("key") +
			`)` + keySuffix + `["\']?\s*[:=]\s*["\']?)[^\s"\'&,;}\])]+`),
		repl: "${1}" + redacted,
		prefilter: func(s string) bool {
			return pfKey(s) || pfSecret(s) || pfPassw(s) || pfPwd(s) || pfToken(s)
		},
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
		re:   regexp.MustCompile(`([a-zA-Z][a-zA-Z0-9+.-]*://[^\s:/@]*:)[^\s"'&,;}\])/@]+(@)`),
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
	// path). This is documented in CLAUDE.md and the config docs; apps should
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
