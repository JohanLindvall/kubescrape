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
// SHAPE (see secretKVCandidate). A scrubbed line allocates (it must — the body
// changes).
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

// secret-kv's prefilter.
//
// The first version scanned for the bare words "key", "secret", "passw", "pwd"
// and "token" anywhere on the line. Those admit far more than the regex can
// match — a bare `key` or `token` with no credential-ish continuation can never
// match the alternation at all — and the regex is the expensive part: a 1 MiB
// record containing the word "token" cost 100 ms of the SINGLE sweep goroutine
// that serves every log file on the node. Admitting a line the pattern cannot
// match is not a small waste; it is the whole cost.
//
// So the prefilter checks the SHAPE, not a word: a keyword, optionally one of
// the curated suffixes, then the `["']?\s*[:=]` assignment and a value byte.
// That is the regex's own tail, minus the leading `[0-9A-Za-z_.-]*?` walk-back,
// which can never fail (walking back from a keyword over word characters always
// reaches either the line start or a non-word character). Being a strict
// SUPERSET of the regex is what makes it safe to skip on a miss — a prefilter
// narrower than its pattern ships secrets unredacted, which
// TestPrefilterIsNotNarrowerThanItsRegex exists to catch.
//
// It is one pass with a first-byte dispatch rather than a scan per keyword: the
// keywords start with a, s, p or t, so an ordinary line pays one lowercase and
// one switch per byte.
var (
	kvWordsA = []string{"apikey", "api_key", "api-key", "accesskey", "access_key", "access-key"}
	kvWordsS = []string{"secret"}
	kvWordsP = []string{"password", "passwd", "pwd"}
	kvWordsT = []string{"token"}
	// kvSuffixWords mirrors keySuffix's alternation, in the same order.
	kvSuffixWords = []string{"key", "value", "token", "secret", "password", "passwd", "pwd"}
)

// secretKVCandidate reports whether the line can match the secret-kv regex.
func secretKVCandidate(s string) bool {
	for i := 0; i < len(s); i++ {
		var words []string
		switch lowerASCII(s[i]) {
		case 'a':
			words = kvWordsA
		case 's':
			words = kvWordsS
		case 'p':
			words = kvWordsP
		case 't':
			words = kvWordsT
		default:
			continue
		}
		for _, w := range words {
			if hasPrefixFold(s[i:], w) && kvTail(s, i+len(w)) {
				return true
			}
		}
	}
	return false
}

// kvTail reports whether the keyword ending at j is followed by an assignment,
// with or without one of the curated suffixes in between (keySuffix).
func kvTail(s string, j int) bool {
	if kvAssign(s, j) {
		return true
	}
	k := j
	if k < len(s) && (s[k] == '_' || s[k] == '-') {
		k++
	}
	for _, w := range kvSuffixWords {
		if hasPrefixFold(s[k:], w) && kvAssign(s, k+len(w)) {
			return true
		}
	}
	return false
}

// kvAssign matches the regex's `["\']?\s*[:=]\s*["\']?` plus the first byte of
// its value class — a keyword with no value after the separator is not a
// credential and must not cost a regex pass.
func kvAssign(s string, j int) bool {
	if j < len(s) && (s[j] == '"' || s[j] == '\'') {
		j++
	}
	for j < len(s) && isRegexpSpace(s[j]) {
		j++
	}
	if j >= len(s) || (s[j] != ':' && s[j] != '=') {
		return false
	}
	j++
	for j < len(s) && isRegexpSpace(s[j]) {
		j++
	}
	// The two value branches have DIFFERENT terminator sets, and the prefilter
	// has to admit both or it is narrower than the regex — which means a secret
	// ships unredacted, the one failure this whole prefilter design must never
	// have. A quoted value runs to its closing quote (`["\'][^"\']+`), so the
	// only byte that cannot start it is another quote; whitespace, commas and
	// brackets are all legal INSIDE the quotes. An unquoted value still stops
	// at the delimiter class.
	if j < len(s) && (s[j] == '"' || s[j] == '\'') {
		j++
		return j < len(s) && s[j] != '"' && s[j] != '\''
	}
	return j < len(s) && !isValueDelim(s[j])
}

func lowerASCII(c byte) byte {
	if 'A' <= c && c <= 'Z' {
		c += 'a' - 'A'
	}
	return c
}

// hasPrefixFold reports whether s starts with the (already lowercase) word,
// ASCII-case-insensitively — the same folding asciiFold gives the regexes.
func hasPrefixFold(s, lowerWord string) bool {
	if len(s) < len(lowerWord) {
		return false
	}
	for i := 0; i < len(lowerWord); i++ {
		if lowerASCII(s[i]) != lowerWord[i] {
			return false
		}
	}
	return true
}

// isRegexpSpace is Go regexp's \s class, EXACTLY: [\t\n\f\r ], with no \v.
// Both halves of that matter. A wider class would skip a byte the regex stops
// at (harmless over-admission), but it is also what isValueDelim negates — and
// there a wider class REJECTS a line whose value begins with \v, which the
// regex matches, making the prefilter narrower than its pattern and the secret
// unredacted.
func isRegexpSpace(c byte) bool {
	return c == ' ' || c == '\t' || c == '\n' || c == '\f' || c == '\r'
}

// isValueDelim is the secret-kv value class' exclusion set, `[\s"'&,;}\])]`.
func isValueDelim(c byte) bool {
	switch c {
	case '"', '\'', '&', ',', ';', '}', ']', ')':
		return true
	}
	return isRegexpSpace(c)
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
		// A QUOTED value ends at its closing quote, not at the first delimiter
		// inside it. The value class below is the UNQUOTED terminator set, and
		// applying it to a quoted value redacted only the first fragment:
		//
		//	password="hunter2 with spaces"  ->  password="[REDACTED] with spaces"
		//	{"password":"p@ss w0rd"}        ->  {"password":"[REDACTED] w0rd"}
		//
		// i.e. every passphrase containing a space, comma, semicolon, ampersand
		// or closing bracket shipped its tail in clear, through the tailer,
		// journald and ingest alike. RE2 has no backreference, so "the same
		// quote that opened it" is spelled as an ordered alternation whose
		// first branch captures the opening quote in group 2 and stops before
		// ANY quote; the closing quote is left in the line, so the replacement
		// re-emits `key="` + redaction and the original terminator survives.
		// The unquoted branch is unchanged. Both branches still require at
		// least one value byte, so `password=""` matches neither, as before.
		re: regexp.MustCompile(`((?:^|[^0-9A-Za-z_.-])[0-9A-Za-z_.-]*?(?:` +
			asciiFold("api") + `[_-]?` + asciiFold("key") +
			`|` + asciiFold("secret") + `|` + asciiFold("password") + `|` + asciiFold("passwd") +
			`|` + asciiFold("pwd") + `|` + asciiFold("token") +
			`|` + asciiFold("access") + `[_-]?` + asciiFold("key") +
			`)` + keySuffix + `["\']?\s*[:=]\s*)(?:(["\'])[^"\']+|[^\s"\'&,;}\])]+)`),
		repl:      "${1}${2}" + redacted,
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
