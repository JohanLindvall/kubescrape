package logscrub

// The secret-kv built-in in one place: its vocabulary, the regex rendered from
// it, and the prefilter gate that must admit everything that regex can match.
// The two halves of that pair live side by side on purpose — a keyword, a
// suffix or a value branch changed in one and not the other is either a secret
// shipped unredacted (a gate narrower than its regex) or a full regex pass per
// line for nothing (a gate wider than it).

import (
	"regexp"
	"strings"
)

// The secret-kv VOCABULARY — one table, four consumers: the regex alternation
// (kvKeywordAlt), keySuffix's alternation (kvSuffixAlt), the prefilter's
// first-byte dispatch (kvDispatch, plus kvTail's suffix probe) and the
// coverage test's cross product all render from these two lists. Keeping them
// in one place IS the security property: a keyword present in the regex but
// absent from the dispatch makes the prefilter narrower than its pattern —
// the secret ships unredacted — while one present in the dispatch but not the
// regex admits lines that cost a full regex pass for nothing.
//
// A keyword is a sequence of parts joined by an OPTIONAL underscore or dash
// (so {"api", "key"} spells apikey, api_key and api-key); kvExpand enumerates
// exactly that language for the literal prefilter.
var (
	kvKeywords = [][]string{
		{"api", "key"},
		{"secret"},
		{"password"},
		{"passwd"},
		{"pwd"},
		{"token"},
		{"access", "key"},
	}
	// kvSuffixes is keySuffix's closed alternation (see keySuffix for why the
	// set is curated rather than open).
	kvSuffixes = []string{"key", "value", "token", "secret", "password", "passwd", "pwd"}
)

// kvKeywordAlt renders the keyword table as the regex alternation body: each
// part ASCII-case-folded (see asciiFold), parts joined by `[_-]?`.
func kvKeywordAlt() string {
	var b strings.Builder
	for i, parts := range kvKeywords {
		if i > 0 {
			b.WriteByte('|')
		}
		for j, p := range parts {
			if j > 0 {
				b.WriteString(`[_-]?`)
			}
			b.WriteString(asciiFold(p))
		}
	}
	return b.String()
}

// kvSuffixAlt renders the suffix table the same way (single-part words).
func kvSuffixAlt() string {
	folded := make([]string, len(kvSuffixes))
	for i, w := range kvSuffixes {
		folded[i] = asciiFold(w)
	}
	return strings.Join(folded, `|`)
}

// kvExpand enumerates a keyword's literal spellings — the exact language of
// its `part[_-]?part` regex form, every choice of "", "_" or "-" at every
// junction. Init- and test-time only; the hot path reads the precomputed
// kvDispatch.
func kvExpand(parts []string) []string {
	out := []string{parts[0]}
	for _, p := range parts[1:] {
		next := make([]string, 0, 3*len(out))
		for _, head := range out {
			for _, sep := range []string{"", "_", "-"} {
				next = append(next, head+sep+p)
			}
		}
		out = next
	}
	return out
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
// construction: only the kvSuffixes words may follow the keyword.
var keySuffix = `(?:[_-]?(?:` + kvSuffixAlt() + `))?`

// secretKVRegexp is the secret-kv built-in's pattern.
//
// The keyword may be a SUFFIX of a compound key: `\b` treats `_` as a
// word character, so `\baccess_token` could never match — and
// access_token, refresh_token, client_secret, AWS_SECRET_ACCESS_KEY,
// DB_PASSWORD and every other snake_case / SCREAMING_SNAKE / camelCase
// spelling shipped in CLEAR. Those are the common forms; the pattern
// was catching only the rarest ones. The prefilter admits them too:
// secretKVCandidate matches a keyword anywhere in the key — including
// as the suffix of a compound one — followed by the assignment, so
// this widens the regex to the reach its guard has.
//
// The value charset also excludes closing brackets: without them an
// unquoted JSON value swallowed the closing brace, corrupting the line
// for logattrs, enrich and log-metrics — which all run AFTER scrubbing.
//
// The keyword may equally be a PREFIX of the key — SECRET_KEY,
// secret_key, secretKey, secretValue, TOKEN_VALUE — which the
// suffix-only form above missed entirely, shipping the whole Django /
// AWS-SDK / camelCase-JSON family in clear (see keySuffix).
// A QUOTED value ends at the closing quote of the SAME kind, not at the
// first delimiter inside it — a passphrase may contain spaces, commas,
// semicolons, ampersands, closing brackets AND the other quote kind
// (`password="don't tell"`, `secret='he said "go"'`), and any class
// that stops earlier ships the value's tail in clear, through the
// tailer, journald and ingest alike. RE2 has no backreference, so "the
// same quote that opened it" is spelled as one ordered branch per quote
// kind, each capturing its opening quote (exactly one group is set) and
// excluding only its OWN kind — a class excluding BOTH kinds would stop
// a double-quoted value at an embedded apostrophe. The closing quote is
// left in the line, so the replacement re-emits `key="` + redaction and
// the original terminator survives. The unquoted branch keeps the
// delimiter class. Every branch requires at least one value byte, so
// `password=""` matches nothing.
//
// A `\"` INSIDE a plainly-quoted value is ESCAPED CONTENT, not that
// value's closing quote, and any JSON encoder writes one for a
// passphrase containing a quote. A bare `[^"]+` stopped there, so
// `{"password":"he said \"hi\" ok"}` came out as
// `{"password":"[REDACTED]"hi\" ok"}` — the same
// report-success-while-failing output the escaped-value branches below
// exist to prevent, one level in, and the one failure no counter can
// tell apart from a clean redaction. So the plain branches take an
// escape and its escapee as ONE unit (`\\[\s\S]` — any byte, newline
// included, since `[^"]` already spanned lines and a multi-line record
// must not start truncating) and stop only at a BARE quote, which is
// exactly where a JSON string ends. The cost is a literal UNPAIRED
// backslash immediately before the closing quote (malformed JSON): it
// reads as an escape and the value runs on to the next quote —
// over-redaction, the safe direction, pinned by
// TestKnownOverRedactionsAreAccepted.
//
// The quotes may be ESCAPED, and that is the ORDINARY shape, not an
// exotic one: any logging library that stringifies a payload into a
// message field emits `{"msg":"password=\"hunter2\" tail"}`. Without
// its own branches the value alternation fell through to the unquoted
// class, which matched the lone `\` and stopped at the quote — output
// `password=[REDACTED]"hunter2\" tail`, which is WORSE than no match:
// the line reads redacted and carries the secret anyway, so a reviewer
// sampling the output stops there. The key's closing quote may be
// escaped too (`{\"password\":\"…`, JSON inside JSON), which is the
// `(?:\\?["'])?` on the separator side.
//
// ONE level of escaping is what these branches decode: inside an
// escaped value a `\"` is the encoded closing quote and TERMINATES,
// while a `\` before anything else is content — exactly
// `(?:[^"\\]|\\[^"])+`. (`\\.` instead would eat the closing `\"` and
// run on to the next quote, redacting the rest of the message.) That
// is the OPPOSITE of the plain-quoted branches' rule above — there
// `\"` is content and only a BARE quote terminates — and deliberately
// so: the two contexts spell their terminator differently, and which
// rule applies is decided by the OPENING quote, which is why they are
// separate branches and why the escaped pair is tried FIRST. The
// unquoted branch drops a `\` that PRECEDES a quote for the same
// reason — that backslash is a value terminator, and matching it alone
// is the false redaction above — while keeping `\` before anything
// else, or `password=C:\Users\svc` would redact `C:` and ship the path.
//
// The keyword alternation and keySuffix render from the
// kvKeywords/kvSuffixes tables — the same tables the prefilter's
// dispatch is built from, which is what keeps regex and prefilter in
// lockstep (see kvDispatch).
var secretKVRegexp = regexp.MustCompile(`((?:^|[^0-9A-Za-z_.-])[0-9A-Za-z_.-]*?(?:` +
	kvKeywordAlt() +
	`)` + keySuffix + `(?:\\?["\'])?\s*[:=]\s*)` +
	`(?:(\\")(?:[^"\\]|\\[^"])+|(\\')(?:[^'\\]|\\[^'])+|(")(?:[^"\\]|\\[\s\S])+|(')(?:[^'\\]|\\[\s\S])+|(?:[^\s"\'&,;}\])\\]|\\[^"\'])+)`)

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
// It is one pass with a first-byte dispatch rather than a scan per keyword: an
// ordinary line pays one lowercase and one table load per byte.
//
// kvDispatch groups every keyword spelling (kvExpand over kvKeywords) by its
// lowercased first byte, built once at init — a keyword added to the table
// reaches the dispatch with no second list to update, and nothing per line
// allocates.
var kvDispatch = func() (d [256][]string) {
	for _, parts := range kvKeywords {
		for _, w := range kvExpand(parts) {
			c := lowerASCII(w[0])
			d[c] = append(d[c], w)
		}
	}
	return
}()

// kvStart is the per-byte GATE — true for the RAW byte, both cases, exactly
// where kvDispatch holds a spelling. It is derived from kvDispatch, so the
// vocabulary still has one home and a keyword added to the table reaches the
// gate with nothing to update.
//
// It is a separate table rather than a widening of kvDispatch for two reasons.
// SIZE: kvDispatch is 256 slice headers — 6 KiB, ninety-six cache lines — and
// the reject path, which is every byte of every line that does not start a
// keyword and therefore almost all of them, was loading one of those headers
// just to test its length; this is 256 bytes, four cache lines, resident
// beside the line being scanned. And SHAPE: kvDispatch's lowercase-only
// grouping is pinned by TestSecretKVDispatchDerivation, which is a security
// pin (a spelling that fell out of the dispatch makes the prefilter narrower
// than its regex), so it is left exactly as that test describes it and the
// fold moves off the per-byte path onto the hit path instead.
var kvStart = func() (g [256]bool) {
	for c := range kvDispatch {
		if len(kvDispatch[c]) > 0 {
			g[c] = true
			g[upperASCII(byte(c))] = true
		}
	}
	return
}()

// secretKVCandidate reports whether the line can match the secret-kv regex.
func secretKVCandidate(s string) bool {
	for i := 0; i < len(s); i++ {
		if !kvStart[s[i]] {
			continue
		}
		for _, w := range kvDispatch[lowerASCII(s[i])] {
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
	for _, w := range kvSuffixes {
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
	// The key's own closing quote may be ESCAPED — `{\"password\":\"x\"}`, JSON
	// inside a JSON message field — which is the regex's `(?:\\?["'])?`.
	// Skipping only a bare quote left the prefilter NARROWER than its pattern
	// on exactly the lines the escaped value branches were added for, i.e. the
	// pattern skipped and the secret shipped in clear.
	if j+1 < len(s) && s[j] == '\\' && (s[j+1] == '"' || s[j+1] == '\'') {
		j += 2
	} else if j < len(s) && (s[j] == '"' || s[j] == '\'') {
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
	// The value branches have DIFFERENT terminator sets, and the prefilter has
	// to admit every one or it is narrower than the regex — which means a
	// secret ships unredacted, the one failure this whole prefilter design
	// must never have. A quoted value runs to the closing quote of its OWN
	// kind (`"[^"]+` / `'[^']+`), so the only byte that cannot start it is
	// that same quote — the OTHER kind is a legal first value byte
	// (`password="'…`), and whitespace, commas and brackets are all legal
	// INSIDE the quotes. An unquoted value still stops at the delimiter class.
	// A backslash opens BOTH the escape-quoted branches (`password=\"x\"`) and
	// an ordinary unquoted value (`C:\Users`), and it is not in the delimiter
	// class, so the fall-through below admits it — deliberately wider than the
	// regex here, which also requires a value byte after the escaped quote.
	if j < len(s) && (s[j] == '"' || s[j] == '\'') {
		q := s[j]
		j++
		return j < len(s) && s[j] != q
	}
	return j < len(s) && !isValueDelim(s[j])
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
