package logscrub

import (
	"fmt"
	"strconv"
	"strings"
	"testing"
)

// A JSON-escaped credential is the ORDINARY shape: any logging library that
// stringifies a payload into a message field writes `password=\"hunter2\"`,
// and the same happens one level up when the message is itself JSON
// (`{\"password\":\"hunter2\"}`). The value alternation had no branch starting
// at a backslash, so it fell through to the unquoted class, matched the lone
// `\` and stopped at the quote:
//
//	{"msg":"password=\"hunter2\" tail"}  ->  {"msg":"password=[REDACTED]"hunter2\" tail"}
//
// That is WORSE than not matching at all. The line says the credential was
// redacted and still carries it, so an operator reviewing a sample — the way a
// "reviewable compliance control" is actually reviewed — stops at the
// [REDACTED] and never sees the secret three bytes later.
func TestSecretKVRedactsEscapeQuotedValues(t *testing.T) {
	s := mustNew(t, Config{Builtin: []string{"defaults"}})
	cases := []struct{ in, want string }{
		// The reported shapes.
		{`{"msg":"password=\"hunter2\" tail"}`, `{"msg":"password=\"[REDACTED]\" tail"}`},
		{`{"msg":"token=\"abc123\" tail"}`, `{"msg":"token=\"[REDACTED]\" tail"}`},
		// The escaped closing quote is the value's terminator and stays on the
		// line, exactly as the plain-quote branches leave theirs — so the
		// record is still well-formed for logattrs/enrich/log-metrics, which
		// all run AFTER scrubbing.
		{`{"msg":"user=bob password=\"hunter2\" ok=true"}`, `{"msg":"user=bob password=\"[REDACTED]\" ok=true"}`},
		{`{"msg":"api_key=\"sk-12345\", next"}`, `{"msg":"api_key=\"[REDACTED]\", next"}`},
		// The whole value goes, delimiters inside the quotes included — the
		// same property the plain-quote branches carry.
		{`{"msg":"password=\"a b,c;d]e\" done"}`, `{"msg":"password=\"[REDACTED]\" done"}`},
		// JSON inside JSON: the KEY's closing quote is escaped too.
		{`{\"password\":\"hunter2\"}`, `{\"password\":\"[REDACTED]\"}`},
		{`{\"access_token\":\"ya29.SECRET\"}`, `{\"access_token\":\"[REDACTED]\"}`},
		// Single quotes escaped the same way (shell/SQL-ish embedding).
		{`password=\'hunter2\'`, `password=\'[REDACTED]\'`},
		// An unterminated escaped value (a cut log line) redacts to the end
		// rather than not at all.
		{`{"msg":"password=\"hunter2`, `{"msg":"password=\"[REDACTED]`},
		// An EMPTY escaped value is not a credential: it must match nothing,
		// like `password=""` does. The old unquoted fall-through "redacted"
		// the lone backslash here too.
		{`password=\"\"`, `password=\"\"`},
		{`password=\"`, `password=\"`},
		// A backslash that is NOT an escaped quote is ordinary value content
		// and must still be redacted WHOLE — narrowing the unquoted class to
		// exclude every backslash would ship the tail of this one.
		{`password=C:\Users\svc`, `password=[REDACTED]`},
		// The unescaped forms are untouched by all of it.
		{`password="plainquoted" tail`, `password="[REDACTED]" tail`},
		{`{"password":"hunter2"}`, `{"password":"[REDACTED]"}`},
		{`password=hunter2`, `password=[REDACTED]`},
		{`password=""`, `password=""`},
	}
	for _, c := range cases {
		if got := s.Scrub(c.in); got != c.want {
			t.Errorf("Scrub(%q)\n got %q\nwant %q", c.in, got, c.want)
		}
	}
}

// The MIRROR of the case above, one level in, and the one the escaped-value
// branches did NOT cover: a `\"` inside a PLAINLY-quoted value is escaped
// CONTENT, not that value's closing quote. `[^"]+` stopped at it and produced
//
//	{"password":"he said \"hi\" ok"}  ->  {"password":"[REDACTED]"hi\" ok"}
//
// — the identical report-success-while-failing output, on a line any JSON
// encoder writes for a passphrase containing a quote. The plain branches now
// take an escape and its escapee as one unit and stop only at a BARE quote.
func TestSecretKVRedactsQuotedValuesContainingEscapedQuotes(t *testing.T) {
	s := mustNew(t, Config{Builtin: []string{"defaults"}})
	cases := []struct{ in, want string }{
		{`{"password":"he said \"hi\" ok"}`, `{"password":"[REDACTED]"}`},
		{`{"password":"a\"b"}`, `{"password":"[REDACTED]"}`},
		{`{"api_key":"a\"b","user":"bob"}`, `{"api_key":"[REDACTED]","user":"bob"}`},
		// Symmetrically for single quotes (Python/JS repr of a passphrase
		// containing an apostrophe).
		{`secret='don\'t tell'`, `secret='[REDACTED]'`},
		// An escaped BACKSLASH is content too, and must not be read as the
		// escape of the closing quote — `\\` then `"` ends the value there.
		{`{"password":"C:\\path\\to"}`, `{"password":"[REDACTED]"}`},
		{`{"password":"a\\","user":"bob"}`, `{"password":"[REDACTED]","user":"bob"}`},
		// A backslash before an ordinary character stays ordinary content: the
		// unit rule must not narrow the value the way excluding every
		// backslash would.
		{`password="C:\Users\svc"`, `password="[REDACTED]"`},
	}
	for _, c := range cases {
		if got := s.Scrub(c.in); got != c.want {
			t.Errorf("Scrub(%q)\n got %q\nwant %q", c.in, got, c.want)
		}
	}
}

// quoteCarrier is a shape that DELIMITS a value — one in which the scrubber can
// see where the credential ends, and therefore one where removing all of it is
// a guarantee rather than a guess.
type quoteCarrier struct {
	name string
	// tmpl takes the credential verbatim via one %s.
	tmpl string
	// quote is the byte this carrier's terminator is spelled with. A value
	// containing that terminator is not a value this carrier can EXPRESS, so
	// such a pair is skipped: it would test an unrepresentable input, not the
	// scrubber. escaped carriers are closed by `\`+quote, inside which no form
	// of the quote can appear at all; plain ones are closed by a BARE quote,
	// so an escaped one is legal content.
	quote   byte
	escaped bool
}

// hasBareQuote reports whether v contains q outside an escape — the terminator
// of a plain carrier.
func hasBareQuote(v string, q byte) bool {
	for i := 0; i < len(v); i++ {
		if v[i] == '\\' {
			i++ // the escape takes the next byte with it
			continue
		}
		if v[i] == q {
			return true
		}
	}
	return false
}

// The scrubber's CORE guarantee, checked as a cross product rather than as a
// list of remembered lines: inside a shape that delimits its value, the WHOLE
// value is replaced and nothing around it moves. The assertion is the exact
// output — a partial redaction, an over-run past the terminator and a dropped
// opening quote each fail it, which a `!strings.Contains(out, secret)` check
// would not (it passes happily while half the value is still on the line).
//
// The credential axis deliberately carries every byte class that has broken
// this before: the value-delimiter set, the OTHER quote kind, an ESCAPED quote
// of the carrier's own kind, backslashes that are not escapes, and \v (not in
// Go regexp's \s). Nothing here is curated around a hole — the shapes that
// genuinely have one are enumerated in TestKnownPartialRedactions, and they
// are the shapes that do NOT delimit their value.
func TestQuotedCredentialsAreRemovedWhole(t *testing.T) {
	s := mustNew(t, Config{Builtin: []string{"defaults"}})
	carriers := []quoteCarrier{
		{name: "double-quoted", tmpl: `password="%s"`, quote: '"'},
		{name: "single-quoted", tmpl: `secret='%s'`, quote: '\''},
		{name: "json-field", tmpl: `{"api_key":"%s","user":"bob"}`, quote: '"'},
		{name: "escape-quoted", tmpl: `{"msg":"password=\"%s\" tail"}`, quote: '"', escaped: true},
		{name: "json-in-json", tmpl: `{\"access_token\":\"%s\"}`, quote: '"', escaped: true},
		{name: "escape-quoted-single", tmpl: `token=\'%s\'`, quote: '\'', escaped: true},
	}
	values := []string{
		"hunter2",
		"hunter2 with spaces",
		`p@ss,w0rd;x&y]z}q)`,  // every byte of the unquoted value class' exclusion set
		`don't tell`,          // the OTHER quote kind, bare
		`he said "go away"`,   // ...twice
		`he said \"go away\"`, // an ESCAPED quote of the carrier's own kind
		`C:\Users\svc`,        // backslashes that are not escapes
		"\vraw",               // \v is not in Go regexp's \s
	}
	checked := map[string]int{}
	for _, c := range carriers {
		for _, v := range values {
			if c.escaped && strings.IndexByte(v, c.quote) >= 0 {
				continue
			}
			if !c.escaped && hasBareQuote(v, c.quote) {
				continue
			}
			checked[c.name]++
			in := fmt.Sprintf(c.tmpl, v)
			want := fmt.Sprintf(c.tmpl, redacted)
			if got := s.Scrub(in); got != want {
				t.Errorf("%s carrier did not remove the whole value:\n  in:   %s\n  got:  %s\n  want: %s", c.name, in, got, want)
			}
		}
	}
	// A representability rule that quietly emptied an axis would pass
	// vacuously — the exact way the corpus-curation this test replaced hid its
	// holes.
	for _, c := range carriers {
		if checked[c.name] < 4 {
			t.Errorf("carrier %q only checked %d values; the skip rule has eaten the axis", c.name, checked[c.name])
		}
	}
}

// The invariant behind the cases above, over the shapes whose credential the
// scrubber can DELIMIT — a quoted value of any of the forms above, an unquoted
// value with no delimiter inside it, a Bearer token, an AWS key id, a DSN
// password: a line the scrubber REWROTE must not still contain the credential.
// Redacting nothing is a known limitation an operator can measure
// (obs.LogScrubbed stays flat, the secret is visibly there); redacting a
// fragment and leaving the rest is a control that reports success while
// failing, and no counter can tell the two apart.
//
// SCOPE, stated because the previous version of this test did not: it is NOT
// true of every line containing a credential, and asserting that while quietly
// leaving the failing shapes out of the corpus was the same
// report-success-while-failing this package exists to prevent. The shapes
// where the scrubber CANNOT see where the value ends — an unquoted value
// containing one of its own delimiters, and more than one level of escaping —
// are enumerated, explained and pinned by TestKnownPartialRedactions. A case
// added here that belongs there is the overclaim; a case there that starts
// passing here belongs here.
func TestRedactionNeverClaimsMoreThanItRemoved(t *testing.T) {
	s := mustNew(t, Config{Builtin: []string{"defaults"}})
	cases := []struct{ line, secret string }{
		{`{"msg":"password=\"hunter2\" tail"}`, "hunter2"},
		{`{"msg":"token=\"abc123\" tail"}`, "abc123"},
		{`{\"password\":\"hunter2\"}`, "hunter2"},
		{`password=\'hunter2\'`, "hunter2"},
		{`password="hunter2 with spaces"`, "hunter2"},
		{`password="don't tell"`, "don't"},
		{`secret='he said "go away"'`, "go away"},
		{`{"password":"p@ss w0rd"}`, "p@ss"},
		// The escaped quote INSIDE a plainly-quoted value: the shape this test
		// used to be silent about, and the one it now covers because the regex
		// was fixed rather than the corpus trimmed.
		{`{"password":"he said \"hi\" ok"}`, "hi"},
		{`secret='don\'t tell'`, "t tell"},
		{`password=hunter2`, "hunter2"},
		{`api_key=sk-12345`, "sk-12345"},
		{`Authorization: Bearer eyJhbGciOiJIUzI1NiJ9.payload.sig`, "eyJhbGciOiJIUzI1NiJ9"},
		{`dial failed: postgres://svc:s3cr3t@db-1:5432/app`, "s3cr3t"},
		// A DSN password containing '@' — the other shape that used to be left
		// out of this corpus because it failed.
		{`dial failed: postgres://svc:p@ss@db-1:5432/app`, "ss"},
		{`leaked AKIAIOSFODNN7EXAMPLE in env`, "AKIAIOSFODNN7EXAMPLE"},
	}
	for _, c := range cases {
		got := s.Scrub(c.line)
		if got == c.line {
			t.Errorf("nothing redacted at all in %q (that is a separate bug, but it is not this test's failure mode)", c.line)
			continue
		}
		if strings.Contains(got, c.secret) {
			t.Errorf("the line was rewritten to claim a redaction and still carries the secret:\n  in:  %s\n  out: %s\n  secret still present: %s", c.line, got, c.secret)
		}
	}
}

// The residual holes in the invariant above, named and pinned rather than left
// out of a corpus. Both are shapes where the scrubber cannot see where the
// value ENDS, so redacting to the end would corrupt the record instead — and
// scrubbing runs BEFORE logAttributes, enrich, logMetrics and the rules, so a
// corrupted line kills every field extraction downstream of it.
//
// These outputs are pinned EXACTLY, in both directions. If a change makes one
// of them redact the whole credential, this test fails and the case belongs in
// TestRedactionNeverClaimsMoreThanItRemoved. If a change makes one of them
// redact LESS, or opens a third hole, this test fails too. That is the whole
// point: the decision gets re-made here, deliberately, instead of a case
// quietly disappearing from a corpus.
func TestKnownPartialRedactions(t *testing.T) {
	s := mustNew(t, Config{Builtin: []string{"defaults"}})

	// HOLE 1 — an UNQUOTED value containing one of the value class' own
	// delimiters. `password=a&b` is a query string with two parameters and it
	// is a password containing an ampersand; nothing on the line distinguishes
	// them. The delimiter class is what keeps `{"password":nospaces}` from
	// swallowing the closing brace (TestScrubKeepsJSONWellFormed), so widening
	// it trades a bounded leak for a corrupted record on every JSON line. The
	// fix available to an operator is quoting the value, which every structured
	// logger already does.
	for _, c := range []struct{ in, want string }{
		{`password=a&b`, `password=[REDACTED]&b`},
		{`password=p@ss,w0rd`, `password=[REDACTED],w0rd`},
		{`password=a;b`, `password=[REDACTED];b`},
		{`password=a}b`, `password=[REDACTED]}b`},
	} {
		if got := s.Scrub(c.in); got != c.want {
			t.Errorf("unquoted-delimiter hole changed shape:\n  in:   %s\n  got:  %s\n  want: %s\n"+
				"If the whole value now goes, move this case into TestRedactionNeverClaimsMoreThanItRemoved.", c.in, got, c.want)
		}
	}

	// HOLE 2 — more than ONE level of escaping. The branches decode exactly one
	// (`\"` terminates an escaped value, `\\` is content); at two levels the
	// terminator is `\\\"` and `\"` is content, which is the OPPOSITE rule, and
	// no regular expression can hold both without committing to a depth. Two
	// sub-shapes, and only the second is a false claim:
	for _, c := range []struct{ in, want string }{
		// The doubly-escaped KEY matches nothing at all — the visible kind of
		// miss, which obs.LogScrubbed reports by staying flat.
		{`{"msg":"{\\\"password\\\":\\\"hunter2\\\"}"}`, `{"msg":"{\\\"password\\\":\\\"hunter2\\\"}"}`},
		// The doubly-escaped VALUE stops at the inner `\"` and leaves the rest:
		// a claim with a leak, accepted only because the alternative is
		// unrepresentable. An operator whose logs are double-stringified should
		// add a user rule.
		{`password=\"he said \\\"hi\\\" ok\"`, `password=\"[REDACTED]\"hi\\\" ok\"`},
	} {
		if got := s.Scrub(c.in); got != c.want {
			t.Errorf("double-escaping hole changed shape:\n  in:   %s\n  got:  %s\n  want: %s\n"+
				"If the whole value now goes, move this case into TestRedactionNeverClaimsMoreThanItRemoved.", c.in, got, c.want)
		}
	}
}

// The accepted OVER-redactions, pinned the way
// TestSecretValueSuffixOverReachesOnSDKCallNames pins its own: over-redaction
// is far safer than under-redaction, but it is not free — scrubbing runs before
// every downstream extraction — so each class is visible here rather than
// discovered in production.
//
// CLASS 1, from `(?:\\?["'])?` on the separator: prose that NAMES a key by
// quoting it and follows with `:` or `=` is byte-for-byte an assignment, and
// RE2 has no way to tell them apart. The bare-quote spelling always
// over-redacted this way; allowing the ESCAPED spelling (which the JSON-in-JSON
// key `{\"password\":\"…` requires) extends the same class to messages that
// were stringified into a JSON field. The damage is bounded to one token — the
// unquoted value class stops at the first delimiter — so the record stays
// parseable.
//
// CLASS 2, from `\\[\s\S]` in the plain-quoted value branches: a literal
// UNPAIRED backslash immediately before a closing quote (malformed JSON) reads
// as an escape, so the value runs on to the next quote.
func TestKnownOverRedactionsAreAccepted(t *testing.T) {
	s := mustNew(t, Config{Builtin: []string{"defaults"}})
	for _, c := range []struct{ in, want string }{
		// CLASS 1, escaped spelling — the one the separator widening added.
		{`{"msg":"unknown field \"token\": ignoring"}`, `{"msg":"unknown field \"token\": [REDACTED]"}`},
		{`{"msg":"decoding \"secret\": unexpected EOF"}`, `{"msg":"decoding \"secret\": [REDACTED] EOF"}`},
		{`{"msg":"config \"api_key\"= missing, using default"}`, `{"msg":"config \"api_key\"= [REDACTED], using default"}`},
		// CLASS 1, bare spelling — the same class, and it predates the
		// widening. Pinned beside it so the two cannot be told apart by
		// accident: narrowing the escaped form while leaving this one would be
		// a distinction without a reason.
		{`unknown field "token": ignoring`, `unknown field "token": [REDACTED]`},
		// ...and the BOUND on class 1: a quoted keyword with no assignment
		// after it is ordinary prose and must survive. Without these the class
		// above would read as "any sentence mentioning a keyword", which is the
		// over-redaction that makes operators turn the defaults off.
		{`{"msg":"missing key \"api_key\" in config"}`, `{"msg":"missing key \"api_key\" in config"}`},
		{`{"msg":"reading \"secret\" from disk"}`, `{"msg":"reading \"secret\" from disk"}`},
		{`the \"token\" was rotated`, `the \"token\" was rotated`},
		// CLASS 2.
		{`{"password":"a\","user":"bob"}`, `{"password":"[REDACTED]"user":"bob"}`},
		{`password="C:\" other="b"`, `password="[REDACTED]"b"`},
	} {
		if got := s.Scrub(c.in); got != c.want {
			t.Errorf("a pinned over-redaction class changed shape:\n  in:   %s\n  got:  %s\n  want: %s\n"+
				"Over-redaction is the safe direction, but it costs downstream extraction — re-make the decision here.", c.in, got, c.want)
		}
	}
}

// Each value branch captures the quote that OPENED it and the replacement
// re-emits it, so the original terminator on the line still closes a quoted
// value. A branch added without its ${n} silently drops that opening quote
// (`password=[REDACTED]"` instead of `password="[REDACTED]"`), which reads as
// the value having ENDED at the redaction and breaks every field extraction
// downstream — logattrs, enrich and log-metrics all run after scrubbing.
func TestSecretKVReplacementReferencesEveryGroup(t *testing.T) {
	p := builtins["secret-kv"]
	if p.re.NumSubexp() < 2 {
		t.Fatalf("secret-kv has %d capture groups; the prefix and at least one quote capture are expected", p.re.NumSubexp())
	}
	for i := 1; i <= p.re.NumSubexp(); i++ {
		if ref := "${" + strconv.Itoa(i) + "}"; !strings.Contains(p.repl, ref) {
			t.Errorf("capture group %d is never referenced by the replacement %q: whatever it captures is DELETED from the redacted line", i, p.repl)
		}
	}
}
