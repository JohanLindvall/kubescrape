package logscrub

import (
	"os"
	"sort"
	"strings"
	"testing"

	"github.com/JohanLindvall/kubescrape/internal/testrace"
)

func mustNew(t *testing.T, cfg Config) *Scrubber {
	t.Helper()
	s, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestDefaultsRedact(t *testing.T) {
	s := mustNew(t, Config{Builtin: []string{"defaults"}})
	cases := map[string]string{
		"Authorization: Bearer eyJhbGciOiJIUzI1NiJ9.payload.sig done": "Authorization: Bearer [REDACTED] done",
		"authorization: basic dXNlcjpwYXNz":                           "authorization: basic [REDACTED]",
		`retry with api_key=sk-12345 next`:                            "retry with api_key=[REDACTED] next",
		`{"password": "hunter2", "user": "bob"}`:                      `{"password": "[REDACTED]", "user": "bob"}`,
		"leaked AKIAIOSFODNN7EXAMPLE in env":                          "leaked [REDACTED] in env",
	}
	for in, want := range cases {
		if got := s.Scrub(in); got != want {
			t.Errorf("Scrub(%q)\n got %q\nwant %q", in, got, want)
		}
	}
}

func TestNoMatchReturnsSameString(t *testing.T) {
	if testrace.Enabled {
		t.Skip("-race perturbs allocation counts")
	}
	s := mustNew(t, Config{Builtin: []string{"defaults"}})
	in := "a perfectly innocuous log line with nothing sensitive"
	if got := s.Scrub(in); got != in {
		t.Fatalf("changed an innocuous line: %q", got)
	}
	allocs := testing.AllocsPerRun(200, func() { _ = s.Scrub(in) })
	if allocs != 0 {
		t.Fatalf("no-match path allocates: %v allocs", allocs)
	}
}

func TestOptInPatternsAndUserRules(t *testing.T) {
	// email/credit-card are NOT in defaults.
	s := mustNew(t, Config{Builtin: []string{"defaults"}})
	if got := s.Scrub("mail bob@example.com now"); !strings.Contains(got, "bob@example.com") {
		t.Fatalf("email redacted by defaults: %q", got)
	}
	s = mustNew(t, Config{Builtin: []string{"email", "credit-card"}})
	if got := s.Scrub("mail bob@example.com now"); strings.Contains(got, "bob@") {
		t.Fatalf("email not redacted: %q", got)
	}
	if got := s.Scrub("paid with 4111 1111 1111 1111 today"); strings.Contains(got, "4111") {
		t.Fatalf("card not redacted: %q", got)
	}

	s = mustNew(t, Config{Rules: []Rule{{Name: "ssn", Regexp: `\b\d{3}-\d{2}-\d{4}\b`, Replacement: "xxx-xx-xxxx"}}})
	if got := s.Scrub("ssn 123-45-6789 on file"); got != "ssn xxx-xx-xxxx on file" {
		t.Fatalf("user rule: %q", got)
	}
}

func TestConfigValidation(t *testing.T) {
	if _, err := New(Config{Builtin: []string{"no-such"}}); err == nil {
		t.Fatal("unknown builtin must fail fast")
	}
	if _, err := New(Config{Rules: []Rule{{Regexp: "("}}}); err == nil {
		t.Fatal("invalid regexp must fail fast")
	}
	if _, err := New(Config{}); err == nil {
		t.Fatal("empty config must fail fast")
	}
}

func BenchmarkScrubNoMatch(b *testing.B) {
	s, _ := New(Config{Builtin: []string{"defaults"}})
	line := `2026-07-24T10:00:00Z INFO handled request path=/api/v1/resource status=200 duration=12ms`
	b.ReportAllocs()
	for b.Loop() {
		_ = s.Scrub(line)
	}
}

// Mixed-case keywords must still be redacted — the (?i) regexes intend
// case-insensitive matching, so the prefilter must not be narrower.
func TestScrubMixedCaseKeywords(t *testing.T) {
	s := mustNew(t, Config{Builtin: []string{"defaults"}})
	cases := []string{
		"authorization: bEaReR abc123XYZ.tok.sig",
		"pAsSwOrD=hunter2",
		"ApIkEy: sekret-value",
	}
	for _, in := range cases {
		if got := s.Scrub(in); !strings.Contains(got, "[REDACTED]") {
			t.Errorf("mixed-case not redacted: %q -> %q", in, got)
		}
	}
}

// A connection string's password is a credential with no key=value shape for
// secret-kv to match, and it reaches logs through dial-failure messages and
// config dumps.
func TestURLUserinfoRedacted(t *testing.T) {
	s := mustNew(t, Config{Builtin: []string{"defaults"}})
	for _, tc := range []struct{ in, wantNot string }{
		{`dial failed: postgres://svc:s3cr3t@db-1:5432/app`, "s3cr3t"},
		{`DATABASE_URL=mysql://root:hunter2@10.0.0.5/db`, "hunter2"},
		{`amqp://guest:guest@rabbit:5672/`, "guest:guest@"},
	} {
		got := s.Scrub(tc.in)
		if strings.Contains(got, tc.wantNot) {
			t.Errorf("credential survived: %q -> %q", tc.in, got)
		}
		if !strings.Contains(got, redacted) {
			t.Errorf("nothing redacted in %q -> %q", tc.in, got)
		}
	}
	// A URL without userinfo must be untouched.
	plain := "GET https://api.example.com/v1/things?page=2"
	if got := s.Scrub(plain); got != plain {
		t.Errorf("over-redacted a plain URL: %q -> %q", plain, got)
	}

	// ONLY the password half goes. The scheme, the user, the host and the path
	// are what make a dial-failure line diagnosable at all, and they are also
	// what logAttributes/enrich parse out of it afterwards — a pattern that
	// eats them turns one redaction into a destroyed record.
	const dsn = "connect failed: https://user:hunter2@host/path?retry=1"
	if got := s.Scrub(dsn); got != "connect failed: https://user:[REDACTED]@host/path?retry=1" {
		t.Errorf("userinfo redaction must replace the password and nothing else:\n  in:  %s\n  out: %s", dsn, got)
	}
}

// A DSN password may itself contain '@' — `p@ssw0rd` is not an exotic choice,
// and a URL-encoding library is what usually hides it. Excluding '@' from the
// password class stopped the match at the FIRST one and emitted
// `svc:[REDACTED]@ss@db-1`: a line that claims a redaction and still carries
// most of the credential, which is the one failure obs.LogScrubbed cannot
// distinguish from a clean one. The class is greedy, so leaving '@' in runs to
// the LAST one inside the token; what bounds the token is the delimiter set,
// pinned by TestURLUserinfoDoesNotEatSurroundingContent.
func TestURLUserinfoRedactsPasswordContainingAtSign(t *testing.T) {
	s := mustNew(t, Config{Builtin: []string{"defaults"}})
	for _, c := range []struct{ in, want string }{
		{`dial failed: postgres://svc:p@ss@db-1:5432/app`, `dial failed: postgres://svc:[REDACTED]@db-1:5432/app`},
		{`{"dsn":"postgres://u:p@ss@h/db"}`, `{"dsn":"postgres://u:[REDACTED]@h/db"}`},
		{`redis://:h@nt@r2@cache:6379`, `redis://:[REDACTED]@cache:6379`},
		// The single-'@' spellings must be untouched by the same change.
		{`connect failed: https://user:hunter2@host/path?retry=1`, `connect failed: https://user:[REDACTED]@host/path?retry=1`},
		{`amqp://guest:guest@rabbit:5672/`, `amqp://guest:[REDACTED]@rabbit:5672/`},
	} {
		if got := s.Scrub(c.in); got != c.want {
			t.Errorf("Scrub(%q)\n got %q\nwant %q", c.in, got, c.want)
		}
	}
}

// The userinfo pattern must not walk past the credential into ordinary
// content. Scrubbing runs BEFORE logAttributes, enrich, logMetrics and the
// rules, so a corrupted line kills every field extraction downstream of it.
func TestURLUserinfoDoesNotEatSurroundingContent(t *testing.T) {
	s := mustNew(t, Config{Builtin: []string{"defaults"}})
	for _, in := range []string{
		`{"msg":"connecting","dsn":"redis://cache:6379","user":"admin@corp.com"}`,
		`endpoint=amqp://rabbit:5672,owner=ops@corp.com`,
		`see http://docs.example.com:8080/guide and mail ops@corp.com`,
		// The two shapes ordinary application logs are actually written in: a
		// structured line whose `url` field is a plain request URL, and a
		// logfmt access line. Neither carries a credential; both contain
		// `://`, so both reach the regex.
		`{"level":"info","url":"https://api.example.com/v1/items?since=2026-01-01","status":200}`,
		`time="2026-07-30T10:00:00Z" msg="GET https://svc/health 200"`,
		// The same request logged with an explicit PORT and an ordinary
		// address later in the line. `host:port` is exactly the shape the
		// pattern reads as `user:password`, and an unbounded password charset
		// then ran from the port through the JSON quote and every field after
		// it to whatever '@' came next — here destroying the `url` field that
		// logAttributes and enrich key on, on a line with no credential in it.
		`{"level":"info","upstream":"https://api.example.com:443","actor":"alice@corp.com","url":"https://api.example.com/v1/items?since=2026-01-01"}`,
	} {
		if got := s.Scrub(in); got != in {
			t.Errorf("no credential present, but the line was rewritten:\n  in:  %s\n  out: %s", in, got)
		}
	}
	// ...while the empty-user spelling, the standard Redis one, IS redacted.
	const redis = `redis://:hunter2@cache:6379`
	got := s.Scrub(redis)
	if !strings.Contains(got, redacted) || strings.Contains(got, "hunter2") {
		t.Errorf("empty-user credential not redacted: %q -> %q", redis, got)
	}
}

// hostileLine is a 1 MiB record containing the secret-kv keywords but no
// assignment anywhere — the shape the old word-anywhere prefilter admitted,
// after which the regex walked the whole megabyte to find nothing. It runs on
// the SINGLE sweep goroutine that serves every log file on the node, so this
// benchmark is a latency budget, not a curiosity.
func hostileLine() string {
	return strings.Repeat("token secret password key ", (1<<20)/26)
}

func BenchmarkScrubHostileLongLine(b *testing.B) {
	s, _ := New(Config{Builtin: []string{"defaults"}})
	line := hostileLine()
	b.SetBytes(int64(len(line)))
	b.ReportAllocs()
	for b.Loop() {
		if got := s.Scrub(line); len(got) != len(line) {
			b.Fatal("the hostile line must not be redacted")
		}
	}
}

// The performance fix is a CORRECTNESS property of the prefilter: a line whose
// keywords are not followed by an assignment cannot match, so it must never
// reach the regex. (The regex answer is unchanged either way; what changes is
// whether a megabyte of log costs 100 ms of the node's only sweep goroutine.)
func TestSecretKVPrefilterRejectsUnmatchableLines(t *testing.T) {
	p := builtins["secret-kv"]
	for _, in := range []string{
		"token",
		"a bare key in prose",
		"the secret of success",
		"password", // keyword with no value
		"token_count 42",
		"secretName my-tls-cert",
		"tokenizer bert-base",
		"idempotency key was reused",
		"apikey",
		"password=", // separator but no value
		"password=  ",
		`{"secret": }`,
	} {
		if p.prefilter(in) {
			t.Errorf("prefilter admitted an unmatchable line (the regex then pays for the whole record): %q", in)
		}
		if p.re.MatchString(in) {
			t.Errorf("setup: %q was supposed to be unmatchable", in)
		}
	}
}

// altCaseASCII renders a word in alternating ASCII case (sEcReT) — the mixed
// form the prefilter's fold must treat exactly as the regex's case classes do.
func altCaseASCII(w string) string {
	b := []byte(strings.ToLower(w))
	for i := 1; i < len(b); i += 2 {
		if 'a' <= b[i] && b[i] <= 'z' {
			b[i] -= 'a' - 'A'
		}
	}
	return string(b)
}

// The prefilter may only ever be WIDER than its regex. This walks the cross
// product of the keyword spellings, separators, quoting and suffixes rather
// than a handful of probes: a shape the regex matches and the prefilter rejects
// is a secret shipped in clear.
//
// The keyword and suffix axes DERIVE from the vocabulary tables (kvKeywords /
// kvSuffixes) — the same tables the regex and the dispatch render from — so a
// word added there is covered here with no hand-synced list to forget.
func TestSecretKVPrefilterCoversEveryMatchingShape(t *testing.T) {
	p := builtins["secret-kv"]
	var keywords []string
	for _, parts := range kvKeywords {
		for _, w := range kvExpand(parts) {
			keywords = append(keywords, w, strings.ToUpper(w), altCaseASCII(w))
		}
	}
	suffixes := []string{""}
	for _, w := range kvSuffixes {
		suffixes = append(suffixes, "_"+w, "-"+strings.ToUpper(w), altCaseASCII(w))
	}
	prefixes := []string{"", "x_", "MY.", "aws-", "{\"", "  ", `{\"`}
	// The last two separators are ESCAPE-quoted: a message stringified into a
	// JSON field carries `{\"password\":\"…`, so the key's closing quote is a
	// backslash-quote pair the prefilter has to walk past exactly as the regex
	// does.
	seps := []string{":", "=", " : ", "\t=\t", "\": \"", "'='", `\": \"`, `\'=\'`}
	// "\vraw" is deliberate: \v is not in Go regexp's \s, so it is a legal
	// first byte of the value class — a prefilter treating it as whitespace
	// would reject a line the regex matches. The next three embed the OPPOSITE
	// quote kind in a quoted value: a legal first value byte a same-kind-only
	// terminator must not reject. The next three are the escape-quoted value
	// branches, plus a bare backslash value. The last three are PLAINLY quoted
	// values carrying an ESCAPED quote of their own kind — the branches now
	// read an escape and its escapee as one unit, so they reach past a `\"` the
	// prefilter must also walk past.
	values := []string{"hunter2", "0", `"quoted"`, "sk-12345", "\vraw", `"don't"`, `'a"b'`, `"'"`,
		`\"escaped\"`, `\'escaped\'`, `C:\Users\svc`,
		`"a\"b"`, `'a\'b'`, `"C:\Users\svc"`}
	matched := 0
	for _, kw := range keywords {
		for _, pre := range prefixes {
			for _, suf := range suffixes {
				for _, sep := range seps {
					for _, val := range values {
						line := pre + kw + suf + sep + val
						if !p.re.MatchString(line) {
							continue
						}
						matched++
						if !p.prefilter(line) {
							t.Fatalf("regex matches %q but the prefilter rejects it: the pattern is skipped and the secret ships unredacted", line)
						}
					}
				}
			}
		}
	}
	// A derivation bug that emptied an axis would pass vacuously.
	if matched == 0 {
		t.Fatal("cross product produced no matching lines; the derivation is broken")
	}
}

// The same property, fuzzed: the seed corpus runs on every `go test`, and
// `-fuzz` explores from there. Any input where the regex matches and the
// prefilter does not is a redaction the scrubber silently skips.
func FuzzSecretKVPrefilterNotNarrower(f *testing.F) {
	for _, s := range []string{
		"password=hunter2", "token_count=42", "SECRET_KEY=abc", `{"secretKey":"x"}`,
		"a perfectly innocuous log line", "pwd:'x'", "api-key\t=\tv", "accessToken: abc",
		"secret_key_ref: db-creds", "AWS_SECRET_ACCESS_KEY=abc", "toKen=abcdef",
		// Mixed-quote values: each quoted branch excludes only its OWN kind,
		// so the other quote is a legal value byte the prefilter must admit.
		`password="don't tell"`, `secret='he said "go"'`, `password="'"`, `token=''`,
		// ESCAPE-quoted values and keys — the shape a payload stringified into
		// a JSON message field takes. The value branches start at a backslash
		// and the key's closing quote is a backslash-quote pair; a prefilter
		// walking past neither is narrower than its pattern on the most
		// ordinary secret spelling there is.
		`{"msg":"password=\"hunter2\" tail"}`, `{\"password\":\"hunter2\"}`,
		`password=\'x\'`, `password=\"\"`, `password=\"`, `password=C:\Users\svc`,
		// A PLAINLY quoted value carrying an escaped quote of its own kind:
		// the branch reads `\"` as content and runs past it, so the prefilter
		// has to admit the whole shape too. The last two are the edges of that
		// rule — an escaped backslash, and a value that is a lone trailing
		// backslash (which the escape-unit form no longer matches at all).
		`{"password":"he said \"hi\" ok"}`, `secret='don\'t tell'`,
		`password="C:\Users\svc"`, `{"password":"a\\","user":"bob"}`, `password="\`,
	} {
		f.Add(s)
	}
	p := builtins["secret-kv"]
	f.Fuzz(func(t *testing.T, line string) {
		if p.re.MatchString(line) && !p.prefilter(line) {
			t.Fatalf("regex matches %q but the prefilter rejects it", line)
		}
	})
}

// A QUOTED secret runs to its closing quote. The value class is the UNQUOTED
// terminator set, and applying it inside quotes redacted only the first
// fragment — so any passphrase containing a space, comma, semicolon, ampersand
// or closing bracket shipped its tail in clear.
func TestSecretKVRedactsWholeQuotedValue(t *testing.T) {
	s, err := New(Config{Builtin: []string{"defaults"}})
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct{ in, want string }{
		{`password="hunter2 with spaces"`, `password="[REDACTED]"`},
		{`{"password":"p@ss w0rd"}`, `{"password":"[REDACTED]"}`},
		{`token='part1,part2;part3'`, `token='[REDACTED]'`},
		{`secret="a}b"`, `secret="[REDACTED]"`},
		{`api_key="a&b;c d]e"`, `api_key="[REDACTED]"`},
		// The unquoted form is unchanged: it still stops at the delimiter, so
		// the surrounding JSON/logfmt structure survives for the extractors
		// that run after scrubbing.
		{`password=nospaces`, `password=[REDACTED]`},
		{`{"password":nospaces}`, `{"password":[REDACTED]}`},
		// An empty value is not a credential and matched neither branch before.
		{`password=""`, `password=""`},
	}
	for _, c := range cases {
		if got := s.Scrub(c.in); got != c.want {
			t.Errorf("Scrub(%q)\n got %q\nwant %q", c.in, got, c.want)
		}
	}
}

// A quoted value ends at the closing quote of its OWN kind: a double-quoted
// value may contain apostrophes and a single-quoted one double quotes. A value
// class excluding BOTH kinds stops a double-quoted value at an embedded
// apostrophe — `password="don't tell"` redacts `don` and ships `'t tell` in
// clear. The WHOLE value must go.
func TestSecretKVMixedQuoteValues(t *testing.T) {
	s := mustNew(t, Config{Builtin: []string{"defaults"}})
	cases := []struct{ in, want string }{
		{`password="don't tell"`, `password="[REDACTED]"`},
		{`{"token":"it's-a-secret"}`, `{"token":"[REDACTED]"}`},
		{`secret='he said "go away"'`, `secret='[REDACTED]'`},
		{`pwd='embedded "quotes" here'`, `pwd='[REDACTED]'`},
		// A value that is ONLY the other quote kind is still a value.
		{`password="'"`, `password="[REDACTED]"`},
		{`api_key='"'`, `api_key='[REDACTED]'`},
		// The empty value still matches neither branch.
		{`password=""`, `password=""`},
		{`password=''`, `password=''`},
	}
	for _, c := range cases {
		if got := s.Scrub(c.in); got != c.want {
			t.Errorf("Scrub(%q)\n got %q\nwant %q", c.in, got, c.want)
		}
	}
}

// A user rule whose regex proves a literal prefix carries a Contains admission
// gate (LiteralPrefix: every match must BEGIN with the literal, so a line not
// containing it cannot match anywhere). The gate may only ever SKIP lines the
// regex cannot match — gated and ungated evaluation must be indistinguishable
// on every input.
func TestUserRulePrefilterMatchesUngated(t *testing.T) {
	cfg := Config{Rules: []Rule{
		{Name: "session", Regexp: `sess_[0-9a-f]{8}`},
		{Name: "anchored", Regexp: `^card=[0-9]{12}`, Replacement: "card=x"},
		{Name: "leading-class", Regexp: `[0-9]{3}-[0-9]{2}-[0-9]{4}`, Replacement: "x-x-x"},
		{Name: "case-fold", Regexp: `(?i)xtok: [0-9a-f]+`},
		{Name: "alt", Regexp: `(?:uid|gid)=[0-9]+`},
	}}
	gated := mustNew(t, cfg)
	ungated := mustNew(t, cfg)
	for i := range ungated.patterns {
		ungated.patterns[i].prefilter = nil
	}
	// The plain-literal rule must actually carry a gate, or this only proves
	// two ungated scrubbers equal.
	if gated.patterns[0].prefilter == nil {
		t.Fatal("rule with a literal prefix got no prefilter")
	}
	corpus := []string{
		"sess_deadbeef at line start",
		"in the middle sess_0123abcd of the line",
		"and at the very end: sess_cafef00d",
		// Near miss: the literal is present but the regex fails — the gate
		// admits, the regex declines, and the line must be untouched.
		"sess_XYZ literal without a match",
		"card=123456789012",
		"prefix card=123456789012 not at start",
		"ssn 123-45-6789 on file",
		"XTOK: abcdef and mixed xToK: 0123",
		"uid=1000 gid=1000",
		"a line matching no rule at all",
		"",
	}
	for _, line := range corpus {
		if g, u := gated.Scrub(line), ungated.Scrub(line); g != u {
			t.Errorf("gated/ungated diverge on %q:\n gated:   %q\n ungated: %q", line, g, u)
		}
	}
}

// maxEnumWindow bounds how far apart the default names may sit and still read
// as ONE enumeration. The widest today is CONFIGURATION.md's, whose secret-kv
// entry carries a parenthetical gloss (~350 bytes); 600 leaves room for prose
// without letting a mention on the other side of the file stand in for a list
// entry.
const maxEnumWindow = 600

// defaultSet is a security control, and its documentation is part of it: an
// operator who spells the list out instead of writing `defaults` gets exactly
// what the docs told them the default was. url-userinfo was added to the set
// and to none of the three places that enumerate it, so following the docs
// silently disabled DSN-password redaction. Nothing pinned that; this does.
//
// It pins the ENUMERATION, not the bare name. A Contains check was vacuous for
// the member most likely to be dropped as a duplicate: `bearer` occurs ten-plus
// times in each of these files for unrelated OTLP- and scrape-auth-token
// reasons, so deleting it from all three lists left the guard green. Requiring
// every name inside ONE small window is what makes a deletion visible.
func TestDefaultSetIsDocumented(t *testing.T) {
	docs := []string{
		"../../../README.md",
		"../../../docs/CONFIGURATION.md",
		"../../../CLAUDE.md",
	}
	for _, name := range defaultSet {
		if _, ok := builtins[name]; !ok {
			t.Errorf("defaultSet names %q, which is not a built-in pattern", name)
		}
	}
	for _, d := range docs {
		b, err := os.ReadFile(d)
		if err != nil {
			t.Fatalf("reading %s: %v", d, err)
		}
		doc := string(b)
		for _, name := range defaultSet {
			if !strings.Contains(doc, name) {
				t.Errorf("%s does not mention the default pattern %q; an operator pinning the "+
					"documented list would silently lose it", d, name)
			}
		}
		if w := minEnumWindow(doc, defaultSet); w < 0 || w > maxEnumWindow {
			t.Errorf("%s does not enumerate the default set in one place (smallest window holding "+
				"all of %v is %d bytes, want <= %d); an operator pinning the documented list would "+
				"silently lose whatever was dropped from it", d, defaultSet, w, maxEnumWindow)
		}
	}

	// The FOURTH enumeration is in this package: Config.Builtin's doc comment,
	// whose own text claims this test pins it. Only the comment block is
	// scanned — over the whole file defaultSet's own literal would satisfy any
	// check trivially.
	src, err := os.ReadFile("logscrub.go")
	if err != nil {
		t.Fatalf("reading logscrub.go: %v", err)
	}
	block := docCommentBefore(string(src), "Builtin []string")
	if block == "" {
		t.Fatal("no doc comment found above Config.Builtin")
	}
	for _, name := range defaultSet {
		if !strings.Contains(block, name) {
			t.Errorf("Config.Builtin's doc comment omits the default pattern %q", name)
		}
	}
}

// The guard above is only worth having if it DISCRIMINATES: this is the exact
// regression it exists for — the enumeration with `bearer` deleted, beside the
// unrelated `-otlp-bearer-token-file` prose that kept a bare Contains check
// green while DSN-token redaction quietly left the documented default set.
func TestEnumWindowCatchesADroppedName(t *testing.T) {
	list := "`basic-auth`, `secret-kv`, `aws-key`, `private-key`, `url-userinfo`"
	filler := strings.Repeat("unrelated prose. ", 100) // well past maxEnumWindow
	dropped := "`-otlp-bearer-token-file` names the file holding the export token." +
		filler + "builtin `defaults` expands to " + list + "." + filler +
		"the scrape-auth bearer token is re-read every minute."

	if !strings.Contains(dropped, "bearer") {
		t.Fatal("fixture must still mention bearer elsewhere, or it proves nothing")
	}
	if w := minEnumWindow(dropped, defaultSet); w >= 0 && w <= maxEnumWindow {
		t.Errorf("a list with `bearer` deleted still read as an enumeration (window %d bytes)", w)
	}

	// Positive control: put it back and the same text is one enumeration.
	intact := strings.Replace(dropped, "expands to `basic-auth`", "expands to `bearer`, `basic-auth`", 1)
	if w := minEnumWindow(intact, defaultSet); w < 0 || w > maxEnumWindow {
		t.Errorf("the intact list did not read as an enumeration (window %d bytes)", w)
	}
}

// minEnumWindow returns the length of the smallest byte window of doc holding
// every name, or -1 when one of them is absent.
func minEnumWindow(doc string, names []string) int {
	type occ struct{ at, end, name int }
	var occs []occ
	for i, n := range names {
		for from := 0; from < len(doc); {
			j := strings.Index(doc[from:], n)
			if j < 0 {
				break
			}
			at := from + j
			occs = append(occs, occ{at: at, end: at + len(n), name: i})
			from = at + 1
		}
	}
	sort.Slice(occs, func(i, j int) bool { return occs[i].at < occs[j].at })
	seen := make([]int, len(names))
	distinct, best, lo := 0, -1, 0
	for hi := range occs {
		if seen[occs[hi].name] == 0 {
			distinct++
		}
		seen[occs[hi].name]++
		for distinct == len(names) {
			end := 0
			for _, o := range occs[lo : hi+1] {
				if o.end > end {
					end = o.end
				}
			}
			if w := end - occs[lo].at; best < 0 || w < best {
				best = w
			}
			seen[occs[lo].name]--
			if seen[occs[lo].name] == 0 {
				distinct--
			}
			lo++
		}
	}
	return best
}

// docCommentBefore returns the contiguous //-comment block immediately above
// the line containing decl.
func docCommentBefore(src, decl string) string {
	i := strings.Index(src, decl)
	if i < 0 {
		return ""
	}
	lines := strings.Split(src[:i], "\n")
	end := len(lines) - 1 // the (partial) line carrying decl
	start := end
	for start > 0 && strings.HasPrefix(strings.TrimSpace(lines[start-1]), "//") {
		start--
	}
	return strings.Join(lines[start:end], "\n")
}
