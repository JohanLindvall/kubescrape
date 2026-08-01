package logscrub

import (
	"strings"
	"testing"
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
	for i := 0; i < b.N; i++ {
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
