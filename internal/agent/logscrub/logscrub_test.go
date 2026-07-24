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
