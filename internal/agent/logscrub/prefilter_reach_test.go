package logscrub

import (
	"strings"
	"testing"
)

// The prefilter is an optimisation: it may only skip lines the regex could not
// have matched. If the regex reaches further than its prefilter, the pattern is
// skipped and the secret ships UNREDACTED — a security control quietly narrower
// than it advertises.
//
// Go's (?i) folds via unicode.SimpleFold, so `(?i)password` also matches
// `paſsword` (U+017F) and `(?i)token` matches `toKen` (U+212A KELVIN SIGN),
// while the literal prefilters fold ASCII only. The built-ins therefore spell
// their keywords with explicit ASCII case classes instead of (?i).
//
// Every built-in with a prefilter must be MATCHED by at least one probe: a
// probe list no pattern's regex matches asserts nothing about that pattern, and
// for url-userinfo, private-key, email and credit-card this one used to.
func TestPrefilterIsNotNarrowerThanItsRegex(t *testing.T) {
	matched := map[string]int{}
	for name, p := range builtins {
		if p.prefilter == nil {
			continue
		}
		for _, probe := range prefilterProbes {
			if !p.re.MatchString(probe) {
				continue
			}
			matched[name]++
			if !p.prefilter(probe) {
				t.Errorf("pattern %q: regex matches %q but the prefilter rejects it — the pattern is skipped and the secret ships unredacted",
					name, probe)
			}
		}
		if matched[name] == 0 {
			t.Errorf("pattern %q: no probe matches its regex, so this test asserts nothing about its prefilter — add one", name)
		}
	}
}

// prefilterProbes are lines at least one built-in's regex matches (and a few
// it must not); FuzzBuiltinPrefiltersNotNarrower seeds from them too.
var prefilterProbes = []string{
	// ASCII case variants — must match and must be redacted.
	"authorization: Bearer abc123XYZ.tok.sig",
	"authorization: bEaReR abc123XYZ.tok.sig",
	"Authorization: BASIC dXNlcjpwYXNzd29yZA==",
	"pAsSwOrD=hunter2trustno1",
	"API_KEY=AKIAIOSFODNN7EXAMPLE",
	"token: abcdef0123456789",
	"secret=topsecretvalue",
	// Unicode fold-equivalents — these must be treated CONSISTENTLY: if the
	// regex matches, the prefilter must let it through.
	"paſsword=hunter2trustno1",
	"ſecret=topsecretvalue",
	"Baſic dXNlcjpwYXNzd29yZA==",
	"toKen=abcdef0123456789",
	"bearerKey=abc123",
	// One per remaining built-in, so every prefilter is exercised.
	"dial redis://u:p@h failed",
	"dsn=postgres://:hunter2@db:5432/app",
	"-----BEGIN RSA PRIVATE KEY-----",
	"-----BEGIN PRIVATE KEY-----MIIEvQ-----END PRIVATE KEY-----",
	"contact a@b.co now",
	"card 4111 1111 1111 1111 declined",
	"card 4111-1111-1111-1111 declined",
	"card 4111111111111111 declined",
	// Plain lines.
	"a perfectly innocuous log line",
	"password", // keyword with no value
}

// End-to-end: everything the scrubber is meant to catch is actually redacted,
// including mixed ASCII case.
func TestMixedCaseSecretsRedacted(t *testing.T) {
	s := mustNew(t, Config{Builtin: []string{"defaults"}})
	for _, in := range []string{
		"authorization: bEaReR abc123XYZ.tok.sig",
		"pAsSwOrD=hunter2",
		"ApIkEy: sekret-value",
		"Authorization: BaSiC dXNlcjpwYXNzd29yZA==",
	} {
		if got := s.Scrub(in); !strings.Contains(got, redacted) {
			t.Errorf("not redacted: %q -> %q", in, got)
		}
	}
}
