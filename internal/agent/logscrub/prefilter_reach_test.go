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
func TestPrefilterIsNotNarrowerThanItsRegex(t *testing.T) {
	probes := []string{
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
		"bearerKey=abc123",
		// Plain lines.
		"a perfectly innocuous log line",
		"password", // keyword with no value
	}

	for name, p := range builtins {
		if p.prefilter == nil {
			continue
		}
		for _, probe := range probes {
			if p.re.MatchString(probe) && !p.prefilter(probe) {
				t.Errorf("pattern %q: regex matches %q but the prefilter rejects it — the pattern is skipped and the secret ships unredacted",
					name, probe)
			}
		}
	}
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
