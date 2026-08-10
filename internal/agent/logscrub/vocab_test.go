package logscrub

import (
	"slices"
	"strings"
	"testing"
)

// The vocabulary lives in ONE table (kvKeywords/kvSuffixes) and everything
// else renders from it. This pins the rendering byte-for-byte against the
// hand-written regex source the table replaced: the derivation must be a pure
// de-duplication, never a semantic change.
func TestSecretKVRegexSourceIsPinned(t *testing.T) {
	handSuffix := `(?:[_-]?(?:` +
		asciiFold("key") + `|` + asciiFold("value") + `|` + asciiFold("token") +
		`|` + asciiFold("secret") + `|` + asciiFold("password") + `|` +
		asciiFold("passwd") + `|` + asciiFold("pwd") + `))?`
	want := `((?:^|[^0-9A-Za-z_.-])[0-9A-Za-z_.-]*?(?:` +
		asciiFold("api") + `[_-]?` + asciiFold("key") +
		`|` + asciiFold("secret") + `|` + asciiFold("password") + `|` + asciiFold("passwd") +
		`|` + asciiFold("pwd") + `|` + asciiFold("token") +
		`|` + asciiFold("access") + `[_-]?` + asciiFold("key") +
		`)` + handSuffix + `(?:\\?["\'])?\s*[:=]\s*)` +
		`(?:(\\")(?:[^"\\]|\\[^"])+|(\\')(?:[^'\\]|\\[^'])+|(")(?:[^"\\]|\\[\s\S])+|(')(?:[^'\\]|\\[\s\S])+|(?:[^\s"\'&,;}\])\\]|\\[^"\'])+)`
	if got := builtins["secret-kv"].re.String(); got != want {
		t.Fatalf("table-derived secret-kv regex drifted from the pinned hand-written source:\n got:  %s\nwant: %s", got, want)
	}
	if got := keySuffix; got != handSuffix {
		t.Fatalf("table-derived keySuffix drifted:\n got:  %s\nwant: %s", got, handSuffix)
	}
}

// The dispatch is built at init from the same table. This pins the grouping:
// every keyword spelling under its lowercased first byte, in table order, and
// nothing under any other byte — a spelling that fell out of the dispatch
// would make the prefilter narrower than the regex.
func TestSecretKVDispatchDerivation(t *testing.T) {
	want := map[byte][]string{
		'a': {"apikey", "api_key", "api-key", "accesskey", "access_key", "access-key"},
		's': {"secret"},
		'p': {"password", "passwd", "pwd"},
		't': {"token"},
	}
	for c := range 256 {
		if got := kvDispatch[c]; !slices.Equal(got, want[byte(c)]) {
			t.Errorf("kvDispatch[%q] = %v, want %v", byte(c), got, want[byte(c)])
		}
	}
}

// The prefix form must redact the SECRET spellings without eating the
// everyday Kubernetes vocabulary built on the same words.
func TestKeyPrefixDoesNotEatOrdinaryFields(t *testing.T) {
	s := mustNew(t, Config{Builtin: []string{"defaults"}})
	for _, in := range []string{
		"secretName=my-tls-cert", // names a Secret; not one
		"secretRef: registry-creds",
		"secret_key_ref: db-creds",
		"token_count=42",
		"tokenBucket=full",
		"passwordPolicy=strict",
		"tokenizer=bert-base",
		"keyboard=us",
	} {
		if got := s.Scrub(in); got != in {
			t.Errorf("over-redacted ordinary content: %q -> %q", in, got)
		}
	}
	for _, in := range []string{
		"SECRET_KEY=django-insecure-8f2b",
		`{"secretKey":"wJalrXUtnFEMIK7MDENG"}`,
		"secret_key: hunter2",
		"secretValue=abc123",
		"TOKEN_VALUE=abc123",
		"AWS_SECRET_ACCESS_KEY=abc",
		"password=hunter2",
	} {
		if got := s.Scrub(in); !strings.Contains(got, redacted) {
			t.Errorf("secret shipped in clear: %q -> %q", in, got)
		}
	}
}
