package logscrub

import (
	"strings"
	"testing"
)

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
