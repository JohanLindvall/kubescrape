package logscrub

import (
	"strings"
	"testing"
)

// The most common secret spellings are compound keys. `\b` treats `_` as a word
// character, so a keyword anchored with it could never match access_token,
// client_secret, DB_PASSWORD or AWS_SECRET_ACCESS_KEY — i.e. the pattern caught
// the rarest forms and shipped the usual ones in clear.
func TestCompoundKeySecretsRedacted(t *testing.T) {
	s := mustNew(t, Config{Builtin: []string{"defaults"}})
	for _, in := range []string{
		"access_token=ya29.SUPERSECRETVALUE",
		"refresh_token=1//0gSUPERSECRET",
		"client_secret=abcdefSUPERSECRET",
		`{"access_token":"ya29.SUPERSECRETVALUE"}`,
		"AWS_SECRET_ACCESS_KEY=SECRETVALUE",
		"DB_PASSWORD=hunter2",
		"MYSQL_PWD=hunter2",
		"X_API_TOKEN=abc123",
		"accessToken: abc123",
		"clientSecret: abc123",
		"id_token=abc123",
		// The keyword as a PREFIX of the key. Django's settings dump, the AWS
		// SDK's camelCase JSON and every `secret_key:` YAML shipped in clear
		// while the pattern only allowed the keyword as a suffix.
		"SECRET_KEY=django-insecure-8f2b",
		"secret_key: hunter2",
		`{"secretKey":"wJalrXUtnFEMIK7MDENG"}`,
		"secretValue=abc123",
		"TOKEN_VALUE=abc123",
		// Forms that already worked must keep working.
		"api_key=sk-12345",
		"password=hunter2",
		`{"password": "hunter2"}`,
	} {
		got := s.Scrub(in)
		if !strings.Contains(got, redacted) {
			t.Errorf("secret shipped in clear: %q -> %q", in, got)
		}
	}
}

// Widening the keyword boundary must not start redacting ordinary words.
func TestCompoundKeyNoOverRedaction(t *testing.T) {
	s := mustNew(t, Config{Builtin: []string{"defaults"}})
	for _, in := range []string{
		"tokenizer=bert-base",
		"keyboard=us",
		"monkey=banana",
		"a perfectly innocuous log line",
		"keys=3 processed",
		// A suffix is only a suffix across a compound-word boundary: no
		// separator and no camelCase hump means this is one ordinary word.
		"TOKENIZER=bert-base",
		"passwords_checked=3",
		// `_id` names WHICH key was used — audit information, not the
		// credential. The credential spelling (api_key=) is still covered, and
		// an AWS access key ID is matched by value through the aws-key rule.
		"api_key_id=AKIAIOSFODNN7",
		// Everyday Kubernetes vocabulary built on the same keywords. These are
		// the lines an unbounded compound suffix destroyed: a Secret's NAME,
		// its namespace and its reference are ordinary object identifiers that
		// every controller logs, and scrubbing runs BEFORE logAttributes,
		// enrich, logMetrics and the rules — so redacting them does not hide a
		// secret, it deletes the field every downstream extraction keys on.
		"secret_name=my-tls-cert",
		`{"secretName":"example-tls"}`,
		"secretRef: istio-ca",
		`{"secret_namespace":"kube-system"}`,
		"token_count=512",
		"password_last_changed=2026-01-01",
	} {
		if got := s.Scrub(in); strings.Contains(got, redacted) {
			t.Errorf("over-redacted an ordinary line: %q -> %q", in, got)
		}
	}

	// ...while the genuine credential spellings the same boundary has to keep
	// catching. Widening or narrowing keySuffix must move exactly one of these
	// two lists, never both.
	for _, in := range []string{
		"api_key=sk-1",
		"secret=hunter2",
		"password=hunter2",
		"authToken=abc",
	} {
		if got := s.Scrub(in); !strings.Contains(got, redacted) {
			t.Errorf("credential shipped in clear: %q -> %q", in, got)
		}
	}
}

// The accepted cost of the curated suffix list. `value` is on it because
// `secretValue=abc123` and `TOKEN_VALUE=abc123` are real credential spellings
// (TestCompoundKeySecretsRedacted requires both), and RE2 has no lookahead to
// tell those from an AWS SDK call named GetSecretValue. So an SDK log line
// loses its ARN — which names a secret rather than containing one.
//
// Pinned deliberately: dropping `value` from keySuffix to recover this line
// would ship the two spellings above in clear, which is the strictly worse
// trade. If the pattern is ever narrowed some other way, this test is where
// the decision gets re-made.
func TestSecretValueSuffixOverReachesOnSDKCallNames(t *testing.T) {
	s := mustNew(t, Config{Builtin: []string{"defaults"}})
	const in = "secretsmanager.GetSecretValue: arn=arn:aws:secretsmanager:eu-west-1:1234:secret:prod/db-AbCdEf"
	got := s.Scrub(in)
	if got != "secretsmanager.GetSecretValue: "+redacted {
		t.Errorf("known over-redaction changed shape:\n  in:  %s\n  out: %s\nIf this now passes through unchanged, check that secretValue=/TOKEN_VALUE= still redact and update this test.", in, got)
	}
}

// An unquoted JSON value must not swallow the closing brace: scrubbing runs
// BEFORE logattrs/enrich/log-metrics, so a corrupted line silently kills every
// downstream field extraction on that record.
func TestScrubKeepsJSONWellFormed(t *testing.T) {
	s := mustNew(t, Config{Builtin: []string{"defaults"}})
	for _, in := range []string{
		`{"level":"error","token":12345}`,
		`{"level":"error","password":null}`,
		`{"a":[1,2],"api_key":abc}`,
	} {
		got := s.Scrub(in)
		if !strings.Contains(got, redacted) {
			t.Errorf("not redacted: %q -> %q", in, got)
		}
		if strings.Count(got, "{") != strings.Count(got, "}") {
			t.Errorf("scrub unbalanced the JSON: %q -> %q", in, got)
		}
	}
}
