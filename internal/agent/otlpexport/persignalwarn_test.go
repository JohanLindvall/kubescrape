package otlpexport

// CONFIGURATION.md promises that a start and -check-config WARN when an
// own-endpoint export.<signal> stops presenting the flag base's collector
// credentials. Only DroppedBaseCredentials was tested, so deleting the Warn
// from ValidateAgainst — the one seam both paths cross — or gating it on the
// wrong condition failed nothing, and the operator's only evidence became a
// transient export failure against a backend they believe is authenticated.

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"
)

// captureDefaultWarns routes the process logger to a buffer at Warn for the
// rest of the test. slog.Default is process-global, so a caller must not run
// in parallel (nothing in this package does).
func captureDefaultWarns(t *testing.T) func() []string {
	t.Helper()
	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn})))
	t.Cleanup(func() { slog.SetDefault(prev) })
	return func() []string {
		var lines []string
		for l := range strings.SplitSeq(strings.TrimSpace(buf.String()), "\n") {
			if l != "" {
				lines = append(lines, l)
			}
		}
		return lines
	}
}

func TestValidateAgainstWarnsWhenAnOwnEndpointDropsBaseCredentials(t *testing.T) {
	base := Config{Endpoint: "collector.monitoring:4317", Protocol: "grpc",
		BearerTokenFile: "/var/run/otlp/token", CAFile: "/var/run/otlp/ca.pem"}
	const own = "tempo.example.com:4317"

	warns := captureDefaultWarns(t)
	cfg := &ExportConfig{Traces: &ExportOverride{Endpoint: own}}
	if err := cfg.ValidateAgainst(base); err != nil {
		t.Fatalf("ValidateAgainst: %v", err)
	}
	var hits []string
	for _, l := range warns() {
		if strings.Contains(l, "names its own endpoint") {
			hits = append(hits, l)
		}
	}
	if len(hits) != 1 {
		t.Fatalf("got %d dropped-credential warnings, want exactly 1:\n%s", len(hits), strings.Join(warns(), "\n"))
	}
	for _, want := range []string{
		"level=WARN",
		"signal=traces",
		"endpoint=" + own,
		"flag=-otlp-bearer-token-file,-otlp-tls-ca-file",
	} {
		if !strings.Contains(hits[0], want) {
			t.Errorf("the warning does not carry %q:\n%s", want, hits[0])
		}
	}
	// A credential path is named by FLAG, never by its value: the line names
	// what stopped, not where the secret lives.
	if strings.Contains(hits[0], base.BearerTokenFile) {
		t.Errorf("the warning names the token file's path:\n%s", hits[0])
	}
}

// The negative half: an override that adds headers but keeps the base's
// destination IS the base, so nothing was dropped and a warning there would
// train an operator to ignore the one that matters.
func TestValidateAgainstIsSilentWhenTheOverrideKeepsTheBaseDestination(t *testing.T) {
	base := Config{Endpoint: "collector.monitoring:4317", Protocol: "grpc",
		BearerTokenFile: "/var/run/otlp/token", CAFile: "/var/run/otlp/ca.pem"}

	warns := captureDefaultWarns(t)
	for _, cfg := range []*ExportConfig{
		{Traces: &ExportOverride{Headers: map[string]string{"X-Scope-OrgID": "tenant-a"}}},
		{Logs: &ExportOverride{Endpoint: base.Endpoint}},
	} {
		if err := cfg.ValidateAgainst(base); err != nil {
			t.Fatalf("ValidateAgainst: %v", err)
		}
	}
	if got := warns(); len(got) != 0 {
		t.Errorf("an override that keeps the base destination warned:\n%s", strings.Join(got, "\n"))
	}
}
