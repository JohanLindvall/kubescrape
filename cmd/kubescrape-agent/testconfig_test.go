// Tests for the -test-config harness (testconfig.go).
package main

import (
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func writeFile(t *testing.T, dir, name, content string) string {
	t.Helper()
	p := filepath.Join(dir, name)
	if err := os.WriteFile(p, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

// The harness runs scrub → enrich → rules → metrics in the tailer's order:
// passing cases pass, and a deliberately wrong expectation fails the run.
func TestRunConfigTests(t *testing.T) {
	dir := t.TempDir()
	cfgPath := writeFile(t, dir, "config.yaml", `
logScrubbing:
  builtin: [defaults]
logs:
  sources:
    - name: containerd
      include: ["/var/log/containers/*.log"]
      containerd: true
  rules:
    - action: drop
      matchRegexp: ["__line__=GET /healthz"]
logMetrics:
  metrics:
    - name: errors_total
      value: "1"
      match: ["level=error"]
`)
	cfg, err := loadAgentConfig(cfgPath)
	if err != nil {
		t.Fatal(err)
	}

	pass := writeFile(t, dir, "tests.yaml", `
tests:
  - name: health checks dropped
    line: 'GET /healthz 200 OK'
    expect:
      kept: false
  - name: bearer scrubbed and error counted
    line: 'level=error msg="x" auth="Bearer abc123token"'
    expect:
      kept: true
      severity: error
      body: 'level=error msg="x" auth="Bearer [REDACTED]"'
      metrics: [errors_total]
`)
	if err := runConfigTests(*cfg, "", pass, slog.Default()); err != nil {
		t.Fatalf("passing cases failed: %v", err)
	}

	fail := writeFile(t, dir, "fail.yaml", `
tests:
  - name: wrong expectation
    line: 'GET /healthz 200 OK'
    expect:
      kept: true
`)
	err = runConfigTests(*cfg, "", fail, slog.Default())
	if err == nil || !strings.Contains(err.Error(), "1 of 1") {
		t.Fatalf("want a 1-of-1 failure, got %v", err)
	}
}
