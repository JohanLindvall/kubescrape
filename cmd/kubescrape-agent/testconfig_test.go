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

// Assertions about the LINE (severity, attributes, body) must hold even for a
// record the rules DROP: the harness evaluates them on a rules-free pass of
// the chain before taking the verdict, so a case can assert both "this parses
// as error" and "this is dropped" at once.
func TestRunConfigTestsAssertsOnDroppedRecords(t *testing.T) {
	dir := t.TempDir()
	cfgPath := writeFile(t, dir, "config.yaml", `
logs:
  sources:
    - name: containerd
      include: ["/var/log/containers/*.log"]
      containerd: true
  rules:
    - action: drop
      match: ["__severity__=error"]
`)
	cfg, err := loadAgentConfig(cfgPath)
	if err != nil {
		t.Fatal(err)
	}
	tests := writeFile(t, dir, "tests.yaml", `
tests:
  - name: dropped but still asserted
    line: 'level=error msg="boom"'
    expect:
      kept: false
      severity: error
      body: 'level=error msg="boom"'
`)
	if err := runConfigTests(*cfg, "", tests, slog.Default()); err != nil {
		t.Fatalf("severity/body assertions must hold for a dropped record: %v", err)
	}
}

// The harness's transform seam must be wired the way run() wires it, emitter
// included: `emit_metric` is a sanctioned verb, the harness exists as CI proof
// of a transform edit, and an unwired emitter made every case of a script using
// it fail with "no logMetrics section is configured" — naming the one section
// that IS present, for a pair a real start runs correctly.
//
// The declared rule deliberately matches NO line, so the series can only have
// been observed through the script's emit_metric and not by the per-line chain.
func TestRunConfigTestsEvaluatesEmitMetric(t *testing.T) {
	dir := t.TempDir()
	cfgPath := writeFile(t, dir, "config.yaml", `
logMetrics:
  metrics:
    - name: script_lines_total
      type: gauge
      action: inc
      match: ["__line__=no line looks like this"]
`)
	cfg, err := loadAgentConfig(cfgPath)
	if err != nil {
		t.Fatal(err)
	}
	transforms := writeFile(t, dir, "transforms.yaml", "logs: |\n"+
		"  def transform(batch):\n"+
		"      for r in batch:\n"+
		"          r.emit_metric(\"script_lines_total\", 1)\n")
	tests := writeFile(t, dir, "tests.yaml", `
tests:
  - name: plain line survives and the script's metric fires
    line: "hello world"
    expect:
      kept: true
      metrics: [script_lines_total]
`)
	if err := runConfigTests(*cfg, transforms, tests, slog.Default()); err != nil {
		t.Fatalf("emit_metric must be evaluated by -test-config, and its series assertable: %v", err)
	}
}
