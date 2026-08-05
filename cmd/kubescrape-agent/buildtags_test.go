package main

import (
	"context"
	"flag"
	"strings"
	"testing"
)

// Both assertions of the build-tag design, written so ONE untagged test file
// proves it in every build: the expectation is derived from the same constants
// the production code reads, so `go test` (neither tag), `go test -tags
// journald,azure` (the Makefile default) and each single-tag build all run it
// and each checks the branch it actually has.
//
// What must hold either way: the FLAG EXISTS (internal/manifestcheck asserts
// the chart's flags do — see manifestflags_test.go — and a flag that vanished
// with a build tag would turn a diagnosable refusal into `flag provided but
// not defined` + exit 2), and enabling a pipeline that was compiled out is a
// LOUD startup error naming the tag, from validateConfig, so -check-config
// catches it before a rollout does.
func TestExcludedPipelinesRefusedByValidateConfig(t *testing.T) {
	for _, p := range optionalPipelines() {
		t.Run(p.tag, func(t *testing.T) {
			old := *p.on
			defer func() { *p.on = old }()
			*p.on = true

			err := validateConfig(agentConfig{}, "")
			if p.built {
				// This build HAS the pipeline: the flag must be accepted (the
				// -azure-* flags have their own requirements, checked by
				// validateAzureFlags, not here).
				if err != nil {
					t.Fatalf("-%s rejected in a build that contains the %s: %v", p.flag, p.what, err)
				}
				return
			}
			if err == nil {
				t.Fatalf("-%s accepted in a build compiled without the %s: the pipeline would silently collect nothing", p.flag, p.what)
			}
			// The message has to be actionable from the log line alone: which
			// flag, which tag, and that a rebuild is the fix.
			for _, want := range []string{"-" + p.flag, p.tag, "make build"} {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("refusal %q does not mention %q", err, want)
				}
			}
		})
	}
}

// The stubs raise their refusal through excludedPipelineErrorFor, which looks
// the (flag, tag, what) triple up from the registry — re-typing it in each
// stub is how one drifts from the wording validateConfig raises. This pin also
// anchors the helper in builds where both pipelines are compiled in and no
// stub exists to call it.
func TestStubErrorsMatchTheRegistry(t *testing.T) {
	for _, p := range optionalPipelines() {
		got, want := excludedPipelineErrorFor(p.tag).Error(), excludedPipelineError(p.flag, p.tag, p.what).Error()
		if got != want {
			t.Errorf("excludedPipelineErrorFor(%q) = %q, want %q", p.tag, got, want)
		}
	}
	if err := excludedPipelineErrorFor("no-such-tag"); err == nil {
		t.Error("an unregistered tag must still produce an error, never nil")
	}
}

// A build variant may drop a pipeline's CODE but never its FLAGS: the shipped
// manifests pass them (internal/manifestcheck guards that against the flag
// set), and a binary that had quietly dropped one would exit 2 with `flag
// provided but not defined` — no message, no mention of a build tag, on a
// manifest the default build accepts.
func TestOptionalPipelineFlagsExist(t *testing.T) {
	anchored := optionalPipelineFlags()
	if len(anchored) < 15 {
		t.Fatalf("only %d optional-pipeline flags anchored; the list has gone stale", len(anchored))
	}
	for name, v := range anchored {
		if v == nil {
			t.Errorf("-%s is anchored as a nil variable", name)
		}
		if flag.Lookup(name) == nil {
			t.Errorf("-%s is not defined in this build variant: enabling the pipeline would exit 2 instead of explaining itself", name)
		}
	}
}

// The start path refuses too. validateConfig runs first on every real start,
// so this is defence in depth — but a pipeline that is absent must never be
// startable by any route, and a stub that silently succeeded would be a
// second, wrong code path.
func TestExcludedPipelinesRefusedAtStart(t *testing.T) {
	p := &pipelines{}
	starts := map[string]func(context.Context) error{
		"journald":          p.startJournald,
		"azure-diagnostics": p.startAzure,
	}
	for _, op := range optionalPipelines() {
		if op.built {
			continue // starting the real thing needs the whole process
		}
		old := *op.on
		*op.on = true
		err := starts[op.flag](context.Background())
		*op.on = old
		if err == nil {
			t.Errorf("start%s succeeded in a build without it", op.what)
		}
	}
}

// Off by default, so a variant binary starts normally as long as nothing asks
// for what it lacks — that is the entire point of shipping one.
func TestUnsetExcludedPipelinesAreFine(t *testing.T) {
	if err := validateConfig(agentConfig{}, ""); err != nil {
		t.Fatalf("a default config must validate in every build variant: %v", err)
	}
	p := &pipelines{}
	if err := p.startJournald(context.Background()); err != nil {
		t.Fatalf("startJournald with -journald=false: %v", err)
	}
	if err := p.startAzure(context.Background()); err != nil {
		t.Fatalf("startAzure with -azure-diagnostics=false: %v", err)
	}
}

// The startup log line and -check-config both report which optional pipelines
// the binary contains. With the tags POSITIVE and the default carried by the
// Makefile, a bare `go build` produces an agent with neither — so this string
// is how an operator tells which binary they are looking at.
func TestBuiltPipelinesNamesTheTags(t *testing.T) {
	got := builtPipelines()
	for _, p := range optionalPipelines() {
		if p.built != strings.Contains(got, p.tag) {
			t.Errorf("builtPipelines() = %q; %s built=%v", got, p.tag, p.built)
		}
	}
	if got == "" {
		t.Error("builtPipelines() must never be empty; a tag-less build reports (none)")
	}
}

// A config file is shared across differently-built binaries (one ConfigMap,
// one chart), so no build variant may make it undecodable: no section of the
// -config file belongs to either optional pipeline, and both are configured by
// flags. Decoding is strict (UnmarshalStrict), which is exactly what would
// break if a section ever became tag-owned.
func TestConfigParsesInEveryVariant(t *testing.T) {
	const y = `
logs:
  rules:
    - action: drop
      match: ["__severity__=debug"]
logMetrics:
  metrics:
    - name: errors
      type: counter
      value: "1"
      matchRegexp: ["__line__=ERROR"]
logScrubbing:
  builtin: [defaults]
`
	cfg, err := loadAgentConfig(writeFile(t, t.TempDir(), "agent.yaml", y))
	if err != nil {
		t.Fatalf("the shared config must decode in every build variant: %v", err)
	}
	if cfg.Logs == nil || cfg.LogMetrics == nil || cfg.LogScrubbing == nil {
		t.Fatalf("sections lost in decode: %+v", cfg)
	}
	if err := validateConfig(*cfg, ""); err != nil {
		t.Fatalf("the shared config must validate in every build variant: %v", err)
	}
}
