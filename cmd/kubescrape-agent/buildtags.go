package main

import (
	"fmt"
	"strings"
)

// Optional-compilation build tags.
//
// Two pipelines are heavy for what they are, and neither is wanted by every
// deployment — so each is behind a build tag:
//
//   - journald compiles IN the systemd journal reader (internal/agent/journald),
//     which is the SOLE reason this binary needs CGO_ENABLED=1 and dynamic
//     linking: it links libsystemd through coreos/go-systemd/sdjournal, which is
//     also why the default image is distroless/base plus seven copied .so files
//     rather than distroless/static. WITHOUT the tag the agent is CGO-free and
//     links fully statically, and needs none of them.
//   - azure compiles IN the Azure diagnostics consumer
//     (internal/agent/azurediag), which drags eleven franz-go packages into
//     every DaemonSet image for a feature that only ever runs in a one-replica
//     Deployment and touches no Kubernetes object.
//
// THE DEFAULT IS PRESERVED BY THE BUILD, NOT BY THE CONSTRAINT. The Makefile
// carries the default tag set — `TAGS ?= journald,azure` — and every build,
// test, vet, lint and image target passes it, so `make build` and `make image`
// produce exactly the binaries and image they always did. The consequence is
// real and deliberate: a BARE `go build ./cmd/kubescrape-agent/` yields an
// agent with NEITHER pipeline. That is why the startup log line names the
// optional pipelines this binary contains, why -check-config reports them, and
// why enabling one that is absent is a startup error rather than silence.
//
// The mechanism is the file-level import: `startJournald` and `startAzure`
// live in tagged file pairs, so a build without the tag never names the
// package and the linker never sees it. The stub half of each pair is one
// constructor returning an error — deliberately NOT a fake implementation,
// which would be a second code path to keep honest.
//
// THE FLAGS DO NOT DISAPPEAR. `-journald` and `-azure-diagnostics` are defined
// in main.go, untagged, in every build: internal/manifestcheck asserts that
// every flag the shipped manifests pass actually exists, and the chart passes
// both — dropping a flag would break that guard for exactly the deployments
// most likely to run these pipelines, and would turn a clear refusal into
// `flag provided but not defined` + exit 2. Instead, ENABLING an absent
// pipeline is a startup error naming the tag and how to rebuild, raised from
// validateConfig so that `-check-config` catches it before a rollout does.
//
// Config PARSING is untouched by the tags: no section of the -config file is
// owned by either pipeline (both are configured by flags, and the log chain
// they share lives under `logs:`), so a config file is decodable by every
// build of this binary regardless of its tags. Only ENABLING fails.

// optionalPipeline is one build-tag-gated pipeline: the flag that asks for it,
// the tag that compiles it in, whether this build has it, and what it is.
type optionalPipeline struct {
	on    *bool
	flag  string
	tag   string
	built bool
	what  string
}

// optionalPipelines is the registry. journaldBuilt and azureBuilt come from
// the tagged file pairs and are the only build-dependent values here.
func optionalPipelines() []optionalPipeline {
	return []optionalPipeline{
		{journaldOn, "journald", "journald", journaldBuilt, "systemd journal reader"},
		{azureOn, "azure-diagnostics", "azure", azureBuilt, "Azure diagnostics consumer"},
	}
}

// optionalPipelineFlags is every flag belonging to a build-tag-gated pipeline,
// keyed by its name — the anchor that keeps the whole surface present in EVERY
// build.
//
// Two reasons, both real. internal/manifestcheck asserts that every flag the
// shipped manifests pass is defined, and the chart passes these whenever the
// pipeline is enabled: a variant that quietly dropped them would exit 2 with
// `flag provided but not defined` on a manifest the default build accepts —
// the opposite of the specific, actionable refusal this design promises. And
// without an anchor a variant build's `unused` linter is RIGHT that nothing
// reads -journald-units there, and the fix it proposes is deleting exactly the
// surface that must not go. Pinned by TestOptionalPipelineFlagsExist.
func optionalPipelineFlags() map[string]any {
	return map[string]any{
		"journald":                              journaldOn,
		"journald-dir":                          journaldDir,
		"journald-units":                        journaldUnits,
		"journald-batch-size":                   journaldBatch,
		"journald-max-batch-bytes":              journaldBytes,
		"journald-flush-interval":               journaldFlush,
		"azure-diagnostics":                     azureOn,
		"azure-eventhub-namespace":              azureNamespace,
		"azure-eventhub-topics":                 azureTopics,
		"azure-eventhub-group":                  azureGroup,
		"azure-eventhub-connection-string-file": azureConnFile,
		"azure-client-id":                       azureClientID,
		"azure-tenant-id":                       azureTenantID,
		"azure-start":                           azureStart,
		"azure-metric-prefix":                   azurePrefix,
	}
}

// checkExcludedPipelines refuses a flag asking for a pipeline this binary was
// not built with. Called from validateConfig — so a real start and
// -check-config refuse identically — and mirrored by each stub, which refuses
// again if it is ever reached another way.
func checkExcludedPipelines() error {
	for _, p := range optionalPipelines() {
		if *p.on && !p.built {
			return excludedPipelineError(p.flag, p.tag, p.what)
		}
	}
	return nil
}

// excludedPipelineErrorFor is excludedPipelineError with the (flag, tag, what)
// triple looked up from the registry, for the stubs: re-typing the triple
// there let a stub's wording drift from the one validateConfig raises for the
// same refusal. Pinned by TestStubErrorsMatchTheRegistry.
func excludedPipelineErrorFor(tag string) error {
	for _, p := range optionalPipelines() {
		if p.tag == tag {
			return excludedPipelineError(p.flag, p.tag, p.what)
		}
	}
	return fmt.Errorf("no optional pipeline is tagged %q", tag) // unreachable: every stub names a registered tag
}

// excludedPipelineError is the one wording, shared by validateConfig and the
// stubs. It names the tag (so the message explains itself without the reader
// knowing this feature exists) and how to get a binary that has the pipeline.
func excludedPipelineError(flagName, tag, what string) error {
	return fmt.Errorf("-%s is set, but this kubescrape-agent was built WITHOUT the %q build tag, so the %s is not compiled into it: "+
		"either drop -%s, or use an agent built with the tag (`make build` / `make image` default to TAGS=journald,azure and contain every pipeline; a bare `go build` contains neither)",
		flagName, tag, what, flagName)
}

// builtPipelines lists the optional pipelines this binary contains, for the
// startup log line and -check-config: with the tags positive, "which binary is
// this?" is a question an operator can otherwise only answer by tripping over
// a refusal.
func builtPipelines() string {
	var out []string
	for _, p := range optionalPipelines() {
		if p.built {
			out = append(out, p.tag)
		}
	}
	if len(out) == 0 {
		return "(none)"
	}
	return strings.Join(out, ",")
}
