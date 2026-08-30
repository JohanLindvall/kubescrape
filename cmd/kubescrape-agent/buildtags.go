package main

import (
	"fmt"
	"strings"
)

// Optional-compilation build tags.
//
// Three pipelines are heavy for what they are, and none is wanted by every
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
//   - events compiles IN the Kubernetes events reader (internal/agent/events
//     plus internal/leader), and it is the big one. This binary is documented
//     as talking to no Kubernetes API — and it does not, EXCEPT here: the
//     events watch and its leader election are the only reason the agent links
//     k8s.io/client-go at all. That is 926 -> 470 dependency packages
//     (k8s.io/ + sigs.k8s.io/ 412 -> 8) and MORE THAN HALF the shipped binary
//     (59,157,640 -> 26,268,328 bytes, -32,889,312 = -31.37 MiB, -55.6%)
//     carried on every node in the fleet for a pipeline that, exactly like
//     azure, only ever runs in the one-replica singleton Deployment. The
//     argument is verbatim the one azure already makes; it is six times
//     larger. Note what does NOT shrink by 31 MiB: `make image` ships BOTH
//     binaries in one image and the metadata service legitimately links
//     client-go, so the image goes from 112.98 MB of binaries to 80.09 MB —
//     -29.1%, still every node's pull.
//
//     Re-measured 2026-08-29 on go1.26.6, -trimpath -ldflags="-s -w",
//     CGO_ENABLED=1 both arms, two byte-identical builds per arm. This comment
//     previously read 29,095,624 / -50.8%: that is 2.84 MB larger than the tag
//     actually delivers, and it is almost exactly the +2,843,680 bytes a
//     clientcmd import in internal/cli costs (measured), i.e. it was taken
//     before the kubecfg split below. A figure that UNDERSTATES its own win is
//     still a figure that does not describe the code.
//
// THE DEFAULT IS PRESERVED BY THE BUILD, NOT BY THE CONSTRAINT. The Makefile
// carries the default tag set — `TAGS ?= journald,azure,events` — and every build,
// test, vet, lint and image target passes it, so `make build` and `make image`
// produce exactly the binaries and image they always did. The consequence is
// real and deliberate: a BARE `go build ./cmd/kubescrape-agent/` yields an
// agent with NONE of them. That is why the startup log line names the
// optional pipelines this binary contains, why -check-config reports them, and
// why enabling one that is absent is a startup error rather than silence.
//
// The mechanism is the file-level import: `startJournald`, `startAzure` and
// `startEvents` live in tagged file pairs, so a build without the tag never
// names the package and the linker never sees it. The stub half of each pair is
// one constructor returning an error — deliberately NOT a fake implementation,
// which would be a second code path to keep honest. events needs one thing
// more, and it is the reason kubecfg exists as its own package: internal/cli is
// imported by EVERY build for the logger and the shared flag blocks, so a
// KubeConfig living there would have pinned clientcmd — and with it client-go —
// into the very build the tag exists to slim.
//
// THE FLAGS DO NOT DISAPPEAR. `-journald`, `-azure-diagnostics` and `-events`
// are defined in main.go, untagged, in every build: internal/manifestcheck
// asserts that every flag the shipped manifests pass actually exists, and the
// chart passes all three — dropping a flag would break that guard for exactly the deployments
// most likely to run these pipelines, and would turn a clear refusal into
// `flag provided but not defined` + exit 2. Instead, ENABLING an absent
// pipeline is a startup error naming the tag and how to rebuild, raised from
// validateConfig so that `-check-config` catches it before a rollout does.
//
// Config PARSING is untouched by the tags: no section of the -config file is
// owned by any of the three (all are configured by flags, and the log chain
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

// optionalPipelines is the registry. journaldBuilt, azureBuilt and eventsBuilt
// come from the tagged file pairs and are the only build-dependent values here.
func optionalPipelines() []optionalPipeline {
	return []optionalPipeline{
		{journaldOn, "journald", "journald", journaldBuilt, "systemd journal reader"},
		{azureOn, "azure-diagnostics", "azure", azureBuilt, "Azure diagnostics consumer"},
		{eventsOn, "events", "events", eventsBuilt, "Kubernetes events reader"},
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
		"events":                                eventsOn,
		"events-namespace":                      eventsNamespace,
		"events-start":                          eventsStart,
		"events-batch-size":                     eventsBatch,
		"events-flush-interval":                 eventsFlush,
		"events-position-interval":              eventsPersist,
		"events-position-configmap":             eventsConfigMap,
		"events-lease":                          eventsLease,
		"events-lease-namespace":                eventsLeaseNS,
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
		"either drop -%s, or use an agent built with the tag (`make build` / `make image` default to TAGS=journald,azure,events and contain every pipeline; a bare `go build` contains none of them)",
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
