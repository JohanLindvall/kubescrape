# Development and verification

[Repository guidance](../AGENTS.md) · [Contributor setup](../CONTRIBUTING.md)

Run commands from the repository root.

## Commands and coverage

| Command | Purpose |
|---|---|
| `make build` | Build both unstripped binaries in `bin/` for debugging |
| `make test` | Run the full test suite with the selected build tags |
| `make vet fmt tidy` | Vet, format Go source and tidy modules |
| `make lint` | Use the golangci-lint version pinned in the Makefile |
| `make race` | Run the whole tree under the race detector with default tags |
| `make vulncheck` | Scan reachable vulnerabilities with the shipped tags |
| `make image` | Build the image selected by `TAGS` |
| `make image-static` | Build with `TAGS_STATIC`; this must exclude journald |
| `make verify-tags` | Verify excluded dependencies and compile every stub |
| `make dist VERSION=vX.Y.Z` | Package chart, manifests, binaries and checksums |
| `make check` | Formatting → vet → lint → Helm lint → tests → tag verification |
| `make e2e` | Build, deploy and exercise the kind test pipeline |
| `make chaos` | Run the scenarios against the deployment left by e2e |

For a focused test, use a package and test name, for example:

```sh
go test ./internal/store/ -run TestWaitIsPerContainer -v
```

`make check` tests the default tag set and compiles the other variants' stubs.
CI also runs `make build test TAGS=`, `make race`, `make vulncheck`, and image
checks that start both binaries with `-check-config`. `make e2e` and the
minutes-long chaos scenarios are separate from these checks.

The linter runs `modernize` on production and test code; `go fix` applies its
fixes. Helm and golangci-lint are bootstrapped into `hack/bin` at the versions
the repository pins. Do not substitute an arbitrary Helm major for rendering
goldens.

## Toolchain and optional builds

Go **1.27.1** is the security and CI-parity floor, enforced by `go.mod`.
When changing it, update both Dockerfiles' `golang:` tags, the README build
requirement and [Toolchain and build floor](CONFIGURATION.md#toolchain-and-build-floor)
together. Review the selected toolchain's `doc/godebug.md` and run
`make vulncheck`. Container base tags float; a current Go binary alone does
not ensure current native libraries in cached image layers.

`journald`, `azure` and `events` are positive build tags. The Makefile owns the
default `TAGS=journald,azure,events`. Untagged imports must not pull excluded
pipelines back into `cmd/kubescrape-agent`:

- `journald` is the sole cgo dependency; excluding it selects the static image.
- Excluding `azure` removes franz-go.
- Excluding `events` removes client-go from the agent. The metadata service
  still uses client-go.

Keep the `*_enabled.go`/`*_disabled.go` boundary at the import. Excluded
pipeline flags remain registered; `checkExcludedPipelines` rejects an enabled
but uncompiled pipeline during `compileConfig`, including `-check-config`.
Stubs return an error naming the required tag, never a fake implementation.
No config-file section belongs exclusively to these tags, so shared ConfigMaps
decode on every variant. Startup and dry-run output include `optionalPipelines`.
The packages' own files are untagged, so their tests need their dependencies
even when the command is built without those pipelines.

`make verify-tags` must keep its final tag-less `go build`; dependency listing
alone does not type-check excluded stubs. `TAGS_STATIC` carrying journald must
be refused. `hack/tooling_test.go` covers image and tool-bootstrap contracts.

## Source and documentation rules

Every Go source starts with `// SPDX-License-Identifier: MIT`, followed by a
blank line before package documentation or build tags. Shell, Python,
Makefiles and Dockerfiles use `# SPDX-License-Identifier: MIT`, after the
shebang when present. `hack/TestEverySourceFileCarriesTheLicenseIdentifier`
checks tracked and new files against `LICENSE`.

The README's claims and example must match actual default behavior. Keep
operator documentation, manifests and chart values aligned with any flag or
behavior change. The agent guides live directly under `docs/` so the existing
documentation guards include them.

| Generated artifact | Regeneration |
|---|---|
| `docs/FLAGS.md`, per binary | `go test ./cmd/<binary> -run TestFlagsDocIsCurrent -update-flags-doc` |
| `docs/METRICS.md` | `go test ./internal/obs/ -run TestMetricsDocIsCurrent -update-metrics-doc` |
| Rendered chart goldens | `go test ./internal/chartcheck -run TestChartGolden -update-chart-golden` |

`TestFlagsAreMentionedInConfigurationDoc` requires every registered flag in
CONFIGURATION.md. `internal/docscheck` rejects table rows for unregistered
flags. `TestDocumentedMetricsExist` checks documented metric and label names.
Chart tests require the pinned Helm under CI; without it they skip locally.

Use injectable clocks instead of sleeps in store/cache tests. Use `t.Parallel`
only when a package has no shared mutable state: command flags and the metrics
test clock are global, and allocation assertions must not run concurrently.
Quote allocation counts when documenting performance, not machine-dependent
timings or warm-path B/op. Charge trailing work inside the benchmark timer;
`b.Loop()` stops it on exit. Never widen allocation ceilings to accommodate
the race detector.

## Cluster and chaos verification

Informer, store and HTTP API changes require a real-cluster check. `make e2e`
builds and loads the image, deploys `deploy/kubernetes.yaml` and `deploy/agent.yaml`
plus the collector fixture, and verifies readiness, discovery, container
lookup, delivered logs/metrics and protobuf histogram conversion. It does not
deploy the events singleton or trace tier. `KEEP=0` tears down afterwards;
`SKIP_BUILD=1` reuses the loaded image.

For interactive work, use `make cluster-up`, then `go run ./cmd/kubescrape`.
The three-node kind cluster has annotated pods, Service-selected pods with
named ports, CronJobs, StatefulSets and DaemonSets. Tools are pinned by the
cluster scripts; `make cluster-down` removes the cluster.

The collector fixture captures each signal to `/data/<signal>.json`; read it
with `kubectl exec -c reader -- cat ...`. Debug verbosity `normal` is required
by e2e's assertions, but resource-attribute values must be checked in captured
payloads. Do not truncate the exporter's open files; recreate the collector
pod for a clean capture. Read the whole stream before filtering to avoid
SIGPIPE under `pipefail`. Exclude the collector namespace from agent log
collection, and allow for connections to terminating collector pods during
rollout.

`hack/nhexporter` supplies native and classic histograms, exemplars, gauges
and summaries. `FORCE_PROTO=1` exercises refusal when native histograms were
not enabled. Test both normal negotiation and operator-controlled refusal.

Data-path chaos tests assert no gaps in numbered records; duplicates are
allowed under at-least-once delivery. Verify that the intended fault occurred:
agent SIGKILL must preserve pod identity and increase restartCount; the outage
writer starts after the collector is down; rotation must be observed before
checking delivery. Use `hack/kind-config.yaml` for the rotation size limit.
Collector-outage and rotation scenarios also check flat loss counters; the
API-server outage scenario checks latched readiness and probe failures without
restarts.

## Releases

A release is an annotated `vMAJOR.MINOR.PATCH[-pre]` tag on a commit with green
CI. Preserve Markdown headings in its release notes:

```sh
git tag -a --cleanup=verbatim -F notes.md vX.Y.Z
git push origin vX.Y.Z
```

The release workflow builds native amd64 and arm64 images, verifies each with
`hack/check-image.sh`, joins the digests, packages assets through `make dist`
and creates the GitHub release. Prereleases move neither `latest` nor the
release badge. Keep `main`'s chart appVersion and deployment images on `latest`;
packaging stamps the release version. Bump the README install URLs for a new
release.

Both binaries receive the version through `-X internal/obs.Version`, with the
full module path in the actual linker argument. Local build/image targets
derive `VERSION` from git unless supplied; Docker cannot read `.git`.
`hack/release_test.go` checks version propagation and asset packaging.
