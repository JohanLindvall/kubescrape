# Repository guidance

kubescrape has two binaries in one image: `cmd/kubescrape` serves Kubernetes
metadata from informer caches; `cmd/kubescrape-agent` collects and exports
telemetry. The per-node agent uses the metadata service for discovery. Events
and Azure diagnostics run in a singleton Deployment; traces use a separate
StatefulSet.

## Read the guide for the area you are changing

These guides are part of the repository instructions. Read
[development and verification](docs/AGENTS-DEVELOPMENT.md) for any change, then
the relevant subsystem guides:

| Area | Guide |
|---|---|
| Build tags, toolchain, releases, generated docs, e2e and chaos | [Development](docs/AGENTS-DEVELOPMENT.md) |
| Shared helpers, configuration, attributes, logging and self-telemetry | [Shared contracts](docs/AGENTS-SHARED.md) |
| Informers, store, owners, HTTP API, monitors and scrape discovery | [Metadata service](docs/AGENTS-METADATA.md) |
| Log chain, tailer, positions, scrubbing and log-derived metrics | [Logs](docs/AGENTS-LOGS.md) |
| Prometheus parsing, splitting, kubelet and cgroup sampling | [Metrics](docs/AGENTS-METRICS.md) |
| OTLP export/ingest, buffering, routing, transforms and debug access | [Transport](docs/AGENTS-TRANSPORT.md) |
| Journald, Kubernetes events and Azure diagnostics | [Optional pipelines](docs/AGENTS-OPTIONAL.md) |
| Trace sharding, service graph, span metrics and sampling | [Traces](docs/AGENTS-TRACES.md) |

## Rules that apply everywhere

- Preserve attribution and delivery contracts. Explain and count refusals,
  degradation and data loss; do not turn a failed dependency into silent success.
- Do not add `recover()`. Fix panics at the parsing or conversion boundary.
- Reuse the shared primitives listed in the shared guide instead of adding
  another implementation of the same decision.
- Public packages under `pkg/` must never import `internal/`.
- New source files need the MIT SPDX identifier; generated documentation must
  be regenerated through its owning test.
- Keep hot paths allocation-disciplined. Benchmarks use `b.Loop()`; allocation
  budgets also need `testing.AllocsPerRun` tests and skip under `-race` through
  `internal/testrace.Enabled`.
- Run `make race` for concurrency changes and verify informer, store or HTTP
  API changes against a real cluster.
- Document accepted security residuals at the code and, when an operator can
  act on them, in [the configuration roster](docs/CONFIGURATION.md#accepted-security-residuals).
- Keep this guidance about current contracts and where to verify them. Put
  change history in commits and release notes, rather than these guides.

The default log-scrubbing set is `bearer`, `basic-auth`, `secret-kv`, `aws-key`,
`private-key` and `url-userinfo`. `email` and `credit-card` are opt-in.
`TestDefaultSetIsDocumented` keeps this enumeration, the README and the
configuration guide aligned with `logscrub.defaultSet`.

## Common commands

```sh
make build             # both binaries; default tags: journald,azure,events
make check             # formatting, vet, lint, chart lint, tests, tag checks
make race              # whole tree with the shipped tags
make e2e               # local kind smoke test; leaves the cluster running
make cluster-down      # remove that cluster
```

Use `make` or pass build tags explicitly: bare `go build` and `go test` do not
select the optional agent pipelines. Operator documentation starts at
[README.md](README.md) and [docs/FIRST-RUN.md](docs/FIRST-RUN.md).
