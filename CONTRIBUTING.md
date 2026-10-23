# Contributing to kubescrape

Contributions are welcome, including bug reports, documentation fixes,
deployment examples, parser fixtures and code. You do not need to be a Go
expert to improve the project. Please follow our
[code of conduct](CODE_OF_CONDUCT.md).

## Find a starting point

- Improve an installation step that was unclear, or add a minimal example
  that would have helped you get started.
- Reproduce an [open issue](https://github.com/JohanLindvall/kubescrape/issues)
  and share the version, configuration and result.
- Browse issues labelled
  [good first issue](https://github.com/JohanLindvall/kubescrape/labels/good%20first%20issue)
  or [help wanted](https://github.com/JohanLindvall/kubescrape/labels/help%20wanted).
  If none fits, propose a small improvement of your own.

For a substantial feature or a change to delivery semantics, open a
[feature request](https://github.com/JohanLindvall/kubescrape/issues/new?template=feature_request.yml)
before investing in an implementation. Small fixes can go straight to a pull
request. [Support](SUPPORT.md) explains where to ask questions; security
vulnerabilities belong in the [private reporting channel](SECURITY.md).

## Set up a development environment

Use Linux with Git, GNU Make and **Go 1.27.1 or newer** (the enforced floor is
in [go.mod](go.mod)). The default agent build also needs a C compiler and
libsystemd headers. On Debian or Ubuntu:

```sh
sudo apt-get update
sudo apt-get install -y build-essential libsystemd-dev
```

Fork the repository on GitHub, clone your fork and create a branch from `main`:

```sh
git clone https://github.com/YOUR-USERNAME/kubescrape.git
cd kubescrape
git switch -c describe-your-change
make build
make test
```

`make` supplies the shipped `journald,azure,events` build tags. A bare
`go build` or `go test` does not select those agent pipelines; use the Makefile
unless you are deliberately testing a
[build variant](docs/REFERENCE.md#build-variants-optional-pipelines).

For the local cluster demo, also install Docker, Python 3 and curl. `make e2e`
downloads pinned `kind` and `kubectl` binaries as needed, builds the image and
checks logs and metrics end to end. It leaves the cluster running;
`make cluster-down` removes it.

## Make and verify a change

Keep a pull request focused on one problem. Explain the user-visible behavior
and include a small reproduction or regression test for a bug. Documentation
changes should be checked for correct commands, links and examples.

Use [AGENTS.md](AGENTS.md) to find the relevant subsystem guide before changing
an implementation. These guides document the architecture, invariants and
verification requirements for both humans and coding tools. In particular:

- Preserve telemetry attribution and delivery semantics. Public packages in
  `pkg/` must not import `internal/`.
- Give new source files the existing MIT SPDX header. Use `make fmt` for Go
  formatting and `b.Loop()` for benchmarks; allocation budgets also need tests.
- Update operator documentation with behavior or flag changes. Flag and metric
  reference files are generated; regeneration commands are in
  [development guide](docs/AGENTS-DEVELOPMENT.md#source-and-documentation-rules).
  Keep chart and manifest flags aligned with the binaries.

Before submitting a code change, run:

```sh
make check
```

This checks formatting, vet, lint, Helm rendering/lint, tests and build-tag
boundaries. Helm and golangci-lint are installed at the repository's pinned
versions when needed. Depending on the change, also run:

| Change | Additional verification |
|---|---|
| Shared state or concurrency | `make race` |
| Informer wiring, store or HTTP API | `make e2e` against kind |
| Dependencies or toolchain | `make vulncheck` |
| Optional pipelines or their stubs | `make build test TAGS=` |
| Container build or runtime dependencies | `make image image-static` and the [image checks](docs/AGENTS-DEVELOPMENT.md#commands-and-coverage) |

CI also exercises the tag-less build, race detector, vulnerability scan and
both container images. In your pull request, state what you ran and any checks
you could not run; do not describe an unrun check as passing.

## Submit a pull request

Push your branch to your fork and open a pull request against `main`. A draft
pull request is useful when you want feedback on an approach. Include:

- The problem, a related issue if there is one, and the resulting behavior.
- The checks you ran and their results.
- Configuration, documentation or compatibility implications, if applicable.

Avoid unrelated formatting or dependency updates. Review may ask for a
smaller change, a reproducer or clarification of a tradeoff. There is no
guaranteed response time; keeping the change small and the evidence concrete
makes review easier.

Contributions are made under the project's [MIT license](LICENSE).
