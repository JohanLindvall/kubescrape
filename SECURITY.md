# Security policy

## Reporting a vulnerability

Please **do not open a public issue** for a security problem. Report it
privately through GitHub's
[private vulnerability reporting](https://github.com/JohanLindvall/kubescrape/security/advisories/new):
the repository's **Security** tab → **Report a vulnerability**. The report is
visible only to you and the repository's maintainers until an advisory is
published.

A useful report says:

- which component is affected — the `kubescrape` metadata service, the
  `kubescrape-agent` DaemonSet, the events/Azure singleton, the trace tier,
  the Helm chart, or one of the importable `pkg/` packages — and the commit it
  was found on;
- what can reach it: which listener, route or input, and whether it needs a
  credential (several listeners are unauthenticated by design, see below);
- how to reproduce it, and what it lets an attacker do.

## Known, accepted residuals

Some holes are deliberately left open, each with its reason and what an
operator can do about it — the unauthenticated OTLP ingest listeners among
them. They are listed in
[Accepted security residuals](docs/CONFIGURATION.md#accepted-security-residuals).
A report showing that one of them is worse than that section says is welcome.

## Supported versions

There are no tagged releases yet. Fixes land on `main`; run a build of the
latest commit.

## Dependencies

CI runs [`govulncheck`](https://go.dev/doc/security/vuln/) over the shipped
build tags on every push to `main` and every pull request (`make vulncheck`),
and the Go toolchain floor in `go.mod` is set by the standard-library
advisories that check found
([Toolchain and build floor](docs/CONFIGURATION.md#toolchain-and-build-floor)).
