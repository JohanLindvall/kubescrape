# Getting help

Start with the [first-run guide](docs/FIRST-RUN.md) for installation checks,
missing telemetry and attribution problems. The
[configuration guide](docs/CONFIGURATION.md), [flag reference](docs/FLAGS.md)
and [technical reference](docs/REFERENCE.md) cover the available behavior.
If you are moving from Alloy, see the
[migration guide](docs/MIGRATING-FROM-ALLOY.md).

## Ask a question or report a problem

[Search existing issues](https://github.com/JohanLindvall/kubescrape/issues)
first. If you still need help, choose the matching form:

- [Question](https://github.com/JohanLindvall/kubescrape/issues/new?template=question.yml)
  for setup, configuration or understanding behavior.
- [Bug report](https://github.com/JohanLindvall/kubescrape/issues/new?template=bug_report.yml)
  for behavior that differs from the documentation or a reproducible failure.
- [Feature request](https://github.com/JohanLindvall/kubescrape/issues/new?template=feature_request.yml)
  for a use case the current project does not cover.

Include the release tag or commit, image tag/digest, Kubernetes version,
installation method, and the smallest configuration and reproduction that
show the problem. Both binaries print their version and effective settings
at startup; the first-run guide explains how to inspect those logs.

Remove tokens, passwords, connection strings and private log contents before
posting. Share only the relevant excerpt, rather than a full cluster dump.
Questions and reports are public and answered as maintainer or community time
allows; there is no guaranteed response time. Only the latest release is
supported, as described in the [security policy](SECURITY.md).

## Private reports

Report security vulnerabilities through
[GitHub private vulnerability reporting](https://github.com/JohanLindvall/kubescrape/security/advisories/new),
following [SECURITY.md](SECURITY.md). For community conduct concerns, use the
private contact in the [code of conduct](CODE_OF_CONDUCT.md#reporting-a-concern).
