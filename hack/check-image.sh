#!/usr/bin/env bash
# SPDX-License-Identifier: MIT

# Starts both binaries of an image inside its own runtime base and checks the
# version they report: hack/check-image.sh <image-ref> <version>.
#
# CI runs it on every image it builds and the release workflow on every image it
# publishes. -check-config acquires nothing (no listeners, no API server, no
# kubeconfig), so it runs anywhere; the exec itself is half the point: it
# resolves the agent's shared libraries against the base image's glibc and the
# .so files the Dockerfile stages from a hand-kept list, which a base-image bump
# can outgrow without failing the build. The other half is the version: the image
# build cannot see .git, so a binary reports a version only when the build was
# given one, and an image whose binaries say "unknown" is one nobody can tie
# back to a commit.
set -euo pipefail

if [ $# -ne 2 ]; then
  echo "usage: $0 <image-ref> <version>" >&2
  exit 2
fi
img=$1
want=$2

started() {
  local out got
  echo "== $img $1"
  if ! out=$(docker run --rm --entrypoint "$1" "$img" -check-config "${@:2}" 2>&1); then
    printf '%s\n' "$out"
    echo "FAIL: $1 did not start in $img" >&2
    exit 1
  fi
  printf '%s\n' "$out"
  # The startup line's own version= field, compared whole: a substring match
  # would take v1.2.3-rc.1 for v1.2.3.
  got=$(grep -oE '(^| )version=[^ ]+' <<<"$out" | head -n 1 | sed 's/^ //' || true)
  if [ "$got" != "version=$want" ]; then
    echo "FAIL: $1 in $img reports ${got:-no version}, want version=$want" >&2
    exit 1
  fi
}

started /kubescrape
started /kubescrape-agent -node-name=check-image
