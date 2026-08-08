#!/usr/bin/env bash
# Prints the path of a helm binary at the PINNED version, downloading it into
# hack/bin when the machine has no matching one — the same pattern cluster-up.sh
# uses for kind and kubectl. Everything that needs helm (make helm-lint, the
# chart golden tests, make check) goes through this, so a fresh checkout renders
# the chart with the same helm as everybody else.
set -euo pipefail

HELM_VERSION="${HELM_VERSION:-v3.19.0}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BIN_DIR="$SCRIPT_DIR/bin"

if [ -x "$BIN_DIR/helm" ]; then
  echo "$BIN_DIR/helm"
  exit 0
fi
# A helm already on PATH is used only when it IS the pinned version. The chart
# goldens (internal/chartcheck) differ across helm majors in whitespace details,
# so an unconditional PATH preference meant the "pin" bound only on a machine
# with no helm at all: one developer's helm records goldens the next one's helm
# reports as a diff. A mismatch falls through to the download, and only a failed
# download falls back to the PATH binary — availability is what this script is
# for, but a version-mismatch warning is worth printing when it happens.
path_helm=""
path_version=""
if command -v helm >/dev/null 2>&1; then
  path_helm="$(command -v helm)"
  path_version="$("$path_helm" version --short 2>/dev/null | cut -d+ -f1)"
  if [ "$path_version" = "$HELM_VERSION" ]; then
    echo "$path_helm"
    exit 0
  fi
  echo "helm ${path_version:-unknown} on PATH is not the pinned $HELM_VERSION" >&2
fi

os="$(uname -s | tr '[:upper:]' '[:lower:]')"
arch="$(uname -m)"
case "$arch" in
  x86_64) arch=amd64 ;;
  aarch64 | arm64) arch=arm64 ;;
  *) echo "unsupported architecture: $arch" >&2; exit 1 ;;
esac

echo "downloading helm $HELM_VERSION to $BIN_DIR" >&2
mkdir -p "$BIN_DIR"
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT
if curl -fsSL "https://get.helm.sh/helm-$HELM_VERSION-$os-$arch.tar.gz" | tar -xz -C "$tmp"; then
  mv "$tmp/$os-$arch/helm" "$BIN_DIR/helm"
  chmod +x "$BIN_DIR/helm"
  echo "$BIN_DIR/helm"
  exit 0
fi
# Offline (or get.helm.sh unreachable) with a mismatched helm on PATH: printing
# it beats refusing to run helm-lint at all — but say so, since chart goldens
# recorded with it may not reproduce.
if [ -n "$path_helm" ]; then
  echo "could not fetch helm $HELM_VERSION; using $path_helm ($path_version) — chart goldens may differ" >&2
  echo "$path_helm"
  exit 0
fi
exit 1
