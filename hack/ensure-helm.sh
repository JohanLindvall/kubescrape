#!/usr/bin/env bash
# Prints the path of a usable helm binary, downloading a pinned version into
# hack/bin if none is available — the same pattern cluster-up.sh uses for kind
# and kubectl. Everything that needs helm (make helm-lint, the chart golden
# tests, make check) goes through this so CI and a fresh checkout behave the
# same as a machine that already has helm.
set -euo pipefail

HELM_VERSION="${HELM_VERSION:-v3.19.0}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BIN_DIR="$SCRIPT_DIR/bin"

if [ -x "$BIN_DIR/helm" ]; then
  echo "$BIN_DIR/helm"
  exit 0
fi
if command -v helm >/dev/null 2>&1; then
  command -v helm
  exit 0
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
curl -fsSL "https://get.helm.sh/helm-$HELM_VERSION-$os-$arch.tar.gz" | tar -xz -C "$tmp"
mv "$tmp/$os-$arch/helm" "$BIN_DIR/helm"
chmod +x "$BIN_DIR/helm"
echo "$BIN_DIR/helm"
