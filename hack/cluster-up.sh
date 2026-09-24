#!/usr/bin/env bash
# Creates a three-node kind cluster (one control plane, two workers) for
# testing kubescrape and deploys sample workloads that exercise both
# endpoints (Deployment-owned pods with prometheus.io annotations, and a
# CronJob for the Job -> CronJob owner chain).
#
# kind and kubectl are downloaded into hack/bin unless one at the PINNED version
# is already there or on PATH (the version check is the point — see below).
# Tear the cluster down again with hack/cluster-down.sh.
set -euo pipefail

CLUSTER_NAME="${CLUSTER_NAME:-kubescrape}"
# kind's version picks its default node image, and hack/kind-config.yaml names
# no image, so THIS pin is what decides the Kubernetes version every e2e and
# chaos run is proven against.
KIND_VERSION="${KIND_VERSION:-v0.29.0}"
# The Kubernetes version kind $KIND_VERSION's default node image runs. kubectl
# is supported within one minor of the API server, so a kubectl anywhere in
# that window is accepted; outside it one is downloaded. (It used to follow
# dl.k8s.io/release/stable.txt, a moving target that drifted several minors
# past the cluster it drives.)
KUBECTL_VERSION="${KUBECTL_VERSION:-v1.33.1}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BIN_DIR="$SCRIPT_DIR/bin"
ORIG_PATH="$PATH"
export PATH="$BIN_DIR:$PATH"

os="$(uname -s | tr '[:upper:]' '[:lower:]')"
arch="$(uname -m)"
case "$arch" in
  x86_64) arch=amd64 ;;
  aarch64 | arm64) arch=arm64 ;;
  *) echo "unsupported architecture: $arch" >&2; exit 1 ;;
esac

if ! command -v docker >/dev/null && ! command -v podman >/dev/null; then
  echo "error: kind needs docker or podman" >&2
  exit 1
fi

# The probes cannot fail, by construction (`|| true`): under errexit and
# pipefail, ensure_tool's `have="$(...)"` would otherwise take the status of a
# tool that cannot run its version subcommand — a wrong-architecture binary, a
# version-manager shim with no version selected — and end the script with 126
# and no message, where an unrunnable tool must read as "unknown" and take the
# download path like any other mismatch.
kind_version_of() { "$1" version 2>/dev/null | awk '{print $2}' || true; }
kind_ok() { [ "$1" = "$KIND_VERSION" ]; }
kubectl_version_of() { "$1" version --client 2>/dev/null | sed -n 's/^Client Version: //p' || true; }
# Same major, and a minor within one of the pinned one (kubectl's skew policy).
kubectl_ok() {
  local want="${KUBECTL_VERSION#v}" have="${1#v}"
  [ "${have%%.*}" = "${want%%.*}" ] || return 1
  local wm hm
  wm="$(echo "$want" | cut -d. -f2)"; hm="$(echo "$have" | cut -d. -f2)"
  case "$hm" in '' | *[!0-9]*) return 1 ;; esac
  [ $((hm - wm)) -le 1 ] && [ $((wm - hm)) -le 1 ]
}

# ensure_tool <name> <want> <version-of> <acceptable> <url>
#
# The version is checked on EVERY run, not only when the binary is absent —
# the bug hack/ensure-helm.sh and the Makefile's lint target already fixed for
# their tools: an existence check makes a pin bind only on the first download,
# so a kind fetched once (or a developer's own on PATH) kept deciding the
# cluster's Kubernetes version long after KIND_VERSION moved. Order: a matching
# hack/bin copy, then a matching one on the caller's PATH, then a download into
# hack/bin. Only a FAILED download falls back to a mismatched binary, with a
# warning: availability is the point of this script, but a silent substitute
# is not.
ensure_tool() {
  local name="$1" want="$2" version_of="$3" acceptable="$4" url="$5" have path_bin tmp fallback
  if [ -x "$BIN_DIR/$name" ]; then
    have="$("$version_of" "$BIN_DIR/$name")"
    if "$acceptable" "$have"; then return 0; fi
    echo "$name ${have:-unknown} in $BIN_DIR is not the pinned $want"
  fi
  path_bin="$(PATH="$ORIG_PATH" command -v "$name" 2>/dev/null || true)"
  if [ -n "$path_bin" ]; then
    have="$("$version_of" "$path_bin")"
    if "$acceptable" "$have"; then
      # hack/bin precedes PATH in this script, so a stale copy there would
      # shadow the matching one.
      rm -f "$BIN_DIR/$name"
      return 0
    fi
    echo "$name ${have:-unknown} on PATH is not the pinned $want"
  fi
  echo "downloading $name $want to $BIN_DIR"
  mkdir -p "$BIN_DIR"
  tmp="$(mktemp "$BIN_DIR/.$name.XXXXXX")"
  if curl -fsSLo "$tmp" "$url"; then
    chmod +x "$tmp"
    mv "$tmp" "$BIN_DIR/$name"
    return 0
  fi
  rm -f "$tmp"
  fallback="$(command -v "$name" 2>/dev/null || true)"
  if [ -n "$fallback" ]; then
    echo "warning: could not download $name $want; using $fallback ($("$version_of" "$fallback"))" >&2
    return 0
  fi
  echo "error: could not download $name $want, and none is installed" >&2
  exit 1
}

ensure_tool kind "$KIND_VERSION" kind_version_of kind_ok \
  "https://kind.sigs.k8s.io/dl/$KIND_VERSION/kind-$os-$arch"
ensure_tool kubectl "$KUBECTL_VERSION" kubectl_version_of kubectl_ok \
  "https://dl.k8s.io/release/$KUBECTL_VERSION/bin/$os/$arch/kubectl"

if kind get clusters 2>/dev/null | grep -qx "$CLUSTER_NAME"; then
  echo "kind cluster '$CLUSTER_NAME' already exists; reusing it"
else
  kind create cluster \
    --name "$CLUSTER_NAME" \
    --config "$SCRIPT_DIR/kind-config.yaml" \
    --wait 180s
fi

kubectl --context "kind-$CLUSTER_NAME" apply -f "$SCRIPT_DIR/test-workloads.yaml"
kubectl --context "kind-$CLUSTER_NAME" -n kubescrape-demo rollout status deployment/demo-web --timeout=180s

echo
kubectl --context "kind-$CLUSTER_NAME" get nodes
echo
echo "Cluster '$CLUSTER_NAME' is ready. Run kubescrape against it with:"
echo
echo "  go run ./cmd/kubescrape"
echo
echo "and try, for example:"
echo
echo "  node=\$(kubectl --context kind-$CLUSTER_NAME -n kubescrape-demo get pods -o jsonpath='{.items[0].spec.nodeName}')"
echo "  curl -s \"localhost:8080/v1/nodes/\$node/targets\" | jq ."
echo
echo "  cid=\$(kubectl --context kind-$CLUSTER_NAME -n kubescrape-demo get pods -o jsonpath='{.items[0].status.containerStatuses[0].containerID}')"
echo "  curl -s \"localhost:8080/v1/containers/\${cid#containerd://}\" | jq ."
