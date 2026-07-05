#!/usr/bin/env bash
# Creates a three-node kind cluster (one control plane, two workers) for
# testing kubescrape and deploys sample workloads that exercise both
# endpoints (Deployment-owned pods with prometheus.io annotations, and a
# CronJob for the Job -> CronJob owner chain).
#
# kind and kubectl are downloaded into hack/bin if not already on PATH.
# Tear the cluster down again with hack/cluster-down.sh.
set -euo pipefail

CLUSTER_NAME="${CLUSTER_NAME:-kubescrape}"
KIND_VERSION="${KIND_VERSION:-v0.29.0}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BIN_DIR="$SCRIPT_DIR/bin"
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

if ! command -v kind >/dev/null; then
  echo "downloading kind $KIND_VERSION to $BIN_DIR"
  mkdir -p "$BIN_DIR"
  curl -fsSLo "$BIN_DIR/kind" "https://kind.sigs.k8s.io/dl/$KIND_VERSION/kind-$os-$arch"
  chmod +x "$BIN_DIR/kind"
fi

if ! command -v kubectl >/dev/null; then
  version="$(curl -fsSL https://dl.k8s.io/release/stable.txt)"
  echo "downloading kubectl $version to $BIN_DIR"
  mkdir -p "$BIN_DIR"
  curl -fsSLo "$BIN_DIR/kubectl" "https://dl.k8s.io/release/$version/bin/$os/$arch/kubectl"
  chmod +x "$BIN_DIR/kubectl"
fi

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
echo "  curl -s \"localhost:8080/v1/containers/\$cid\" | jq ."
