#!/usr/bin/env bash
# Deletes the kind test cluster created by hack/cluster-up.sh.
set -euo pipefail

CLUSTER_NAME="${CLUSTER_NAME:-kubescrape}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
export PATH="$SCRIPT_DIR/bin:$PATH"

if ! command -v kind >/dev/null; then
  echo "kind not found; nothing to tear down" >&2
  exit 1
fi

if kind get clusters 2>/dev/null | grep -qx "$CLUSTER_NAME"; then
  kind delete cluster --name "$CLUSTER_NAME"
else
  echo "kind cluster '$CLUSTER_NAME' does not exist"
fi
