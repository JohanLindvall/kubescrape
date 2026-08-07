#!/usr/bin/env bash
# End-to-end smoke test against a kind cluster (`make e2e`).
#
# CLAUDE.md has always said informer/store/API changes should be verified
# against a real cluster; this script is that checklist as code. It builds the
# image, loads it into the kind cluster (created if absent — cluster-up.sh is
# idempotent), deploys the SHIPPED manifests (deploy/*.yaml — the copy
# internal/manifestcheck guards textually) plus the debug collector, and then
# asserts the pipeline actually works:
#
#   1. both readiness gates clear (the agent's /readyz genuinely depends on
#      the metadata service, so the DaemonSet rollout is itself an assertion),
#   2. the metadata service discovers the annotated test workloads as scrape
#      targets and resolves a live container ID,
#   3. the agent's own /debug/targets sees targets on its node,
#   4. telemetry (logs AND metrics) reaches the collector.
#
# The cluster is left running for iteration (the same trade cluster-up makes);
# KEEP=0 tears it down at the end. SKIP_BUILD=1 skips the image build+load for
# a fast re-run against the already-loaded image.
set -euo pipefail

CLUSTER_NAME="${CLUSTER_NAME:-kubescrape}"
IMAGE="${IMAGE:-ghcr.io/johanlindvall/kubescrape}"
TAG="${TAG:-latest}"
KEEP="${KEEP:-1}"
SKIP_BUILD="${SKIP_BUILD:-0}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(dirname "$SCRIPT_DIR")"
export PATH="$SCRIPT_DIR/bin:$PATH"

KCTL=(kubectl --context "kind-$CLUSTER_NAME")

log() { echo ">>> $*"; }

fail() {
  echo "FAIL: $*" >&2
  echo "--- recent events (monitoring) ---" >&2
  "${KCTL[@]}" -n monitoring get events --sort-by=.lastTimestamp 2>/dev/null | tail -15 >&2 || true
  exit 1
}

# wait_until <seconds> <description> <command...>: retry until the command
# succeeds (output discarded) or the budget runs out.
wait_until() {
  local budget="$1" desc="$2"
  shift 2
  local deadline=$((SECONDS + budget))
  until "$@" >/dev/null 2>&1; do
    if ((SECONDS >= deadline)); then
      fail "timed out after ${budget}s waiting for: $desc"
    fi
    sleep 2
  done
  log "ok: $desc"
}

PF_PIDS=()
cleanup() {
  for pid in "${PF_PIDS[@]:-}"; do kill "$pid" 2>/dev/null || true; done
}
trap cleanup EXIT

# port_forward <local-port> <target> <remote-port>
port_forward() {
  "${KCTL[@]}" -n monitoring port-forward "$2" "$1:$3" >/dev/null 2>&1 &
  PF_PIDS+=($!)
}

log "ensuring kind cluster '$CLUSTER_NAME'"
"$SCRIPT_DIR/cluster-up.sh"

if [ "$SKIP_BUILD" != 1 ]; then
  log "building and loading $IMAGE:$TAG"
  make -C "$REPO_DIR" image IMAGE="$IMAGE" TAG="$TAG"
  kind load docker-image "$IMAGE:$TAG" --name "$CLUSTER_NAME"
fi

log "deploying the collector and the shipped manifests"
# kubernetes.yaml first: it creates the monitoring namespace the other two
# manifests' resources live in, and kubectl applies files in argument order.
"${KCTL[@]}" apply -f "$REPO_DIR/deploy/kubernetes.yaml"
"${KCTL[@]}" apply -f "$SCRIPT_DIR/otel-collector.yaml" -f "$REPO_DIR/deploy/agent.yaml"

# :latest with IfNotPresent — a freshly loaded image is only picked up by new
# pods, so restart the two kubescrape workloads. NOT the collector: it runs a
# registry image no `kind load` refreshes, and a needless restart leaves a
# terminating replica around that the log assertions could read instead of the
# live one. (First deploys make these no-ops.)
"${KCTL[@]}" -n monitoring rollout restart deploy/kubescrape ds/kubescrape-agent >/dev/null

log "waiting for rollouts (the agent's readiness gates on the metadata service)"
"${KCTL[@]}" -n monitoring rollout status deploy/otel-collector --timeout=180s
"${KCTL[@]}" -n monitoring rollout status deploy/kubescrape --timeout=180s
"${KCTL[@]}" -n monitoring rollout status ds/kubescrape-agent --timeout=240s

log "asserting the metadata service's API"
port_forward 18080 deploy/kubescrape 8080
wait_until 30 "metadata service /readyz answers 200" \
  curl -fsS http://127.0.0.1:18080/readyz

demo_node="$("${KCTL[@]}" -n kubescrape-demo get pods -l app=demo-web \
  -o jsonpath='{.items[0].spec.nodeName}')"
targets_has_demo() {
  curl -fsS "http://127.0.0.1:18080/v1/nodes/$demo_node/targets" | grep '"url"' >/dev/null
}
wait_until 60 "node $demo_node serves scrape targets for the annotated demo pods" \
  targets_has_demo

cid="$("${KCTL[@]}" -n kubescrape-demo get pods -l app=demo-web \
  -o jsonpath='{.items[0].status.containerStatuses[0].containerID}')"
container_resolves() {
  curl -fsS "http://127.0.0.1:18080/v1/containers/${cid#containerd://}" | grep '"pod"' >/dev/null
}
wait_until 30 "a live container ID resolves through the store" container_resolves

demo_pod="$("${KCTL[@]}" -n kubescrape-demo get pods -l app=demo-web \
  -o jsonpath='{.items[0].metadata.name}')"
explain_answers() {
  curl -fsS "http://127.0.0.1:18080/v1/explain/kubescrape-demo/$demo_pod" \
    | grep '"scrapeable": *true' >/dev/null
}
wait_until 30 "the explain endpoint explains the demo pod" explain_answers

log "asserting an agent sees its node's targets"
agent_pod="$("${KCTL[@]}" -n monitoring get pods -l app=kubescrape-agent \
  --field-selector "spec.nodeName=$demo_node" -o jsonpath='{.items[0].metadata.name}')"
port_forward 18081 "pod/$agent_pod" 8081
wait_until 30 "agent /readyz answers 200" \
  curl -fsS http://127.0.0.1:18081/readyz
agent_sees_targets() {
  curl -fsS http://127.0.0.1:18081/debug/targets | grep '"url"' >/dev/null
}
wait_until 90 "agent /debug/targets lists this node's targets" agent_sees_targets

debug_home_links() {
  curl -fsS http://127.0.0.1:18081/debug | grep '/debug/otlp/ui' >/dev/null
}
wait_until 15 "agent /debug homepage links the debug surfaces" debug_home_links

# The live OTLP stream must deliver a real payload line (not just the banner).
# Both `|| true`s are pipefail armor: grep -m1 SIGPIPEs curl on success.
otlp_stream_delivers() {
  local line
  line="$( (timeout 20 curl -sN 'http://127.0.0.1:18081/debug/otlp?signal=logs&signal=metrics' || true) | grep -m1 '^{' || true)"
  [ -n "$line" ]
}
wait_until 60 "agent /debug/otlp streams an exported payload" otlp_stream_delivers

log "asserting telemetry reaches the collector"
collector_got() {
  # By label, not deploy/: a deployment reference picks ONE pod, and during a
  # rollout that can persistently be the terminating one, which received
  # nothing. --tail=-1 because the selector form defaults to 10 lines per
  # pod, and any fixed tail races the debug exporter's per-record output —
  # the agents feed it every log line on three nodes, which drowns the one
  # Metrics summary line per export out of a bounded window; --since=2m
  # bounds the volume instead (and asserts freshness). And grep WITHOUT -q:
  # -q exits on the first match, kubectl dies of SIGPIPE (141), and under
  # this script's `set -o pipefail` every matching attempt then reads as a
  # failure — the whole assertion times out precisely when the data is there.
  "${KCTL[@]}" -n monitoring logs -l app=otel-collector --tail=-1 --since=2m 2>/dev/null | grep "$1" >/dev/null
}
wait_until 120 "collector received log records" collector_got '"log records"'
wait_until 120 "collector received metric data points" collector_got '"data points"'

log "e2e smoke test PASSED"
if [ "$KEEP" != 1 ]; then
  "$SCRIPT_DIR/cluster-down.sh"
else
  echo "cluster '$CLUSTER_NAME' left running (KEEP=0 tears it down; re-run fast with SKIP_BUILD=1)"
fi
