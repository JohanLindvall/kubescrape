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
#   4. telemetry (logs AND metrics) reaches the collector,
#   5. the PROTOBUF scrape path converts a real Prometheus NATIVE histogram
#      into an OTLP exponential histogram — and refuses a target that serves
#      protobuf when -scrape-native-histograms is off (hack/nhexporter).
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
NH_IMAGE="${NH_IMAGE:-kubescrape-e2e/nhexporter:latest}"

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

  # The native-histogram fixture. Built here rather than vendored as a
  # published image because it must be the CURRENT client_golang: native
  # histograms only exist over the protobuf exposition, and which fields that
  # emits is a property of the library version.
  log "building and loading the native-histogram fixture"
  ( cd "$REPO_DIR" && CGO_ENABLED=0 go build -trimpath -o hack/nhexporter/nhexporter ./hack/nhexporter )
  docker build -q -t "$NH_IMAGE" "$REPO_DIR/hack/nhexporter" >/dev/null
  rm -f "$REPO_DIR/hack/nhexporter/nhexporter"
  kind load docker-image "$NH_IMAGE" --name "$CLUSTER_NAME"
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

service_home_links() {
  curl -fsS http://127.0.0.1:18080/debug | grep '/v1/explain/' >/dev/null
}
wait_until 15 "the service /debug homepage carries the explain form" service_home_links

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
  # `-c collector` because the pod also runs the `reader` sidecar the payload
  # assertions below read the file exporter through.
  "${KCTL[@]}" -n monitoring logs -l app=otel-collector -c collector --tail=-1 --since=2m 2>/dev/null | grep "$1" >/dev/null
}
wait_until 120 "collector received log records" collector_got '"log records"'
wait_until 120 "collector received metric data points" collector_got '"data points"'

log "asserting the PROTOBUF scrape path (native histograms)"
# Two targets from one fixture image: a well-behaved one that negotiates the
# format from Accept, and a MISBEHAVING one (FORCE_PROTO) that answers protobuf
# whatever was asked. The shipped agent runs WITHOUT -scrape-native-histograms,
# so the well-behaved target is scraped as text and the misbehaving one must be
# REFUSED rather than decoded — the format is the operator's choice, never the
# target's, because the protobuf decode materialises the whole message and is
# gzip-amplified.
"${KCTL[@]}" apply -f - >/dev/null <<EOF
apiVersion: v1
kind: Namespace
metadata: {name: kubescrape-nh}
---
apiVersion: apps/v1
kind: Deployment
metadata: {name: nh-negotiating, namespace: kubescrape-nh, labels: {app: nh-negotiating}}
spec:
  replicas: 1
  selector: {matchLabels: {app: nh-negotiating}}
  template:
    metadata:
      labels: {app: nh-negotiating}
      annotations:
        prometheus.io/scrape: "true"
        prometheus.io/port: "9400"
        prometheus.io/path: /metrics
    spec:
      containers:
        - name: exporter
          image: $NH_IMAGE
          imagePullPolicy: IfNotPresent
          ports: [{containerPort: 9400}]
---
apiVersion: apps/v1
kind: Deployment
metadata: {name: nh-forceproto, namespace: kubescrape-nh, labels: {app: nh-forceproto}}
spec:
  replicas: 1
  selector: {matchLabels: {app: nh-forceproto}}
  template:
    metadata:
      labels: {app: nh-forceproto}
      annotations:
        prometheus.io/scrape: "true"
        prometheus.io/port: "9400"
        prometheus.io/path: /metrics
    spec:
      containers:
        - name: exporter
          image: $NH_IMAGE
          imagePullPolicy: IfNotPresent
          env: [{name: FORCE_PROTO, value: "1"}]
          ports: [{containerPort: 9400}]
EOF
"${KCTL[@]}" -n kubescrape-nh rollout status deploy/nh-negotiating --timeout=180s
"${KCTL[@]}" -n kubescrape-nh rollout status deploy/nh-forceproto --timeout=180s

nh_node="$("${KCTL[@]}" -n kubescrape-nh get pods -l app=nh-forceproto \
  -o jsonpath='{.items[0].spec.nodeName}')"
nh_agent="$("${KCTL[@]}" -n monitoring get pods -l app=kubescrape-agent \
  --field-selector "spec.nodeName=$nh_node" -o jsonpath='{.items[0].metadata.name}')"

# Without the opt-in, the forced-protobuf target must fail the scrape VISIBLY.
#
# The WHOLE log, not the last three minutes: the per-target scrape-failure line
# is throttled (one per target per reason per five minutes — fifty broken
# targets on two hundred nodes would otherwise be 20k identical lines a
# minute), so a fixed recent window can legitimately hold none of them while the
# refusal is very much still happening.
forceproto_refused() {
  "${KCTL[@]}" -n monitoring logs "$nh_agent" --tail=-1 2>/dev/null \
    | grep "native histograms are not enabled" >/dev/null
}
wait_until 120 "a protobuf-serving target is REFUSED without -scrape-native-histograms" \
  forceproto_refused

# The well-behaved target must be unaffected: it negotiates down to text and is
# scraped normally, which is what makes the refusal above a targeted guard
# rather than collateral damage.
negotiating_scraped() {
  collector_got 'e2e_classic_latency_seconds'
}
wait_until 180 "a well-behaved target still scrapes (text) with the flag off" \
  negotiating_scraped

# Now turn the opt-in ON and prove the conversion: schema -> scale, spans and
# deltas -> dense positive buckets.
log "enabling -scrape-native-histograms and asserting the conversion"
"${KCTL[@]}" -n monitoring patch ds/kubescrape-agent --type=json \
  -p '[{"op":"add","path":"/spec/template/spec/containers/0/args/-","value":"-scrape-native-histograms"}]' >/dev/null
"${KCTL[@]}" -n monitoring rollout status ds/kubescrape-agent --timeout=240s

native_converted() {
  local pod out
  pod="$("${KCTL[@]}" -n monitoring get pods -l app=otel-collector -o jsonpath='{.items[0].metadata.name}')"
  # Capture to a variable, THEN parse: piping kubectl straight into a reader
  # that stops early makes kubectl die of SIGPIPE, and under this script's
  # `set -o pipefail` the attempt reads as a failure exactly when the data is
  # there (the same trap documented for `grep -q` in collector_got). The
  # reader below also consumes all of stdin for the same reason.
  # Bounded read. The capture grows for the whole run and reaches the
  # exporter's rotation threshold, so `cat` streamed ~97 MB per attempt through
  # the exec channel — and wait_until retries every 2s for up to 180s, i.e.
  # gigabytes per `make e2e`. The answer only needs the RECENT tail: the
  # conversion under test is re-exported every scrape interval.
  out="$("${KCTL[@]}" -n monitoring exec "$pod" -c reader -- \
    sh -c 'tail -c 8000000 /data/metrics.json' 2>/dev/null)" || return 1
  printf '%s' "$out" | python3 -c '
import json, sys
ok = False
for line in sys.stdin:                    # consume ALL of stdin, never exit early
    line = line.strip()
    if not line or "e2e_native_latency_seconds" not in line or ok:
        continue
    try:
        payload = json.loads(line)
    except Exception:
        continue
    for rm in payload.get("resourceMetrics", []):
        for sm in rm.get("scopeMetrics", []):
            for m in sm.get("metrics", []):
                if m["name"] != "e2e_native_latency_seconds" or "exponentialHistogram" not in m:
                    continue
                for dp in m["exponentialHistogram"].get("dataPoints", []):
                    # The Prometheus schema must arrive as the OTLP scale, and
                    # the span/delta encoding must have decoded into real
                    # buckets — that pair is the conversion under test.
                    if dp.get("scale") == 3 and len(dp.get("positive", {}).get("bucketCounts", [])) > 10:
                        ok = True
sys.exit(0 if ok else 1)
'
}
wait_until 180 "a NATIVE histogram converts to an OTLP exponential histogram (scale=schema, spans decoded)" \
  native_converted

# Put the DaemonSet back the way the shipped manifest has it, so a re-run starts
# from the documented configuration.
"${KCTL[@]}" apply -f "$REPO_DIR/deploy/agent.yaml" >/dev/null
"${KCTL[@]}" -n monitoring rollout status ds/kubescrape-agent --timeout=240s >/dev/null

log "e2e smoke test PASSED"
if [ "$KEEP" != 1 ]; then
  "$SCRIPT_DIR/cluster-down.sh"
else
  echo "cluster '$CLUSTER_NAME' left running (KEEP=0 tears it down; re-run fast with SKIP_BUILD=1)"
fi
