#!/usr/bin/env bash
# Shared helpers for the chaos scenarios in this directory.
#
# Every scenario runs against the SHIPPED deployment that hack/e2e.sh creates
# (deploy/*.yaml + hack/otel-collector.yaml) — no bespoke overlay — so what they
# prove is what an operator actually gets.
#
# The invariant they all check is the same one, and it is the universal one:
# NO GAP. A writer emits densely numbered lines; at-least-once delivery means
# duplicates are ALLOWED and expected, but a sequence number the writer produced
# and no pass ever delivered is data loss. Counting deliveries cannot show that;
# only the numbering can.
set -uo pipefail

CLUSTER_NAME="${CLUSTER_NAME:-kubescrape}"
KCTL=(kubectl --context "kind-$CLUSTER_NAME")
# hack/cluster-up.sh accepts docker OR podman, so the scenarios must not assume
# one: on a podman cluster a hard-coded `docker` fails, and a scenario that does
# not check the failure then draws a confidently wrong conclusion.
CRI="${CRI:-$(command -v docker >/dev/null 2>&1 && echo docker || echo podman)}"
NS=monitoring
CHAOS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CAP_DIR="${CAP_DIR:-$CHAOS_DIR/.cap}"
mkdir -p "$CAP_DIR"

say()  { echo; echo ">>> $*"; }
info() { echo "    $*"; }
fail() { echo "CHAOS FAIL: $*" >&2; exit 1; }

need_cluster() {
  "${KCTL[@]}" -n "$NS" get deploy/kubescrape >/dev/null 2>&1 || \
    fail "no kubescrape deployment in '$NS' — run 'make e2e' first to stand the stack up"
}

collector_pod() {
  "${KCTL[@]}" -n "$NS" get pods -l app=otel-collector \
    -o jsonpath='{.items[0].metadata.name}' 2>/dev/null
}

# grab <signal> — snapshot the collector's captured payloads (logs|metrics|traces).
#
# Reads through the `reader` sidecar because the collector image is distroless.
#
# It concatenates the ROTATED siblings too, oldest first. The file exporter
# rotates at max_megabytes, renaming the old file to <sig>-<timestamp>.json, and
# reading only the live file made every line written before a rotation
# unreadable — which gap_report then reports as MISSING, i.e. it accuses
# kubescrape of losing data the harness itself discarded. That is most likely
# exactly when `make chaos` is used as intended: four scenarios in a row on a
# cluster that has been up a while.
#
# NEVER truncate these files in place: the exporter holds an open fd at its own
# offset, so a truncate leaves a sparse file and every later line count lies.
# Use recycle_collector for a clean slate instead.
grab() {
  local sig="$1" pod
  pod="$(collector_pod)" || return 1
  [ -n "$pod" ] || return 1
  # `ls -1` sorts the timestamped siblings ascending, and the live file (no
  # timestamp) sorts before them, so feed it explicitly last.
  "${KCTL[@]}" -n "$NS" exec "$pod" -c reader -- sh -c \
    "for f in \$(ls -1 /data/$sig-*.json 2>/dev/null); do cat \"\$f\"; done; cat /data/$sig.json 2>/dev/null" \
    > "$CAP_DIR/$sig.json" 2>/dev/null
}

recycle_collector() {
  "${KCTL[@]}" -n "$NS" delete pod -l app=otel-collector --wait=true >/dev/null 2>&1
  "${KCTL[@]}" -n "$NS" rollout status deploy/otel-collector --timeout=180s >/dev/null 2>&1
}

# writer_pod <name> <node> <mark> <count> <sleep> — a densely numbered log writer.
writer_pod() {
  local name="$1" node="$2" mark="$3" count="$4" nap="$5"
  "${KCTL[@]}" -n default delete pod "$name" --ignore-not-found --grace-period=0 --force >/dev/null 2>&1
  cat <<EOF | "${KCTL[@]}" apply -f - >/dev/null
apiVersion: v1
kind: Pod
metadata:
  name: $name
  namespace: default
  labels: {app: chaos-writer}
spec:
  restartPolicy: Never
  nodeName: $node
  containers:
    - name: w
      image: busybox:1.36
      imagePullPolicy: IfNotPresent
      command: ["/bin/sh","-c","i=0; while [ \$i -lt $count ]; do i=\$((i+1)); echo \"$mark seq=\$i\"; sleep $nap; done; sleep 3600"]
EOF
  "${KCTL[@]}" -n default wait --for=condition=Ready "pod/$name" --timeout=120s >/dev/null
}

# gap_report <mark> <produced> — the verdict. Exits non-zero on a gap.
gap_report() {
  local mark="$1" produced="$2"
  grab logs || fail "could not read the collector capture (is the reader sidecar present?)"
  python3 - "$mark" "$produced" "$CAP_DIR/logs.json" <<'PY'
import re, sys
mark, produced, path = sys.argv[1], int(sys.argv[2]), sys.argv[3]
seen = {}
for line in open(path, errors="replace"):
    for m in re.finditer(re.escape(mark) + r" seq=(\d+)", line):
        i = int(m.group(1)); seen[i] = seen.get(i, 0) + 1
missing = [i for i in range(1, produced + 1) if i not in seen]
dupes = sum(1 for c in seen.values() if c > 1)
print(f"    produced   : {produced}")
print(f"    delivered  : {len(seen)} distinct")
print(f"    duplicates : {dupes}  (at-least-once ALLOWS these)")
print(f"    MISSING    : {len(missing)}" + (f"  first 20: {missing[:20]}" if missing else ""))
raise SystemExit(1 if missing else 0)
PY
}

# counters <substring> — latest value of each matching kubescrape_* self-metric.
counters() {
  grab metrics || return 0
  python3 - "${1:-}" "$CAP_DIR/metrics.json" <<'PY'
import json, sys
pat, path = sys.argv[1], sys.argv[2]
seen = {}
for line in open(path, errors="replace"):
    line = line.strip()
    if not line:
        continue
    try:
        d = json.loads(line)
    except Exception:
        continue
    for rm in d.get("resourceMetrics", []):
        ra = {a["key"]: list(a["value"].values())[0] for a in rm["resource"].get("attributes", [])}
        pod = ra.get("k8s.pod.name", ra.get("service.instance.id", "?"))
        for sm in rm.get("scopeMetrics", []):
            for m in sm.get("metrics", []):
                n = m["name"]
                if not n.startswith("kubescrape_") or pat not in n:
                    continue
                for t in ("gauge", "sum"):
                    if t in m:
                        for dp in m[t].get("dataPoints", []):
                            at = ",".join(f'{a["key"]}={list(a["value"].values())[0]}'
                                          for a in dp.get("attributes", []))
                            seen[(pod, n, at)] = dp.get("asInt", dp.get("asDouble"))
for (pod, n, at), v in sorted(seen.items()):
    print(f"    {pod} {n}{{{at}}} = {v}")
PY
}

cleanup_writers() {
  "${KCTL[@]}" -n default delete pod -l app=chaos-writer \
    --ignore-not-found --grace-period=0 --force >/dev/null 2>&1 || true
}
