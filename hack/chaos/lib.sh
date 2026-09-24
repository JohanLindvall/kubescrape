#!/usr/bin/env bash
# Shared helpers for the chaos scenarios in this directory.
#
# Every scenario runs against the SHIPPED deployment that hack/e2e.sh creates
# (deploy/kubernetes.yaml + deploy/agent.yaml + hack/otel-collector.yaml; the
# events singleton and the trace tier are not deployed) — no bespoke overlay —
# so what they prove is what an operator actually gets.
#
# The three DATA-PATH scenarios (collector-outage, agent-kill, log-rotation)
# all check the same invariant, and it is the universal one: NO GAP. A writer
# emits densely numbered lines; at-least-once delivery means duplicates are
# ALLOWED and expected, but a sequence number the writer produced and no pass
# ever delivered is data loss. Counting deliveries cannot show that; only the
# numbering can. apiserver-blackhole.sh moves no data, so it runs no writer and
# asserts something else: that /readyz LATCHES at 200 through the outage, that
# kubescrape_apiserver_probe_failures_total moves, and that nothing restarts.
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

# writer_pod <name> <node> <mark> <count> <sleep> [pad] — a densely numbered
# log writer: <count> lines "<mark> seq=<i>", one per <sleep> seconds, each
# followed by a space and <pad> bytes of padding when pad > 0 (log-rotation.sh
# uses that to cross the kubelet's rotation threshold).
writer_pod() {
  local name="$1" node="$2" mark="$3" count="$4" nap="$5" pad="${6:-0}"
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
      command:
        - /bin/sh
        - -c
        - |
          padding=""
          [ $pad -gt 0 ] && padding=" \$(head -c $pad /dev/zero | tr '\\0' 'x')"
          i=0
          while [ \$i -lt $count ]; do
            i=\$((i+1))
            echo "$mark seq=\$i\$padding"
            sleep $nap
          done
          sleep 3600
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

# The captured self-metrics are read by ONE walk, hack/chaos/otlpcap.py. The
# two helpers below used to carry their own copies of it, which had already
# drifted (a missing value read as None in one and 0 in the other); see that
# file for what a SERIES is and why an agent is keyed by service.instance.id.
OTLPCAP=(python3 "$CHAOS_DIR/otlpcap.py")

# metrics_capture [capture] — the path of the metrics capture to read: the one
# given (an earlier `metrics_capture` result, when a scenario asks several
# questions of ONE moment), else a fresh `grab metrics`. Every grab re-reads
# every rotated capture through `kubectl exec` — hundreds of megabytes on a
# cluster that has been up a while — so two questions in a row should not pay
# for two.
metrics_capture() {
  if [ -n "${1:-}" ]; then echo "$1"; return 0; fi
  grab metrics || return 1
  echo "$CAP_DIR/metrics.json"
}

# counters <substring> [capture] — latest value of each matching kubescrape_*
# self-metric.
#
# It PROPAGATES a failed capture. It used to `return 0`, so a scenario whose
# collector could not be read printed nothing and read as "the counter did not
# move" — indistinguishable from the counter genuinely staying flat, on the
# scripts whose whole job is to notice a signal.
counters() {
  local cap
  cap=$(metrics_capture "${2:-}") || return 1
  "${OTLPCAP[@]}" print "$cap" "${1:-}"
}

# counter_total <metric-name> [capture] — the SUM of that metric's latest data
# points across every reporting process, as a bare integer. Prints nothing and
# returns non-zero when the collector capture cannot be read, so a caller can
# tell "no capture" from "zero", which the human-readable `counters` cannot.
#
# The collector's file exporter holds every push, so the LAST value of a series
# is its current one; a counter that has never been incremented was never
# exported at all and reads as absent, which for a total is 0.
counter_total() {
  local cap
  cap=$(metrics_capture "${2:-}") || return 1
  "${OTLPCAP[@]}" total "$cap" "$1"
}

# loss_baseline <file> <metric>... — record the named counters' current values,
# per series, BEFORE a fault. assert_losses_flat compares against it.
#
# A BASELINE and not "must be zero": the counters are cumulative over each
# agent's lifetime, and `make chaos` runs its scenarios back to back, so an
# absolute check fails every later run for something an earlier one did —
# apiserver-blackhole.sh's reason for asserting that its counter MOVED rather
# than that it is non-zero, applied the other way round.
loss_baseline() {
  local out="$1"; shift
  grab metrics || return 1
  "${OTLPCAP[@]}" snapshot "$CAP_DIR/metrics.json" "$@" > "$out"
}

# assert_losses_flat <baseline> <metric>... — fail when any series of the named
# loss counters rose since loss_baseline recorded <baseline>. The counters are
# the agent's OWN admission of loss, so one moving is a failure even when the
# gap check passed: the numbered lines are one writer's, and the counters see
# every file on every node the fault touched. It also refuses to pass
# VACUOUSLY: a capture holding no agent self-metric pushed after the baseline
# cannot say whether a counter moved (otlpcap.py exits 2 for that), and that
# fails too.
assert_losses_flat() {
  local base="$1" rc; shift
  grab metrics || fail "could not read the collector's metrics capture after the run — the loss counters cannot be compared"
  "${OTLPCAP[@]}" grew "$base" "$CAP_DIR/metrics.json" "$@"
  rc=$?
  case $rc in
    0) ;;
    1) fail "an agent counted a loss during the run (the series marked MOVED above)" ;;
    2) fail "no agent self-metrics reached the collector after the baseline, so the loss counters were not observed" ;;
    *) fail "could not compare the loss counters against $base (otlpcap.py exited $rc)" ;;
  esac
}

# restart_snapshot <label> — one sorted "name restarts" line per pod matching
# the label selector, restarts summed across the pod's containers. Take it
# BEFORE the fault and hand it to assert_no_restarts afterwards.
#
# A BASELINE and not an absolute check: the lifetime restartCount of a pod says
# nothing about THIS run. An absolute "every count is 0" false-failed on any
# cluster whose pods ever restarted (a kind node restart is enough) and could
# never be applied to the agents at all once agent-kill.sh — which raises their
# count on purpose — had run earlier in `make chaos`; it also passed vacuously
# for a pod REPLACED during the run, whose fresh count starts at 0. Comparing
# the whole snapshot catches all three: a count that moved, and a name set that
# changed.
#
# Returns non-zero (message on stderr) when nothing matches or a pod has no
# container status yet, because a baseline over a pod that is not running
# cannot say whether it restarted.
restart_snapshot() {
  local sel="$1" out snap
  out=$("${KCTL[@]}" -n "$NS" get pods -l "$sel" -o \
    jsonpath='{range .items[*]}{.metadata.name}{" "}{range .status.containerStatuses[*]}{.restartCount}{" "}{end}{"\n"}{end}' \
    2>/dev/null) || { echo "could not read restart counts for '$sel'" >&2; return 1; }
  # A pod with no containerStatuses renders as its bare name, and an empty
  # field compared as a number reads as 0 — so the non-numeric case is spelled
  # out rather than left to awk's coercion (the trap an earlier absolute check
  # here fell into, where a Pending pod read as a restart).
  snap=$(printf '%s\n' "$out" | awk 'NF {
      ok = NF > 1; n = 0
      for (i = 2; i <= NF; i++) { if ($i !~ /^[0-9]+$/) ok = 0; n += $i }
      print $1, (ok ? n : "?")
    }' | sort)
  [ -n "$snap" ] || { echo "no pods matched '$sel' — the scenario cannot have observed what it claims" >&2; return 1; }
  if printf '%s\n' "$snap" | grep -q ' ?$'; then
    echo "a pod matching '$sel' has no container status yet:" >&2
    printf '%s\n' "$snap" | grep ' ?$' | sed 's/^/    /' >&2
    return 1
  fi
  printf '%s\n' "$snap"
}

# assert_no_restarts <label> <snapshot> — the pods matching the label must be
# EXACTLY the ones restart_snapshot recorded, each with the same restart count.
# A crash-and-restart can hide a scenario's whole point: the process comes back,
# resumes, and the invariant under test passes for the wrong reason.
assert_no_restarts() {
  local sel="$1" before="$2" after
  after=$(restart_snapshot "$sel") || fail "could not re-read the restart counts for '$sel' after the run"
  printf '%s\n' "$after" | sed 's/ / restarts=/;s/^/    /'
  if [ "$before" != "$after" ]; then
    info "before -> after:"
    diff <(printf '%s\n' "$before") <(printf '%s\n' "$after") | grep '^[<>]' | sed 's/^/    /'
    fail "the pods matching '$sel' changed during the run (a restart, or a pod replaced) — the invariant under test was not exercised by one continuously running process"
  fi
}

cleanup_writers() {
  "${KCTL[@]}" -n default delete pod -l app=chaos-writer \
    --ignore-not-found --grace-period=0 --force >/dev/null 2>&1 || true
}
