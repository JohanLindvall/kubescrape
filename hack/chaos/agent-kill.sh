#!/usr/bin/env bash
# CHAOS: SIGKILL the agent mid-stream.
#
# What must hold: log offsets are checkpointed, so the replacement resumes where
# the dead one stopped. Duplicates are expected — the checkpoint lags the read
# position by design, and everything after it is re-read — but a GAP would mean
# the checkpoint advanced past data that never shipped.
set -uo pipefail
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"
need_cluster
trap cleanup_writers EXIT

MARK="CHAOSKILL$(date +%s)"
NODE="${NODE:-$("${KCTL[@]}" get nodes -o jsonpath='{.items[1].metadata.name}')}"
COUNT="${COUNT:-300}"

say "writer: $COUNT numbered lines on $NODE (4/s)"
writer_pod chaos-kill-writer "$NODE" "$MARK" "$COUNT" 0.25
info "writer ready at $(date -Iseconds)"

say "letting it ship part of the stream, then killing the agent"
sleep 25
AGENT=$("${KCTL[@]}" -n "$NS" get pods -l app=kubescrape-agent \
  --field-selector "spec.nodeName=$NODE" -o jsonpath='{.items[0].metadata.name}')
RESTARTS_BEFORE=$("${KCTL[@]}" -n "$NS" get pod "$AGENT" \
  -o jsonpath='{.status.containerStatuses[0].restartCount}' 2>/dev/null)
info "SIGKILLing $AGENT on $NODE (restartCount=$RESTARTS_BEFORE)"
# A kill inside the node beats deleting the pod: it is a genuine crash, with no
# graceful shutdown and so no final flush — which is the case the checkpoint
# exists for, and the kubelet restarts the container IN PLACE (same pod, same
# sandbox, restartCount+1) rather than scheduling a replacement.
#
# The PID is resolved through the node's CRI, from THIS pod's `agent` container,
# and killed by number. Two name-based spellings got this wrong, in opposite
# directions, and both are worth remembering:
#
#   - `sh -c "pkill -9 -f kubescrape-agent"`: kind's node image is Debian-based,
#     so /bin/sh is dash, and dash does NOT exec-optimize `sh -c` — the forked
#     shell's own command line matched `-f`, pkill (which exempts only ITSELF)
#     killed the agent and then its own parent, `$CRI exec` returned 137, the
#     `||` fallback force-deleted the pod, and every run took the REPLACEMENT-POD
#     path instead of the in-place restart claimed above (both re-read the
#     hostPath positions file, so the no-gap verdict passed either way and hid
#     it).
#   - `pkill -9 -x kubescrape-agent`: `-x` matches the kernel's `comm`, which is
#     cut to 15 bytes (TASK_COMM_LEN) — the agent's reads `kubescrape-agen` —
#     so a 16-byte name can never match (procps-ng says so and exits 1), and the
#     scenario failed on every run before it killed anything.
#
# A name cannot say WHICH agent either: an events singleton or a trace-tier
# shard scheduled on this node runs the same binary, and a kill that also took
# one of those would crash a pod this scenario never looks at. The container
# PID names exactly one process, and `$CRI exec` runs `kill` directly, with no
# wrapper shell to match or to die.
crictl_node() { "$CRI" exec "$NODE" crictl "$@" 2>/dev/null; }
POD_ID=$(crictl_node pods -q --name "^${AGENT}\$" --namespace "^${NS}\$" --state ready)
[ "$(printf '%s' "$POD_ID" | grep -c .)" = 1 ] || \
  fail "crictl on $NODE found $(printf '%s' "$POD_ID" | grep -c .) ready sandboxes for $NS/$AGENT (want exactly 1) — cannot resolve the process to kill"
CTR_ID=$(crictl_node ps -q --pod "$POD_ID" --name '^agent$' --state running)
[ "$(printf '%s' "$CTR_ID" | grep -c .)" = 1 ] || \
  fail "crictl on $NODE found no single running 'agent' container in $AGENT's sandbox — cannot resolve the process to kill"
AGENT_PID=$(crictl_node inspect --output go-template --template '{{.info.pid}}' "$CTR_ID")
[[ "$AGENT_PID" =~ ^[1-9][0-9]*$ ]] || \
  fail "crictl inspect on $NODE returned no PID for $AGENT's agent container (got '$AGENT_PID')"
# Belt and braces: the PID must BE the agent binary, not a wrapper or a shim.
AGENT_CMD=$("$CRI" exec "$NODE" cat "/proc/$AGENT_PID/cmdline" 2>/dev/null | tr '\0' ' ')
case "$AGENT_CMD" in
  /kubescrape-agent\ *) ;;
  *) fail "PID $AGENT_PID on $NODE is not the agent (cmdline '${AGENT_CMD:0:80}') — refusing to kill it" ;;
esac
"$CRI" exec "$NODE" kill -9 "$AGENT_PID"
KILL_RC=$?
[ "$KILL_RC" = 0 ] || fail "kill -9 $AGENT_PID ($AGENT's agent container) on $NODE exited $KILL_RC — nothing was killed, so this run would prove nothing"
info "killed at $(date -Iseconds)"

say "waiting for the kubelet to restart the container in place"
# WHICH recovery path ran is part of the claim, not a detail: a force-deleted
# pod comes back on a fresh sandbox, which exercises startup-from-checkpoint but
# not the kubelet's in-place container restart. Poll for the restartCount rather
# than asserting once after `rollout status`: the readiness probe has its own
# period, so the DaemonSet can still report available for a second or two after
# the process is already gone.
RESTARTS_AFTER="${RESTARTS_BEFORE:-0}"
for _ in $(seq 1 60); do
  RESTARTS_AFTER=$("${KCTL[@]}" -n "$NS" get pod "$AGENT" \
    -o jsonpath='{.status.containerStatuses[0].restartCount}' 2>/dev/null)
  [ "${RESTARTS_AFTER:-0}" -gt "${RESTARTS_BEFORE:-0}" ] && break
  sleep 2
done
"${KCTL[@]}" -n "$NS" get pod "$AGENT" >/dev/null 2>&1 || \
  fail "$AGENT is gone — the kill took the pod with it instead of the container, so the in-place crash-restart path was not exercised"
[ "${RESTARTS_AFTER:-0}" -gt "${RESTARTS_BEFORE:-0}" ] || \
  fail "$AGENT's restartCount did not move (${RESTARTS_BEFORE:-0} -> ${RESTARTS_AFTER:-0}) within 120s — the agent was never killed, so a no-gap verdict below would be vacuous"
"${KCTL[@]}" -n "$NS" rollout status ds/kubescrape-agent --timeout=240s >/dev/null 2>&1
info "back at $(date -Iseconds) — same pod $AGENT, restartCount ${RESTARTS_BEFORE:-0} -> ${RESTARTS_AFTER:-0}"

say "letting the writer finish and the agent catch up"
sleep 150

say "verdict"
if gap_report "$MARK" "$COUNT"; then
  echo
  echo "CHAOS PASS: a SIGKILLed agent was restarted in place by the kubelet (restartCount ${RESTARTS_BEFORE:-0} -> ${RESTARTS_AFTER:-0}) and resumed from its checkpoint with no gap"
else
  echo
  fail "the agent lost lines across the kill"
fi
