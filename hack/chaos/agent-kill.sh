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
# pkill inside the node beats deleting the pod: it is a genuine crash, with no
# graceful shutdown and so no final flush — which is the case the checkpoint
# exists for, and the kubelet restarts the container IN PLACE (same pod, same
# sandbox, restartCount+1) rather than scheduling a replacement.
#
# `pkill -x` and not `pkill -f`. kind's node image is Debian-based, so /bin/sh
# is dash, and dash does NOT exec-optimize `sh -c`: the forked shell is a
# distinct process whose own command line is `sh -c pkill -9 -f
# kubescrape-agent`, which `-f` matches. pkill exempts only ITSELF, so it killed
# the agent and then its own parent, `$CRI exec` returned 137, the `||` fallback
# force-deleted the pod, and every run of this scenario took the REPLACEMENT-POD
# path — never the in-place restart the comment above claims to exercise (both
# re-read the hostPath positions file, so the no-gap invariant passed either
# way and hid it). `-x` matches the executable NAME exactly, which the wrapper
# shell — named `sh` — cannot be.
"$CRI" exec "$NODE" pkill -9 -x kubescrape-agent
KILL_RC=$?
[ "$KILL_RC" = 0 ] || fail "pkill -9 -x kubescrape-agent on $NODE exited $KILL_RC — nothing was killed, so this run would prove nothing (1 = no process matched, 137 = pkill killed its own wrapper, which is the bug -x exists to avoid)"
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
