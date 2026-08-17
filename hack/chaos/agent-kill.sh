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
info "SIGKILLing $AGENT on $NODE"
# pkill inside the node beats deleting the pod: it is a genuine crash, with no
# graceful shutdown and so no final flush — which is the case the checkpoint
# exists for.
"$CRI" exec "$NODE" sh -c "pkill -9 -f kubescrape-agent" 2>/dev/null || \
  "${KCTL[@]}" -n "$NS" delete pod "$AGENT" --grace-period=0 --force >/dev/null 2>&1
info "killed at $(date -Iseconds)"

say "waiting for the DaemonSet to replace it"
sleep 10
"${KCTL[@]}" -n "$NS" rollout status ds/kubescrape-agent --timeout=240s >/dev/null 2>&1
info "back at $(date -Iseconds)"

say "letting the writer finish and the agent catch up"
sleep 150

say "verdict"
if gap_report "$MARK" "$COUNT"; then
  echo
  echo "CHAOS PASS: a SIGKILLed agent resumed from its checkpoint with no gap"
else
  echo
  fail "the agent lost lines across the kill"
fi
