#!/usr/bin/env bash
# CHAOS: the collector goes away mid-stream.
#
# What must hold: no line the writer produced is lost. Without -buffer-dir the
# tailer REWINDS to its committed offset and re-reads once the collector is
# back; with -buffer-dir the payloads spool to disk and drain on recovery.
# Either way the observable contract is the same — at-least-once, no gap — and
# this asserts that rather than the mechanism, so it is meaningful against the
# shipped manifests as well as a buffered deployment.
set -uo pipefail
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"
need_cluster
trap cleanup_writers EXIT

MARK="CHAOSCOL$(date +%s)"
NODE="${NODE:-$("${KCTL[@]}" get nodes -o jsonpath='{.items[1].metadata.name}')}"
OUTAGE="${OUTAGE:-90}"
# One line per second for exactly the outage, so "every line is produced while
# there is nowhere to deliver it" is TRUE rather than nearly-half true: at the
# old default of 200 the writer outlived the outage by 110s and most lines took
# the ordinary path.
COUNT="${COUNT:-$OUTAGE}"

# The collector goes down FIRST, and only then does the writer start. Every
# line is therefore produced while there is nowhere to deliver it, so every
# line must arrive AFTER recovery — which is the property under test, and it
# is also the only form immune to the capture reset: scaling the collector to
# zero gives the replacement a fresh emptyDir, so anything delivered before
# the outage is no longer readable and would read as a false loss.
say "taking the collector down for ${OUTAGE}s"
"${KCTL[@]}" -n "$NS" scale deploy/otel-collector --replicas=0 >/dev/null
"${KCTL[@]}" -n "$NS" wait --for=delete pod -l app=otel-collector --timeout=120s >/dev/null 2>&1
info "collector DOWN at $(date -Iseconds)"

say "writer: $COUNT numbered lines on $NODE (1/s), ALL of them during the outage"
writer_pod chaos-collector-writer "$NODE" "$MARK" "$COUNT" 1
info "writer ready at $(date -Iseconds)"
sleep "$OUTAGE"

say "restoring the collector"
"${KCTL[@]}" -n "$NS" scale deploy/otel-collector --replicas=1 >/dev/null
"${KCTL[@]}" -n "$NS" rollout status deploy/otel-collector --timeout=180s >/dev/null
info "collector UP at $(date -Iseconds)"

say "letting the writer finish and the agent catch up"
sleep $((COUNT + 60))

say "verdict"
if gap_report "$MARK" "$COUNT"; then
  echo
  echo "CHAOS PASS: no line lost across a ${OUTAGE}s collector outage"
else
  echo
  fail "lines were lost across the collector outage"
fi
say "loss counters (all must be zero or absent)"
counters permanent_dropped; counters buffer_dropped; counters buffer_full
