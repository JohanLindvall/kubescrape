#!/usr/bin/env bash
# CHAOS: the kubelet rotates a container log out from under the tailer, repeatedly.
#
# What must hold: the tailer follows a rename rotation without losing the
# rotated tail. It records the old inode as a SEGMENT, drains it, and retires it
# only once its whole range has committed — so densely numbered lines written
# across many rotations must all arrive.
#
# The test PROVES it rotated. An earlier version wrote a few hundred kilobytes
# against the kubelet's 10Mi default, rotated nothing, and passed vacuously —
# so this asserts the rotation count before it asserts the invariant, and fails
# loudly if the cluster's threshold makes the write volume a no-op.
# hack/kind-config.yaml sets containerLogMaxSize: 1Mi for exactly this reason.
set -uo pipefail
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"
need_cluster
trap cleanup_writers EXIT

MARK="CHAOSROT$(date +%s)"
NODE="${NODE:-$("${KCTL[@]}" get nodes -o jsonpath='{.items[1].metadata.name}')}"
COUNT="${COUNT:-2000}"
PAD="${PAD:-2000}"   # bytes of padding per line: COUNT*PAD must cross 1Mi several times

# The rotation-specific loss counters: a rotated-away segment given up on as
# unrecoverable, and an unterminated final line of a renamed file. Baselined
# rather than required to be zero, because they are cumulative and `make chaos`
# runs the scenarios back to back (lib.sh, loss_baseline).
LOSS_METRICS=(kubescrape_log_prefix_lost_total kubescrape_log_torn_final_lines_total)
BASELINE="$CAP_DIR/log-rotation.baseline.json"
say "loss-counter baseline"
loss_baseline "$BASELINE" "${LOSS_METRICS[@]}" || \
  fail "could not read the collector's metrics capture before the run — without a baseline the loss-counter assertion would be vacuous"

say "writer: $COUNT lines of ~${PAD}B on $NODE (~$((COUNT * PAD / 1024 / 1024))MiB, so the kubelet rotates repeatedly)"
# 0.02s a line: fast enough to rotate several times, slow enough that the
# tailer is following a LIVE file rather than reading a finished one.
writer_pod chaos-rot-writer "$NODE" "$MARK" "$COUNT" 0.02 "$PAD"
info "writer ready at $(date -Iseconds)"

say "waiting for the writer to finish and the tailer to drain every segment"
sleep $((COUNT / 50 + 150))

say "did the kubelet actually rotate the log?"
# A rotated container log is the live <n>.log plus timestamped siblings; more
# than one file means at least one rename happened under the tailer.
ROTATED=$("$CRI" exec "$NODE" sh -c \
  'find /var/log/pods -path "*chaos-rot-writer*" -name "*.log*" 2>/dev/null | wc -l' | tr -d '[:space:]')
info "container log files on the node: ${ROTATED:-0} (1 = never rotated)"
if [ "${ROTATED:-0}" -lt 2 ]; then
  fail "the log never rotated, so this run proves nothing about rotation.
    The kubelet's containerLogMaxSize is probably the 10Mi default — recreate the
    cluster so hack/kind-config.yaml's 1Mi applies (make cluster-down cluster-up).
    Do NOT simply raise COUNT/PAD to force it: past containerLogMaxFiles (5) the
    KUBELET deletes the oldest rotated file, and that real loss would be reported
    here as a kubescrape gap."
fi
info "rotations observed: $((ROTATED - 1))"

say "verdict"
gap_report "$MARK" "$COUNT" || fail "lines were lost across rotation"

say "rotation-specific loss counters (none may have moved since the baseline)"
assert_losses_flat "$BASELINE" "${LOSS_METRICS[@]}"

echo
echo "CHAOS PASS: no line lost across $((ROTATED - 1)) kubelet log rotations, and no agent counted a loss"
