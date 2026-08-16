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

say "writer: $COUNT lines of ~${PAD}B on $NODE (~$((COUNT * PAD / 1024 / 1024))MiB, so the kubelet rotates repeatedly)"
"${KCTL[@]}" -n default delete pod chaos-rot-writer --ignore-not-found --grace-period=0 --force >/dev/null 2>&1
cat <<EOF | "${KCTL[@]}" apply -f - >/dev/null
apiVersion: v1
kind: Pod
metadata: {name: chaos-rot-writer, namespace: default, labels: {app: chaos-writer}}
spec:
  restartPolicy: Never
  nodeName: $NODE
  containers:
    - name: w
      image: busybox:1.36
      imagePullPolicy: IfNotPresent
      command:
        - /bin/sh
        - -c
        - |
          pad=\$(head -c $PAD /dev/zero | tr '\\0' 'x')
          i=0
          while [ \$i -lt $COUNT ]; do
            i=\$((i+1))
            echo "$MARK seq=\$i \$pad"
            # Fast enough to rotate several times, slow enough that the tailer
            # is following a LIVE file rather than reading a finished one.
            sleep 0.02
          done
          sleep 3600
EOF
"${KCTL[@]}" -n default wait --for=condition=Ready pod/chaos-rot-writer --timeout=120s >/dev/null
info "writer ready at $(date -Iseconds)"

say "waiting for the writer to finish and the tailer to drain every segment"
sleep $((COUNT / 50 + 150))

say "did the kubelet actually rotate the log?"
# A rotated container log is the live <n>.log plus timestamped siblings; more
# than one file means at least one rename happened under the tailer.
ROTATED=$(docker exec "$NODE" sh -c \
  'find /var/log/pods -path "*chaos-rot-writer*" -name "*.log*" 2>/dev/null | wc -l' | tr -d '[:space:]')
info "container log files on the node: ${ROTATED:-0} (1 = never rotated)"
if [ "${ROTATED:-0}" -lt 2 ]; then
  fail "the log never rotated, so this run proves nothing about rotation.
    The kubelet's containerLogMaxSize is probably the 10Mi default — recreate the
    cluster so hack/kind-config.yaml's 1Mi applies (make cluster-down cluster-up),
    or raise COUNT/PAD until it crosses the threshold."
fi
info "rotations observed: $((ROTATED - 1))"

say "verdict"
if gap_report "$MARK" "$COUNT"; then
  echo
  echo "CHAOS PASS: no line lost across $((ROTATED - 1)) kubelet log rotations"
else
  echo
  fail "lines were lost across rotation"
fi
say "rotation-specific loss counters (all must be zero or absent)"
counters torn; counters prefix_lost
