#!/usr/bin/env bash
# CHAOS: the API server is BLACKHOLED (not refused).
#
# `docker pause` freezes the control-plane container, so connections hang
# instead of being refused. That distinction is the whole point: client-go's
# watch-error path is SILENT for a hanging outage (the reflector never returns),
# which is why the metadata service carries its own reachability probe.
#
# What must hold:
#   * /readyz keeps answering 200 — readiness LATCHES on purpose, because an
#     unready service loses its endpoints and would cut every agent off a cache
#     that is still serving useful data,
#   * kubescrape_apiserver_probe_failures_total moves and the service logs a
#     transition WARN then a recovery INFO,
#   * nothing restarts.
#
# kubectl is unusable while paused, so a prober pod records /readyz to a
# hostPath and we read it back with `docker exec` afterwards.
set -uo pipefail
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"
need_cluster

OUTAGE="${OUTAGE:-120}"
CP="${CP:-${CLUSTER_NAME}-control-plane}"
SVCNODE=$("${KCTL[@]}" -n "$NS" get pods -l app=kubescrape -o jsonpath='{.items[0].spec.nodeName}')
SVCIP=$("${KCTL[@]}" -n "$NS" get pods -l app=kubescrape -o jsonpath='{.items[0].status.podIP}')
[ "$SVCNODE" = "$CP" ] && fail "the metadata service is ON the control plane; pausing it would pause the service too"
info "metadata service on $SVCNODE at $SVCIP; pausing $CP for ${OUTAGE}s"

cleanup() {
  docker unpause "$CP" >/dev/null 2>&1 || true
  "${KCTL[@]}" -n default delete pod chaos-readyz-prober --ignore-not-found --grace-period=0 --force >/dev/null 2>&1 || true
}
trap cleanup EXIT

say "deploying the /readyz prober (writes to a hostPath, survives the outage)"
"${KCTL[@]}" -n default delete pod chaos-readyz-prober --ignore-not-found --grace-period=0 --force >/dev/null 2>&1
cat <<EOF | "${KCTL[@]}" apply -f - >/dev/null
apiVersion: v1
kind: Pod
metadata: {name: chaos-readyz-prober, namespace: default}
spec:
  restartPolicy: Never
  nodeName: $SVCNODE
  containers:
    - name: p
      image: busybox:1.36
      imagePullPolicy: IfNotPresent
      env: [{name: TARGET, value: "$SVCIP"}]
      command:
        - /bin/sh
        - -c
        - |
          mkdir -p /probe; : > /probe/readyz.log
          while true; do
            if wget -q -T 3 -O /dev/null "http://\$TARGET:8080/readyz" 2>/dev/null; then
              echo OK >> /probe/readyz.log
            else
              echo FAIL >> /probe/readyz.log
            fi
            sleep 2
          done
      volumeMounts: [{name: probe, mountPath: /probe}]
  volumes:
    - name: probe
      hostPath: {path: /var/log/kubescrape-chaos, type: DirectoryOrCreate}
EOF
"${KCTL[@]}" -n default wait --for=condition=Ready pod/chaos-readyz-prober --timeout=120s >/dev/null
sleep 15

say "PAUSING $CP (kubectl is unusable until it is unpaused)"
docker pause "$CP" >/dev/null
sleep "$OUTAGE"
docker unpause "$CP" >/dev/null
say "UNPAUSED; letting the cluster settle"
sleep 90

say "what the prober saw"
docker exec "$SVCNODE" sh -c 'cat /var/log/kubescrape-chaos/readyz.log' | sort | uniq -c | sed 's/^/    /'
# `grep -c` prints 0 AND exits 1 when there is no match, so a `|| echo 0`
# fallback appends a SECOND zero and the comparison below sees "0\n0".
# Count with awk instead: one line of output, always exit 0.
BAD=$(docker exec "$SVCNODE" sh -c \
  "awk '/FAIL/{n++} END{print n+0}' /var/log/kubescrape-chaos/readyz.log" 2>/dev/null | tr -d '[:space:]')
OKS=$(docker exec "$SVCNODE" sh -c \
  "awk '/OK/{n++} END{print n+0}' /var/log/kubescrape-chaos/readyz.log" 2>/dev/null | tr -d '[:space:]')

say "the compensating signal"
counters apiserver
info "service log transitions:"
"${KCTL[@]}" -n "$NS" logs -l app=kubescrape --tail=-1 --since=10m 2>/dev/null \
  | grep -iE 'API server (unreachable|reachable)' | tail -4 | sed 's/^/    /'

say "restarts (must be zero)"
"${KCTL[@]}" -n "$NS" get pods -l app=kubescrape \
  -o jsonpath='{range .items[*]}{"    "}{.metadata.name}{" restarts="}{.status.containerStatuses[0].restartCount}{"\n"}{end}'

say "verdict"
[ "${OKS:-0}" -gt 0 ] || fail "the prober recorded nothing — it never reached /readyz at all"
if [ "${BAD:-1}" = "0" ]; then
  echo "CHAOS PASS: /readyz answered 200 $OKS/$OKS times across a ${OUTAGE}s API-server blackhole (readiness latched)"
else
  fail "/readyz returned non-200 $BAD times — readiness did not latch"
fi
