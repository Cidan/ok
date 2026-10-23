#!/usr/bin/env bash
# Chaos against a staging installation while ok load runs (docs/hosted.md, "Proof at scale, as
# built"). For a duration, every interval it deletes a running pod of ok's server or deployer, at
# random; a third of the way in it cordons and drains one zone's nodes, uncordoning them two thirds
# in; halfway it restarts the bus. Each action is logged with its time, to set against the run's
# report. KUBECTL names kubectl.
#   scripts/chaos.sh [-n namespace] [-d seconds] [-i seconds] [-b bus-namespace/statefulset]
set -euo pipefail
ns=ok duration=600 interval=30 bus=nats/nats
kubectl=${KUBECTL:-kubectl}
while getopts n:d:i:b: o; do
  case $o in
    n) ns=$OPTARG ;; d) duration=$OPTARG ;; i) interval=$OPTARG ;; b) bus=$OPTARG ;;
    *) echo "usage: $0 [-n namespace] [-d seconds] [-i seconds] [-b bus-namespace/statefulset]" >&2; exit 2 ;;
  esac
done
log() { echo "$(date -u +%FT%TZ) chaos: $*"; }
selectors=(app.kubernetes.io/component=server app.kubernetes.io/component=deploy)
drained=() zone="" restarted=""
uncordon() {
  for n in "${drained[@]}"; do
    log "uncordoning $n"
    $kubectl uncordon "$n" || log "uncordoning $n failed"
  done
  drained=()
}
trap uncordon EXIT
start=$(date +%s)
while :; do
  t=$(( $(date +%s) - start ))
  (( t >= duration )) && break
  sel=${selectors[RANDOM % ${#selectors[@]}]}
  pod=$($kubectl -n "$ns" get pods -l "$sel" --field-selector=status.phase=Running -o name | shuf -n1 || true)
  if [[ -n $pod ]]; then
    log "deleting $pod"
    $kubectl -n "$ns" delete "$pod" --wait=false || log "deleting $pod failed"
  fi
  if [[ -z $zone ]] && (( t >= duration / 3 )); then
    zone=$($kubectl get nodes -o jsonpath='{.items[*].metadata.labels.topology\.kubernetes\.io/zone}' | tr ' ' '\n' | sort -u | shuf -n1)
    log "draining zone $zone"
    for n in $($kubectl get nodes -l "topology.kubernetes.io/zone=$zone" -o name); do
      drained+=("$n")
      $kubectl cordon "$n"
      $kubectl drain "$n" --ignore-daemonsets --delete-emptydir-data --timeout=180s || log "draining $n did not finish"
    done
  fi
  if (( ${#drained[@]} > 0 )) && (( t >= 2 * duration / 3 )); then
    uncordon
  fi
  if [[ -z $restarted ]] && (( t >= duration / 2 )); then
    restarted=1
    log "restarting the bus, ${bus}"
    $kubectl -n "${bus%%/*}" rollout restart "statefulset/${bus#*/}" || log "restarting the bus failed"
  fi
  sleep "$interval"
done
log "done after ${duration}s"
