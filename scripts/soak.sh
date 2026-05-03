#!/usr/bin/env bash
# Soak the broker with traffic while killing pgmqttd Pods on a fixed cadence,
# then assert the cmd/soak rig reports zero loss / dups for QoS≥1.
#
# Usage (against a kind cluster with a running pgmqtt release):
#   PGMQTT_NS=mqtt ./scripts/soak.sh 10m 1000 1
#
# Args:
#   $1  duration       (default 10m)
#   $2  rate (msg/s)   (default 1000)
#   $3  qos            (default 1)
#
# Required env:
#   PGMQTT_NS          k8s namespace where pgmqttd runs
#   PGMQTT_HOST        broker host:port (e.g. 127.0.0.1:1883 if port-forwarded)
#   PGMQTT_USER        broker username (set up via User CR before running)
#   PGMQTT_PASS        broker password
set -euo pipefail

DUR="${1:-10m}"
RATE="${2:-1000}"
QOS="${3:-1}"
NS="${PGMQTT_NS:?set PGMQTT_NS to the broker namespace}"
HOST="${PGMQTT_HOST:?set PGMQTT_HOST to broker host:port (e.g. 127.0.0.1:1883)}"
USER="${PGMQTT_USER:?set PGMQTT_USER}"
PASS="${PGMQTT_PASS:?set PGMQTT_PASS}"

# Background chaos loop — kill one random pgmqttd Pod every 30s for the run.
chaos_pid=""
chaos() {
    while true; do
        sleep 30
        target=$(kubectl -n "$NS" get pod -l app.kubernetes.io/name=pgmqtt \
                  -o jsonpath='{.items[*].metadata.name}' 2>/dev/null \
                  | tr ' ' '\n' | shuf | head -1)
        [ -z "$target" ] && continue
        echo "$(date -u +%FT%TZ) chaos: kubectl delete pod $target"
        kubectl -n "$NS" delete pod "$target" --grace-period=0 --force 2>/dev/null || true
    done
}
chaos &
chaos_pid=$!
trap '[[ -n "$chaos_pid" ]] && kill $chaos_pid 2>/dev/null || true' EXIT

# Build (or rebuild) cmd/soak alongside this script for portability.
go build -o /tmp/pgmqtt-soak ./cmd/soak

echo "$(date -u +%FT%TZ) soak: starting $DUR @ $RATE msg/s QoS$QOS against $HOST"
/tmp/pgmqtt-soak \
  -broker "$HOST" -user "$USER" -pass "$PASS" \
  -duration "$DUR" -rate "$RATE" -qos "$QOS" -subs 5 \
  -topic soak/$(date +%s)
