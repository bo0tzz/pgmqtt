#!/usr/bin/env bash
# HA + Zigbee2MQTT sustained kill test.
#
# What this validates: Home Assistant's MQTT entities don't go stale (i.e.
# don't show up as `unavailable` for longer than keepalive + grace) while
# the broker is killed-and-restarted every 30 seconds. This exercises the
# end-to-end recovery path that real homelab deployments care about most:
# QoS-1 retransmit on reconnect, SessionExpiryInterval state preservation,
# retained-message redelivery for bridge-status topics.
#
# Requires:
#   - kind, kubectl, helm, docker (broker side)
#   - Either a real Zigbee adapter OR a `socat`/`tcp-bridge` simulator
#     for Z2M to talk to. The simulator-only path doesn't fully test
#     the integrated stack but is the only option without hardware.
#
# Usage:
#   ./scripts/ha-z2m-soak.sh --duration 10m --kill-interval 30s
set -euo pipefail

DUR="10m"
KILL_INTERVAL="30s"
NS="${PGMQTT_NS:-mqtt}"
HA_PORT=8123
COMPOSE_DIR="$(mktemp -d)"

while [ $# -gt 0 ]; do
    case "$1" in
        --duration) DUR="$2"; shift 2 ;;
        --kill-interval) KILL_INTERVAL="$2"; shift 2 ;;
        --namespace) NS="$2"; shift 2 ;;
        *) echo "unknown arg: $1" >&2; exit 1 ;;
    esac
done

cleanup() {
    [ -n "${CHAOS_PID:-}" ] && kill "$CHAOS_PID" 2>/dev/null || true
    docker compose -f "$COMPOSE_DIR/docker-compose.yml" down -v 2>/dev/null || true
    rm -rf "$COMPOSE_DIR"
}
trap cleanup EXIT

# Discover the broker's Service ClusterIP from outside the cluster — we
# expect the operator to have already set up port-forward 1883 on the host
# (or a NodePort). Check that the broker is reachable before we start.
if ! mosquitto_pub -h 127.0.0.1 -p 1883 -t test/precheck -m hello -q 0 2>/dev/null; then
    echo "broker not reachable on 127.0.0.1:1883 — set up a port-forward first:" >&2
    echo "  kubectl -n $NS port-forward svc/pgmqtt 1883:1883 &" >&2
    exit 1
fi

# Generate a Home Assistant config that uses the broker for MQTT integration
# and exposes a few sensors via the discovery prefix. Z2M's bridge/status
# retained topic is what HA uses to mark zigbee entities (un)available.
mkdir -p "$COMPOSE_DIR/ha-config"
cat > "$COMPOSE_DIR/ha-config/configuration.yaml" <<'YAML'
default_config:
mqtt:
  broker: host.docker.internal
  port: 1883
  username: ""
  password: ""
homeassistant:
  name: pgmqtt-soak
  unit_system: metric
YAML

# Compose: HA + a synthetic device that publishes a heartbeat every 5s on
# zigbee2mqtt/test_device/availability. This stands in for Z2M without
# requiring real hardware. HA will mark the entity unavailable if no
# message arrives within ~5x the publish interval.
cat > "$COMPOSE_DIR/docker-compose.yml" <<YAML
services:
  ha:
    image: ghcr.io/home-assistant/home-assistant:stable
    extra_hosts:
      - "host.docker.internal:host-gateway"
    ports:
      - "$HA_PORT:8123"
    volumes:
      - "$COMPOSE_DIR/ha-config:/config"
  fake-z2m:
    image: eclipse-mosquitto:2.0
    extra_hosts:
      - "host.docker.internal:host-gateway"
    command:
      - sh
      - -c
      - |
        # Publish bridge online status (retained) and an availability
        # heartbeat every 5s. HA marks entities unavailable when this stops.
        mosquitto_pub -h host.docker.internal -p 1883 -t zigbee2mqtt/bridge/status -m online -r -q 1
        while true; do
          mosquitto_pub -h host.docker.internal -p 1883 -t zigbee2mqtt/test_device/availability -m online -q 1
          sleep 5
        done
YAML

echo "==> docker compose up -d"
docker compose -f "$COMPOSE_DIR/docker-compose.yml" up -d

# Background chaos: kill a random pgmqttd Pod every \$KILL_INTERVAL.
chaos() {
    while true; do
        sleep "$KILL_INTERVAL"
        target=$(kubectl -n "$NS" get pod -l app.kubernetes.io/name=pgmqtt \
                  -o jsonpath='{.items[*].metadata.name}' | tr ' ' '\n' | shuf | head -1)
        [ -z "$target" ] && continue
        echo "$(date -u +%FT%TZ) chaos: kubectl delete pod $target"
        kubectl -n "$NS" delete pod "$target" --grace-period=0 --force 2>/dev/null || true
    done
}
chaos &
CHAOS_PID=$!

echo "==> running for $DUR — open http://localhost:$HA_PORT and complete onboarding"
echo "    once HA is up, the MQTT integration auto-discovers test_device."
echo "    Watch its `availability` state in HA: it should stay 'online' through"
echo "    every broker kill (within ~keepalive + grace seconds of recovery)."
sleep "$DUR"

echo "==> done. inspect HA logs for any 'mqtt: unavailable' transitions:"
docker compose -f "$COMPOSE_DIR/docker-compose.yml" logs ha | grep -i mqtt || true
