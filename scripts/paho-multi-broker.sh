#!/usr/bin/env bash
# Run the Paho MQTT conformance suite against a 3-Pod pgmqttd kind cluster,
# exercising cross-Pod fanout via the Service VIP.
#
# Re-uses scripts/paho-conformance.py (the existing wrapper), pointed at a
# port-forward of the broker Service rather than a single Pod.
#
# Required: kind, kubectl, helm, docker, python3, the paho.mqtt.testing
# repo cloned somewhere local (--paho /path/to/repo).
set -euo pipefail

PAHO=""
NS=mqtt
CLUSTER=pgmqtt-paho-multi
REPLICAS=3
VERSION=both

while [ $# -gt 0 ]; do
    case "$1" in
        --paho) PAHO="$2"; shift 2 ;;
        --namespace) NS="$2"; shift 2 ;;
        --cluster) CLUSTER="$2"; shift 2 ;;
        --replicas) REPLICAS="$2"; shift 2 ;;
        --version) VERSION="$2"; shift 2 ;;
        *) echo "unknown arg: $1" >&2; exit 1 ;;
    esac
done

if [ -z "$PAHO" ]; then
    echo "usage: $0 --paho /path/to/paho.mqtt.testing [--cluster name] [--replicas N] [--version 3|5|both]" >&2
    exit 1
fi

cleanup() {
    [ -n "${PF_PID:-}" ] && kill "$PF_PID" 2>/dev/null || true
    kind delete cluster --name "$CLUSTER" 2>/dev/null || true
}
trap cleanup EXIT

echo "==> create kind cluster '$CLUSTER'"
kind create cluster --name "$CLUSTER" --wait 60s

echo "==> build broker image and load into kind"
docker build -t pgmqtt:multi-broker .
kind load docker-image pgmqtt:multi-broker --name "$CLUSTER"

echo "==> install postgres + broker"
kubectl create namespace "$NS"
kubectl -n "$NS" apply -f .github/ci/postgres.yaml
kubectl -n "$NS" rollout status statefulset/postgres --timeout=180s

helm install pgmqtt deploy/helm/pgmqtt \
    --namespace "$NS" \
    --set image.repository=pgmqtt \
    --set image.tag=multi-broker \
    --set image.pullPolicy=IfNotPresent \
    --set replicaCount="$REPLICAS" \
    --set "auth.allowAnonymous=true" \
    --set "limits.maxQueuedDeliveriesPerClient=0" \
    --set "limits.maxConnections=0" \
    --set "limits.maxInboundMsgsPerSec=0" \
    --set "operator.bcryptCost=4" \
    --set database.url='postgres://pgmqtt:pgmqtt@postgres.mqtt.svc:5432/pgmqtt?sslmode=disable'

kubectl -n "$NS" rollout status deployment/pgmqtt --timeout=180s

# allowAnonymous is set via --set auth.allowAnonymous=true on install — no
# post-install env-patch needed (the previous "set env + rollout" sequence
# raced with port-forward; new pods came up while the forward still pointed
# at the terminating ones and Paho saw ConnectionRefused).
echo "==> port-forward Service to local 11883 (round-robins across $REPLICAS Pods)"
kubectl -n "$NS" port-forward svc/pgmqtt 11883:1883 &
PF_PID=$!
sleep 2

echo "==> run paho conformance via the Service VIP"
python3 scripts/paho-conformance.py \
    --paho "$PAHO" \
    --host 127.0.0.1 \
    --port 11883 \
    --version "$VERSION" \
    --per-test-timeout 60 \
    > /tmp/paho-multi-results.txt
RESULT=$?

echo "==> results summary"
grep -E "PASS|FAIL|ok|FAILED" /tmp/paho-multi-results.txt || true

exit $RESULT
