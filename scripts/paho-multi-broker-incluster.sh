#!/usr/bin/env bash
# Run the Paho MQTT conformance suite as a Pod inside a kind cluster,
# connecting to the pgmqtt Service VIP directly. Replaces the kubectl
# port-forward path used by scripts/paho-multi-broker.sh, which dies
# mid-stream under sustained connection churn (port-forward to svc/
# stalls after ~10 connections in our reproducible kind setup).
#
# Inside-cluster ⇒ direct ClusterIP, no userland forwarder, no localhost
# hops. The Pod streams test-by-test PASS/FAIL output to stdout; we
# kubectl logs -f and capture the full transcript on success.
#
# Usage:
#   scripts/paho-multi-broker-incluster.sh \
#       --cluster pgmqtt-perf \
#       --namespace mqtt \
#       --version both
#
# Optional:
#   --paho-image  reuse a pre-built image instead of building one
#   --user U --pass P  if the cluster doesn't have allowAnonymous=true
#
# Required tooling: docker, kind, kubectl.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

CLUSTER=""
NS=""
VERSION="both"
PER_TEST_TIMEOUT="60"
PAHO_IMAGE=""
KEEP_POD=""

while [ $# -gt 0 ]; do
    case "$1" in
        -h|--help)
            sed -n '2,22p' "$0"; exit 0 ;;
        --cluster)            CLUSTER="$2"; shift 2 ;;
        --namespace)          NS="$2"; shift 2 ;;
        --version)            VERSION="$2"; shift 2 ;;
        --per-test-timeout)   PER_TEST_TIMEOUT="$2"; shift 2 ;;
        --paho-image)         PAHO_IMAGE="$2"; shift 2 ;;
        --keep-pod)           KEEP_POD=1; shift ;;
        *) echo "paho-multi-broker-incluster: unknown arg: $1" >&2; exit 2 ;;
    esac
done

if [ -z "$CLUSTER" ] || [ -z "$NS" ]; then
    echo "usage: $0 --cluster <kind-name> --namespace <ns> [--version 311|5|both]" >&2
    echo "        [--per-test-timeout 60] [--paho-image IMAGE] [--keep-pod]" >&2
    exit 2
fi

for bin in docker kind kubectl; do
    if ! command -v "$bin" >/dev/null 2>&1; then
        echo "paho-multi-broker-incluster: required tool '$bin' not found in PATH" >&2
        exit 127
    fi
done

KCTX="kind-$CLUSTER"
if ! kubectl --context "$KCTX" version >/dev/null 2>&1; then
    echo "paho-multi-broker-incluster: kube context '$KCTX' not reachable" >&2
    exit 1
fi
if ! kubectl --context "$KCTX" -n "$NS" get svc pgmqtt >/dev/null 2>&1; then
    echo "paho-multi-broker-incluster: no Service 'pgmqtt' in namespace '$NS'" >&2
    exit 1
fi

SHA="$(date +%Y%m%d%H%M%S)-$$"
POD="paho-runner-$SHA"
TS="$(date +%Y%m%d-%H%M%S)"
OUT="/tmp/paho-incluster-${TS}.log"

WORK="$(mktemp -d -t pgmqtt-paho.XXXXXX)"
BUILT_IMAGE=""
cleanup() {
    local rc=$?
    set +e
    if [ -z "$KEEP_POD" ] && kubectl --context "$KCTX" -n "$NS" get pod "$POD" >/dev/null 2>&1; then
        kubectl --context "$KCTX" -n "$NS" delete pod "$POD" \
            --grace-period=0 --force >/dev/null 2>&1 || true
    fi
    if [ -n "$BUILT_IMAGE" ]; then
        docker image rm -f "$BUILT_IMAGE" >/dev/null 2>&1 || true
    fi
    rm -rf "$WORK"
    exit $rc
}
trap cleanup EXIT INT TERM

if [ -z "$PAHO_IMAGE" ]; then
    PAHO_IMAGE="pgmqtt-paho-runner:$SHA"
    BUILT_IMAGE="$PAHO_IMAGE"

    echo "==> [1/4] build paho-runner image $PAHO_IMAGE"
    cp scripts/paho-conformance.py "$WORK/paho-conformance.py"
    cat >"$WORK/Dockerfile" <<'EOF'
FROM python:3.13-slim
RUN apt-get update \
 && apt-get install -y --no-install-recommends git ca-certificates \
 && rm -rf /var/lib/apt/lists/*
RUN git clone --depth=1 https://github.com/eclipse-paho/paho.mqtt.testing.git /paho-testing
RUN pip install --no-cache-dir paho-mqtt
COPY paho-conformance.py /paho-conformance.py
WORKDIR /
ENTRYPOINT ["python3", "/paho-conformance.py", "--paho", "/paho-testing"]
EOF
    docker build --quiet -t "$PAHO_IMAGE" "$WORK" >/dev/null

    echo "==> [2/4] kind load $PAHO_IMAGE into $CLUSTER"
    kind load docker-image "$PAHO_IMAGE" --name "$CLUSTER" >/dev/null
else
    echo "==> [1/4] reusing pre-built image $PAHO_IMAGE (assuming kind-loaded)"
fi

echo "==> [3/4] kubectl run $POD against pgmqtt.$NS.svc.cluster.local:1883"
kubectl --context "$KCTX" -n "$NS" run "$POD" \
    --image="$PAHO_IMAGE" \
    --image-pull-policy=Never \
    --restart=Never \
    --command -- python3 /paho-conformance.py \
        --paho /paho-testing \
        --host "pgmqtt.$NS.svc.cluster.local" \
        --port 1883 \
        --version "$VERSION" \
        --per-test-timeout "$PER_TEST_TIMEOUT" >/dev/null

# Wait for the Pod to start producing logs (kubectl wait Ready returns
# the moment the container starts; we want to confirm it's actually
# emitting log lines so we can stream them).
echo "==> [4/4] streaming logs"
kubectl --context "$KCTX" -n "$NS" wait --for=condition=Ready pod/"$POD" \
    --timeout=60s >/dev/null

# Tee logs to stdout AND to OUT so the operator sees real-time progress
# and we have a transcript to grep at the end.
kubectl --context "$KCTX" -n "$NS" logs -f "$POD" | tee "$OUT"

# kubectl logs -f exits when the Pod terminates. Read the exit code from
# the Pod status.
PHASE="$(kubectl --context "$KCTX" -n "$NS" get pod "$POD" -o jsonpath='{.status.phase}')"
EXIT_CODE="$(kubectl --context "$KCTX" -n "$NS" get pod "$POD" \
    -o jsonpath='{.status.containerStatuses[0].state.terminated.exitCode}' 2>/dev/null)"

echo
if [ "$PHASE" = "Succeeded" ] || [ "$EXIT_CODE" = "0" ]; then
    echo "paho-multi-broker-incluster: OK (transcript at $OUT)"
    exit 0
fi
echo "paho-multi-broker-incluster: FAILED phase=$PHASE exit=$EXIT_CODE (transcript at $OUT)" >&2
exit 1
