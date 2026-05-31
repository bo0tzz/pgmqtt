#!/usr/bin/env bash
# Perf characterisation bench for pgmqtt.
#
# This is NOT a capacity-squeezing rig. The job is to characterise the
# broker's current behaviour — particularly the latency shape — at a
# sustainable rate well below the cliff. It exists because the dashboard
# was showing occasional p99 spikes that nobody could explain: histograms
# tell you the distribution, not the events behind the tail.
#
# What it does:
#   1. Spins up a 3-broker kind cluster (matches tier3 setup).
#   2. Deploys the broker with PGMQTT_SLOW_STAGE_LOG_MS set so any stage
#      observation slower than the threshold logs a structured event.
#   3. Runs a steady-rate soak (default 200 msg/s × 3 pubs × 3 subs at
#      QoS-1 for 5 minutes — modest, well under capacity).
#   4. Polls /metrics from every pod every $SAMPLE_SEC seconds and saves
#      the snapshot. The histograms emit cumulative buckets; per-window
#      percentile-shape is recoverable by diffing snapshots in post.
#   5. Captures broker logs at the end (all pods, full stream).
#   6. Tears down the cluster but KEEPS the artefacts under /tmp/perf-bench-*.
#
# Usage:
#   scripts/perf-bench.sh
#   scripts/perf-bench.sh --duration 10m --rate 300 --slow-ms 20
#
# Artefacts (per run, under /tmp/perf-bench-<ts>/):
#   metrics/<phase>.<pod>.txt   — /metrics snapshots
#   logs/<pod>.log              — full pod stdout/stderr
#   soak.json, soak.log         — cmd/soak's own report
#   summary.md                  — high-level findings (count of slow-stage
#                                  events, top-N slowest, deltas)
#
# Requires: kind, kubectl, helm, docker, python3, curl.

set -euo pipefail

DURATION="5m"
RATE="200"
PUBS="3"
SUBS="3"
QOS="1"
SAMPLE_SEC="5"
SLOW_MS="50"
KEEP_CLUSTER=0
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

while [ $# -gt 0 ]; do
    case "$1" in
        -h|--help) sed -n '2,30p' "$0"; exit 0 ;;
        --duration) DURATION="$2"; shift 2 ;;
        --rate)     RATE="$2"; shift 2 ;;
        --pubs)     PUBS="$2"; shift 2 ;;
        --subs)     SUBS="$2"; shift 2 ;;
        --qos)      QOS="$2"; shift 2 ;;
        --sample-sec) SAMPLE_SEC="$2"; shift 2 ;;
        --slow-ms)  SLOW_MS="$2"; shift 2 ;;
        --keep-cluster) KEEP_CLUSTER=1; shift ;;
        *) echo "perf-bench: unknown arg: $1" >&2; exit 2 ;;
    esac
done

TS="$(date +%Y%m%d-%H%M%S)"
OUTDIR="/tmp/perf-bench-${TS}"
mkdir -p "$OUTDIR/metrics" "$OUTDIR/logs"
CLUSTER="pgmqtt-perf"
NS="mqtt"
CTX="kind-${CLUSTER}"

cd "$ROOT"

cleanup() {
    if [ "$KEEP_CLUSTER" = "0" ]; then
        kind delete cluster --name "$CLUSTER" >/dev/null 2>&1 || true
    fi
}
trap cleanup EXIT

echo "==> perf-bench: artefacts to $OUTDIR"
echo "    rate=$RATE pubs=$PUBS subs=$SUBS qos=$QOS duration=$DURATION slow_ms=$SLOW_MS sample_sec=$SAMPLE_SEC"

echo "==> docker build pgmqtt:perf"
docker build --quiet -t pgmqtt:perf . >/dev/null

echo "==> kind cluster $CLUSTER"
kind delete cluster --name "$CLUSTER" >/dev/null 2>&1 || true
kind create cluster --name "$CLUSTER" --wait 60s >/dev/null
kind load docker-image pgmqtt:perf --name "$CLUSTER" >/dev/null
kubectl --context "$CTX" create namespace "$NS" >/dev/null
kubectl --context "$CTX" -n "$NS" apply -f .github/ci/postgres.yaml >/dev/null
kubectl --context "$CTX" -n "$NS" rollout status statefulset/postgres --timeout=180s >/dev/null

echo "==> helm install (PGMQTT_SLOW_STAGE_LOG_MS=$SLOW_MS)"
helm --kube-context "$CTX" install pgmqtt deploy/helm/pgmqtt \
    --namespace "$NS" \
    --set image.repository=pgmqtt --set image.tag=perf --set image.pullPolicy=IfNotPresent \
    --set replicaCount=3 --set auth.allowAnonymous=true \
    --set 'limits.maxQueuedDeliveriesPerClient=0' --set 'limits.maxConnections=0' \
    --set 'limits.maxInboundMsgsPerSec=0' --set 'limits.maxConnectsPerIPPerSec=0' \
    --set 'limits.maxAuthFailuresPerIPPerMin=0' --set operator.bcryptCost=4 \
    --set-string "extraEnv[0].name=PGMQTT_SLOW_STAGE_LOG_MS" \
    --set-string "extraEnv[0].value=$SLOW_MS" \
    --set "database.url=postgres://pgmqtt:pgmqtt@postgres.$NS.svc:5432/pgmqtt?sslmode=disable" >/dev/null
kubectl --context "$CTX" -n "$NS" rollout status deployment/pgmqtt --timeout=180s >/dev/null

scrape() {
    local PHASE="$1"
    for pod in $(kubectl --context "$CTX" -n "$NS" get pods -l app.kubernetes.io/name=pgmqtt -o jsonpath='{.items[*].metadata.name}'); do
        local out="$OUTDIR/metrics/${PHASE}.${pod}.txt"
        kubectl --context "$CTX" -n "$NS" port-forward "$pod" 19090:9090 >/dev/null 2>&1 &
        local pf=$!
        sleep 1
        curl -s --max-time 5 http://127.0.0.1:19090/metrics > "$out" 2>/dev/null || true
        kill "$pf" 2>/dev/null || true
        wait "$pf" 2>/dev/null || true
    done
}

echo "==> baseline scrape"
scrape "00-baseline"

echo "==> sampler running every ${SAMPLE_SEC}s in background"
SAMPLER_PID=
(
    n=0
    while sleep "$SAMPLE_SEC"; do
        n=$((n+1))
        scrape "$(printf "sample-%04d" "$n")"
    done
) &
SAMPLER_PID=$!

echo "==> soak — $DURATION at $RATE msg/s × $PUBS pubs × $SUBS subs qos=$QOS"
set +e
bash scripts/soak-incluster.sh \
    --cluster "$CLUSTER" --namespace "$NS" \
    --duration "$DURATION" --rate "$RATE" \
    --pubs "$PUBS" --subs "$SUBS" --inflight 25 --qos "$QOS" \
    > "$OUTDIR/soak.log" 2>&1
SOAK_RC=$?
set -e
kill "$SAMPLER_PID" 2>/dev/null || true
wait "$SAMPLER_PID" 2>/dev/null || true
cp "$(ls -t /tmp/soak-incluster-*.json | head -1)" "$OUTDIR/soak.json" 2>/dev/null || true

echo "==> final scrape"
scrape "99-final"

echo "==> dump broker logs"
for pod in $(kubectl --context "$CTX" -n "$NS" get pods -l app.kubernetes.io/name=pgmqtt -o jsonpath='{.items[*].metadata.name}'); do
    kubectl --context "$CTX" -n "$NS" logs "$pod" > "$OUTDIR/logs/${pod}.log" 2>&1 || true
done

echo "==> summarise"
python3 "$ROOT/scripts/internal/perf-bench-summary.py" "$OUTDIR" > "$OUTDIR/summary.md"

echo
echo "==> RESULTS at $OUTDIR"
echo "    soak rc=$SOAK_RC"
echo "    summary.md:"
sed 's/^/    /' "$OUTDIR/summary.md"
