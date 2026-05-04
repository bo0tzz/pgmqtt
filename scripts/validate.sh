#!/usr/bin/env bash
# Tiered validation entrypoint for pgmqtt.
#
# One canonical orchestrator over the existing pieces (make targets, helm,
# Paho conformance wrapper, soak rig). Born out of v0.1.1: a regression in
# migration 0010 made it past commit + tag because TestSlowSubscriberQuotaExceeded
# was never re-run. Tiers exist so "I made a change" has an obvious answer
# to "what do I run now?".
#
# Tiers (additive — tier3 implies tier2 implies tier1):
#
#   tier1   fast — after every change. ~1–2 min on a developer laptop.
#           go vet + make test-race + helm lint + helm template (default
#           values + a couple of overrides).
#
#   tier2   slow — before commit. ~5–10 min.
#           tier1 + make coverage + Paho v3 + Paho v5 single-broker.
#           Requires --paho /path/to/paho.mqtt.testing.
#
#   tier3   full — before tag / nightly CI.
#           tier2 + multi-broker Paho via kind + helm test in kind +
#           soak smoke (60s × 1000 msg/s).
#
# Usage:
#   scripts/validate.sh tier1
#   scripts/validate.sh tier2 --paho /tmp/paho-testing
#   scripts/validate.sh tier3 --paho /tmp/paho-testing

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

TIER="${1:-}"
shift || true

PAHO=""
KEEP_CLUSTER=""
while [ $# -gt 0 ]; do
    case "$1" in
        -h|--help)
            sed -n '2,30p' "$0"; exit 0 ;;
        --paho) PAHO="$2"; shift 2 ;;
        --keep-cluster) KEEP_CLUSTER=1; shift ;;
        *) echo "validate.sh: unknown arg: $1" >&2; exit 2 ;;
    esac
done

case "$TIER" in
    tier1) ;;
    tier2|tier3)
        if [ -z "$PAHO" ]; then
            echo "validate.sh: $TIER requires --paho /path/to/paho.mqtt.testing" >&2
            exit 2
        fi
        if [ ! -d "$PAHO/interoperability" ]; then
            echo "validate.sh: --paho path doesn't look like the paho.mqtt.testing repo: $PAHO" >&2
            exit 2
        fi
        ;;
    "")  echo "usage: $0 tier1|tier2|tier3 [--paho PATH] [--keep-cluster]" >&2; exit 2 ;;
    *)   echo "validate.sh: unknown tier: $TIER" >&2; exit 2 ;;
esac

PHASES=()
phase() {
    local name="$1"; shift
    echo
    echo "==> [$TIER] $name"
    local start end rc=0
    start=$(date +%s)
    "$@" || rc=$?
    end=$(date +%s)
    PHASES+=("$((end - start))s  $name  rc=$rc")
    if [ $rc -ne 0 ]; then
        echo "validate.sh: phase '$name' failed (rc=$rc)" >&2
        printf '  %s\n' "${PHASES[@]}"
        exit $rc
    fi
}

OVERALL_START=$(date +%s)

# ---------- tier1 phases ----------

t1_vet()         { go vet ./...; }
t1_test_race()   { go test ./... -count=1 -race -timeout 10m; }

t1_helm_lint() {
    helm lint deploy/helm/pgmqtt --set database.url='postgres://x/y'
}

# Render the chart with a few representative value sets so a typo in
# values.yaml or a misuse of a value in a template fails here, not in
# someone's `helm install`.
t1_helm_template() {
    helm template deploy/helm/pgmqtt \
        --set database.url='postgres://x/y' \
        > /tmp/pgmqtt-render-default.yaml
    test -s /tmp/pgmqtt-render-default.yaml

    # 3 replicas + UI on + tests off (operator-style install).
    helm template deploy/helm/pgmqtt \
        --set database.url='postgres://x/y' \
        --set replicaCount=3 \
        --set ui.enabled=true \
        --set tests.enabled=false \
        > /tmp/pgmqtt-render-multi.yaml
    test -s /tmp/pgmqtt-render-multi.yaml

    # External secret + bcrypt cost override (homelab-style install).
    helm template deploy/helm/pgmqtt \
        --set database.existingSecret=pgmqtt-db \
        --set operator.bcryptCost=4 \
        > /tmp/pgmqtt-render-secret.yaml
    test -s /tmp/pgmqtt-render-secret.yaml
}

run_tier1() {
    phase "go vet"           t1_vet
    phase "make test-race"   t1_test_race
    phase "helm lint"        t1_helm_lint
    phase "helm template"    t1_helm_template
}

# ---------- tier2 phases ----------

t2_coverage() {
    go test ./... -count=1 -coverprofile=coverage.out -covermode=atomic -timeout 10m
    go tool cover -func=coverage.out | tee coverage.txt | tail -5
}

# Boot a single-broker pgmqttd (TCP only) against an ephemeral postgres
# container, run the Paho v3+v5 conformance wrapper, capture results.
T2_PG_PORT=55435
T2_BROKER_PORT=11885
T2_PG_NAME="pgmqtt-validate-pg-$$"
t2_paho_setup() {
    docker rm -f "$T2_PG_NAME" >/dev/null 2>&1 || true
    docker run --rm -d --name "$T2_PG_NAME" \
        -p "$T2_PG_PORT:5432" \
        -e POSTGRES_USER=pgmqtt -e POSTGRES_PASSWORD=pgmqtt -e POSTGRES_DB=pgmqtt \
        postgres:18-alpine \
        -c shared_preload_libraries=pg_stat_statements >/dev/null
    # Wait for postgres ready.
    for _ in $(seq 1 30); do
        if docker exec "$T2_PG_NAME" pg_isready -U pgmqtt >/dev/null 2>&1; then break; fi
        sleep 1
    done
    go build -o /tmp/pgmqttd-validate ./cmd/pgmqttd
    PGMQTT_DATABASE_URL="postgres://pgmqtt:pgmqtt@localhost:$T2_PG_PORT/pgmqtt?sslmode=disable" \
    PGMQTT_TCP_ADDR="127.0.0.1:$T2_BROKER_PORT" \
    PGMQTT_WS_ADDR= \
    PGMQTT_METRICS_ADDR= \
    PGMQTT_ALLOW_ANONYMOUS=true \
        /tmp/pgmqttd-validate >/tmp/pgmqttd-validate.log 2>&1 &
    echo $! >/tmp/pgmqttd-validate.pid
    # Wait for the broker to start listening.
    for _ in $(seq 1 30); do
        if (echo > /dev/tcp/127.0.0.1/$T2_BROKER_PORT) 2>/dev/null; then break; fi
        sleep 1
    done
}

t2_paho_teardown() {
    if [ -f /tmp/pgmqttd-validate.pid ]; then
        kill "$(cat /tmp/pgmqttd-validate.pid)" 2>/dev/null || true
        rm -f /tmp/pgmqttd-validate.pid
    fi
    docker rm -f "$T2_PG_NAME" >/dev/null 2>&1 || true
}

t2_paho_run() {
    python3 scripts/paho-conformance.py \
        --paho "$PAHO" \
        --host 127.0.0.1 \
        --port "$T2_BROKER_PORT" \
        --version both \
        --per-test-timeout 60
}

run_tier2() {
    run_tier1
    phase "make coverage"    t2_coverage
    phase "paho setup"       t2_paho_setup
    trap t2_paho_teardown EXIT
    phase "paho v3+v5"       t2_paho_run
    phase "paho teardown"    t2_paho_teardown
    trap - EXIT
}

# ---------- tier3 phases ----------

# t3_multi_broker spins a fresh kind cluster, helm-installs the broker,
# then runs scripts/paho-multi-broker-incluster.sh — the in-cluster Pod
# variant. The earlier port-forward path (scripts/paho-multi-broker.sh)
# dropped after ~11 connections under paho's churn; the in-cluster
# runner sidesteps that by talking directly to the Service VIP via
# cluster DNS. Cluster name is namespaced to this script + tier3 so
# parallel sessions don't collide.
T3_CLUSTER="pgmqtt-validate-tier3-$$"
T3_NS="mqtt"

t3_multi_broker_setup() {
    docker build --quiet -t pgmqtt:tier3 . >/dev/null
    kind create cluster --name "$T3_CLUSTER" --wait 60s
    kind load docker-image pgmqtt:tier3 --name "$T3_CLUSTER" >/dev/null
    kubectl --context "kind-$T3_CLUSTER" create namespace "$T3_NS"
    kubectl --context "kind-$T3_CLUSTER" -n "$T3_NS" apply -f .github/ci/postgres.yaml
    kubectl --context "kind-$T3_CLUSTER" -n "$T3_NS" rollout status statefulset/postgres --timeout=180s
    helm --kube-context "kind-$T3_CLUSTER" install pgmqtt deploy/helm/pgmqtt \
        --namespace "$T3_NS" \
        --set image.repository=pgmqtt --set image.tag=tier3 --set image.pullPolicy=IfNotPresent \
        --set replicaCount=3 \
        --set auth.allowAnonymous=true \
        --set 'limits.maxQueuedDeliveriesPerClient=0' \
        --set 'limits.maxConnections=0' \
        --set 'limits.maxInboundMsgsPerSec=0' \
        --set 'limits.maxConnectsPerIPPerSec=0' \
        --set 'limits.maxAuthFailuresPerIPPerMin=0' \
        --set operator.bcryptCost=4 \
        --set "database.url=postgres://pgmqtt:pgmqtt@postgres.$T3_NS.svc:5432/pgmqtt?sslmode=disable" >/dev/null
    kubectl --context "kind-$T3_CLUSTER" -n "$T3_NS" rollout status deployment/pgmqtt --timeout=180s
}

t3_multi_broker() {
    bash scripts/paho-multi-broker-incluster.sh \
        --cluster "$T3_CLUSTER" \
        --namespace "$T3_NS" \
        --version both
}

t3_multi_broker_teardown() {
    kind delete cluster --name "$T3_CLUSTER" >/dev/null 2>&1 || true
}

t3_soak_smoke() {
    # In-cluster soak against the same tier3 kind cluster t3_multi_broker
    # set up. Direct Service VIP — no port-forward — so it's reliable under
    # the rate the multi-broker run can take. 60s × 1000 msg/s × 3 pubs ×
    # 3 subs at QoS 1.
    bash scripts/soak-incluster.sh \
        --cluster "$T3_CLUSTER" \
        --namespace "$T3_NS" \
        --duration 60s --rate 1000 \
        --pubs 3 --subs 3 --inflight 25 --qos 1
}

run_tier3() {
    run_tier2
    # tier3 reuses one kind cluster for both multi-broker paho and the
    # soak smoke — same broker install, no per-phase teardown. The
    # cluster is created in t3_multi_broker_setup and torn down in
    # t3_multi_broker_teardown.
    phase "tier3 cluster setup" t3_multi_broker_setup
    trap t3_multi_broker_teardown EXIT
    phase "multi-broker paho"   t3_multi_broker
    phase "soak smoke"          t3_soak_smoke
    phase "tier3 cluster teardown" t3_multi_broker_teardown
    trap - EXIT
}

case "$TIER" in
    tier1) run_tier1 ;;
    tier2) run_tier2 ;;
    tier3) run_tier3 ;;
esac

OVERALL_END=$(date +%s)

echo
echo "==> validate.sh $TIER summary"
printf '  %s\n' "${PHASES[@]}"
echo "  ----"
echo "  $((OVERALL_END - OVERALL_START))s total"
echo "  $TIER OK"
