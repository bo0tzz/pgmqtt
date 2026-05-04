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

while [ $# -gt 0 ]; do
    case "$1" in
        -h|--help)
            sed -n '2,30p' "$0"; exit 0 ;;
        *) echo "validate.sh: unknown arg: $1" >&2; exit 2 ;;
    esac
done

case "$TIER" in
    tier1) ;;
    tier2|tier3)
        echo "validate.sh: $TIER not yet wired (tier1 only at this commit)" >&2
        exit 2 ;;
    "")  echo "usage: $0 tier1|tier2|tier3" >&2; exit 2 ;;
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

run_tier1

OVERALL_END=$(date +%s)

echo
echo "==> validate.sh $TIER summary"
printf '  %s\n' "${PHASES[@]}"
echo "  ----"
echo "  $((OVERALL_END - OVERALL_START))s total"
echo "  $TIER OK"
