#!/usr/bin/env bash
# Lint + render the chart under several value sets, then strict-parse every
# render the way Flux / kustomize / `kubectl apply` do.
#
# `helm template` happily emits YAML that Kubernetes tooling rejects —
# notably duplicate mapping keys (issue #38: `pgmqtt.labels` hardcoded
# `app.kubernetes.io/component: broker` and the UI templates added
# `component: ui` on top; helm rendered both, Flux's post-renderer refused
# the release). Feeding each render through `kustomize build` catches that
# class because kustomize uses the same strict yaml.v3 decoder.
#
# Used by scripts/validate.sh (tier1) and the CI helm-lint job, so the set
# of value combinations exercised is the same in both places.
#
# Usage:
#   scripts/helm-check.sh lint
#   scripts/helm-check.sh render
#   scripts/helm-check.sh          # both

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CHART="$ROOT/deploy/helm/pgmqtt"
OUT="${HELM_CHECK_OUT:-/tmp/pgmqtt-helm-check}"

lint() {
    helm lint "$CHART" --set database.url='postgres://x/y'
}

# render <name> <helm args...>
render() {
    local name="$1"; shift
    local dir="$OUT/$name"
    mkdir -p "$dir"
    helm template pgmqtt "$CHART" "$@" > "$dir/rendered.yaml"
    test -s "$dir/rendered.yaml"
    printf 'resources:\n  - rendered.yaml\n' > "$dir/kustomization.yaml"
    if ! kustomize build "$dir" > /dev/null; then
        echo "helm-check: render '$name' failed strict parse (see $dir/rendered.yaml)" >&2
        return 1
    fi
    echo "helm-check: $name OK ($(grep -c '^kind:' "$dir/rendered.yaml") objects)"
}

renders() {
    command -v kustomize >/dev/null 2>&1 || {
        echo "helm-check: kustomize not found on PATH (needed for strict YAML parse)" >&2
        return 2
    }

    render default \
        --set database.url='postgres://x/y'

    # 3 replicas + UI on + tests off (operator-style install).
    render multi \
        --set database.url='postgres://x/y' \
        --set replicaCount=3 \
        --set ui.enabled=true \
        --set tests.enabled=false

    # External secret + bcrypt cost override (homelab-style install).
    render secret \
        --set database.existingSecret=pgmqtt-db \
        --set operator.bcryptCost=4

    # Every optional resource on, so each template's labels get parsed.
    render everything \
        --set database.url='postgres://x/y' \
        --set ui.enabled=true \
        --set metrics.enabled=true \
        --set serviceMonitor.enabled=true \
        --set grafana.dashboard.enabled=true \
        --set networkPolicy.enabled=true \
        --set autoscaling.enabled=true \
        --set podDisruptionBudget.enabled=true
}

case "${1:-all}" in
    lint)   lint ;;
    render) renders ;;
    all)    lint; renders ;;
    *) echo "usage: $0 [lint|render]" >&2; exit 2 ;;
esac
