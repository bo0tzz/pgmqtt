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
# Parsing is not enough for value combinations that render valid YAML with
# the wrong content, so those renders also assert on their output, and
# combinations the chart must reject are checked to fail for that reason.
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
    # A named port that nothing declares only fails at runtime: the kubelet
    # cannot resolve it and the probe never passes.
    local port
    for port in $(grep -oE '^ +(port|targetPort): [a-z][a-z0-9-]*$' "$dir/rendered.yaml" | awk '{print $2}' | sort -u); do
        if ! grep -qE "^ +- name: $port\$" "$dir/rendered.yaml"; then
            echo "helm-check: render '$name' references undeclared port '$port'" >&2
            return 1
        fi
    done
    echo "helm-check: $name OK ($(grep -c '^kind:' "$dir/rendered.yaml") objects)"
}

# has / lacks <name> <fixed string>: assert on the output of an earlier render.
has() {
    grep -qF -- "$2" "$OUT/$1/rendered.yaml" || {
        echo "helm-check: render '$1' is missing: $2" >&2
        return 1
    }
}

lacks() {
    if grep -qF -- "$2" "$OUT/$1/rendered.yaml"; then
        echo "helm-check: render '$1' must not contain: $2" >&2
        return 1
    fi
}

# refuses <name> <message fragment> <helm args...>: values the chart must
# reject at render time, with an error that names the actual problem.
refuses() {
    local name="$1" want="$2" err; shift 2
    if err="$(helm template pgmqtt "$CHART" "$@" 2>&1 > /dev/null)"; then
        echo "helm-check: '$name' rendered but the chart should refuse it" >&2
        return 1
    fi
    if ! grep -qF -- "$want" <<< "$err"; then
        echo "helm-check: '$name' was refused for another reason than '$want':" >&2
        echo "$err" >&2
        return 1
    fi
    echo "helm-check: $name OK (refused)"
}

renders() {
    command -v kustomize >/dev/null 2>&1 || {
        echo "helm-check: kustomize not found on PATH (needed for strict YAML parse)" >&2
        return 2
    }

    render default \
        --set database.url='postgres://x/y'
    has default 'sleep:'

    # The preStop delay is independent of the probes.
    render probes-off \
        --set database.url='postgres://x/y' \
        --set probes.enabled=false
    has probes-off 'sleep:'
    lacks probes-off 'Probe:'

    refuses prestop-outlasts-grace 'lifecycle.preStopSleepSeconds must be less than' \
        --set database.url='postgres://x/y' \
        --set lifecycle.preStopSleepSeconds=30 \
        --set terminationGracePeriodSeconds=30

    # The health endpoints live on the metrics HTTP server, so it stays up
    # for the probes; only its exposure goes away.
    render metrics-off \
        --set database.url='postgres://x/y' \
        --set metrics.enabled=false \
        --set serviceMonitor.enabled=true \
        --set networkPolicy.enabled=true
    has metrics-off 'path: /healthz/ready'
    lacks metrics-off 'targetPort: metrics'
    lacks metrics-off 'kind: ServiceMonitor'
    lacks metrics-off 'port: 9090'

    # No HTTP server at all: TCP probes on the MQTT port.
    render metrics-off-tcp-probes \
        --set database.url='postgres://x/y' \
        --set metrics.enabled=false \
        --set probes.useHTTP=false
    lacks metrics-off-tcp-probes 'containerPort: 9090'
    lacks metrics-off-tcp-probes 'path: /healthz'

    # Under autoscaling the HPA owns the replica count, and its floor (not
    # replicaCount) decides whether a PDB of 1 would block node drains.
    render hpa \
        --set database.url='postgres://x/y' \
        --set autoscaling.enabled=true \
        --set replicaCount=1
    lacks hpa '  replicas:'
    has hpa 'minAvailable: 1'

    render hpa-floor-1 \
        --set database.url='postgres://x/y' \
        --set autoscaling.enabled=true \
        --set autoscaling.minReplicas=1 \
        --set replicaCount=3
    lacks hpa-floor-1 '  replicas:'
    has hpa-floor-1 'minAvailable: 0'

    # Values the binary refuses at startup must not get past the chart.
    refuses keepalive-zero 'keepaliveMaxSec' \
        --set database.url='postgres://x/y' \
        --set v5.keepaliveMaxSec=0
    refuses packet-size-over-max 'maxPacketSize' \
        --set database.url='postgres://x/y' \
        --set limits.maxPacketSize=268435456
    refuses no-listeners 'are both false' \
        --set database.url='postgres://x/y' \
        --set listeners.tcp.enabled=false \
        --set listeners.ws.enabled=false

    # Keys the helpers read (and `global`, which Helm hands to subcharts)
    # must pass the schema; an explicit 0 must not turn into the default.
    render overrides \
        --set database.url='postgres://x/y' \
        --set nameOverride=broker \
        --set fullnameOverride=mqtt \
        --set global.anything=x \
        --set probes.liveness.initialDelaySeconds=0 \
        --set probes.readiness.initialDelaySeconds=0
    has overrides 'app.kubernetes.io/name: broker'
    has overrides 'name: mqtt-leader-election'
    lacks overrides 'initialDelaySeconds: 5'
    lacks overrides 'initialDelaySeconds: 2'

    # A disabled listener is advertised to the operator as port 0, and the
    # TCP probes move to the listener that is left.
    render ws-only \
        --set database.url='postgres://x/y' \
        --set listeners.tcp.enabled=false \
        --set probes.useHTTP=false
    lacks ws-only '"1883"'
    has ws-only '"8083"'

    render tcp-only \
        --set database.url='postgres://x/y' \
        --set listeners.ws.enabled=false
    lacks tcp-only '"8083"'
    has tcp-only '"1883"'

    # Keys the chart sets itself, repeated in podLabels or
    # serviceMonitor.labels, used to render as duplicate mapping keys.
    render label-collisions \
        --set database.url='postgres://x/y' \
        --set-string 'podLabels.app\.kubernetes\.io/name=other' \
        --set-string 'podLabels.app\.kubernetes\.io/component=other' \
        --set-string 'podLabels.team=home' \
        --set serviceMonitor.enabled=true \
        --set-string 'serviceMonitor.labels.app\.kubernetes\.io/name=other' \
        --set-string 'serviceMonitor.labels.release=prometheus'
    has label-collisions 'team: home'
    has label-collisions '"release": "prometheus"'
    lacks label-collisions 'other'

    # The convenience anti-affinity plus other affinity kinds is fine; plus
    # a hand-written podAntiAffinity it would be a duplicate key.
    render anti-affinity \
        --set database.url='postgres://x/y' \
        --set podAntiAffinity.enabled=true \
        --set 'affinity.nodeAffinity.requiredDuringSchedulingIgnoredDuringExecution.nodeSelectorTerms[0].matchExpressions[0].key=zone' \
        --set 'affinity.nodeAffinity.requiredDuringSchedulingIgnoredDuringExecution.nodeSelectorTerms[0].matchExpressions[0].operator=Exists'
    has anti-affinity 'nodeAffinity:'
    has anti-affinity 'podAntiAffinity:'
    refuses anti-affinity-twice 'cannot be combined with affinity.podAntiAffinity' \
        --set database.url='postgres://x/y' \
        --set podAntiAffinity.enabled=true \
        --set 'affinity.podAntiAffinity.requiredDuringSchedulingIgnoredDuringExecution[0].topologyKey=zone'

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
        --set grafana.dashboard.instanceSelector.matchLabels.foo=bar \
        --set networkPolicy.enabled=true \
        --set autoscaling.enabled=true \
        --set podDisruptionBudget.enabled=true
    # The policy must let the broker reach its database with no peers set.
    has everything 'port: 5432'
    # The helm test Pod must not match the broker selectors (Service, PDB,
    # NetworkPolicy).
    has everything 'app.kubernetes.io/component: test'

    render netpol-pinned \
        --set database.url='postgres://x/y' \
        --set networkPolicy.enabled=true \
        --set networkPolicy.egress.postgresPort=6432 \
        --set 'networkPolicy.egress.postgres[0].podSelector.matchLabels.cnpg\.io/cluster=pg' \
        --set 'networkPolicy.egress.kubernetesAPI[0].ipBlock.cidr=10.0.0.1/32'
    has netpol-pinned 'cnpg.io/cluster: pg'
    has netpol-pinned 'port: 6432'
    has netpol-pinned 'cidr: 10.0.0.1/32'
    lacks netpol-pinned 'component: apiserver'
}

case "${1:-all}" in
    lint)   lint ;;
    render) renders ;;
    all)    lint; renders ;;
    *) echo "usage: $0 [lint|render]" >&2; exit 2 ;;
esac
