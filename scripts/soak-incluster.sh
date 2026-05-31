#!/usr/bin/env bash
# Run cmd/soak as a Pod inside a kind cluster, connecting to the pgmqtt
# Service VIP directly. Replaces the kubectl port-forward path used by
# scripts/paho-multi-broker.sh and ad-hoc cmd/soak invocations, both of
# which die mid-stream under sustained load (port-forward stalls under
# backpressure; the soak then sees socket EOF and reports spurious loss).
#
# Inside-cluster ⇒ direct ClusterIP, no userland forwarder, no localhost
# hops. The Pod streams its JSON output to stdout; we tail the logs and
# parse them on success.
#
# Usage:
#   scripts/soak-incluster.sh \
#       --cluster pgmqtt-perf \
#       --namespace mqtt \
#       --duration 30s \
#       --rate 1000 \
#       --pubs 3 --subs 3 --inflight 25 --qos 1
#
#   # With auth:
#   scripts/soak-incluster.sh ... --user soak --pass s3kret
#
# Required tooling: docker, kind, kubectl, go.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

CLUSTER=""
NS=""
DURATION="30s"
RATE="1000"
PUBS="3"
SUBS="3"
INFLIGHT="25"
QOS="1"
USER_FLAG=""
PASS_FLAG=""
TOPIC="soak/incluster"
DETACH=0

while [ $# -gt 0 ]; do
    case "$1" in
        -h|--help)
            sed -n '2,25p' "$0"; exit 0 ;;
        --cluster)   CLUSTER="$2"; shift 2 ;;
        --namespace) NS="$2"; shift 2 ;;
        --duration)  DURATION="$2"; shift 2 ;;
        --rate)      RATE="$2"; shift 2 ;;
        --pubs)      PUBS="$2"; shift 2 ;;
        --subs)      SUBS="$2"; shift 2 ;;
        --inflight)  INFLIGHT="$2"; shift 2 ;;
        --qos)       QOS="$2"; shift 2 ;;
        --user)      USER_FLAG="$2"; shift 2 ;;
        --pass)      PASS_FLAG="$2"; shift 2 ;;
        --topic)     TOPIC="$2"; shift 2 ;;
        --detach)    DETACH=1; shift ;;
        *) echo "soak-incluster: unknown arg: $1" >&2; exit 2 ;;
    esac
done

if [ -z "$CLUSTER" ] || [ -z "$NS" ]; then
    echo "usage: $0 --cluster <kind-name> --namespace <ns> [--duration 30s] [--rate 1000]" >&2
    echo "        [--pubs 3] [--subs 3] [--inflight 25] [--qos 0|1|2]" >&2
    echo "        [--user U --pass P] [--topic prefix]" >&2
    exit 2
fi

# Sanity-check tools up front so we fail fast with a clear message.
for bin in docker kind kubectl go; do
    if ! command -v "$bin" >/dev/null 2>&1; then
        echo "soak-incluster: required tool '$bin' not found in PATH" >&2
        exit 127
    fi
done

KCTX="kind-$CLUSTER"
if ! kubectl --context "$KCTX" version >/dev/null 2>&1; then
    echo "soak-incluster: kube context '$KCTX' not reachable (is the kind cluster up?)" >&2
    exit 1
fi
if ! kubectl --context "$KCTX" -n "$NS" get svc pgmqtt >/dev/null 2>&1; then
    echo "soak-incluster: no Service 'pgmqtt' in namespace '$NS' on context '$KCTX'" >&2
    exit 1
fi

SHA="$(date +%Y%m%d%H%M%S)-$$"
IMAGE="pgmqtt-soak:$SHA"
POD="pgmqtt-soak-$SHA"
TS="$(date +%Y%m%d-%H%M%S)"
OUT="/tmp/soak-incluster-${TS}.json"

# Build a self-contained scratch dir so we don't pollute the repo with the
# soak binary or the throwaway Dockerfile.
WORK="$(mktemp -d -t pgmqtt-soak.XXXXXX)"
cleanup() {
    local rc=$?
    set +e
    # In --detach mode the Pod must outlive this script. Skip the Pod
    # delete so a host hiccup (sleep, network blip, terminal close)
    # doesn't take the soak Pod down with the launcher script. Operator
    # runs `scripts/soak-harvest.sh` later to collect logs + verdict
    # and then explicitly removes the Pod.
    if [ "$DETACH" = "0" ] && kubectl --context "$KCTX" -n "$NS" get pod "$POD" >/dev/null 2>&1; then
        kubectl --context "$KCTX" -n "$NS" delete pod "$POD" \
            --grace-period=0 --force >/dev/null 2>&1 || true
    fi
    docker image rm -f "$IMAGE" >/dev/null 2>&1 || true
    rm -rf "$WORK"
    exit $rc
}
trap cleanup EXIT INT TERM

echo "==> [1/5] build static cmd/soak binary"
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go build -trimpath -ldflags '-s -w' -o "$WORK/soak-bin" ./cmd/soak

echo "==> [2/5] build distroless image $IMAGE"
cat >"$WORK/Dockerfile" <<'EOF'
FROM gcr.io/distroless/static-debian13:nonroot
COPY soak-bin /soak
ENTRYPOINT ["/soak"]
EOF
docker build --quiet -t "$IMAGE" "$WORK" >/dev/null

echo "==> [3/5] kind load docker-image $IMAGE --name $CLUSTER"
kind load docker-image "$IMAGE" --name "$CLUSTER" >/dev/null

# Build the soak argv. Note: cmd/soak's flag parser treats empty -user / -pass
# as "send empty CONNECT credentials", which is what allowAnonymous=true wants.
SOAK_ARGS=(
    "-broker" "pgmqtt.${NS}.svc.cluster.local:1883"
    "-user" "$USER_FLAG"
    "-pass" "$PASS_FLAG"
    "-duration" "$DURATION"
    "-rate" "$RATE"
    "-pubs" "$PUBS"
    "-subs" "$SUBS"
    "-inflight" "$INFLIGHT"
    "-qos" "$QOS"
    "-topic" "$TOPIC"
)

# kubectl run requires `--` before flags that look like its own. Build the
# full command via JSON-overrides so we don't have to fight kubectl's parser.
# Use NUL separators between args so empty strings (e.g. anonymous user/pass
# pair) survive — a previous newline-delimited path silently dropped empty
# fields and merged adjacent flags ("-user", "-pass" → broker saw -user=-pass).
ARGS_JSON=$(printf '%s\0' "${SOAK_ARGS[@]}" | python3 -c '
import json, sys
data = sys.stdin.buffer.read()
# Trailing NUL after last arg → drop the empty final element.
parts = data.split(b"\0")
if parts and parts[-1] == b"":
    parts = parts[:-1]
print(json.dumps([p.decode("utf-8") for p in parts]))
')
OVERRIDES=$(cat <<JSON
{
  "spec": {
    "restartPolicy": "Never",
    "containers": [
      {
        "name": "soak",
        "image": "$IMAGE",
        "imagePullPolicy": "IfNotPresent",
        "args": $ARGS_JSON
      }
    ]
  }
}
JSON
)

echo "==> [4/5] run soak Pod $POD (broker=pgmqtt.${NS}.svc.cluster.local:1883)"
kubectl --context "$KCTX" -n "$NS" run "$POD" \
    --image="$IMAGE" \
    --image-pull-policy=IfNotPresent \
    --restart=Never \
    --overrides="$OVERRIDES" >/dev/null

# Wait until the Pod is Running (or until it terminated very fast). With
# --restart=Never the Pod never enters Ready=True; we want PodScheduled +
# at least one container started.
deadline=$(( $(date +%s) + 60 ))
while :; do
    phase=$(kubectl --context "$KCTX" -n "$NS" get pod "$POD" \
        -o jsonpath='{.status.phase}' 2>/dev/null || true)
    case "$phase" in
        Running|Succeeded|Failed) break ;;
    esac
    if [ "$(date +%s)" -ge "$deadline" ]; then
        echo "soak-incluster: Pod $POD did not start within 60s; phase=$phase" >&2
        kubectl --context "$KCTX" -n "$NS" describe pod "$POD" >&2 || true
        exit 1
    fi
    sleep 1
done

if [ "$DETACH" = "1" ]; then
    echo "==> [5/5] detach (Pod runs to completion independent of this script)"
    echo
    echo "Pod:        $POD"
    echo "Namespace:  $NS"
    echo "Context:    $KCTX"
    echo "Duration:   $DURATION"
    echo "Shape:      qos=$QOS rate=$RATE pubs=$PUBS subs=$SUBS inflight=$INFLIGHT"
    echo
    echo "Harvest when done:"
    echo "  scripts/soak-harvest.sh --cluster $CLUSTER --namespace $NS --pod $POD"
    exit 0
fi

echo "==> [5/5] follow Pod logs → $OUT"
# Stream logs to both the JSON file and stdout. cmd/soak prints periodic
# progress lines and ends with a single JSON line — we keep all of it in
# the artifact and grep the last JSON object out for the verdict.
LOG_RAW="${OUT%.json}.log"
kubectl --context "$KCTX" -n "$NS" logs -f "$POD" | tee "$LOG_RAW"

# Extract the JSON summary (cmd/soak pretty-prints; spans multiple lines
# and is the only `{...}` block in the output). Grab from the first `{`
# at column 0 to the matching `}` at column 0.
python3 - "$LOG_RAW" "$OUT" <<'PY'
import json, sys, re
src, dst = sys.argv[1], sys.argv[2]
with open(src) as f:
    text = f.read()
# Find each top-level "{...}" block (open-brace at column 0 to matching
# close-brace at column 0). cmd/soak emits exactly one such block.
blocks = re.findall(r"(?ms)^\{.*?^\}", text)
if not blocks:
    sys.exit(2)
# Validate the last block parses as JSON; if not, fall through with empty.
try:
    obj = json.loads(blocks[-1])
except json.JSONDecodeError:
    sys.exit(3)
with open(dst, "w") as f:
    json.dump(obj, f, indent=2)
    f.write("\n")
PY
JSON_RC=$?
if [ "$JSON_RC" -ne 0 ] || [ ! -s "$OUT" ]; then
    echo "soak-incluster: no JSON summary found in pod logs (run failed?)" >&2
    kubectl --context "$KCTX" -n "$NS" describe pod "$POD" >&2 || true
    exit 1
fi

echo
echo "==> JSON summary saved to: $OUT"
cat "$OUT"

# Inspect verdict: non-zero loss/dups for QoS≥1 is failure. cmd/soak's JSON
# uses total_lost / total_dups for the aggregate; fall back to lost/dups in
# case the schema changes back in a future refactor.
LOST=$(python3 -c '
import json, sys
o = json.load(open(sys.argv[1]))
print(o.get("total_lost", o.get("lost", 0)))
' "$OUT")
DUPS=$(python3 -c '
import json, sys
o = json.load(open(sys.argv[1]))
print(o.get("total_dups", o.get("dups", 0)))
' "$OUT")
PUBLISHED=$(python3 -c '
import json, sys
print(json.load(open(sys.argv[1])).get("published", 0))
' "$OUT")
RECEIVED=$(python3 -c '
import json, sys
o = json.load(open(sys.argv[1]))
reps = o.get("sub_reports") or []
print(sum(r.get("received", 0) for r in reps))
' "$OUT")

echo
echo "==> verdict: published=$PUBLISHED received=$RECEIVED lost=$LOST dups=$DUPS qos=$QOS"

if [ "$QOS" != "0" ]; then
    if [ "$LOST" != "0" ] || [ "$DUPS" != "0" ]; then
        echo "soak-incluster: FAIL — lost=$LOST dups=$DUPS at qos=$QOS" >&2
        exit 1
    fi
fi
if [ "$PUBLISHED" = "0" ] || [ "$RECEIVED" = "0" ]; then
    echo "soak-incluster: FAIL — published=$PUBLISHED received=$RECEIVED (no traffic flowed)" >&2
    exit 1
fi
# QoS-0 is at-most-once so the cmd/soak per-message-loss gate above
# doesn't fire for it. In a sane in-cluster setup we still expect near-
# total delivery though, and a regression of the May 2026 deliveries-
# accumulate wedge manifests as ~100% loss past the first ~10k messages
# per subscriber. Gate on aggregate delivery rate to catch that
# specifically.
#
# 70% threshold (was 95%): the broker's sustained throughput on a kind
# cluster running on a laptop varies 70–100% across trials for the
# tier3 shape (3 pubs × 3 subs × 1000 msg/s QoS-1 after multi-broker
# paho's churn), with 0 lost / 0 dups across the variance window — i.e.
# the variance is in *throughput*, not correctness. 95% was producing
# ~30% false-fail rate on tier3 CI; 70% still catches the deliveries-
# accumulate wedge (~0% delivery past the cap) and any catastrophic
# regression (broker tops out at <30% of target) while leaving the
# kind-cluster headroom variance alone. The cmd/soak gap-analysis
# (lost/dups) stays at zero-tolerance and is the real correctness gate.
EXPECTED=$((PUBLISHED * SUBS))
MIN=$((EXPECTED * 70 / 100))
if [ "$RECEIVED" -lt "$MIN" ]; then
    echo "soak-incluster: FAIL — received=$RECEIVED below 70%% of expected=$EXPECTED at qos=$QOS (published=$PUBLISHED × subs=$SUBS)" >&2
    exit 1
fi

echo "soak-incluster: OK — published=$PUBLISHED received=$RECEIVED lost=$LOST dups=$DUPS at qos=$QOS"
