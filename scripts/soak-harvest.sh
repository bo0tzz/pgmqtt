#!/usr/bin/env bash
# Harvest a soak Pod that was launched with `soak-incluster.sh --detach`.
# Waits for the Pod to terminate, grabs its full log, parses out the JSON
# summary, applies the same verdict rules as the inline path, and (unless
# --keep-pod) deletes the Pod when done.
#
# Pairs with `scripts/soak-incluster.sh --detach` so a long-running soak
# (overnight, weekly) survives host churn: the launcher creates the Pod
# and exits, the Pod runs to completion on the kubelet's own watch, and
# this script collects the verdict whenever the operator gets around to
# it. The previous trap-heavy single-script flow killed the Pod whenever
# the host script died, truncating two overnight soaks in this repo's
# history.
#
# Usage:
#   scripts/soak-harvest.sh --cluster pgmqtt-soak --namespace mqtt \
#       --pod pgmqtt-soak-20260531120000-12345
#
#   scripts/soak-harvest.sh --cluster ... --namespace ... --pod ... \
#       --wait-timeout 12h --keep-pod
#
# Required tooling: kubectl, python3.

set -uo pipefail

CLUSTER=""
NS=""
POD=""
WAIT_TIMEOUT="13h"
KEEP_POD=0

while [ $# -gt 0 ]; do
    case "$1" in
        -h|--help)
            sed -n '2,25p' "$0"; exit 0 ;;
        --cluster)      CLUSTER="$2"; shift 2 ;;
        --namespace)    NS="$2"; shift 2 ;;
        --pod)          POD="$2"; shift 2 ;;
        --wait-timeout) WAIT_TIMEOUT="$2"; shift 2 ;;
        --keep-pod)     KEEP_POD=1; shift ;;
        *) echo "soak-harvest: unknown arg: $1" >&2; exit 2 ;;
    esac
done

if [ -z "$CLUSTER" ] || [ -z "$NS" ] || [ -z "$POD" ]; then
    echo "usage: $0 --cluster <kind-name> --namespace <ns> --pod <pod-name> [--wait-timeout 13h] [--keep-pod]" >&2
    exit 2
fi

KCTX="kind-$CLUSTER"
if ! kubectl --context "$KCTX" version >/dev/null 2>&1; then
    echo "soak-harvest: kube context '$KCTX' not reachable" >&2
    exit 1
fi
if ! kubectl --context "$KCTX" -n "$NS" get pod "$POD" >/dev/null 2>&1; then
    echo "soak-harvest: Pod $POD not found in $NS on $KCTX" >&2
    exit 1
fi

OUT="/tmp/soak-incluster-${POD}.json"
LOG_RAW="${OUT%.json}.log"

echo "==> waiting for Pod to terminate (timeout $WAIT_TIMEOUT)"
phase=""
deadline=$(( $(date +%s) + $(python3 -c "
import sys, re
m = re.match(r'(\d+)([smh])', '$WAIT_TIMEOUT')
if not m: sys.exit('bad --wait-timeout')
n, u = int(m.group(1)), m.group(2)
print(n * {'s':1, 'm':60, 'h':3600}[u])
") ))
while [ "$(date +%s)" -lt "$deadline" ]; do
    phase=$(kubectl --context "$KCTX" -n "$NS" get pod "$POD" -o jsonpath='{.status.phase}' 2>/dev/null || true)
    case "$phase" in
        Succeeded|Failed) break ;;
    esac
    sleep 30
done
if [ "$phase" != "Succeeded" ] && [ "$phase" != "Failed" ]; then
    echo "soak-harvest: Pod did not terminate within $WAIT_TIMEOUT (still in phase=$phase)" >&2
    exit 1
fi
echo "Pod phase: $phase"

echo "==> capturing full logs to $LOG_RAW"
kubectl --context "$KCTX" -n "$NS" logs "$POD" > "$LOG_RAW"

echo "==> parsing JSON summary to $OUT"
python3 - "$LOG_RAW" "$OUT" <<'PY'
import json, sys, re
src, dst = sys.argv[1], sys.argv[2]
with open(src) as f:
    text = f.read()
blocks = re.findall(r"(?ms)^\{.*?^\}", text)
if not blocks:
    sys.exit(2)
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
    echo "soak-harvest: no JSON summary found in pod logs" >&2
    echo "  Pod phase: $phase" >&2
    echo "  Last 30 log lines:" >&2
    tail -30 "$LOG_RAW" >&2 || true
    exit 1
fi
echo
echo "==> JSON summary saved to: $OUT"
cat "$OUT"

LOST=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1])).get("total_lost", 0))' "$OUT")
DUPS=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1])).get("total_dups", 0))' "$OUT")
PUBLISHED=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1])).get("published", 0))' "$OUT")
RECEIVED=$(python3 -c 'import json,sys
o = json.load(open(sys.argv[1]))
print(sum(r.get("received", 0) for r in (o.get("sub_reports") or [])))' "$OUT")
QOS=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1])).get("qos", 1))' "$OUT")
SUBS=$(python3 -c 'import json,sys; print(len(json.load(open(sys.argv[1])).get("sub_reports", [])))' "$OUT")

echo
echo "==> verdict: published=$PUBLISHED received=$RECEIVED lost=$LOST dups=$DUPS qos=$QOS"

RC=0
if [ "$QOS" != "0" ]; then
    if [ "$LOST" != "0" ] || [ "$DUPS" != "0" ]; then
        echo "soak-harvest: FAIL — lost=$LOST dups=$DUPS at qos=$QOS" >&2
        RC=1
    fi
fi
if [ "$PUBLISHED" = "0" ] || [ "$RECEIVED" = "0" ]; then
    echo "soak-harvest: FAIL — published=$PUBLISHED received=$RECEIVED (no traffic flowed)" >&2
    RC=1
fi
if [ "$SUBS" -gt 0 ]; then
    # 70% threshold (matches scripts/soak-incluster.sh): the broker's
    # sustained throughput on kind varies 70–100% across trials for the
    # tier3 soak shape, with 0 lost / 0 dups across the variance window.
    # See soak-incluster.sh for the longer rationale.
    EXPECTED=$((PUBLISHED * SUBS))
    MIN=$((EXPECTED * 70 / 100))
    if [ "$RECEIVED" -lt "$MIN" ]; then
        echo "soak-harvest: FAIL — received=$RECEIVED below 70%% of expected=$EXPECTED at qos=$QOS" >&2
        RC=1
    fi
fi

if [ "$KEEP_POD" = "0" ]; then
    echo "==> deleting Pod"
    kubectl --context "$KCTX" -n "$NS" delete pod "$POD" --grace-period=10 >/dev/null 2>&1 || true
fi

if [ "$RC" = "0" ]; then
    echo "soak-harvest: OK — published=$PUBLISHED received=$RECEIVED lost=$LOST dups=$DUPS at qos=$QOS"
fi
exit $RC
