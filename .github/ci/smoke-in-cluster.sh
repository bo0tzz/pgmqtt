#!/usr/bin/env bash
# Smoke test inside CI's kind cluster: apply a User, exercise pub/sub.
set -euo pipefail

NS=mqtt

# Apply a User; the leader Pod's reconciler mints a credentials Secret.
cat <<'EOF' | kubectl -n "$NS" apply -f -
apiVersion: pgmqtt.io/v1alpha1
kind: User
metadata:
  name: smoketest
EOF

echo "==> waiting for credentials Secret"
for _ in $(seq 1 60); do
  if kubectl -n "$NS" get secret smoketest-mqtt-credentials >/dev/null 2>&1; then
    break
  fi
  sleep 1
done
kubectl -n "$NS" get secret smoketest-mqtt-credentials >/dev/null

echo "==> running mosquitto round-trip in-cluster"
kubectl -n "$NS" delete pod mqtt-smoke --ignore-not-found
kubectl -n "$NS" apply -f - <<'EOF'
apiVersion: v1
kind: Pod
metadata:
  name: mqtt-smoke
spec:
  restartPolicy: Never
  containers:
    - name: mosq
      image: eclipse-mosquitto:2.0
      command: ["sh", "-c"]
      args:
        - |
          set -eux
          (mosquitto_sub -h "$HOST" -p "$PORT" -u "$U" -P "$P" -t 'smoke/#' -C 1 -W 10 > /tmp/got 2>&1) &
          SUB=$!
          sleep 1
          mosquitto_pub -h "$HOST" -p "$PORT" -u "$U" -P "$P" -t smoke/x -m hello -q 1
          wait $SUB
          cat /tmp/got
          grep -q hello /tmp/got
      env:
        - name: HOST
          valueFrom: { secretKeyRef: { name: smoketest-mqtt-credentials, key: host } }
        - name: PORT
          valueFrom: { secretKeyRef: { name: smoketest-mqtt-credentials, key: port } }
        - name: U
          valueFrom: { secretKeyRef: { name: smoketest-mqtt-credentials, key: username } }
        - name: P
          valueFrom: { secretKeyRef: { name: smoketest-mqtt-credentials, key: password } }
EOF

echo "==> waiting for Pod to finish"
for _ in $(seq 1 60); do
  phase=$(kubectl -n "$NS" get pod mqtt-smoke -o jsonpath='{.status.phase}' 2>/dev/null || true)
  if [ "$phase" = "Succeeded" ] || [ "$phase" = "Failed" ]; then
    break
  fi
  sleep 2
done
phase=$(kubectl -n "$NS" get pod mqtt-smoke -o jsonpath='{.status.phase}' 2>/dev/null || echo Unknown)
kubectl -n "$NS" logs mqtt-smoke
if [ "$phase" != "Succeeded" ]; then
  echo "smoke pod ended in phase: $phase" >&2
  exit 1
fi
echo "==> smoke OK"

echo "==> exercising User deletion (DB row + Secret GC)"
kubectl -n "$NS" delete user smoketest
for _ in $(seq 1 30); do
  if ! kubectl -n "$NS" get secret smoketest-mqtt-credentials >/dev/null 2>&1; then
    break
  fi
  sleep 1
done
if kubectl -n "$NS" get secret smoketest-mqtt-credentials >/dev/null 2>&1; then
  echo "credentials Secret was not garbage-collected" >&2
  exit 1
fi
echo "==> deletion OK"
