#!/usr/bin/env bash
# Smoke test inside CI's kind cluster: apply a User, exercise pub/sub.
set -euo pipefail

NS=mqtt

# Apply a User; this triggers the reconciler to mint a credentials Secret.
cat <<'EOF' | kubectl -n "$NS" apply -f -
apiVersion: pgmqtt.io/v1alpha1
kind: User
metadata:
  name: smoketest
EOF

# Wait for the credentials Secret to be created.
for _ in $(seq 1 60); do
  if kubectl -n "$NS" get secret smoketest-mqtt-credentials >/dev/null 2>&1; then
    break
  fi
  sleep 1
done
kubectl -n "$NS" get secret smoketest-mqtt-credentials -o yaml >/dev/null

# Run an in-cluster mosquitto round-trip.
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
          URI="$(echo $URI | tr -d '\n')"
          # Subscribe in background, publish foreground, look for the message.
          (mosquitto_sub -L "$URI" -t 'smoke/#' -C 1 -W 10 > /tmp/got) &
          SUB=$!
          sleep 1
          mosquitto_pub -L "$URI" -t 'smoke/x' -m hello -q 1
          wait $SUB
          grep -q hello /tmp/got
      env:
        - name: URI
          valueFrom:
            secretKeyRef:
              name: smoketest-mqtt-credentials
              key: uri
EOF

kubectl -n "$NS" wait --for=condition=Ready pod/mqtt-smoke --timeout=120s || true
kubectl -n "$NS" wait --for=jsonpath='{.status.phase}'=Succeeded pod/mqtt-smoke --timeout=120s
kubectl -n "$NS" logs mqtt-smoke
