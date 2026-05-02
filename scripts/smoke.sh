#!/usr/bin/env bash
# Quick local smoke test: starts a Postgres container, builds the binary,
# runs it, exercises a publish/subscribe round-trip with mosquitto_pub/sub
# (if installed) or with the included Go test harness.

set -euo pipefail

ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

PG_NAME="pgmqtt-smoke-pg"
PG_PORT="${PG_PORT:-55432}"
DB_URL="postgres://pgmqtt:pgmqtt@localhost:${PG_PORT}/pgmqtt?sslmode=disable"

cleanup() {
  set +e
  if [[ -n "${BROKER_PID:-}" ]] && kill -0 "$BROKER_PID" 2>/dev/null; then
    kill "$BROKER_PID"
    wait "$BROKER_PID" 2>/dev/null || true
  fi
  docker rm -f "$PG_NAME" >/dev/null 2>&1 || true
}
trap cleanup EXIT

echo "==> launching postgres"
docker run --rm -d --name "$PG_NAME" \
  -p "${PG_PORT}:5432" \
  -e POSTGRES_USER=pgmqtt -e POSTGRES_PASSWORD=pgmqtt -e POSTGRES_DB=pgmqtt \
  postgres:16-alpine >/dev/null

echo "==> waiting for postgres"
for _ in $(seq 1 60); do
  if docker exec "$PG_NAME" pg_isready -U pgmqtt >/dev/null 2>&1; then
    break
  fi
  sleep 1
done

echo "==> building pgmqttd"
go build -o "$ROOT/pgmqttd" ./cmd/pgmqttd

echo "==> launching broker (will run migrations)"
PGMQTT_DATABASE_URL="$DB_URL" PGMQTT_TCP_ADDR=127.0.0.1:11883 PGMQTT_WS_ADDR=127.0.0.1:18083 \
  "$ROOT/pgmqttd" >"$ROOT/.smoke-broker.log" 2>&1 &
BROKER_PID=$!

echo "==> waiting for broker"
for _ in $(seq 1 60); do
  if (echo > /dev/tcp/127.0.0.1/11883) 2>/dev/null; then
    break
  fi
  sleep 0.2
done

echo "==> seeding test user"
HASH="$(go run ./scripts/internal/hash.go test)"
docker exec -e PGPASSWORD=pgmqtt "$PG_NAME" psql -U pgmqtt -d pgmqtt -c \
  "INSERT INTO users(username,password_hash) VALUES('test', '${HASH}') ON CONFLICT (username) DO UPDATE SET password_hash=EXCLUDED.password_hash;"

if command -v mosquitto_pub >/dev/null && command -v mosquitto_sub >/dev/null; then
  echo "==> mosquitto round-trip"
  ( mosquitto_sub -h 127.0.0.1 -p 11883 -u test -P test -t 'smoke/#' -C 1 -W 5 \
      | tee "$ROOT/.smoke-recv.txt" ) &
  SUB=$!
  sleep 0.5
  mosquitto_pub -h 127.0.0.1 -p 11883 -u test -P test -t 'smoke/test' -m hello -q 1
  wait "$SUB"
  if grep -q hello "$ROOT/.smoke-recv.txt"; then
    echo "OK"
  else
    echo "FAIL: did not receive expected message" >&2
    exit 1
  fi
else
  echo "==> mosquitto_pub/sub not found, running Go round-trip"
  go test ./internal/engine/... -run TestQoS1PublishSubscribe -count=1
fi

echo "==> smoke OK"
