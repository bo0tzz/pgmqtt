# pgmqtt

A stateless MQTT 3.1.1 + 5 broker that uses Postgres as the only coordination
substrate. Run N replicas behind one Kubernetes Service; killing any Pod is
safe — no Raft, no gossip, no embedded KV, no in-memory routing tables.

```
                                    ┌──────────────────────────────┐
   Home Assistant ──┐                │       Postgres (SoT)         │
   Zigbee2MQTT     ─┼─► Service ─►   │ users sessions subscriptions │
   Tasmota / ESP   ─┘   (1883/8083)  │ retained messages deliveries │
                                     └──────────────────────────────┘
   Pod A  Pod B  Pod C                  ▲    ▲    ▲   │
   pgmqttd pgmqttd pgmqttd               │    │    │   │ NOTIFY pgmqtt_<uuid>
       │     │     │                     │    │    │   │ NOTIFY pgmqtt_takeover_<uuid>
       └─────┴─────┴── pgxpool queries ──┘    │    │   ▼
                                               └────┴── LISTEN +
                                                        advisory-lock holder
```

## Why?

Homelab clusters are great until you start running an MQTT broker on them.
Mosquitto is a single binary and forgets everything when its Pod restarts.
The popular distributed brokers (EMQX, VerneMQ, NanoMQ-cluster) come with
their own clustering, persistence, and ops surface. pgmqtt picks a different
trade: every broker concern that needs durability or fan-out goes through a
Postgres you almost certainly already run.

- **Stateless Pods.** Each `pgmqttd` Pod holds only TCP sockets, a per-Pod
  client_id→\*Conn map, a `pgxpool`, and one dedicated `*pgx.Conn` for
  `LISTEN` + `pg_advisory_lock`. Restart any Pod; clients reconnect.
- **No bespoke clustering.** Postgres is the source of truth. Liveness
  checking is "did the advisory lock release?". Cross-Pod fan-out is
  `pg_notify`. Leader-elected work uses `pg_advisory_lock(42)`.
- **MQTT 3.1.1 and 5.0** in the same daemon. The codec
  ([`mochi-mqtt/server/v2/packets`](https://pkg.go.dev/github.com/mochi-mqtt/server/v2/packets))
  covers both; we use only the codec subpackage, not the in-memory broker.
- **CRD-driven users.** The only way to provision a user is a
  `pgmqtt.io/v1alpha1.User` CR. The leader Pod runs an in-process
  controller-runtime reconciler that materializes them into the `users`
  table and (optionally) generates a credentials Secret per cnpg style.
- **TLS lives outside.** Front pgmqtt with an L4 TLS terminator (Nginx
  `stream`, HAProxy TCP, ingress-nginx `tcp-services`) for `mqtts://1883` and
  any HTTPS proxy for `wss://8083/mqtt`.

## Status

v1 covers everything in the design plan. See [PLAN.md](docs/PLAN.md) and the
verification checklist in [docs/VERIFY.md](docs/VERIFY.md).

### What's NOT in v1

- **ACLs / topic-level authorization.** Auth ends at username.
- **Shared subscriptions** (`$share/{group}/{filter}`) — the wire form is
  parsed but the underlying filter is treated as a normal subscription.
- **MQTT v5 message expiry on retained.** Retained messages persist until
  overwritten or explicitly cleared. v5 `Message Expiry Interval` is stored
  but not enforced.
- **MQTT v5 topic aliases.** Aliases are decoded but not maintained
  per-connection. Add as a `topicAliasMaximum`-aware client side feature.
- **Inbound QoS-2 dedup.** The PUBREL/PUBCOMP dance works on the publisher
  side, but the broker doesn't track inbound packet-id state to suppress a
  duplicate PUBLISH from a buggy client. Realistic clients don't retry mid-
  PUBREL.
- **Bridges, plugin hooks, dashboards.**
- **`ltree`-backed retained/subscription indexes.** Linear scan with the SQL
  match function is fine until tens of thousands of subs.

## Quick start (Kubernetes)

You need Postgres reachable from your cluster. CloudNativePG works great;
any Postgres ≥ 14 is fine.

```bash
# Provision Postgres first, then a Secret with the connection URL:
kubectl create namespace mqtt
kubectl -n mqtt create secret generic pgmqtt-db \
    --from-literal=url='postgres://pgmqtt:secret@postgres.default.svc:5432/pgmqtt?sslmode=disable'

helm install pgmqtt deploy/helm/pgmqtt \
    --namespace mqtt \
    --set database.existingSecret=pgmqtt-db \
    --set replicaCount=2

kubectl apply -f - <<'YAML'
apiVersion: pgmqtt.io/v1alpha1
kind: User
metadata:
  name: homeassistant
  namespace: mqtt
YAML

# A Secret was created at mqtt/homeassistant-mqtt-credentials with a generated
# password and ready-to-use uri/ws-uri values.
kubectl -n mqtt get secret homeassistant-mqtt-credentials -o jsonpath='{.data.uri}' | base64 -d
```

## Quick start (local)

```bash
# Postgres via your favourite means; for testing:
docker run --rm -d --name pgmqtt-pg -p 5432:5432 \
    -e POSTGRES_USER=pgmqtt -e POSTGRES_PASSWORD=pgmqtt \
    -e POSTGRES_DB=pgmqtt postgres:16-alpine

export PGMQTT_DATABASE_URL='postgres://pgmqtt:pgmqtt@localhost:5432/pgmqtt?sslmode=disable'
go run ./cmd/pgmqttd

# In another terminal — seed a user the manual way (no K8s here):
psql "$PGMQTT_DATABASE_URL" -c "INSERT INTO users(username,password_hash) VALUES('test', '\$2a\$10\$...');"
```

## Configuration

| Env var | Default | Purpose |
| - | - | - |
| `PGMQTT_DATABASE_URL` | (required) | Postgres connection URL |
| `PGMQTT_TCP_ADDR` | `:1883` | MQTT-over-TCP listener; empty disables |
| `PGMQTT_WS_ADDR` | `:8083` | MQTT-over-WS listener; empty disables |
| `PGMQTT_SERVICE_HOST` | (auto in helm) | Host advertised in auto-generated User Secrets |
| `PGMQTT_SERVICE_PORT` | `1883` | TCP port advertised in auto-generated Secrets |
| `PGMQTT_SERVICE_WS_PORT` | `8083` | WS port advertised in auto-generated Secrets |
| `PGMQTT_LOG_LEVEL` | `info` | `debug` / `info` / `warn` / `error` |

## Development

```bash
go test ./...               # full unit + integration test suite (uses Docker)
go vet ./...
golangci-lint run           # if you have it installed
helm lint deploy/helm/pgmqtt --set database.url='dev'
```

Integration tests require Docker (testcontainers spins up Postgres). Set
`PGMQTT_TEST_DATABASE_URL` to use an existing DB instead.

## License

MIT.
