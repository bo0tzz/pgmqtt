# pgmqtt

A stateless MQTT 3.1.1 + 5 broker that uses Postgres as the only
coordination substrate. Run N replicas behind one Kubernetes Service;
killing any Pod is safe — no Raft, no gossip, no embedded KV.

## Why?

Mosquitto loses everything when its Pod restarts. EMQX/VerneMQ ship
their own clustering, persistence, and ops surface. `pgmqtt` picks a
different trade: every concern that needs durability or fanout goes
through a Postgres you almost certainly already run. Each broker Pod
holds only TCP sockets, a per-Pod `client_id → *Conn` map, a
`pgxpool`, and one dedicated `*pgx.Conn` for `LISTEN` +
`pg_advisory_lock`. Cross-Pod fanout is `pg_notify`. There is no
singleton leader.

When a Pod dies, its clients reconnect to another one through the
Service. The surviving Pods notice the dead broker by its released
advisory lock and reap it about 30–35 s later. Wills for its clients
that have not reconnected by then are published no sooner than that.
The wait is deliberate: a Postgres restart frees every broker's lock
for a few seconds, and that must not look like every broker dying.

Conformance, measured with the Eclipse Paho interoperability suite
(`scripts/paho-conformance.py` runs both protocol versions): v3.1.1
9/10, v5 23–25 of 27. `test_subscribe_failure` (both versions) and
`test_shared_subscriptions` always fail, because pgmqtt has no ACLs and
no shared subscriptions. Two more v5 tests, `test_request_response` and
`test_subscribe_options`, publish without waiting for their own
subscribe to be acknowledged; they fail in most single-broker runs
(23/27) and usually pass against a multi-broker cluster (25/27).

## Quick start (Kubernetes)

You need Postgres reachable from your cluster. pgmqtt is developed and
tested against Postgres 18 only; nothing verifies an older version.

```bash
kubectl create namespace mqtt
kubectl -n mqtt create secret generic pgmqtt-db \
    --from-literal=uri='postgres://pgmqtt:secret@postgres.default.svc:5432/pgmqtt?sslmode=disable'

helm install pgmqtt deploy/helm/pgmqtt \
    --namespace mqtt \
    --set database.existingSecret=pgmqtt-db \
    --set replicaCount=2

# Provision a user via CR; the operator generates a credentials Secret.
kubectl apply -f - <<'YAML'
apiVersion: pgmqtt.io/v1alpha1
kind: User
metadata:
  name: homeassistant
  namespace: mqtt
YAML

kubectl -n mqtt get secret homeassistant-mqtt-credentials \
    -o jsonpath='{.data.uri}' | base64 -d
```

For TLS, front the broker with an L4 terminator (HAProxy, ingress-nginx
`tcp-services`, etc.) for `mqtts://1883` and any HTTPS proxy for
`wss://8083/mqtt`. The broker itself does not terminate TLS.

## Quick start (local)

```bash
docker run --rm -d --name pgmqtt-pg -p 5432:5432 \
    -e POSTGRES_USER=pgmqtt -e POSTGRES_PASSWORD=pgmqtt \
    -e POSTGRES_DB=pgmqtt postgres:18-alpine

export PGMQTT_DATABASE_URL='postgres://pgmqtt:pgmqtt@localhost:5432/pgmqtt?sslmode=disable'
go run ./cmd/pgmqttd
```

## Configuration

Most knobs have sensible defaults. The ones you may want to set:

| Env var | Default | Purpose |
| - | - | - |
| `PGMQTT_DATABASE_URL` | (required) | Postgres connection URL |
| `PGMQTT_TCP_ADDR` | `:1883` | MQTT-over-TCP listener |
| `PGMQTT_WS_ADDR` | `:8083` | MQTT-over-WS listener |
| `PGMQTT_METRICS_ADDR` | `:9090` | Prometheus `/metrics` |
| `PGMQTT_LOG_LEVEL` | `info` | `debug` / `info` / `warn` / `error` |
| `PGMQTT_LOG_FORMAT` | `text` | `text` or `json` |
| `PGMQTT_BCRYPT_COST` | `10` | Auth bcrypt cost (4–31) |
| `PGMQTT_MAX_CONNECTIONS` | `5000` | Per-Pod connection cap |

The full list is in `internal/config/config.go`; chart values are
documented inline in `deploy/helm/pgmqtt/values.yaml`.

## Development

```bash
go test ./...                       # full suite; DB tests need Docker and skip without it
make validate TIER=tier1            # vet + full suite under -race + helm checks
make validate TIER=tier2 PAHO=...   # adds Paho v3+v5 conformance against one broker
make validate TIER=tier3 PAHO=...   # adds a kind cluster: multi-broker Paho, helm test, soak
```

`PAHO` is a checkout of
[paho.mqtt.testing](https://github.com/eclipse/paho.mqtt.testing).
Every tier needs Docker and fails without it. Timings and what each
tier catches are in
[`scripts/validate-timings.md`](scripts/validate-timings.md).

For agents working in this repo (parallel-worktree pattern, common
pitfalls, validation tier model), see [`AGENTS.md`](AGENTS.md).

## License

MIT.
