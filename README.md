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

Conformance: Paho v3.1.1 9/10, v5 24/27 (`scripts/paho-conformance.py`
runs both — the off-list ones are documented-flaky upstream or
out-of-scope features like ACLs and shared subscriptions).

## Quick start (Kubernetes)

You need Postgres reachable from your cluster. Any Postgres ≥ 14.

```bash
kubectl create namespace mqtt
kubectl -n mqtt create secret generic pgmqtt-db \
    --from-literal=url='postgres://pgmqtt:secret@postgres.default.svc:5432/pgmqtt?sslmode=disable'

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
go test ./...                       # full suite (uses Docker for Postgres)
make validate TIER=tier1            # vet + race + helm lint (~10s)
make validate TIER=tier3 PAHO=...   # adds multi-broker paho + soak smoke
```

For agents working in this repo (parallel-worktree pattern, common
pitfalls, validation tier model), see [`AGENTS.md`](AGENTS.md).

## License

MIT.
