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

When a Pod shuts down (a rollout, a drain), it closes its clients'
connections and, as MQTT requires for a connection the server closes,
publishes their wills. A v5 client that should not show as offline
across a rollout sets a Will Delay Interval; reconnecting within it
cancels the will.

A Pod that dies without shutting down (a crash, a lost node) cannot do
that. Its clients reconnect to another Pod through the Service, and the
surviving Pods notice the dead broker by its released advisory lock and
reap it about 30–35 s after the release. Only then are the wills of
its clients that have not reconnected published. The wait is
deliberate: a Postgres restart frees every broker's lock for a few
seconds, and that must not look like every broker dying. The other side
of it: a broker that cannot take its own lock back within 20 s exits,
so a Postgres outage longer than that restarts every broker Pod.
Neither figure is configurable.

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
The chart needs Kubernetes 1.30 or newer (it uses the native preStop
`sleep`) and refuses to install on anything older.

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

The broker does not speak the PROXY protocol, so behind a terminator,
a proxy or SNAT every client reaches it from the same address, and the
per-address limits apply to all of them together. Connects from one
address are capped at `limits.maxConnectsPerIPPerSec` (default 5 a
second), and once an address passes `limits.maxAuthFailuresPerIPPerMin`
failed logins (default 30 a minute) every connection from it is dropped
for 60 s: one client with a wrong password locks out all the others.
The broker logs a warning when an address is locked out. In that setup
raise both limits, or set them to 0 to turn them off.

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
| `PGMQTT_MAX_CONNECTIONS` | `5000` | Per-Pod connection cap. Past it, new connections are closed without a CONNACK, so clients retry |
| `PGMQTT_MAX_QUEUED_DELIVERIES_PER_CLIENT` | `10000` | Undelivered messages one client may have queued, counted separately for QoS 0 and QoS 1/2. A client over either is disconnected with reason 0x97 |
| `PGMQTT_MAX_CONNECTS_PER_IP_PER_SEC` | `5` | Connects per source address; 0 turns it off. See the proxy note above |
| `PGMQTT_MAX_AUTH_FAILURES_PER_IP_PER_MIN` | `30` | Failed logins per source address before it is locked out for 60 s; 0 turns it off |

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
