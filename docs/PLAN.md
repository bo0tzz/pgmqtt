# pgmqtt — design plan

This is the design plan that drove the v1 implementation. It captures *why*
each subsystem exists, not just *what* it does.

## Goal

Run an MQTT 3.1.1 + 5.0 broker in a homelab Kubernetes cluster as N
identical Pods, with no broker-specific clustering layer. Postgres provides
durability and coordination; killing any Pod is safe.

## Architecture

Each `pgmqttd` Pod holds **only**:

- TCP/WS sockets for currently-connected clients (inherent to TCP).
- A `client_id → *Conn` map (a routing index, not a source of truth).
- A `pgxpool.Pool` for queries.
- A random UUID generated at startup, used as `sessions.broker_id`.
- One dedicated `*pgx.Conn` doing `LISTEN pgmqtt_<uuid>` +
  `LISTEN pgmqtt_takeover_<uuid>` and holding `pg_advisory_lock(...)` for
  its lifetime.
- A reconciler goroutine and a janitor goroutine, both gated by a global
  leader-election advisory lock so only one Pod runs each at a time.

Everything that affects routing or persistence — subscriptions, retained,
inflight, will, session ownership — lives in Postgres. There is **no**
subscription tree, retained map, or inflight queue in process memory.

### Liveness without a brokers table

There is no `brokers` table. Pod liveness is the advisory lock: as long as
the listener's `*pgx.Conn` is alive, the lock is held. When the Pod
crashes, is partitioned, or is killed, the connection drops and Postgres
releases the lock automatically. We tune
`tcp_keepalives_idle/interval/count` on that connection so a partitioned
Pod's lock releases within ~25 s instead of the default 2 h.

### Takeover

When a client reconnects with the same `client_id` to a different Pod, the
new owner's CONNECT path:

1. CASes `sessions.broker_id` to itself in a single transaction.
2. If the prior `broker_id` differs from self, emits
   `pg_notify('pgmqtt_takeover_<prev>', client_id)`.
3. The prior owner Pod's listener picks up the NOTIFY, looks up the local
   client_id→\*Conn map, closes that socket.

If the prior Pod is dead, it never receives the NOTIFY — but the new
connection is fully functional regardless. The leader-elected janitor
eventually clears the dead Pod's ownership rows.

### Codec

We use [`github.com/mochi-mqtt/server/v2/packets`](https://pkg.go.dev/github.com/mochi-mqtt/server/v2/packets)
as a **codec** only — not the broker. MQTT 3.1.1 (2014) and 5.0 (2019) are
frozen specs; the codec is just a parser/serializer with thorough tests.
Mochi's broker has slowed maintenance, but at the codec level there's
nothing to maintain.

### Postgres library

`pgxpool` for query traffic. Raw `*pgx.Conn` (not pooled) for the listener
and the leader, because pooled connections lose `LISTEN` subscriptions when
returned and lose advisory locks held on them. `pgx.Conn.WaitForNotification`
is the listener's loop primitive.

## Schema (migrations/0001 + 0002)

| Table | Purpose |
| - | - |
| `users` | username + bcrypt password |
| `sessions` | one row per known client_id, including `broker_id UUID` (owner Pod) and the will fields |
| `subscriptions` | one row per (client_id, topic_filter) with v5 options (no_local, retain_as_published, retain_handling, subscription_id) |
| `retained` | one row per topic |
| `messages` | publishes; referenced by `deliveries`. Swept by the janitor when no deliveries reference them and they're > 10 min old. |
| `deliveries` | per-recipient delivery state machine (queued → sent → pubrec-received) |

Helper functions:

- `mqtt_topic_match(filter, topic)` — single-segment `+` and trailing `#`,
  with the `$SYS` exclusion rule.
- `mqtt_next_packet_id(client_id)` — atomic per-session packet-id allocator
  that wraps at 65535 and skips IDs reserved by un-acked deliveries.
- `mqtt_publish(...)` — single-RT publisher path: insert message, fan out
  `deliveries`, return the set of broker IDs to NOTIFY.

## Coordination flows

### CONNECT (single transaction)

```
SELECT broker_id FROM sessions WHERE client_id=$1 FOR UPDATE        -> prev
INSERT/UPDATE sessions SET broker_id=self, connected=true, will_*=...
if prev IS NOT NULL AND prev <> self:
    pg_notify('pgmqtt_takeover_<prev>', client_id)
if clean_start:
    DELETE FROM subscriptions, deliveries WHERE client_id=$1
session_present := !clean_start AND existed
send CONNACK(session_present)
register $client_id → *Conn locally
drain queue (states 0,1,2)
```

### PUBLISH (publisher path)

```
if retain: UPSERT/DELETE retained
    -- one transaction:
INSERT INTO messages(...) RETURNING id
INSERT INTO deliveries (client_id, message_id, qos, state)
SELECT s.client_id, msg_id, LEAST(s.qos, $qos), 0
FROM subscriptions s JOIN sessions sess USING (client_id)
WHERE mqtt_topic_match(s.topic_filter, $topic)
  AND NOT (s.no_local AND s.client_id = $publisher)
  AND (LEAST(s.qos, $qos) > 0 OR sess.connected)
RETURNING client_id
SELECT array_agg(DISTINCT sess.broker_id) FROM ...
COMMIT
foreach broker_id: pg_notify('pgmqtt_<broker_id>', msg_id::text)
respond PUBACK / PUBREC per QoS
```

### Receive a `pgmqtt_<self>` NOTIFY

```
parse msg_id
SELECT id, client_id, qos, state, packet_id, topic, payload, properties
FROM deliveries d JOIN sessions s ON s.client_id = d.client_id
JOIN messages m ON m.id = d.message_id
WHERE d.message_id = $1 AND s.broker_id = $self AND d.state IN (0,1)
foreach row:
    if state=0: allocate packet_id via mqtt_next_packet_id(client_id), set state=1
    look up *Conn locally (skip if not connected)
    encode + write PUBLISH (DUP if state>=1)
```

### Janitor (leader-only, every 10 s)

```
SELECT DISTINCT broker_id FROM sessions WHERE broker_id IS NOT NULL
foreach broker_id:
    if pg_try_advisory_lock(hashtextextended('pgmqtt:broker:'||broker_id, 0)):
        -- this Pod is dead; we now hold its lock
        SELECT will_* FROM sessions WHERE broker_id=$1 AND will_topic IS NOT NULL
        foreach will: engine.PublishWill(...)
        UPDATE sessions SET connected=false, broker_id=NULL, will_*=NULL WHERE broker_id=$1
        pg_advisory_unlock(...)
DELETE FROM messages
 WHERE created_at < now() - 10 min
   AND NOT EXISTS (SELECT 1 FROM deliveries WHERE message_id=messages.id)
```

### User reconciler (leader-only)

CRD-driven: a `pgmqtt.io/v1alpha1.User` is the only way to create a user.
Spec is just `username` + optional `passwordSecretRef` (BYO). Without
`passwordSecretRef`, the reconciler generates a `<name>-mqtt-credentials`
Secret with `username`, `password`, `host`, `port`, `ws-port`, `uri`,
`ws-uri` — owned by the User so `kubectl delete user` cascades.
