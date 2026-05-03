# pgmqtt operations runbook

Day-2 procedures for running `pgmqttd` in production. The broker is
stateless w.r.t. local data — every authoritative piece of state lives
in Postgres — so most "broker is wedged" answers are queries against
the database plus a Pod restart.

The queries assume you connect as a user with read access to all the
tables in `public.`; many of the writes need INSERT/UPDATE on those same
tables.

## At a glance — health checks

```bash
# Connectivity (Service VIP):
kubectl -n mqtt run -it --rm probe --image=ghcr.io/eclipse-mosquitto:2.0 \
  --restart=Never --command -- mosquitto_sub -h pgmqtt -p 1883 -t '$SYS/#' -W 5

# Per-Pod liveness:
kubectl -n mqtt get pods -l app.kubernetes.io/name=pgmqtt -o wide

# Per-Pod broker UUID + leader status (from logs):
kubectl -n mqtt logs -l app.kubernetes.io/name=pgmqtt --tail=50 | grep -E "broker|leader"

# Metrics (one Pod):
kubectl -n mqtt port-forward $(kubectl -n mqtt get pod -l app.kubernetes.io/name=pgmqtt -o name | head -1) 9090:9090
curl -s localhost:9090/metrics | grep -E "pgmqtt_(connections|publishes|deliveries)"
```

Keys to look for in `/metrics`:

- `pgmqtt_connections` — should match `kubectl describe svc pgmqtt | grep Endpoints`-resolved load.
- `pgmqtt_pgx_in_use_conns` near `pgmqtt_pgx_total_conns` for sustained periods → pool exhausted.
- `pgmqtt_deliveries_inflight{state="queued"}` climbing over many scrapes → slow-consumer / dead-broker scenario.
- `pgmqtt_rate_limited_total` increasing → review `PGMQTT_MAX_INBOUND_MSGS_PER_SEC`.
- `pgmqtt_publish_seconds{stage}` — per-stage latency for the inbound
  PUBLISH path. See [`PERF.md`](PERF.md) for what each stage covers and
  how to read the breakdown when PUBACK latency creeps up.

## "Leader stuck" — janitor + reconciler stop running

The leader is whichever Pod currently holds `pg_advisory_lock(42)` on
its dedicated `*pgx.Conn`. Exactly one Pod runs the janitor and the
User-CR reconciler; if it dies hard, the lock should auto-release when
the connection's TCP keepalive trips (~25 s by default).

**Symptoms**

- `pgmqtt_dead_brokers_handled_total` flatlines.
- New `User` CRs don't get a Secret minted within ~5 s.
- Will messages from a hard-dead Pod don't fire (the dead Pod's broker
  ID is still seen as the owner of the will rows).

**Diagnose**

```sql
-- Who currently holds advisory lock 42 (the leader)?
SELECT a.pid, a.application_name, a.client_addr, a.state, l.granted
  FROM pg_locks l
  JOIN pg_stat_activity a USING (pid)
 WHERE l.locktype = 'advisory'
   AND l.objid = 42;

-- The application_name is "pgmqttd-leader" for the leader connection.
-- client_addr is the Pod's IP — cross-reference with `kubectl get pods -o wide`.
```

If you see no rows, no Pod has the lock — the next janitor scan should
acquire it. If 30+ seconds elapse without acquisition, check:
- Are any pgmqttd Pods running and Ready?
- Are they connected to the right Postgres? (`PGMQTT_DATABASE_URL`)
- Is Postgres wedged? (`pg_stat_activity` shows blocked sessions?)

If a row exists but the Pod IP is no longer running, the dead Pod
didn't shed the lock cleanly. Forcing release:

```sql
-- Replace 12345 with the pid from the query above.
SELECT pg_terminate_backend(12345);
```

The next pgmqttd scan picks up leadership within `interval` (default 1 s).

## "Zombie session ownership" — broker_id points at a dead Pod

A session row's `broker_id` is the Pod that currently owns the client.
On graceful shutdown the Pod clears it; on hard kill the janitor's
`findDeadBrokers` scan re-claims it via `pg_try_advisory_lock`. If the
janitor is also down (see "Leader stuck"), `broker_id` may stay stale
and inbound NOTIFY fanout for that client goes nowhere.

**Diagnose**

```sql
-- Sessions whose broker_id no longer corresponds to any held lock:
SELECT s.client_id, s.broker_id, s.connected, s.last_seen
  FROM sessions s
 WHERE s.broker_id IS NOT NULL
   AND NOT EXISTS (
     SELECT 1 FROM pg_locks l
      WHERE l.locktype = 'advisory'
        AND l.classid = hashtextextended('pgmqtt:broker:' || s.broker_id::text, 0)::int
   );
-- (The classid math here is illustrative; the broker UUID hashes via
--  hashtextextended in the broker code path. The simpler check is to
--  list distinct broker_ids in sessions and grep for any not also seen
--  in pg_stat_activity from your pgmqtt Pods.)
```

**Manual cure** (only when the janitor isn't going to come back soon):

```sql
-- Clear ownership for a specific stuck client. The session row stays;
-- the next CONNECT for the client takes over normally.
UPDATE sessions
   SET connected = false,
       broker_id = NULL,
       will_topic = NULL, will_payload = NULL, will_qos = NULL,
       will_retain = NULL, will_delay = NULL, will_properties = NULL
 WHERE client_id = $1;
```

The will fields are nulled because the Pod that would have fired the
will is gone — either fire it manually first if it still matters, or
accept it as lost.

## "Stuck delivery row" — message neither sent nor freed

`deliveries` rows in state 0 (queued), 1 (sent, awaiting PUBACK), or 2
(awaiting PUBCOMP) for a session that's currently disconnected are
expected — they'll resume on the client's next CONNECT (clean_start=false
+ SessionExpiryInterval>0). Rows for a *connected* session that don't
move within seconds indicate the broker thinks they've been sent but
the wire hasn't ack'd.

**Diagnose**

```sql
SELECT d.id, d.client_id, d.qos, d.state, d.packet_id,
       extract(epoch from now() - m.created_at) AS age_seconds,
       m.topic
  FROM deliveries d
  JOIN messages m ON m.id = d.message_id
  JOIN sessions s ON s.client_id = d.client_id
 WHERE s.connected = true
   AND d.state IN (1, 2)
   AND m.created_at < now() - interval '60 seconds'
 ORDER BY age_seconds DESC LIMIT 50;
```

If you see rows where age > a few seconds, the client is alive but slow
or stuck. With `PGMQTT_MAX_QUEUED_DELIVERIES_PER_CLIENT` set the broker
will eventually DISCONNECT 0x97 the conn (see `pgmqtt_quota_exceeded_total`).
Otherwise:

```sql
-- Force-clear a single delivery (loses the message for that subscriber):
DELETE FROM deliveries WHERE id = <row id>;
```

`deliveries.client_id` cascades from `sessions.client_id`, so deleting
the session row is the nuclear option that clears all outstanding
deliveries for that client at once.

## Postgres connection limits — pool exhaustion

`pgxpool` opens connections lazily up to its max-conns setting. The
broker ships with library defaults (5 connections per pool); each Pod
owns one pool plus a dedicated `*pgx.Conn` for the listener. Two Pods
default to 12 connections at peak.

**Diagnose**

```sql
-- Pgmqtt-attributable backend count:
SELECT application_name, state, count(*)
  FROM pg_stat_activity
 WHERE application_name LIKE 'pgmqttd%'
 GROUP BY application_name, state;

-- Postgres' own ceiling:
SHOW max_connections;
```

If `pgmqtt_pgx_in_use_conns ≈ pgmqtt_pgx_total_conns` for sustained
periods, the pool is saturated; Acquire calls block. Symptoms include
slow PUBLISH/SUBSCRIBE handling and `pgmqtt_pgx_acquire_duration_seconds_total`
growing fast.

**Fix**

- Bump Postgres `max_connections` (postgresql.conf) before adjusting
  pgxpool — the latter is bounded by the former.
- The pgxpool max-conns is set via the connection-string parameter
  `pool_max_conns` in `PGMQTT_DATABASE_URL`. Set it to roughly
  `(your max_connections − reserve_for_other_apps − N_pgmqttd_pods × 1)
  / N_pgmqttd_pods`.
- For a sustained busy broker, 25–40 per Pod is a reasonable starting
  point; revisit if pgxpool acquire latency stays > 5 ms.

## Schema migrations during a rolling restart

Migrations run on every Pod start. They acquire a different advisory
lock (`migrateLockKey`) so concurrent Pod starts queue rather than
race. Each migration is idempotent (`CREATE TABLE IF NOT EXISTS`,
`CREATE OR REPLACE FUNCTION`, etc.).

**What's safe:**

- Adding columns / indexes / new tables — old Pods keep running with
  the old schema; new Pods see the new schema. Both can coexist as
  long as the *old* Pods don't break on the *new* schema (they don't,
  because they only touch columns they know about).
- Adding new migrations between releases.

**What's not safe:**

- Renaming or dropping columns that the old code reads.
- Changing `mqtt_publish` SQL function signatures without a
  `DROP FUNCTION IF EXISTS old_signature` first (we do this in
  `0004_message_expiry.sql` and `0005_quota.sql` — keep it in mind for
  future changes).
- Long table-rewriting migrations during peak traffic — these acquire
  ACCESS EXCLUSIVE on the target table. Run during low-traffic windows.

## Forced restart of a single Pod

```bash
kubectl -n mqtt delete pod <pod-name>
```

The deleted Pod's clients reconnect to its peers within ~keepalive.
The dedicated listener connection drops, so its advisory lock auto-
releases inside ~25 s; the surviving Pod's janitor then picks up the
ownership of the dead Pod's sessions and fires their wills.

## Database connection emergency switchover

If Postgres needs to be replaced (failover, restore from backup):

1. Drain pgmqtt: `kubectl -n mqtt scale deployment pgmqtt --replicas=0`
2. Switch `PGMQTT_DATABASE_URL` (Helm `--set database.url=...` or the
   external Secret).
3. Apply migrations against the new DB if not already present.
4. Scale up: `kubectl -n mqtt scale deployment pgmqtt --replicas=2`

Clients lose connection during step 1 and reconnect after step 4. v5
clients with a persistent session resume their inflight QoS-1/QoS-2
state on reconnect.
