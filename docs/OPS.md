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

# Per-Pod broker UUID + operator leader-election state (from logs):
# (Operator leader-election lines come from controller-runtime's K8s
# Lease implementation; the broker itself emits no "leader" line since
# the leaderless refactor.)
kubectl -n mqtt logs -l app.kubernetes.io/name=pgmqtt --tail=50 | grep -E "broker|leader_election"

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
- `pgmqtt_e2e_publish_to_deliver_seconds` — single-number
  ingest-to-delivery SLO. p99 > 100 ms on a healthy homelab → check
  `publish_seconds` and `delivery_seconds` to localise.
- `pgmqtt_pgx_acquire_seconds` — pool-saturation signal across all
  callers. p99 > 10 ms sustained → bump `pool_max_conns` or scale out.

## "Janitor / reconciler stopped running"

There is no singleton-leader anymore (since the leaderless refactor):

- **Janitor** runs on every Pod independently. Sweep operations are
  concurrency-safe by construction (per-row locks, SKIP LOCKED claims,
  idempotent DELETEs, per-resource `try_advisory_lock`); see
  `internal/janitor/janitor.go` package doc for the per-job analysis.
- **User-CR reconciler** uses controller-runtime's K8s Lease leader
  election. The Lease lives at
  `coordination.k8s.io/leases/<broker-namespace>/pgmqtt-operator`.
  Exactly one Pod's manager reconciles at a time; on loss
  controller-runtime exits the manager and a peer Pod takes over —
  no Pod restart involved.

**Symptoms**

- `pgmqtt_dead_brokers_handled_total` flatlines on EVERY Pod for
  more than a tick or two: the cluster has lost connectivity to PG, or
  no Pod is alive. (One Pod's counter being flat is fine — they only
  increment on the Pod that *won* the per-broker advisory lock for
  that scan tick.)
- `pgmqtt_janitor_errors_total{job=...}` increasing on every Pod →
  PG-side issue, not a leader issue. Check `pg_stat_activity` for
  blocked sessions, `pg_locks` for contention, autovacuum status on
  `sessions`/`deliveries`.
- New `User` CRs don't get a Secret minted within ~5 s →
  reconciler is wedged. Check the Lease:
  ```bash
  kubectl -n mqtt get lease pgmqtt-operator -o yaml
  ```
  `holderIdentity` shows which Pod currently has it. If it's a Pod
  IP that no longer exists, the Lease will time out (default 15 s)
  and a peer takes over. To force handoff: delete the Lease.

  ```bash
  kubectl -n mqtt delete lease pgmqtt-operator
  ```
  Next reconciler in the cluster will recreate it.

## User CRD operations — bcrypt cost rollouts

`PGMQTT_BCRYPT_COST` (or Helm `operator.bcryptCost`) controls the cost
parameter the operator uses when (re)hashing User passwords. It defaults
to 10 (`bcrypt.DefaultCost`). Existing User rows store the cost in the
`$2a$NN$` prefix of their `password_hash` column.

**Bumping cost forces a rehash.** Each User reconcile reads the cost
embedded in the stored hash; if it is below the configured cost, the
reconciler re-`bcrypt`s the existing cleartext (no plaintext rotation,
no Secret update) at the new cost and UPSERTs the row. The metric
`pgmqtt_user_rehash_total{reason="cost_bump"}` increments once per row
rewritten for this reason; `reason="rotation"` covers the
cleartext-changed path (User CR creation, Secret rotation).

**Rehash storm on large fleets.** When operators bump cost from e.g.
10 → 14, every existing User CR triggers exactly one cost_bump reconcile
the next time controller-runtime re-syncs. At default Pod CPU, bcrypt
cost 14 takes ~1 s per row vs. ~70 ms at cost 10. A fleet of 1 000
Users will saturate the operator's reconcile worker for ~16 minutes. To
avoid stalling other reconciles or triggering Lease timeouts:

1. **Bump in stages**: 10 → 12 first, watch
   `pgmqtt_user_rehash_total{reason="cost_bump"}` reach the User CR
   count, then 12 → 13, then 13 → 14. Each stage roughly halves the
   bcrypt time per row vs. doing the full bump at once.
2. **Watch for Lease timeouts** during the rehash — if reconciles take
   long enough that the operator misses the Lease renewal interval (15 s
   default), controller-runtime exits the manager and a peer takes
   over, restarting any in-flight rehash. The metric counter is
   per-process so the absolute count may double-increment across Pods.
3. **For very large fleets** (10 000+ Users) consider scripting a
   manual UPDATE that walks the `users` table out-of-band and rehashes
   in batches with paced sleeps; the operator-driven rollout is fine
   for hundreds of Users but not designed for tens of thousands.

There is no rollback path other than dropping cost back and waiting
for the next reconcile — bcrypt verifies any stored cost regardless of
the current configured value, so a partially-completed bump still
authenticates correctly.

## "Zombie session ownership" — broker_id points at a dead Pod

A session row's `broker_id` is the Pod that currently owns the client.
On graceful shutdown the Pod clears it; on hard kill the janitor's
dead-broker scan re-claims it via `pg_try_advisory_lock`. If every
Pod's janitor is wedged, `broker_id` may stay stale and inbound
NOTIFY fanout for that client goes nowhere.

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
race. Each migration is recorded in `schema_migrations` and skipped
on re-apply; do not re-run migration SQL by hand (most files use bare
`CREATE TABLE` / `ALTER TABLE` and will error on a populated DB).

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
