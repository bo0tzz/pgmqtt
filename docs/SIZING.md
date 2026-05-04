# Sizing guide

Rules of thumb for picking pod counts, resources, Postgres connection
limits, and when to consider non-default indexes. **All numbers are
order-of-magnitude on a homelab profile** (sub-millisecond Postgres
latency, average payload < 1 KB). Re-measure for production traffic.

For per-stage attribution of inbound and outbound latency on your
own hardware, see [`PERF.md`](PERF.md). The histograms there
(`pgmqtt_publish_seconds`, `pgmqtt_delivery_seconds`,
`pgmqtt_janitor_tick_seconds`) make the rules of thumb here checkable
against measurement.

## Pod sizing

The broker is mostly I/O-bound (TCP read/write + Postgres queries) and
spends very little CPU per packet. The supplied default resource block
in `values.yaml`:

```yaml
resources:
  requests:
    cpu: 100m
    memory: 128Mi
  limits:
    memory: 512Mi
```

…handles roughly **5,000 idle connections + ~1,000 msg/s sustained**
per Pod on a modern x86_64 core. Doubling either dimension wants:

- Connections: ~4 KB of resident memory per idle conn (kernel TCP +
  Go runtime). 10k conns ≈ 40 MB just for sockets, plus per-conn Go
  goroutines/stacks at ~8 KB → another 80 MB. Bump request to 256 Mi
  if you cross 10k conns.
- Messages/s: each PUBLISH is one Postgres `mqtt_publish` round-trip.
  After the v0.1.x perf round (FK drop in migration 0006, partial
  indexes in 0007/0008, in-broker packet ID counter in 2B), the
  homelab/idle-Postgres ceiling is bounded by the
  `mqtt_publish_query` stage's tail latency and the per-Conn write
  goroutine's CPU. At 1k msg/s the publish stage is sub-ms; at 10k+
  msg/s expect Postgres to saturate on `pg_xact_commit` before the
  broker does. See `PERF.md` for what each histogram bucket actually
  measured on a contended kind run, and recalibrate from
  `pgmqtt_publish_seconds` and `pgmqtt_delivery_seconds` on your own
  hardware before treating these as targets.

## When to scale out vs. up

Scale **out** (more replicas) when:

- A single Pod's Postgres pool exhausts — `pgmqtt_pgx_in_use_conns ≈
  pgmqtt_pgx_total_conns` for sustained periods. Adding Pods
  multiplies the total pool (each Pod owns its own pgxpool).
- You hit `PGMQTT_MAX_CONNECTIONS` (default 5000) on a Pod and want
  more headroom without bumping the per-Pod cap.
- You want N+1 redundancy across nodes / AZs.

Scale **up** (more CPU/memory per Pod) when:

- Per-conn keepalive timers + drain loops are getting starved (Go
  runtime metrics show GC pause > 10 ms or scheduler latency rising).
- Single-publisher fanout to thousands of subscribers is taking
  whole-CPU time (rare unless you're running a single mega-fanout
  topic at high QPS).

## Postgres connection count

Each Pod opens:

- One dedicated `*pgx.Conn` for the listener (always-on).
- One dedicated `*pgx.Conn` if it's currently the leader (advisory
  lock 42).
- A `pgxpool.Pool` whose max-conn is set via the
  `pool_max_conns` parameter on the connection string. Default is 5.

So **two pods** on a leader+follower split → at most `2 × (1 + 5) + 1 = 13`
connections. Multiply your `pool_max_conns` × replicas, plus `replicas + 1`
for the listener / leader connections, and that's your minimum
`max_connections` setting in `postgresql.conf`.

Recommended starting points:

| Pods | Steady traffic | `pool_max_conns` | postgres `max_connections` |
| - | - | - | - |
| 2 | < 500 msg/s | 5 | 25 |
| 2 | 1k–5k msg/s | 25 | 80 |
| 4 | 1k–5k msg/s | 25 | 130 |
| 4 | 10k+ msg/s | 50 | 250 |

Beyond ~250 connections, prefer **PgBouncer in transaction mode** in
front of Postgres rather than bumping `max_connections` further.
pgmqttd's queries are short and pgbouncer-compatible (no LISTEN on
the *pool* — those are on dedicated `*pgx.Conn`s outside the pool, so
pgbouncer doesn't break LISTEN/NOTIFY).

## Memory ballpark per N connections

Per-conn memory cost is dominated by Go runtime overhead, not message
buffering:

- Goroutine stack: ~8 KB (the per-conn read goroutine).
- Connection struct (`*Conn` + per-conn fields): ~1 KB.
- Read buffer: 4 KB minimum.
- Per-conn topic-alias map (v5 only, when negotiated): a few KB.

→ **~16 KB resident per active conn.** 10k conns ≈ 160 MB on top of
base Go runtime (~30 MB). Bump the `resources.limits.memory` to
512 Mi for 10k–20k conns; 1 Gi covers ~50k.

`messages` and `deliveries` rows live in Postgres, so no broker-side
ballooning under load — the Postgres data dir grows instead.

## When to consider ltree indexes

Today subscriptions are matched topic-by-filter via the
`mqtt_topic_match` SQL function (a regex-like `~` against a BTRee
on `topic_filter`). It's good to about ~10k subscriptions on one
topic-prefix segment.

Consider PostgreSQL's `ltree` extension when:

- `EXPLAIN ANALYZE` on the `subscriptions ⋈ mqtt_topic_match` join
  shows a sequential scan dominating fanout latency.
- Subscription count crosses ~50k *and* fanout latency p99 > 50 ms.

Migrating to ltree is not free: the topic format becomes
`a.b.c.d`-shaped instead of `a/b/c/d`, and the codec needs
translation at ingest. We did not implement this in v1; track it as
a future optimisation if you're seeing the symptoms above. Section 6
of the original ultraplan called it out as out of scope.

## Disk usage

The `messages` table grows with traffic but is sweep-cleaned by the
janitor (orphan messages older than `orphanGrace` — default 10 min —
get deleted once nobody references them via `deliveries`). Sustained
1k msg/s with 1 KB payloads + 10 subscribers each → ~10k rows
buffered for ~10 minutes → ~100 MB working set in `messages`. Add
WAL overhead at ~3× and budget 300 MB/min × retention.

`retained` grows monotonically with the topic count of retained
messages. v5 message-expiry naturally trims this; for v3.1.1, the
operator must explicitly publish empty payloads to clear retained
slots they no longer need.

## Smoke targets

If you're building a sizing test, the targets we use internally:

- 1k msg/s for 10 minutes with 100 subscribers — broker CPU < 30%, no
  client disconnects.
- 10k connection idle hold for 30 minutes — broker memory stable
  (no growth past steady state), keepalive PINGREQs all answered.
- A 30-second `kubectl delete pod pgmqttd-0 && pgmqttd-1` rotation
  during traffic — no QoS-1 loss, QoS-2 no duplicates (verify via
  client-side counters).

## Calibration measurements actually taken

The numbers above are derived from the v0.1.x perf work plus the
post-FK-drop re-measurement; they are **not** the result of running
the smoke targets above as a calibrated benchmark. PERF.md records
what was actually measured (a contended kind cluster, ~287 msg/s
post-fix on the soak shape, with the bottleneck shifted from
MultiXact SLRU thrash to the resume-scan path that 0008 then
addressed). A clean dedicated-host re-measurement on the same shape
is filed as a separate task and will replace the order-of-magnitude
numbers above with measured ones once it lands. Until then, treat
this guide as a starting point and confirm with `pgmqtt_*_seconds`
histograms on your deployment.
