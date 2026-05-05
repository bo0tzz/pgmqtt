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
spends very little CPU per packet, with bcrypt the one CPU spike on
CONNECT auth.

The supplied default resource block in `values.yaml` is sized for the
chart's default `limits.maxConnections=5000`:

```yaml
resources:
  requests:
    cpu: 100m
    memory: 256Mi
  limits:
    memory: 1Gi
```

…and handles **5,000 idle connections + ~1,000 msg/s QoS-1 sustained**
per Pod on a modern x86_64 core.

### Memory: measured (PG18, GOMAXPROCS=8, 2026-05-05)

A 5-point sweep against a freshly-built `pgmqttd` from this commit
gave a clean linear fit above the Go-runtime baseline:

```
RSS_MB ≈ 48 + 0.042 × N_connections      (settled, idle, post-CONNECT)
```

| conns  | measured RSS | heap-in-use | goroutines |
| ---    | ---          | ---         | ---        |
| 0      | ~42 MiB      | ~5 MiB      | 16         |
| 100    | 50.6 MiB     | 8.5 MiB     | 218        |
| 500    | 68.9 MiB     | 14.8 MiB    | 1,018      |
| 1,000  | 87.8 MiB     | 30.9 MiB    | 2,018      |
| 2,500  | 152.3 MiB    | 39.9 MiB    | 5,018      |
| 5,001  | 248.4 MiB    | 68.0 MiB    | 10,018     |

Two goroutines per connection (one read, one drain). Per-conn marginal
RSS settles at ~42 KB once the runtime overhead amortises (above ~500
conns). Below that, baseline overhead dominates the per-conn quotient.

The chart's 1 GiB default gives ~4× burst headroom over the 5k-conn
idle measurement. Burst sources covered: reconnect storms (~4 KB extra
heap per in-CONNECT handshake), retained-flood-on-subscribe.

### Throughput: measured (same shape, single broker pod)

| QoS | Pubs | Inflight | Subs | Achieved msg/s | pub_total mean |
| --- | ---  | ---      | ---  | ---            | ---            |
| 0   | 1    | 1        | 1    | 1,081          | 1.3 ms         |
| 1   | 1    | 1        | 1    | 97             | 2.9 ms         |
| 1   | 1    | 50       | 1    | 504            | 1.9 ms         |
| 1   | 5    | 50       | 3    | 1,070          | 4.6 ms         |
| 1   | 10   | 50       | 5    | 813            | 12.6 ms        |
| 1   | 10   | 100      | 5    | 807            | 12.7 ms        |
| 2   | 1    | 1        | 1    | 97             | 3.2 ms         |
| 2   | 1    | 1        | 1    | 355            | 2.1 ms (target 500) |

Per-pod ceiling on this 8-core dev box: **~1,100 msg/s QoS-0**,
**~1,000 msg/s QoS-1**, **~350 msg/s QoS-2**. The QoS-1 ceiling moves
left as concurrent publishers fan out to overlapping subscribers —
same pattern documented in `PERF.md`. The bottleneck under
multi-publisher saturation is the `mqtt_publish_query` stage (89% of
publish time at 10 concurrent publishers): Postgres lock contention
on the fanout INSERT, exactly as the v0.1.x perf round called out.

### Re-measure on your hardware

Numbers above are calibrated on a single dev box and shouldn't be
treated as targets. Re-run with the soak rig at your shape and read
`pgmqtt_publish_seconds` / `pgmqtt_delivery_seconds` for the real
ceiling. Any of:

- Postgres contention (shared host, slow disk, busy WAL);
- a low CPU-limit cap on the broker pod;
- a bcrypt-CPU connect storm (cost-10 default → ~85 connects/sec/4-core);

…will move the per-pod ceiling around significantly.

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
- A `pgxpool.Pool` whose max-conn is set via the
  `pool_max_conns` parameter on the connection string. Default is 5.

So **two pods** → at most `2 × (5 + 1) = 12` connections (each Pod's
pool plus its listener conn). The general formula is
`replicas × pool_max_conns + replicas`, and that's your minimum
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
