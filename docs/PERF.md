# Performance: measuring + interpreting

This is a runbook for the question *"why is my pgmqtt slower than I
expected?"*. It assumes you've read [OPS.md](OPS.md) for the basic
metrics surface and have Prometheus + Grafana scraping `/metrics`.

The TL;DR for sizing: at QoS-1, sustained per-connection throughput is
bounded by `RTT + Postgres COMMIT time`. Postgres COMMIT time is
`fsync` time when `synchronous_commit=on`, which is the default and
the right setting for a broker that promises at-least-once delivery.
On NVMe with a battery-backed write cache, `fsync` is ~1 ms; on a
container volume on overlay storage, expect 10–30 ms. Total system
throughput scales linearly with parallel publisher connections.

## What's instrumented

The broker exposes `pgmqtt_publish_seconds`, a histogram broken down
by stage of the inbound PUBLISH path:

| Stage                | Covers                                                  |
| ---                  | ---                                                     |
| `total`              | `handlePublish` entry → return                          |
| `qos2_dedup`         | The `inbound_qos2` upsert (QoS-2 only)                  |
| `retain`             | The `retained` UPDATE/DELETE (retain=true only)         |
| `tx_begin`           | `pool.BeginTx` — pool acquire + `BEGIN` round-trip      |
| `mqtt_publish_query` | The `SELECT FROM mqtt_publish(...)` row + scan          |
| `tx_commit`          | The `COMMIT` — fsync-bound on `synchronous_commit=on`   |
| `notify`             | NOTIFY to peer Pods via the delivery channel            |
| `response_write`     | PUBACK (QoS-1) or PUBREC (QoS-2) packet write           |

`pgxpool` connection pool depth and acquire latency surface as
`pgmqtt_pgxpool_*` (registered via the pool collector).

## Reading the histograms

`pgmqtt_publish_seconds_sum / pgmqtt_publish_seconds_count` per stage
gives mean stage time. For tail latency use the standard histogram
quantile pattern:

```promql
histogram_quantile(0.99,
  sum by (le, stage)(rate(pgmqtt_publish_seconds_bucket[5m])))
```

A typical breakdown on a healthy cluster (NVMe-backed Postgres):

| Stage                | Median  | p99    |
| ---                  | ---     | ---    |
| `total`              | 2 ms    | 8 ms   |
| `tx_begin`           | 0.1 ms  | 1 ms   |
| `mqtt_publish_query` | 0.5 ms  | 3 ms   |
| `tx_commit`          | 1 ms    | 5 ms   |
| `notify`             | 0.1 ms  | 1 ms   |
| `response_write`     | 0.05 ms | 0.5 ms |

If `total` p99 is much higher than the sum of the per-stage p99s,
something is happening between stages — typically goroutine scheduling
under CPU pressure.

## Common patterns

**Slow `tx_commit`, everything else fast.** This is fsync. Either
your disk is slow (kind on overlayfs is the canonical example), or
Postgres' WAL volume is contended. Check `pg_stat_io` (PG16+) for
`fsync_time`:

```sql
SELECT backend_type, object, fsyncs, fsync_time
  FROM pg_stat_io
 WHERE backend_type = 'client backend'
   AND fsyncs > 0;
```

If `fsync_time / fsyncs` is in the tens of ms on supposedly-fast disk,
the disk path is the issue (kernel block cache, virtualised storage,
network-attached volume).

**Slow `tx_begin`, everything else fast.** Pool exhaustion. Check
`pgmqtt_pgxpool_acquire_seconds_total / pgmqtt_pgxpool_acquire_count`
for mean acquire time. If it's > 1 ms you're contending on a small
pool. Bump `PGMQTT_DB_MAX_CONNS` (default 10) if your worker count
warrants it.

**Slow `mqtt_publish_query`.** This is the fanout SQL — the work of
expanding subscribers and writing delivery rows. If it dominates,
you have either a very fanned-out topic (many matching subscribers)
or `pg_stat_statements` will tell you which subquery is slow:

```sql
SELECT query, calls, mean_exec_time, total_exec_time, rows
  FROM pg_stat_statements
 WHERE query ILIKE '%mqtt_publish%'
 ORDER BY total_exec_time DESC
 LIMIT 10;
```

**Slow `notify`.** A NOTIFY queue is in shared memory and bounded.
If LISTENers are slow to consume, NOTIFY blocks. Check
`pg_listening_channels()` from a `psql` session — should return
`pgmqtt_deliveries` and the per-broker quota channels. If brokers
get behind on LISTEN, the queue grows; PG's `pg_notification_queue_usage()`
returns the fraction filled (0.0..1.0). > 0.5 means a broker has
gone quiet.

## Enabling `pg_stat_statements`

Not enabled by default in `postgres:16-alpine`. To turn on:

```ini
# postgresql.conf
shared_preload_libraries = 'pg_stat_statements'
pg_stat_statements.track = all
pg_stat_statements.max = 10000
```

Then in the `pgmqtt` database:

```sql
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;
```

For the kind dev cluster, the easiest path is a configmap with the
above and mount it as `/etc/postgresql/postgresql.conf`, plus a
`postgres -c config_file=/etc/postgresql/postgresql.conf` command
override. The CI manifest in `.github/ci/postgres.yaml` is intentionally
minimal — it doesn't enable the extension; perf runs need a tweaked
copy.

## Deciding to optimise

The two main broker-side levers (in increasing complexity):

1. **Connection-pool tuning**. If `tx_begin` is your dominant stage
   under load, raise `PGMQTT_DB_MAX_CONNS`. Cheap.
2. **Async PUBLISH dispatch with pipelined commits**. If `tx_commit`
   dominates and your disk is already as fast as it'll get, the broker
   could pipeline up to `serverReceiveMaximum` (100) commits per
   connection and let Postgres group-commit them. Estimated 5–10×
   single-connection win on slow disks. Real engineering project,
   not a tweak. Profile first.

The two main Postgres-side levers:

1. **Faster disk** — NVMe with battery-backed write cache. Single
   biggest no-brainer.
2. **`commit_delay` + `commit_siblings`** — small group-commit window
   that helps when there are multiple concurrent publisher transactions.
   Useless for a single-connection workload.

What to **not** do: `synchronous_commit=off` or UNLOGGED tables.
Both give up the at-least-once durability promise. A PUBACK for a
message Postgres later loses is exactly the failure mode QoS-1 exists
to prevent.

## Validating throughput on your hardware

The soak rig at `cmd/soak/main.go` doubles as a benchmarking tool:

```bash
go build -o /tmp/soak ./cmd/soak
/tmp/soak -broker $BROKER:1883 -user '' -pass '' \
  -duration 60s -rate 5000 -qos 1 \
  -pubs 5 -inflight 50 -subs 3 -topic perf/baseline
```

Output is JSON: `published`, per-sub `received/dups/lost`, totals.
`-pubs N` linearly scales the writer fanout; `-inflight N` enables
the pipelined publisher (replays un-ACKed seqs on reconnect, drains
PUBACKs at end-of-run). Keep `-inflight` ≤ broker's
`serverReceiveMaximum` (default 100).

For comparing against a reference broker, the same rig runs against
Mosquitto, EMQX, or any MQTT 3.1.1+5 broker — the metrics shape
(published / received / lost / dups) is broker-independent.
