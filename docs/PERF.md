# Performance: measuring + interpreting

This is a runbook for the question *"why is my pgmqtt slower than I
expected?"*. It assumes you've read [OPS.md](OPS.md) for the basic
metrics surface and have Prometheus + Grafana scraping `/metrics`.

The architectural shape: at QoS-1, sustained per-connection throughput
is bounded by `RTT + the broker's per-PUBLISH cost`, where the
per-PUBLISH cost is dominated by Postgres time (BeginTx + the fanout
SELECT + COMMIT). With `synchronous_commit=on` (the right default for
at-least-once delivery), `COMMIT` includes a WAL fsync.

This document is intentionally light on absolute numbers — calibrated
results require a run on your hardware with the histograms below.
A baseline benchmark suite for pgmqtt's own dev / CI clusters lives
in [VERIFY.md](VERIFY.md); contribute results back if you measure
something interesting.

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

When `total` p99 is materially higher than the sum of the per-stage
p99s, something is happening between stages — typically goroutine
scheduling under CPU pressure, or a stage that isn't yet
instrumented. Cross-check against `process_cpu_seconds_total` and
`go_sched_latencies_seconds`.

## A measured baseline

To avoid the "is my cluster broken or is this normal?" question, here's
one calibrated point — an instrumented kind cluster running on a single
Linux x86_64 host with NVMe storage. **Numbers below are from this
specific shape; they are NOT a target or a guarantee.**

### Setup

- 3 broker replicas (`pgmqtt:perf` build, instrumented), Service VIP
  cluster IP, `auth.allowAnonymous=true`.
- Postgres 16 (alpine), single replica, `emptyDir` volume on the kind
  node's NVMe through Docker's overlay storage. `synchronous_commit=on`,
  `fsync=on`, `wal_sync_method=fdatasync`. `pg_stat_statements` and
  `track_io_timing` enabled.
- Default broker pool config (`MaxConns=8`).
- kind cluster sharing a host with other concurrent workloads (other
  kind clusters running chaos / Mosquitto soaks during the run); CPU
  contention is real, not a clean bench.

### Shape A: single publisher, strict-serial QoS-1 at 50 msg/s target

Rig: `-pubs 1 -inflight 1 -subs 1 -rate 50 -duration 60s`. Result: 2974
publishes, 0 lost / 0 dups. The rig hit its target rate, so the broker
was not the bottleneck at this load.

| Stage                | Mean (ms) | Share |
| ---                  | ---       | ---   |
| `total`              | 3.49      | 100%  |
| `mqtt_publish_query` | 2.17      | 62%   |
| `tx_commit`          | 0.60      | 17%   |
| `tx_begin`           | 0.33      | 9%    |
| `notify`             | 0.27      | 8%    |
| `response_write`     | 0.06      | 2%    |

`pg_stat_io` for the same window: mean fsync **0.109 ms** (NVMe through
Docker overlay is essentially free). `tx_commit`'s 0.60 ms is dominated
by the broker→PG round-trip, not the disk.

The dominant stage is the fanout SELECT inside `mqtt_publish()` — even
with one subscriber. A `pg_stat_statements` breakdown attributes ~37%
of total PG time to the function's outer SELECT and another ~24% to
the embedded `WITH matches` CTE that does the topic-filter JOIN +
delivery insert.

### Shape B: 5 publishers × `-inflight 50`, 3 subs, 5000 msg/s target

Rig: `-pubs 5 -inflight 50 -subs 3 -rate 5000 -duration 60s`. Result:
7545 publishes per sub, 0 lost / 0 dups, ~125 msg/s total = **2.5%
of the rig's 5000/s target**. The broker is the bottleneck here.

What the histograms say (per-Pod, one pod's view):

| Stage                | Mean (ms) under load | Shape A baseline |
| ---                  | ---                  | ---              |
| `total`              | 41.8                 | 3.49             |
| `mqtt_publish_query` | 41.1                 | 2.17             |
| `tx_commit`          | 0.48                 | 0.60             |
| `tx_begin`           | 0.13                 | 0.33             |
| `notify`             | 0.06                 | 0.27             |
| `response_write`     | 0.03                 | 0.06             |

The fanout query went from 2.17 ms to **41 ms** — 19× slowdown — while
fsync, network, and pool acquire stayed the same or got faster. This
is Postgres lock / row contention, not disk. `pg_stat_statements`
during the same window:

- `mqtt_publish` SELECT: **39.9 ms** mean (was 1.7 ms)
- `mqtt_next_packet_id` (per outbound delivery): **6.1 ms** mean
- `UPDATE sessions SET next_packet_id`: **6.0 ms** mean
- Outbound delivery SELECT: 8.3 ms mean

Mean fsync across the same window is still 0.109 ms — disk path is
unaffected.

### What this tells us

- **Fsync is not the bottleneck on this cluster.** A single PUBLISH's
  durable commit is ~0.1 ms of disk time inside a ~0.5–0.6 ms
  `tx_commit` round-trip; the rest is network and PG transaction
  overhead.
- **The fanout SQL is the bottleneck** under concurrency. `mqtt_publish`
  + `mqtt_next_packet_id` + the per-delivery `UPDATE sessions` all
  contend on the same handful of rows when many publishers concurrently
  fan out to the same set of subscribers. This is where future
  optimisation effort goes — not async dispatch, not group commit.
- **Disclosure**: this was a single-host kind cluster with active
  contention from concurrent workloads. Real-hardware multi-node
  Postgres on dedicated WAL volumes will look different. Re-run on
  your shape before treating any of these numbers as a baseline.

## Common patterns

**`tx_commit` dominates.** This is fsync. Either your disk is slow,
or Postgres' WAL volume is contended. Check `pg_stat_io` (PG16+):

```sql
SELECT backend_type, object, fsyncs, fsync_time
  FROM pg_stat_io
 WHERE backend_type = 'client backend'
   AND fsyncs > 0;
```

`fsync_time / fsyncs` gives mean fsync cost. Compare against the
underlying disk's expected fsync — if much slower, the disk path is
indirected (kernel block cache, virtualised storage, copy-on-write
overlay, network-attached volume).

**`tx_begin` dominates.** Pool exhaustion. Check
`pgmqtt_pgxpool_acquire_seconds_total / pgmqtt_pgxpool_acquire_count`
for mean acquire time and compare against `pgmqtt_pgxpool_in_use`
vs `pgmqtt_pgxpool_total`. Saturated pool → bump
`PGMQTT_DB_MAX_CONNS`.

**`mqtt_publish_query` dominates.** This is the fanout SQL — expand
subscribers and write delivery rows. Either the topic is very
fanned-out (many matching subscribers, big delivery insert), or a
specific subquery is slow. `pg_stat_statements` will tell you which:

```sql
SELECT query, calls, mean_exec_time, total_exec_time, rows
  FROM pg_stat_statements
 WHERE query ILIKE '%mqtt_publish%'
 ORDER BY total_exec_time DESC
 LIMIT 10;
```

**`notify` shows up at all.** Postgres' NOTIFY queue is in shared
memory and bounded. If LISTENers fall behind, NOTIFY blocks.
`pg_notification_queue_usage()` returns fill ratio (0.0..1.0); > 0.5
means at least one broker's LISTEN has gone quiet. Check
`pg_listening_channels()` from a `psql` session — pgmqtt brokers
should LISTEN on `pgmqtt_<broker_uuid>`, `pgmqtt_takeover_<uuid>`,
and `pgmqtt_quota_<uuid>`.

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

**Profile before changing anything.** The histograms will tell you
which stage dominates; the right lever depends on which one.

Possible levers, framed by which stage they target — none of these
have been benchmarked yet on documented hardware, so the *whether*
and *how much* is unverified:

- `tx_begin` dominates → raise `PGMQTT_DB_MAX_CONNS`. Cheap.
- `tx_commit` dominates with slow disk → faster disk (NVMe with
  battery-backed write cache).
- `tx_commit` dominates with multiple concurrent publishers →
  `commit_delay` + `commit_siblings` for group commit. Useless on
  single-connection workloads.
- `tx_commit` dominates and disk is already fast → async PUBLISH
  dispatch with pipelined commits inside the broker (let up to
  `serverReceiveMaximum` PUBLISHes per conn fly with PG group commit).
  Real engineering project; not yet implemented.

What to **not** do: `synchronous_commit=off` or UNLOGGED tables.
Both give up the at-least-once durability promise — a PUBACK for a
message Postgres later loses is exactly the failure mode QoS-1
exists to prevent.

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
