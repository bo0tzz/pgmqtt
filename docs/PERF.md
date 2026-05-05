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

> **Two measurements**: §"PG18 calibration" below is the most recent
> run (single broker, fresh `pgmqttd` from this commit, PG18-alpine,
> dev-box CPU). §"PG16 calibration (historical)" further down is the
> earlier in-cluster run that documented the `mqtt_publish` lock
> contention pattern. Numbers shifted on PG18 (different commit
> latency profile) but the *shape* of the bottleneck is identical:
> `mqtt_publish_query` saturates first under multi-publisher
> concurrency.

## PG18 calibration (2026-05-05)

Single broker pod, `pgmqttd` built from this commit, postgres:18-alpine
container, GOMAXPROCS=8, no CPU limit, allow-anonymous on. 30s/shape.

### Throughput by shape

| QoS | Pubs | Inflight | Subs | Target msg/s | Achieved | pub_total mean | mqtt_publish_query mean |
| --- | ---  | ---      | ---  | ---          | ---      | ---            | ---                     |
| 0   | 1    | 1        | 1    | 100          | 98.2     | 3.8 ms         | 2.0 ms                  |
| 0   | 1    | 1        | 1    | 1,000        | 935.2    | 1.5 ms         | 1.1 ms                  |
| 0   | 1    | 1        | 1    | 5,000        | 1,081    | 1.3 ms         | 1.0 ms                  |
| 1   | 1    | 1        | 1    | 100          | 97.1     | 2.9 ms         | 1.9 ms                  |
| 1   | 1    | 50       | 1    | 1,000        | 503.8    | 1.9 ms         | 1.4 ms                  |
| 1   | 5    | 50       | 3    | 5,000        | 1,069.8  | 4.6 ms         | 3.7 ms                  |
| 1   | 10   | 50       | 5    | 10,000       | 812.7    | 12.6 ms        | 11.2 ms                 |
| 1   | 10   | 100      | 5    | 20,000       | 806.9    | 12.7 ms        | 11.2 ms                 |
| 2   | 1    | 1        | 1    | 100          | 96.9     | 3.2 ms         | 2.0 ms                  |
| 2   | 1    | 1        | 1    | 500          | 355.0    | 2.1 ms         | 1.4 ms                  |

Per-pod ceilings on this hardware:

- **QoS-0** ~1,100 msg/s (pure publish-no-ack)
- **QoS-1** ~1,000 msg/s under heavy fanout (multi-pub × multi-sub)
- **QoS-2** ~350 msg/s (full PUBREC/PUBREL/PUBCOMP handshake)

### Where the time goes (QoS-1, 5 pubs × 50 inflight × 3 subs, near-saturation)

| Stage                | Mean (ms) | Share of pub_total |
| ---                  | ---       | ---                |
| `total`              | 4.63      | 100%               |
| `mqtt_publish_query` | 3.74      | 81%                |
| `tx_commit`          | 0.54      | 12%                |
| `notify`             | 0.12      | 3%                 |
| `tx_begin`           | 0.08      | 2%                 |
| `response_write`     | 0.02      | <1%                |

At 10 publishers (closer to saturation) `mqtt_publish_query` mean
moves from 3.7 ms to **11.2 ms** — exact same lock-contention pattern
documented in the PG16 baseline. The 19× scaling factor that
appeared on the noisy kind cluster is a 3× factor on a clean dev box;
the *direction* matches.

### Idle memory

A 5-point sweep at 0 / 100 / 500 / 1,000 / 2,500 / 5,001 idle
connections gives a clean linear fit:

```
RSS_MB ≈ 48 + 0.042 × N_connections
```

Validated within ±5% of the regression at every measured point. Two
goroutines per connection (one read, one drain). At maxConnections=
5,000 the idle working set is ~250 MiB.

## PG16 calibration (historical)

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

### Shape B, again, after the FK drop + partial index

Same 5-publisher × `inflight=50` × 3-sub × 5000 msg/s shape on a
fresh kind cluster, with migrations 0006 (drop
`deliveries.client_id` FK) and 0007 (partial inflight-delivery index)
applied:

| Metric                       | Pre-fix    | Post-fix       | Δ          |
| ---                          | ---        | ---            | ---        |
| Total publishes / 90 s       | 9,242      | **25,820**     | **2.79×**  |
| `mqtt_publish` mean          | 39.9 ms    | 16.1 ms        | 2.5×       |
| `mqtt_next_packet_id` mean   | 6.1 ms     | 0.65 ms        | **9.4×**   |
| `MultiXactMember` blks_read  | 2.4 M      | 0              | gone       |
| `MultiXactOffset` blks_read  | 2.8 M      | 3              | gone       |
| FK-trigger time (% of total) | 10.6%      | 0%             | gone       |

The MultiXact SLRU thrash is **completely eliminated** as the
deep-dive predicted. `mqtt_next_packet_id` got the full ~10× because
its cost was wholly contention-bound. `mqtt_publish` only got 2.5×
because the multixact path was one component — the
`WITH matches AS (...) INSERT INTO deliveries` body itself takes
~14.9 ms (now 19.8% of total PG time).

After the fix, the new dominant hot path was `drainSessionQueue`'s
resume scan in `internal/engine/deliver.go` (~36% of PG time at 501 ms
mean). The query filters on `state IN (0,1,2)` which doesn't match
the `state=0 AND qos>0` partial index, so the planner fell back to
the broader `(client_id, state, id)` index and walked dead-tuple
chains. **Migration 0008** added
`deliveries(client_id, id) WHERE state IN (0, 1, 2)` — a partial
index whose predicate exactly matches the resume WHERE clause — so
the planner now picks it deterministically and the resume scan no
longer walks the broader index. Remaining follow-up for that path:

1. Investigate why subscribers are reconnecting ~5×/sec/sub under
   load — likely chaos from rig DISCONNECTs, but if the production
   shape sees the same churn, the resume query frequency itself
   becomes worth tracking even with the index in place.

## Common patterns

**`tx_commit` dominates.** This is fsync. Either your disk is slow,
or Postgres' WAL volume is contended. Check `pg_stat_io` (PG18+):

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

Not enabled by default in `postgres:18-alpine`. To turn on:

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

## How does this compare to other brokers?

Most published broker numbers measure **in-memory** throughput; pgmqtt's
durability model (every PUBLISH committed to Postgres before PUBACK) is
fundamentally different and the comparison is only meaningful against
brokers in a comparable durable mode. From a 2026 survey of public
benchmarks (see commit history for sources):

| Broker | Mode | Hardware | Throughput |
| --- | --- | --- | --- |
| EMQX | QoS 1, in-memory | 1× c5.4xlarge (16 vCPU) | 50k msg/s p2p, 250k fan-out |
| Mosquitto | QoS 1, persistence off | 1× c5.4xlarge | 37k p2p, 81k fan-out |
| HiveMQ 4.18 | QoS 1, **persisted** | 3× m6a.8xlarge cluster | 270k msg/s |
| RabbitMQ + MQTT plugin | QoS 1, **durable classic queues** | 1× 8 vCPU / 32 GB | ~18k msg/s |
| **TBMQ on PostgreSQL** | QoS 1 persistent, INSERT-per-msg | 1 PG node, 12 cores / 64 GB | **30k msg/s** ceiling |
| Kafka | acks=all, default fsync (pagecache) | OpenMessaging cluster | ~600k msg/s |
| Kafka | acks=all + `flush.messages=1` (per-msg fsync) | — | "two to three orders of magnitude lower"; rarely used |
| Raw Postgres single-row INSERTs | `synchronous_commit=on`, full ACID | Ryzen 9 7950X / NVMe | ~1.9k writes/s real workload |

The closest direct analogue is **TBMQ's PostgreSQL prototype** — same
"every PUBLISH is INSERTed and COMMITed in PG before PUBACK" model.
ThingsBoard hit ~30k msg/s on dedicated 12-core / 64 GB and subsequently
migrated TBMQ off PG to Redis to scale further. That's the realistic
ceiling for this architecture on well-provisioned dedicated hardware.

Where pgmqtt should plausibly land:

- **10k–30k msg/s on dedicated hardware** at QoS-1 sync commit. One to
  two orders of magnitude below in-memory brokers (EMQX/NanoMQ at 50k–
  250k); within an order of magnitude of RabbitMQ-durable (~18k) and
  TBMQ-on-PG (~30k).
- The gap to in-memory is the price of "every PUBLISH is in WAL before
  PUBACK." It is not an implementation defect — TBMQ migrating off PG
  and Kafka explicitly avoiding per-message fsync are evidence this is
  a structural cost.
- The 125 msg/s we measured on a contended kind cluster is **not**
  representative. Kind on shared host with concurrent workloads,
  through Docker overlay, with PG and broker on the same node, is
  about 2.5 orders of magnitude below the architectural ceiling — that
  matches what the Postgres-write-performance literature predicts when
  WAL fsync, pgxpool contention, and concurrent small transactions
  stack up.

If you're picking pgmqtt over an in-memory broker, you're paying that
1–2 OOM throughput cost in exchange for "messages are in your existing
Postgres cluster's backup story", "no separate broker state to back
up", and "operator-managed users via the existing Kubernetes auth
boundary." Those trade-offs are the value proposition; the throughput
table is the cost.
