# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed — modernization

- **Helm chart distribution**: the chart is now published to
  `oci://ghcr.io/bo0tzz/charts/pgmqtt` by `.github/workflows/chart-release.yml`
  on every `vX.Y.Z` tag push. There is no HTTP-hosted chart repo;
  `helm install pgmqtt oci://ghcr.io/bo0tzz/charts/pgmqtt --version X.Y.Z`
  is the supported install path. README + docs/TODO.md updated.
- **CI helm pin**: `azure/setup-helm@v4` was pinned to v3.16.1 (Sep 2024)
  in the `helm-lint` and `smoke` jobs; both now pin `version: latest`,
  matching the new `chart-release.yml` workflow.
- **Container base**: `Dockerfile` now uses
  `gcr.io/distroless/static-debian13:nonroot` (was `debian12:nonroot`).
  Pure rebase to the current distroless major; no library surface change.
- **go.mod hygiene**: `prometheus/client_model` was listed as
  `// indirect` even though `internal/metrics` imports its `dto`
  subpackage directly; promoted to a direct require. Added a comment
  next to the `gorilla/websocket` pseudo-version explaining why we
  keep it (k8s.io/* v0.36 pulls a post-v1.5.3 commit transitively;
  downgrading to v1.5.3 would rewind shared resolution).

No production code paths changed in this round; `go test ./...` and
`go test ./... -race` are green, and `govulncheck ./...` reports no
known vulnerabilities at the current resolution.

### Fixed — broker correctness

- Migration **0011** fixes an off-by-one introduced by migration 0010's
  publish-cap short-circuit. The `EXISTS (... OFFSET p_max_queued LIMIT 1)`
  formulation evaluates `over_cap` as `depth >= p_max_queued + 1`, one row
  too lenient relative to the original migration-0005 semantics
  (`depth >= p_max_queued`). Concretely: with cap=N and N rows already
  queued, the broker should DISCONNECT 0x97 the slow subscriber on the
  next overflowing publish — instead it silently accepted one more row
  before tripping. `OFFSET (p_max_queued - 1) LIMIT 1` restores the
  intended `>= cap` boundary. The existing
  `engine_test.go::TestSlowSubscriberQuotaExceeded` regression test
  catches this exactly.

## [0.1.1] - 2026-05-04

### Changed — broker correctness

- `publishCore` is now a single transaction covering retain mutation,
  optional QoS-2 inbound dedup (was a separate `pool.Exec`), the message
  INSERT + fanout, the cross-Pod `pg_notify`, and the COMMIT. Previously
  retain and dedup ran on a separate connection before the publish tx
  began, and `pg_notify` ran AFTER commit — both windows admitted silent
  failure modes (orphan retained row + missing live publish; QoS-2
  message swallowed by ON CONFLICT after a crashed publishCore;
  successful PUBACK with no peer notified).
- `handleDisconnect` now NULLs `will_topic`/`will_payload`/`will_qos`/
  `will_retain`/`will_delay`/`will_properties` when it just fired the
  immediate-will, closing the window where a later pod death would have
  fired the same will a second time via the dead-broker scan.
- `handleDisconnect`'s clean-session path and `janitor.expireSessions`
  now wrap their cleanup in one tx (DELETE deliveries, DELETE session,
  COMMIT). Same for the CONNECT clean-start path.
- `janitor.fireDueWills` publishes the will *before* clearing
  `will_fire_at` / will_*. A crash between publish-commit and clear
  surfaces as a duplicate-will on the next janitor tick rather than
  silent loss.
- Migration **0006** drops the `deliveries.client_id → sessions` FK.
  The implicit `FOR KEY SHARE` lock on every delivery insert was the
  dominant Postgres bottleneck under fan-out load (MultiXact SLRU
  thrash, ~10× slowdown on every `sessions` lookup). Cleanup now happens
  explicitly in the broker's session-delete paths plus a new
  `janitor.sweepOrphanDeliveries`.
- Migration **0007** adds a partial index
  `deliveries(client_id, id) WHERE state=0 AND qos>0` so the broker's
  inflight-delivery scan stops falling back to a full pkey walk.
- Migration **0008** adds a partial index
  `deliveries(client_id, id) WHERE state IN (0, 1, 2)` covering
  `drainSessionQueue`'s resume scan. After 0006/0007 landed, the
  resume scan became the new dominant PG hot path (~36% of total PG
  time, 501 ms mean) because its `state IN (0,1,2)` predicate didn't
  match 0007's narrower `state=0 AND qos>0` index. The new index's
  predicate matches the WHERE clause exactly, so the planner picks
  it deterministically.
- Migration **0009** drops `sessions.next_packet_id` and the
  `mqtt_next_packet_id()` SQL function. Outbound packet ID
  allocation moved into a per-`*Conn` atomic counter seeded from
  `MAX(packet_id)` on session takeover, eliminating the per-delivery
  HOT-update churn that bloated the `sessions` row over hours of
  operation. Spec only requires uniqueness per inflight; the seed
  provides crash-recovery without persisted state.
- `cleanStart=true` reconnect now also DELETEs `inbound_qos2`
  rows for the client. Without this, stale QoS-2 dedup tombstones
  from the prior incarnation persisted (the FK CASCADE didn't
  trigger because takeOwnership reuses the existing sessions row).
  A fresh QoS-2 PUBLISH that reused a packet_id from the prior
  session would otherwise hit ON CONFLICT and be silently swallowed.
- AUTH packet handling: CONNECT with `AuthenticationMethod` set is
  now rejected with CONNACK 0x8C (Bad authentication method), and a
  stray AUTH packet mid-connection draws DISCONNECT 0x82 (Protocol
  error). Previously fell through to a generic "unsupported packet
  type" socket close — non-conformant per MQTT-4.12.0-2.
- `c.sessionExpiry` and `c.willDelay` switched from `*int32` to
  `*uint32`. Spec values in the `[0x80000000, 0xFFFFFFFE]` range
  no longer wrap to negative; the "never expire" sentinel is now
  `MaxUint32` (matching `0xFFFFFFFF` from the spec) instead of the
  ambiguous `MaxInt32`/`-1` pair. Persisted DB column stays INT
  (int32) and clamps for record-keeping; in-memory authoritative
  value preserves the full spec range.
- `c.sessionExpiry` is now only assigned when the CONNECT actually
  carried a `SessionExpiryInterval` property
  (`SessionExpiryIntervalFlag == true`). The struct comment claimed
  "nil = no value sent" but the assignment ignored that — the
  graceful-DISCONNECT increase-from-0 invalidIncrease check keys
  off this sentinel.

### Added — observability

- `pgmqtt_publish_seconds` Prometheus histogram with stages `total`,
  `qos2_dedup`, `retain`, `tx_begin`, `mqtt_publish_query`, `tx_commit`,
  `notify`, `response_write`. Per-stage attribution of inbound PUBLISH
  latency without correlating against `pg_stat_statements`.
- `pgmqtt_delivery_seconds{stage}` histogram — outbound counterpart
  with stages `total`, `scan`, `alloc`, `write`. Bounds the whole
  publish→subscriber latency story together with `publish_seconds`.
- `pgmqtt_janitor_tick_seconds{job}` histogram +
  `pgmqtt_janitor_errors_total{job}` counter — per-sub-job timing
  and error attribution for janitor.Tick. A single sub-job blowing
  past the 1 s tick interval (or failing repeatedly) was previously
  invisible in metrics.
- `pgmqtt_auth_failures_total{reason}` counter, labels
  `bad_credentials`, `not_authorized`, `bad_auth_method`,
  `client_id_invalid`, `unsupported_protocol`, `other`. Brute-force
  / misconfigured-client detection.
- `pgmqtt_subscribes_total` / `pgmqtt_unsubscribes_total` counters,
  symmetric with `publishes_total`. Bound topic-churn driven load.
- `pgmqtt_subscriptions` / `pgmqtt_sessions` / `pgmqtt_retained_count`
  / `pgmqtt_inbound_qos2_pending` gauges — table cardinalities,
  refreshed each janitor tick.
- `pgmqtt_will_fire_lateness_seconds` histogram — `(now - will_fire_at)`
  at janitor fire time. SLO: delayed wills fire within ~1 s of
  scheduled at default tick interval.
- `pgmqtt_outbound_inflight_saturation` histogram —
  `len(inflight)/cap(inflight)` sampled per delivery. Slow-consumer
  shape detection.
- `pgmqtt_connections_capacity_ratio` gauge — current accepted
  connections / `maxConnections`. HPA scale-out signal.
- `pgmqtt_wills_notify_failed_total` and
  `pgmqtt_retained_dispatch_failed_total` counters — production-no-op
  counters that surface InProcessNotifier failures (test) and
  silent retained-dispatch failures (post-SUBACK).
- Controller-runtime metrics (`controller_runtime_reconcile_*`,
  `workqueue_*`) are now surfaced on our existing `/metrics`
  endpoint via a dedupe-aware merge gatherer. Operator reconcile
  latency / error rate / queue depth are observable without
  scraping a second port.
- `PGMQTT_PG_STATEMENT_TIMEOUT_MS` (default `30000`) plumbed into the
  pgxpool ConnConfig.RuntimeParams. Bounds wedged Postgres so publisher
  dispatch can't hang past keepalive.
- `PGMQTT_LOG_FORMAT` (default `text`, accepts `json`) switches the
  slog handler at startup. Production deployments behind log
  aggregation (Loki, Datadog, Cloud Logging) get structured JSON
  without a sidecar / regex extractor.
- Helm `auth.allowAnonymous` and `extraEnv` values — the chart
  previously documented `--set auth.allowAnonymous=true` but no
  template rendered it; setting it was a silent no-op.
- Helm `crds.install` is now actually wired. The User CRD moved from
  `crds/users.yaml` (Helm v3 install-only) into a templated CRD in
  `templates/crd-users.yaml` gated on `.Values.crds.install`.
- Helm production knobs: `podAntiAffinity` (soft preset, off by
  default), `imagePullSecrets`, `podLabels`, `extraVolumes` /
  `extraVolumeMounts`, `priorityClassName`,
  `topologySpreadConstraints`, `terminationGracePeriodSeconds`,
  full probe tunability (initialDelay/period/timeout/failure/success
  thresholds for both liveness and readiness), `extraEnvFrom`,
  `service.externalTrafficPolicy` / `loadBalancerIP` /
  `loadBalancerSourceRanges`, `hostNetwork`, `dnsPolicy`,
  `dnsConfig`, `runtimeClassName`,
  `automountServiceAccountToken: false` by default (broker doesn't
  call the K8s API).
- `cfg.PodName` (was read from POD_NAME env via Downward API but never
  consumed) is now pinned onto every log line via `slog.With`, so
  aggregated-log operators can correlate pod ↔ broker UUID.

### Added — broker resilience

- Goroutine panic recovery at every long-lived background boundary:
  janitor.Run + per-tick (one panic in any sub-job no longer takes
  the broker down), listener.run + per-NOTIFY dispatch, per-Conn
  `run()`, `runDrainLoop`, the metrics serve goroutine, and the
  operator.Run goroutine. All log a stack at ERROR before returning.
  Per-iteration recovery means a malformed payload or panic on one
  event doesn't kill the loop for subsequent ones.
- New per-engine ownership-sweep goroutine reconciles the local
  conns map against `sessions.broker_id` every 5 s. Sockets we
  still hold for client_ids the DB now attributes to a different
  broker get `Shutdown()`ed. Closes the takeover-NOTIFY-fire-and-
  forget gap where an orphaned socket could keep PUBLISHing
  duplicates after a silent ownership transfer.

### Changed — leaderless coordination

The post-tag rounds explored several flavours of singleton-leader
fencing and crash-loop policy. The architecturally cleaner answer
turned out to be: **don't have a singleton.** Every leader-gated
janitor operation is already concurrency-safe by construction, and
the operator can use controller-runtime's K8s Lease leader election.
This release ships that refactor:

- **Janitor**: every Pod runs an independent Tick loop. Sweep
  operations are concurrency-safe by construction —
  `pg_try_advisory_lock` per dead broker, `SELECT … FOR UPDATE
  [SKIP LOCKED]` for wills/expiries, idempotent DELETEs for retained
  / inbound_qos2 / orphan rows. See `internal/janitor/janitor.go`
  package doc for the per-job analysis.
- **Operator**: `manager.Options.LeaderElection: true` with a
  namespaced `pgmqtt-operator` Lease. controller-runtime handles
  acquisition + reconciliation gating + handoff internally. On loss
  the manager exits and a peer Pod takes over — no Pod restart, no
  re-arm code in our tree.
- **`internal/leader/` package deleted**, along with the
  PG-advisory-lock based `leader.Start`, the watchdog goroutine in
  cmd/pgmqttd that crashed the pod on `Lost()`, and the
  Lost-cancellable-tick wiring in janitor.RunWith. None of those
  failure modes can happen anymore.
- New Helm RBAC: namespaced Role + RoleBinding granting
  `coordination.k8s.io/leases` and `events` on the broker SA in the
  release namespace. `automountServiceAccountToken` defaults flipped
  back to `true` (operator now genuinely needs the SA token).
- New `POD_NAMESPACE` env (Downward API) used as the Lease
  namespace. Empty falls back to controller-runtime's in-cluster
  auto-detect.

This closes audit findings L1 (CRITICAL leader-write fence), L2
(HIGH re-arm), and L4 (LOW lost-cancellable tick) at the source
rather than retrofitting fences onto the singleton model.

### Fixed — janitor

- `handleDeadBroker` advisory-lock leak across pgxpool conns: the
  `pg_try_advisory_lock` and the deferred `pg_advisory_unlock`
  could land on different conns (pgxpool auto-acquires per call),
  and `pg_advisory_unlock` from a different session is a no-op.
  Fix: `pool.Acquire` one conn for the entire lock + work + unlock
  sequence, defer `Release`. The metric increment also moved into
  the claimed-true branch (previously over-counted on transient
  errors).

### Added — docs

- `docs/PERF.md` runbook: stage-by-stage attribution, calibrated kind
  baseline (3.49 ms total / 0.1 ms fsync — fsync is *not* the
  bottleneck), under-load measurement showing MultiXact SLRU thrash,
  and a "how does this compare to other brokers?" table grounded in
  published benchmarks.
- `docs/VERSIONING.md` defines the SemVer policy across broker /
  operator API / PG schema and the CHANGELOG flow. New
  "Migration policy: rolling-deploy safety" section codifies the
  2-phase rule (release N stops the code from depending on a schema
  item; release N+1 removes it) after migration 0009 demonstrated
  the rolling-deploy error window.
- `docs/CONFORMANCE.md` documents the v5 spec areas not exercised by
  the Paho suite where pgmqtt is conformant by *omission* —
  enhanced auth, ResponseInformation, large-uint32 SessionExpiry,
  will-publish MessageExpiryInterval-after-delay choice.
- `docs/SIZING.md` cross-references the PERF.md histograms and
  flags the existing rules-of-thumb numbers as preliminary pending
  a clean dedicated-host re-measurement.
- `docs/BACKUP.md` schema audit: clarified that pg_dump captures the
  full schema (functions, partial indexes, sequences) by default, not
  just the operator-facing survival set listed; added migration 0006
  rollforward guidance and a `schema_migrations` cross-check to the
  recovery-drill validation step.
- `docs/OPS.md` "Crash-loop on unexpected leader-loss" subsection
  documents the operator-visible signal (Restarts > 0 with
  "leader lost outside of shutdown" log line) and what to investigate
  when a single pod restart-loops continuously.

### Added — tests

- 19 new `internal/operator/user_controller_test.go` cases covering
  BYO Secret error paths, finalizer-add/Secret-create transient
  failures, DB error paths, deletion paths, multi-User isolation,
  OwnerReference encoding, and bcrypt-cost edges. Coverage on the
  controller's per-function lines is now 96–100% (Reconcile 97.6%,
  reconcileDelete 100%, resolveCredentialSecret 96%).
- `internal/db/migrate_test.go` exercises the new `statement_timeout`
  knob via testcontainers Postgres.

### Verified

- Network-partition chaos via Chaos Mesh: 5 partition cycles × 30 s
  on/off during 300 s soak with `-inflight 50`, 0 lost / 0 dups across
  all subscribers. Recorded in `docs/VERIFY.md`.
- Soak rig cross-validation against Mosquitto 2.x: with
  `persistence true`, identical clean metrics shape — confirms the
  rig's reports are broker-attributable, not rig-attributable.

## [0.1.0] - 2026-05-03

First tagged release. Production-ready scope per `docs/TODO.md`.

### Added — broker quality

- v1 implementation: Postgres-coordinated MQTT 3.1.1 / 5 broker.
- Stateless `pgmqttd` Pods; Postgres advisory lock = liveness; `pg_notify` for
  cross-Pod fanout and takeover.
- Leader-elected janitor: dead-broker detection, will fan-out, orphan sweep,
  v5 session-expiry, retained-message-expiry, `inbound_qos2` tombstone GC.
- `pgmqtt.io/v1alpha1.User` CRD with in-broker reconciler — auto-generates
  credentials Secrets in cnpg style or accepts `passwordSecretRef`.
- v5: ReceiveMaximum, TopicAliasMaximum (outbound), MaxPacketSize,
  ServerKeepAlive cap, MessageExpiryInterval, SessionExpiryInterval,
  WillDelayInterval, SubscriptionIdentifier aggregation, retain-as-published.
- Slow-subscriber backpressure: per-client deliveries cap with DISCONNECT
  0x97 and cross-Pod NOTIFY (`PgQuotaNotifier`).
- Per-Pod max-connections cap with CONNACK 0x9F.
- Per-conn inbound rate limit on PUBLISH/SUBSCRIBE with DISCONNECT 0x96.
- Configurable bcrypt cost.

### Added — observability

- Prometheus `/metrics` (off-broker, listens on `:9090` by default) with
  pgmqtt_connections / publishes_total{qos} / deliveries_inflight{state} /
  takeovers_total / dead_brokers_handled_total / sessions_expired_total /
  wills_fired_total / quota_exceeded_total / rate_limited_total plus
  `pgmqtt_pgx_*` from a pgxpool collector.

### Added — Helm

- PodDisruptionBudget (default `minAvailable: 1`).
- HorizontalPodAutoscaler template (off by default; CPU + customMetrics).
- NetworkPolicy template (off by default; DNS / postgres / kube-API egress).
- ServiceMonitor template gated on the existing `serviceMonitor.enabled` flag.
- Helm `chart-tests` Pod doing a mosquitto round-trip.
- Optional MQTTX Web companion (`ui.enabled=true`, pinned to v1.13.0).

### Added — docs

- `docs/TODO.md` — production-readiness checklist (this release closes it).
- `docs/OPS.md` — day-2 runbook.
- `docs/SIZING.md` — Pod resources / postgres connection count guidance.
- `docs/SECURITY.md` — trust boundaries + threat model.
- `docs/BACKUP.md` — pg_dump / cnpg flows + recovery drill.
- `docs/TLS.md` — four working TLS termination patterns.
- `docs/UI.md` — MQTTX Web companion install + workflow.

### Added — release engineering

- `.goreleaser.yaml` with linux/{amd64,arm64} + darwin/{amd64,arm64}
  binaries plus checksums; matching `release` workflow with auto-publish
  disabled.
- `make test-race` and `make coverage`; CI runs both.

### Added — tests

- `cmd/soak` traffic-generator + `scripts/soak.sh` chaos wrapper.
  Reconnects on broker death, uses persistent v5 sessions on
  subscribers (clean_start=false + SessionExpiryInterval=3600) so
  messages published during the disconnect window queue server-side
  and drain on reconnect — verified zero-loss QoS-1 and zero-loss
  QoS-2 under broker-kill-every-6s chaos at 200 msg/s. Skips
  re-SUBSCRIBE when CONNACK reports `session_present=true` to avoid
  drain/SUBACK interleave.
- `scripts/paho-multi-broker.sh` runs Paho conformance against a 3-Pod
  kind broker via the Service VIP.
- Engine tests for `handleUnsubscribe`, broker-side outbound QoS-2
  receiver state (`handlePubrec` + `handlePubcomp`).
- Metrics: `TestServeBindsAndShutsDown` exercises the http.Server
  lifecycle.

### Verification & test rig

- `cmd/soak` publisher pipelining: new `-inflight N` flag enables
  QoS-1 PUBLISH→PUBACK pipelining via a dedicated reader goroutine
  that demuxes PUBACKs by packet ID. Pushes per-publisher throughput
  past the strict-serial RTT ceiling. Strict-serial path
  (`inflight=1`) preserved as the default; QoS-0 / QoS-2 still take
  the strict-serial path. Pipelined publisher records every
  outstanding `(pid → seq)` and folds in-flight + un-replayed entries
  into a replay queue on disconnect, so seqs that were on the wire
  when the broker died are resent on the new conn. End-of-run drain
  waits up to 2s for `outstanding` to empty so `published` doesn't
  under-report by the in-flight window.
- `cmd/soak` parallel publishers: `-pubs N` runs N concurrent
  publishers each on its own connection, client_id, and topic
  (`<topic>/pub-<idx>`); subscribers wildcard-subscribe `<topic>/+`.
  Payloads now encode `(pub_id, seq)` so loss / dup analysis is
  per-publisher × per-subscriber. Total `-rate` is split across
  publishers (remainder spilled onto the first few).
- `cmd/soak` subscriber single-read-loop: one goroutine handles all
  reads (PUBLISH delivery + PUBREC ack interleave) so QoS-2 doesn't
  deadlock on shared-conn ordering with multiple inbound publishers.
- `cmd/soak` PUBACK validation: publisher now asserts the ack frame
  is actually `PUBACK` with the matching packet ID before counting
  the seq as published. Stops e.g. a graceful-shutdown DISCONNECT
  0x8B from being mis-counted as a successful publish, which would
  hide loss in the report.
- Broker-side soak diagnostics: extra log breadcrumbs around
  PUBLISH-arrived / PUBACK-emitted / cross-Pod fan-out timing for
  reproducing soak failures locally without re-running the full
  10-minute kind chaos loop.
- `docs/CONFORMANCE.md` adds a "Multi-broker via Service VIP" section
  recording the 3-replica kind run with subscribers and publishers
  routinely landing on different Pods (kube-proxy per-conn
  round-robin), exercising the Postgres-coordinated handoff. Same
  9/10 v3.1.1 and 23/27 deterministic v5 result as single-broker.
- Soak rig scope trimmed to broker-only chaos: dropped the bundled
  Home Assistant + Zigbee2MQTT integration probe; the in-tree rig
  is now `Postgres + pgmqttd + cmd/soak` only. Operators who want
  HA/Z2M coverage can layer it on top of the broker chaos loop
  themselves.

### Conformance

- v3.1.1 Paho: 9/10 (only `test_subscribe_failure`, which needs ACLs —
  out of v1 scope).
- v5 Paho: 23/27 deterministic; 2 flakes are upstream `waitfor` typos
  (callback vs callback2) documented in `docs/CONFORMANCE.md`.
