# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
