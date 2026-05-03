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
- `scripts/ha-z2m-soak.sh` — Home Assistant + synthetic Z2M heartbeats
  with `kubectl delete pod` chaos.
- Engine tests for `handleUnsubscribe`, broker-side outbound QoS-2
  receiver state (`handlePubrec` + `handlePubcomp`).
- Metrics: `TestServeBindsAndShutsDown` exercises the http.Server
  lifecycle.

### Conformance

- v3.1.1 Paho: 9/10 (only `test_subscribe_failure`, which needs ACLs —
  out of v1 scope).
- v5 Paho: 23/27 deterministic; 2 flakes are upstream `waitfor` typos
  (callback vs callback2) documented in `docs/CONFORMANCE.md`.
