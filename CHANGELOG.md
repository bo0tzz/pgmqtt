# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- v1 implementation: Postgres-coordinated MQTT 3.1.1 / 5 broker.
- Stateless `pgmqttd` Pods; Postgres advisory lock = liveness; `pg_notify` for
  cross-Pod fanout and takeover.
- Leader-elected janitor (dead-broker detection, will fan-out, orphan sweep).
- `pgmqtt.io/v1alpha1.User` CRD with in-broker reconciler — auto-generates
  credentials Secrets in cnpg style or accepts `passwordSecretRef`.
- Helm chart, multi-arch Dockerfile, GitHub Actions CI with kind smoke.
