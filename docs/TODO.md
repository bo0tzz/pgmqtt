# Production-readiness TODO

Authoritative checklist for the post-v1 production-readiness pass. Each item
should land on `main` with a test where applicable, in the order roughly
suggested below. Cross items off in this file as they ship.

## 1. Broker quality

- [x] **`inbound_qos2` GC.** Janitor sweeps rows whose `received_at` is past
      the grace period and whose session is currently disconnected; default
      grace 1h, tunable via `SetInboundQoS2Grace`. Connected sessions are
      left alone — those rows are still in-flight. Test in
      `janitor_test.go::TestJanitorInboundQoS2Sweep`.

- [x] **Slow-subscriber backpressure.** `mqtt_publish` SQL takes
      `p_max_queued`; matching subscribers at/over cap have their delivery
      insert skipped. QoS-0 drops are silent (spec-OK); QoS>0 drops surface
      in `overflow_clients` so the engine can DISCONNECT 0x97 the conn
      (`Engine.dispatchQuotaExceeded`). Cross-Pod path uses a new
      `pgmqtt_quota_<broker_id>` LISTEN channel + `PgQuotaNotifier`. Cap is
      `PGMQTT_MAX_QUEUED_DELIVERIES_PER_CLIENT` (default 10000) and
      `Values.limits.maxQueuedDeliveriesPerClient`. Test:
      `TestSlowSubscriberQuotaExceeded` seeds the deliveries table at the
      cap and asserts DISCONNECT 0x97 lands.

- [x] **Connection limits + rate limiting.**
        * `PGMQTT_MAX_CONNECTIONS` (default 5000) — atomic reservation in
          `Engine.tryReserveConn`; over-cap accept emits a v5-shaped
          CONNACK 0x9F and closes the socket before CONNECT processing.
        * `PGMQTT_MAX_INBOUND_MSGS_PER_SEC` (default 1000) — token-bucket
          on PUBLISH/SUBSCRIBE only (acks/PING are protocol-required and
          not metered). Bucket capacity == rate; refill rate-per-second.
          Trip → DISCONNECT 0x96. Helm exposes both as
          `Values.limits.{maxConnections,maxInboundMsgsPerSec}`.
        Tests: `TestMaxConnectionsRejects`, `TestRateLimitDisconnects`.

- [x] **Prometheus metrics.** `internal/metrics` registers
      `pgmqtt_connections`, `pgmqtt_publishes_total{qos}`,
      `pgmqtt_deliveries_inflight{state}` (queued/inflight/awaiting_pubcomp,
      refreshed each janitor tick), `pgmqtt_takeovers_total`,
      `pgmqtt_dead_brokers_handled_total`, `pgmqtt_sessions_expired_total`,
      `pgmqtt_wills_fired_total`, `pgmqtt_quota_exceeded_total`,
      `pgmqtt_rate_limited_total`, plus a pgxpool collector for connection
      pool depth/acquire latency. Served on `:9090/metrics` by default
      (`PGMQTT_METRICS_ADDR`). Helm: `metrics.{enabled,port}`,
      `serviceMonitor.{enabled,interval,path}`, plus the existing
      ServiceMonitor flag is now wired to a real template.

- [x] **Configurable v5 server policy.** `PGMQTT_RECEIVE_MAXIMUM`,
      `PGMQTT_TOPIC_ALIAS_MAXIMUM`, `PGMQTT_KEEPALIVE_MAX_SEC` and Helm
      `v5.{receiveMaximum,topicAliasMaximum,keepaliveMaxSec}` plumb to
      engine/conn/connect/publish via `Engine.serverReceiveMaximum()`
      etc. Defaults preserve historical 100/0/60s.

- [x] **Bcrypt cost configurable.** `PGMQTT_BCRYPT_COST` (default 10),
      validated 4..31 in `config.FromEnv`, plumbed through
      `operator.Options.BcryptCost` to `UserReconciler.BcryptCost`. To
      rehash an existing user at a new cost: bump the CR (e.g.
      `kubectl annotate user/<name> pgmqtt.io/rotated-at=now --overwrite`)
      to force reconcile; bcrypt verifies any cost embedded in the hash,
      so existing rows continue to authenticate during the rollout.

## 2. Helm / k8s

- [x] **PodDisruptionBudget** template — minAvailable=1 default.
- [x] **HorizontalPodAutoscaler** template (off by default;
      `autoscaling.enabled`). Scales on CPU by default; supports
      `customMetrics` for `pgmqtt_connections` once a prometheus-adapter
      is installed.
- [x] **NetworkPolicy** template (off by default). Includes DNS egress,
      configurable postgres egress selector, optional kubernetesAPI
      egress for the leader's reconciler.
- [x] **TLS termination example.** `docs/TLS.md` with the four working
      patterns (HTTPS Ingress for wss, ingress-nginx tcp-services, HAProxy
      frontend tls/backend tcp, cloud LB with ACM/GCLB).
- [ ] **PSP / SCC**: chart already sets `runAsNonRoot`, etc. Verify against
      OpenShift restricted-v2 SCC.
- [x] **Helm chart-tests directory** (`helm test pgmqtt`) with a mosquitto
      round-trip Pod (helm.sh/hook=test).

## 3. Testing actually run

- [ ] **10-minute soak**: 1k msg/s in flight with `kubectl delete pod
      pgmqttd-<random>` every 30s. Assert: no QoS-1 loss; QoS-2 receiver
      sees no duplicates. Output a report at the end (received vs.
      published, duplicates, retries).

- [ ] **Multi-broker Paho conformance.** Currently the Paho suite is run
      against a single-Pod broker. Reproduce against a 3-Pod kind cluster
      via the Service VIP — exercises cross-Pod fanout under conformance
      load. Expect the same pass rate; document any differences.

- [ ] **HA + Z2M sustained kill test.** Spin up Home Assistant +
      Zigbee2MQTT (containers) → connect to broker → kill pod every 30s for
      10 minutes → assert HA entity state never marked stale beyond
      `keepalive + grace`.

- [x] **`go test -cover` summary.** `make coverage` writes
      `coverage.out`/`.txt`. CI runs it and uploads as artifact. Total
      48% — internal/listener (83%) > internal/config (79%) >
      internal/leader (75%) > internal/db (70%) > internal/mqtt (67%) >
      internal/engine (56%) > internal/janitor (56%) > internal/operator
      (52%) > internal/metrics (29%). Lift the laggards in a follow-up.
- [x] **Race-mode CI.** `make test-race` and the equivalent CI step run
      `go test -race` across the whole tree. Caught real races in the
      runtime-tunable knobs (cfg fields read by accept loop while a test
      setter mutated them); fix in the same commit moves the knobs to
      atomic.Int64 fields on Engine.

## 4. Paho upstream

- [~] **Paho `waitfor` typo** — already documented in `docs/CONFORMANCE.md`
      with a description of the race (callback vs callback2). Filing an
      upstream issue is out of scope for the AI assistant per the
      no-external-writes rule; the user will file it manually if/when
      they choose. Local evidence (broker logs showing subscribe/publish
      ordering) is in the conformance run.

## 5. Off-the-shelf web UI

- [x] **Optional UI = MQTTX Web** as a sibling Deployment + Service when
      `ui.enabled=true`. Shipped as a templated extra (not a subchart),
      using `emqx/mqttx-web:latest` by default. The "no copy-paste of
      credentials" goal in the original TODO turned out to require a
      server-side auth-proxy (browsers can't read kube Secrets); the
      pragmatic flow is documented in `docs/UI.md` — `kubectl
      port-forward` then a single `kubectl get secret` go-template that
      prints all five connection fields ready to paste. Verdict on the
      other candidates (mqtt-explorer / HiveMQ / various forks) is in
      the same doc.

## 6. Docs

- [x] **Operational runbook** (`docs/OPS.md`) — leader-stuck flow,
      zombie ownership, stuck delivery row, postgres pool sizing,
      schema-migration safety, plus a forced-restart and DB-failover
      walk-through.
- [x] **Backup/restore guide** (`docs/BACKUP.md`) — survival-set vs
      ephemeral table inventory, pg_dump flow, cnpg flow, recovery-drill
      checklist.
- [x] **Sizing guide** (`docs/SIZING.md`) — Pod resources per N conns,
      postgres `max_connections` table by traffic, memory ballpark, when
      to consider ltree, smoke targets.
- [x] **Security threat model** (`docs/SECURITY.md`) — trust boundaries
      diagram, what the broker enforces (auth, per-conn limits, sweeps),
      what it deliberately doesn't (TLS, ACLs, mTLS), production
      recommendations, secrets handling.

## 7. Release engineering

- [ ] **Tag v0.1.0 locally** once 1–4 are done. Helm chart publishing
      (gh-pages branch, helm-charts repo) is for the human — the AI
      assistant doesn't perform external writes (push, gh-pages publish).

- [x] **Goreleaser** + matching workflow. `.goreleaser.yaml` produces
      `pgmqttd_<version>_<os>_<arch>.tar.gz` for `linux/{amd64,arm64}`
      and `darwin/{amd64,arm64}` plus a sha256 checksum file. The
      `release` workflow runs on tag push with auto-publish disabled
      so the maintainer reviews `dist/` before flipping `release.disable`
      to false. Manual `workflow_dispatch` runs `--snapshot` for
      dry-runs.

- [ ] **Semver policy** + CHANGELOG discipline: docs/CHANGELOG.md exists,
      keep it up to date.

- [ ] **Deprecation policy** for the `User` CRD (`v1alpha1` → `v1beta1` →
      `v1`). For now `v1alpha1` is fine.

## Out of scope (per original v1 plan)

These were explicitly listed in the plan as out-of-scope and stay that way:
- TLS termination inside `pgmqttd` (handled by L4/L7 terminator).
- ACLs / topic-level authorization.
- Bundled web dashboard UI we wrote ourselves (an off-the-shelf one is fine
  per task in section 5).
- `ltree`-backed retained/subscription indexes.
- Shared subscriptions.

---

When in doubt: add it as a checkbox here, file a separate test, and leave
the production-readiness label on the chart until the matrix is green.

**Note on completion.** This list is a planning aid, not the definition of
done. When every box above is ticked, do *not* declare the project shipped
— re-audit the repo (drift between docs and code, weak failure modes,
freshly-introduced dead code, missing tests, conformance/CI status) and
add fresh items to this file. Production-ready is a property of the
software, not of the checklist.
