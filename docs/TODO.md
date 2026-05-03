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

- [ ] **Evaluate dropping a UI into the Helm chart.** We don't write our
      own. Candidates (each needs a proper sniff for "self-hostable +
      static-ish + auth-aware"):
        * **MQTTX Web** — EMQX's. Has a `docker-compose` for `mqttx/mqttx-web`.
          ([github.com/emqx/MQTTX](https://github.com/emqx/MQTTX)) — likely
          the strongest candidate; commercial-quality UI, supports v5, runs
          stateless.
        * **mqtt-explorer** — desktop only; skip.
        * **HiveMQ Web Client** — single-page, hosted only, no self-host
          docker image last I checked.
        * **mqtt-web-client** (various forks) — too unmaintained.
      Once a candidate is chosen, ship as an `optional` Helm subchart or
      a sibling chart that mounts the credential Secret automatically.
      Acceptance: a user can `helm install pgmqtt --set ui.enabled=true`,
      then `kubectl port-forward` to the UI Service and connect to the
      broker without copy-pasting credentials.

## 6. Docs

- [ ] **Operational runbook** (`docs/OPS.md`):
        * "Leader stuck": how to find which Pod holds `pg_advisory_lock(42)`,
          how to force-release.
        * "Zombie session ownership": if a Pod hard-died and the janitor
          isn't running, how to manually clear `sessions.broker_id`.
        * "Stuck delivery row": how to inspect, when to delete.
        * "Postgres connection limits": pool sizing guidance, what happens
          when exhausted.
        * "Schema migrations during rolling restart": what's safe.

- [ ] **Backup/restore guide** (`docs/BACKUP.md`): pg_dump usually fine; if
      using cnpg, refer to its backup story. Document which tables are
      essential (sessions + subscriptions + retained) vs. ephemeral
      (deliveries + messages + inbound_qos2).

- [ ] **Sizing guide** (`docs/SIZING.md`): rough CPU/memory per N
      connections, postgres connection count needed, when to consider
      ltree indexes.

- [ ] **Security threat model** (`docs/SECURITY.md`): trust boundaries,
      what bcrypt cost achieves, recommendation to put TLS terminator in
      front, secrets handling.

## 7. Release engineering

- [ ] **Tag v0.1.0 locally** once 1–4 are done. Helm chart publishing
      (gh-pages branch, helm-charts repo) is for the human — the AI
      assistant doesn't perform external writes (push, gh-pages publish).

- [ ] **Goreleaser** (or a hand-rolled `release.yml`) to push static
      binaries on tag push.

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
