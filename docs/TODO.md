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

- [ ] **Slow-subscriber backpressure.** Today the drain loop will keep
      writing to a slow client without bound; deliveries pile up in Postgres.
      Cap the per-conn outbound queue depth (in the deliveries table) and
      either (a) drop QoS 0 messages past the cap, (b) drop the conn with
      DISCONNECT 0x97 (Quota Exceeded). Wire a configurable
      `PGMQTT_MAX_QUEUED_DELIVERIES_PER_CLIENT` (default ~10000).
      Tests: feed a TCP-paused subscriber until cap hits, assert behaviour.

- [ ] **Connection limits + rate limiting.** No max concurrent conns per
      Pod, no per-conn inbound publish rate limit. DoS surface today. Add:
      - `PGMQTT_MAX_CONNECTIONS` (per Pod) — reject CONNACK with 0x9F
        (Connection Rate Exceeded) when at limit.
      - Token-bucket per conn for inbound PUBLISH/SUBSCRIBE — drop with
        DISCONNECT 0x96 (Message Rate Too High) when sustained over.
      Defaults reasonable for a homelab: 5000 conns, 1000 pps per conn.

- [ ] **Prometheus metrics.** Helm `serviceMonitor.enabled` exists but the
      broker doesn't expose `/metrics`. Implement (`prometheus/client_golang`
      is conventional for Go; `slog`-driven counters are an alternative if
      we want zero deps). Minimum gauges/counters to expose:
        * `pgmqtt_connections{state="connected|disconnected"}`
        * `pgmqtt_publishes_total{qos}` (counter)
        * `pgmqtt_deliveries_inflight{qos}` (gauge from deliveries table)
        * `pgmqtt_takeovers_total` (counter)
        * `pgmqtt_dead_brokers_handled_total`
        * `pgmqtt_session_expired_total`
        * `pgmqtt_will_fired_total`
        * Postgres pool stats from pgxpool.
      Add `metricsPort` in Helm + ServiceMonitor template.

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

- [ ] **PodDisruptionBudget** in chart. `minAvailable: 1` by default.
      A rolling cluster upgrade can otherwise take both replicas down.

- [ ] **HorizontalPodAutoscaler** template (off by default; toggle via
      `autoscaling.enabled`). Once metrics land we can scale on
      `pgmqtt_connections` or CPU.

- [ ] **NetworkPolicy** template (off by default). Restrict broker→postgres
      egress to the postgres Service.

- [ ] **TLS termination example.** `docs/TLS.md` with a working config for:
      Nginx ingress `tcp-services` ConfigMap, HAProxy `frontend tls / backend
      tcp` mode, and an `Ingress` example for `wss://`. Don't ship the
      terminator in our chart; just document.

- [ ] **PSP / SCC**: chart already sets `runAsNonRoot`, etc. Verify against
      OpenShift restricted-v2 SCC.

- [ ] **Helm chart-tests directory** (`helm test pgmqtt`) with a Pod that
      runs the same mosquitto round-trip as `.github/ci/smoke-in-cluster.sh`.

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

- [ ] **`go test -cover` summary.** Add `make coverage` and post the
      summary to CI. Aim for ≥75% on each `internal/` package.

- [ ] **Race-mode CI.** Run `go test -race` in CI (we run it locally; CI
      currently doesn't).

## 4. Paho upstream

- [ ] **File a Paho issue / PR** for the `waitfor(callback.subscribeds, ...)`
      typo in `test_request_response` and `test_subscribe_options`. Both
      should be `callback2.subscribeds`. Link the issue from
      `docs/CONFORMANCE.md` and link back to my evidence (the broker logs
      that show subscribe/publish ordering).

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

- [ ] **Tag v0.1.0** once 1–4 are done. Helm chart in `gh-pages` branch
      (or `helm-charts` repo) so users can `helm repo add bo0tzz https://...`.

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
