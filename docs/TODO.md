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
- [x] **PSP / SCC**: chart's broker pod sets `runAsNonRoot=true`,
      `allowPrivilegeEscalation=false`, `readOnlyRootFilesystem=true`,
      drops all capabilities, and uses `seccompProfile: RuntimeDefault`.
      That matches OpenShift `restricted-v2` SCC's *requirements*. The
      one OpenShift wrinkle: hardcoded `runAsUser: 65532` is overridden
      by the SCC mutating admission (which assigns a UID from the
      namespace range). Operators on OpenShift should `--set
      podSecurityContext.runAsUser=null,podSecurityContext.runAsGroup=null,podSecurityContext.fsGroup=null`
      to let SCC populate them. (Verify with `oc adm policy
      who-can use scc/restricted-v2`.)
- [x] **Helm chart-tests directory** (`helm test pgmqtt`) with a mosquitto
      round-trip Pod (helm.sh/hook=test).

## 3. Testing actually run

- [x] **10-minute soak**: `cmd/soak` traffic-generator runs publishers +
      N subscribers, tracks per-message sequence numbers, emits a JSON
      report (`published`, `received`, `lost`, `dups` per sub) and exits
      non-zero on QoS≥1 loss or QoS-2 dups. `scripts/soak.sh` wraps it
      with a `kubectl delete pod` chaos loop on a 30s cadence. Local
      smoke verified zero loss/dups for QoS-1 and QoS-2 at 200 msg/s
      against a single-broker Postgres.

- [x] **Multi-broker Paho conformance.** `scripts/paho-multi-broker.sh`
      spins a kind cluster, helm-installs a 3-replica broker, port-
      forwards the Service to localhost, and runs the existing
      `paho-conformance.py` wrapper through the VIP. Verifies cross-Pod
      fanout under conformance load. (Operator-run; not in CI by default
      — kind+helm install takes ~3 min.)

- [x] **Sustained chaos covers the workload shapes that needed it.**
      `cmd/soak` runs the publisher/subscriber rig; `scripts/soak.sh`
      wires the kubectl-delete-pod chaos loop. We previously bundled a
      `scripts/ha-z2m-soak.sh` that booted Home Assistant + a fake-Z2M
      heartbeat publisher, but it added containers without testing
      anything our broker-only chaos didn't already cover; removed in
      492ab1e. Operators wanting HA/Z2M-specific coverage can layer it
      on top of `cmd/soak` themselves.

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

## 8. Post-tag round 2 (after the v0.1.0 retag)

Driven by repeated audits of the post-tag tree (a transactional-
consistency audit, a dead-wiring audit, a perf round, an MQTT 5 spec
walk-through, a metrics-completeness audit, and a Helm-knobs audit).
Each found real material; the items below are the ones that landed.

- [x] **Atomicity / correctness round 1.** publishCore single-tx
      (covers retain + qos2 dedup + insert + fanout + pg_notify),
      cleanStart cleanup tx, will-fire ordering inversion, immediate-
      will column NULL after fire, dead-broker pg_try_advisory_lock,
      janitor.expireSessions in one tx with explicit deliveries DELETE.
- [x] **Atomicity / correctness round 2.** inbound_qos2 leak across
      cleanStart=true reconnect, AUTH packet silent drop, sessionExpiry
      uint32 narrowing + presence flag, panic recovery at every
      long-lived goroutine boundary (per-Conn run + drainLoop, listener
      run + per-NOTIFY dispatch, janitor RunWith + per-tick, metrics
      serve, operator.Run), tick context cancels on leader.Lost(),
      ownership sweep closes orphaned sockets after silent takeover.
- [x] **Perf.** Drop deliveries.client_id FK (MultiXact thrash), partial
      index for inflight scan (0007), partial index for resume scan
      (0008), in-broker per-Conn packet ID counter (0009 drops the
      sessions.next_packet_id column + the SQL function). Re-measure
      showed 2.79× throughput post-FK-drop alone.
- [x] **Operability — broker.** delivery_seconds histogram, auth_failures
      counter, janitor per-job timing + errors, table-cardinality
      gauges, will-fire lateness, outbound-inflight saturation,
      connections-capacity-ratio, controller-runtime metrics on our
      /metrics endpoint, JSON log handler.
- [x] **Operability — Helm.** podAntiAffinity preset, imagePullSecrets,
      podLabels, extraVolumes, priorityClassName,
      topologySpreadConstraints, terminationGracePeriodSeconds,
      probe tunability (delays/thresholds), extraEnvFrom, service LB
      knobs, hostNetwork / dnsPolicy / dnsConfig / runtimeClassName,
      automountServiceAccountToken: false default.
- [x] **Leader.** Crash-loop on unexpected leader-loss (kubelet
      restarts the pod), bounded-exposure documentation of the fence
      window (10s Ping interval mitigated by row-level locking in
      expireSessions, advisory_lock in handleDeadBroker, idempotent
      writes in operator).
- [x] **Docs.** PERF.md, BACKUP.md schema audit, CONFORMANCE.md v5
      spec omissions, OPS.md crash-loop-on-leader-loss, SIZING.md
      recalibration pointer.

### 8a. Round 2 — deferred

Real but lower-impact than what landed:

- [x] **Strict leader-write fence.** Resolved by going leaderless:
      the janitor now runs on every Pod (every operation is already
      concurrency-safe by construction — see janitor package doc),
      and the operator switched to controller-runtime's K8s Lease
      leader election. The L1 fence concern, L2 re-arm concern, and
      L4 lost-cancellable-tick concern all dissolve because there's
      no singleton leader to flap.
- [ ] **Real-hardware perf re-measure.** All current numbers in
      PERF.md are from a contended kind cluster. A clean dedicated-
      host run on the same shape (5 pubs × inflight=50 × 5000/s ×
      90s) closes the loop.
- [ ] **Long-soak validation.** A 12h soak run validating WAL
      bloat / autovacuum behavior over hours is run-once and worth
      recording.
- [ ] **Sub-disconnect investigation at high rate.** The post-FK-drop
      perf re-measure observed `drainSessionQueue` ~5/sec/sub at
      5pubs × inflight=50 × 5000/s. The 100 msg/s soak run at v0.1.x
      HEAD shows zero churn, so the high-rate observation needs its
      own investigation with the new metrics. Filed as
      task #108 (drain_session_queue_total{reason} counter).
