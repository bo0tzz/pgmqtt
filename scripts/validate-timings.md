# validate.sh — measured wall-clock + bug coverage per tier

`scripts/validate.sh` is the canonical pre-commit / pre-tag validation
entrypoint. This page records what each tier costs in wall-clock, what
categories of bugs each tier catches that the previous tier misses, and
where the time goes.

Two sets of numbers:

- **Local**: 2026-11-18, a 20-thread laptop (i9-12900H, NVMe, Docker 29),
  Go build cache and container images warm.
- **CI**: the `conformance` job's own phase summary on GitHub's
  `ubuntu-latest` runners, October–November 2026. Those runners start
  with a cold Go build cache, which is where most of the difference in
  the first two phases comes from.

Every tier needs Docker, and fails without it. About three quarters of
the Go suite (122 of 163 tests on 2026-11-18: the db, engine, janitor,
listener and operator packages) runs against Postgres 18 in a
testcontainer, one container per package. `internal/db/dbtest` skips
those tests when the container cannot start, unless `CI` or
`PGMQTT_TEST_REQUIRE_DB` is set; `validate.sh` sets the latter, so a
green tier always means they ran.
`PGMQTT_TEST_DATABASE_URL` points them at an existing server instead.

`govulncheck` is not part of any tier. `--vulncheck` adds it as a first
phase (about 5 s once installed); CI runs it as its own job.

## tier1 — `scripts/validate.sh tier1`

Wall clock: **≈15–20 s** local, **3–5 min** on a CI runner.

| Phase            | Local | CI     | What it runs                                           |
|------------------|-------|--------|--------------------------------------------------------|
| `go vet`         | ~1s   | 1–2m   | `go vet ./...`                                         |
| `make test-race` | 14–17s | 2–3m   | `go test ./... -count=1 -race -timeout 10m`: the whole suite, including every database-backed test |
| `helm lint`      | <1s   | <1s    | `scripts/helm-check.sh lint`                           |
| `helm template`  | a few s | a few s | `scripts/helm-check.sh render`: renders the chart at its Kubernetes floor (1.30) under a couple of dozen value sets, strict-parses each with `kustomize build`, asserts on the content of several, and checks that the combinations the chart must refuse are refused with the right error |

**Bug classes caught here:**
- Type errors / missing-import / compile fails.
- Static `go vet` issues (mostly printf-vet and copy-by-value-of-Mutex).
- Anything the Go tests cover, which is most of the broker: the SQL
  path, migrations, the engine, the janitor, the listener, and the
  operator's reconciler against controller-runtime's fake client.
- Data races in all of the above.
- Helm chart issues that show up in a render: typoed values keys,
  indentation drift, render-time-only Sprig errors, duplicate mapping
  keys that helm emits but Flux and kustomize reject, named ports that
  nothing declares, value combinations the binary would refuse at
  startup.

**Bug classes NOT caught here:**
- Protocol-level bugs that only a real MQTT client finds (no client
  other than the test harness speaks to the broker).
- Multi-replica fanout issues (no kind cluster).
- A chart value rendered in scientific notation. No render is searched
  for it; see "Helm scientific-notation rendering" in `AGENTS.md`.

Run after: every code change. It should always be green before you
write a commit message.

## tier2 — `scripts/validate.sh tier2 --paho PATH`

Wall clock: **≈7 min** (408 s) local, **10–12 min** on a CI runner. CI runs
exactly this as the `paho v3+v5 (single broker)` job in `ci.yml`, on
every PR, push to main and tag.

| Phase           | Local | CI      | Notes                                          |
|-----------------|-------|---------|------------------------------------------------|
| (tier1 prefix)  | ~15s  | 3–5m    | re-runs the whole tier1                        |
| `make coverage` | ~10s  | 30–45s  | the suite a second time, without `-race`, for `coverage.out` / `coverage.txt` |
| `paho setup`    | ~4s   | 5–7s    | starts `postgres:18-alpine` and one broker on free loopback ports |
| `paho v3+v5`    | ~6.5m | ~6m     | paho.mqtt.testing's `client_test.py` + `client_test5.py`, run sequentially by `scripts/paho-conformance.py` |
| `paho teardown` | <1s   | <1s     | stops the broker, removes the container        |

The Paho phase is nearly all of it, and nearly all of that is the
suite's own `sleep` calls, so it does not get faster on a faster
machine. Ports, container name and scratch files are per run, so two
worktrees can run tier2 at the same time.

**Bug classes caught here that tier1 doesn't:**
- MQTT 3.1.1 + 5.0 protocol conformance as an independent client sees
  it: PUBREC reason codes, retain semantics, will-delay timing,
  session expiry, session-takeover ordering, v5 property shapes. The
  packet-cap=1 bug surfaced here as publish failures before being
  root-caused as helm scientific-notation rendering.

Expected result: `v311: 9/10 passing (+1 known-flaky)` and
`v5: 23/27 passing (+4 known-flaky)`; a v5 count of 24 or 25 is also
normal. The known-flaky list is explained in `AGENTS.md`.

**Bug classes NOT caught here:**
- Multi-pod fanout (single broker only).
- Sustained-load behaviour (paho's tests are correctness-first, not
  rate-driven).
- Deploy-shape bugs (no kind / helm install).

Run after: any non-trivial change, before commit / before opening a PR.

## tier3 — `scripts/validate.sh tier3 --paho PATH`

Wall clock: **≈18 min** (1064 s) local. CI runs exactly this as the `tier3`
job of `soak-weekly.yml` (Mondays 04:00 UTC); that job has not run
yet, so there is no CI figure.

| Phase                    | Local  | Notes                                          |
|--------------------------|--------|------------------------------------------------|
| (tier2 prefix)           | ~7m    | full tier2                                     |
| `tier3 cluster setup`    | ~47s   | docker build `pgmqtt:tier3`, kind create, Postgres 18 StatefulSet, helm install with 3 replicas |
| `multi-broker paho`      | ~6.5m  | `scripts/paho-multi-broker-incluster.sh`: builds a python + paho image, kind-loads it, runs it as a Pod against `pgmqtt.<ns>.svc.cluster.local:1883` |
| `helm test`              | ~7s    | applies a User CR, waits for the operator's credentials Secret, runs the chart's round-trip test hook |
| `soak smoke (qos1)`      | ~80s   | `scripts/soak-incluster.sh`: 60s × 1000 msg/s × QoS 1, 3 pubs / 3 subs |
| `soak smoke (qos0)`      | ~135s  | `scripts/soak-incluster.sh`: 120s × 100 msg/s × QoS 0, 1 pub / 3 subs |
| `tier3 cluster teardown` | a few s | kind delete cluster                            |

Everything that talks to the cluster runs as a Pod inside it. The older
port-forward path (`scripts/paho-multi-broker.sh`) died after about ten
connections and is not used by anything. The cluster is whatever node
image the installed `kind` defaults to; the chart refuses Kubernetes
older than 1.30.

If a cluster phase fails, `validate.sh` prints pod state, the last 200
log lines of every broker Pod and recent events before it deletes the
cluster. `--keep-cluster` leaves the cluster up instead.

**Bug classes caught here that tier2 doesn't:**
- Cross-pod fanout: subscriptions on Pod A, publishes on Pod C, the
  delivery row + NOTIFY plumbing is exercised end-to-end.
- Sustained-load behaviour over kind networking. The packet-cap=1
  regression that escaped to a tagged release would have surfaced as
  zero-or-near-zero throughput under the soak smoke.
- Helm-rendered templates applied to a real apiserver: schema
  validation, security context, the chart's own test hook.
- The operator end to end, by `helm test`: a User CR is reconciled into
  a `users` row and a credentials Secret, and a client logs in with
  that password. The Paho and soak phases connect anonymously.
- Image build correctness (the Dockerfile's static binary path runs
  here; a CGO-on or libc-linked drift would surface as a crash on
  start).

Expected Paho result: `v311: 9/10 passing (+1 known-flaky)` and
`v5: 25/27 passing (+2 known-flaky)`.

Run before: cutting a tag, and after any change to the chart's pod spec
or to the broker's connection / fanout path.

## Cumulative coverage

| Tier  | What it adds over the previous tier                              |
|-------|------------------------------------------------------------------|
| tier1 | static checks + the whole Go suite under `-race` + chart render. |
| tier2 | protocol conformance from an independent client; coverage profile. |
| tier3 | deploy shape + multi-pod fanout + operator + sustained-load smoke. |

Where each tier runs in CI:

| Tier  | Workflow / job                                   | When                          |
|-------|--------------------------------------------------|-------------------------------|
| tier1 | `ci.yml`: `test`, `helm-lint` (the same commands, as separate jobs) | every PR, push to main, tag |
| tier2 | `ci.yml`: `paho v3+v5 (single broker)`           | every PR, push to main, tag   |
| tier3 | `soak-weekly.yml`: `tier3`                       | Mondays 04:00 UTC, or by hand |

There is no nightly workflow. `ci.yml` also runs `smoke (helm + kind)`,
a chart install against a CloudNativePG cluster with a User CR
round-trip, which no tier runs locally.
