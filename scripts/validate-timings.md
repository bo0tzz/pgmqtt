# validate.sh — measured wall-clock + bug coverage per tier

`scripts/validate.sh` is the canonical pre-commit / pre-tag validation
entrypoint. This page records what each tier actually costs in wall-clock
on a representative developer laptop, what categories of bugs each tier
catches that the previous tier misses, and where the dominant time goes.

Numbers are from a 2026-05 session on a 12-core laptop, NVMe disk, Docker
in rootless mode. Treat them as the lower-bound floor; CI runners are
slower (typically 1.5–2× on the same workload).

## tier1 — `scripts/validate.sh tier1`

Wall clock: **≈10s** (cold), 5–7s warm cache.

Phases:

| Phase             | Time  | What it actually runs                                  |
|-------------------|-------|--------------------------------------------------------|
| `go vet`          |  ~2s  | `go vet ./...`                                         |
| `make test-race`  |  ~5s  | `go test ./... -count=1 -race -timeout 10m` (test-light packages — full unit suite is in tier2) |
| `helm lint`       |  <1s  | `helm lint deploy/helm/pgmqtt`                         |
| `helm template`   |  ~2s  | three render variants: defaults, multi-replica + UI, external secret + bcrypt-cost override |

The `go test -race` time is misleading: most of pgmqtt's heavy tests are
gated on `testcontainers` and only run when `PGMQTT_TEST_DATABASE_URL` is
set. Tier1 deliberately runs only the side without it (fast, hermetic).
Coverage of the test-DB path lives in tier2's `make coverage` phase.

**Bug classes caught here that nothing else does:**
- Type errors / missing-import / compile fails (fast feedback).
- Static `go vet` issues (mostly printf-vet and copy-by-value-of-Mutex).
- Helm chart structural issues (`helm lint` + `helm template`) — typoed
  values keys, indentation drift, render-time-only Sprig errors. The
  recent strict int-parsing fix (`maxPacketSize=1` silently accepted)
  was caught at `helm template` time.
- The data race detector when run against the small set of pure-Go
  tests in tier1.

**Bug classes NOT caught here:**
- Anything that needs Postgres (no testcontainer here).
- Protocol-level bugs (no MQTT client speaks to the broker).
- Multi-replica fanout issues (no kind cluster).

Run after: every code change, in the editor save loop. Treat tier1 as
"`make ci-light`" — should always be green before you write a commit
message.

## tier2 — `scripts/validate.sh tier2 --paho PATH`

Wall clock: **≈6m12s** total on this laptop.

Phases (from a real run):

| Phase                    | Time   | Notes                                          |
|--------------------------|--------|------------------------------------------------|
| (tier1 prefix)           | ~10s   | re-runs the whole tier1                        |
| `make coverage`          | ~80s   | full `go test ./... -coverprofile -timeout 10m` against testcontainer Postgres |
| `paho setup`             | ~10s   | docker-pulls postgres:18-alpine, boots single broker |
| `paho v3+v5`             | ~355s  | dominant cost: paho.mqtt.testing's `client_test_v3.py` + `client_test_v5.py`, run sequentially |
| `paho teardown`          |  <1s   | docker-rm + kill                              |

The 6m12s figure is the wall-clock observed in the same session that
landed migration 0010 + this page. The paho v3+v5 phase is ~95% of it;
splitting v3 and v5 to run in parallel is the obvious next optimisation
(blocked on parameterising the broker port and conformance wrapper).

**Bug classes caught here that tier1 doesn't:**
- Anything that touches the SQL path (most of pgmqtt's behaviour).
  Migration 0010's publish-cap short-circuit + autovacuum tuning was
  validated here.
- MQTT 3.1.1 + 5.0 protocol conformance: PUBREC reason codes, retain
  semantics, will-delay timing, session-takeover ordering, v5
  property-shape regressions. The packet-cap=1 bug surfaced as paho
  v5 publish-cap-conformance failures before being root-caused as
  helm scientific-notation rendering at the chart level.
- Subscriber-quota DISCONNECT 0x97 path (TestSlowSubscriberQuotaExceeded,
  re-tested as part of the full coverage run).

**Bug classes NOT caught here:**
- Multi-pod fanout (single broker only).
- Sustained-load behaviour (paho's tests are correctness-first, not
  rate-driven).
- Deploy-shape bugs (no kind / helm install).

Run after: any non-trivial change, before commit / before opening a PR.

## tier3 — `scripts/validate.sh tier3 --paho PATH`

Wall clock: **measurement TBD** pending #142's in-cluster soak rig
landing fully (a previous run via kubectl port-forward died mid-stream
under sustained load — exactly the bug-class the rig fixes). Expect
~12–18m end-to-end:

| Phase                    | Estimated  | Notes                                          |
|--------------------------|------------|------------------------------------------------|
| (tier2 prefix)           | ~6m        | full tier2                                     |
| `multi-broker paho`      | ~5–7m      | `scripts/paho-multi-broker.sh`: kind create + helm install + paho via Service VIP |
| `soak setup`             | ~10s       | re-runs single-broker for the soak smoke phase |
| `soak smoke`             | ~70s       | 60s × 1000 msg/s × QoS 1 across 3 pubs / 3 subs |

Once `scripts/soak-incluster.sh` (#142) is wired into validate.sh's tier3
the soak smoke phase will move into the same kind cluster as the multi-
broker paho run, and the dual setup/teardown will collapse.

**Bug classes caught here that tier2 doesn't:**
- Cross-pod fanout: subscriptions on Pod A, publishes on Pod C, the
  delivery row + NOTIFY plumbing is exercised end-to-end.
- Sustained-load behaviour over kind networking. The packet-cap=1
  regression that escaped to a tagged release would have surfaced as
  zero-or-near-zero throughput under the soak smoke.
- Helm-rendered template applied to a real apiserver — schema
  validation, security context, PDB/HPA/NetworkPolicy admission.
- Operator (User CR) reconciler behaviour under realistic Lease
  contention.
- Image build correctness (the Dockerfile's static binary path runs
  here; a CGO-on or libc-linked drift would surface as ImagePullBackOff
  or runtime crash).

Run before: cutting a tag, weekly nightly CI, after any change to the
helm chart's pod-spec or to the broker's connection / fanout path.

## Cumulative coverage

| Tier  | What it adds over the previous tier                              |
|-------|------------------------------------------------------------------|
| tier1 | static + lint + small unit tests + chart render. Cheap floor.    |
| tier2 | full coverage (testcontainer DB) + protocol conformance.         |
| tier3 | deploy-shape + multi-pod fanout + sustained-load smoke.          |

The cadence in CI follows the same shape: tier1 surface is in `ci.yml`
(every PR), tier2 is `conformance-nightly.yml` (daily 03:00 UTC), tier3
is `soak-weekly.yml` (Mondays 04:00 UTC). See those workflow files for
the exact wiring.
