# AGENTS.md

Operational notes for working in this repo as an autonomous agent (Claude
Code or otherwise). Designed as a load-bearing playbook: read it before
your first commit, link it from PRs that follow non-obvious workflows.

## Validation tier model

Three tiers, additive (`tier3 ⊃ tier2 ⊃ tier1`). Wall-clock + exact bug
coverage are documented in [`scripts/validate-timings.md`](scripts/validate-timings.md).
Short version:

| Tier  | When to run                       | Wall clock (local; CI)   | Run command                                       |
|-------|-----------------------------------|--------------------------|---------------------------------------------------|
| tier1 | After every code change           | ~20s; 3–5m               | `make validate TIER=tier1`                        |
| tier2 | Before commit / before PR         | ~7m; 10–12m              | `make validate TIER=tier2 PAHO=/tmp/paho-testing` |
| tier3 | Before tag (CI runs it weekly)    | ~18m                     | `make validate TIER=tier3 PAHO=/tmp/paho-testing` |

Every tier needs Docker. The Go suite runs against a Postgres
testcontainer, and under `validate.sh` (and in CI) a test that cannot
get one fails instead of skipping. A bare `go test ./...` on a machine
without Docker still skips those tests, which is most of them.

What each tier closes that the prior tier misses:

- **tier1** — vet, the whole Go suite under `-race`, chart lint + four
  renders + strict parse. Catches compile errors, SQL-path and engine
  regressions, data races, and chart structure problems (typoed values
  keys, duplicate label keys that helm emits but Flux/kustomize reject).
- **tier2** — paho v3+v5 conformance against a single broker, plus a
  second, non-race run of the suite for the coverage profile. Catches
  MQTT protocol shape bugs (reason codes, retain, will-delay, takeover
  ordering) that the Go tests don't.
- **tier3** — a 3-replica kind cluster: multi-broker paho, `helm test`,
  two soak smokes (QoS 1 and QoS 0). Catches cross-pod fanout bugs,
  deploy-shape regressions (helm-rendered manifests applied to a real
  apiserver), the operator path (User CR → Secret → password login) and
  sustained-load issues.

In CI:

- `ci.yml` runs on every PR, push to main and tag. `test`, `helm-lint`
  and `smoke (helm + kind)` cover tier1's surface plus a chart install
  against CNPG; `paho v3+v5 (single broker)` runs `validate.sh tier2`.
  All green is your gate to merge.
  Dependency scanning lives in its own `govulncheck (deps)` job so an
  upstream advisory doesn't read as a test failure; it still gates merge.
  For the same reason `validate.sh` doesn't run govulncheck in any tier;
  pass `--vulncheck` (or `make validate ... VULNCHECK=1`) to add it.
- Publishing is part of `ci.yml` too. `docker` pushes the image on main
  and on tags, and only after `test`, `helm-lint`, conformance and smoke
  have passed. On a tag, `chart` then publishes the chart (it calls
  `chart-release.yml`) once the image is up. A tag on which any of
  those four is red publishes nothing. `govulncheck` is not one of
  them.
- `soak-weekly.yml` — Mondays 04:00 UTC, two jobs. `tier3` runs
  `validate.sh tier3` and is the only automated multi-broker conformance
  run. `soak` runs a 60 s QoS 1 soak (fails on any loss or duplicate)
  and a 30 min QoS 0 soak (fails under 95% delivery). A failure in
  either job fails the run.

There is no nightly workflow; tier 2 moved into `ci.yml`.

## Worktree-based parallel agent workflow

This codebase has been built with parallel agents on a worktree-per-task
model. The pattern:

1. Each task gets its own git worktree under `.claude/worktrees/agent-<short-id>/`,
   on a branch named `worktree-agent-<short-id>`. This means N agents can
   `cd` into N different working copies and edit / build / test in parallel
   without stepping on each other's files.
2. Each worktree commits independently. The parent agent merges back to
   `main` via `git merge --no-ff worktree-agent-<id>` (see commits prefixed
   `merge worktree-agent-…` in the history).
3. Boundaries between worktrees are enforced by *task scope*, not by file:
   a worktree may touch any file relevant to its task, but the parent
   agent is responsible for sequencing merges so two worktrees aren't
   both modifying overlapping hot files concurrently.

Why this pattern works for this codebase:

- pgmqtt has a small set of dense files (engine, conn, janitor) and a long
  tail of small files (helm templates, tests, docs). Mechanical work on
  the tail (helm schema additions, doc fixes, lint passes) parallelises
  cleanly. Dense files are typically owned by one agent at a time.
- The merge cost is low: most worktrees touch ≤3 files outside their core
  task. Conflicts when they do happen are usually in `internal/engine/conn.go`,
  which we treat as a critical-section file: only one in-flight worktree
  may modify it.
- Each worktree's commits stay coherent — the merge commit on main names
  what each agent landed and why.

When NOT to use it:

- If the task is "make these three changes that all touch `engine.go`":
  that's a single-agent serial task. Don't spawn three worktrees that all
  fight for the same hot file.
- If the task crosses an architectural seam (e.g. "rip out the leader and
  go leaderless"): one agent, one branch. Coordinated rewrites lose
  coherence under parallelism.

## Common pitfalls observed

These have all bitten this codebase at least once. Listed here so future
agents can pattern-match instead of re-discovering.

### Helm scientific-notation rendering of int fields

Helm holds numbers from `values.yaml` as floats and prints them in
scientific notation from 1,000,000 up, unless the template forces an
integer. This turned `maxPacketSize: 16777216` into `1.6777216e+07`,
which the broker read as `1`: `getenvInt` used `fmt.Sscanf("%d")`, which
takes the leading integer prefix and drops the rest.

Two fixes:

- `getenvInt` in `internal/config/config.go` uses `strconv.Atoi`, which
  rejects `1.6777216e+07`. A rejected value does not stop the broker:
  it logs `config: failed to parse env var as int — using default` and
  runs with the default. So today this bug class shows up as a limit
  quietly sitting at its default, with one warning line at startup.
- The numeric env values in `templates/deployment.yaml` go through
  `int` before `quote`. (The two port values do not; a port cannot
  reach a million.)

Nothing automated checks the second point. `scripts/helm-check.sh`
strict-parses every render and asserts on some of their content, but
nothing searches a render for scientific notation, and its values all
come from `--set`, which hands helm an integer and never reproduces the
problem. When you add a numeric env to the chart, render it once with a
value of a million or more given in a values file, and read the output.

### kubectl port-forward instability under sustained load

`kubectl port-forward` uses an in-process userland forwarder that stalls
under backpressure. Symptoms in this repo:
- Paho conformance tests against a multi-broker kind cluster flake mid-run
  with `ConnectionRefused` on every fourth or fifth re-connect.
- `cmd/soak` reports loss / dups for QoS≥1 even when the broker is healthy,
  because the publisher's PUBLISH backs up against the forwarder, then the
  forwarder closes the socket.

Fix: don't port-forward. Either:
- Run the test client *inside* the cluster (`scripts/soak-incluster.sh`).
- For local single-broker work, run the broker directly on the host
  (`cmd/pgmqttd` against a docker postgres) — no kind, no forwarder.

The in-cluster soak rig (`scripts/soak-incluster.sh`) is the canonical
way to soak-test against a kind cluster. It builds a tiny distroless
image with the soak binary, side-loads it into kind, and runs the soak
as a Pod that connects to the Service VIP directly.
`scripts/paho-multi-broker-incluster.sh` does the same for Paho
conformance and is what tier 3 runs. `scripts/paho-multi-broker.sh` is
the old port-forward path, kept for debugging; nothing automated calls
it, and the weekly workflow step that did failed every week behind
`continue-on-error`.

### Agent over-claiming completeness

When an agent declares a task "done", that's a *claim*, not a *fact*.
This codebase has had at least three regressions where an earlier agent
asserted "tests pass" without re-running them after a refactor, including
the migration 0010 packet-cap=1 bug.

Two practices that actually help:
- The `validate.sh` tier model exists specifically so "I made a change"
  has an obvious answer to "what do I run now?". Use it. Don't substitute
  reasoning for a green tier2 run.
- "Production-ready means state, not list-cleared." When a TODO is empty,
  audit the repo for what's still rough — don't declare done just because
  the file has no unchecked boxes. (See the agent's
  [feedback memory](https://github.com/anthropics/claude-code) on this if
  you have access; the rule of thumb is: if you can't articulate the bug
  class your last commit catches, you didn't actually validate.)

### A free broker lock is not proof the broker is dead

Each broker holds a `pg_advisory_lock` on its LISTEN connection, and the
janitor reaps brokers whose lock is free. But a Postgres restart,
failover or crash recovery drops every listener session at once, so for
a few seconds every lock is free while every broker is alive. Reaping
on that evidence fired retained wills ("offline") for clients that were
still connected and wiped their will columns. It happened twice: a peer
reaped its neighbours (2026-06-29), and a broker whose pool reconnected
before its listener reaped itself (2026-11-17).

The rules now, in `internal/janitor` and `internal/listener`:

- A broker without its lock for `listener.LockReacquireDeadline` (20 s)
  exits.
- The janitor reaps a peer only after its lock has been free on every
  check for `janitor.DeadBrokerGrace` (the deadline plus 10 s).
- It counts that time only while its own lock was held without
  interruption, never judges anyone while its own lock is missing, and
  never judges itself.

The price is latency. The janitor checks every 5 s, so a broker that
really did die is reaped about 30–35 s after Postgres releases its
lock (at once, for a killed process). Only then are its clients' wills
fired and its sessions' ownership cleared. Anything that waits for a
will after killing a broker (SIGKILL, a lost node) has to allow for
that. A broker that shuts down normally is different: it publishes the
wills of the clients it disconnects itself, as it closes them.

The other side of the contract is just as fixed: neither 20 s nor the
margin is configurable, so a Postgres outage longer than 20 s makes
every broker Pod exit and restart.

Don't shorten the grace below the deadline, and don't "repair" a reap
after the fact instead of preventing it: a will that was published
can't be recalled.

### Janitor tick interval changes break paho

`PGMQTT_JANITOR_INTERVAL_MS` controls how often the janitor sweeps. Two
Paho v5 tests assume the default is 1 s:

- `test_session_expiry` reconnects about 2 s after a session's expiry
  and expects the session to be gone.
- `test_will_delay` accepts a will up to 2 s after its delay.

Bumping the default tick to 5 s landed once and was reverted within the
same session. With a 5 s tick both tests fail on every run (6 of 6 on
2026-11-18).

Keep the default at 1 s. Operators who want longer intervals set the
env explicitly.

Both are hard tests, so a longer default turns tier 2 and the CI
conformance job red.

### Paho conformance: the known-flaky list

`scripts/paho-conformance.py` runs the upstream Eclipse Paho test suite.
A test on its `--known-flaky` list that fails prints `FAIL(known-flaky)`
and does not change the exit code; any other failure does, and adds a
`hard fails:` line. The default list has four tests:

- `test_subscribe_failure` (in both the v3.1.1 and the v5 suite) and
  `test_shared_subscriptions` need ACLs and shared subscriptions, which
  pgmqtt does not have. They always fail.
- `test_request_response` and `test_subscribe_options` wait for the
  wrong client's SUBACK and then publish, so the publish races their
  own subscribe. They fail in most single-broker runs and usually pass
  in the multi-broker run.

`test_will_delay` was on the list until 0.3.0. It accepts a will up to
2 s late, and the janitor's 1 s jobs used to skip a tick whenever one
started slightly under a second after their last run, so a will could
be just over 2 s late (9 of 200 tries, 2026-11-18). Job gating now
allows half a tick of slack, and the test is hard again.

So the normal summary lines are `v311: 9/10 passing (+1 known-flaky)`
and, for v5, anything from `23/27 passing (+4 known-flaky)` (typical
for a single broker) to `25/27 passing (+2 known-flaky)` (typical for
the multi-broker run). Override the list with `--known-flaky ''` to
make every test hard.

