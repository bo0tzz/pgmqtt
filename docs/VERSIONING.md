# Versioning policy

`pgmqtt` follows [Semantic Versioning 2.0.0](https://semver.org/spec/v2.0.0.html)
across three surfaces, treated as a single product version:

1. The `pgmqttd` broker binary and its on-the-wire MQTT 3.1.1 / 5 behaviour.
2. The `pgmqtt.io/*` Kubernetes operator API (CRDs and their reconciler
   semantics).
3. The Postgres schema, defined as a sequence of versioned migrations
   in `internal/db/migrations/` (currently `0001_init.sql` through
   `0011_fix_publish_cap_off_by_one.sql`). **This line is bumped per
   migration** — if you're adding a migration, update the top filename
   here in the same commit so future drift is obvious.

The Helm chart version tracks the same `MAJOR.MINOR.PATCH` and is bumped
in lockstep with the broker.

## Migration policy: rolling-deploy safety

Postgres migrations apply unconditionally on broker startup. During a
rolling Deployment update there is a window — typically seconds, but
unbounded in degenerate cases — where one Pod runs the new binary
(having applied any pending migrations) while N-1 Pods still run the
old binary. **Any migration that removes a column, function, or table
that the old binary still references will produce error spam from the
old Pods until they finish rolling.**

Mitigation: split removal-style schema changes across two releases.

1. Release N: stop the broker code from depending on the soon-to-be-
   removed schema item (e.g. drop the call to a SQL function in
   favour of an in-memory implementation, or stop reading a column).
   Keep the schema item itself.
2. Release N+1: remove the schema item with a migration. By the time
   this lands, no live broker pod is using it.

A new `### Migration` callout in `CHANGELOG.md` should accompany any
release that removes schema; explain the prior release that dropped
the dependency. If a single release must do both (rare; usually only
acceptable when no production user is on the prior version yet),
flag it in the CHANGELOG and warn operators to scale the broker
deployment to 1 replica before the upgrade.

This policy is recorded after migration `0009` dropped
`sessions.next_packet_id` + the `mqtt_next_packet_id()` SQL function
in the same release that converted the broker to in-memory packet-id
allocation. The change was safe because nothing was in production at
that point, but the rolling-deploy error window was visible during
internal soak rebuilds.

## What bumps which segment

### MAJOR (X.0.0) — break

- A change in MQTT spec compliance that a previously-working client may
  observe as a regression (e.g. a CONNACK reason code flips, a previously-
  delivered packet shape stops being delivered, retain semantics change).
- Removal or rename of a CRD field, CRD version, or a CRD whose
  `storage: true` migration cannot be done in-place.
- Postgres schema change that is not safe to apply against an existing
  populated DB without a documented migration step (column drop, type
  narrowing, NOT NULL added without default, table rename, etc.).
- Removal of a `PGMQTT_*` env var that previously had a non-default
  effect, or removal of a Helm value with no replacement.

### MINOR (x.Y.0) — additive

- New feature behind a default-off flag, or a new feature whose default
  is observably the prior behaviour.
- New `PGMQTT_*` env var with a safe default that preserves prior
  behaviour.
- New CRD field with a safe default; new CRD version added alongside
  the existing storage version.
- Postgres schema addition that is safe to apply against a populated
  DB (new nullable column, new table, new index).
- New Helm value with a safe default.
- New conformance result going from FAIL to PASS without affecting
  unrelated clients.

### PATCH (x.y.Z) — fix

- Bugfix that brings behaviour back into line with documented spec /
  intent.
- Documentation, comment, test, or CI change.
- Performance improvement with no observable behaviour change.
- Dependency bump that doesn't change the public surface.
- Operator reconciler internal refactor that produces identical
  reconciled state.

When in doubt: a thing a careful operator could legitimately depend on
is a public surface, and breaking it is MAJOR. The CRD's
`v1alpha1` label is *not* a license to break — it sets expectations,
but each break still gets a MAJOR bump until graduation.

## CHANGELOG discipline

The repo's [`CHANGELOG.md`](../CHANGELOG.md) follows the [Keep a
Changelog](https://keepachangelog.com/en/1.1.0/) convention.

- Every PR with an observable change adds an entry under
  `## [Unreleased]`, in the appropriate `### Added | Changed |
  Deprecated | Removed | Fixed | Security` group. Pure-internal
  refactors with no observable effect can skip the entry.
- Entries are written in the past tense, present-perfect or
  imperative — match the surrounding style. Reference the relevant
  PG schema table, env var, CRD field, Helm value, or test name when
  it makes the entry locatable.
- Cutting a release renames the `[Unreleased]` heading to
  `[X.Y.Z] - YYYY-MM-DD`, leaves the entries in place, and adds a
  fresh empty `## [Unreleased]` section above it. The git tag
  `vX.Y.Z` is created on the same commit that performs that rename.
- Tag re-pointing (e.g. moving `v0.1.0` to a later HEAD before any
  `v0.1.0` artifact has been published) is acceptable until first
  release; once a tag has shipped to users it is immutable.

## Pre-1.0

While the version is `0.y.z`, MINOR bumps may include breaking
changes per [SemVer §4](https://semver.org/spec/v2.0.0.html#spec-item-4).
We try to avoid this — every break still gets a CHANGELOG entry
under `### Removed` or `### Changed` and a migration note — but
upgrades across `0.y` boundaries should be read carefully.
