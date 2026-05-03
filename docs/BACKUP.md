# Backup and restore

`pgmqtt` keeps every authoritative piece of state in Postgres. Backing
up the database is sufficient to recover the broker — there is no
per-Pod local state worth preserving.

## What's worth backing up

Tables in `public.`:

| Table | Essential? | Notes |
| - | - | - |
| `users` | Yes | Bcrypt-hashed credentials. Lose them and clients can't authenticate. The User CRDs in Kubernetes can re-mint them, but only if the cleartext is in their referenced Secret. |
| `sessions` | Yes | v5 persistent sessions. Lose this and clean_start=false clients lose their queued/inflight QoS-1/2 state. |
| `subscriptions` | Yes | Per-session topic filters. Cascades from `sessions`. |
| `retained` | Yes | Retained messages. New subscribers won't receive history if you lose this. |
| `messages` | No | Ephemeral payloads referenced by `deliveries`. Lose them and any in-flight QoS>0 messages drop, but new traffic recovers. |
| `deliveries` | No | Same — re-derived from new traffic. |
| `inbound_qos2` | No | QoS-2 dedup tombstones. Lose them and you risk delivering a duplicate to a v5 client that PUBREL-replays its single un-completed message during the recovery window (1 h grace by default). Acceptable for most use-cases. |
| `mochi_*` if present | No | Reserved by the codec; not currently used. |
| `schema_migrations` | Yes | Records which migrations have been applied. Required for the broker to come up cleanly against a backup. |

In short: `users + sessions + subscriptions + retained + schema_migrations`
are the survival set; the rest is regenerable.

## pg_dump (any Postgres)

Stop-the-world consistent backup using a transaction snapshot. Run from
any host with `psql`/`pg_dump` and reach to the broker DB:

```bash
PGPASSWORD=$PASS pg_dump \
  -h $HOST -U $USER -d $DB \
  --format=custom --file=pgmqtt-$(date +%Y%m%d-%H%M).dump
```

Restore (into a fresh, empty DB):

```bash
createdb -h $HOST -U $USER pgmqtt
PGPASSWORD=$PASS pg_restore \
  -h $HOST -U $USER -d pgmqtt \
  --no-owner --no-acl pgmqtt-YYYYMMDD-HHMM.dump
```

## CloudNativePG (cnpg)

If your Postgres is managed by CloudNativePG, use the cnpg-native
flow — it understands streaming WAL backups and point-in-time
recovery. Reference: <https://cloudnative-pg.io/documentation/current/backup_recovery/>.

A typical setup:

```yaml
apiVersion: postgresql.cnpg.io/v1
kind: Cluster
metadata:
  name: pgmqtt-pg
  namespace: mqtt
spec:
  instances: 3
  storage:
    size: 10Gi
  backup:
    barmanObjectStore:
      destinationPath: s3://my-bucket/pgmqtt-backups
      s3Credentials:
        accessKeyId:
          name: backup-creds
          key: ACCESS_KEY_ID
        secretAccessKey:
          name: backup-creds
          key: SECRET_ACCESS_KEY
      wal:
        compression: gzip
    retentionPolicy: "30d"
```

## What to test in a recovery drill

After a restore, the broker should come up cleanly and:

1. `pgmqttd` Pods reach Ready with no migration errors.
2. Existing clients with cleansession=false / SessionExpiryInterval>0 can
   reconnect and receive any queued QoS-1/2 messages. (Test with one
   pre-seeded subscriber + a known retained topic.)
3. Retained messages are still delivered to new subscribers.
4. The User CRD reconciler doesn't constantly rewrite the `users` table
   (it shouldn't — `ObservedSecretHash` in CR status drives a no-op
   when nothing changed).

A 5-minute drill: stop the broker → drop `messages` and `deliveries`
(the regenerable set) → restart → verify a fresh publish reaches a
new subscriber. This confirms the survival set is enough for "broker
keeps working." Then test the actually-essential set on a separate
quiet DB by restoring the dump and pointing the broker at it.

## Schema migrations vs. restored backups

The broker applies any unapplied migrations on startup. If the dump
predates the running binary's migration set, the new migrations apply
on first boot — usually a non-issue, but check the migrations'
diff before restoring an old dump under a much newer binary.

If the dump *postdates* the binary (downgrade), startup will fail
with an "unknown migration" error. Roll the binary forward or restore
into a known-matching version.
