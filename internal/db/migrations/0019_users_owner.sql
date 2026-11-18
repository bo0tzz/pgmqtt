-- Record which User object owns each users row.
--
-- The table is keyed on the bare MQTT username, but User objects are
-- namespaced and may set spec.username freely, so two of them can resolve
-- to the same row. Without an owner on the row the operator could not tell
-- them apart: the last one reconciled set the password, and deleting either
-- one deleted the account.
--
-- The operator writes (owner_namespace, owner_name, owner_uid) with every
-- upsert and only updates or deletes a row whose owner matches. A row with
-- owner_uid IS NULL has no owner yet: every row that existed before this
-- migration, and rows inserted by hand. The first User reconciled against
-- such a row adopts it.
--
-- Rolling upgrade: the columns are nullable with no default, so a broker
-- from before this migration keeps working. Its INSERT names only
-- (username, password_hash) and leaves the owner NULL; its SELECT and
-- DELETE do not reference the new columns. Adding them is a catalog-only
-- change (no table rewrite).
ALTER TABLE users
  ADD COLUMN IF NOT EXISTS owner_namespace TEXT,
  ADD COLUMN IF NOT EXISTS owner_name      TEXT,
  ADD COLUMN IF NOT EXISTS owner_uid       TEXT;
