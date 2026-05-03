-- v5 timing semantics. Both columns are NULL when nothing is scheduled.
ALTER TABLE sessions
  ADD COLUMN will_fire_at TIMESTAMPTZ,
  ADD COLUMN session_expires_at TIMESTAMPTZ;

-- Helps the janitor scan for due wills / expired sessions cheaply.
CREATE INDEX sessions_will_fire_idx ON sessions(will_fire_at) WHERE will_fire_at IS NOT NULL;
CREATE INDEX sessions_expires_idx ON sessions(session_expires_at) WHERE session_expires_at IS NOT NULL;

-- Inbound QoS-2 dedup: track packet IDs received for which we've already
-- forwarded the publish. Indexed PRIMARY KEY supplies the dedup check.
CREATE TABLE inbound_qos2 (
  client_id   TEXT REFERENCES sessions(client_id) ON DELETE CASCADE,
  packet_id   INT NOT NULL,
  received_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (client_id, packet_id)
);

-- Retained-message expiry: store an absolute expires_at when v5 publishers
-- include MessageExpiryInterval. Janitor sweeps expired retained rows.
ALTER TABLE retained
  ADD COLUMN expires_at TIMESTAMPTZ;
CREATE INDEX retained_expires_idx ON retained(expires_at) WHERE expires_at IS NOT NULL;
