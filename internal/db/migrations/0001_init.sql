CREATE TABLE users (
  username      TEXT PRIMARY KEY,
  password_hash TEXT NOT NULL
);

CREATE TABLE sessions (
  client_id        TEXT PRIMARY KEY,
  broker_id        UUID,
  connected        BOOLEAN NOT NULL DEFAULT false,
  protocol_version SMALLINT NOT NULL,
  clean_start      BOOLEAN NOT NULL,
  expiry_interval  INT,
  next_packet_id   INT NOT NULL DEFAULT 1,
  will_topic       TEXT,
  will_payload     BYTEA,
  will_qos         SMALLINT,
  will_retain      BOOLEAN,
  will_delay       INT,
  will_properties  JSONB,
  last_seen        TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX sessions_broker_idx ON sessions(broker_id) WHERE broker_id IS NOT NULL;

CREATE TABLE subscriptions (
  client_id           TEXT REFERENCES sessions(client_id) ON DELETE CASCADE,
  topic_filter        TEXT NOT NULL,
  qos                 SMALLINT NOT NULL,
  no_local            BOOLEAN NOT NULL DEFAULT false,
  retain_as_published BOOLEAN NOT NULL DEFAULT false,
  retain_handling     SMALLINT NOT NULL DEFAULT 0,
  subscription_id     INT,
  PRIMARY KEY (client_id, topic_filter)
);
CREATE INDEX subscriptions_filter_idx ON subscriptions(topic_filter);

CREATE TABLE retained (
  topic       TEXT PRIMARY KEY,
  payload     BYTEA NOT NULL,
  qos         SMALLINT NOT NULL,
  properties  JSONB,
  updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE messages (
  id          BIGSERIAL PRIMARY KEY,
  topic       TEXT NOT NULL,
  payload     BYTEA NOT NULL,
  qos         SMALLINT NOT NULL,
  retain      BOOLEAN NOT NULL,
  properties  JSONB,
  publisher_client_id TEXT,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX messages_created_idx ON messages(created_at);

CREATE TABLE deliveries (
  id          BIGSERIAL PRIMARY KEY,
  client_id   TEXT NOT NULL REFERENCES sessions(client_id) ON DELETE CASCADE,
  message_id  BIGINT NOT NULL REFERENCES messages(id) ON DELETE CASCADE,
  qos         SMALLINT NOT NULL,
  packet_id   INT,
  state       SMALLINT NOT NULL,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX deliveries_client_state_idx ON deliveries(client_id, state, id);
CREATE UNIQUE INDEX deliveries_client_packet_idx ON deliveries(client_id, packet_id) WHERE packet_id IS NOT NULL;

CREATE OR REPLACE FUNCTION mqtt_topic_match(filter TEXT, topic TEXT)
RETURNS BOOLEAN LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE AS $$
DECLARE
  f TEXT[] := string_to_array(filter, '/');
  t TEXT[] := string_to_array(topic,  '/');
  i INT := 1;
  flen INT := array_length(f, 1);
  tlen INT := array_length(t, 1);
BEGIN
  IF flen IS NULL OR tlen IS NULL THEN
    RETURN false;
  END IF;
  -- $-prefixed topics never match wildcard-rooted filters per MQTT spec.
  IF (f[1] = '#' OR f[1] = '+') AND substr(topic, 1, 1) = '$' THEN
    RETURN false;
  END IF;
  WHILE i <= flen LOOP
    IF f[i] = '#' THEN
      RETURN true;
    END IF;
    IF i > tlen THEN
      RETURN false;
    END IF;
    IF f[i] <> '+' AND f[i] <> t[i] THEN
      RETURN false;
    END IF;
    i := i + 1;
  END LOOP;
  RETURN flen = tlen;
END$$;

-- Atomically allocate the next packet identifier for a session, wrapping at 65535
-- while skipping IDs currently reserved by un-acked deliveries.
CREATE OR REPLACE FUNCTION mqtt_next_packet_id(p_client_id TEXT)
RETURNS INT LANGUAGE plpgsql AS $$
DECLARE
  candidate INT;
  i INT := 0;
BEGIN
  LOOP
    UPDATE sessions
       SET next_packet_id = CASE WHEN next_packet_id >= 65535 THEN 1 ELSE next_packet_id + 1 END
     WHERE client_id = p_client_id
     RETURNING next_packet_id INTO candidate;
    IF candidate IS NULL THEN
      RAISE EXCEPTION 'session not found: %', p_client_id;
    END IF;
    -- Conflict only happens with QoS>0 inflight reuse, which is rare; bound the loop.
    PERFORM 1 FROM deliveries WHERE client_id = p_client_id AND packet_id = candidate;
    IF NOT FOUND THEN
      RETURN candidate;
    END IF;
    i := i + 1;
    IF i > 65535 THEN
      RAISE EXCEPTION 'no free packet id for session %', p_client_id;
    END IF;
  END LOOP;
END$$;
