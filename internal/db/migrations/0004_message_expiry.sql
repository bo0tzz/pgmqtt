-- v5 Message Expiry Interval. Stored as an absolute timestamp derived from
-- properties.me (mochi's JSON tag for MessageExpiryInterval) at insert.
ALTER TABLE messages
  ADD COLUMN expires_at TIMESTAMPTZ;
CREATE INDEX messages_expires_idx ON messages(expires_at) WHERE expires_at IS NOT NULL;

-- Replace mqtt_publish to set expires_at when properties.me > 0.
CREATE OR REPLACE FUNCTION mqtt_publish(
    p_topic         TEXT,
    p_payload       BYTEA,
    p_qos           SMALLINT,
    p_retain        BOOLEAN,
    p_properties    JSONB,
    p_publisher     TEXT
) RETURNS TABLE (msg_id BIGINT, broker_ids UUID[]) AS $$
DECLARE
    v_msg_id BIGINT;
    v_brokers UUID[];
    v_expires TIMESTAMPTZ;
    v_me INT;
BEGIN
    v_me := COALESCE((p_properties->>'me')::int, 0);
    IF v_me > 0 THEN
        v_expires := now() + make_interval(secs => v_me);
    END IF;

    INSERT INTO messages(topic, payload, qos, retain, properties, publisher_client_id, expires_at)
    VALUES (p_topic, p_payload, p_qos, p_retain, p_properties, p_publisher, v_expires)
    RETURNING id INTO v_msg_id;

    WITH matches AS (
        SELECT DISTINCT ON (s.client_id)
               s.client_id,
               sess.broker_id,
               sess.connected,
               LEAST(s.qos, p_qos)::smallint AS deliver_qos
          FROM subscriptions s
          JOIN sessions sess USING (client_id)
         WHERE mqtt_topic_match(s.topic_filter, p_topic)
           AND NOT (s.no_local AND p_publisher IS NOT NULL AND s.client_id = p_publisher)
         ORDER BY s.client_id, s.qos DESC
    ),
    inserted AS (
        INSERT INTO deliveries (client_id, message_id, qos, state)
        SELECT m.client_id, v_msg_id, m.deliver_qos, 0
          FROM matches m
         WHERE m.deliver_qos > 0 OR m.connected
        RETURNING client_id
    )
    SELECT array_agg(DISTINCT m.broker_id)
      INTO v_brokers
      FROM matches m
     WHERE m.broker_id IS NOT NULL
       AND m.connected
       AND m.client_id IN (SELECT client_id FROM inserted);

    msg_id := v_msg_id;
    broker_ids := COALESCE(v_brokers, '{}'::uuid[]);
    RETURN NEXT;
END;
$$ LANGUAGE plpgsql;

-- Retained messages also get expires_at — refresh helper here.
CREATE OR REPLACE FUNCTION mqtt_retained_expires_at(p_props JSONB)
RETURNS TIMESTAMPTZ LANGUAGE sql IMMUTABLE AS $$
    SELECT CASE
        WHEN p_props IS NULL THEN NULL
        WHEN COALESCE((p_props->>'me')::int, 0) <= 0 THEN NULL
        ELSE now() + make_interval(secs => (p_props->>'me')::int)
    END;
$$;
