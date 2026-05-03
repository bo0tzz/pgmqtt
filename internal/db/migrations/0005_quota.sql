-- Slow-subscriber backpressure. mqtt_publish gains a p_max_queued cap. When
-- a matching subscriber already has >= p_max_queued rows in deliveries we
-- skip inserting any new delivery for them; QoS-0 drops are silent (the spec
-- permits it), QoS>0 drops are surfaced to the caller in overflow_clients
-- so the engine can DISCONNECT 0x97 (Quota Exceeded) the offending conn.
--
-- p_max_queued = 0 means "unlimited" (no cap, behaves like pre-0005).

DROP FUNCTION IF EXISTS mqtt_publish(TEXT, BYTEA, SMALLINT, BOOLEAN, JSONB, TEXT);

CREATE OR REPLACE FUNCTION mqtt_publish(
    p_topic         TEXT,
    p_payload       BYTEA,
    p_qos           SMALLINT,
    p_retain        BOOLEAN,
    p_properties    JSONB,
    p_publisher     TEXT,
    p_max_queued    INT DEFAULT 0
) RETURNS TABLE (msg_id BIGINT, broker_ids UUID[], overflow_clients TEXT[]) AS $$
DECLARE
    v_msg_id  BIGINT;
    v_brokers UUID[];
    v_overflow TEXT[];
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

    -- matches: one row per client, with the highest applicable QoS.
    -- pending: count of currently-queued rows in deliveries per client.
    -- over_cap: clients whose pending >= cap (when cap > 0).
    -- inserted: client_ids that actually got a delivery row.
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
    pending AS (
        SELECT m.client_id,
               (SELECT count(*) FROM deliveries d WHERE d.client_id = m.client_id) AS depth
          FROM matches m
    ),
    inserted AS (
        INSERT INTO deliveries (client_id, message_id, qos, state)
        SELECT m.client_id, v_msg_id, m.deliver_qos, 0
          FROM matches m
          JOIN pending p ON p.client_id = m.client_id
         WHERE (m.deliver_qos > 0 OR m.connected)
           AND (p_max_queued = 0 OR p.depth < p_max_queued)
        RETURNING client_id
    )
    SELECT
        array_agg(DISTINCT m.broker_id) FILTER (WHERE m.broker_id IS NOT NULL AND m.connected
                                                   AND m.client_id IN (SELECT client_id FROM inserted)),
        array_agg(DISTINCT m.client_id) FILTER (WHERE p_max_queued > 0
                                                   AND m.deliver_qos > 0
                                                   AND m.client_id IN (SELECT pp.client_id FROM pending pp WHERE pp.depth >= p_max_queued))
      INTO v_brokers, v_overflow
      FROM matches m;

    msg_id := v_msg_id;
    broker_ids := COALESCE(v_brokers, '{}'::uuid[]);
    overflow_clients := COALESCE(v_overflow, '{}'::text[]);
    RETURN NEXT;
END;
$$ LANGUAGE plpgsql;
