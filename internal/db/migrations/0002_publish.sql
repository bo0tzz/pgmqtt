-- mqtt_publish performs the publisher-path fanout in a single round-trip:
--   * inserts the message
--   * inserts a delivery row per matching subscription, applying QoS
--     downgrade, no_local filtering, and offline-QoS-0 dropping
--   * computes the set of broker IDs that own currently-connected matching
--     subscribers; the caller is responsible for emitting pg_notify on those
--     channels (we return them rather than calling pg_notify here so the
--     caller can decide whether to NOTIFY in the same tx vs. after commit).
--
-- The function is invoked from a transactional context and so should not COMMIT.
-- It does not handle retain — the caller writes the retained table separately.
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
BEGIN
    INSERT INTO messages(topic, payload, qos, retain, properties, publisher_client_id)
    VALUES (p_topic, p_payload, p_qos, p_retain, p_properties, p_publisher)
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
