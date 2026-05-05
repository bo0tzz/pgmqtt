-- Extend mqtt_publish RETURNS TABLE with delivered_count BIGINT.
--
-- Lets the broker observe a histogram of "subscribers fanned out per
-- publish" without a second SQL roundtrip. The count comes directly
-- from the existing `inserted` CTE in the function body, so it costs
-- one extra aggregate pass over the same set of rows we already build.
--
-- Publishes with no matching subscriber → 0. Publishes that hit the
-- per-client deliveries cap → only the non-overflow inserts are counted
-- (the overflow set is already surfaced via overflow_clients). This
-- matches the metric's semantic: "how many deliveries did we actually
-- create for this publish?"
--
-- Used by metrics/publish.go to feed pgmqtt_publish_fanout_subscribers.

DROP FUNCTION IF EXISTS mqtt_publish(TEXT, BYTEA, SMALLINT, BOOLEAN, JSONB, TEXT, INT);

CREATE OR REPLACE FUNCTION mqtt_publish(
    p_topic         TEXT,
    p_payload       BYTEA,
    p_qos           SMALLINT,
    p_retain        BOOLEAN,
    p_properties    JSONB,
    p_publisher     TEXT,
    p_max_queued    INT DEFAULT 0
) RETURNS TABLE (msg_id BIGINT, broker_ids UUID[], overflow_clients TEXT[], delivered_count BIGINT) AS $$
DECLARE
    v_msg_id  BIGINT;
    v_brokers UUID[];
    v_overflow TEXT[];
    v_delivered BIGINT;
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
    pending AS (
        SELECT m.client_id,
               (CASE WHEN p_max_queued = 0 THEN false
                     ELSE EXISTS (
                         SELECT 1 FROM deliveries d
                          WHERE d.client_id = m.client_id
                            AND d.state IN (0, 1, 2)
                          OFFSET (p_max_queued - 1) LIMIT 1
                     )
                END) AS over_cap
          FROM matches m
    ),
    inserted AS (
        INSERT INTO deliveries (client_id, message_id, qos, state)
        SELECT m.client_id, v_msg_id, m.deliver_qos, 0
          FROM matches m
          JOIN pending p ON p.client_id = m.client_id
         WHERE (m.deliver_qos > 0 OR m.connected)
           AND NOT p.over_cap
        RETURNING client_id
    )
    SELECT
        array_agg(DISTINCT m.broker_id) FILTER (WHERE m.broker_id IS NOT NULL AND m.connected
                                                   AND m.client_id IN (SELECT client_id FROM inserted)),
        array_agg(DISTINCT m.client_id) FILTER (WHERE m.deliver_qos > 0
                                                   AND m.client_id IN (SELECT pp.client_id FROM pending pp WHERE pp.over_cap)),
        (SELECT count(*) FROM inserted)
      INTO v_brokers, v_overflow, v_delivered
      FROM matches m;

    msg_id := v_msg_id;
    broker_ids := COALESCE(v_brokers, '{}'::uuid[]);
    overflow_clients := COALESCE(v_overflow, '{}'::text[]);
    delivered_count := COALESCE(v_delivered, 0);
    RETURN NEXT;
END;
$$ LANGUAGE plpgsql;
