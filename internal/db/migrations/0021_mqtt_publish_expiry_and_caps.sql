-- Three corrections to the publish path's SQL.
--
-- 1. Message Expiry Interval is a uint32. Both functions below cast it to
--    INT, so any value of 2^31 or more made the publish fail outright
--    (the publisher got reason 0x88 on every attempt). BIGINT holds it.
--
-- 2. The per-client queue cap counted every delivery row but only ever
--    applied to QoS>0 deliveries (0015). A burst of QoS 0 rows waiting
--    for a connected client's sender could therefore push that client
--    "over the cap" and get its next QoS 1 message dropped and the
--    client disconnected, while QoS 0 itself had no bound at all: a
--    connected client that reads slowly could grow the table without
--    limit. The cap is now applied per class. Queued QoS>0 rows count
--    against QoS>0 deliveries, queued QoS 0 rows against QoS 0
--    deliveries, and in both cases going over means the message is not
--    queued for that client and the client is reported in
--    overflow_clients (the broker disconnects it with 0x97). A client
--    that is that far behind on QoS 0 loses the backlog when its session
--    ends, which is what at-most-once allows, and starts clean.
--
-- 3. overflow_clients no longer lists a QoS 0 subscriber that is not
--    connected: nothing was going to be queued for it either way.
--
-- Signatures are unchanged, so brokers of the previous version keep
-- working against these during a rolling upgrade.

CREATE OR REPLACE FUNCTION mqtt_retained_expires_at(p_props JSONB)
RETURNS TIMESTAMPTZ LANGUAGE sql STABLE AS $$
    SELECT CASE
        WHEN p_props IS NULL THEN NULL
        WHEN COALESCE((p_props->>'me')::bigint, 0) <= 0 THEN NULL
        ELSE now() + make_interval(secs => (p_props->>'me')::bigint)
    END;
$$;

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
    v_me BIGINT;
BEGIN
    v_me := COALESCE((p_properties->>'me')::bigint, 0);
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
    -- QoS 0 is only queued for a client that is connected to receive it.
    wanted AS (
        SELECT * FROM matches m WHERE m.deliver_qos > 0 OR m.connected
    ),
    pending AS (
        SELECT w.client_id,
               (p_max_queued > 0 AND EXISTS (
                    SELECT 1 FROM deliveries d
                     WHERE d.client_id = w.client_id
                       AND d.state IN (0, 1, 2)
                       AND (d.qos > 0) = (w.deliver_qos > 0)
                     OFFSET (p_max_queued - 1) LIMIT 1
                )) AS over_cap
          FROM wanted w
    ),
    inserted AS (
        INSERT INTO deliveries (client_id, message_id, qos, state)
        SELECT w.client_id, v_msg_id, w.deliver_qos, 0
          FROM wanted w
          JOIN pending p ON p.client_id = w.client_id
         WHERE NOT p.over_cap
        RETURNING client_id
    )
    SELECT
        -- Routing is by broker_id alone (0018): a row that names a live
        -- broker gets its NOTIFY whatever `connected` says.
        array_agg(DISTINCT w.broker_id) FILTER (WHERE w.broker_id IS NOT NULL
                                                   AND w.client_id IN (SELECT client_id FROM inserted)),
        array_agg(DISTINCT w.client_id) FILTER (WHERE w.client_id IN (SELECT pp.client_id FROM pending pp WHERE pp.over_cap)),
        (SELECT count(*) FROM inserted)
      INTO v_brokers, v_overflow, v_delivered
      FROM wanted w;

    msg_id := v_msg_id;
    broker_ids := COALESCE(v_brokers, '{}'::uuid[]);
    overflow_clients := COALESCE(v_overflow, '{}'::text[]);
    delivered_count := COALESCE(v_delivered, 0);
    RETURN NEXT;
END;
$$ LANGUAGE plpgsql;
