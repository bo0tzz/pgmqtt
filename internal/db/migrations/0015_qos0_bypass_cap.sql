-- QoS-0 subscribers no longer accumulate `deliveries` rows after
-- migration 0014 + the deliver.go DELETE-on-send change. The per-client
-- cap was put in place to bound queue growth for slow subscribers, but
-- that growth only happens for QoS≥1 (which holds rows in state 0/1/2
-- until ack). For QoS-0, the cap was a footgun: once a connected
-- subscriber's row count happened to reach p_max_queued (with the
-- pre-fix accumulation that's now closed), every subsequent QoS-0
-- delivery was dropped silently — `overflow_clients` only populates
-- for deliver_qos > 0, so neither the publisher nor the operator saw
-- a signal. Real-world repro: zigbee2mqtt's `clean_start=true` sub
-- subscribed to `zigbee2mqtt/#` with `no_local=false`, hit 10000 rows
-- in `deliveries`, and silently stopped receiving Home Assistant's
-- `/set` commands.
--
-- Fix: skip the over_cap gate when deliver_qos = 0. QoS-0 is at-most-
-- once by spec, and with the delete-on-send change there's no queue
-- to bound. QoS≥1 keeps the existing gate + the matching
-- DISCONNECT-0x97 surfacing via overflow_clients.

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
           AND (m.deliver_qos = 0 OR NOT p.over_cap)
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
