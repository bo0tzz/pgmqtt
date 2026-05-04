-- Fix off-by-one in the migration-0010 publish-cap short-circuit.
--
-- Migration 0010 rewrote the `pending` CTE to use `EXISTS (... OFFSET
-- p_max_queued LIMIT 1)`. That short-circuits at the (cap+1)-th matching
-- row — but the original semantics (migration 0005) gated INSERT on
-- `depth < p_max_queued`, i.e. cap means "reject when depth >= cap".
-- With OFFSET p_max_queued the EXISTS evaluates to true only when
-- `depth >= p_max_queued + 1`, which is one row too lenient.
--
-- Concretely: cap=2, two rows already queued. The original would skip
-- the insert (depth=2 NOT < cap=2). Migration 0010's logic returns 0
-- rows from `OFFSET 2 LIMIT 1`, so over_cap=false, the insert lands,
-- and the broker fails to DISCONNECT 0x97 the slow subscriber. The
-- existing `TestSlowSubscriberQuotaExceeded` regression test catches
-- exactly this.
--
-- Fix: `OFFSET (p_max_queued - 1) LIMIT 1` — returns a row exactly when
-- `depth >= p_max_queued`. The CASE branch on `p_max_queued = 0` keeps
-- the cap-disabled fast path; we never evaluate OFFSET -1.

DROP FUNCTION IF EXISTS mqtt_publish(TEXT, BYTEA, SMALLINT, BOOLEAN, JSONB, TEXT, INT);

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
                                                   AND m.client_id IN (SELECT pp.client_id FROM pending pp WHERE pp.over_cap))
      INTO v_brokers, v_overflow
      FROM matches m;

    msg_id := v_msg_id;
    broker_ids := COALESCE(v_brokers, '{}'::uuid[]);
    overflow_clients := COALESCE(v_overflow, '{}'::text[]);
    RETURN NEXT;
END;
$$ LANGUAGE plpgsql;
