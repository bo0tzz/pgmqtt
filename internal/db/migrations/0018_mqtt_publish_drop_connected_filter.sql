-- mqtt_publish: don't filter the broker_ids fan-out on `m.connected`.
--
-- Background: a 2026-06-29 production incident pinned the root cause to
-- `m.connected` being load-bearing in the NOTIFY routing path. A CNPG
-- primary failover killed every broker's listener PG session simultaneously
-- (SQLSTATE 57P01). PG releases pg_advisory_locks automatically on session
-- death, so during the window where all brokers were reconnecting, NO
-- broker held its broker-UUID advisory lock. The first broker to reconnect
-- ran `find_dead_brokers`, saw every other broker's lock available, and
-- ran `handleDeadBroker` for them — marking their sessions
-- connected=false, broker_id=NULL. The "reaped" brokers reconnected
-- seconds later, but the damage was done: their clients' session rows
-- in the DB now lied about reality (clients' TCP sockets to those
-- brokers stayed alive — TCP layer was undisturbed — but the DB said
-- they were disconnected).
--
-- This function was the bug's load-bearing surface. The broker_ids
-- aggregate filtered on `m.connected`, so a session with connected=false
-- dropped out of the NOTIFY fan-out. Result: publishes for those clients'
-- topics inserted delivery rows but never NOTIFY'd the owning broker,
-- and the broker never knew to drain them. Silent data loss for as long
-- as the divergence persisted.
--
-- The fix here is the routing side: drop the `m.connected` filter from
-- the broker_ids aggregate. Routing depends only on broker_id being
-- non-NULL. If broker_id points at a broker that's actually dead, the
-- NOTIFY lands on no listener — harmless. If it points at a broker that's
-- alive but whose `connected` is stale, the NOTIFY arrives and the
-- broker delivers via its in-memory ConnFor lookup. `connected` is no
-- longer load-bearing for delivery; it stays as an observability field.
--
-- The companion fix is in the engine: after listener reconnect, each
-- broker self-attests its in-memory sessions back to the truth (broker_id,
-- connected, session_token gate to avoid stomping legitimate takeovers).
-- Together: routing tolerates brief stale state, and the broker
-- self-heals when it comes back from a flap.
--
-- The QoS-0 INSERT gate at line 77 (`m.deliver_qos > 0 OR m.connected`)
-- is intentionally left alone. QoS-0 is at-most-once; not inserting a
-- delivery row for a "disconnected" client matches the spec, and the
-- brief window between reap and self-attestation is bounded to ~1s after
-- the listener reconnects in practice. The transient miss is acceptable
-- for QoS-0; QoS≥1 stays correct because that gate already passes
-- regardless of connected.

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
        -- broker_ids: route NOTIFY for fan-out. Filter changed in 0018:
        -- no longer requires `m.connected` — routing is broker_id-only.
        -- A NULL broker_id (genuinely unowned session) still skips, but
        -- a stale connected=false on a row whose broker_id points at a
        -- live broker no longer drops the NOTIFY on the floor.
        array_agg(DISTINCT m.broker_id) FILTER (WHERE m.broker_id IS NOT NULL
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
