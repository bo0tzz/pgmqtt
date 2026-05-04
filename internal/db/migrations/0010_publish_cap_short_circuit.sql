-- 12h soak profiling found `mqtt_publish` taking 260 ms mean (2.8 s tail)
-- with the matches CTE accounting for essentially the entire cost. Inside,
-- the `pending` CTE was running an unconstrained `SELECT count(*)` on
-- `deliveries` per matching subscriber per publish — five subs × one count
-- each ≈ 5 × ~60 ms on a heavily-bloated index = the observed cost.
--
-- Two fixes apply, this migration ships both:
--
-- (1) Rewrite `pending` to short-circuit at `p_max_queued + 1`. We never
--     need the exact count, only "is depth >= cap?". Use
--     `EXISTS (SELECT 1 FROM deliveries WHERE … OFFSET p_max_queued LIMIT 1)`
--     so the scan stops at the (cap+1)-th matching row. Also filter by
--     `state IN (0,1,2)` so the `deliveries_client_id_resume_idx` partial
--     index from migration 0008 is usable. When `p_max_queued = 0` (cap
--     disabled) the EXISTS is skipped via CASE.
--
-- (2) Tune autovacuum on `deliveries` and `messages` — both high-churn
--     tables. Defaults (`autovacuum_vacuum_scale_factor=0.2`) wait until
--     20% of the table is dead before triggering; the soak's deliveries
--     table accumulated 2.24M dead tuples / 641 MB despite 724 autovacuum
--     runs. Drop scale_factor to 0.02 (vacuum at 2% bloat), bump
--     cost_limit so each pass clears more before yielding, and set
--     threshold to 1000 so small-but-bloated cases trigger reliably.

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

    -- matches: one row per matching subscriber, with the highest applicable QoS.
    -- pending: per-client over_cap boolean — short-circuited via OFFSET+LIMIT
    --          and filtered by state IN (0,1,2) so the partial index hits.
    --          When p_max_queued=0 (cap disabled) the EXISTS is skipped.
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
               (CASE WHEN p_max_queued = 0 THEN false
                     ELSE EXISTS (
                         SELECT 1 FROM deliveries d
                          WHERE d.client_id = m.client_id
                            AND d.state IN (0, 1, 2)
                          OFFSET p_max_queued LIMIT 1
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

-- High-churn table autovacuum tuning. Default scale_factor=0.2 means
-- vacuum waits for 20% of the table to be dead before triggering, which
-- under sustained publish load lets bloat accumulate faster than vacuum
-- can clear it. Lower threshold + scale_factor + bigger cost_limit keep
-- bloat in check.
ALTER TABLE deliveries SET (
    autovacuum_vacuum_scale_factor = 0.02,
    autovacuum_vacuum_threshold = 1000,
    autovacuum_vacuum_cost_limit = 2000,
    autovacuum_analyze_scale_factor = 0.05,
    autovacuum_analyze_threshold = 1000
);

ALTER TABLE messages SET (
    autovacuum_vacuum_scale_factor = 0.05,
    autovacuum_vacuum_threshold = 1000,
    autovacuum_vacuum_cost_limit = 2000,
    autovacuum_analyze_scale_factor = 0.05,
    autovacuum_analyze_threshold = 1000
);
