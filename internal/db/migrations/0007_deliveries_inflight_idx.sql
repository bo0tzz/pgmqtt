-- Add a partial index to make the broker's "next inflight delivery for
-- this client" query stop falling back to a deliveries_pkey scan.
--
-- The query in internal/engine/conn.go (drainSessionQueue's outbound
-- SELECT, plus the on-publish own-deliveries scan) is
--
--     SELECT ...
--       FROM deliveries d JOIN messages m ON m.id = d.message_id
--      WHERE d.client_id = $1 AND d.state = 0 AND d.qos > 0
--        AND (m.expires_at IS NULL OR m.expires_at > now())
--      ORDER BY d.id LIMIT 1
--
-- The existing deliveries_client_state_idx (client_id, state, id) covers
-- (client_id, state) equality + id ordering, but EXPLAIN ANALYZE under
-- load showed the planner picking the pkey and applying client_id as a
-- post-filter, scanning the whole pkey for each call. ~19k buffer hits
-- per call, 9.4 ms × 27k calls = 13% of PG time on the deep-dive run.
--
-- A partial index that *exactly* matches the WHERE clause (state=0 AND
-- qos>0) lets the planner pick it deterministically: client_id is a
-- prefix, id is the order, and the predicate prunes everything else.
-- The full index is much smaller too (most deliveries spend zero time
-- in state=0,qos>0 — they're either acked-and-gone or in higher states
-- mid-handshake), so cache footprint is negligible.

CREATE INDEX IF NOT EXISTS deliveries_client_id_inflight_idx
    ON deliveries(client_id, id)
 WHERE state = 0 AND qos > 0;
