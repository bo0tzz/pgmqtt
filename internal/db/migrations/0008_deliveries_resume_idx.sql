-- Add a partial index covering drainSessionQueue's resume scan so it
-- stops walking the broader pkey / (client_id, state, id) index.
--
-- The query in internal/engine/deliver.go::drainSessionQueue is
--
--     SELECT ...
--       FROM deliveries d JOIN messages m ON m.id = d.message_id
--      WHERE d.client_id = $1
--        AND d.state IN (0, 1, 2)
--        AND (m.expires_at IS NULL OR m.expires_at > now())
--      ORDER BY d.id
--
-- After 0006 (FK drop) and 0007 (state=0 AND qos>0 partial index)
-- landed, EXPLAIN ANALYZE under load showed this resume scan as the
-- new dominant PG hot path: ~36% of total PG time at 501 ms mean.
-- 0007's partial index doesn't match — its predicate is too narrow
-- (state=0 AND qos>0) — so the planner falls back to the
-- (client_id, state, id) index and walks dead-tuple chains for every
-- reconnect.
--
-- A partial index whose predicate exactly matches the resume WHERE
-- clause (state IN (0, 1, 2)) lets the planner pick it
-- deterministically: client_id is a prefix, id is the order, and the
-- predicate prunes the long-tail of acked-and-gone deliveries.
-- Footprint is small in steady state — the vast majority of
-- deliveries either never reach state>2 (QoS-0 ack-and-vanish) or
-- transition out quickly through the QoS-1 / QoS-2 handshake.
--
-- Note: not CREATE INDEX CONCURRENTLY. The migration framework runs
-- each file inside a transaction, and there is no production traffic
-- to keep online. A blocking CREATE INDEX is the simpler correct
-- choice here.

CREATE INDEX deliveries_client_id_resume_idx
    ON deliveries(client_id, id)
 WHERE state IN (0, 1, 2);
