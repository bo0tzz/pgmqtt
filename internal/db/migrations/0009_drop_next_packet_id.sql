-- Drop sessions.next_packet_id column and the mqtt_next_packet_id() SQL
-- function. Outbound packet-id allocation moves to a per-Conn in-memory
-- atomic counter (see internal/engine/conn.go::AllocPacketID).
--
-- Why: every QoS>0 outbound delivery did
--     UPDATE sessions SET next_packet_id = ... RETURNING next_packet_id
-- which, even after the FK drop in 0006 (~9.4× speedup), bloated the
-- sessions row over hours of operation due to HOT-update churn — every
-- delivery touches the same row, the visibility map keeps that page
-- hot, and dead tuples accumulate faster than autovacuum can reclaim
-- them. EXPLAIN ANALYZE under sustained load showed the table doubling
-- in size every few hours; vacuum was a constant tail-latency source.
--
-- MQTT only requires packet-id uniqueness across in-flight packets for a
-- given session — it does NOT require durability across crashes. On
-- broker restart, a fresh per-Conn counter seeded from
--     SELECT COALESCE(MAX(packet_id), 0) FROM deliveries
--      WHERE client_id=$1 AND packet_id IS NOT NULL
-- is fully sufficient: any unacked deliveries already hold their ids,
-- and the seed guarantees we won't reissue one. Collisions (truly rare
-- given the seed) are caught by the deliveries_client_packet_idx unique
-- partial index and the AllocPacketID retry loop.
--
-- The deliveries.packet_id column and its unique index stay — they're
-- what we seed *from* and the only authority for "is this id in flight".

DROP FUNCTION IF EXISTS mqtt_next_packet_id(TEXT);

ALTER TABLE sessions DROP COLUMN IF EXISTS next_packet_id;
