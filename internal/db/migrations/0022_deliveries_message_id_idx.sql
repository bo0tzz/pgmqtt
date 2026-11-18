-- deliveries(message_id) was only indexed for state = 0 (0017), which is
-- what the fan-out lookup needs. Two other things look deliveries up by
-- message alone and could not use it: the cascade when a message is
-- deleted (the foreign key from 0001), and the orphan-message sweep's
-- "does anything still reference this message". Both degrade to scanning
-- the table once it is not tiny, in one statement under the pool's
-- statement timeout.
--
-- (message_id, state) serves all three, so it replaces the partial index
-- rather than joining it: deliveries is the hottest table for writes.
CREATE INDEX IF NOT EXISTS deliveries_message_id_state_idx ON deliveries(message_id, state);
DROP INDEX IF EXISTS deliveries_message_id_state0_idx;
