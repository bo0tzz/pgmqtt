-- Migration 0001 created subscriptions_filter_idx ON subscriptions(topic_filter)
-- intending to support a filter-keyed lookup. The query that would have used
-- it never landed: every read of `subscriptions` is either by the PK
-- (client_id, topic_filter) or a full-table scan with mqtt_topic_match (the
-- publish-fanout join, which has no equality predicate the index could help).
--
-- Net result: the index pays INSERT/UPDATE cost on every SUBSCRIBE for zero
-- query benefit. Drop it.
DROP INDEX IF EXISTS subscriptions_filter_idx;
