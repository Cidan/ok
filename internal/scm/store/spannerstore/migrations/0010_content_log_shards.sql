-- The content log's commit-timestamp index carries a shard before the timestamp: every writer of an
-- index holds that index's entry at the pending commit timestamp until it commits — Spanner's
-- WriterSharedTimestamp lock, on the one key every writer's row shares then — so writers waited on
-- each other there whatever their rows; a writer picks its shard at random, and a reader of the log
-- reads every shard, merging them by timestamp.
ALTER TABLE content_log ADD COLUMN shard INT64 NOT NULL DEFAULT (0);
DROP INDEX content_log_logged;
CREATE INDEX content_log_logged ON content_log (scope, owner_id, shard, logged_at) STORING (batch_id);
