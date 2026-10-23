-- A content index's log in commit order (docs/scm.md, "The content index"): each row carries its
-- transaction's commit timestamp — a staged row its batch marker's — so a reader reads only what
-- committed after its last read, the snapshot's timestamp it keeps as its place, and a fold records
-- the place it read to: its generation holds every row committed by then, which a fold deletes
-- after it, not in the transaction that makes the generation. Rows are no longer numbered by a
-- counter of the index's, which every logging transaction wrote: a transaction numbers its own,
-- in order, from a random base. An insert that names no time — an older binary's — commits its
-- own.
ALTER TABLE content_log ADD COLUMN logged_at TIMESTAMP DEFAULT (PENDING_COMMIT_TIMESTAMP()) OPTIONS (allow_commit_timestamp = true);
ALTER TABLE content_log_batch ADD COLUMN logged_at TIMESTAMP DEFAULT (PENDING_COMMIT_TIMESTAMP()) OPTIONS (allow_commit_timestamp = true);
ALTER TABLE content_index ADD COLUMN folded_through TIMESTAMP;
CREATE INDEX content_log_logged ON content_log (scope, owner_id, logged_at) STORING (batch_id);
CREATE NULL_FILTERED INDEX content_log_batched ON content_log (scope, owner_id, batch_id);
CREATE INDEX content_log_batch_logged ON content_log_batch (scope, owner_id, logged_at);
