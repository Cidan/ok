-- What a transaction writes that its repository's sequencing pass orders just after it commits
-- (docs/scm.md, "Sequencing"): each CL it makes, to number, and each event, to place in the log.
-- Rows sort in commit order: by the transaction's commit timestamp, then a random tie of its own,
-- then their place in it. The pass deletes what it sequences, so the table holds rows for an
-- instant; it is not interleaved, so finding a repository with rows a writer left is a scan of it
-- alone.
CREATE TABLE sequence_pending (
    repository_id INT64      NOT NULL,
    committed_at  TIMESTAMP  NOT NULL OPTIONS (allow_commit_timestamp = true),
    txn           INT64      NOT NULL,
    place         INT64      NOT NULL,
    changelist_id INT64,                -- a CL to number
    event_at      TIMESTAMP,            -- an event, and when its writer wrote it
    body          BYTES(MAX),           -- an ok.scm.v1.Event, its new CLs by their provisional numbers
) PRIMARY KEY (repository_id, committed_at, txn, place);

-- The tie of the transaction an event was written in, by which the writer finds its events' places
-- when another replica's pass sequenced them.
ALTER TABLE event ADD COLUMN txn INT64;
CREATE NULL_FILTERED INDEX event_txn ON event (repository_id, txn);
