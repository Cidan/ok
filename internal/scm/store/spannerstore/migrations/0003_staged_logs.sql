-- A transaction that logs more content index updates than a Spanner transaction holds (100 MiB: a
-- pin walk over a million and a half files) stages the rest of its log rows in transactions of
-- their own under a batch, and commits the batch's marker: a staged row reads only once its
-- batch's marker has committed, as if the transaction had written it. A fold deletes the staged
-- rows of batches with no marker, whose transactions are gone, and the markers of batches whose
-- rows it folded.
ALTER TABLE content_log ADD COLUMN batch_id INT64;

CREATE TABLE content_log_batch (
    scope    INT64 NOT NULL,
    owner_id INT64 NOT NULL,
    batch_id INT64 NOT NULL,
) PRIMARY KEY (scope, owner_id, batch_id), INTERLEAVE IN content_index;
