-- Where a repository's compaction looks next through the parts of its tail no index finds what
-- moves of (store.tailCursor): read as a pass begins and written as it ends, by whichever replica
-- runs it — kept in the database, not a replica (docs/state.md). A hint: one lost, or left behind
-- by a move, starts the windows again from the tail's start.
CREATE TABLE compaction_cursor (
    repository_id INT64      NOT NULL,
    cursor        BYTES(MAX) NOT NULL,
) PRIMARY KEY (repository_id);
