-- Operation logs leave the tail for segments (segment kind 9) once settled — touched before
-- collection's window — and collection, finding one there unreachable, writes a tombstone here,
-- which compaction moves into a segment too. A read takes the tail's row, then a tombstone here,
-- then the newest segment's record. A re-publish deletes a tombstone as it puts the row again.
CREATE TABLE op_tombstone (
    op_log_id bigint   NOT NULL REFERENCES op_log ON DELETE CASCADE,
    kind      smallint NOT NULL,                     -- 6 operation, 7 view
    id        bytea    NOT NULL,
    PRIMARY KEY (op_log_id, kind, id)
);
