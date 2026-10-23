-- A content index's log in the order it applies (docs/scm.md, "The content index"). Each row
-- carries its transaction's ID, so a reader reads only the rows its last snapshot did not see —
-- the snapshot it keeps as its place — and a fold records the snapshot it read to: its generation
-- holds every row visible in it, which a fold deletes after it, not in the transaction that makes
-- the generation. A row's stamp orders it against the transactions that hold the index alone: a
-- writer, holding the index in share mode, stamps what the index's sequence says; one that holds it
-- alone — a sweep, compaction — stamps the next, and moves the sequence past it.
ALTER TABLE content_log ADD COLUMN xid xid8 NOT NULL DEFAULT pg_current_xact_id(), ADD COLUMN stamp bigint NOT NULL DEFAULT 0;
CREATE INDEX content_log_xid ON content_log (scope, owner_id, xid);
ALTER TABLE content_index ADD COLUMN folded_through pg_snapshot, ADD COLUMN log_seq bigint NOT NULL DEFAULT 0;
