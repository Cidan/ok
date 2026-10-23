-- The audit log's lines have random IDs (internal/data.NewID), so they are unique across every
-- installation and cell, and no database hands them out in order (docs/hosted.md, "Shared
-- primitives"); a line's order is its time. Lines written before keep theirs.
ALTER TABLE audit ALTER COLUMN id DROP DEFAULT;
DROP SEQUENCE audit_id_seq;
