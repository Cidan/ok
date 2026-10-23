-- Checkpoints and operation logs have random IDs (internal/data.NewID), each chosen as its row is
-- made, so they are unique across every installation and cell a repository may move between, and
-- no database hands them out in order (docs/hosted.md, "Shared primitives"). A workspace's
-- checkpoints are ordered by when they were made, the ID breaking ties. Rows made before keep
-- theirs. (A pack's number, a dictionary's, and the content log's order stay counters: each is
-- encoded compactly, and on Spanner each is a counter of its repository's.)
ALTER TABLE checkpoint ALTER COLUMN id DROP IDENTITY;
ALTER TABLE op_log ALTER COLUMN id DROP IDENTITY;
DROP INDEX checkpoint_workspace;
CREATE INDEX checkpoint_workspace ON checkpoint (repository_id, principal, workspace, created_at, id);
