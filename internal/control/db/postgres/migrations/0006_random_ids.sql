-- Comments and a CL's log lines have random IDs (internal/data.NewID), each chosen as its row is
-- made, so they are unique across every installation and cell a repository may move between, and
-- no database hands them out in order (docs/hosted.md, "Shared primitives"). Their order is their
-- times: a comment's creation, the clock's as it is written; a log line's own time, and among
-- lines of one time — an upload's, say — when each was written. Rows made before keep their IDs,
-- which order them as before.
ALTER TABLE comment ALTER COLUMN id DROP IDENTITY;
ALTER TABLE event ALTER COLUMN id DROP IDENTITY;
ALTER TABLE event ADD COLUMN recorded_at timestamptz NOT NULL DEFAULT now();
DROP INDEX comment_cl;
CREATE INDEX comment_cl ON comment (repository_id, number, created_at, id);
DROP INDEX event_cl;
CREATE INDEX event_cl ON event (repository_id, number, at, recorded_at, id);
