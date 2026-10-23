-- Runs, plans, submissions, and attempts have random IDs (internal/data.NewID), each chosen as its
-- row is made, so they are unique across every installation and cell a repository may move
-- between, and no database hands them out in order (docs/hosted.md, "Shared primitives"). What
-- needs their order reads it from their times: a run's queue time, the others' creation, the ID
-- breaking ties. Rows made before keep theirs.
ALTER TABLE plan ALTER COLUMN id DROP IDENTITY;
ALTER TABLE run ALTER COLUMN id DROP IDENTITY;
ALTER TABLE submission ALTER COLUMN id DROP IDENTITY;
ALTER TABLE attempt ALTER COLUMN id DROP IDENTITY;

DROP INDEX run_queued;
CREATE INDEX run_queued ON run (priority DESC, queued_at, id) WHERE status = 1;
DROP INDEX run_cl;
CREATE INDEX run_cl ON run (repository_id, cl_number, patchset, queued_at, id);
DROP INDEX run_commit;
CREATE INDEX run_commit ON run (repository_id, commit_id, queued_at, id);
DROP INDEX run_plan;
CREATE INDEX run_plan ON run (plan_id, queued_at, id);
DROP INDEX submission_active;
CREATE INDEX submission_active ON submission (repository_id, created_at, id) WHERE status = 1;
DROP INDEX attempt_submission;
CREATE INDEX attempt_submission ON attempt (submission_id, created_at, id);
