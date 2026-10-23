-- Plans, services, deployments, and their log lines have random IDs (internal/data.NewID), each
-- chosen as its row is made, so they are unique across every installation and cell a repository
-- may move between, and no database hands them out in order (docs/hosted.md, "Shared
-- primitives"). What needs their order reads it from their times, the ID breaking ties. Rows made
-- before keep theirs.
ALTER TABLE plan ALTER COLUMN id DROP IDENTITY;
ALTER TABLE service ALTER COLUMN id DROP IDENTITY;
ALTER TABLE deployment ALTER COLUMN id DROP IDENTITY;
ALTER TABLE deployment_event ALTER COLUMN id DROP IDENTITY;

DROP INDEX deployment_open;
CREATE INDEX deployment_open ON deployment (created_at, id) WHERE status IN (1, 2);
DROP INDEX deployment_deploy;
CREATE INDEX deployment_deploy ON deployment (repository_id, deploy, created_at, id);
CREATE INDEX deployment_recent ON deployment (repository_id, created_at, id);
DROP INDEX deployment_event_of;
CREATE INDEX deployment_event_of ON deployment_event (deployment_id, at, id);
