-- A workspace's checkpoints, newest first, with whether each is a fork: its latest is the first
-- entry not a fork, read without the workspace's other checkpoints — or any other workspace's, which
-- a plan reading the table by repository would hold, so that recording one workspace's checkpoint
-- aborted another's. Its queries name it (@{FORCE_INDEX=checkpoint_newest}).
CREATE INDEX checkpoint_newest ON checkpoint (repository_id, principal, workspace, created_at DESC, id DESC) STORING (fork);
DROP INDEX checkpoint_workspace;
