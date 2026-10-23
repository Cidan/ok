-- The deployer follows the SCM's events on the bus's stream (docs/hosted.md, "Consumers"), its
-- place in each repository's log here (pgdata.CursorsSchema); feed_cursor is its place in the
-- SCM's feed before, until it leaves it.
CREATE TABLE follow_cursor (
    topic text   NOT NULL,
    key   bigint NOT NULL,
    seq   bigint NOT NULL,
    PRIMARY KEY (topic, key)
);
