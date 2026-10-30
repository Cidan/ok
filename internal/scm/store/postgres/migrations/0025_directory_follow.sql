-- The SCM follows the directory's events (docs/hosted.md, "Work without leaders"): a repository
-- deleted is purged once its retention ends, a job booked as the event is applied. Its place in
-- each repository's log on the bus's stream (pgdata.CursorsSchema).
CREATE TABLE follow_cursor (
    topic text   NOT NULL,
    key   bigint NOT NULL,
    seq   bigint NOT NULL,
    PRIMARY KEY (topic, key)
);
