-- The SCM follows the directory's events (docs/hosted.md, "Work without leaders"): a repository
-- deleted is purged once its retention ends, a job booked as the event is applied. Its place in
-- each repository's log on the bus's stream (spannerdata.CursorsDDL).
CREATE TABLE follow_cursor (
    topic STRING(MAX) NOT NULL,
    key   INT64       NOT NULL,
    seq   INT64       NOT NULL,
) PRIMARY KEY (topic, key);
