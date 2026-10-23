-- A segment's age orders a repository's segments of a kind, newest first, for kinds whose records a
-- newer one supersedes (the content index's, segment kind 8, as repacking moves a pinned chunk): a
-- segment written from the tail takes the next age; one merged from others takes the newest of
-- theirs, which no segment outside the merge comes between, since a merge takes every segment at its
-- level. created_at cannot order them: a merge beginning after a newer segment was written holds
-- older records.
CREATE SEQUENCE segment_age;
ALTER TABLE segment ADD COLUMN age bigint NOT NULL DEFAULT nextval('segment_age');
